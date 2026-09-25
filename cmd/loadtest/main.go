// Load tester for EventFlow. Measures the three numbers the plan asks for:
//
//  1. write throughput   - commands/sec and events/sec into the event store,
//  2. read latency       - p50/p95/p99 of GET /orders/{id} from the read model,
//  3. projection lag     - delay between a write committing and the read model
//                          reflecting it (write -> relay -> Kafka -> projector).
//
// Stdlib only. Run it on the compose network so it can reach the internal service
// names; see docs/09-load-testing.md for the exact command.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1morshed1/eventflow/internal/config"
)

func main() {
	cmdURL := config.Env("COMMAND_URL", "http://command-service:8080")
	qryURL := config.Env("QUERY_URL", "http://query-service:8081")
	workers := config.EnvInt("WORKERS", 32)
	duration := time.Duration(config.EnvInt("DURATION_SEC", 10)) * time.Second
	lagSamples := config.EnvInt("LAG_SAMPLES", 50)

	hc := &http.Client{Timeout: 10 * time.Second}
	waitHealthy(hc, cmdURL+"/healthz")
	waitHealthy(hc, qryURL+"/healthz")

	fmt.Printf("== EventFlow load test ==\ncommand=%s query=%s workers=%d duration=%s\n\n",
		cmdURL, qryURL, workers, duration)

	ids := throughput(hc, cmdURL, workers, duration)
	if len(ids) > 0 {
		readLatency(hc, qryURL, ids)
	}
	projectionLag(hc, cmdURL, qryURL, lagSamples)
}

// throughput: as many order lifecycles as possible for `duration`. Each lifecycle is
// 3 commands => 3 events (OrderCreated, ItemAdded, OrderConfirmed).
func throughput(hc *http.Client, cmdURL string, workers int, duration time.Duration) []string {
	fmt.Println("[1] Write throughput")
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var lifecycles int64
	var events int64
	var mu sync.Mutex
	var ids []string
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				id, ev, err := oneLifecycle(hc, cmdURL)
				if err != nil {
					continue
				}
				atomic.AddInt64(&lifecycles, 1)
				atomic.AddInt64(&events, int64(ev))
				if id != "" {
					mu.Lock()
					if len(ids) < 2000 {
						ids = append(ids, id)
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	fmt.Printf("  lifecycles: %d  (%.0f/sec)\n", lifecycles, float64(lifecycles)/elapsed)
	fmt.Printf("  events:     %d  (%.0f events/sec)\n\n", events, float64(events)/elapsed)
	return ids
}

// oneLifecycle: create -> add item -> confirm. Returns the order id and #events.
func oneLifecycle(hc *http.Client, cmdURL string) (string, int, error) {
	id, err := createOrder(hc, cmdURL)
	if err != nil {
		return "", 0, err
	}
	if err := post(hc, cmdURL+"/orders/"+id+"/items",
		`{"product_id":"`+newUUID()+`","quantity":1,"price":1000}`); err != nil {
		return id, 1, err
	}
	if err := post(hc, cmdURL+"/orders/"+id+"/confirm", ``); err != nil {
		return id, 2, err
	}
	return id, 3, nil
}

// readLatency: hammer GET /orders/{id} and report percentiles.
func readLatency(hc *http.Client, qryURL string, ids []string) {
	fmt.Println("[2] Read latency (GET /orders/{id})")
	// Give the read model a moment to contain the ids we just wrote.
	time.Sleep(2 * time.Second)

	const requests = 2000
	lat := make([]time.Duration, 0, requests)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)

	for i := 0; i < requests; i++ {
		id := ids[i%len(ids)]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			resp, err := hc.Get(qryURL + "/orders/" + id)
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			d := time.Since(t0)
			mu.Lock()
			lat = append(lat, d)
			mu.Unlock()
		}()
	}
	wg.Wait()
	printPercentiles("  ", lat)
	fmt.Println()
}

// projectionLag: write, then poll the read side until the order appears with the
// expected status; record the delay.
func projectionLag(hc *http.Client, cmdURL, qryURL string, samples int) {
	fmt.Printf("[3] Projection lag (write -> visible in read model), %d samples\n", samples)
	lags := make([]time.Duration, 0, samples)

	for i := 0; i < samples; i++ {
		id, err := createOrder(hc, cmdURL)
		if err != nil {
			continue
		}
		t0 := time.Now()
		deadline := t0.Add(10 * time.Second)
		for time.Now().Before(deadline) {
			resp, err := hc.Get(qryURL + "/orders/" + id)
			if err == nil {
				code := resp.StatusCode
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if code == http.StatusOK {
					lags = append(lags, time.Since(t0))
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	printPercentiles("  ", lags)
}

// --- helpers ---

func createOrder(hc *http.Client, cmdURL string) (string, error) {
	resp, err := hc.Post(cmdURL+"/orders", "application/json",
		bytes.NewReader([]byte(`{"customer_id":"`+newUUID()+`"}`)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("create -> %d", resp.StatusCode)
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.ID, nil
}

func post(hc *http.Client, url, body string) error {
	resp, err := hc.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s -> %d", url, resp.StatusCode)
	}
	return nil
}

func printPercentiles(indent string, ds []time.Duration) {
	if len(ds) == 0 {
		fmt.Println(indent + "no samples")
		return
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	p := func(q float64) time.Duration { return ds[int(float64(len(ds)-1)*q)] }
	fmt.Printf("%sn=%d  p50=%s  p95=%s  p99=%s  max=%s\n",
		indent, len(ds), p(0.50).Round(time.Microsecond), p(0.95).Round(time.Microsecond),
		p(0.99).Round(time.Microsecond), ds[len(ds)-1].Round(time.Microsecond))
}

func waitHealthy(hc *http.Client, url string) {
	for i := 0; i < 60; i++ {
		if resp, err := hc.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(time.Second)
	}
	log.Fatalf("service never became healthy: %s", url)
}

// newUUID makes a random UUID v4 string without pulling in a dependency here.
func newUUID() string {
	var b [16]byte
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> (i % 8 * 8))
	}
	// add per-call entropy
	seed := atomic.AddInt64(&uuidSeed, 1)
	b[8] = byte(seed) | 0x80
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[9] = byte(seed >> 8)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

var uuidSeed int64
