// Relay: the bridge from the event store to Kafka (an application-level, log-based
// "CDC").
//
// Why a separate relay instead of publishing to Kafka inside the command service?
// Because writing to Postgres AND Kafka in one request is a dual write: if the DB
// commit succeeds but the Kafka publish fails (or vice versa), the two systems
// disagree and there is no clean way to recover. The event store is our single
// source of truth. The relay reads committed events in global id order and
// republishes them to Kafka, tracking a checkpoint so it resumes exactly where it
// left off. Delivery is at-least-once (a crash between publish and checkpoint
// update re-publishes a few events), which is fine because downstream projectors
// are idempotent.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1morshed1/eventflow/internal/config"
	"github.com/1morshed1/eventflow/internal/db"
	"github.com/1morshed1/eventflow/internal/eventstore"
	"github.com/1morshed1/eventflow/internal/kafka"
	"github.com/1morshed1/eventflow/internal/metrics"
)

const batchSize = 500

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := config.Env("EVENTSTORE_DSN", "postgres://eventflow:eventflow@localhost:5432/eventstore?sslmode=disable")
	pool, err := db.Connect(ctx, dsn)
	if err != nil {
		slog.Error("connect event store", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := eventstore.New(pool)
	brokers := config.EnvList("KAFKA_BROKERS", "localhost:9092")
	pub := kafka.NewPublisher(brokers)
	defer pub.Close()

	// Expose Prometheus metrics (this service has no HTTP API of its own).
	go func() {
		addr := config.Env("METRICS_ADDR", ":9100")
		if err := metrics.Serve(addr); err != nil {
			slog.Error("metrics server", "error", err)
		}
	}()

	pollInterval := 200 * time.Millisecond
	slog.Info("relay started", "brokers", brokers)

	for {
		select {
		case <-ctx.Done():
			slog.Info("relay shutting down")
			return
		default:
		}

		n, err := pumpOnce(ctx, store, pub)
		if err != nil {
			slog.Error("relay pump", "error", err)
			time.Sleep(time.Second) // back off on error
			continue
		}
		if n == 0 {
			// Caught up: nothing new. Sleep briefly to avoid busy-looping. A
			// production system would use LISTEN/NOTIFY to wake instantly.
			time.Sleep(pollInterval)
		}
	}
}

// pumpOnce publishes one batch of new events and advances the checkpoint. It
// returns the number of events published. The order matters for correctness:
//  1. read checkpoint,
//  2. read events after it,
//  3. publish to Kafka,
//  4. only then advance the checkpoint.
//
// If we crash after step 3 but before step 4, those events are published again on
// restart (at-least-once) - never lost.
func pumpOnce(ctx context.Context, store *eventstore.Store, pub *kafka.Publisher) (int, error) {
	last, err := store.RelayCheckpoint(ctx)
	if err != nil {
		return 0, err
	}

	events, err := store.LoadAllAfter(ctx, last, batchSize)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}

	if err := pub.Publish(ctx, events); err != nil {
		return 0, err
	}

	newCheckpoint := events[len(events)-1].ID
	if err := store.SetRelayCheckpoint(ctx, newCheckpoint); err != nil {
		return 0, err
	}
	metrics.RelayPublished.Add(float64(len(events)))
	metrics.RelayCheckpoint.Set(float64(newCheckpoint))
	slog.Info("relayed events", "count", len(events), "checkpoint", newCheckpoint)
	return len(events), nil
}
