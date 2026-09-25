// Package metrics provides Prometheus instrumentation shared by all services:
// an HTTP middleware that records request counts and latencies, a /metrics handler,
// and a couple of domain gauges (relay lag, projector lag). Keeping it in one place
// means every service exposes metrics the same way.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// httpRequests counts HTTP requests by method, route, and status code.
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "eventflow_http_requests_total",
		Help: "Total HTTP requests handled.",
	}, []string{"method", "route", "code"})

	// httpDuration is a latency histogram in seconds, by method and route. The
	// buckets are tuned for a fast read/write API (sub-ms to ~1s).
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "eventflow_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"method", "route"})

	// EventsAppended counts events written to the event store (command side).
	EventsAppended = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventflow_events_appended_total",
		Help: "Total events appended to the event store.",
	})

	// RelayPublished counts events the relay published to Kafka.
	RelayPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventflow_relay_events_published_total",
		Help: "Total events published to Kafka by the relay.",
	})

	// RelayCheckpoint is the global id of the last event the relay published. It
	// marks the head of the stream that consumers chase.
	RelayCheckpoint = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "eventflow_relay_checkpoint_event_id",
		Help: "Global id of the last event published by the relay.",
	})

	// ProjectorProcessed counts events each projector has applied.
	ProjectorProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "eventflow_projector_processed_total",
		Help: "Total events applied by a projector.",
	}, []string{"projector"})

	// ProjectorPosition is the global id of the last event a projector applied.
	// Projection lag (events behind) = relay_checkpoint - projector_position, which
	// you compute in Prometheus/Grafana without any cross-database query here.
	ProjectorPosition = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "eventflow_projector_position_event_id",
		Help: "Global id of the last event applied by a projector.",
	}, []string{"projector"})
)

// Handler returns the Prometheus /metrics HTTP handler.
func Handler() http.Handler { return promhttp.Handler() }

// Middleware records request count and latency for each handled request. `route` is
// a low-cardinality label (the pattern, not the concrete path) to avoid a metrics
// explosion from unique ids in URLs.
func Middleware(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(rec, r)
		httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
	}
}

// statusRecorder captures the response status code for the metrics labels.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Serve starts a tiny HTTP server exposing /metrics on addr, for services (relay,
// projector) that have no HTTP API of their own. It blocks; run it in a goroutine.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	return http.ListenAndServe(addr, mux)
}
