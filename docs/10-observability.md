# 10 — Observability

Every service is instrumented with **Prometheus** metrics, and the repo ships a
**Grafana** dashboard. This is the "can I see what the system is doing?" layer.

## Run it

```bash
make up             # the app stack
make observability  # Prometheus + Grafana (opt-in profile)
```

- Grafana: http://localhost:13000 (anonymous admin enabled) → dashboard **EventFlow**.
- Prometheus: http://localhost:19090 (try the Targets page — all four should be `up`).

The observability services live behind a Compose **profile**, so a plain `make up`
stays lean; `make down` tears down both.

## What's exposed

Instrumentation lives in one shared package, `internal/metrics`:

| Metric | Type | Meaning |
|--------|------|---------|
| `eventflow_http_requests_total{method,route,code}` | counter | HTTP requests |
| `eventflow_http_request_duration_seconds{method,route}` | histogram | request latency |
| `eventflow_events_appended_total` | counter | events written to the event store |
| `eventflow_relay_events_published_total` | counter | events published to Kafka |
| `eventflow_relay_checkpoint_event_id` | gauge | head of the stream (last published id) |
| `eventflow_projector_processed_total{projector}` | counter | events applied per projector |
| `eventflow_projector_position_event_id{projector}` | gauge | last event id applied per projector |

- HTTP services (`command`, `query`) expose `/metrics` on their API port.
- `relay` and `projector` have no API, so they serve `/metrics` on `:9100`.

The `route` label is the **pattern** (`POST /orders/{id}/items`), never the concrete
path — putting order ids in labels would explode cardinality and melt Prometheus.

## The headline metric: projection lag

Projection lag = how far each read model trails the write log. Compute it directly in
PromQL by subtracting the projector's position from the relay's head:

```promql
eventflow_relay_checkpoint_event_id{job="relay"}
  - on() group_right() eventflow_projector_position_event_id{job="projector"}
```

Watch this during `make loadtest`: it spikes while writes outrun the projectors, then
drains to ~0 — a live picture of eventual consistency ([09](09-load-testing.md)). It's
the number you'd alert on as an SLO ("orders_view more than N events / T seconds
behind").

## A gotcha worth understanding: shared registry, job-scoped queries

`internal/metrics` declares its metrics as package-level `promauto` vars, so **every
service that imports the package registers all of them** — most sitting at zero. That
means `eventflow_relay_checkpoint_event_id` shows up on all four jobs, not just the
relay.

Consequence: always **scope queries by `job`** (e.g. `{job="relay"}`), which is what
the dashboard does. Without it, an expression like the lag subtraction hits
"duplicate series … many-to-many matching not allowed", because the same metric name
appears under four jobs.

Alternative designs (not taken, to keep the shared package simple): give each service
its own registry and register only the metrics it uses, or split the metrics into
per-service packages. Job-scoping in queries is the pragmatic fix here.

## What's intentionally left as next steps

The spec lists a fuller stack (Jaeger, Loki). Metrics are the highest-leverage first
layer; the rest slot in cleanly:

- **Tracing (Jaeger / OpenTelemetry).** Propagate a `trace_id` through the event
  `Metadata` field (already reserved in the schema) so a command can be traced across
  relay → Kafka → projector. Add the OTel SDK to each service and a Jaeger container.
- **Logs (Loki + Promtail).** Services already log structured JSON via `slog`; point
  Promtail at the container logs and query them in Grafana beside the metrics.
- **Alerting.** Add Prometheus alert rules on projection lag and HTTP error rate.

These are additive — no change to the domain code — which is itself a sign the
boundaries are in the right place.
