# 09 — Load Testing

The spec sets three targets. `cmd/loadtest/main.go` measures each. Run it against a
live stack:

```bash
make up
make loadtest
```

It runs on the compose network so it can reach the internal service names. Tune with
env vars: `WORKERS`, `DURATION_SEC`, `LAG_SAMPLES`.

## What it measures

1. **Write throughput** — order lifecycles/sec and events/sec appended to the event
   store. Each lifecycle = create + add item + confirm = 3 events.
2. **Read latency** — p50/p95/p99 of `GET /orders/{id}` from the read model.
3. **Projection lag** — time from a write committing to it being visible on the read
   side (write → relay → Kafka → projector → read DB).

## A representative run (single laptop, Docker Compose)

```
[1] Write throughput
  lifecycles: 18940  (1892/sec)
  events:     56820  (5677 events/sec)

[2] Read latency (GET /orders/{id})
  n=2000  p50=2.6ms  p95=9.5ms  p99=21.9ms  max=29.8ms

[3] Projection lag (write -> visible in read model), 40 samples
  p50=213ms  p95=306ms  p99=306ms  max=3.2s
```

Your numbers will vary with hardware. The point is the *shape* and how to read it.

## Interpreting the results (honest analysis)

### Write throughput: ~5.7k events/sec vs 10k target

The spec targets 10,000+ events/sec; a single Postgres on a laptop through the HTTP
API hits ~5.7k. Where the ceiling comes from and how you'd raise it:

- **Every command does a load (replay) + append.** Reads compete with writes on the
  same instance.
- **One event per INSERT, one HTTP round-trip per command.** Batching multiple events
  per append and using connection pooling/pipelining would help.
- **Optimistic-concurrency loads** add a read before each write.
- **Scaling levers**: partition the event store by aggregate id (shard), put it on
  real hardware/NVMe, batch appends, and add write connection pool tuning. The
  architecture scales horizontally on the write side because aggregates are
  independent.

### Read latency: p99 ~22ms vs <10ms target

Reads hit a single denormalized, indexed table — inherently fast (p50 = 2.6ms). The
p99 tail comes from:

- **No read replicas / no cache** in the demo. The spec's <10ms p99 assumes a tuned,
  possibly cached read tier.
- **Client-side contention** in the load tool (32 concurrent goroutines sharing one
  process) inflates the tail.
- **Cold connections.** With a warmed pgx pool and `GET` responses cached (or served
  from replicas), p99 drops toward p50.

The key architectural win still holds: reads are O(1) index lookups with **no event
replay on the hot path** — that's what CQRS buys you.

### Projection lag: p50 ~210ms, p99 ~306ms — under the 1s target ✓

Comfortably under the 1-second goal in steady state. The lag budget is:

- relay poll interval (200ms) + Kafka publish/consume + one read-model transaction.

The **3.2s max** is the lesson: those samples were created while the projectors were
still draining the 56k-event backlog from phase 1. **Projection lag grows under write
backlog** — the read side is eventually consistent, and "eventually" stretches when
producers outrun consumers. Mitigations: more projector instances/partitions, a
push-based relay (`LISTEN/NOTIFY`) to cut the 200ms poll, and monitoring lag as a
first-class SLO (checkpoint distance behind the log head).

## Saga compensation under failure

The spec also asks to "kill the payment service mid-saga and verify inventory is
released." That path is deterministic and covered by the smoke test and a unit test —
see [05-saga](05-saga.md) and run:

```bash
make smoke   # includes the forced-failure compensation case
```

## Takeaways

- CQRS delivers the fast-read promise (single-digit-ms median reads with no replay).
- Write throughput and read-tail targets need real hardware + the scaling levers
  above; the *architecture* supports them, the laptop doesn't.
- Eventual consistency is real and measurable — watch projection lag, especially under
  load, and treat it as an SLO.
