# 08 — Decision Log (ADRs)

Short architecture decision records: **what** was chosen, **why**, and the
**alternatives** rejected. These capture the reasoning behind the code so you can
learn the *thinking*, not just the result.

---

## ADR-1: Go standard library over a web framework

**Decision.** HTTP services use `net/http` with the Go 1.22 `ServeMux` (method+path
patterns), plus a tiny `internal/httpx` helper. No Gin/Echo/Fiber.

**Why.** The routing and error mapping stay visible and dependency-light — ideal for
learning. Go's stdlib mux now covers method-based routing and path params, which was
the historical reason to reach for a framework.

**Rejected.** A framework would add magic (middleware chains, binding) that obscures
what's happening in a teaching repo.

---

## ADR-2: `pgx` directly, no ORM

**Decision.** Use `jackc/pgx/v5` and write SQL by hand.

**Why.** Event sourcing needs precise control over the append (transaction + unique
constraint handling) and over JSONB. An ORM hides exactly the mechanics we want to
teach, and event stores don't map cleanly to ORMs anyway.

---

## ADR-3: The event store is the single source of truth; a relay publishes to Kafka

**Decision.** The command service writes **only** to Postgres. A separate **relay**
polls committed events by global id and republishes them to Kafka, tracking a
checkpoint.

**Why.** Avoids the **dual-write problem** (see [04](04-relay-and-kafka.md)): writing
to Postgres *and* Kafka in one request can't be atomic, so a partial failure
permanently desyncs them. With a relay there is exactly one write; publishing is a
resumable, at-least-once downstream process.

**Rejected.**
- *Publish inside the command handler* — the dual-write bug.
- *Debezium/WAL CDC* — the "correct" production tool, but adds a heavy dependency for
  a local demo. The app-level relay teaches the same concept with zero extra infra.

**Consequence.** At-least-once delivery → every consumer must be idempotent.

---

## ADR-4: Idempotent projectors via a transactional checkpoint

**Decision.** Each projector applies an event and advances its checkpoint in **one
transaction**, skipping events whose id ≤ checkpoint.

**Why.** Turns at-least-once delivery into exactly-once *effect* on the read model. A
redelivered event is applied once or not at all — no double counting. See
[02](02-cqrs-and-projections.md).

**Alternative considered.** Rely solely on Kafka's committed offsets. That commits the
offset *after* the handler but not atomically with the DB write, so a crash between DB
commit and offset commit could reapply. Our own checkpoint closes that gap.

---

## ADR-5: Two Postgres instances, not one with two schemas

**Decision.** `eventstore-db` and `readmodel-db` are separate containers.

**Why.** Makes the CQRS write/read split **physical and obvious** — you can see two
databases. It also mirrors how you'd scale them independently in production
(read replicas for the query side).

**Rejected.** One instance with two schemas (the spec's cheaper option). Fine in
practice, but blurs the separation this project is trying to teach.

---

## ADR-6: Optimistic concurrency via `UNIQUE(aggregate_id, version)`

**Decision.** Detect concurrent writes with a unique constraint + retry loop, rather
than pessimistic locks.

**Why.** Conflicts on a single order are rare, so optimistic wins: no held locks, no
deadlocks, no serialization of unrelated work. See [03](03-optimistic-concurrency.md).

**Rejected.** `SELECT ... FOR UPDATE` — correct but serializes access and holds a lock
across the business logic.

---

## ADR-7: Orchestrated saga (not choreographed), with a fail switch

**Decision.** One `saga-service` orchestrates the place-order flow and calls other
services via HTTP. A `fail_payment` flag forces the failure path.

**Why.** Orchestration keeps the whole flow *and its rollback* readable in one place —
easiest to reason about. The fail switch makes compensation reproducible (the spec's
"kill payment mid-saga" test). See [05](05-saga.md).

**Rejected.** Choreography (services reacting to events) — more decoupled but the flow
becomes implicit and hard to trace.

---

## ADR-8: Money as integer cents

**Decision.** All monetary values are `int64` cents.

**Why.** Floating point can't represent money exactly (`0.1 + 0.2 != 0.3`). Integer
cents avoid rounding bugs. A classic, non-negotiable correctness choice.

---

## ADR-9: Build & test in Docker, no host Go required

**Decision.** The `Makefile` runs `go` inside the `golang:1.23` image; only Docker is
required on the host.

**Why.** Reproducible builds, zero host setup, matches the "everything runs in Docker
Compose locally" goal from the spec.

---

## ADR-10: Idempotent migrations run at service startup

**Decision.** Services run their `CREATE TABLE IF NOT EXISTS ...` migrations on boot
(`internal/db/Migrate`).

**Why.** Simplest thing that works for a demo — no migration tool, no manual step, and
safe to re-run. **Limitation:** no migration history/versioning; a production system
would use a real migrator (golang-migrate, goose) tracking applied versions. Called
out honestly so the shortcut isn't mistaken for a recommendation.
