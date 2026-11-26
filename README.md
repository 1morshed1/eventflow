# EventFlow — Event-Sourced E-Commerce with CQRS

[![CI](https://github.com/1morshed1/eventflow/actions/workflows/ci.yml/badge.svg)](https://github.com/1morshed1/eventflow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

EventFlow is a small but complete e-commerce backend built on **event sourcing**
and **CQRS**. Instead of storing an order as a mutable row, it stores every event
that ever happened to it (`OrderCreated`, `ItemAdded`, `PaymentReceived`, …) and
derives current state by replaying those events. Reads are served from separate,
denormalized **read models** kept up to date by projectors consuming a Kafka stream.

The code is heavily commented, and the [`docs/`](docs/) folder explains every pattern and every design decision.

---

## Why this project matters

Event sourcing + CQRS give you:

- **A complete audit trail** — every change is an immutable, replayable fact.
- **Time travel** — reconstruct any entity's state at any past instant.
- **Independent read/write scaling** — the write model and each read model evolve
  and scale separately.
- **Rebuildable read models** — found a bug in a view? Fix the projector and replay
  the log. No data migration.

Most engineers have heard of these patterns; far fewer have built one. This is a
working implementation you can run, break, and reason about.

---

## Architecture

```
 client ──commands──▶ command-service ──append──▶ [event store: Postgres]
                                                         │
                                                    (source of truth)
                                                         │
                                              relay polls events by global id
                                                         │
                                                         ▼
                                                   [ Kafka: order-events ]
                                                         │
                         ┌───────────────────────────────┼───────────────────────────┐
                         ▼                               ▼                             ▼
                  orders_view proj.           revenue_daily_view proj.        history proj.
                         │                               │                             │
                         └───────────────┬───────────────┴─────────────┬───────────────┘
                                         ▼                             ▼
                                  [ read model: Postgres ] ◀──reads── query-service ──▶ client

 saga-service ── orchestrates place-order (reserve → pay → confirm) with compensation
```

Five Go services, each with one job:

| Service           | Role                                                          | Port (host) |
|-------------------|--------------------------------------------------------------|-------------|
| `command-service` | Write side: validates commands, appends events               | 18080       |
| `relay`           | Publishes committed events to Kafka (app-level CDC)           | —           |
| `projector`       | Builds read models from the Kafka stream                      | —           |
| `query-service`   | Read side: serves denormalized views + temporal queries      | 18081       |
| `saga`            | Orchestrates place-order with compensating actions           | 18082       |

Infrastructure: two Postgres instances (event store + read model) and single-node
Kafka (KRaft, no ZooKeeper). Everything runs locally in Docker Compose.

---

## Quickstart

Prerequisites: **Docker** (with Compose) and **jq** (for the smoke test). You do
**not** need Go installed — all builds and tests run inside a `golang` container.

```bash
make up            # build images and start the whole stack
make smoke         # run the end-to-end smoke test (needs the stack up + jq)
make loadtest      # throughput / read-latency / projection-lag benchmark
make observability # Prometheus + Grafana (Grafana at http://localhost:13000)
make logs          # tail logs from all services
make down          # stop everything and wipe volumes
```

Develop without a local Go toolchain:

```bash
make test      # unit tests inside the golang container
make build     # compile everything
make vet       # go vet
```

---

## Try it by hand

```bash
CMD=http://localhost:18080
QRY=http://localhost:18081
SAGA=http://localhost:18082
CUST=$(cat /proc/sys/kernel/random/uuid)

# --- write side ---
ORDER=$(curl -s -X POST $CMD/orders -d "{\"customer_id\":\"$CUST\"}" | jq -r .id)
curl -s -X POST $CMD/orders/$ORDER/items -d '{"product_id":"'$(uuidgen)'","quantity":2,"price":1500}'
curl -s -X POST $CMD/orders/$ORDER/confirm

# --- saga: place the order (reserve → pay → mark paid) ---
curl -s -X POST $SAGA/sagas/place-order -d "{\"order_id\":\"$ORDER\",\"amount\":3000}" | jq

# --- read side (eventually consistent, usually < 1s) ---
curl -s $QRY/orders/$ORDER | jq
curl -s $QRY/orders/$ORDER/history | jq
curl -s "$QRY/orders/$ORDER/at?t=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | jq   # time travel
curl -s $QRY/admin/revenue | jq
```

### API summary

**Command service (writes)** — returns `202 Accepted`; the effect is an appended event.

| Method | Path                                   | Body                                        |
|--------|----------------------------------------|---------------------------------------------|
| POST   | `/orders`                              | `{customer_id}`                             |
| POST   | `/orders/{id}/items`                   | `{product_id, quantity, price}` (cents)     |
| DELETE | `/orders/{id}/items/{productID}`       | —                                           |
| POST   | `/orders/{id}/confirm`                 | —                                           |
| POST   | `/orders/{id}/pay`                     | `{amount, payment_ref}`                     |
| POST   | `/orders/{id}/ship`                    | `{tracking_number}`                         |
| POST   | `/orders/{id}/cancel`                  | `{reason}`                                  |

**Query service (reads)**

| Method | Path                          | Returns                                  |
|--------|-------------------------------|------------------------------------------|
| GET    | `/orders/{id}`                | current order from `orders_view`         |
| GET    | `/orders/{id}/history`        | support timeline                         |
| GET    | `/orders/{id}/at?t=RFC3339`   | **temporal** state (replayed from log)   |
| GET    | `/customers/{id}/orders`      | a customer's orders                      |
| GET    | `/admin/revenue?day=YYYY-MM-DD` | daily revenue                          |

**Saga service**

| Method | Path                    | Body                                   |
|--------|-------------------------|----------------------------------------|
| POST   | `/sagas/place-order`    | `{order_id, amount, fail_payment?}`    |

Set `fail_payment: true` to watch compensation run.

---

## Learn the internals

Read the docs in order:

1. [Overview & mental model](docs/00-overview.md)
2. [Event sourcing](docs/01-event-sourcing.md)
3. [CQRS & projections](docs/02-cqrs-and-projections.md)
4. [Optimistic concurrency](docs/03-optimistic-concurrency.md)
5. [The relay & Kafka](docs/04-relay-and-kafka.md)
6. [The saga pattern](docs/05-saga.md)
7. [Temporal queries & snapshots](docs/06-temporal-queries-and-snapshots.md)
8. [Operations: rebuilds & debugging](docs/07-operations.md)
9. [Decision log (ADRs)](docs/08-decisions.md)
10. [Load testing](docs/09-load-testing.md)
11. [Observability](docs/10-observability.md)

---

## Repository layout

```
cmd/                 one main.go per service
  command-service/   write API
  query-service/     read API
  relay/             event store -> Kafka
  projector/         Kafka -> read models
  saga/              place-order orchestrator
internal/
  eventstore/        the append-only event store + snapshots
  domain/order/      the Order aggregate, events, rules, repository
  projection/        projectors + read-model idempotency
  kafka/             thin Kafka publisher/consumer wrapper
  saga/              generic saga engine + order saga
  db/, httpx/, config/  small shared helpers
migrations/          SQL for the event store and the read model
scripts/smoke.sh     end-to-end test
docs/                the learning material
```

## License

[MIT](LICENSE)
