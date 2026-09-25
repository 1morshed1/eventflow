# 02 — CQRS & Projections

## The split

**CQRS** = Command Query Responsibility Segregation. Two sides, two models:

| | Command side (write) | Query side (read) |
|-|----------------------|-------------------|
| Job | enforce rules, append events | answer queries fast |
| Data | the event log (normalized facts) | denormalized views |
| Service | `command-service` | `query-service` |
| Database | event store (Postgres) | read model (Postgres) |
| Shape | just enough to decide | exactly what the screen needs |

They never share tables. The **only** link between them is the event stream: writes
append events → relay → Kafka → projectors → read models → reads.

## Why separate them?

- **Different shapes.** The write model needs the minimum to enforce invariants. The
  read model wants a fat row you can render with no joins. Forcing both into one
  schema compromises both.
- **Independent scaling.** Read traffic usually dwarfs writes. You can run 10 query
  services against read replicas without touching the write path.
- **Multiple views of the same facts.** One event stream feeds many read models,
  each optimized for a different question.

## Projectors: turning events into read models

A **projector** consumes events and writes one or more read-model tables. This repo
has three, all reading the *same* `order-events` stream
(`internal/projection/*.go`):

- **`orders_view`** (`orders.go`) — one denormalized row per order: current status,
  running total, item count, tracking. Powers "my orders" and order detail.
- **`revenue_daily_view`** (`revenue.go`) — reacts *only* to `PaymentReceived`,
  aggregating money per day per customer for the admin dashboard.
- **`customer_order_history_view`** (`history.go`) — reacts to *every* event, turning
  each into a human sentence for the support timeline.

That one stream feeds three very different tables is the whole point of CQRS. Add a
fourth view tomorrow by writing a fourth projector — no change to writers, no data
migration.

```
                       ┌── orders_view projector      → orders_view
order-events (Kafka) ──┼── revenue projector          → revenue_daily_view
                       └── history projector           → customer_order_history_view
```

## Each projector is an independent consumer group

In `cmd/projector/main.go` every projector gets its **own Kafka consumer group**
(named after the projector) and its **own checkpoint row**. Consequences:

- They progress independently. A slow projector never blocks the others.
- Any one can be **rebuilt alone** by replaying from offset 0 — see
  [07-operations](07-operations.md).

## Idempotency: exactly-once *effect* from at-least-once *delivery*

Kafka (and our relay) deliver **at least once** — after a crash you may see an event
twice. If a projector applied a duplicate `PaymentReceived`, revenue would double.

The guard is in `internal/projection/projection.go`. For each event the runner opens
one transaction that does **both** the read-model write **and** the checkpoint
advance:

```go
tx := begin()
checkpoint := SELECT last_event_id FROM projector_checkpoint WHERE projector = $name
if event.ID <= checkpoint { return }          // already applied → skip (idempotent)
proj.Handle(tx, event)                          // update the read model
UPDATE projector_checkpoint SET last_event_id = event.ID
tx.commit()
```

Because the view update and the checkpoint move commit **atomically**, a redelivered
event is either fully applied once or skipped — never applied twice. This is how you
get exactly-once *effects* on top of at-least-once *delivery*.

## Eventual consistency — and how to live with it

After a write returns `202`, the read model isn't updated *yet*. There's a short lag
(write → relay → Kafka → projector → read DB), normally sub-second. Ways clients
cope:

- **Read-your-writes**: return the new id from the command and, if you must show it
  immediately, reconstruct from the write side (the temporal endpoint does exactly
  this).
- **Poll/subscribe**: the UI polls the read model until it reflects the change (the
  smoke test does this).
- **Accept it**: for most e-commerce screens a sub-second lag is invisible.

The `revenue` projector shows a subtle ordering case: it needs the `orders_view` row
(for the customer id) which a *different* projector writes. If it's not there yet, it
returns an error so Kafka redelivers the event later — eventual consistency healing
itself. See the comment in `internal/projection/revenue.go`.

## Try it

```bash
# same events, three different tables:
docker compose exec readmodel-db psql -U eventflow -d readmodel -c "SELECT * FROM orders_view;"
docker compose exec readmodel-db psql -U eventflow -d readmodel -c "SELECT * FROM revenue_daily_view;"
docker compose exec readmodel-db psql -U eventflow -d readmodel -c "SELECT * FROM customer_order_history_view;"
# how far each projector has processed:
docker compose exec readmodel-db psql -U eventflow -d readmodel -c "SELECT * FROM projector_checkpoint;"
```
