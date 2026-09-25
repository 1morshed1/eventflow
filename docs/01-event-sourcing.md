# 01 — Event Sourcing

## What an event is

An **event** is an immutable statement that *something happened*, in the past tense:
`OrderCreated`, `ItemAdded`, `PaymentReceived`. Not a command ("add item") — a fact
("an item was added"). Events are never updated or deleted.

In code (`internal/eventstore/event.go`):

```go
type Event struct {
    ID            int64           // global sequence (BIGSERIAL) — total order of all events
    AggregateID   uuid.UUID       // which entity (an order)
    AggregateType string          // "Order"
    EventType     string          // "ItemAdded"
    EventData     json.RawMessage // the payload, JSONB
    Metadata      json.RawMessage // trace_id, user_id, ...
    Version       int             // per-aggregate version (1,2,3,...)
    CreatedAt     time.Time
}
```

Two numbers matter:

- **`Version`** is *per aggregate*: order X's events are versions 1,2,3… It powers
  optimistic concurrency ([03](03-optimistic-concurrency.md)).
- **`ID`** is *global*: a single increasing sequence across all aggregates. It gives
  every event one canonical order, which the relay follows when streaming to Kafka.

## The aggregate: state = fold(apply, events)

An **aggregate** is a consistency boundary — here, one `Order`. Its current state is
computed by folding its events through an `Apply` function
(`internal/domain/order/order.go`):

```go
func (o *Order) Apply(e eventstore.Event) {
    switch e.EventType {
    case EventOrderCreated:
        // set id, customer, status=draft
    case EventItemAdded:
        // append item, add to total
    case EventOrderConfirmed:
        o.Status = StatusConfirmed
    // ...
    }
    o.Version = e.Version
}
```

`Apply` has two rules that make the whole system trustworthy:

1. **Deterministic** — the same events always produce the same state.
2. **Side-effect free** — no DB, no network, no clock. It only mutates the struct.

Because of (1) and (2), you can replay events anywhere, anytime, and get an
identical result. That is what makes rebuilds, snapshots, and time travel possible.

### Unknown events are ignored

`Apply`'s `switch` has no `default` that errors. If replay meets an event type it
doesn't recognize, it just advances the version and moves on. This is deliberate: it
means you can **add** new event types without breaking replay of historical
aggregates. Backward compatibility is a first-class concern in event sourcing — see
the test `TestUnknownEventIgnored`.

## Writing events: append, don't update

The write path (`internal/domain/order/service.go`) always looks like:

1. **Load** the aggregate (replay its events).
2. **Decide** — apply business rules to the *current* state. This may reject the
   command (e.g. "order not editable") or produce one or more new events.
3. **Append** the new events with `version = currentVersion + 1, +2, …`.

Note what's absent: no `UPDATE orders SET …`. The only write is an `INSERT` into the
`events` table. State changes are a *consequence* of new events, never a direct edit.

## Why bother? The payoffs

- **Audit trail for free.** The log *is* the history. "Who changed this and when?"
  is answerable by construction.
- **Debugging.** You can replay exactly what happened, in order.
- **New read models anytime.** Want a new report? Write a projector and replay the
  log — no schema migration of existing data.
- **Time travel.** Replaying events up to time `T` reconstructs past state
  ([06](06-temporal-queries-and-snapshots.md)).

## The costs (be honest)

- **Eventual consistency** on the read side ([02](02-cqrs-and-projections.md)).
- **Schema evolution of events** takes discipline — events are forever, so you
  version/extend them rather than repurpose fields.
- **Replaying long histories is slow** — mitigated with snapshots
  ([06](06-temporal-queries-and-snapshots.md)).
- **More moving parts** than a CRUD app.

Use event sourcing where history, audit, and temporal reasoning are genuinely
valuable (orders, payments, ledgers, workflows) — not for every table.

## See it run

```bash
# every event ever, in global order:
docker compose exec eventstore-db psql -U eventflow -d eventstore \
  -c "SELECT id, aggregate_id, event_type, version FROM events ORDER BY id;"
```
