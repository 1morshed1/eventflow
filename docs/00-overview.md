# 00 — Overview & Mental Model

Start here. This page gives you the one-paragraph version of every idea in the
system, then points at the deeper docs.

## The one big idea

> **Don't store state. Store the events that produce state.**

A traditional system stores an order as a row and *overwrites* it on every change.
You lose history the moment you `UPDATE`. Event sourcing flips this: the database is
an **append-only log of facts** ("what happened"), and current state is a *derived
view* you compute by replaying those facts.

```
traditional:   orders(id, status='shipped', total=3000)   ← overwritten, history gone

event-sourced: events:
                 1  OrderCreated   {customer}
                 2  ItemAdded      {product, qty, price}
                 3  OrderConfirmed
                 4  PaymentReceived{amount}
                 5  OrderShipped   {tracking}
               state = fold(apply, events)  →  {status:'shipped', total:3000}
```

Because the log is never mutated, you get a perfect audit trail and the ability to
recompute *any* view — including "what did this look like last Tuesday?".

## The second big idea: CQRS

Writes and reads want different data shapes. A write needs just enough state to
enforce rules (is this order still editable?). A read wants a fat, denormalized row
ready to render. **CQRS** (Command Query Responsibility Segregation) splits them:

- **Command side** (write): loads an aggregate, checks rules, appends events.
- **Query side** (read): serves pre-built, denormalized **read models**.

They don't share a schema, or even a database here. They're connected only by the
event stream.

## How a request flows through EventFlow

1. **Command** → `command-service` loads the order (replays its events), applies
   business rules, and appends new events to the **event store** (Postgres).
   *(Nothing else happens synchronously — the write is done.)*
2. The **relay** notices the new events (it polls the log by global id) and
   publishes them to **Kafka**.
3. Each **projector** consumes the Kafka stream and updates its own **read model**
   table (orders view, revenue view, history view).
4. A **query** → `query-service` reads straight from a read model. Fast, no replay.

The gap between step 1 and step 4 is **eventual consistency** — usually well under a
second here. That trade-off is the price of independent scaling; see
[02-cqrs-and-projections](02-cqrs-and-projections.md).

## The supporting cast

- **Optimistic concurrency** keeps two writers from corrupting an aggregate —
  [03](03-optimistic-concurrency.md).
- **The relay** turns the event store into a Kafka stream *without* the dual-write
  bug — [04](04-relay-and-kafka.md).
- **The saga** coordinates a multi-step checkout with rollback — [05](05-saga.md).
- **Snapshots** keep replay fast for long-lived aggregates, and **temporal queries**
  do time travel — [06](06-temporal-queries-and-snapshots.md).

## Where each idea lives in the code

| Idea                    | File |
|-------------------------|------|
| Event value object      | `internal/eventstore/event.go` |
| Append + OCC            | `internal/eventstore/store.go` |
| Aggregate replay (`Apply`) | `internal/domain/order/order.go` |
| Business rules + retry  | `internal/domain/order/service.go` |
| Load w/ snapshot        | `internal/domain/order/repository.go` |
| Relay                   | `cmd/relay/main.go` |
| Projectors              | `internal/projection/*.go` |
| Saga engine             | `internal/saga/saga.go` |

Read the code alongside the docs — every file has a header comment explaining its
role.
