# 04 — The Relay & Kafka

## The dual-write problem (why the relay exists)

The naive design has the command service do two writes in one request:

```
1. INSERT event into Postgres     ✓
2. publish event to Kafka         ✗  ← what if this fails? or the process crashes here?
```

There is **no way** to make two separate systems commit atomically without
distributed transactions (which are slow and operationally painful). If step 1
succeeds and step 2 fails, Postgres and Kafka now disagree forever. This is the
**dual-write problem**, and it silently corrupts event-sourced systems.

## The fix: the log *is* the outbox

EventFlow's command service writes to **exactly one** system — the event store. That
is the single source of truth. A separate **relay** process then reads committed
events and republishes them to Kafka. There is no dual write anywhere: the write is
one Postgres transaction, and publishing is a *downstream, resumable* activity.

This is the same idea as the Transactional Outbox / log-based CDC (Change Data
Capture) pattern. Debezium would do this by tailing the Postgres WAL; here we do the
app-level version by polling the `events` table, which keeps the demo dependency-free.

## How the relay works

`cmd/relay/main.go`, function `pumpOnce`:

```go
last     := store.RelayCheckpoint()          // 1. where did we stop?
events   := store.LoadAllAfter(last, batch)  // 2. next batch in GLOBAL id order
publish(events)                              // 3. send to Kafka
store.SetRelayCheckpoint(events.last.ID)     // 4. remember progress
```

The **order of steps 3 and 4 is the whole correctness argument**:

- Publish *then* checkpoint. If we crash between 3 and 4, on restart we re-read from
  the old checkpoint and **re-publish** those events. That's **at-least-once**
  delivery — an event is never lost, though it may be duplicated.
- We never checkpoint before publishing, which would risk *losing* events (the
  dangerous direction).

Duplicates are fine because every consumer downstream is idempotent
([02](02-cqrs-and-projections.md)).

The checkpoint is a single row in `relay_checkpoint` (`migrations/eventstore/001_init.sql`).
Reading events in `id` order (the global sequence) guarantees the relay streams the
whole system's events in one canonical order.

> **Production note.** Polling every 200ms is simple and fine for a demo. A
> production relay would use Postgres `LISTEN/NOTIFY` (or logical replication /
> Debezium) to wake instantly instead of polling. The correctness model is identical.

## Ordering & partitioning in Kafka

`internal/kafka/kafka.go`:

- Messages are keyed by **aggregate id** with a `Hash` balancer. Kafka guarantees
  ordering **within a partition**, and same key → same partition, so all events for
  one order arrive **in order**. Events for *different* orders may interleave — which
  is fine, they're independent.
- `RequireAll` acks: the producer waits for all in-sync replicas before considering a
  write durable.
- Topic routing by aggregate type: `Order` → `order-events` (payment/inventory
  topics are reserved for future aggregates).

## The startup race we hit (and fixed)

A real bug from building this, worth learning from:

**Symptom.** Writes worked, the relay published, Kafka had the messages — but the
read models stayed empty. `kafka-consumer-groups --describe` showed each projector as
a group member with **0 partitions assigned**.

**Cause.** The projectors started and joined their consumer groups *before* the topic
existed (the relay auto-created it ~11s later, on first publish). A consumer that
joins a non-existent topic is assigned zero partitions, and — with partition
watching off — never notices when the topic later appears. It sits idle forever.

**Fix** (`internal/kafka/kafka.go`, `cmd/projector/main.go`):

1. `EnsureTopics` explicitly creates the topic at projector startup, before any
   consumer joins.
2. The reader sets `WatchPartitionChanges: true` with a short
   `PartitionWatchInterval`, so even if partitions change later it rebalances and
   picks them up.

Lesson: **consumer-group assignment is decided at join time.** Don't rely on lazy
auto-creation for topics your consumers depend on.

## Rebuilding from the stream

Because Kafka retains the log, a brand-new consumer group starting at
`FirstOffset` replays the entire history. That's the mechanism behind read-model
rebuilds — see [07-operations](07-operations.md).

## Inspect it

```bash
# messages on the topic:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order-events --from-beginning --max-messages 5

# consumer group health (assignment should be non-empty):
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders_view

# relay progress:
docker compose exec eventstore-db psql -U eventflow -d eventstore -c "SELECT * FROM relay_checkpoint;"
```
