# 03 — Optimistic Concurrency Control (OCC)

## The problem

Two requests hit the same order at the same time. Both load it in `draft` with 49
items. Both check "fewer than 50? yes" and both append an `ItemAdded`. Now the order
has 51 items — an invariant was violated because each writer decided based on stale
state.

We need to stop two writers from *both* appending the "next" event to the same
aggregate. Two broad options:

- **Pessimistic** locking: `SELECT … FOR UPDATE`, hold a lock while you decide.
  Simple, but serializes access and holds locks across your business logic.
- **Optimistic** concurrency: don't lock. Assume you'll win, and *detect* a conflict
  at write time. Retry on the rare collision. Great when conflicts are uncommon —
  which they are for a single order.

EventFlow uses optimistic concurrency.

## The mechanism: one unique constraint

Every event carries a per-aggregate `version` (1, 2, 3, …). The event store table
has (`migrations/eventstore/001_init.sql`):

```sql
UNIQUE (aggregate_id, version)
```

That's the entire enforcement. If two writers both try to append **version N** to the
same aggregate, the database lets exactly one succeed; the other's `INSERT` fails
with a unique-violation. In `internal/eventstore/store.go` we translate that specific
Postgres error (`SQLSTATE 23505`) into a typed `ErrVersionConflict`:

```go
if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
    return ErrVersionConflict
}
```

## The retry loop

The command service turns a conflict into a retry with fresh state
(`internal/domain/order/service.go`, `execute`):

```go
for attempt := 0; attempt < maxRetries; attempt++ {
    o := repo.Load(id)                 // 1. fresh state (version = N)
    events := decide(o)                // 2. apply rules → new events at version N+1
    err := repo.Save(o, o.Version, events)
    if errors.Is(err, ErrVersionConflict) {
        continue                       // 3. someone else took N+1 → reload & retry
    }
    return err
}
return ErrConcurrencyConflict          // gave up after maxRetries
```

Walk through the race:

```
writer A: load v=49 ─┐
writer B: load v=49 ─┘
A: append v=50  ✓ (commits first)
B: append v=50  ✗ ErrVersionConflict  → reload (now v=50) → re-check rules → append v=51 ✓
```

The invariant holds because B **re-decides** against the state A produced. If the
order had hit 50 items, B's retry would now correctly reject with `ErrTooManyItems`.

## Why the decision must live *inside* the loop

The retry re-runs the business rules, not just the append. That's essential: a
conflict means the world changed, so the previous decision may no longer be valid.
This is why `service.go` passes a `decision` function into `execute` rather than
computing events once up front.

## HTTP surface

If all retries lose (heavy contention on one aggregate), the command service returns
`503 Service Unavailable` (`ErrConcurrencyConflict`) — a *transient* signal telling
the client it's safe to retry. Business-rule rejections instead return `409 Conflict`
(permanent for that state). See `httpStatus` in `cmd/command-service/main.go`.

## Trade-offs

- **Great** when writes to a single aggregate are rarely simultaneous (typical).
- **Poor** for a hotspot everyone writes at once — you'd thrash on retries. Then
  reshape the aggregate or serialize deliberately.
- **No held locks**, so no lock-ordering deadlocks and no long-held transactions.

## Prove it to yourself

Fire two concurrent adds at a fresh order and watch versions stay gap-free and
correct:

```bash
CMD=http://localhost:18080
ORDER=$(curl -s -X POST $CMD/orders -d '{"customer_id":"'$(uuidgen)'"}' | jq -r .id)
for i in $(seq 1 10); do
  curl -s -X POST $CMD/orders/$ORDER/items \
    -d '{"product_id":"'$(uuidgen)'","quantity":1,"price":100}' &
done; wait
docker compose exec eventstore-db psql -U eventflow -d eventstore \
  -c "SELECT version, event_type FROM events WHERE aggregate_id='$ORDER' ORDER BY version;"
```

Versions are contiguous (1..N) with no duplicates — the unique constraint guaranteed
it.
