# 06 — Temporal Queries & Snapshots

Two features that fall out of "state = fold(apply, events)".

## Temporal queries (time travel)

Because current state is just a replay of events, **past** state is a replay of the
events **up to a chosen time**. Reconstruct an order as it was at instant `T` by
folding only the events with `created_at <= T`.

`internal/eventstore/store.go`:

```sql
SELECT ... FROM events WHERE aggregate_id = $1 AND created_at <= $2 ORDER BY version ASC
```

`internal/domain/order/repository.go` → `LoadAt` replays that slice. Exposed at
`GET /orders/{id}/at?t=RFC3339` in the query service.

```bash
QRY=http://localhost:18081
# state right now:
curl -s "$QRY/orders/$ORDER/at?t=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | jq '{status,total,version}'
# state one hour ago (likely "did not exist yet"):
curl -s "$QRY/orders/$ORDER/at?t=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" | jq
```

Why this is a big deal:

- **Debugging**: "what was the order state when payment was attempted?"
- **Compliance/audit**: "show the balance on Dec 31 for the auditor."
- **Analytics**: replay history to compute a metric you didn't think to record.

None of this is possible in a CRUD system that overwrote its rows. Here it's a
`WHERE created_at <= T`.

> Note the temporal endpoint reads the **event store**, not a read model. Read models
> only hold "now"; the log holds all of history.

## Snapshots (making replay fast)

Replaying is cheap for short histories, but an aggregate with thousands of events is
slow to load if you replay from event 1 every time. A **snapshot** is a cached
materialized state at a known version. On load: start from the latest snapshot, then
replay only the events *after* it.

Schema (`migrations/eventstore/001_init.sql`): `snapshots(aggregate_id, version,
state JSONB, ...)`.

Load path (`internal/domain/order/repository.go`):

```go
snap := store.LatestSnapshot(id)
if snap != nil {
    o = decode(snap.state)                       // start from snapshot
    for e in store.LoadAfterVersion(id, snap.version) { o.Apply(e) }  // replay the tail
} else {
    for e in store.Load(id) { o.Apply(e) }       // no snapshot: full replay
}
```

Write path: after appending events, `Save` writes a new snapshot when the version
crosses a multiple of `snapshotEvery` (50).

Key properties:

- **Snapshots are a pure optimization.** They can always be recomputed from events,
  so a failed snapshot write is non-fatal (we log and move on). The log stays the
  source of truth.
- **Temporal queries deliberately ignore snapshots.** A snapshot may be *newer* than
  the requested time `T`, so `LoadAt` always replays from the full history up to `T`.
  Correctness beats speed on the rare time-travel path.

## The tension with schema evolution

A snapshot is serialized aggregate state. If you change the `Order` struct's shape,
old snapshots may not deserialize cleanly. Strategies: version your snapshot format,
or simply discard snapshots on deploy (they rebuild from events). Because they're
disposable, throwing them away is always safe — another payoff of keeping events as
the source of truth.
