# 07 — Operations: Rebuilds & Debugging

Practical recipes for running, inspecting, and rebuilding EventFlow.

## Run the stack

```bash
make up      # build + start everything
make ps  ||  docker compose ps           # service status
make logs    # tail all logs
make smoke   # end-to-end test (needs jq)
make down    # stop + wipe volumes (fresh start next time)
```

Host ports: command `18080`, query `18081`, saga `18082`, event-store Postgres
`55432`, read-model Postgres `55433`, Kafka `9092`. (The DB/command ports were moved
off the defaults because `5432`/`8080` are commonly taken locally.)

## Inspect the event store (write side)

```bash
docker compose exec eventstore-db psql -U eventflow -d eventstore

-- the whole log, global order:
SELECT id, aggregate_id, event_type, version, created_at FROM events ORDER BY id;
-- one aggregate's history:
SELECT version, event_type, event_data FROM events WHERE aggregate_id = '<uuid>' ORDER BY version;
-- snapshots:
SELECT aggregate_id, version FROM snapshots;
-- relay progress:
SELECT * FROM relay_checkpoint;
```

## Inspect the read models (query side)

```bash
docker compose exec readmodel-db psql -U eventflow -d readmodel

SELECT * FROM orders_view;
SELECT * FROM revenue_daily_view;
SELECT * FROM customer_order_history_view ORDER BY order_id, id;
SELECT * FROM projector_checkpoint;     -- how far each projector has processed
```

## Inspect Kafka

```bash
# topics:
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# messages:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order-events --from-beginning --max-messages 10
# consumer group health — ASSIGNMENT must be non-empty (see 04 for the gotcha):
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders_view
```

## The killer feature: rebuild a read model from scratch

This is what makes event sourcing operationally special. Suppose you find a bug in
how `revenue_daily_view` was computed. In a CRUD system you'd write a risky data
migration. Here you **fix the projector and replay the log**.

Mechanism: a read model is rebuilt by a **fresh consumer group** starting at offset 0
(Kafka retains the whole log) plus a **fresh checkpoint**. `cmd/projector/main.go`
supports a `REBUILD` suffix that makes new group names/checkpoints:

```bash
# 1. fix the projector code, rebuild the image
docker compose build projector

# 2. clear the target table + its checkpoint (do this for the view you're rebuilding)
docker compose exec readmodel-db psql -U eventflow -d readmodel -c \
  "TRUNCATE revenue_daily_view; DELETE FROM projector_checkpoint WHERE projector LIKE 'revenue_daily_view%';"

# 3. start projectors with a new consumer-group suffix → they replay from offset 0
REBUILD=v2 docker compose up -d projector

# 4. once caught up, the view is correct. (In prod you'd swap reads over to it.)
```

No downtime for the write side, no data migration, no history lost. Because the log is
immutable and complete, the rebuilt view is *derivable* — you just recompute it.

## Common issues

- **Read model empty after writes** — check consumer-group ASSIGNMENT is non-empty
  (the startup race from [04](04-relay-and-kafka.md)); check `relay_checkpoint` is
  advancing; check projector logs for handler errors.
- **Revenue row missing but order exists** — the revenue projector needs the
  `orders_view` row first; it errors and Kafka redelivers until that row lands
  (eventual consistency, self-healing).
- **`503` from a command** — optimistic-concurrency retries exhausted; the client may
  retry ([03](03-optimistic-concurrency.md)).

## Build/test without Go installed

Everything runs in the `golang:1.23` container via the `Makefile`:

```bash
make test    # go test ./...
make build   # go build ./...
make vet     # go vet ./...
make tidy    # go mod tidy
```

The `.gocache/` and `.gomodcache/` bind mounts (gitignored) cache dependencies so
repeat runs are fast.
