-- Event store schema (the WRITE side / source of truth).
--
-- Two tables only: an append-only `events` log and an optional `snapshots` cache.
-- There is deliberately NO "orders" table here - current order state is derived
-- by replaying events, never stored as a mutable row on the write side.

CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,            -- global total order of events
    aggregate_id    UUID        NOT NULL,             -- which entity (an order id)
    aggregate_type  VARCHAR(50) NOT NULL,             -- 'Order', 'Product', ...
    event_type      VARCHAR(100) NOT NULL,            -- 'OrderCreated', 'ItemAdded', ...
    event_data      JSONB       NOT NULL,             -- event-specific payload
    metadata        JSONB,                            -- trace_id, user_id, causation_id
    version         INTEGER     NOT NULL,             -- per-aggregate version
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- The keystone of optimistic concurrency control: two concurrent writers that
    -- both try to append version N to the same aggregate cannot both succeed. The
    -- loser gets a unique-violation, which the app turns into a retry.
    UNIQUE (aggregate_id, version)
);

-- Replay one aggregate in version order (aggregate reconstruction, snapshots).
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events (aggregate_id, version ASC);

-- Scan events by type over time (analytics, targeted rebuilds).
CREATE INDEX IF NOT EXISTS idx_events_type ON events (aggregate_type, created_at);

-- The relay polls new events in global id order; this supports WHERE id > $1.
CREATE INDEX IF NOT EXISTS idx_events_id ON events (id ASC);

CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id    UUID        NOT NULL,
    aggregate_type  VARCHAR(50) NOT NULL,
    version         INTEGER     NOT NULL,             -- state as of this version
    state           JSONB       NOT NULL,             -- materialized aggregate
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aggregate_id, version)
);

-- The relay stores its Kafka-publish checkpoint here: the id of the last event it
-- successfully published. On restart it resumes from this point (at-least-once).
CREATE TABLE IF NOT EXISTS relay_checkpoint (
    id            INTEGER PRIMARY KEY DEFAULT 1,       -- single-row table
    last_event_id BIGINT  NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT relay_checkpoint_singleton CHECK (id = 1)
);
INSERT INTO relay_checkpoint (id, last_event_id) VALUES (1, 0)
    ON CONFLICT (id) DO NOTHING;
