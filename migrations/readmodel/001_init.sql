-- Read model schema (the QUERY side).
--
-- These tables are DENORMALIZED and DISPOSABLE. Every one of them can be dropped
-- and rebuilt from scratch by replaying the event log through the projectors. They
-- are shaped for how clients read, not for how the domain writes. Never write to
-- these tables from the command side - only projectors own them.

-- "My orders" list + order detail. One row per order, kept up to date by the
-- order projector as it consumes events.
CREATE TABLE IF NOT EXISTS orders_view (
    id           UUID PRIMARY KEY,
    customer_id  UUID        NOT NULL,
    status       VARCHAR(20) NOT NULL,
    total        BIGINT      NOT NULL DEFAULT 0,   -- cents
    item_count   INTEGER     NOT NULL DEFAULT 0,
    tracking_number TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shipped_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_orders_view_customer ON orders_view (customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_view_status ON orders_view (status);

-- Per-day, per-customer revenue for the admin dashboard. Aggregated at projection
-- time so the dashboard query is a trivial, fast SELECT.
CREATE TABLE IF NOT EXISTS revenue_daily_view (
    day         DATE NOT NULL,
    customer_id UUID NOT NULL,
    revenue     BIGINT NOT NULL DEFAULT 0,   -- cents, from PaymentReceived events
    order_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, customer_id)
);
CREATE INDEX IF NOT EXISTS idx_revenue_day ON revenue_daily_view (day);

-- Customer support timeline: one row per event, human-readable. Shows the full
-- history of an order without touching the raw event store.
CREATE TABLE IF NOT EXISTS customer_order_history_view (
    id           BIGSERIAL PRIMARY KEY,
    order_id     UUID        NOT NULL,
    customer_id  UUID        NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    description  TEXT        NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_order ON customer_order_history_view (order_id, occurred_at);

-- Projector checkpoints: each Kafka consumer group also records the last global
-- event id it processed, giving us idempotency (skip already-applied events) on
-- top of Kafka's own offset tracking.
CREATE TABLE IF NOT EXISTS projector_checkpoint (
    projector     VARCHAR(50) PRIMARY KEY,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
