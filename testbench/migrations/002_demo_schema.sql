-- testbench/migrations/002_demo_schema.sql
--
-- Walera v1.1 testbench demo schema. Runs AFTER `001_publication.sql` via
-- PostgreSQL's `/docker-entrypoint-initdb.d` lexical ordering — assumes the
-- empty publication `cdc_sse_streamer` (created in 001) already exists.
--
-- Surface created here:
--   1. pgcrypto extension (for gen_random_uuid() on the devices table).
--   2. Four demo tables — `orders` (int8 PK), `devices` (uuid PK),
--      `articles` (text PK), `line_items` (int8 PK, FK to orders).
--      All four use REPLICA IDENTITY DEFAULT (implicit when a PK exists);
--      no override here — explicit FULL would 2-3x WAL volume (project constraint).
--   3. Per-table autovacuum overrides on the high-churn pair (`orders`,
--      `line_items`) — `autovacuum_vacuum_scale_factor=0.0`,
--      `autovacuum_vacuum_threshold=10000` (SCHEMA-03; Pitfall P6 mitigation).
--   4. Root-bump trigger on `line_items` — PL/pgSQL function that runs
--      `UPDATE orders SET updated_at = now() WHERE id = COALESCE(NEW.orders_id,
--      OLD.orders_id)` in the SAME transaction (SCHEMA-02 — exercises the
--      composite-view root-bump contract Walera relies on).
--   5. `ALTER PUBLICATION cdc_sse_streamer ADD TABLE` enumerating exactly
--      these four tables (SCHEMA-03 explicitly forbids the all-tables form).
--   6. Deterministic seed — stable PKs across `demo-reset` (SCHEMA-04):
--      `orders.id ∈ {1..5}`, fixed UUIDs for devices, stable article slugs.
--
-- NOTE: initdb only runs on an EMPTY data directory. `CREATE TABLE` (not
-- `CREATE TABLE IF NOT EXISTS`) is therefore correct here — a re-run would
-- only occur after `make demo-reset` (which removes the volume).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- orders — root entity #1, int8 PK. High-churn → autovacuum override.
CREATE TABLE orders (
    id            int8        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name text        NOT NULL,
    total_cents   int8        NOT NULL DEFAULT 0,
    status        text        NOT NULL DEFAULT 'pending',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
) WITH (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold    = 10000
);

-- devices — root entity #2, uuid PK. Demo low-churn → no autovacuum override.
CREATE TABLE devices (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name             text        NOT NULL,
    firmware_version text,
    last_seen_at     timestamptz,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb
);

-- articles — root entity #3, text PK. Demo low-churn → no autovacuum override.
CREATE TABLE articles (
    slug         text        PRIMARY KEY,
    title        text        NOT NULL,
    body         text        NOT NULL DEFAULT '',
    published    bool        NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- line_items — child of orders, int8 PK; FK column `orders_id` (NOT part of PK
-- — composite PKs are out of scope per ENT-02). High-churn → autovacuum override.
CREATE TABLE line_items (
    id                int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    orders_id         int8 NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku               text NOT NULL,
    qty               int4 NOT NULL DEFAULT 1,
    unit_price_cents  int8 NOT NULL DEFAULT 0
) WITH (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold    = 10000
);

-- Root-bump trigger per SCHEMA-02 — bumps `orders.updated_at` in the same
-- transaction whenever a `line_items` row changes. This is the Walera v1.0
-- "root-entity routing with backend discipline" composite-view contract.
--
-- Re-parent semantics: when an UPDATE changes `line_items.orders_id`, BOTH
-- the old parent (which lost a line item) and the new parent (which gained
-- one) must be bumped — otherwise the composite-view subscriber on the old
-- parent never sees the membership change. The COALESCE fallback handles
-- the INSERT (NEW only) and DELETE (OLD only) cases.
CREATE OR REPLACE FUNCTION bump_orders_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (TG_OP = 'UPDATE') AND (NEW.orders_id IS DISTINCT FROM OLD.orders_id) THEN
        UPDATE orders
            SET updated_at = now()
            WHERE id IN (NEW.orders_id, OLD.orders_id);
    ELSE
        UPDATE orders
            SET updated_at = now()
            WHERE id = COALESCE(NEW.orders_id, OLD.orders_id);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER line_items_bump_orders
    AFTER INSERT OR UPDATE OR DELETE ON line_items
    FOR EACH ROW
    EXECUTE FUNCTION bump_orders_updated_at();

-- Populate the previously-empty publication with the four demo tables.
-- Explicit table list — SCHEMA-03 forbids the all-tables form. `line_items` MUST
-- be included: Walera decodes its WAL records to surface the root-bump update
-- (the writer in phase 07 inserts line_items; Walera routes the resulting
-- orders update to subscribers of `orders:<id>`).
ALTER PUBLICATION cdc_sse_streamer ADD TABLE orders, devices, articles, line_items;

-- Deterministic seed (SCHEMA-04) — stable PKs across `demo-reset` so the
-- demo UI can always subscribe to e.g. `orders:1` and find a real row.
-- `OVERRIDING SYSTEM VALUE` is REQUIRED because `orders.id` / `line_items.id`
-- are `GENERATED ALWAYS AS IDENTITY` (RESEARCH.md Assumption A3).

INSERT INTO orders (id, customer_name, total_cents, status) OVERRIDING SYSTEM VALUE VALUES
    (1, 'Alice Demo',   10000, 'pending'),
    (2, 'Bob Demo',     20000, 'shipped'),
    (3, 'Eve Demo',     30000, 'delivered'),
    (4, 'Mallory Demo', 40000, 'pending'),
    (5, 'Trent Demo',   50000, 'shipped');

INSERT INTO devices (id, name, firmware_version, metadata) VALUES
    ('11111111-1111-1111-1111-111111111111', 'sensor-north', '1.0.0', '{"location":"north"}'),
    ('22222222-2222-2222-2222-222222222222', 'sensor-south', '1.0.0', '{"location":"south"}'),
    ('33333333-3333-3333-3333-333333333333', 'sensor-east',  '1.0.1', '{"location":"east"}'),
    ('44444444-4444-4444-4444-444444444444', 'sensor-west',  '1.0.1', '{"location":"west"}'),
    ('55555555-5555-5555-5555-555555555555', 'sensor-core',  '1.1.0', '{"location":"core"}');

INSERT INTO articles (slug, title, body, published) VALUES
    ('hello-world',   'Hello, World',       'First post.',   true),
    ('walera-launch', 'Walera v1.0 Launch', 'CDC over SSE.', true),
    ('testbench-v11', 'Testbench v1.1',     'Demo UI.',      true),
    ('roadmap',       'Roadmap',            'Coming next.',  false),
    ('changelog',     'Changelog',          '...',           true);

-- NOTE: each line_items INSERT below fires the root-bump trigger on orders.
-- That's expected — the post-seed `orders.updated_at` will reflect now()+ε
-- rather than the original created_at, which is fine for the demo.
INSERT INTO line_items (id, orders_id, sku, qty, unit_price_cents) OVERRIDING SYSTEM VALUE VALUES
    (1, 1, 'SKU-A', 1, 10000),
    (2, 2, 'SKU-B', 2, 10000),
    (3, 3, 'SKU-C', 3, 10000);

-- Realign IDENTITY sequences past the seeded explicit values so future
-- auto-generated PKs (writer in phase 07) don't collide with seed rows.
SELECT setval(pg_get_serial_sequence('public.orders',     'id'), (SELECT MAX(id) FROM orders));
SELECT setval(pg_get_serial_sequence('public.line_items', 'id'), (SELECT MAX(id) FROM line_items));
