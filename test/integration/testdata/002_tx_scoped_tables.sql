-- test/integration/testdata/002_tx_scoped_tables.sql
-- Non-destructive second init script: adds todo_lists + tasks tables used by
-- Test16TxScopedDelivery (plan 01-04) to the existing cdc_sse_streamer publication.
--
-- Conventions (matching 001_publication.sql):
--   - REPLICA IDENTITY DEFAULT (no FULL, no NOTHING).
--   - Single-column bigserial PRIMARY KEY only (ENT-02 / D-24).
--   - Bare ALTER PUBLICATION ... ADD TABLE (no WITH clause on ADD TABLE in PG18+).
--
-- 001_publication.sql is NOT modified; cdc_sse_streamer already exists for
-- users/orders when this script runs.

CREATE TABLE todo_lists (
    id    bigserial PRIMARY KEY,
    title text NOT NULL
);

CREATE TABLE tasks (
    id           bigserial PRIMARY KEY,
    todo_list_id bigint,
    title        text NOT NULL
);

ALTER PUBLICATION cdc_sse_streamer ADD TABLE public.todo_lists, public.tasks;
