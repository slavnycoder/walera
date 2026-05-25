-- migrations/001_publication.sql
-- DBA-owned logical replication publication for Walera CDC.
--
-- IMPORTANT — PK TYPE CONSTRAINT (ENT-02, D-24):
--   All tables in this publication MUST use a single-column int2/int4/int8 or uuid or text
--   PRIMARY KEY. TOAST-unsafe PK types (jsonb, bytea, large text) will cause silent UPDATE
--   loss — see spec/01-data-source-and-wal.md §1.3 and internal/wal/relation.go D-24
--   enforcement. Composite primary keys are also rejected (ENT-02: scalar PK only).
--
-- IMPORTANT — REPLICA IDENTITY:
--   All tables must use REPLICA IDENTITY DEFAULT (the PostgreSQL default when a PK exists).
--   REPLICA IDENTITY FULL doubles/triples WAL volume and is explicitly rejected by spec.
--   REPLICA IDENTITY NOTHING means DELETE messages carry no identifying columns — rejected.
--
-- IMPORTANT — REPLICATION SLOT HEADROOM (OP-06):
--   max_replication_slots must be set per spec §10.4 formula:
--     max_replication_slots >= ceil(N * 1.25) + reserved_slots + 2
--   where N is the number of expected concurrent Walera instances.
--   Walera checks at startup and logs a warning if:
--     (max_replication_slots - count of active slots) < wal.slot_headroom_min (default 2)
--   Exhausted slots cause "no replication slot available" errors and service unavailability.
--
-- IMPORTANT — REPLICATION USER:
--   The user specified in wal.replication_dsn must have the REPLICATION attribute:
--     CREATE ROLE walera_repl WITH REPLICATION LOGIN PASSWORD '...';
--   The user specified in wal.postgres_dsn (admin connection) needs at minimum:
--     GRANT SELECT ON pg_publication_tables TO walera_admin;
--     GRANT pg_monitor TO walera_admin;  -- for pg_replication_slots view
--
-- IMPORTANT — TRUNCATE EXCLUSION:
--   TRUNCATE is excluded from the publication (publish = 'insert, update, delete').
--   TRUNCATE semantics map poorly to per-row CDC: there is no PK to route by.
--   Clients must handle gaps via snapshot-on-reconnect after a TRUNCATE event.

-- POLICY UPDATE (2026-05-18): Walera now supports auto-bootstrapping a
-- FOR ALL TABLES publication when wal.bootstrap.mode=auto (the default).
-- This migration file is therefore OPTIONAL / REFERENCE-ONLY — keep it
-- when you want a curated table list managed by your DBA and run Walera
-- with wal.bootstrap.mode=verify. For greenfield deployments where every
-- table in the database should stream, leave bootstrap.mode at its
-- default and Walera will create the publication on first boot.

-- Example publication — replace with your actual table list.
CREATE PUBLICATION walera_pub
    FOR TABLE
        public.users
        -- , public.orders
        -- , public.products
    WITH (publish = 'insert, update, delete');

-- Grant SELECT on pg_publication_tables so the admin user can verify at startup.
-- (Walera runs: SELECT count(*) FROM pg_publication_tables WHERE pubname = 'walera_pub')
-- GRANT SELECT ON pg_publication_tables TO walera_admin;
