-- test/integration/testdata/001_publication.sql.test
-- Test fixture: DBA-owned logical replication publication for the Walera
-- integration suite. Extends migrations/001_publication.sql with the table
-- stubs the scenarios need (users, orders, audit_log).
--
-- Conventions (matching the production migration):
--   - REPLICA IDENTITY DEFAULT (no FULL, no NOTHING).
--   - Single-column int/uuid/text PRIMARY KEY only (ENT-02 / D-24).
--   - publish = 'insert, update, delete' (no TRUNCATE).
--
-- audit_log is INTENTIONALLY EXCLUDED from the publication so scenario 03
-- (whitelist-filter) can prove field-level filtering by contrast: a row
-- written to audit_log MUST NOT produce a tx event for any subscriber.

-- Tables used across scenarios 01..04.
CREATE TABLE users (
    id    bigserial PRIMARY KEY,
    email text NOT NULL,
    name  text
);

CREATE TABLE orders (
    id      bigserial PRIMARY KEY,
    user_id bigint,
    total   numeric
);

CREATE TABLE audit_log (
    id     bigserial PRIMARY KEY,
    action text
);

-- Publication: explicit FOR TABLE list (never FOR ALL TABLES).
-- audit_log is omitted on purpose; see header comment.
CREATE PUBLICATION cdc_sse_streamer
    FOR TABLE
        public.users,
        public.orders
    WITH (publish = 'insert, update, delete');
