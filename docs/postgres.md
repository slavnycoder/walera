# PostgreSQL setup

Walera reads changes through logical replication with the built-in
`pgoutput` plugin. No extension is needed. Supported: PostgreSQL 14 or newer.

## Server settings

```ini
# postgresql.conf — changing wal_level needs a restart
wal_level = logical
max_replication_slots = 10   # Walera uses 1 while running
max_wal_senders = 10         # Walera uses 1 while running
```

The defaults for `max_replication_slots` and `max_wal_senders` (10) are
enough; Walera only requires at least 1 of each and warns
(`low replication slot headroom`) when fewer than 2 slots are free.

Connect Walera to PostgreSQL directly. PgBouncer and other poolers do not
support the replication protocol.

## Replication role

```sql
CREATE ROLE walera LOGIN REPLICATION PASSWORD 'change-me';
```

Walera checks at startup that the connecting role has the `REPLICATION`
attribute or is a superuser (`pg_roles.rolreplication` / `rolsuper`), and
refuses to start otherwise. It never reads your tables directly, so the role
needs no `SELECT` privilege. To let Walera create the publication (see below),
the role must also own the tables and have `CREATE` on the database;
otherwise create the publication yourself.

## Tables

Each streamed table needs:

- a single-column primary key of type `smallint`, `integer`, `bigint`,
  `uuid` or `text`;
- `REPLICA IDENTITY DEFAULT` (the default), so updates and deletes carry
  the primary key. With `FULL` every column is sent as key and with
  `NOTHING` none is; Walera skips such tables and logs
  `relation skipped: PK constraint not met`.

Only tables in the `public` schema can be subscribed to.

## Publication

Walera streams the publication named by `WALERA_PUBLICATION` (default
`walera_pub`). Either create it yourself:

```sql
CREATE PUBLICATION walera_pub FOR TABLE public.items, public.item_tags
  WITH (publish = 'insert, update, delete');
```

or set `WALERA_TABLES=public.items,public.item_tags` and Walera runs exactly
that statement on first start, when the publication does not exist yet.
`TRUNCATE` is never streamed.

Add or remove tables later with `ALTER PUBLICATION walera_pub ADD TABLE …` /
`DROP TABLE …`. Walera does not change an existing publication.

## What Walera checks at startup

In order; any failure stops startup with a message that says how to fix it:

1. `wal_level` is `logical`; `max_replication_slots` and `max_wal_senders`
   are at least 1.
2. The role has `REPLICATION` or is a superuser.
3. The publication exists, or `WALERA_TABLES` is set so Walera can create it.
4. The publication is not `FOR ALL TABLES` and does not include
   `FOR TABLES IN SCHEMA` (PostgreSQL 15+). Both would stream tables nobody
   chose, including ones added later.
5. The publication has at least one table.

If `WALERA_TABLES` is set and an existing publication lists different
tables, Walera logs a warning naming the missing and extra tables and keeps
running with the publication as it is.

## Replication slot

Walera creates a **temporary** slot (`walera_<hostname>_<pid>`) on every
connection. PostgreSQL drops it as soon as the connection closes, so a
stopped Walera never holds back WAL and there is nothing to clean up. The
price: changes committed while Walera is down or reconnecting are never
delivered. Clients reload state on reconnect anyway (see the README).

Walera confirms its position to PostgreSQL every 5 s, also while it waits
for slow subscribers, so `wal_sender_timeout` does not cut it off. The
confirmed position follows the server's WAL end even when no published table
changes (PostgreSQL 15+ sends only keepalives for writes to other tables), so
busy unpublished tables do not make the slot hold back WAL.

## Managed PostgreSQL

- **Amazon RDS / Aurora:** set `rds.logical_replication = 1` in the
  parameter group and reboot. Walera's startup check
  requires `rolreplication` or `rolsuper` in `pg_roles` for the connecting
  role. Membership in `rds_replication` alone does not set either, so a role
  that has only that membership is refused at startup.
- **Google Cloud SQL:** set the `cloudsql.logical_decoding` flag to `on`, then
  `ALTER ROLE walera REPLICATION`.
- Connect to the database host itself, not to a provider's connection pooler.
