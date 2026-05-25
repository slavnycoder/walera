# 1. Data source and WAL

## 1.1. PostgreSQL logical replication

Source is PostgreSQL 14+. The service connects as a replication client and consumes changes via the `pgoutput` binary protocol.

Use `github.com/jackc/pglogrepl` for the replication protocol. It parses pgoutput natively and provides typed values with OID awareness. Other options (`wal2json`, Debezium+Kafka) are rejected — `wal2json` requires extension installation (unavailable on most managed PG offerings) and Debezium+Kafka adds infrastructure we don't need at this scale.

## 1.2. Publication

The publication is created by DBA via migration with an explicit table list. The service does NOT create or modify publications at runtime.

```sql
CREATE PUBLICATION cdc_sse_streamer FOR TABLE
    public.users,
    public.orders,
    public.documents
WITH (publish = 'insert, update, delete');
```

Key points:
- **No `FOR ALL TABLES`** — risks accidentally streaming audit, migration, or internal tables.
- **TRUNCATE excluded** via `publish = 'insert, update, delete'`. Truncate semantics ("all rows gone") map poorly to per-row CDC.
- Adding a new table requires a migration (publication update) + a service restart. This is intentional — keeps the contract with the auth backend explicit.

All tables in the publication MUST have a PRIMARY KEY. PostgreSQL refuses UPDATE/DELETE replication for tables without one.

## 1.3. REPLICA IDENTITY

Use **DEFAULT** (the default). This means:
- INSERT: full new row in WAL.
- UPDATE: PK + only the columns named in `SET`. No `before` image.
- DELETE: only PK.

FULL is rejected because it doubles or triples WAL volume for wide tables. The client uses the "after" state (or a snapshot) for UI; diffs are not the primary use case.

In the SSE event, UPDATE payloads carry only the modified columns in the unified `data` map (the same field used for INSERT; `op` disambiguates the shape — see [§3.5](03-subscriptions-and-sse.md#35-event-format)). **Absence of a field means "not changed"** (NOT "now null"); `null` value means "now null". These are distinct semantics; document them in client-facing API docs.

## 1.4. Replication slot

The slot is **temporary**: the service creates it on each PG connect via `CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL pgoutput`. PostgreSQL automatically drops it when the replication connection ends (graceful shutdown, crash, network drop). The slot starts at the current WAL LSN — there is no replay of pre-connect WAL.

A configurable slot-name prefix is allowed for log/metric correlation (e.g., `cdc_sse_streamer_<hostname>_<pid>`), but the slot itself never outlives the connection.

**Rationale.** The service uses a "snapshot for state + SSE for deltas" client contract (see [§3.5](03-subscriptions-and-sse.md#35-event-format): `Last-Event-ID` resume is **not** implemented). On any disconnect the client refetches state via the snapshot backend before resubscribing — the snapshot already covers everything that happened during the gap. A persistent slot would therefore read accumulated WAL only to drop it (no subscribers are present yet during catch-up — see [§7.2](07-operational-concerns.md#72-restart-behavior)). The persistent variant is justified only by `Last-Event-ID`-style resume, which we deliberately do not build.

**What a temporary slot trades:**
- ✅ No slot bloat on PG disk if the service is down — slot is gone, WAL retention drops to baseline.
- ✅ No DBA-managed slot lifecycle, no migration to create/drop it.
- ✅ No "stale slot from a previous deployment" failure mode.
- ❌ No cross-restart LSN continuity. Any crash-window events between assembling a tx and the next client snapshot are skipped by the new connection; clients cover them by snapshotting on reconnect.
- ❌ No replay of the duplicate-tx-on-crash case ([§1.5](#15-lsn-acknowledgment)) — but that case stops existing, since the new slot starts from current LSN, not from the last ack.

**Operational note.** `wal_lsn_lag_bytes` is still tracked ([§8.1](08-observability.md#81-prometheus-metrics)) but reframed: it now signals "the service is falling behind WAL **during the current session**", not "slot bloat across downtime". Lag should drop to ~0 within seconds; sustained growth means the router/writers can't keep up and is a real-time pressure signal, not a disk-space risk.

## 1.5. LSN acknowledgment

The service sends `StandbyStatusUpdate` to PG. Timing:

1. Read transaction data from WAL.
2. Wait for the Commit message.
3. Push the assembled transaction event into in-memory subscriber queues (`subscriber.ch <- event`).
4. Update `lastCommittedLSN` (atomic).

A separate ticker goroutine reads `lastCommittedLSN` atomically every 5 seconds and sends the standby status update. **The ack does NOT wait for client TCP delivery.** SSE is best-effort; clients reconnect and resync via snapshot if they miss events.

The `StandbyStatusUpdate` exists for one purpose only: while the connection is alive, it advances `confirmed_flush_lsn` so PG can recycle WAL files behind us. **It does NOT provide cross-restart resume** — the slot is temporary ([§1.4](#14-replication-slot)) and is dropped by PG when the connection ends. On the next start, a fresh slot begins at the current WAL LSN; nothing is replayed. Any in-flight transaction at the moment of crash is therefore lost from the WAL stream, and clients cover the gap by snapshotting on reconnect. There is no duplicate-tx-after-crash scenario to defend against, so clients are not required to be idempotent for replay (idempotence is still a good general property).

## 1.6. Entity model

`entity_name` in `entity_name:id` is the **root table name** of a logical entity (no schema prefix in the public-facing channel). A logical entity may span multiple PG tables: a **root table** identifies the entity, and **child tables** hold related rows attached to a root via FK (e.g., `todo_list` root with `tasks` children; `wallet` root with `transactions` children).

Only single-column scalar PKs are supported (`int2/4/8`, `uuid`, `text`, etc.) for root identification. Composite keys are not supported.

In the subscription index, keys are stored as strings: `"{schema}.{root_table}:{pk_as_text}"`, e.g., `"public.todo_list:42"`.

### Root vs child: declared by auth backend

A table's role (root or child) is **per-user**, declared by the auth backend in the `roots` field of the auth response ([§2.3](02-authorization.md#23-response-contract)). `tables` lists all visible tables with their whitelists; `roots` is the subset on which this user may subscribe. The same table may be root for one user and child for another, depending on the view shape the auth backend exposes.

Subscription handshake validates that the channel's table is in the user's `roots` ([§3.2](03-subscriptions-and-sse.md#32-subscription-handshake-handler-logic)) — otherwise 403.

### Backend discipline: bump root in the same transaction

For composite views, the backend MUST ensure that any transaction modifying a child row also touches the corresponding root row in the same tx (typical pattern: `UPDATE todo_list SET updated_at = now() WHERE id = $task.todo_list_id`). This makes the root row the routing key for the whole tx — see [§4.4](04-routing-and-wal-pipeline.md#44-tx-routing-with-root-entity-membership).

**At most one root row per transaction.** A tx that matches **two or more root rows** under a subscriber's `roots` and channel is dropped for that subscriber with a logged error and `tx_dropped_total{reason="multi_root"}` increment. A tx that touches **zero** root rows from a subscriber's perspective is silently delivered to no one — this is the normal case for txs whose only changes are on tables nobody subscribes to via that path.

Cross-entity bulk operations (e.g., reorder all todo_lists at once) MUST be split into one tx per root entity. Bundling them is a backend bug and causes silent loss for wildcard subscribers on the affected root table.

## 1.7. Schema changes (Relation messages and DDL)

`pgoutput` sends Relation messages describing each table's columns and OIDs. They arrive before the first change for a table and after any schema change.

Maintain an in-memory `map[OID]RelationInfo`. Update on every Relation message. Use it for typed value decoding.

DDL is **not** transmitted directly. Behavior on schema changes:
- **ADD COLUMN:** appears in subsequent Relation messages. Not forwarded to clients until the auth backend lists it in the field whitelist.
- **DROP COLUMN:** disappears from Relation messages. Auto-filtered.
- **RENAME COLUMN:** appears as drop-then-add at the pgoutput level.
- **DROP TABLE:** events stop arriving. Subsequent subscriptions return 403 from auth.

Do not implement active DDL handlers; the schema-of-record is PG itself plus the auth map.
