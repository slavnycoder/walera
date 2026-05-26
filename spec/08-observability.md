# 8. Observability

## 8.1. Prometheus metrics

**WAL / PG:**
- `wal_lsn_received` (gauge): last LSN received from PG.
- `wal_lsn_committed` (gauge): last LSN acked to PG.
- `wal_lsn_lag_bytes` (gauge): `pg_current_wal_lsn()` − `confirmed_flush_lsn`. **Primary in-session pressure metric** — signals the service is falling behind WAL while connected. Not a slot-bloat metric: the slot is temporary ([§1.4](01-data-source-and-wal.md#14-replication-slot)) and is dropped by PG on disconnect, so this metric only matters during a live session.
- `wal_events_total{op}` (counter): one of `insert`, `update`, `delete`, `begin`, `commit`, `relation`.
- `wal_tx_size_rows` (histogram).
- `pg_connection_status` (gauge 0/1).
- `pg_reconnects_total` (counter).

**Subscribers:**
- `subscribers_active{type}` (gauge): `exact`, `wildcard`.
- `subscriber_queue_depth` (histogram): buffer fill across subscribers.
- `subscriber_disconnects_total{reason}` (counter): `client_closed`, `slow_consumer`, `auth_revoked`, `auth_refresh_failed`, `tx_too_large`, `shutdown`.
- `subscriber_connect_duration_seconds` (histogram).
- `subscriber_lifetime_seconds` (histogram).
- `events_sent_total{type}` (counter).
- `events_filtered_total{reason}` (counter): `no_subscribers`, `all_fields_hidden`, `below_start_lsn`.
- `tx_dropped_total{reason}` (counter): `multi_root` — per-subscriber drop for backend discipline violations ([§4.4](04-routing-and-wal-pipeline.md#44-tx-routing-with-root-entity-membership)). Counted once per (tx, subscriber) pair, not per tx.

**Auth:**
- `auth_requests_total{result}` (counter): `ok`, `4xx`, `5xx`, `timeout`.
- `auth_request_duration_seconds` (histogram).
- `auth_circuit_breaker_state` (gauge 0/1).
- `auth_refresh_total{result}` (counter).
- `auth_map_changes_total{type}` (counter): `field_added`, `field_removed`, `table_removed`.
- `auth_breaker_stale_subscribers` (gauge): subscribers whose last successful auth refresh is older than `1.5 × ttl_seconds`. Non-zero is expected during a breaker-open window ([§2.6](02-authorization.md#26-circuit-breaker)); non-zero **outside** a breaker-open window indicates a refresh-logic bug.

**Routing / index:**
- `index_size` (gauge).
- `index_shard_size{shard}` (gauge): for balance check.
- `routing_lookup_duration_seconds` (histogram).
- `routing_fan_out` (histogram): subscribers matched per change.
- `walera_tx_fan_out_work` (histogram): per-transaction sum of post-filter delivered changes across all eligible subscribers (Σ delivered changes per subscriber). Observe-only capacity signal for whole-transaction fan-out work distribution; no hard cap is enforced. Complements `routing_fan_out` (per-change match count) by measuring total delivered-change work at the transaction level.
- `walera_co_tx_beyond_anchor_total` (counter): cumulative changes delivered to subscribers from matched transactions beyond the subscriber's own anchor-matched key(s). Measures the incremental delivery volume added by whole-transaction delivery — changes that would not have been delivered under per-change matching alone. Observe-only; no hard cap enforced.

**Runtime:** standard Go exporters (`go_goroutines`, `go_memstats_*`, `process_open_fds`, etc.).

## 8.2. Structured logs

JSON logs. Mandatory fields: `ts`, `level`, `msg`, `caller`.

Contextual:
- WAL: `lsn`, `op`, `table`.
- Subscriber: `subscriber_id`, `user_id`, `channel`.
- Auth: `subscriber_id`, `user_id`, `http_code`, `duration_ms`.

Levels:
- **INFO:** lifecycle events (start, stop, auth success, SSE open/close). Sample if too noisy.
- **WARN:** slow consumer disconnects, auth retries, large tx skipped, high lag.
- **ERROR:** loss of PG connection, auth failures past retry, recovered panics.

**Never log:** row data (PII risk), tokens, secrets. PK values are OK (they're identifiers, not content).

## 8.3. Health endpoints

- `GET /healthz` — **liveness**. Returns 200 while:
  - the process is running and not shutting down, AND
  - `pg_connection_status == 1`.

  Fails (503) immediately on PG disconnect. Combined with the kubelet probe config (`periodSeconds=2, failureThreshold=3`, see [§10.5](10-resources-and-deployment.md#105-k8s-probes-and-lifecycle)), this bounds the silent-gap window for existing subscribers to ~5 seconds — see [§7.4](07-operational-concerns.md#74-pg-reconnection) for the policy rationale.
- `GET /readyz` — readiness. Returns 200 when: PG replication connected AND `lastCommittedLSN` is advancing (or has advanced within the last N seconds) AND auth backend responds to a ping. Fails immediately on PG disconnect to stop new SSE handshakes landing on a broken pod.
- `GET /metrics` — Prometheus scrape endpoint.

## 8.4. Alerts

- `wal_lsn_lag_bytes > 100 MiB` for 5 min → **P1** (service falling behind WAL during the live session; sustained lag means the router/writers can't keep up with `pgoutput`).
- `pg_connection_status == 0` for 1 min → **P1**.
- `auth_circuit_breaker_state == 1` for 1 min → **P1** (bounded fail-open active; up to ~2min stale-permissions window — see [§2.6](02-authorization.md#26-circuit-breaker)).
- `auth_breaker_stale_subscribers > 0 AND auth_circuit_breaker_state == 0` for 1 min → **P1** (subscribers operating with stale auth maps with no breaker reason — refresh logic is broken).
- `rate(subscriber_disconnects_total{reason="slow_consumer"}[5m]) > 10/min` → **P2**.
- `rate(tx_dropped_total{reason="multi_root"}[5m]) > 0` → **P2** (backend bug: tx touches multiple root rows visible to one subscriber; see [§1.6](01-data-source-and-wal.md#16-entity-model)).
- `process_open_fds / soft_limit > 0.8` → **P2**.
- `go_memstats_heap_inuse_bytes` rising monotonically over 1h → **P2** (suspected leak).
