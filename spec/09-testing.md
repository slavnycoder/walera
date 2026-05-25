# 9. Testing

## 9.1. Unit tests

- **Type mapper:** every PG type → expected JSON. Edge cases: NULL, empty string, max bigint, special chars in text, jsonb with unicode, bytea with null bytes, multidimensional array, naïve vs aware timestamp.
- **Sharded index:** concurrent reads/writes, correctness of removal, no deadlocks. Run with `-race`.
- **AuthMap filter:** whitelist logic, table absence, PK always present, fully-hidden update drop.
- **Backpressure:** non-blocking send semantics, full-buffer kill.
- **start_lsn gate:** txs with `commit_lsn ≤ start_lsn` are not delivered.

Coverage target: > 85% lines. Don't chase 100%.

## 9.2. Integration tests (testcontainers + PG)

Spin up real PostgreSQL in Docker. In `TestMain`, create publication and slot. Mock the auth backend with `httptest.Server`.

Required scenarios:
- **Basic flow:** INSERT/UPDATE/DELETE via regular SQL → correct SSE events.
- **Transactional atomicity:** multi-row tx → one SSE event with multi-element `changes`.
- **Whitelist filtering:** restricted auth map → extra fields stripped.
- **Auth rejection:** mock returns 403 → SSE handshake gets 403.
- **Auth refresh + revoke:** change mock response mid-stream → new map applied / connection closes on revoke.
- **start_lsn cutoff:** UPDATE pre-subscription does NOT arrive; UPDATE post-subscription does.
- **Slow consumer:** don't read the stream → service kills the connection.
- **Restart resume:** UPDATE → restart service → new UPDATE arrives; pre-restart UPDATE does not.
- **Graceful shutdown:** SIGTERM during active subscription → `event: shutdown` received.
- **DDL adaptation:** ADD COLUMN to a table → new field appears in events once auth map includes it.
- **Wildcard:** subscribe to `users:*` → receive events for multiple rows.

All tests run under `-race`. CI must enforce this.

## 9.3. Load testing

Separate milestone, not blocking MVP. Use k6 or a custom harness:
- Open 10k SSE connections.
- Drive UPDATEs via pgbench against a target table.
- Measure p50/p95/p99 latency from PG COMMIT to SSE delivery.

Run before first production deployment and after major refactors.
