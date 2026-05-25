# 3. Subscriptions and SSE

## 3.1. Connection model

**One SSE connection = one channel.** Clients open separate `EventSource` instances for each subscription. On HTTP/2 these multiplex into one TCP connection; on HTTP/1.1 they share the per-host limit (~6).

URL scheme:
- Exact: `GET /sse/v1/{table}/{pk}`
- Wildcard: `GET /sse/v1/{table}/all` (or whatever distinct path is chosen; do not use `*` in URL — some proxies mangle it)

Versioned via URL (`/v1/`). For incompatible changes, introduce `/v2/`.

## 3.2. Subscription handshake (handler logic)

```
1. Parse URL → (table, pk_or_wildcard). Validate format.
2. Global concurrency check. Reject with 503 + Retry-After if exceeded.
3. Per-user rate limit (token bucket, keyed by user_id once known; for the very first check, key by IP or a coarser bucket).
4. Auth backend call. Translate response → authMap or HTTP error.
5. Per-user concurrent-connections check. Reject 429 if exceeded.
6. Verify the channel's table is listed in `authMap.roots`. If absent from `tables` → 403 (`reason: "not_allowed"`); if present in `tables` but not in `roots` → 403 (`reason: "not_a_root"`). Channels target root entities only — see [§1.6](01-data-source-and-wal.md#16-entity-model).
7. Build subscriber{
       ch: make(chan Event, bufferSize),
       ctx: context.WithCancel(r.Context()),
       authMap: atomic.Pointer[AuthMap]{stores fresh map},
       start_lsn: atomic.Load(&lastCommittedLSN),
       user_id, channel info, ...
   }.
8. Write SSE response headers:
       Status: 200 OK
       Content-Type: text/event-stream
       Cache-Control: no-cache
       Connection: keep-alive
       X-Accel-Buffering: no   (disables nginx response buffering)
       <CORS headers>
   From this point, errors must be conveyed as SSE events, not HTTP status.
9. Register subscriber in the sharded index under the shard lock.
10. Start goroutines: writer loop, heartbeat ticker, auth refresh ticker.
11. Block in writer loop until ctx.Done() or write error.
12. Cleanup: deregister from index (shard lock), decrement per-user counter, stop tickers. Do NOT close sub.ch explicitly.
```

## 3.3. Starting from the next COMMIT

`start_lsn` is captured **after** auth succeeds, **before** index registration. The router filters with `if tx.commit_lsn > sub.start_lsn { deliver }`.

This guarantees:
- No partial transaction is delivered to a new subscriber.
- No transactions from "the past" leak in (which would conflict with a fresh snapshot).

**Optional `?since_lsn=X`:** if the client knows the LSN of its snapshot (obtained from the snapshot backend), it can pass this to override `start_lsn`. If not provided, defaults to the current `lastCommittedLSN`.

## 3.4. Wildcard subscriptions

**Intended use: publicly-enumerable entities only.** A wildcard channel (`currency_rates:*`, `public_catalog:*`, `status_feed:*`) inherently discloses the set of primary keys in the table to every approved subscriber — see [§2.7](02-authorization.md#27-field-map-semantics). This is acceptable for entities where PK enumeration is part of the public contract, and **not** acceptable for tables containing PII or otherwise sensitive identifiers (e.g., `users:*` on a real user table). The auth backend is the sole gatekeeper of this distinction: approving an `entity:*` channel is an explicit decision that the table's PK space is safe to enumerate. The service does not enforce a separate policy — it relies on this trust contract.

Channel format `entity:*` (or whatever URL form chosen). Authorization is uniform — the auth backend receives the channel string and decides.

Two separate indexes:
- **Exact index:** sharded hash map keyed by `schema.table:pk` ([§4.2](04-routing-and-wal-pipeline.md#42-subscription-index-exact-subscriptions)).
- **Wildcard index:** single map keyed by `schema.table`, guarded by a single RWMutex.

On each WAL change, the router does both lookups and unions the subscriber sets.

Wildcard-specific tuning:
- Buffer size: 512 events (vs. 64 for exact).
- Max changes per tx: 10,000 (vs. 1,000 for exact).
- Disconnect threshold: same (buffer full).
- Separate metrics: `subscribers_active{type="wildcard"}`, `events_sent_total{type="wildcard"}`.

## 3.5. Event format

```
event: tx
id: 12345
data: {"tx_id":12345,"commit_ts":"2026-05-14T10:23:45.123Z","changes":[...]}

```

(Note the trailing blank line — SSE event terminator.)

Each `change` is one of:

```json
{"op":"insert","table":"users","pk":"42","data":{"id":42,"name":"Alice","email":"a@x.com"}}
{"op":"update","table":"users","pk":"42","data":{"name":"Alicia"}}
{"op":"delete","table":"users","pk":"42"}
```

Rules:
- A single `data` map carries the row payload; `op` disambiguates the shape:
  - **insert** — `data` is the full new row, filtered by whitelist (PK always included).
  - **update** — `data` contains **only** the modified columns, filtered by whitelist (PK always included). Absence of a field means "not changed"; presence with `null` means "now NULL" — these are distinct.
  - **delete** — `data` is absent (matches REPLICA IDENTITY DEFAULT; PK is the sole identifier).
- `table` is the bare table name (no schema). If multi-schema support is added later, this becomes `schema.table`.

The SSE `id:` field is set to `{tx_id}` for tracing. `Last-Event-ID` resume is NOT implemented — on reconnect, the client takes a fresh snapshot. The Postgres commit LSN is deliberately not exposed on the wire: it is a physical WAL offset that leaks an internal Postgres detail and carries no client-visible semantics. LSN remains available internally for routing, auth-refresh ordering, logs, and metrics.

Other event types:
- `event: error` — sent before disconnect when possible: `data: {"reason": "...", "details": "..."}`. Reasons: `auth_revoked`, `auth_unavailable`, `slow_consumer`, `tx_too_large`.
- `event: shutdown` — sent during graceful shutdown: `data: {"reason": "service_restart"}`.
- SSE comment `:\n\n` — keep-alive ([§6.2](06-network-behavior.md#62-heartbeat)). Not visible to client EventSource API.

## 3.6. Large transactions

Per-subscriber size limits per transaction:
- Exact subscription: 1,000 changes.
- Wildcard subscription: 10,000 changes.
- Payload byte size: 10 MB.

On exceeding any limit, send `event: error\ndata: {"reason": "tx_too_large"}` and close the connection. Chunking with `partial: true` is deliberately not implemented in MVP — add if a real use case appears.
