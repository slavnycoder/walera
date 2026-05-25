# 0. Overview & Quick Reference

> This document is the authoritative specification for implementing the service. Every decision below has been made deliberately; do not deviate without explicit instruction.

## Purpose

This service streams PostgreSQL row-level changes to clients over Server-Sent Events (SSE). A client subscribes to a channel of the form `entity_name:id`. The service authorizes the subscription via an external auth backend (which returns a map of accessible tables and fields), then tails the WAL and delivers only the relevant changes, filtered by the allowed fields.

**Target load:** ~10,000 concurrent SSE subscribers, ~5,000 WAL transactions/second.

**Language/runtime:** Go (latest stable, 1.22+).

---

## Quick Reference: Key Decisions

| # | Topic | Decision |
|---|---|---|
| 1 | Source DB | PostgreSQL ≥ 14 with logical replication |
| 2 | `entity_name` meaning | **Root table** name of a logical entity. Entity may span multiple PG tables (root + FK-attached children); see #37 |
| 3 | Primary key | Single scalar PK only (int, uuid, text) |
| 4 | Scale | ~10k subscribers, ~5k tx/s |
| 5 | Deployment | Single instance + **temporary** replication slot (created on connect, dropped on disconnect) |
| 6 | Event granularity | One SSE event = one whole transaction (atomic) |
| 7 | Snapshot | Pure CDC; no snapshot on subscribe |
| 8 | WAL plugin | `pgoutput` (built-in, binary) |
| 9 | Publication | Explicit table list from config |
| 10 | REPLICA IDENTITY | DEFAULT (sparse updates, no `before`) |
| 11 | LSN ack timing | Immediately after routing into in-memory broadcaster |
| 12 | Slot lifecycle | Temporary slot created by the service on each PG connect; PG drops it on disconnect. No DBA setup, no persistence across restarts |
| 13 | TRUNCATE / DDL | TRUNCATE disabled in publication; DDL handled passively via Relation messages |
| 14 | Auth timing | On open + periodic refresh |
| 15 | Auth key | Bearer token in `Authorization` header, forwarded to auth backend |
| 16 | Auth refresh failure (per-subscriber) | Fail-closed with retry (2-3 attempts + backoff). Systemic outages are handled separately by the circuit breaker — see #38 |
| 17 | Field map semantics | Whitelist; table absent → 403; PK always visible (tautology for exact subs; enumeration exposure for wildcards — see #32); update with only hidden fields → drop silently |
| 18 | Auth map updates | Atomic replace, applied to txs with `commit_lsn > refresh_lsn` |
| 19 | Subscription index | Sharded hash-map with per-shard RWMutex |
| 20 | Goroutines | 1 reader + 1 router + N writers (per subscriber) |
| 21 | Slow consumer policy | Disconnect on buffer overflow |
| 22 | SSE payload | `event: tx` with `tx_id`, `commit_ts`, `changes[]` (commit LSN deliberately not exposed — internal-only) |
| 23 | Keep-alive | SSE-comment `:\n\n` every ~15s + TCP keepalive |
| 24 | Limits | Global concurrency, per-user rate, per-user max conns, max tx size, max payload size |
| 25 | New subscriber cutoff | Starts from next COMMIT after registration; optional `?since_lsn=` |
| 26 | Registration order | HTTP → auth → build `subscriber{auth, start_lsn}` → register in index → start writer |
| 27 | PG → JSON mapping | JS-safe: bigint/numeric as strings, timestamp as RFC3339 UTC, jsonb embedded, bytea base64 |
| 28 | Catch-up on restart | None: a fresh temporary slot starts from current LSN. Events that occurred during downtime are not delivered — clients refetch state via snapshot on reconnect (snapshot covers them) |
| 29 | Graceful shutdown | Stop accept → finish current tx → broadcast shutdown event → close replication conn (PG drops temp slot) → exit |
| 30 | Observability | Detailed Prometheus metrics + structured logs + health endpoints |
| 31 | Tests | Unit + integration with real PG (testcontainers) + mock auth; `-race` enabled |
| 32 | Wildcard subscriptions | Supported (`entity:*`) **for publicly-enumerable entities only** (currency rates, public catalogs). Wildcards inherently disclose PK enumeration; auth backend is the sole gatekeeper. Not for PII tables |
| 33 | Auth request format | `GET /auth/permissions?channel=...` with Bearer token |
| 34 | Max SSE lifetime | No hard limit; rely on keep-alive and reconnect |
| 35 | Instance size | 4 CPU / 8 GB RAM; k8s 2/4 requests, 4/8 limits |
| 36 | PG disconnect policy | Service keeps subscribers alive and reconnects; `/healthz` fails immediately, kubelet liveness (`periodSeconds=2, failureThreshold=3`) kills the pod after ~5s if PG doesn't recover. Bounded silent-gap window ~5s; longer outages → clients snapshot-resync via TCP RST |
| 37 | Composite views / root-entity routing | Auth backend declares `roots` per user (subset of `tables`). Subscriptions target root tables only. Backend MUST bump `<root>.updated_at` in any tx changing related child rows. Router delivers full tx (whitelist-filtered) to subscribers matched on root. Multi-root tx → per-subscriber drop + log (`tx_dropped_total{reason="multi_root"}`); no-root tx → silently delivered to no one |
| 38 | Auth-backend outage policy | Asymmetric: per-subscriber refresh failure → fail-closed (disconnect). Systemic outage (breaker open) → **bounded** fail-open (subscribers keep current maps; new opens still fail-closed). Worst-case stale-permissions window ~2 min. On breaker close → immediate refresh of all stale subscribers |
