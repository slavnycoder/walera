# internal/sse Invariants

Canonical home for load-bearing invariants removed from per-file headers
during the SWEEP-01 sweep. See also: internal/sse/doc.go (package-level
narrative). Each entry carries a file:line-range anchor against the
pre-sweep commit `de6b665` so the full rationale is recoverable via
`git show de6b665:internal/sse/<file>` or `git log -p`.

## Concurrency

1. **Single-writer / cancel-not-close discipline** — `stream` runs on the
   handler's own goroutine and is the sole owner of `http.ResponseWriter`
   for the connection's lifetime. It exits via `sub.Done()` (fail-closed
   revoke/shutdown signal) or `r.Context().Done()`, always through
   `sub.Drop(reason)` rather than closing the underlying channels
   directly. There is no separate writer pool: one request goroutine per
   subscriber, bounded by that subscriber's own `queue` channel.
   Anchor: `stream.go:122-200`.

## State Machine Ordering

2. **SSE handshake gate sequence (1-4)** — (1) `limits.AcquireGlobal`
   → 503 + Retry-After:5 on fail; (2) Bearer token presence +
   `authClient.Permissions` (breaker-gated; 401/403/404 forwarded
   verbatim, 5xx → 503 + Retry-After:5); (3) `limits.AcquirePerUser(userID)`
   → 429 on fail; (4) table in `authMap.Tables` → 403
   `{"reason":"not_allowed"}`. Each gate failure short-circuits with the
   matching HTTP response; SSE response headers are never emitted on a
   gate failure (validate-then-execute, doc.go invariant 3). On partial
   success the deferred release flags in `handshakeResult`
   (`globalAcquired`, `perUserAcquired`) drive correct limit-release in
   `runHandshakeAndWriter`. Token-bucket rate limiting (per-IP pre-auth,
   per-user post-auth) is intentionally **not** done in-process — push it
   onto the upstream proxy (traefik, ingress, etc.) where it can apply
   uniformly across replicas and absorb pathological clients before they
   consume a Goroutine.

## Security / PII

3. **PII discipline (mirror of doc.go invariant 5)** — the writer logs
   `subscriber_id`, `channel`, `table`, `pk`, `commit_lsn`, `tx_id`,
   `reason`. It MUST NEVER log `Data` / `Changed` maps from a
   `wal.Change`, and bearer tokens MUST NOT appear in any log field.
   Anchor: `stream.go:177-188`.
