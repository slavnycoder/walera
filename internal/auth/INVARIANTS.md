# internal/auth Invariants

Canonical home for load-bearing invariants removed from per-file headers
during Phase 3 SWEEP-04. See also: internal/auth/doc.go (package-level
narrative).

## State Machine Ordering

1. **Breaker FSM** — Closed / Open / HalfOpen state transitions, driven by
   the `>FailureRateThreshold` sliding-window failure-rate trigger
   (debounced by `DebounceFloor` sample count) and a `Cooldown`-gated
   half-open probe. `Allow()` gates only NEW opens (`State != Open`);
   subscriptions already streaming are unaffected by the breaker and are
   instead governed by fail-closed refresh (invariant 4). There is no
   stale-subscriber watcher goroutine — the breaker has no visibility into,
   and no side effect on, existing subscribers. Anchor: `breaker.go:94-157`.

## Concurrency

2. **Whitelist.Swap atomic publication.** Per-subscriber refresh publishes a
   new `*Whitelist` snapshot via `atomic.Pointer[Whitelist].Store`. Readers
   (`FilterClosure`, `FilterWithLSN`) always observe a complete snapshot —
   never a partial update. The 1-slot `PrevWhitelist` back-buffer holds the
   previous snapshot so a transaction whose `CommitLSN ≤ AuthMap.RefreshLSN`
   is filtered through the older map (it logically committed BEFORE the
   refresh became visible). Anchor: `subscriber.go:swapMap` (~line 308)
   (pre-sweep de6b665).

3. **LSN stamping order.** `swapMap` stamps `fresh.RefreshLSN = s.lsn()`
   BEFORE demoting `AuthMap → PrevWhitelist` and promoting `fresh → AuthMap`.
   Concurrent `FilterWithLSN` readers therefore see either the OLD map
   (pre-swap world) or the NEW map (post-swap world) — never a
   "promoted-without-back-buffer" intermediate. Anchor: `subscriber.go:swapMap`
   (~line 308) (pre-sweep de6b665).

4. **Fail-closed refresh.** Each subscriber runs its own `RefreshLoop`
   ticking on its TTL; there is no central stale-subscriber watcher or
   sampler — a subscriber's refresh is entirely self-scheduled.
   `tryRefresh` retries through `s.backoffs` on `*ErrUnavailable`, but once
   the chain exhausts those retries without a successful swap, the
   subscriber is DROPPED (`Sub.Drop("auth_unavailable")`) rather than kept
   alive on a stale whitelist — refresh failure fails closed, it never
   fails open. A revoked grant (`attemptRevoked`) drops immediately with
   `"auth_revoked"`, without waiting for backoff. Anchor:
   `subscriber.go:tryRefresh,dropIfUnchanged` (~line 248-303).

## Lifecycle & Shutdown

_(No auth-package lifecycle invariants required relocation. `Breaker.Run`
is driven by an `app.Runnable` and `Subscriber.RefreshLoop` is launched
with a plain `go` statement from `internal/sse/stream.go`; both exit
cleanly on `ctx.Done()` / `Sub.Done()` and a panic in either crashes the
process rather than being recovered — these are local to their
respective files and need no cross-file anchor.)_

## Security / PII

5. **PK always preserved.** `Whitelist.Filter` always copies the primary-key
   column into the filtered output regardless of whether the PK appears in
   the user's allowed column set. The wire-format invariant: an SSE consumer
   can always join the change event back to its source row by PK. INSERTs
   with only the PK still emit ("row exists" signal); UPDATEs whose
   `Changed` map has no non-PK whitelisted survivor are silently dropped
   (Rule 3 — PK-presence cannot rescue an UPDATE that carries no whitelisted
   data). Anchor: `map.go:Filter` (~line 50, switch on `c.Op`) (pre-sweep
   de6b665).

6. **Bearer tokens never persisted beyond handshake.** The user's bearer
   credential is read once in `internal/sse/auth.go:runHandshake` and passed
   directly to `Client.OpenSession` as a function parameter. As soon as
   `OpenSession` returns, `r.Header.Del("Authorization")` and the local
   `token` variable are cleared in the same scope — the bearer is not
   captured into any struct field, logged, or otherwise retained. All
   subsequent refresh requests for that subscriber are authenticated via
   `RefreshPermissions` using HMAC-SHA256 over the durable identity
   `(user_id, channel, ts, nonce)` with the service-wide
   `auth.signing.secret`; the bearer plays no further role. The CI grep
   gate `! grep -nE 'Str\(.*[Tt]oken|Str\(.*Authorization' internal/auth/client.go`
   enforces zero-log discipline at build time. Anchors:
   `internal/sse/auth.go:runHandshake` (OpenSession call site),
   `internal/auth/client.go:OpenSession`,
   `internal/auth/client.go:RefreshPermissions`,
   `internal/auth/signing.go:Signer`.
