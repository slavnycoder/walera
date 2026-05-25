# internal/auth Invariants

Canonical home for load-bearing invariants removed from per-file headers
during Phase 3 SWEEP-04. See also: internal/auth/doc.go (package-level
narrative).

## State Machine Ordering

1. **Breaker FSM** — Closed / Open / HalfOpen state transitions, the
   `>FailureRateThreshold` sliding-window failure-rate trigger (debounced by
   DebounceFloor sample count), and the `install-new-channel-before-close-old`
   ordering of `signalClose` (so concurrent `WaitForClose()` readers cannot
   receive a closed channel intended for the NEXT cycle) all live in
   `internal/auth/breaker.go`'s package comment.
   **This is the canonical home per REQUIREMENTS.md SWEEP-04 — do NOT
   restate the FSM here.** Anchor: `internal/auth/breaker.go` top-of-file
   package comment (pre-sweep de6b665).

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

4. **Stale-subs sampler.** `Subscribers.fanoutStaleRefreshes` walks the
   registry under `mu`, builds a copy of stale-subscriber pointers
   (`lastRefresh < now - ttlSeconds`), then RELEASES `mu` before scheduling
   `time.AfterFunc`-driven `tryRefresh` calls. Releasing the mutex before
   scheduling prevents a long jitter window from stalling `Add`/`Remove`.
   The `walera_auth_breaker_stale_subscribers` gauge is set to `len(stale)`
   per cohort, so observability shows fan-out cohort size. Anchor:
   `registry.go:fanoutStaleRefreshes` (~line 91) (pre-sweep de6b665).

## Lifecycle & Shutdown

_(No auth-package lifecycle invariants required relocation. The
`auth-breaker-fsm` and `auth-refresh-*` goroutines are spawned by `safego.Go`
in the composition root and exit cleanly on `ctx.Done()` / `Sub.Done()` —
these are local to their respective files and need no cross-file anchor.)_

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

6. **Tokens never logged.** Bearer credentials are passed only as a function
   parameter to `Client.Permissions` and written only to the `Authorization`
   header. They are NEVER assigned to local variables beyond the header set
   call. NEVER captured into struct fields beyond `Subscriber.token`
   (unexported, never read by any log statement). The CI grep gate
   `! grep -nE 'Str\(.*[Tt]oken|Str\(.*Authorization' internal/auth/client.go`
   enforces this discipline at build time. Anchor: `client.go:Permissions`
   header-set block (~line 167) (pre-sweep de6b665).
