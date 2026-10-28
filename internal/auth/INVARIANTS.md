# internal/auth Invariants

Canonical home for load-bearing invariants removed from per-file headers
during Phase 3 SWEEP-04. See also: internal/auth/doc.go (package-level
narrative).

## Concurrency

1. **Whitelist.Swap atomic publication.** Per-subscriber refresh publishes a
   new `*Whitelist` snapshot via `atomic.Pointer[Whitelist].Store`. Readers
   (`FilterClosure`, `FilterWithLSN`) always observe a complete snapshot —
   never a partial update. The 1-slot `PrevWhitelist` back-buffer holds the
   previous snapshot so a transaction whose `CommitLSN ≤ AuthMap.RefreshLSN`
   is filtered through the older map (it logically committed BEFORE the
   refresh became visible). Anchor: `subscriber.go:swapMap` (~line 308)
   (pre-sweep de6b665).

2. **LSN stamping order.** `swapMap` stamps `fresh.RefreshLSN = s.lsn()`
   BEFORE demoting `AuthMap → PrevWhitelist` and promoting `fresh → AuthMap`.
   Concurrent `FilterWithLSN` readers therefore see either the OLD map
   (pre-swap world) or the NEW map (post-swap world) — never a
   "promoted-without-back-buffer" intermediate. Anchor: `subscriber.go:swapMap`
   (~line 308) (pre-sweep de6b665).

3. **Fail-closed refresh.** Each subscriber runs its own `RefreshLoop`
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

_(No auth-package lifecycle invariants required relocation.
`Subscriber.RefreshLoop` is launched with a plain `go` statement from
`internal/sse/stream.go`; it exits cleanly on `ctx.Done()` / `Sub.Done()`
and a panic crashes the process rather than being recovered — this is
local to its own file and needs no cross-file anchor.)_

## Security / PII

4. **PK always preserved.** `Whitelist.Filter` always copies the primary-key
   column into the filtered output regardless of whether the PK appears in
   the user's allowed column set. The wire-format invariant: an SSE consumer
   can always join the change event back to its source row by PK. INSERTs
   with only the PK still emit ("row exists" signal); UPDATEs whose
   `Changed` map has no non-PK whitelisted survivor are silently dropped
   (Rule 3 — PK-presence cannot rescue an UPDATE that carries no whitelisted
   data). Anchor: `map.go:Filter` (~line 50, switch on `c.Op`) (pre-sweep
   de6b665).

5. **Bearer/forwarded credentials are held only for in-process replay, never
   logged.** The user's bearer token and forwarded cookies/headers are read
   once in `internal/sse/auth.go:runHandshake`, passed to `Client.OpenSession`,
   and then carried on the `Subscriber` (`SubscriberConfig.Bearer` /
   `.Forwarded`, fields `bearer` / `fwd`) purely so `tryRefresh` can replay the
   exact same `OpenSession` call on every TTL tick — refresh re-authenticates
   with the client's own credentials, not a service-wide secret, so logout and
   token expiry reach a live stream. These fields are never logged, never
   serialized, and never appear in an error value. The CI grep gate
   `! grep -nE 'Str\(.*[Tt]oken|Str\(.*Authorization' internal/auth/client.go`
   enforces zero-log discipline at build time. Anchors:
   `internal/sse/auth.go:runHandshake` (OpenSession call site),
   `internal/auth/client.go:OpenSession`,
   `internal/auth/subscriber.go:tryRefresh`.
