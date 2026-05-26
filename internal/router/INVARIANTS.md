# internal/router Invariants

Canonical home for load-bearing invariants removed from per-file headers
during Phase 3 SWEEP-02. See also: internal/router/doc.go (package-level
narrative).

Pre-sweep archaeology anchor: commit `de6b665` (pre-SWEEP-02 HEAD).

## Concurrency

1. **mergeMatches out-param signature.** `mergeMatches` takes
   `eligible map[*Subscriber]struct{}` as an in-out parameter rather
   than returning the map; the caller pre-allocates
   `eligible := make(map[*Subscriber]struct{}, 8)` on its own stack
   frame (`router.go` inside `routeTx`). This prevents heap-escape that
   would breach the ≤3% allocs/op BENCH-02 gate (DECOMP-06 sub-shape
   `exact_10` / `wildcard_100`). The eligible-set shape inherently
   deduplicates multiple per-change hits for the same subscriber (a
   second `eligible[sub] = struct{}{}` is a no-op), so doc.go #2
   (one Event per subscriber per tx) is preserved without extra dedup
   logic inside `mergeMatches`. `routeTx` allocates `fullIndices`
   once per tx (one `make([]int, len(tx.Changes))`) and passes it
   read-only to each sequential `dispatchEvent` call, eliminating the
   per-subscriber-per-change `append` allocation of the prior shape.

2. **tx_dropped_total{reason="multi_root"} pre-touch.** Pre-touched
   ONLY in `router.New` via `b.metrics.TxDropped("multi_root").Add(0)`.
   No `.Inc()` or `.Add(` for `"multi_root"` exists anywhere else in
   the package. The code path is unreachable by construction — every
   channel's table IS its root in the current pipeline (no multi-root
   entity hierarchy) — but the series is pre-touched at construction
   so `Gather()` always shows the sentinel from t=0. See doc.go
   invariant 7 (paragraph after the numbered list in `router.go`'s
   former file-header invariants block).

3. **slow_consumer / tx_too_large drop sites.** Both reasons drive
   `sub.Drop(reason)` + `metrics.TxDropped(reason).Inc()` exclusively
   from `(*Broadcaster).dispatchEvent` in `router.go` — the
   non-blocking send shim's `false` return triggers `slow_consumer`;
   the per-tx change cap and the encoder-overflow branch both trigger
   `tx_too_large`. No other call sites Increment these counters in
   the router package.

4. **routeTx stack-frame ownership.** The `lookupTimer` defer,
   `RoutingFanOut().Observe(float64(len(eligible)))`,
   `TxFanOutWork().Observe(float64(totalDelivered))`, and
   `CoBeyondAnchorTotal().Add(float64(totalBeyondAnchor))` MUST all
   stay in `routeTx`'s stack frame (not lifted into `mergeMatches` or
   `dispatchEvent`). `RoutingFanOut` reflects the post-merge eligible
   subscriber count; `TxFanOutWork` (D-03) is observed after the
   dispatch loop with Σ delivered changes across all eligible
   subscribers and is observed only when `totalDelivered > 0` (a matched tx
   whose eligible subscribers were all dropped records no histogram sample —
   the registry pre-touch already seeds the series at t=0); the
   `lookupTimer` defer brackets the merge + dispatch as one atomic
   measurement window. `CoBeyondAnchorTotal` (D-01/SAFE-02) is the
   beyond-anchor counter accumulated across the dispatch loop from the
   second return value of `dispatchEvent` and observed only when
   `totalBeyondAnchor > 0` — alongside `TxFanOutWork`.

5. **Sticky-reason atomic.Pointer in Subscriber.** `reasonPtr
   atomic.Pointer[string]` is written by `Drop` BEFORE `cancel` so any
   goroutine observing `ctx.Done()` can read a non-empty `Reason()`
   reliably. `sync.Once` (`reasonOnce`) guards against double-drop
   (router-side `slow_consumer` racing with writer-side
   `client_closed`); only the FIRST drop reason wins. Drop NEVER
   closes any channel — the pool owns the per-sub queue and tears it
   down on the worker goroutine when it observes the drop (doc.go
   invariant 6).

6. **encoderIface decoupling — two same-named seams, not a duplicate.**
   `internal/router` and `internal/sse` each define a package-private
   `encoderIface`, but the method sets are entirely non-overlapping:
   router's seam exposes `Encode(Event) ([]byte, bool)` (Event-to-wire
   bytes); sse's seam exposes `EncodeHeartbeat() / EncodeShutdown() /
   EncodeError(string)` (control-frame bytes). Because router must not
   import sse (preserving the unidirectional dependency: sse depends on
   router, not the reverse), a single shared interface would couple the
   two packages. The two definitions are therefore deliberately kept
   independent. This is reconciled as DEAD-03 outcome (b): "defined
   exactly once OR documented reason for cross-package independence."

## Authorization

1. **Co-transactional fan-out requires a surviving anchor.**
   A subscriber is only eligible to receive beyond-anchor rows from a
   matched transaction when at least one raw channel-matching change
   survives `Subscriber.Filter`. A raw exact or wildcard match that is
   later dropped by field filtering does not authorize delivery of
   other whitelisted rows in the same transaction. This preserves the
   existing hidden-column semantics: an UPDATE that only changes fields
   invisible to the subscriber remains invisible and cannot be used as a
   fan-out anchor. Anchor: `router.go:dispatchEvent` and
   `matchesAnchor`.

## Lifecycle & Shutdown

_No additional invariants required for SWEEP-02 — `Shutdown`'s
copy-before-unlock + per-sub `safego.Go` fan-out is captured directly
in `(*Broadcaster).Shutdown`'s godoc and in doc.go invariants 4 + 8._

## State Machine Ordering

_Not applicable — the router has no FSM. See internal/auth/INVARIANTS.md
for the breaker FSM and internal/sse/INVARIANTS.md for the drain
sequence._
