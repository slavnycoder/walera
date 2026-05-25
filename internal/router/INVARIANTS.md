# internal/router Invariants

Canonical home for load-bearing invariants removed from per-file headers
during Phase 3 SWEEP-02. See also: internal/router/doc.go (package-level
narrative).

Pre-sweep archaeology anchor: commit `de6b665` (pre-SWEEP-02 HEAD).

## Concurrency

1. **mergeMatches out-param signature.** `mergeMatches` takes
   `matched map[*Subscriber][]int` as an in-out parameter rather than
   returning the map; the caller pre-allocates
   `matched := make(map[*Subscriber][]int, 8)` on its own stack frame
   (`router.go` inside `routeTx`). This prevents heap-escape that would
   breach the ≤3% allocs/op BENCH-02 gate (DECOMP-06 sub-shape
   `exact_10` / `wildcard_100`). The per-tx subscriber-set accumulation
   invariant (doc.go #2 — one Event per subscriber per tx) is preserved
   by this signature shape: every matched-change pair lands in the same
   map keyed by `*Subscriber`, regardless of how many `tx.Changes` it
   matches.

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

4. **routeTx stack-frame ownership.** The `lookupTimer` defer +
   `RoutingFanOut().Observe(float64(len(matched)))` MUST stay in
   `routeTx`'s stack frame (not lifted into `mergeMatches`), so the
   fan-out histogram observation reflects the post-merge subscriber
   count and the lookup timer brackets the merge + dispatch as one
   atomic measurement window.

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

## Lifecycle & Shutdown

_No additional invariants required for SWEEP-02 — `Shutdown`'s
copy-before-unlock + per-sub `safego.Go` fan-out is captured directly
in `(*Broadcaster).Shutdown`'s godoc and in doc.go invariants 4 + 8._

## State Machine Ordering

_Not applicable — the router has no FSM. See internal/auth/INVARIANTS.md
for the breaker FSM and internal/sse/INVARIANTS.md for the drain
sequence._
