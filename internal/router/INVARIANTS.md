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

2. **multi_root drop site.** The only site is the multi-root guard
   inside `(*Broadcaster).dispatchEvent`, which fires when a tx contains >1
   distinct PK for the subscriber's anchor `(schema, table)` pair
   (helper: `hasMultipleAnchorRoots`). Multi-root drop is **per-subscriber
   tx drop, not a disconnect**: only a warning is logged. `sub.Drop()` is
   NOT called — the connection stays open and the next well-formed tx is
   delivered normally. Clients resync from the primary API on their own.
   The guard applies to KindExact subscribers only.

3. **slow_consumer / tx_too_large drop sites.** Both reasons drive
   `sub.Drop(reason)` + a warning log exclusively from
   `(*Broadcaster).dispatchEvent` in `router.go` — the non-blocking send
   shim's `false` return triggers `slow_consumer`; the per-tx change cap
   and the encoder-overflow branch both trigger `tx_too_large`.

4. _(Retired: routeTx no longer records per-tx metrics.)_

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
copy-before-unlock + per-sub goroutine fan-out is captured directly
in `(*Broadcaster).Shutdown`'s godoc and in doc.go invariants 4 + 8._

## State Machine Ordering

_Not applicable — the router has no FSM. See internal/sse/INVARIANTS.md
for the drain sequence._
