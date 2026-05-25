// Package router maintains the subscription indexes and the per-tx fan-out
// logic that delivers wal.Tx changes to SSE subscribers.
//
// Design invariants:
//
//  1. Single-reader / N-writer fan-out. One reader goroutine produces wal.Tx;
//     one router goroutine consumes the tx channel; N writer goroutines
//     (one per SSE connection) consume per-subscriber buffered channels.
//
//  2. 8-shard exact subscription index keyed by xxhash.Sum64String(key)%8
//     with per-shard sync.RWMutex. Shard locks are released BEFORE any
//     per-subscriber work to prevent slow consumers from blocking the index
//     hot path ("copy-before-unlock").
//
//  3. Single-mutex wildcard subscription index keyed by "<schema>.<table>".
//     Wildcard cardinality is bounded by table count, so a single
//     sync.RWMutex is sufficient.
//
//  4. Copy-before-unlock rule for both indexes. Lookup acquires the lock,
//     copies the subscriber pointer (or a slice copy for wildcard), releases
//     the lock, then returns — the caller does all subsequent work outside
//     the lock.
//
//  5. Non-blocking subscriber send. After encoding the per-sub Event into
//     SSE wire bytes, the router calls sub.send(frame); the wired closure
//     (installed via Subscriber.WireSendFunc by pool.Attach) is a
//     non-blocking enqueue on the pool's per-sub queue. It returns false
//     when the queue is full → the router calls sub.Drop("slow_consumer").
//     The router never owns a buffered channel of its own; backpressure is
//     signalled via ctx cancel plus a sticky reason (see invariant 6).
//
//  6. Cancel-not-close cleanup. Subscriber.Drop sets a sticky reason via
//     atomic.Pointer[string] and cancels the subscriber's context. It NEVER
//     closes any channel — the pool worker that owns the per-sub queue
//     closes it on its own goroutine when it observes the drop. The SSE
//     writer's defer observes Done() + Reason() and emits a best-effort
//     error event.
//
//  7. Single-owner deregistration by the SSE writer goroutine. The router
//     calls Drop but never removes a subscriber from the index — that
//     responsibility belongs exclusively to the writer's defer. This
//     eliminates lock-ordering bugs between the router and the writer.
//
//  8. safego.Go is the SOLE goroutine spawn in production code. Tests may
//     use raw `go`.
//
// See also internal/router/INVARIANTS.md for the canonical invariant list.
package router
