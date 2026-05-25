# internal/sse Invariants

Canonical home for load-bearing invariants removed from per-file headers
during the SWEEP-01 sweep. See also: internal/sse/doc.go (package-level
narrative). Each entry carries a file:line-range anchor against the
pre-sweep commit `de6b665` so the full rationale is recoverable via
`git show de6b665:internal/sse/<file>` or `git log -p`.

## Concurrency

1. **Single-writer / cancel-not-close discipline** — the writer goroutine
   is the sole owner of `http.ResponseWriter` for the connection's
   lifetime; the handler exits via `<-doneCh` (or `r.Context().Done()`
   with a bounded `WriteTimeout` wait) and triggers `sub.Drop("client_closed")`
   rather than closing the underlying channels directly. The pool's
   `evictDone` observes `sub.Done()` and runs teardown.
   Anchor: `stream.go:60-108,249-288` (de6b665).

2. **safego.Go discipline** — no direct `go` keyword launches in this
   package. The single goroutine launch site (the auth refresh ticker) is
   wrapped by `safego.Go("auth-refresh-"+sub.ID(), ...)` so every
   goroutine carries a name and a panic recover.
   Anchor: `stream.go:166-168` (de6b665).

3. **Per-worker single-writer of subState fields** — each `subState` is
   accessed only by its owning worker goroutine: `lastWriteAt`, `buffer`,
   `bufBytes`, `inDirty`, `inDisconnected`, and `dropReason` carry no
   locks because the worker is the sole reader and writer. Cross-goroutine
   handoff is the bounded `queue` channel (publish-side) and the `done`
   signal channel (close-side, guarded by `safeCloseDone`).
   Anchor: `attach.go:22-105`, `drain_helpers.go:130-147` (de6b665).

## Lifecycle & Shutdown

4. **WR-02 — abandonCh priority FIRST in collectDirty** — the per-sub
   shutdown iteration begins with a non-blocking `select { case
   <-w.abandonCh: ... default: }` BEFORE the `st.done` skip check. When
   `Pool.Shutdown`'s ctx fires the abandon signal must short-circuit the
   loop on the very next iteration; the prior ordering observed `st.done`
   first and could continue draining one more sub past the budget.
   Anchor: `shutdown.go:55-67` (de6b665).

5. **WR-03 — recover scope + named `writeErr` return in emitFinalFrames** —
   the inner `func() { defer recover(); ... writeErr = ... }()`
   immediately-invoked closure exists so that a respWriter+rc-fallback
   write that panics inside net/http (handler-already-finished race) is
   captured into the named return value rather than crashing the worker.
   The named `writeErr` return is required: the recover's assignment is
   how the truthful-reason override in `closeAndCount` learns the frame
   write failed.
   Anchor: `shutdown.go:105-171` (de6b665).

6. **CR-01 — recover atomic with Write/Flush in evictDone** — the
   evict-done error/shutdown frame write wraps `Write` plus `Flush`
   inside a single `func() { defer recover(); ... }()` block so a
   respWriter teardown between `Write` and `Flush` cannot leak a panic
   out of the worker. The deferred recover scope MUST bracket both
   calls; splitting them across two recover blocks would leave a
   recover-free window.
   Anchor: `worker_loop.go:303-326`, `shutdown.go:139-169` (de6b665).

7. **LIFE-02 — drainShutdownDeadline hard-cap per-sub** — the spec §3.5
   per-sub final-frame write budget is the package-level constant
   `drainShutdownDeadline = 50ms`. It is HARDCODED — not exposed via
   koanf and not operator-tunable. The matching `PoolConfig.drainShutdownDeadline`
   field exists for test injection only and defaults to this constant
   in `applyDefaults`. One wedged sub cannot pin a worker partition
   past this budget.
   Anchor: `pool.go:37-43,100-103,132-136`, `shutdown.go:124-131,152` (de6b665).

## State Machine Ordering

8. **SSE-06/07 pool struct ownership + pollAllQueues observes
   attach/shutdown** — the WriterPool owns workers; each worker owns its
   `subs []*subState` partition exclusively. The inner `pollAllQueues`
   non-blocking select carries three cases beyond the per-sub queue
   receive: `attachCh` (apply mid-drain so a new sub does not starve),
   `shutdownCh` (mirror the outer select's heartbeat-stop + drainAll +
   drainShutdown + return), and `default` (goto nextSub). Without these,
   sustained per-sub inflow would starve attach/shutdown signals.
   Anchor: `worker_loop.go:386-454`, `pool.go:239-254,447-569` (de6b665).

9. **DECOMP-01 pool-worker fields promoted from loop-locals** — `timer`,
   `timerArmed`, `shutdownObservedInPoll`, and `dirty` were promoted
   from `run()` outer-loop locals to `*poolWorker` struct fields so the
   decomposed helpers (`drainAll`, `pollAllQueues`, `sweepHeartbeats`,
   `evictDone`, `drainShutdown`) can access them without taking
   pointer parameters that would force the locals to escape to the heap
   and regress the BENCH allocator profile. Field-zero values are the
   correct initial state; `dirty` is pre-allocated at cap 128 in
   `newPoolWorker` to preserve the stack-allocated capacity of the
   pre-decomposition `make([]*subState, 0, 128)`. `run()` is invoked
   exactly once per worker lifetime so no re-init is required across
   iterations.
   Anchor: `pool.go:551-568,608-622` (de6b665).

10. **SSE handshake gate sequence (1-6)** — (1) `limits.AcquireGlobal`
    → 503 + Retry-After:5 on fail; (2) `limits.AllowPreAuthRate(clientIP)`
    → 429 + Retry-After:1 on fail; (3) Bearer token presence +
    `authClient.Permissions` (breaker-gated; 401/403/404 forwarded
    verbatim, 5xx → 503 + Retry-After:5); (4) `limits.AcquirePerUser(userID)`
    → 429 on fail; (5) `limits.AllowPerUserRate(userID)` → 429 +
    Retry-After:1 on fail; (6) table in `authMap.Tables` → 403
    `{"reason":"not_allowed"}`. Each gate failure short-circuits with the
    matching HTTP response; SSE response headers are never emitted on a
    gate failure (validate-then-execute, doc.go invariant 3). On partial
    success the deferred release flags in `handshakeResult`
    (`globalAcquired`, `perUserAcquired`) drive correct limit-release in
    `runHandshakeAndWriter`.
    Anchor: `auth.go:228-310`, `handler.go:396-423` (de6b665).

## Security / PII

11. **PII discipline (mirror of doc.go invariant 5)** — the writer logs
    `subscriber_id`, `channel`, `table`, `pk`, `commit_lsn`, `tx_id`,
    `reason`. It MUST NEVER log `Data` / `Changed` maps from a
    `wal.Change`, and bearer tokens MUST NOT appear in any log field.
    Anchor: `stream.go:177-188`, `doc.go:23-25` (de6b665).
