# internal/app Invariants

Canonical home for load-bearing invariants removed from per-file headers
during Phase 3 SWEEP-03. See also: internal/app/doc.go (package-level
narrative).

## Concurrency

§1 — Goroutines are plain `go` statements; panics are not recovered.
`lifecycle.go` starts one goroutine per Runnable and one per shutdown
step (`pprof` when `a.PProfServer != nil`, `http`, `broadcast`). A panic
in any of them crashes the process so the orchestrator restarts it.

§2 — DI-03 cycle break encapsulated inside wireAuth.
The sole production call site of `authClient.SetBreaker(breaker)` is
`initialize.go:186`, between `wireAuth`'s opening (line 174) and closing
(line 206). No other call site exists. `SetBreaker` is guarded by
`sync.Once` internally so the install is init-only. Anchor:
`initialize.go:174-206` (pre-sweep `de6b665`).

## Lifecycle & Shutdown

§3 — 5-step shutdown sequence (`lifecycle.go`).
`(*App).Shutdown` runs:

1. pool drain
2. pprof close (conditional on `PProfServer != nil`)
3. http shutdown
4. broadcast drain
5. final cleanup (admin-conn close + 50 ms flush sleep)

Each step has its own bounded deadline propagated from `ShutdownConfig`;
the per-sub drain inside step 1 is bounded by `drainShutdownDeadline`
(sse package; 50 ms default). The outer hard cap is
`a.Config.Shutdown.Deadline` (10 s default) installed as a `time.AfterFunc`
before `wg.Wait`. Anchor: `lifecycle.go:200-310` (pre-sweep `de6b665`).

## Security / PII

§4 — PII-clean log surface.
`internal/app` emits no log statements that include row data, tokens, or
secrets; structured fields are limited to identifiers (`subscriber_id`,
`channel`, `table`, `pk`, `commit_lsn`, `tx_id`, `reason`). Anchor:
package-wide.
