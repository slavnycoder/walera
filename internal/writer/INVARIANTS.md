# internal/writer Invariants

Canonical home for load-bearing invariants removed from per-file headers
during the v2.4 phase-04 sweep (WRITER-CLN-01). Each entry carries a
file:line anchor against the pre-sweep commit `6194d9d` so the full
rationale is recoverable via
`git show 6194d9d:internal/writer/<file>` or `git log -p`.

The writer is the testbench load-generator that drives quantitative,
scenario-named traffic against the Walera testbench Postgres. It runs
exactly one process per testbench instance.

## Concurrency & Atomic Publication

1. **scenarioPtr is the lock-free publication channel** — the commit loop
   reads `scenarioPtr.Load()` on every iteration; the POST /control handler
   constructs a fresh `*scenarioState` and `Store`s it. Fields of
   `scenarioState` are effectively immutable once published — mutation is
   pointer-swap, not field-write. `nextIdx` is the one mutable field and
   it lives behind `atomic.Uint64.Add`. Concurrent /control POSTs may
   interleave; the last `Store` wins (testbench operator-drives-it model).
   Anchor: `scenario.go:26-38`, `server.go:14-22` (6194d9d).

2. **scenarioPtr type alias for cross-package atomic.Pointer** —
   `ScenarioStateExport = scenarioState` is a type-alias so cmd/writer can
   construct `*atomic.Pointer[ScenarioStateExport]` that unifies with the
   package-internal `*atomic.Pointer[scenarioState]` expected by
   `RunCommitLoop` and the /control handlers.
   Anchor: `scenario.go:40-43` (6194d9d).

## /control Handler Contract

3. **Validate before mutating** — `decodeControlRequest` enforces the
   per-field bounds (`commit_rate > 0`, `rows_per_tx >= 1`, scenario name
   in the registry) and returns 400 BEFORE any state mutation. Partial
   `/control` bodies must never leave the registry, the limiter, or the
   scenarioPtr in a half-updated state.
   Anchor: `server.go:184-212` (6194d9d).

4. **BuildScenario, not Registry()[name], at /control mutation time** —
   the runtime scenario MUST be constructed via `BuildScenario` so the
   operator-supplied `commit_rate` and `rows_per_tx` are baked into the
   scenario's `Tick()` return value. The `Registry()` map carries
   hard-coded baseline rates (steady=100, stress=200, …) used only for
   name enumeration; sourcing the active scenario from it would let the
   scenario-evaluator goroutine re-assert those baselines over the
   operator's override ~100ms later, silently dropping the rate change.
   Anchor: `server.go:242-259`, `scenario.go:100-135` (6194d9d).

5. **Always construct a fresh scenario object on /control apply** — even
   when only `commit_rate` or `rows_per_tx` changed (no scenario name in
   request), `applyControlMutation` builds a new scenario via
   `BuildScenario`. This guarantees `Tick()` honours the new rate; reusing
   the prior scenario object would leak the prior rate.
   Anchor: `server.go:243-272` (6194d9d).

6. **StartedAt preservation on partial update** — when the operator tunes
   only `commit_rate` / `rows_per_tx` (no `scenario` field), the new
   `scenarioState` MUST inherit `prev.StartedAt`. Resetting `StartedAt` on
   a partial update would restart ramp-up progress at 0% and re-trigger
   spike's burst window — surprising side effects of an unrelated knob.
   When the scenario IS being switched, `StartedAt = time.Now()` (the new
   scenario starts fresh).
   Anchor: `server.go:261-270` (6194d9d).

7. **Registry SetActiveScenario reset ordering** — on a scenario *switch*
   the handler calls `deps.Registry.SetActiveScenario(newName)` BEFORE
   `deps.Registry.SetCommitRate(newName, newRate)`. `SetActiveScenario`
   `Reset()`s the entire `commitRate` family so old-scenario series drop
   from the gather output; only then is the new scenario's commit-rate
   gauge re-created.
   Anchor: `server.go:278-288`, `metrics.go:179-188` (6194d9d).

8. **Body size cap before decode** — `http.MaxBytesReader` caps the
   /control body at `controlBodyMaxBytes = 1024`. Strict-mode JSON decode
   is intentionally OFF (unknown fields silently ignored) so future
   forward-compatible knobs (e.g. `burst_factor`) can coexist with older
   binaries.
   Anchor: `server.go:186-197` (6194d9d).

## CORS Contract (mirror of internal/sse.handleCORS)

9. **Vary: Origin set unconditionally when allowlist non-empty** — even on
   a denied Origin, `Vary: Origin` is set so caches never serve a CORS
   response computed for a different origin. ACAO + ACAC are reflected
   only on exact, case-sensitive Origin match.
   Anchor: `server.go:301-330` (6194d9d).

10. **Preflight always 204; ACAM/ACAH only on match** — OPTIONS /control
    returns 204 No Content regardless of Origin. On a match the response
    also emits ACAO + ACA-Methods + ACA-Headers + Max-Age. On a
    non-matching Origin the absent ACAO is the browser's signal to fail
    the preflight.
    Anchor: `server.go:350-360` (6194d9d).

11. **withCORS does NOT block the wrapped handler** — even when the Origin
    is denied, the request body is still processed and the wrapped
    handler returns its normal response. The browser is the gate that
    blocks the JS caller from reading the response (no-credentials fetch
    model).
    Anchor: `server.go:338-343` (6194d9d).

## Commit Loop

12. **Single commit goroutine** — `RunCommitLoop` is the writer's one
    dedicated commit goroutine. It blocks in `waitArrival`, reads
    `scenarioPtr.Load()` on every iteration, calls `commitOnceFn`, and
    dispatches via the nil-safe `onCommit` / `onError` callbacks. Returns
    `ctx.Err()` on cancellation; never returns nil.
    Anchor: `loop.go:162-227` (6194d9d).

13. **commitOnceFn indirection for unit tests** — `var commitOnceFn =
    realCommitOnce` is a package-level seam so tests can substitute
    deterministic success/failure behaviour without standing up a real
    pgxpool.Pool. The `commitOncePool` interface narrows the pool API to
    the single `BeginTx` method commitOnce needs.
    Anchor: `loop.go:36-50` (6194d9d).

14. **tx-commit parity invariant** — `onCommit` is invoked ONLY after
    `tx.Commit` returns nil. A rolled-back tx leaves `writer_tx_total`
    unchanged and instead bumps `writer_errors_total{reason}` via
    `onError`. The ±2% parity envelope between commits and counter
    increments depends on this contract.
    Anchor: `loop.go:209-225`, `metrics.go:147-149` (6194d9d).

15. **Error classification labels** — `classify(err)` returns one of the
    four label values registered on the writer_errors_total CounterVec:
    `pg_constraint` (SQLSTATE class 23), `pg_conn` (net.OpError,
    io.EOF, syscall.ECONNRESET), `pg_other` (everything else including
    other pgconn.PgError codes), or `""` when err is nil. The
    `tx_timeout` label is pre-touched at registry construction but is
    not currently emitted by `classify` itself (cmd/writer would emit it
    from a separate timeout-detection path).
    Anchor: `loop.go:135-159`, `metrics.go:127-131` (6194d9d).

## Arrivals & Rate-Limiter Composition

16. **Limiter is the rate source-of-truth** — `cmd/writer` calls
    `lim.SetLimit` on every Tick from the scenario-evaluator goroutine.
    In DistPoisson mode the same `lim.Limit()` value is the λ that drives
    the exponential inter-arrival sample. The /control handler updates
    `lim.SetLimit` immediately after publishing the new scenarioState so
    the very next `waitArrival` reads the new rate.
    Anchor: `arrivals.go:47-73`, `server.go:275-276` (6194d9d).

17. **Poisson mode does NOT consume a limiter token** — composing the
    `Exp(1)/λ` sleep with `lim.WaitN(ctx, 1)` would double the mean
    inter-arrival to roughly `1.5/λ` (empirically 136ms at λ=10),
    violating the "inter-arrival mean ≈ 1/λ" contract enforced by
    `TestWaitArrival_Poisson_ExponentialShape`. The limiter cap still
    bounds aggregate rate; only the timing of each release is exponential.
    Anchor: `arrivals.go:1-15,47-73` (6194d9d).

18. **Degenerate λ ≤ 0 falls back to limiter wait** — if the limiter rate
    is non-positive (paused, mis-set), `waitArrival` falls back to
    `lim.WaitN(ctx, 1)` rather than dividing by zero.
    Anchor: `arrivals.go:55-58` (6194d9d).

## Pool

19. **Explicit pool bounds — no implicit defaults leak** — `NewPool` sets
    `MaxConns` / `MinConns` from `WriterPoolConfig` (no zero-value
    defaults), `MaxConnIdleTime = 30s` (recycle stale idle conns),
    `MaxConnLifetime = 1h` (rotate before server-side timeouts),
    `MaxConnLifetimeJitter = 5m` (stagger rotations across goroutines).
    Anchor: `pool.go:14-39` (6194d9d).

20. **commitOnce tx timeout fallback** — `commitOnceImpl` enforces
    `cfg.TxTimeout` via `context.WithTimeout` on the per-tx ctx. When
    `cfg.TxTimeout <= 0` it falls back to a 5s safety value defensively;
    `WriterConfig.validate` independently rejects non-positive
    `pg.tx_timeout` so this branch is unreachable in production.
    Anchor: `loop.go:62-68` (6194d9d).

## NextTarget Round-Robin

21. **Round-robin via atomic.Uint64.Add minus 1** — `NextTarget` returns
    `s.Targets[(s.nextIdx.Add(1)-1) % len(s.Targets)]`. The post-increment
    minus one gives a 0-based index so the first call yields `Targets[0]`.
    Empty `Targets` returns "" defensively (the commit loop checks for
    empty and logs `writer commit loop: empty target list` rather than
    panicking the hot path).
    Anchor: `scenario.go:60-69`, `loop.go:202-207` (6194d9d).

## Metrics Registry

22. **Private registry; DefaultRegisterer untouched** — `NewRegistry`
    constructs a private `prometheus.NewRegistry()` and `MustRegister`s
    every metric family + the standard Go runtime + process collectors
    on it. The global `prometheus.DefaultRegisterer` is NOT touched, so
    multiple WriterRegistry instances in tests do not collide.
    Anchor: `metrics.go:55-124` (6194d9d).

23. **SetActiveScenario double-Reset** — both `scenarioGauge.Reset()` and
    `commitRate.Reset()` are invoked on scenario switch. Old-scenario
    series drop from the gather output entirely (cheap `Reset()` vs
    hand-deleting label pairs); the next `SetCommitRate(name, value)`
    re-creates the commit-rate series for the new scenario only.
    Anchor: `metrics.go:179-188` (6194d9d).

24. **Pre-touch known error reasons at construction** — Prometheus does
    not emit CounterVec children until `WithLabelValues` materialises
    them. `NewRegistry` pre-touches `pg_conn`, `pg_constraint`,
    `pg_other`, `tx_timeout` so alert rules and dashboards can reference
    these label values from t=0 without waiting for the first error.
    Anchor: `metrics.go:127-131` (6194d9d).

## Security / PII

25. **PII discipline** — the writer logs scenario name, target table,
    rows count, error class (`classify` label), commit-rate, and the
    operator's `from`/`to` scenario names on /control switches. It MUST
    NEVER log `customer_name`, `sku`, raw JSON bodies, or the `pg.dsn`
    (which carries the Postgres password).
    Anchor: `loop.go:212-217`, `server.go:281-286`, `config.go:18-20`
    (6194d9d).

26. **/control is unauthenticated by design** — testbench-internal only,
    localhost-bound in compose. `http.MaxBytesReader` caps the body at
    1KB; the server has bounded `ReadHeaderTimeout`, `ReadTimeout`,
    `WriteTimeout`, `IdleTimeout`. Operator-controlled testbench scope.
    Anchor: `server.go:14-22,178-181` (6194d9d).

## Config / Env Mapping

27. **WRITER_ prefix → koanf key transform** — env vars are mapped as
    `WRITER_FOO_BAR_BAZ → foo.bar_baz` (strip prefix, lowercase, first
    underscore becomes a dot). `pg.target_tables` and `http.cors_origins`
    are CSV-split into `[]string` (whitespace-trimmed, empty entries
    dropped).
    Anchor: `config.go:117-138` (6194d9d).

28. **Flag overrides only for explicitly-set flags (Visit semantics)** —
    `applyFlagOverrides` calls `flagSet.Visit` (not `VisitAll`) so an
    unprovided flag's default does not stomp an env value. The CLI flag
    name → koanf key map is hard-coded; unknown flag names are silently
    ignored so cmd/writer can add CLI knobs without touching this
    function.
    Anchor: `config.go:177-215` (6194d9d).

29. **Validate fails with full error list** — `validate` accumulates every
    problem into `errs []error` and returns `errors.Join(errs...)` so
    operators fix all config errors in one pass rather than iterating.
    Anchor: `config.go:217-250` (6194d9d).
