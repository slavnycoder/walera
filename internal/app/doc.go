// Package app owns the cdc-sse binary's composition root. Every long-lived
// singleton (the zerolog Logger, the *metrics.Registry, the walconn.AdminConn,
// the WAL reader, the SSE WriterPool, the router Broadcaster, the auth Client
// and Breaker, the limits keeper, the health server) is constructed here and
// handed back to the cmd-package as a single *App value. cmd/cdc-sse/main.go
// keeps only the responsibilities that genuinely belong to a binary entry
// point: parsing CLI flags, configuring the logger writer, calling
// app.LoadAppConfig, constructing *App via app.InitializeApp, and blocking on
// (*App).Run / (*App).Shutdown around a signal.NotifyContext.
//
// Cycle prohibition. internal/app sits at the TOP of the import DAG inside
// the repository. No other package under internal/ may import
// "github.com/walera/walera/internal/app" — doing so would introduce an
// upward edge that breaks the layered architecture (every internal/<pkg>
// owns its own typed Config and Deps and is constructed BY app, never the
// reverse). Symmetrically, internal/app MUST NOT import anything under
// cmd/ — the composition root is the upper bound of the internal/ subtree,
// and importing a sibling cmd/ binary would make the package un-reusable
// (any future writer/loadgen binaries, if ever migrated to wire, would
// re-import this same composition root). The Makefile deps-check target
// enforces both rules at CI time.
//
// Composition root. internal/app/initialize.go is the single
// hand-written file that constructs the singleton graph; it reads
// top-to-bottom, declares each long-lived value as one local
// variable, and returns *App plus a cleanup func to cmd/cdc-sse.
// No build tags, no code generation, no provider sets — the
// construction order is exactly the order of the lines. The auth
// cycle (auth.Client ↔ auth.Breaker) appears as three explicit
// sequential lines: authClient := auth.New(...) → breaker :=
// auth.NewBreaker(...) → authClient.SetBreaker(breaker). For the
// design decision behind the hand-wired composition root (and the
// exit-criteria for a future return to code-generated wiring), see
// docs/architecture.md §"Composition Root — Hand-Wired vs Codegen".
//
// Layout. app.go — *App struct and its lifecycle handles.
// runnable.go — the Runnable type shared by lifecycle.go's Run
// loop. lifecycle.go — (*App).Run and (*App).Shutdown (verbatim
// port of the 5-step shutdown sequence from cmd/cdc-sse/main.go's
// pre-extraction runShutdown). initialize.go — the composition
// root: every singleton constructed in order. runnables.go — the
// buildRunnables helper Run iterates. bootstrap.go —
// PrepareDatabase + the PG prereq / publication / slot headroom
// helpers that run once at startup. config.go — the aggregate
// AppConfig + LoadAppConfig. types.go — named-duration wrappers
// (ShutdownDeadline, DrainDeadline) used by ShutdownConfig.
//
// Wirer grouping. InitializeApp dispatches to four grouped helpers that
// partition the dependency graph by lifecycle concern. wireCore constructs
// Logger, metrics.Registry, walconn.AdminConn. wireAuth constructs
// auth.Client + auth.Breaker; the auth cycle break
// (authClient.SetBreaker(breaker)) is encapsulated inside this helper so the
// cycle has exactly one production call site. wireDataPlane constructs the
// WAL reader, router.Broadcaster, sse.WriterPool. wireHTTP constructs the
// health server and registers the /sse mux. Each helper returns its
// constructed values plus any cleanups; the four call sites form a
// top-to-bottom narrative in initialize.go that mirrors the lifecycle order
// in (*App).Run.
//
// Shutdown sequence. (*App).Shutdown runs a fixed 5-step sequence
// (pool drain → pprof close → http shutdown → broadcast drain → final
// cleanup); each step is launched via safego.Go with a per-step bounded
// deadline. The five safego.Go production call sites all live in
// lifecycle.go; see internal/app/INVARIANTS.md §Concurrency for the
// canonical site list.
//
// See also internal/app/INVARIANTS.md for the canonical invariant list.
package app
