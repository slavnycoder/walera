# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-05-25

Initial public release of Walera: PostgreSQL CDC over SSE with
authorization refresh, routing fan-out, slow-client handling, Prometheus
metrics, health endpoints, container publishing, and deployment
documentation.

### Added

- Phase 1: root-level `LICENSE` file (MIT) and `docs/licenses.md`
  enumerating every direct dependency with its SPDX identifier. The
  published container image embeds `org.opencontainers.image.licenses=MIT`
  in its OCI metadata.
- Phase 3: long-form architecture documentation under `docs/` —
  `architecture.md`, `auth.md`, `delivery-semantics.md`,
  `operations.md` — plus four foundational ADRs under `docs/adr/`
  (delivery semantics, auth model, slow-client policy, replication-slot
  policy).
- Phase 6: `make deps-check` Makefile target that enforces the
  directional `internal/` import graph. Any back-edge between
  packages fails the build with a named violation message.
- Phase 8: `## Configuration` section in `docs/operations.md`
  partitioning every `WALERA_*` env var into Required runtime /
  Operational tuning / Development-only tables with safe defaults and
  per-field validation rules.
- Phase 8: `make config-check` Makefile target that fails when a
  production-build source file declares an `EXPERIMENTAL_` /
  `DEBUG_FORCE_` / `PLAN_` env var without the `dev` build tag.
- Phase 9: new Prometheus counter `walera_slow_client_drops_total`
  documented in `docs/operations.md` Metrics section. Increments
  alongside the existing `walera_subscriber_disconnects_total{reason="slow_consumer"}`
  counter on every slow-client drop.
- Phase 9: `make sse-stress` Makefile target running
  `go test -race -count=100 ./internal/sse` against an expanded
  `TestPoolSlowClientIsolationStress` covering N=100 mixed
  healthy/stalled/disconnected subscribers.
- Phase 10: `make wal-stress` Makefile target running
  `go test -race -count=10 ./internal/wal`.
- Phase 10: integration tests covering publication / temporary-slot
  lifecycle (`test/integration/14_slot_lifecycle_test.go`) and
  bounded-memory behaviour for a 10k-row transaction
  (`test/integration/15_large_tx_test.go`).
- Phase 11: `CHANGELOG.md` (this file) and the
  `.github/workflows/release.yml` workflow that publishes a GitHub
  Release with CHANGELOG-derived notes on every `v*` tag push.
- Phase 11: `.github/workflows/checks.yml` PR gate workflow with
  parallel `format`, `vet`, `vulncheck`, `test-race`, `deps-check`,
  and `config-check` jobs.
- Phase 11: `.github/workflows/flake-detect.yml` scheduled workflow
  running `make sse-stress`, `make wal-stress`, and a router-package
  stress in parallel daily at 06:00 UTC. On failure, the workflow
  opens an issue labelled `flake` with a link to the failing run.
- Phase 11: `## Container images`, `## Upgrade`, and `## Rollback`
  sections in `docs/operations.md` with concrete kubectl commands and
  a prominent "`:latest` is NOT for production" warning.

### Changed

- Phase 2: comment hygiene pass across the codebase — removed stale
  planning-prose comments, restored Go doc-comment conventions on
  exported identifiers, removed phase / plan citations from shipped
  source files.
- Phase 3: `README.md` rewritten in place — reduced from 761 lines to
  407 lines, locked H2 section order, cross-links to every `docs/*`
  and `docs/adr/*` artifact. No content removed; long-form material
  relocated to `docs/`.
- Phase 4: every exported identifier under `internal/` audited
  against the API-surface test (>1 caller or boundary-crossing).
  Zero-caller exports removed; redundant accessor methods retired.
  The exported surface of every `internal/` package shrunk without
  any production-call-site changes.
- Phase 5: `internal/sse/pool.go` (1429 lines) split into focused
  pipeline-stage files (`pool.go`, `drain.go`, `attach.go`, `errors.go`)
  with every file under the 600-line cap. `internal/wal/assembly.go`
  renamed to `decode.go`; slot bootstrap extracted into
  `internal/wal/slot.go`.
- Phase 6: `internal/config` reduced to a primitives-only package
  exposing `LoadKoanf(path, applyDefaults)`. Each `internal/<pkg>`
  now owns its own `LoadConfig(*koanf.Koanf) (Config, error)` plus
  `ApplyDefaults(*koanf.Koanf)` companion. The aggregate `AppConfig`
  lives in `cmd/cdc-sse/config.go`. `internal/config` no longer
  imports any sibling `internal/*` package.
- Phase 6: `internal/health` declares its own consumer-owned
  `PgChecker` / `AuthChecker` interfaces; the concrete `Health` and
  `CheckAuth` methods live on `*wal.Reader` / `*auth.Client`. The
  health package no longer imports `internal/wal` or `internal/auth`.
- Phase 7: every heavy constructor under `internal/` and `cmd/`
  reshaped to a uniform `New(cfg Config, deps Deps)` signature with
  nil-checked required `Deps` fields. The migration is wire-compatible
  — every callsite was updated atomically.
- Phase 7: `cmd/cdc-sse/main.go` reduced from 799 lines to 611 lines
  (with `func main()` body down from 706 to 288 lines); bootstrap
  helpers relocated to `cmd/cdc-sse/bootstrap.go`.
- Phase 8: per-package `LoadConfig` now layers schema validation
  (DSN/URL parseable, port range, non-negative durations, identifier
  regex) and cross-package combination validation (`per_user_burst >=
  per_user_rate_per_second`, `breaker.cooldown >= breaker.probe_timeout`,
  `http.write_timeout >= http.read_timeout`, etc.). Errors follow the
  format `config: <KEY_OR_PAIR> (<value>) <problem>; <remediation>`
  with DSN passwords redacted.
- Phase 9: `internal/sse` and `internal/wal` test suites now run
  under `go.uber.org/goleak` via `TestMain` — any goroutine leak in
  the SSE or WAL lifecycle fails the test run.
- Phase 9: every `chan` declaration in `internal/sse` production code
  carries a four-field doc-comment block (owner / closer / publishers
  / post-close) on a `// ch <name>:` grep anchor.
- Phase 10: `internal/wal/slot.go` carries an inline doc comment at
  the slot-create call site cross-referencing ADR 0004. The
  temporary-slot policy is now locked end-to-end by integration test.
- Phase 11: every PR runs the `Checks` workflow gating on format /
  vet+staticcheck / govulncheck / `go test -race ./...` /
  `make deps-check` / `make config-check`. Tag pushes (`v*`) produce
  both the published Docker image (existing behaviour) and a published
  GitHub Release with notes extracted from this file.

### Fixed

- Phase 5: the `internal/sse` 1429-line `pool.go` no longer hides
  worker-pickup logic inline; the new `pickWorker` helper makes the
  `xxhash.Sum64String(subID) % poolSize` mapping a single auditable
  function.
- Phase 6: removed an import cycle that previously forced
  `internal/config` to know about every consumer's config shape.
  Adding a new `internal/<pkg>` no longer requires editing
  `internal/config`.

### Removed

- Phase 1: every remaining `License: TBD` marker in shipped artifacts
  (README, OCI labels, `.planning/PROJECT.md`, `.planning/STATE.md`).
- Phase 4: zero-caller exported symbols under `internal/metrics`
  (`SubscriberQueueDepth` accessor), `internal/config`
  (`WALConfig.SlotName` duplicate of `wal.Config.SlotName`), and
  others identified by the per-package audit. No production callsite
  was affected.
- Phase 6: `internal/config`'s imports of every sibling `internal/*`
  package. The package is now leaf-only by design.
- Phase 7: positional-argument constructor overloads — `NewHandler`
  no longer accepts 11 positional arguments; `NewPool`, `NewBreaker`,
  `NewSubscriber`, `NewSubscribers`, `router.New`, `wal.New`,
  `limits.New`, `health.New`, `writer.NewServer`, and `auth.New` all
  follow the `(Config, Deps)` shape.
