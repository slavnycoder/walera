# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Config-allowlisted forwarding of client cookies and headers to the
  auth backend on session open. Two opt-in name allowlists —
  `auth.forwarded_cookies` (`[]string`) and `auth.forwarded_headers`
  (`[]string`) — thread matching credentials from the SSE handshake into
  the `POST /auth/sessions` open call. Empty (the default) forwards
  nothing. Forwarding applies to the open call only, never to the
  HMAC-signed periodic refresh. With an allowlisted cookie or header
  present, the bearer token becomes **optional**: an open is rejected
  `401` (`{"reason":"missing_credentials"}`) only when no credential at
  all is supplied. Cookie names match case-sensitively (RFC 6265),
  header names case-insensitively; reserved headers managed by Walera
  (`Authorization`, `Host`, `Content-Length`, `Content-Type`, `Accept`,
  `Connection`, `Transfer-Encoding`, `Cookie`, `X-Request-Id`,
  `X-Walera-Sig`, `X-Walera-Kid`) can never be forwarded and are
  rejected at startup. Forwarded values are never logged.

## [2.0.1] - 2026-06-02

No functional or API changes — a documentation correction and expanded
security test coverage only.

### Fixed

- **README "Rule 3" (don't mix children of different roots) example.** The
  previous SQL anchored both roots, which actually trips the Rule 1 multi-root
  guard and is dropped — not leaked — so it did not demonstrate a real leak.
  Replaced it with a faithful case (one root anchored once plus a child of a
  different root) and corrected the prose that claimed it leaks "even with each
  root anchored."

### Tests

- Security integration coverage: cross-subscriber field isolation on a shared
  row, case-sensitive whitelist (no column-name normalization bypass),
  no-PII/token in frames or logs, mid-stream field narrowing via refresh,
  wildcard routing confinement, breaker fail-open for existing subscribers, and
  a table-driven handshake-admission suite (400 / 429 / 503 with slot-release
  assertions).
- Auth unit coverage (`-race`): refresh-LSN narrowing-boundary no-leak through
  the real swap path, bounded fail-open with revoke-on-recovery, and concurrent
  grant-swap/drop versus delivery.

## [2.0.0] - 2026-05-28

### Removed (breaking)

- In-process per-IP and per-user **token-bucket rate limiting**. The
  `limits.AllowPreAuthRate` and `limits.AllowPerUserRate` admission
  gates, their `golang.org/x/time/rate` token buckets, the rate-entry
  sweeper, and the associated config + metric surface are gone. Rate
  limiting is now delegated to the upstream proxy (traefik / NGINX /
  ingress), which can apply it uniformly across replicas and shed
  pathological traffic before it consumes a Goroutine.
- Removed config keys (presence in YAML now produces no
  `limits.*` section override — they are no longer recognized; supply
  them and `koanf` will silently ignore them since they map to no
  struct field):
  - `limits.per_user_rate_per_second` / `WALERA_LIMITS_PER_USER_RATE_PER_SECOND`
  - `limits.per_user_burst` / `WALERA_LIMITS_PER_USER_BURST`
  - `limits.pre_auth_rate_per_second` / `WALERA_LIMITS_PRE_AUTH_RATE_PER_SECOND`
  - `limits.pre_auth_burst` / `WALERA_LIMITS_PRE_AUTH_BURST`
  - `limits.sweep_interval` / `WALERA_LIMITS_SWEEP_INTERVAL`
  - `limits.sweep_idle_threshold` / `WALERA_LIMITS_SWEEP_IDLE_THRESHOLD`
- Removed Prometheus metric labels
  `walera_limit_rejected_total{kind="pre_auth_rate"}` and
  `walera_limit_rejected_total{kind="per_user_rate"}`. Existing alerts
  / dashboards referencing them will report no series.
- Removed `limits-sweeper` runnable. The sweeper only existed to GC
  idle rate-bucket entries from `sync.Map`s that no longer exist.

### Changed (breaking)

- SSE handshake gate sequence collapses from six gates to four:
  1. `limits.AcquireGlobal` → 503 + `Retry-After: 5` on overflow.
  2. Bearer present + `authClient.OpenSession` (breaker-gated).
  3. `limits.AcquirePerUser` → 429 on overflow (still in-process —
     a proxy cannot enforce per-`user_id` concurrency without seeing
     the authenticated identity).
  4. Table in `authMap.Tables` → 403 `{"reason":"not_allowed"}`.
- `internal/sse/INVARIANTS.md` invariant 10 rewritten to reflect the
  new sequence.
- `spec/03-subscriptions-and-sse.md` §3.2 handshake list renumbered
  (former step 3 — per-user rate limit — removed).
- `CLAUDE.md` "Token-Bucket Rate Limiter" section rewritten:
  `golang.org/x/time/rate` is no longer in the limits package
  (the writer commit-loop still uses it for `WaitN` pacing, so the
  module stays in `go.mod`).

### Migration

The only operator action required is to **delete** any
`limits.per_user_rate_per_second` / `*_burst` / `*_pre_auth_*` /
`*_sweep_*` keys from your YAML or env. If you needed those caps,
configure equivalent rate limiting at the upstream proxy:

- **traefik**: `RateLimit` middleware keyed on `client.ip` (pre-auth)
  and / or `request.header.X-Forwarded-For`.
- **NGINX**: `limit_req_zone` + `limit_req`.
- **Cloud LB / ingress**: per-vendor rate-limit policy.

Per-user (post-auth) rate limiting cannot move outside Walera because
the proxy does not see `user_id`. If you need it, build it against
`walera_auth_requests_total` exported by Walera + a downstream policy
engine.

### Added

- Optional `initial_data` field in the auth backend's open-time
  permission response. When present, Walera emits its raw JSON value
  to the subscriber as a single `event: initial_data` SSE frame before
  any `tx` events. Payloads exceeding `max_payload_bytes` are dropped
  with a warning; the stream opens normally without the frame. The
  field is opaque to Walera (any JSON value is forwarded verbatim
  after whitespace compaction) and emitted only on the open-time map —
  not re-emitted on permission refresh.

## [1.0.1] - 2026-05-27

### Changed

- Applied `gofmt` to `test/integration/15_large_tx_test.go` (trailing
  newline removed). No functional changes.

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
