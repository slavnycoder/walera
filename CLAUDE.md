<!-- GSD:project-start source:PROJECT.md -->
## Project

**Walera — PostgreSQL CDC over SSE**

Walera is a Go service that streams PostgreSQL row-level changes to clients over Server-Sent Events. A client subscribes to a channel of the form `entity_name:id`; Walera authorizes the subscription against an external auth backend (which returns a per-user whitelist of accessible tables and fields), tails the WAL via `pgoutput` logical replication, and delivers only the relevant changes filtered by the allowed fields. The target audience is internal product teams that need real-time per-entity push without writing bespoke CDC plumbing.

**Core Value:** Authorized, field-filtered, transactionally-atomic delivery of Postgres row changes to ~10,000 concurrent SSE subscribers at ~5,000 tx/s, with no client-visible duplicate or out-of-order events.

### Constraints

- **Tech stack**: Go (latest stable, **1.22+**) — required by spec §0
- **Replication library**: `github.com/jackc/pglogrepl` — `wal2json` and Debezium+Kafka explicitly rejected
- **PostgreSQL**: version ≥ 14, `wal_level = logical`, DBA-owned publication, replication user with `REPLICATION` attribute, no PgBouncer in the replication path
- **Deployment**: single Kubernetes instance; 2 CPU / 4 GiB requests, 4 CPU / 8 GiB limits; `terminationGracePeriodSeconds: 30`; liveness `periodSeconds=2, failureThreshold=3`
- **Concurrency**: must be `-race`-clean; CI enforces; coverage target > 85% lines
- **Performance**: sustain ~5k WAL tx/s and ~10k concurrent SSE subscribers on a single 4-CPU instance
- **Security/PII**: never log row data, tokens, or secrets; PK values are OK (identifiers, not content); wildcards intended for publicly-enumerable entities only
- **Compatibility**: SSE endpoint versioned via URL (`/sse/v1/`); breaking changes go to `/v2/`
- **Backwards compatibility**: none required — greenfield service
- **Future scale-out path**: broadcaster/router interfaces designed for swap-out (NATS / Redis Streams) but the implementation is single-instance only
<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->
## Technology Stack

## Locked Choices — Confirmed
### Core Replication & Database
| Library | Confirmed Version | Purpose | Spec Reference |
|---------|------------------|---------|----------------|
| `github.com/jackc/pglogrepl` | `v0.0.0-20260401131349-e37c41485510` (no semver tags; use latest pseudo-version) | pgoutput logical replication protocol — WAL decoding, `StartReplication`, `StandbyStatusUpdate`, typed `RelationMessage` / `InsertMessage` / `UpdateMessage` / `DeleteMessage` | spec §1, WAL-01 |
| `github.com/jackc/pgx/v5` | `v5.9.2` (Apr 19 2026) | Admin DB connection (non-replication queries: health checks, schema queries). Also provides `pgconn.PgConn` used directly by pglogrepl for the replication connection | spec §1, pglogrepl companion |
| `github.com/jackc/pgx/v5/pgconn` | (part of pgx v5) | Raw `PgConn` that pglogrepl uses for the replication-protocol connection — pglogrepl's own examples import `pgx/v5/pgconn` and `pgx/v5/pgproto3` directly | spec §1 |
### Observability
| Library | Confirmed Version | Purpose |
|---------|------------------|---------|
| `github.com/prometheus/client_golang` | `v1.23.2` (Sep 5 2025) | Prometheus metrics — counters, gauges, histograms for all OBS-01 metrics |
### Testing
| Library | Confirmed Version | Purpose |
|---------|------------------|---------|
| `github.com/testcontainers/testcontainers-go` | `v0.42.0` (Apr 9 2026) | Integration test container lifecycle |
| `github.com/testcontainers/testcontainers-go/modules/postgres` | `v0.42.0` | PostgreSQL container with `wal_level=logical` init scripts, snapshot/restore for fast test resets |
### Runtime Tuning
| Library | Confirmed Version | Purpose |
|---------|------------------|---------|
| `go.uber.org/automaxprocs` | `v1.6.0` (Sep 23 2024) | Reads container CPU quota from cgroups v1/v2 and sets `GOMAXPROCS` correctly; prevents the Go scheduler from using host-CPU count inside a 2/4-CPU k8s pod |
## Open Choices — Recommended Defaults
### Structured Logging: zerolog `v1.35.1`
- **Zero allocations at hot path.** At ~5k WAL tx/s with per-subscriber fan-out, every log
- **Zero-alloc string fields without boxing.** `log.Str("lsn", lsn)` stays on stack.
- **`log/slog` bridge.** zerolog v1.35.0+ ships `zerolog.NewSlogHandler` so third-party
- **Simpler API for contextual loggers.** `log.With().Str("subscriber_id", id).Logger()`
### Config: koanf v2 `v2.3.4`
- **YAML file + env override is the exact pattern.** koanf's provider/parser architecture
- **No global state.** Viper uses package-level globals (`viper.GetString()`). koanf
- **Smaller dependency graph.** Viper pulls in `fsnotify`, `mapstructure`, and several
- **Struct unmarshaling via `mapstructure`.** `k.Unmarshal("", &cfg)` maps the flat
### Token-Bucket Rate Limiter: removed
- **Deprecated and removed.** Per-IP and per-user token-bucket rate limiting is
  delegated to the upstream proxy (traefik / NGINX / ingress) — a load balancer
  can enforce it uniformly across replicas and shed traffic before it consumes
  a Goroutine. In-process, walera only keeps two admission-control primitives:
  - **Global concurrency semaphore** (`limits.AcquireGlobal`, default 50k). A
    buffered `chan struct{}` of `cap=GlobalConcurrent`; on overflow the SSE
    handshake returns 503 + `Retry-After: 5`. Protects against accept-storm
    Goroutine blowup.
  - **Per-user concurrency counter** (`limits.AcquirePerUser`, default 10). An
    `atomic.Int64` per `user_id` in a `sync.Map`; on overflow returns 429.
    Protects against a single compromised token saturating the fan-out.
- **Original spec items LIM-01 (rate limit) / LIM-02 (Retry-After signal)
  were retired.** `golang.org/x/time/rate` is no longer imported by the
  limits package; it remains in `go.mod` only because the writer
  commit-loop uses `WaitN` for transactional pacing.
### xxHash: `cespare/xxhash/v2` v2.3.0
- **Sharded subscription index (ROUTE-02).** The index shards on `xxhash.Sum64String(key)
- **Zero allocation for the hot path.** `xxhash.Sum64String` takes a string directly.
- **Assembly-optimized on amd64/arm64.** The k8s pod runs on amd64; the asm path is
### Circuit Breaker: Hand-rolled FSM (primary) with gobreaker v2 as reference
- **The spec defines custom state semantics.** AUTH-04 requires: ">50% failure rate over
- **The FSM is small.** Three states (Closed / Open / HalfOpen), two transitions
- **On-close broadcast.** A `chan struct{}` closed by the FSM's state transition gives
### Schema Validation Library: None needed
### Build Tooling: golangci-lint v2 `v2.12.2`
- `exhaustive`: The pgoutput message-type switch (`case *pglogrepl.InsertMessageV2`,
- `errcheck`: Every `conn.Write()` and `flusher.Flush()` in the SSE writer must be
- `nilerr`: Common bug pattern in error-wrapping code.
# Install via go install (not go get — it's a tool, not a library dep)
### Hot-Reload for Dev: air `v1.65.1`
## Go Version
- `log/slog` (1.21+) is available — used by zerolog's bridge
- Range-over-integer (1.22+) — syntactic convenience
- `net/http` HTTP/2 server improvements
## Complete Dependency Table
### Production Dependencies
| Import Path | Version | Category | Purpose |
|-------------|---------|----------|---------|
| `github.com/jackc/pglogrepl` | `v0.0.0-20260401131349-e37c41485510` | LOCKED | WAL decoding, pgoutput protocol |
| `github.com/jackc/pgx/v5` | `v5.9.2` | LOCKED | Admin DB pool + `pgconn`/`pgproto3` for replication conn |
| `github.com/prometheus/client_golang` | `v1.23.2` | LOCKED | Prometheus metrics |
| `go.uber.org/automaxprocs` | `v1.6.0` | LOCKED | k8s-correct GOMAXPROCS |
| `github.com/rs/zerolog` | `v1.35.1` | OPEN → zerolog | Structured JSON logging |
| `github.com/knadh/koanf/v2` | `v2.3.4` | OPEN → koanf | Config (YAML + env) |
| `github.com/cespare/xxhash/v2` | `v2.3.0` | CONFIRMED | Sharded index hashing |
| `golang.org/x/time` | `v0.15.0` | OPEN → stdlib-ext | Writer commit-loop `WaitN` pacing (SSE rate limit was removed; see "Token-Bucket Rate Limiter") |
### Test Dependencies
| Import Path | Version | Purpose |
|-------------|---------|---------|
| `github.com/testcontainers/testcontainers-go` | `v0.42.0` | Container lifecycle |
| `github.com/testcontainers/testcontainers-go/modules/postgres` | `v0.42.0` | PostgreSQL container |
### Dev Tools (not go.mod)
| Tool | Version | Purpose |
|------|---------|---------|
| `golangci-lint` | `v2.12.2` | Static analysis |
| `air` | `v1.65.1` | Dev hot-reload |
## What NOT to Use
| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `wal2json` PostgreSQL plugin | Requires PostgreSQL extension installation; unavailable on managed PG (RDS, CloudSQL, Supabase). Spec §1 explicitly rejects it. | `pgoutput` (built-in, no extension) via `pglogrepl` |
| Debezium + Kafka | Adds a Kafka cluster and Debezium JVM process for a service that targets single-instance Go; massively over-engineered for this scale and deployment model. Spec §1 explicitly rejects it. | `pglogrepl` direct logical replication |
| `database/sql` + any ORM (`gorm`, `ent`, `sqlx`) | There is no application-level SQL in the hot path. The admin connection runs at most a handful of queries at startup (health check, schema validation). ORMs add reflection overhead and large dep trees for zero benefit. | `pgx/v5` direct for the 2-3 admin queries |
| Gin / Echo / Chi / Fiber (web frameworks) | The SSE handler has one route (`/sse/v1/{table}/{pk}`) and two health routes. A framework adds 5-10 ms of middleware stack, opinionated request lifecycle, and 100k+ lines of code for what amounts to `http.HandleFunc`. Spec §SSE-01 specifies `net/http` stdlib. | `net/http` stdlib with `http.NewServeMux()` (Go 1.22 pattern syntax) |
| `github.com/spf13/viper` | Global state, `fsnotify` watcher not needed for k8s (restarts handle config changes), heavier transitive dep tree than koanf. | `koanf/v2` |
| `go.uber.org/ratelimit` | AIMD/leaky-bucket — sleeps the caller. Moot anyway; SSE rate limiting is no longer done in-process. | upstream proxy (traefik / NGINX / ingress) |
| `go.uber.org/zap` | Comparable allocation performance to zerolog but more verbose API (explicit `zap.String()` fields everywhere). No allocation advantage over zerolog for this use case; zerolog is simpler for the contextual-logger-per-subscriber pattern. | `zerolog` |
| `github.com/sony/gobreaker` (v1 or v2) for the auth breaker | v1 uses fixed-interval counter reset, not a sliding window. v2 adds bucket-period rolling window but still can't express the "bounded fail-open for existing subs / fail-closed for new opens" posture without external scaffolding. | Hand-rolled FSM in `internal/auth/breaker.go` (~120 lines) |
| Binary pgoutput mode | Spec explicitly defers binary mode to post-MVP. Text mode is sufficient at 5k tx/s on 4 CPUs. Binary decoding adds non-trivial implementation complexity for marginal throughput gain. | `pgoutput` text mode (default in pglogrepl) |
| `pgbouncer` in the replication path | PgBouncer does not support the PostgreSQL replication protocol. The replication connection must connect directly to PostgreSQL. Spec §10.4 explicitly calls this out. | Direct `pgconn` connection to PostgreSQL |
| `Last-Event-ID` resume + persistent replication slot | Persistent slot accumulates WAL during downtime; on restart Walera would replay all accumulated changes to 0 connected subscribers — pure disk waste. Spec §1.4 explicitly rejects this. Walera makes no continuity guarantee across reconnect — clients resync state from the primary API on reconnect (PROJECT.md §Out of Scope). | Temporary slot (`CREATE_REPLICATION_SLOT ... TEMPORARY`) |
## Dependency Introduction Order (Roadmap Note)
## Version Compatibility Notes
| Pair | Status | Note |
|------|--------|------|
| `pglogrepl` + `pgx/v5` | Required companion | pglogrepl imports `pgx/v5/pgconn` and `pgx/v5/pgproto3`; must use pgx v5 (not v4) |
| `zerolog v1.35+` + `log/slog` | Bridge available | `zerolog.NewSlogHandler` routes stdlib slog calls through zerolog; no version conflict |
| `koanf/v2` providers + parsers | Separate modules | Each provider/parser is its own Go module; must `go get` individually at matching versions |
| `testcontainers-go` + `testcontainers-go/modules/postgres` | Same version | Both at v0.42.0; mismatched versions cause API incompatibilities |
| `golangci-lint v2` config | Breaking change from v1 | `.golangci.yml` requires `version: "2"` top-level key; v1 configs silently misbehave |
## Sources
- Context7 `/jackc/pglogrepl` — pgoutput usage, `StartReplication`, `StandbyStatusUpdate` examples; confirms pgx/v5 as required companion
- Context7 `/jackc/pgx` — v5 connection pool patterns
- Context7 `/rs/zerolog` — zero-allocation benchmarks, slog bridge (`NewSlogHandler`)
- Context7 `/knadh/koanf` — YAML + env provider patterns, v2 import paths
- Context7 `/sony/gobreaker` — state machine API; confirmed `Interval` is cyclic reset not sliding window
- Context7 `/cespare/xxhash` — `Sum64String` API, assembly optimization
- Context7 `/golangci/golangci-lint` — v2 config format, linter list
- Context7 `/testcontainers/testcontainers-go` — postgres module, `WithInitScripts`
- pkg.go.dev — version verification for all libraries (accessed 2026-05-14)
- go.dev/doc/devel/release — Go 1.26.3 confirmed as latest stable (May 7 2026)
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
