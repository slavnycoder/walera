# Operations

This document covers deployment, runtime configuration, observability,
upgrade procedure, and troubleshooting for a Walera pod. For the
project orientation, start with the [README](../README.md). For the
architectural reasoning behind the deployment shape, see
[Architecture](./architecture.md).

## Overview

Walera ships as a single multi-arch container image
(`linux/amd64`, `linux/arm64`) on GitHub Container Registry. The
intended deployment target is a single Kubernetes pod per environment
with the following resources:

- Requests: 2 CPU, 4 GiB memory.
- Limits: 4 CPU, 8 GiB memory.
- `terminationGracePeriodSeconds: 30`.
- Liveness probe `periodSeconds: 2`, `failureThreshold: 3`.

A single pod is designed to sustain ~5,000 WAL transactions per second
and ~10,000 concurrent SSE subscribers. Multi-instance scale-out is
not supported in this revision.

## Deployment

The recommended deployment path is the bundled Kustomize overlay
under `deploy/k8s/`. Docker Compose is provided for evaluation. See
the subsections below for prerequisites and full manifests.

## Container images

Walera ships as a multi-arch container image
(`linux/amd64`, `linux/arm64`) on GitHub Container Registry. The
release pipeline (`.github/workflows/publish-image.yml`) publishes
the following tags on every `v*` tag push:

| Tag                                   | Updated when                       | Production use? |
| ------------------------------------- | ---------------------------------- | --------------- |
| `ghcr.io/<owner>/walera:v2.0.0`       | Pinned at release time; immutable. | **Yes** — recommended. |
| `ghcr.io/<owner>/walera:2.0.0`        | Same release; immutable.           | Yes.            |
| `ghcr.io/<owner>/walera:2.0`          | Floats to the latest 2.0.x patch.  | Acceptable; rebuilds on patch release. |
| `ghcr.io/<owner>/walera:2`            | Floats to the latest 2.x.y minor.  | Not recommended — minor releases may change defaults. |
| `ghcr.io/<owner>/walera:latest`       | Floats to the highest semver tag.  | **NO — not for production.** |

**`:latest` is not for production.** Use an immutable
`v<semver>` tag in Deployment manifests so pod rollouts are
deterministic and rollback is one `kubectl rollout undo` away.
`:latest` exists for local development and CI smoke tests.

Replace `<owner>` with the GitHub organisation / user that owns the
repository (the value is wired into the workflow via
`IMAGE_NAME: ${{ github.repository }}` and resolves at build time).

## PostgreSQL prerequisites

Walera requires PostgreSQL ≥ 14 with logical replication enabled. The
following SQL and configuration must be in place before Walera
starts.

### 1. Enable logical replication

```ini
# postgresql.conf
wal_level             = logical
max_replication_slots = 10
max_wal_senders       = 10
```

A PostgreSQL restart is required after changing `wal_level`. Walera
verifies all three values at startup and refuses to run if they are
misconfigured.

### 2. Create roles

```sql
-- Replication role: reads WAL
CREATE ROLE walera_repl WITH REPLICATION LOGIN PASSWORD 'change-me';
GRANT CONNECT ON DATABASE app TO walera_repl;
GRANT USAGE ON SCHEMA public TO walera_repl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO walera_repl;

-- Admin role: health checks, schema queries
CREATE ROLE walera_admin WITH LOGIN PASSWORD 'change-me';
GRANT CONNECT ON DATABASE app TO walera_admin;
GRANT pg_monitor TO walera_admin;
GRANT SELECT ON pg_publication_tables TO walera_admin;
```

The replication role must have the `REPLICATION` attribute, and the
replication DSN must connect directly to PostgreSQL — PgBouncer does
not support the replication protocol and will reject the connection
with `unsupported startup parameter: replication`.

### 3. Create a publication

```sql
CREATE PUBLICATION cdc_sse_streamer
  FOR TABLE public.orders, public.devices
  WITH (publish = 'insert, update, delete');
```

Use an explicit table list. `FOR ALL TABLES` is supported by
PostgreSQL but is discouraged because it makes the streamed surface
opaque.

For development and testbench environments, Walera can optionally
bootstrap the publication (and the replication / admin roles) on
startup. Production deployments should keep the publication
DBA-owned.

## Kubernetes deployment

Kustomize manifests live under `deploy/k8s/`. The typical workflow is
to bump the image tag in `deploy/k8s/walera-deployment.yaml`, provide
a `Secret` containing the DSNs and auth token, then apply the overlay:

```bash
kubectl apply -k deploy/k8s/
```

The deployment manifest pins resources, the termination grace period,
and the liveness probe to the values listed under
[Overview](#overview):

```yaml
resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 2
  failureThreshold: 3
terminationGracePeriodSeconds: 30
```

If Walera is deployed behind a TLS-terminating ingress, the
`limits.trusted_proxies` configuration must include the CIDR ranges
of trusted proxies so that the right client IP is used for pre-auth
rate limiting. The default empty allowlist treats every external user
as the ingress controller's IP.

```yaml
limits:
  trusted_proxies:
    - "10.0.0.0/8"
    - "172.16.0.0/12"
    - "192.168.0.0/16"
```

CORS origins are configured under `http.cors_origins` and are
canonicalised at startup (scheme and host are lowercased; paths and
trailing slashes are stripped; ports are preserved as typed —
`https://example.com` does not match `https://example.com:443`).

## Docker Compose deployment

A root-level `docker-compose.yml` boots Walera against a fresh
PostgreSQL with `wal_level=logical` and the publication migration
pre-applied. This is suitable for evaluation only — a real auth
backend is still required at the configured `AUTH_BACKEND_URL`.

```bash
git clone https://github.com/slavnycoder/walera.git
cd walera
docker compose up
```

A full local demo stack (PostgreSQL, Walera, a mock auth service, a
load writer, Prometheus, Grafana, and a browser UI) is available
under `testbench/`.

```yaml
# docker-compose.yml (abbreviated)
services:
  postgres:
    image: postgres:16
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=10
      - -c
      - max_wal_senders=10

  walera:
    image: ghcr.io/slavnycoder/walera:latest
    environment:
      WALERA_DATABASE_URL: postgres://walera:secret@postgres:5432/app?sslmode=disable
      WALERA_AUTH_BACKEND_URL: http://auth:9000
      WALERA_AUTH_ALLOW_PLAINTEXT: "1"
    ports:
      - "8080:8080"
    depends_on:
      - postgres
```

## Configuration

Walera reads a YAML configuration file (`--config`) first; values are
then overridden by environment variables prefixed with `WALERA_`.
Configuration loading is driven by koanf v2. Every configuration key
falls into exactly one of three partitions:

- **Required runtime** — must be set; the loader refuses to start
  without them.
- **Operational tuning** — has a safe default; operators may override
  for capacity or latency targets.
- **Development-only** — gated behind a build tag; never read in
  production builds.

### Spec-name to Walera env-var mapping

External specifications sometimes refer to configuration values by
short names (`DATABASE_URL`, `REPLICATION_URL`, ...). Walera namespaces
every env var with the `WALERA_` prefix to avoid collisions in shared
Kubernetes namespaces and to match the koanf transform
(`WALERA_<TOPLEVEL>_<rest>` → `<toplevel>.<rest>` koanf path). The
mapping is:

| Spec name           | Walera env var                       |
| ------------------- | ------------------------------------ |
| `DATABASE_URL`      | `WALERA_DATABASE_URL`                |
| `AUTH_BACKEND_URL`  | `WALERA_AUTH_BACKEND_URL`            |
| `PUBLICATION_NAME`  | `WALERA_WAL_PUBLICATION_NAME`        |
| `LISTEN_ADDR`       | `WALERA_HTTP_ADDR`                   |
| `SLOT_NAME`         | `WALERA_WAL_SLOT_NAME_PREFIX` (the operator sets the prefix; the slot is temporary and the suffix is `<hostname>_<pid>`) |

### Required runtime

The loader refuses to start when any of these are unset. There are no
defaults — every deployment must supply them.

| Env var                       | YAML key                | Purpose                                                              | Safe value example                                                                          |
| ----------------------------- | ----------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `WALERA_DATABASE_URL`         | `database.url`          | Single Postgres DSN — admin connection and the derived replication connection. Direct to PG (no PgBouncer). | `postgres://walera:secret@db:5432/app?sslmode=require`                                       |
| `WALERA_AUTH_BACKEND_URL`     | `auth.backend_url`      | Auth backend base URL (https).                                       | `https://auth.example.com`                                                                  |

The single role in `WALERA_DATABASE_URL` MUST hold the `REPLICATION`
attribute. Walera derives the replication connection by adding
`replication=database` to this base URL at load time (preserving `sslmode`
and any other query params). If the base URL already carries a `replication`
parameter, Walera strips it from the admin connection and sets the derived
replication connection to `replication=database`. Config validation only
checks that the base URL parses — a role lacking the `REPLICATION` attribute
is therefore NOT caught at config load; it fails at RUNTIME when
`START_REPLICATION` is issued against PostgreSQL.

Note: `WALERA_WAL_PUBLICATION_NAME` and `WALERA_HTTP_ADDR` are listed in
the spec-name mapping above but ship with safe defaults (`walera_pub`
and `:8080` respectively) and are therefore documented under Operational
tuning rather than Required runtime. Production deployments should
override both explicitly even though the loader does not refuse to
start without them.

### Operational tuning

Each row has a safe default. Operators override only when reshaping
capacity, latency, or rate-limit budgets for a specific deployment.

#### `wal.*` — replication

| Env var                                       | YAML key                                  | Default       | Purpose                                                              | Safe value example   |
| --------------------------------------------- | ----------------------------------------- | ------------- | -------------------------------------------------------------------- | -------------------- |
| `WALERA_WAL_PUBLICATION_NAME`                 | `wal.publication_name`                    | `walera_pub`  | PostgreSQL publication consumed by `pgoutput`.                       | `cdc_sse_streamer`   |
| `WALERA_WAL_SLOT_NAME_PREFIX`                 | `wal.slot_name_prefix`                    | `walera`      | Prefix for the temporary replication slot.                           | `walera`             |
| `WALERA_WAL_SLOT_HEADROOM_MIN`                | `wal.slot_headroom_min`                   | `2`           | Free-slots threshold for the startup warning.                        | `2`                  |
| `WALERA_WAL_NAIVE_TIMESTAMP_ASSUME_UTC`       | `wal.naive_timestamp_assume_utc`          | `true`        | Interpret naive TIMESTAMP as UTC.                                    | `true`               |
| `WALERA_WAL_BOOTSTRAP_MODE`                   | `wal.bootstrap.mode`                      | `auto`        | Publication bootstrap policy: `auto`, `verify`, or `off`.            | `verify`             |
| `WALERA_WAL_BOOTSTRAP_TABLES`                 | `wal.bootstrap.tables`                    | `[]`          | Schema-qualified tables when `bootstrap.mode=auto`.                  | `public.orders,public.invoices` |
| `WALERA_WAL_BOOTSTRAP_CREATE_ROLES`           | `wal.bootstrap.create_roles`              | `false`       | Create `walera_repl`/`walera_admin` roles (dev convenience).         | `false`              |
| `WALERA_WAL_RECONNECT_RESET_AFTER_SUCCESS_DURATION` | `wal.reconnect.reset_after_success_duration` | `60s`   | Healthy-run duration that resets the attempt counter.                | `60s`                |
| `WALERA_WAL_LAG_SAMPLE_INTERVAL`              | `wal.lag_sample_interval`                 | `5s`          | Cadence for `pg_wal_lsn_diff` polling.                               | `5s`                 |

#### `auth.*` — auth client / circuit breaker

| Env var                                       | YAML key                                  | Default   | Purpose                                                          | Safe value example |
| --------------------------------------------- | ----------------------------------------- | --------- | ---------------------------------------------------------------- | ------------------ |
| `WALERA_AUTH_DEFAULT_TTL_SECONDS`             | `auth.default_ttl_seconds`                | `60`      | Default permission-map TTL when the backend omits `ttl_seconds`. | `60`               |
| `WALERA_AUTH_HEALTH_CHANNEL`                  | `auth.health_channel`                     | `_health` | Channel name used by the background auth probe.                  | `_health`          |
| `WALERA_AUTH_REQUEST_TIMEOUT`                 | `auth.request_timeout`                    | `2s`      | Cap on every HTTP call to the auth backend.                      | `2s`               |
| `WALERA_AUTH_BREAKER_WINDOW_BUCKETS`          | `auth.breaker.window_buckets`             | `30`      | Rolling-window bucket count for failure-rate calc.               | `30`               |
| `WALERA_AUTH_BREAKER_BUCKET_SECONDS`          | `auth.breaker.bucket_seconds`             | `1`       | Bucket duration in seconds.                                      | `1`                |
| `WALERA_AUTH_BREAKER_FAILURE_RATE_THRESHOLD`  | `auth.breaker.failure_rate_threshold`     | `0.5`     | Failure ratio that opens the breaker after the floor.            | `0.5`              |
| `WALERA_AUTH_BREAKER_DEBOUNCE_FLOOR`          | `auth.breaker.debounce_floor`             | `20`      | Minimum sample size before the threshold can fire.               | `20`               |
| `WALERA_AUTH_BREAKER_COOLDOWN`                | `auth.breaker.cooldown`                   | `30s`     | Open → HalfOpen transition delay.                                | `30s`              |
| `WALERA_AUTH_BREAKER_STALE_REFRESH_JITTER`    | `auth.breaker.stale_refresh_jitter`       | `5s`      | Random jitter on background refresh scheduling.                  | `5s`               |
| `WALERA_AUTH_ALLOW_PLAINTEXT`                 | (env-only)                                | unset     | Dev escape hatch — set to `1` to allow `http://` backends.       | unset              |

#### `router.*` — subscriber indexes and fan-out

| Env var                                         | YAML key                                  | Default   | Purpose                                              | Safe value example |
| ----------------------------------------------- | ----------------------------------------- | --------- | ---------------------------------------------------- | ------------------ |
| `WALERA_ROUTER_EXACT_BUFFER`                    | `router.exact_buffer`                     | `64`      | Per-subscriber buffer for exact subscriptions.        | `64`               |
| `WALERA_ROUTER_WILDCARD_BUFFER`                 | `router.wildcard_buffer`                  | `512`     | Per-subscriber buffer for wildcard subscriptions.     | `512`              |
| `WALERA_ROUTER_MAX_CHANGES_PER_TX`              | `router.max_changes_per_tx`               | `10000`   | Cap on matched changes per tx (all subscriber classes). | `10000`          |
| `WALERA_ROUTER_HEARTBEAT_INTERVAL`              | `router.heartbeat_interval`               | `15s`     | SSE heartbeat (`:\n\n`) cadence.                      | `15s`              |

#### `limits.*` — admission control

| Env var                                        | YAML key                                  | Default   | Purpose                                              | Safe value example |
| ---------------------------------------------- | ----------------------------------------- | --------- | ---------------------------------------------------- | ------------------ |
| `WALERA_LIMITS_GLOBAL_CONCURRENT`              | `limits.global_concurrent`                | `50000`   | Cap on in-flight SSE handshakes (pre-auth).          | `50000`            |
| `WALERA_LIMITS_PER_USER_CONCURRENT`            | `limits.per_user_concurrent`              | `10`      | Max simultaneous SSE streams per user.               | `10`               |
| `WALERA_LIMITS_TRUSTED_PROXIES`                | `limits.trusted_proxies`                  | `[]`      | CIDR allowlist for honouring `X-Forwarded-For`.      | `10.0.0.0/8`       |

Walera intentionally exposes only concurrency caps. Per-IP and per-user
token-bucket rate limiting belongs at the upstream proxy (traefik,
NGINX, an ingress controller, etc.) where it can apply uniformly across
replicas and shed pathological traffic before it consumes a Goroutine.

#### `http.*` — SSE listener and writer pool

| Env var                                  | YAML key                                  | Default          | Purpose                                              | Safe value example       |
| ---------------------------------------- | ----------------------------------------- | ---------------- | ---------------------------------------------------- | ------------------------ |
| `WALERA_HTTP_ADDR`                       | `http.addr`                               | `:8080`          | SSE listen address (host optional, port required).   | `:8080`                  |
| `WALERA_HTTP_CORS_ORIGINS`               | `http.cors_origins`                       | `[]`             | Comma-separated CORS allowlist.                      | `https://app.example.com`|
| `WALERA_HTTP_MAX_PAYLOAD_BYTES`          | `http.max_payload_bytes`                  | `10485760` (10 MiB) | Max serialised SSE payload per event.             | `10485760`               |
| `WALERA_HTTP_WRITE_TIMEOUT`              | `http.write_timeout`                      | `5s`             | Per-frame write deadline.                            | `5s`                     |
| `WALERA_HTTP_MAX_HEADER_BYTES`           | `http.max_header_bytes`                   | `16384`          | Max request-header byte size (stdlib pre-handler).   | `16384`                  |
| `WALERA_HTTP_H2C_ENABLED`                | `http.h2c_enabled`                        | `true`           | Toggle unencrypted HTTP/2 (h2c, prior-knowledge).    | `true`                   |
| `WALERA_HTTP_PPROF_ADDR`                 | `http.pprof_addr`                         | (unset/disabled) | Optional opt-in pprof listener (loopback default).   | `127.0.0.1:6060`         |
| `WALERA_HTTP_POOL_FACTOR`                | `http.pool_factor`                        | `2`              | Writer-pool worker multiplier (× GOMAXPROCS).        | `2`                      |
| `WALERA_HTTP_SUB_QUEUE_SIZE`             | `http.sub_queue_size`                     | `32`             | Per-subscriber `chan []byte` capacity.               | `32`                     |
| `WALERA_HTTP_MAX_WAIT_MS`                | `http.max_wait_ms`                        | `2`              | Writer-pool batch lag ceiling (ms).                  | `2`                      |
| `WALERA_HTTP_DRAIN_THRESHOLD_SUBS`       | `http.drain_threshold_subs`               | `0` (auto)       | Dirty-subscriber count that forces an immediate drain. | `0`                    |
| `WALERA_HTTP_MAX_BATCH_BYTES_PER_SUB`    | `http.max_batch_bytes_per_sub`            | `65536`          | Per-subscriber buffered-frame byte cap.              | `65536`                  |
| `WALERA_HTTP_BATCHING_DISABLED`          | `http.batching_disabled`                  | `false`          | Drain every writer-pool cycle.                       | `false`                  |
| `WALERA_PPROF_ALLOW_PUBLIC`              | (env-only override)                       | unset            | Allow non-loopback pprof listener (dev only).        | unset                    |

#### `log.*`, `health.*`, `shutdown.*`

| Env var                              | YAML key                       | Default | Purpose                                              | Safe value example |
| ------------------------------------ | ------------------------------ | ------- | ---------------------------------------------------- | ------------------ |
| `WALERA_LOG_LEVEL`                   | `log.level`                    | `info`  | Logger minimum level.                                | `info`             |
| `WALERA_LOG_DEV_MODE`                | `log.dev_mode`                 | `false` | Human-readable console writer (stderr).              | `false`            |
| `WALERA_HEALTH_READYZ_PROBE_INTERVAL`| `health.readyz_probe_interval` | `5s`    | Cadence for the background readyz prober.            | `5s`               |
| `WALERA_SHUTDOWN_DEADLINE`           | `shutdown.deadline`            | `10s`   | Hard cap on the entire shutdown sequence.            | `10s`              |
| `WALERA_SHUTDOWN_DRAIN_DEADLINE`     | `shutdown.drain_deadline`      | `8s`    | Broadcaster drain deadline (`< shutdown.deadline`).  | `8s`               |

### Development-only

| Env var                          | YAML key | Purpose                                                                                          |
| -------------------------------- | -------- | ------------------------------------------------------------------------------------------------ |
| _none_                           | —        | No dev-only env vars are recognised today. Reserved name patterns (refused by `config.Load` in production builds) are `WALERA_EXPERIMENTAL_*`, `WALERA_DEBUG_FORCE_*`, `WALERA_PLAN_*`. See `internal/config/dev_guard.go` for the runtime guard and `make config-check` for the static check. |

## Observability

Walera exposes Prometheus metrics on `GET /metrics` and standard
health endpoints suitable for Kubernetes probes.

| Endpoint        | Purpose                                                |
| --------------- | ------------------------------------------------------ |
| `GET /healthz`  | Liveness. `200` when the WAL reader is connected.      |
| `GET /readyz`   | Readiness. `200` when PostgreSQL and auth are healthy. |
| `GET /metrics`  | Prometheus metrics.                                    |

Key metrics surfaced for operations:

- **WAL lag.** Histogram of the gap between commit time and SSE
  fan-out time, plus the slot lag reported by PostgreSQL.
- **Subscriber count.** Gauge of currently-connected SSE subscribers.
- **Auth failures.** Counter of auth-backend errors, partitioned by
  status code and whether the breaker was open.
- **Fan-out latency.** Histogram of per-event broadcast latency from
  WAL decode to writer enqueue.
- **Slow-client drops.** `walera_slow_client_drops_total` (unlabelled
  counter) — total subscribers dropped because the bounded per-client
  send queue could not keep pace with WAL. Pairs with
  `walera_subscriber_disconnects_total{reason="slow_consumer"}` (the
  labelled disconnect-by-reason surface); both counters move in lockstep
  on every slow-client drop. Use the unlabelled counter for the
  spec-named alert path and the labelled counter when correlating
  against other disconnect reasons.
- **Replication slot state.** Gauge for slot connection state and
  current WAL position.

These metrics are the operational contract — alerting and dashboards
should be built against them.

## Replication slot policy

Walera uses a temporary replication slot
(`CREATE_REPLICATION_SLOT ... TEMPORARY`). The slot is created when
the replication connection opens and is dropped automatically when
the connection closes. Walera does not maintain a persistent slot
across restarts.

This policy is deliberate: a persistent slot would accumulate WAL on
the PostgreSQL side during any Walera downtime, and on restart Walera
would replay accumulated changes to zero connected subscribers —
which provides no value (subscribers resync via the primary API per
the delivery-semantics contract) while consuming Postgres disk.

For the full rationale, see
[ADR 0004: Replication Slot Policy](./adr/0004-replication-slot-policy.md).

## Slow client policy

Each SSE subscriber has a small bounded send queue. A subscriber that
cannot drain at WAL pace is disconnected rather than buffered
indefinitely. The disconnect is explicit: the client receives a
connection close and is expected to reconnect and resync via the
primary API.

This trades a bounded amount of slow-client churn against a
predictable per-subscriber memory footprint, which is necessary to fit
the 10,000-subscriber target inside the pod's memory limit.

The slow-client disconnect rate is exposed as the Prometheus counter
`walera_slow_client_drops_total` (and, redundantly,
`walera_subscriber_disconnects_total{reason="slow_consumer"}` — the two
counters move in lockstep) and should be monitored. A sustained
increase indicates either a client-side regression (subscribers that
can no longer keep up) or a WAL pace that has outgrown the
per-subscriber bandwidth budget.

For the full rationale, see
[ADR 0003: Slow Client Policy](./adr/0003-slow-client-policy.md).

## Upgrade

Walera supports rolling restarts as the standard upgrade
mechanism. The expected sequence for a Kubernetes deployment:

```bash
kubectl -n walera set image deployment/walera \
  walera=ghcr.io/<owner>/walera:v2.0.0
kubectl -n walera rollout status deployment/walera --timeout=2m
```

> **Replication slot.** The replication slot is **temporary** (see
> [ADR 0004: Replication Slot Policy](./adr/0004-replication-slot-policy.md)).
> During the rolling restart, the old pod's replication connection
> closes and the slot is dropped automatically; the new pod creates
> a fresh slot and begins streaming. Connected clients receive a
> `shutdown` event from the old pod before the connection closes,
> observe a brief cutover gap, reconnect, and resync via their
> primary API. There is no migration step between versions of Walera
> itself — state lives entirely in PostgreSQL (the publication) and
> the auth backend.

Per-version upgrade notes are tracked in
[CHANGELOG.md](../CHANGELOG.md).

## Rollback

If an upgrade exposes a regression, roll back to the previous
revision:

```bash
kubectl -n walera rollout undo deployment/walera
kubectl -n walera rollout status deployment/walera --timeout=2m
```

> **Replication slot.** The rolled-back pod opens a fresh
> replication connection and creates a new temporary slot — same
> semantics as the upgrade above (see
> [ADR 0004: Replication Slot Policy](./adr/0004-replication-slot-policy.md)).
> Connected clients reconnect automatically over the same brief
> cutover gap.

For older revisions (more than one back), use:

```bash
kubectl -n walera rollout history deployment/walera
kubectl -n walera rollout undo deployment/walera --to-revision=<N>
```

If the rollback target has known incompatibilities with the current
PostgreSQL publication or auth backend, consult
[CHANGELOG.md](../CHANGELOG.md) for migration notes before
rolling back.

## Release process

Walera publishes a GitHub Release on every `v*` tag push via
[`.github/workflows/release.yml`](../.github/workflows/release.yml).
The release notes are extracted directly from `CHANGELOG.md` by
matching the `## [<version>]` heading. To prevent the common mistake
of tagging a release before the CHANGELOG date is filled in, the
workflow refuses to publish if the extracted section still contains
the literal placeholder `YYYY-MM-DD`.

### CHANGELOG heading format

Between releases, the next-version entry in `CHANGELOG.md` is drafted
with a literal `YYYY-MM-DD` date placeholder:

```markdown
## [Unreleased]

<!-- Update YYYY-MM-DD to the v2.0.0 tag date before publishing the release. -->
## [2.0.0] - YYYY-MM-DD

### Added
- ...
```

Immediately before tagging, the operator substitutes the placeholder
with the actual ISO-8601 date the tag is being pushed (NOT the date
the entry was originally drafted):

```markdown
## [Unreleased]

## [2.0.0] - 2026-05-23

### Added
- ...
```

The substitution is literal text: `YYYY-MM-DD` → today's date in
`YYYY-MM-DD` form. Both the HTML reminder comment and the section
header must be updated — the guard reports a hit on either line.

### Cut a release

1. Pick the next semver version `X.Y.Z` and the tag date (today, in
   ISO-8601 `YYYY-MM-DD` form, e.g. `2026-05-23`).
2. Open `CHANGELOG.md`. Find the `## [X.Y.Z] - YYYY-MM-DD` line under
   `## [Unreleased]`. Replace the literal `YYYY-MM-DD` with the
   concrete date. Remove (or update) the
   `<!-- Update YYYY-MM-DD ... -->` reminder comment immediately
   above the heading.
3. (Optional, recommended) Run the guard locally:
   ```bash
   make check-changelog-date
   ```
   Expected output: `check-changelog-date: OK (no YYYY-MM-DD placeholder in CHANGELOG.md)`.
   If the guard exits 1, fix the offending line(s) and re-run.
4. Commit the CHANGELOG edit and push to `master`:
   ```bash
   git add CHANGELOG.md
   git commit -m "docs(changelog): set release date for vX.Y.Z"
   git push origin master
   ```
5. Push the tag:
   ```bash
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   git push origin vX.Y.Z
   ```
6. Watch `Actions → Release` for the workflow run. On success, the
   run uploads the extracted CHANGELOG section as the GitHub Release
   body for `vX.Y.Z`. The parallel `publish-image.yml` workflow
   produces the matching multi-arch container images on the same
   trigger (see [Container images](#container-images)).

### Fail-fast behavior

If a `v*` tag is pushed against a CHANGELOG section that still
contains the literal `YYYY-MM-DD` placeholder, the
`Extract CHANGELOG section` step in `release.yml` aborts with a
GitHub Actions annotation of the form:

```
::error file=$RUNNER_TEMP/changelog-section.md,line=3,title=CHANGELOG date placeholder::Literal "YYYY-MM-DD" placeholder found at line 3. Update CHANGELOG.md before pushing the v* tag.
::error::CHANGELOG section for [X.Y.Z] still contains the YYYY-MM-DD placeholder. Update CHANGELOG.md with the release date before pushing the v* tag.
```

The line number refers to the line within the extracted
`## [X.Y.Z]` section (not the absolute line number in `CHANGELOG.md`);
the corresponding line in the source file is whichever line of the
`## [X.Y.Z]` section the annotation points at.

> **Note.** The bad tag remains in the repository after the workflow
> aborts. To recover, update `CHANGELOG.md` on `master` with the
> real date, then delete and re-push the tag:
>
> ```bash
> git tag -d vX.Y.Z
> git push origin :refs/tags/vX.Y.Z
> # ... after CHANGELOG fix is on master ...
> git tag -a vX.Y.Z -m "Release X.Y.Z"
> git push origin vX.Y.Z
> ```
>
> Deleting the remote tag is easy to miss — without it, the
> re-pushed tag will not trigger a fresh workflow run.

### Where the guard lives

| Surface | Path | Role |
|---------|------|------|
| Guard script | [`scripts/check-changelog-date.sh`](../scripts/check-changelog-date.sh) | Bash + `grep -nF`; scans for the literal `YYYY-MM-DD` substring and emits one line-numbered `::error` annotation per hit. |
| Release-time gate | [`.github/workflows/release.yml`](../.github/workflows/release.yml) (`Extract CHANGELOG section` step) | Calls the guard against the extracted CHANGELOG section on every `v*` tag push. |
| Continuous gate | [`.github/workflows/checks.yml`](../.github/workflows/checks.yml) (`changelog-date-test` job) | Runs the guard's shell-level unit test on every PR and every push to `master`, so the enforcement cannot be silently disabled (REQUIREMENTS.md RELEASE-02 SC2). |
| Local entry point | `make check-changelog-date` / `make check-changelog-date-test` | Same guard + same unit test, invokable locally. |

## Troubleshooting

| Symptom                                            | Fix                                                                                          |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `auth.backend_url is required`                     | Set `WALERA_AUTH_BACKEND_URL`.                                                               |
| Browser CORS error                                 | Set `WALERA_HTTP_CORS_ORIGINS` to the exact frontend origin (including port).                |
| Native `EventSource` cannot authenticate           | Use `@microsoft/fetch-event-source`, a backend proxy, or cookie auth.                        |
| `unsupported startup parameter: replication`       | Replication DSN is going through PgBouncer. Connect directly to PostgreSQL.                  |
| Stream closes with `auth_revoked`                  | Auth backend revoked permissions on refresh. Check the token and the returned `tables` map.  |
| Stream closes with `tx_too_large`                  | Serialized event exceeded `http.max_payload_bytes`, or too many changes for this subscriber. |
| Repeated slow-client drops on otherwise healthy pod | Investigate downstream network or client-side render bottleneck; raise the per-subscriber queue only as a last resort. |

## See also

- [Architecture](./architecture.md)
- [ADR 0003: Slow Client Policy](./adr/0003-slow-client-policy.md)
- [ADR 0004: Replication Slot Policy](./adr/0004-replication-slot-policy.md)
