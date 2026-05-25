# Walera Testbench

Local docker-compose stack that streams PostgreSQL row-level changes (`pgoutput`
logical replication) to a browser over Server-Sent Events. Single-machine, no
external services, no node_modules — eight containers wired into one
`testbench-net` bridge. See `.planning/REQUIREMENTS.md`
(`BENCH-*` / `SCHEMA-*` / `MOCK-*` / `OBS-*` / `DOC-*`) for the v1.1 spec.

## Hardware floor (read this first)

> **DOC-03.** This bench claims SLO numbers only at `cpus: 4.0 / memory: 8G`
> limits (the `walera` service block in `docker-compose.yml`). Numbers measured
> above that ceiling on an unconstrained laptop are NOT representative of
> production capacity. The `writer` service is intentionally uncapped — it is
> the load generator, not the system under test; capping the generator would
> distort the SLO claims that are made FROM walera under that generator's load.

Production walera runs in a Kubernetes pod with `2 CPU / 4 GiB` requests and
`4 CPU / 8 GiB` limits (spec §0). The testbench mirrors the upper bound so the
testbench is the most permissive environment in which a production SLO regression
will still be visible.

## Quick start

```bash
cp .env.example .env        # one-time, dev-only values
make demo-up                # build + start the whole stack in detached mode
```

Then open:

- **Demo UI** —          http://localhost:8081
- **Grafana** —          http://localhost:3000 (anonymous Viewer; no login)
- **Prometheus** —       http://localhost:9090 (`/targets`, `/rules`, `/alerts`)
- **Walera /metrics** —  http://localhost:8080/metrics
- **Writer /metrics** —  http://localhost:9100/metrics

Cold-start to first-tx is typically 30–60s on a warm Docker host (longer on
first-pull). If `make demo-up` returns before the stack is fully healthy, use
`make demo-logs` to inspect which service is still flapping (see
[Troubleshooting](#troubleshooting)).

## Architecture

```
   +----------+        +-----------+         +-------------+         +----------+
   | postgres | <----- | walera    | <-----  | mock-auth   |   ----> | frontend |
   |  :5432   |  WAL   |  :8080    |  auth   |  (internal) | static  |  :8081   |
   +----------+        +-----------+         +-------------+         +----------+
        ^                  ^   |
        |                  |   | SSE
   DDL/ |                  |   v
   load |    writer :9100 -+   subscribers
        |     |   |
        |     v   v
        |    +---+----------+ scrape +----------+
        |    | prometheus   | <----- | grafana  |
        +----|  :9090       |        |  :3000   |
             +--------------+        +----------+
```

- **postgres** — `postgres:18-alpine` with `wal_level=logical`. The data
  substrate; the testbench's only stateful service.
- **walera** — the system under test. Tails PG's WAL via `pglogrepl`,
  authenticates SSE clients against mock-auth, fans out filtered tx events to
  the matching subscribers.
- **mock-auth** — Python stdlib HTTP server. Issues field-level whitelists by
  user; admin endpoints (`fail-on` / `fail-off` / `revoke`) drive the
  failure-mode scenarios. NOT host-published (BENCH-01).
- **writer** — Go load generator. Drives quantitative scenarios (smoke / steady
  / spike / soak / stress) into postgres so walera observes realistic commit
  traffic. Reconfigurable at runtime via `POST /control`.
- **frontend** — Caddy static-asset origin for the demo UI under `testbench/web/`.
  No Node, no bundler — native ES modules + one vendored polyfill.
- **prometheus** — `prom/prometheus:v2.55.1` LTS. 1s scrape of walera and
  writer; loads 8 alert rules across 4 groups (stripped from the production
  `deploy/prometheus/alerts.yaml`).
- **grafana** — `grafana/grafana:13.0.1`. Two hand-written dashboards
  provisioned from `./grafana/dashboards/`; datasource UID
  `walera-prometheus` is the contract every panel references.

## Services

| Service     | Image                          | Host port    | Purpose                                                                 |
| ----------- | ------------------------------ | ------------ | ----------------------------------------------------------------------- |
| postgres    | `postgres:18-alpine`           | 127.0.0.1:5432 | `wal_level=logical`, 20 slots, demo schema applied at initdb (orders, devices, articles, line_items) |
| mock-auth   | `walera-testbench-mock-auth`   | (internal)   | Auth backend; field whitelists per user; admin fail-on/fail-off/revoke endpoints (BENCH-01: NOT host-published) |
| walera      | `walera-testbench-walera`      | 127.0.0.1:8080 | SSE server + WAL reader. `cpus:4.0, memory:8G` per DOC-03               |
| writer      | `walera-testbench-writer`      | 127.0.0.1:9100 | Load generator; `POST /control` switches scenario at runtime            |
| frontend    | `caddy:2-alpine`               | 127.0.0.1:8081 | Static origin for `testbench/web/` (demo UI)                            |
| prometheus  | `prom/prometheus:v2.55.1`      | 127.0.0.1:9090 | 1s scrape; 1h retention; `--web.enable-lifecycle` enabled               |
| grafana     | `grafana/grafana:13.0.1`       | 127.0.0.1:3000 | Anonymous Viewer; provisioned dashboards + datasource                   |

Every host port binds to **`127.0.0.1` only**, never `0.0.0.0`. Each binding is
documented inline in `docker-compose.yml` with the reason; override the host
port via the relevant `*_HOST_PORT` env var in `.env` if you have a local
collision (see [Port conflicts](#port-conflicts)).

## Make targets

| Target          | Action                                                                         |
| --------------- | ------------------------------------------------------------------------------ |
| `make demo-up`  | Build images + bring the stack up in detached mode. Idempotent.                |
| `make demo-down`| Stop all containers; preserve the `testbench_pgdata` named volume.             |
| `make demo-reset` | `down -v` + `up` — cold-start. Required after migration / `seed.json` edits. |
| `make demo-logs`| Tail logs from all services (Ctrl-C exits without stopping anything).          |

## Grafana panel guide

Both dashboards are filesystem-provisioned from `./grafana/dashboards/` and
visible without authentication. UI-created dashboards are intentionally
ephemeral; to persist a panel change, export the JSON model and commit it.

### walera-overview — http://127.0.0.1:3000/d/walera-overview

| Panel                              | PromQL                                                                                                                | What it tells you                                          |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| Subscribers active                 | `sum by (type) (walera_subscribers_active)`                                                                           | Active SSE clients by `type` label (`exact` / `wildcard`). |
| WAL LSN lag                        | `walera_wal_lsn_lag_bytes`                                                                                            | Distance between PG's current LSN and walera's flushed LSN. |
| WAL tx rate                        | `rate(walera_wal_tx_size_changes_count[10s])`                                                                         | Decoded commits per second.                                 |
| Auth circuit breaker state         | `walera_auth_circuit_breaker_state`                                                                                   | 0=Closed (normal), 1=Open (failing), 2=HalfOpen (probing). |
| Fan-out histogram                  | `sum by (le) (rate(walera_routing_fan_out_bucket[1m]))`                                                               | Distribution of subscribers matched per tx.                |
| Route-lookup p99                   | `histogram_quantile(0.99, rate(walera_route_lookup_duration_seconds_bucket[1m]))`                                     | Tail latency of the per-tx index lookup.                   |
| Tx dropped by reason               | `sum by (reason) (rate(walera_tx_dropped_total[10s]))`                                                                | Drops stacked by reason: `slow_consumer`, `tx_too_large`, `multi_root`. |
| Writer vs walera tx-rate (OBS-05) | `writer_commit_rate` next to `rate(walera_wal_tx_size_changes_count[10s])`                                            | Side-by-side parity; >2% divergence is the WRITER-05 regression signal. |

### walera-routing — http://127.0.0.1:3000/d/walera-routing

| Panel              | PromQL                                                                                  | What it tells you                                                |
| ------------------ | --------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| Index size by kind | `walera_routing_index_size`                                                             | Per-`index_kind` (exact / wildcard) subscriber counts.           |
| Queue depth p99    | `histogram_quantile(0.99, sum by (le, type) (rate(walera_subscriber_queue_depth_bucket[1m])))` | Per-subscriber buffered-channel saturation by `type`.       |

The index-balance (shard CoV) panel is intentionally **deferred to v1.2**: the
walera registry exposes no per-shard size gauge today. See `walera-routing.json`
description for the deferral note.

## Failure-mode run-book (DOC-02)

Each scenario has an **automated** one-liner that self-verifies its expected
metric reaction in under 90s. The **manual recipe** is provided so an operator
understands what the script does. Every metric name below comes from
`internal/metrics/registry.go` verbatim (no router-namespace typos — the real
metric families are `walera_routing_*` and `walera_route_lookup_*`).

> mock-auth admin recipes use `docker compose exec mock-auth wget …` — mock-auth
> is not host-published (BENCH-01) and the distroless walera image carries
> neither `curl` nor `wget`, so going through the mock-auth python:alpine
> derivative is the only path that works.

### (a) Auth circuit breaker trip + recover (AUTH-04)

**Contract demonstrated.** Bounded fail-open / fail-closed semantics: when the
auth backend fails >50% over the breaker's rolling window, the breaker opens;
new subscription opens fail-closed, existing subscriptions remain authenticated
until their TTL expires. When auth recovers, half-open trial successes close
the breaker.

**Automated.**
```bash
bash testbench/scripts/failure-modes/fail-auth-breaker.sh
```

**Manual recipe.**
```bash
# 1. Trip the breaker by making mock-auth return 500 to every /auth/permissions.
#    Note: busybox wget (python:alpine) lacks --method=POST; the canonical
#    busybox idiom is --post-data='' (empty body POST). Use 127.0.0.1 so the
#    v4 bind always wins over a potentially broken `localhost` AAAA lookup.
docker compose -f testbench/docker-compose.yml exec -T mock-auth \
  wget -q -O- --post-data='' http://127.0.0.1:9000/_admin/fail-on

# 2. Drive enough new SSE opens that walera's auth client trips the breaker
#    (a clean way: open 20 short SSE clients over ~20s — each open triggers
#     an auth call that now fails 500):
for i in $(seq 1 20); do
  curl -sN --max-time 1 -H "Authorization: Bearer demo-alice" \
    -H "Origin: http://localhost:8081" \
    "http://127.0.0.1:8080/sse/v1/orders/${i}" >/dev/null 2>&1 || true
done

# 3. Observe the breaker open in Grafana → walera-overview → "Auth circuit
#    breaker state" panel (value transitions 0 → 1 within ~30s).
curl 'http://127.0.0.1:9090/api/v1/query?query=walera_auth_circuit_breaker_state'

# 4. Recover and watch the breaker close:
docker compose -f testbench/docker-compose.yml exec -T mock-auth \
  wget -q -O- --post-data='' http://127.0.0.1:9000/_admin/fail-off
```

**Expected metric reaction.** `walera_auth_circuit_breaker_state` transitions
`0 → 1` (Closed → Open) within ~30s of the failure window filling, then
`1 → 2 → 0` (Open → HalfOpen → Closed) within ~60s after fail-off.

**Optional alert firing.** After running the documented sed recipe in
`testbench/prometheus/alerts.yaml` (header) to shorten `for:` durations and
issuing `POST /-/reload`, `WaleraAuthBreakerOpen` fires on the Prometheus
`/alerts` page once the shortened threshold elapses.

### (b) Slow consumer disconnect

**Contract demonstrated.** Walera maintains a bounded per-subscriber buffered
channel. A subscriber that cannot drain the channel fast enough is disconnected
with `event: error reason=slow_consumer` rather than blocking the broadcaster.

**Automated.**
```bash
bash testbench/scripts/failure-modes/fail-slow-consumer.sh
```

**Manual recipe.**
```bash
# 1. Spike the writer so walera fans out heavy tx traffic:
curl -fsS -X POST http://127.0.0.1:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"commit_rate":500,"rows_per_tx":10,"scenario":"spike"}'

# 2. Open an SSE subscription whose reader stalls (OS pipe buffer fills,
#    walera's per-subscriber bounded channel fills, walera kicks the client):
(curl -sN --max-time 90 -H "Authorization: Bearer demo-alice" \
  -H "Origin: http://localhost:8081" \
  http://127.0.0.1:8080/sse/v1/orders/all > /tmp/slow.$$ 2>&1) &
# do NOT read /tmp/slow.$$ — let the buffer fill.

# 3. Observe Grafana → walera-overview → "Tx dropped by reason" (slow_consumer
#    series climbs) and/or query Prometheus:
curl 'http://127.0.0.1:9090/api/v1/query?query=walera_subscriber_disconnects_total{reason="slow_consumer"}'
curl 'http://127.0.0.1:9090/api/v1/query?query=walera_tx_dropped_total{reason="slow_consumer"}'

# 4. Restore writer to smoke baseline:
curl -fsS -X POST http://127.0.0.1:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"commit_rate":5,"rows_per_tx":1,"scenario":"smoke"}'
```

**Expected metric reaction.** Either or both of
`walera_subscriber_disconnects_total{reason="slow_consumer"}` and
`walera_tx_dropped_total{reason="slow_consumer"}` increment within ~30–60s.

### (c) PostgreSQL restart

**Contract demonstrated.** Transient WAL reader reconnect; `/readyz` returns 503
during the gap; long-lived SSE subscribers remain connected across the
disconnect window (modulo the documented missed-events window per WAL-04).

**Automated.**
```bash
bash testbench/scripts/failure-modes/fail-pg-restart.sh
```

**Manual recipe.**
```bash
# 1. Restart postgres:
docker compose -f testbench/docker-compose.yml restart postgres

# 2. Observe walera's PG-connection gauge drop, /readyz return 503:
curl 'http://127.0.0.1:9090/api/v1/query?query=walera_pg_connection_status'
curl -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/readyz

# 3. Wait ~30s for postgres to become healthy + walera to reconnect; verify
#    the reconnect counter incremented:
curl 'http://127.0.0.1:9090/api/v1/query?query=walera_pg_reconnects_total'
```

**Expected metric reaction.** `walera_pg_connection_status` flips `1 → 0`
within ~7s (lag sampler ticks every 5s) and `0 → 1` within ~30s; `/readyz`
transitions `200 → 503 → 200`; `walera_pg_reconnects_total` increments by ≥ 1.

**Optional alert firing.** After the sed recipe + `/-/reload`,
`WaleraPGDisconnected` fires on `/alerts` once the shortened `for:` elapses.

### (d) High WAL lag (writer spike)

**Contract demonstrated.** When writer commit-rate exceeds walera's effective
decode + broadcast throughput, `walera_wal_lsn_lag_bytes` accumulates; when load
drops, the lag drains as walera catches up.

**Automated.**
```bash
bash testbench/scripts/failure-modes/fail-wal-lag.sh
```

**Manual recipe.**
```bash
# 1. Spike the writer:
curl -fsS -X POST http://127.0.0.1:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"commit_rate":500,"rows_per_tx":10,"scenario":"spike"}'

# 2. Observe Grafana → walera-overview → "WAL LSN lag" climb; the OBS-05
#    side-by-side panel shows writer_commit_rate diverging from the walera
#    observed rate (lag accumulates because walera lags writer):
curl 'http://127.0.0.1:9090/api/v1/query?query=walera_wal_lsn_lag_bytes'

# 3. Drain — return the writer to steady; lag drops back to baseline:
curl -fsS -X POST http://127.0.0.1:9100/control \
  -H 'Content-Type: application/json' \
  -d '{"commit_rate":100,"rows_per_tx":1,"scenario":"steady"}'
```

**Expected metric reaction.** `walera_wal_lsn_lag_bytes` exceeds baseline by
≥ 5 MiB within ~60s of the spike, then returns to within 2× baseline within
~90s of the drain.

**Optional alert firing.** After the sed recipe + `/-/reload`,
`WaleraWALLagHigh` fires once `walera_wal_lsn_lag_bytes > 100*1024*1024` holds
for the shortened `for:` window.

## Troubleshooting

### Grafana login escape hatch

Anonymous Viewer is the default — visiting http://localhost:3000 should land
you on the home dashboard with no login. If you need write-edit access for
dashboard development:

```bash
# .env (already documented inline):
GF_SECURITY_ADMIN_PASSWORD=admin   # compose default; override as needed
```

then visit http://localhost:3000/login?disableLoginForm=false and log in as
`admin / <password>`. Edits made through the UI are ephemeral by design (no
persistent Grafana volume); to make a panel change permanent, export the
dashboard JSON and commit to `testbench/grafana/dashboards/`.

### Port conflicts

Every host port is loopback-bound. Override via `.env`:

| Var                  | Default | Service    |
| -------------------- | ------- | ---------- |
| `PG_HOST_PORT`       | 5432    | postgres   |
| `WALERA_HOST_PORT`   | 8080    | walera     |
| `WRITER_HOST_PORT`   | 9100    | writer     |
| `FRONTEND_HOST_PORT` | 8081    | frontend   |
| `PROM_HOST_PORT`     | 9090    | prometheus |
| `GRAFANA_HOST_PORT`  | 3000    | grafana    |

Never bind to `0.0.0.0` — the dev creds in `.env.example` (`POSTGRES_PASSWORD=walera`,
mock-auth's unauthenticated admin endpoints, Grafana's anonymous Viewer)
would otherwise be exposed to the LAN.

### Healthcheck flicker on cold start

Healthcheck dependency graph:

```
postgres (5–10s) → walera (10s start_period) → prometheus → grafana
                ↘ mock-auth ↗     writer (5s start_period) ↗
                                  frontend (independent)
```

Total cold-start healthy time is 30–60s on a warm Docker host, longer on first
image pull. If `make demo-up` returns before everything is healthy:

```bash
docker compose -f testbench/docker-compose.yml ps   # see who's still flapping
make demo-logs                                       # inspect the offender
```

### mock-auth not reachable from the host

By design (BENCH-01). To exercise `/_admin/*` from the host, go through
`docker compose exec mock-auth wget …` as documented in the failure-mode
run-book above. The walera distroless image has no `curl` or `wget`, so
`docker compose exec walera …` is **not** a working substitute.

### Stale demo data

```bash
make demo-reset    # down -v + up — cold-start with fresh seed
```

Use this whenever a migration in `testbench/migrations/` or `mock-auth/seed.json`
changes. `make demo-down` preserves the `testbench_pgdata` volume; `make
demo-reset` blows it away.

## Reference

- `.planning/REQUIREMENTS.md` — full v1.1 requirement set (BENCH-\*, OBS-\*,
  DOC-\*, AUTH-\*, WAL-\*, WALERA-\*, SCHEMA-\*, MOCK-\*).
- `.planning/ROADMAP.md` — phase index and success criteria.
- `deploy/prometheus/alerts.yaml` — canonical production alert source;
  `testbench/prometheus/alerts.yaml` is its stripped (k8s envelope dropped)
  testbench counterpart.
- `internal/metrics/registry.go` — authoritative metric name + label registry.
