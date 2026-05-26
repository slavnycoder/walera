# Walera

Walera is a Go service that streams PostgreSQL row-level changes to
clients over Server-Sent Events. It is targeted at internal product
teams that need real-time per-entity push without building or
operating Debezium and Kafka.

```
PostgreSQL WAL ──▶ Walera ──▶ SSE clients
                     │
                     └──▶ your auth backend
```

> **Disclaimer.** This project was AI-assisted end to end. Treat it
> as a reference implementation, not battle-hardened software;
> review before relying on it in production.

## What it does

Walera tails the PostgreSQL WAL via `pgoutput` logical replication,
authorizes each SSE subscription against an external auth backend,
filters row changes down to the fields a user is allowed to see, and
delivers them as SSE events. A single pod is designed to sustain
roughly 5,000 WAL transactions per second and roughly 10,000
concurrent SSE subscribers on a 4-CPU instance.

Use Walera when the product backend already owns the database and the
auth logic, and clients want realtime per-entity updates without a
full message-broker tier in between. Walera is a single Go binary
that speaks PostgreSQL logical replication on one side and plain
HTTP/SSE on the other.

The runtime contract is authorized, field-filtered,
transactionally-atomic delivery of Postgres row changes:
authorization is enforced inside Walera at fan-out time, every SSE
event corresponds to one Postgres transaction, and ordering reflects
the WAL's commit order.

## What it does not guarantee

Walera does not replay missed events to disconnected clients. Clients must resynchronize through the primary API after reconnect.

The full delivery posture is documented in
[Delivery semantics](./docs/delivery-semantics.md). At a glance, the
non-guarantees are:

- No durable event store. Events are not persisted past fan-out.
- No `Last-Event-ID` resume.
- No across-restart delivery to disconnected clients.
- No client-side filtering — the per-user field whitelist is
  enforced inside Walera, not negotiated by the client.
- No client acknowledgement protocol.

If durable, replayable event delivery is a requirement, Walera is not
the right tool — that scope belongs to a different product with a
different failure model.

## Quick start

Walera ships as a multi-arch container image
(`linux/amd64`, `linux/arm64`) on GitHub Container Registry. The
simplest working invocation:

```bash
docker pull ghcr.io/slavnycoder/walera:latest

docker run --rm \
  -p 8080:8080 \
  -e WALERA_DATABASE_URL='postgres://walera:secret@host:5432/app?sslmode=disable' \
  -e WALERA_AUTH_BACKEND_URL='https://auth.example.com' \
  ghcr.io/slavnycoder/walera:latest
```

Subscribe with curl once the pod is healthy:

```bash
curl -N \
  -H "Authorization: Bearer alice-token" \
  http://localhost:8080/sse/v1/orders/42
```

For a full local demo (PostgreSQL, Walera, mock auth, load writer,
Prometheus, Grafana, browser UI):

```bash
cd testbench
cp .env.example .env
make demo-up
```

| Service          | URL                              |
| ---------------- | -------------------------------- |
| Demo UI          | http://localhost:8081            |
| Walera metrics   | http://localhost:8080/metrics    |
| Prometheus       | http://localhost:9090            |
| Grafana          | http://localhost:3000            |

For Kubernetes, Docker Compose, and production deployment procedures,
see [Operations](./docs/operations.md).

## Configuration

Walera reads its configuration from a YAML file plus `WALERA_*`
environment variables. The required runtime variables are listed
below; see [docs/operations.md#configuration](./docs/operations.md#configuration)
for the full reference (operational tuning + development-only patterns).

- `WALERA_DATABASE_URL` — the single Postgres DSN (the admin connection
  plus the automatically-derived replication connection). The role must
  hold the `REPLICATION` attribute, and the connection must be direct (no
  PgBouncer).
- `WALERA_AUTH_BACKEND_URL` — auth service base URL.

A minimal `config.yaml` skeleton looks like:

```yaml
database:
  url: postgres://walera:secret@host:5432/app?sslmode=disable

auth:
  backend_url: https://auth.example.com
```

For the full configuration reference, including tuning, CORS
canonicalisation rules, trusted-proxy parsing, the optional pprof
listener, and bootstrap helpers for development environments, see
[Operations](./docs/operations.md).

### HTTP API

Two SSE endpoints, both requiring `Authorization: Bearer <user-token>`:

| Endpoint                       | Streams                                 |
| ------------------------------ | --------------------------------------- |
| `GET /sse/v1/{table}/{pk}`     | Changes to a single row.                |
| `GET /sse/v1/{table}/all`      | Changes to every row in the table.      |

The endpoint is versioned in the URL (`/sse/v1/`). Breaking changes
will be served from `/sse/v2/` rather than mutating `v1`.

## Security model

Walera does not authenticate users itself. Every SSE subscription
carries a bearer token, which Walera forwards to the external auth
backend the operator configures. The backend returns a per-user
whitelist of accessible tables and fields; Walera enforces that
whitelist at fan-out time and only forwards whitelisted columns to
the subscriber. Tokens and row payloads are never logged.

Wildcard streams (`/sse/v1/{table}/all`) reveal which primary keys
are changing and are gated separately. They should only be enabled
for tables whose enumeration is already public information.

An auth-backend outage trips a circuit breaker that takes a bounded
fail-open posture for established subscriptions and a fail-closed
posture for new opens.

See [Auth model](./docs/auth.md) for the backend contract, request
and response schemas, status codes, the wildcard-stream policy, and
an example backend implementation.

## Delivery semantics

Walera delivers each row change to each authorized subscriber at most
once. Changes from a single Postgres transaction are grouped into a
single SSE event, so within a transaction subscribers see WAL-order
updates atomically. Events that occur while a subscriber is
disconnected are not delivered to that subscriber on reconnect —
clients recover by re-reading the affected entity from the primary
API.

```text
event: tx
id: 735
data: {"tx_id":735,"commit_ts":"2026-05-18T08:30:12.123456Z","changes":[{"op":"update","table":"orders","pk":"42","data":{"status":"paid"}}]}
```

The `data` payload shape:

```ts
type WaleraTx = {
  tx_id: number;
  commit_ts: string;
  changes: Array<{
    op: "insert" | "update" | "delete";
    table: string;
    pk: string;
    // insert → full new row; update → only modified columns
    // (absence ≠ null); delete → absent.
    data?: Record<string, unknown>;
  }>;
};
```

Other event types Walera emits:

| Event       | When it fires                                                |
| ----------- | ------------------------------------------------------------ |
| `error`     | Permission revoked, payload too large, etc. Includes reason. |
| `shutdown`  | Service is restarting; the client should reconnect.          |
| `:` comment | Heartbeat. Not visible to browser JS.                        |

The recommended client recovery pattern is:

1. Load current state from the primary product API.
2. Open the Walera SSE stream and apply incoming events to that
   state.
3. On any SSE disconnect or `shutdown` event, repeat step 1 before
   resubscribing — do not assume local state is current.

**Transaction-scoped delivery (v2.4).** Subscribers whitelisted for
multiple related tables receive all whitelisted changes from a matched
transaction in a single atomic event — not just the change that
anchored the match. Relatedness is the application's responsibility:
co-write related rows in the same Postgres `BEGIN` / `COMMIT` block.
Authorization is enforced solely by the per-user field whitelist; Walera
does not route by foreign keys. A raw channel match authorizes
co-transactional delivery only if at least one matching anchor change
survives that whitelist. The Prometheus counter
`walera_co_tx_beyond_anchor_total` tracks the incremental delivery
volume this behaviour adds over per-change matching alone.

See [Delivery semantics](./docs/delivery-semantics.md) for the
complete specification and
[ADR 0001](./docs/adr/0001-delivery-semantics.md) for the rationale.

## Operational notes

Walera targets a single Kubernetes pod per environment with 2 CPU /
4 GiB requests, 4 CPU / 8 GiB limits, and
`terminationGracePeriodSeconds: 30`. PostgreSQL ≥ 14 with
`wal_level=logical`, a DBA-owned publication, and a replication user
with the `REPLICATION` attribute are required. PgBouncer is not
supported in the replication path.

The replication slot is temporary: Walera creates it at startup and
PostgreSQL drops it automatically when the connection closes. No
manual slot cleanup is required if the deployment is decommissioned.
A restart causes a brief delivery gap; clients reconnect and resync
through the primary API.

Subscribers that cannot drain at WAL pace are disconnected rather
than buffered indefinitely, keeping the per-subscriber memory
footprint predictable at the ~10,000-subscriber target. A Prometheus
counter exposes the slow-client disconnect rate and should be
monitored alongside WAL lag and auth-backend failures.

| Endpoint        | Purpose                                                |
| --------------- | ------------------------------------------------------ |
| `GET /healthz`  | Liveness. `200` when the WAL reader is connected.      |
| `GET /readyz`   | Readiness. `200` when PostgreSQL and auth are healthy. |
| `GET /metrics`  | Prometheus metrics.                                    |

Key Prometheus metrics surfaced for operations:

- WAL lag histogram and PostgreSQL slot lag gauge.
- Currently-connected SSE subscriber gauge.
- Auth-backend failure counter, partitioned by status code and
  circuit-breaker state.
- Per-event fan-out latency histogram.
- Slow-client disconnect counter.
- Replication slot connection-state gauge.

See [Operations](./docs/operations.md) for deployment manifests,
PostgreSQL prerequisites, observability, the slot and slow-client
policies, and the upgrade procedure.

Per-version upgrade notes will be tracked in `CHANGELOG.md`
(introduced in a later release).

## Development

Go 1.22 or later is required; the toolchain version is pinned in
`go.mod`. Walera is `-race`-clean by policy; the test suite is
expected to pass under `-race` on every change. Integration tests
use `testcontainers-go` to spin up a real PostgreSQL with
`wal_level=logical` and run the WAL pipeline end-to-end.

```bash
git clone https://github.com/slavnycoder/walera.git
cd walera
make build               # produces ./cdc-sse
make test                # unit tests with -race
make test-integration    # testcontainers-go + PostgreSQL
make deps-check          # enforces the internal/ import-graph rules
./cdc-sse --config ./config.yaml --dev-log
```

#### Import-graph gate

`make deps-check` enforces the directional `internal/` import graph:

- `internal/router` does not import `internal/auth` or `internal/sse`.
- `internal/wal` does not import `internal/sse`.
- `internal/config` does not import any sibling `internal/*` package.
- `internal/health` does not import `internal/auth`, `internal/router`,
  `internal/sse`, or `internal/wal`.

These rules keep composition concerns in `cmd/cdc-sse` and prevent
import cycles. A future CI gate invokes this target as a required step.

The repository layout:

| Path                 | Contents                                              |
| -------------------- | ----------------------------------------------------- |
| `cmd/cdc-sse/`       | Main service binary.                                  |
| `cmd/loadgen/`       | SSE load generator (subscriber-side capacity tests).  |
| `internal/wal/`      | Logical replication reader and type mapping.          |
| `internal/router/`   | Subscriber indexes and transaction fan-out.           |
| `internal/sse/`      | HTTP routes, SSE writer, encoder, CORS.               |
| `internal/auth/`     | Auth client, permission map, refresh loop, breaker.   |
| `deploy/`            | Kubernetes manifests, PostgreSQL guide, Prom rules.   |
| `testbench/`         | Full local demo stack.                                |
| `test/integration/`  | Integration tests.                                    |
| `spec/`              | Product and implementation specs.                     |

Each `internal/` package owns one runtime responsibility: the WAL
reader decodes `pgoutput` messages and maintains a relation cache;
the router shards subscribers and fans transactions out to writers;
the SSE package serves HTTP requests, applies the per-user
whitelist, and frames events; the auth package wraps the external
backend in a circuit breaker; and the writer package owns the
bounded send queues and the slow-client disconnect path.

For the full architectural reference, the failure model, the runtime
component breakdown, and the operational assumptions, see
[Architecture](./docs/architecture.md).

### Frontend integration

Native `EventSource` cannot send an `Authorization` header. The
recommended client is
[`@microsoft/fetch-event-source`](https://www.npmjs.com/package/@microsoft/fetch-event-source),
which preserves SSE semantics but uses `fetch` under the hood. A
minimal vanilla-JS example:

```html
<pre id="out"></pre>

<script type="module">
  import { fetchEventSource } from "https://esm.sh/@microsoft/fetch-event-source";

  const out = document.querySelector("#out");

  fetchEventSource("http://localhost:8080/sse/v1/orders/42", {
    headers: { Authorization: "Bearer alice-token" },
    onmessage(msg) {
      if (msg.event === "tx") {
        const tx = JSON.parse(msg.data);
        out.textContent += JSON.stringify(tx, null, 2) + "\n";
      }
      if (msg.event === "error") {
        out.textContent += "error: " + msg.data + "\n";
      }
      if (msg.event === "shutdown") {
        out.textContent += "server is restarting\n";
      }
    },
    onerror(err) {
      console.warn("Walera disconnected, retrying", err);
      return 15000;
    },
  });
</script>
```

React, Svelte, and other framework integrations follow the same
shape: open the stream, switch on `msg.event`, and reconnect with
backoff. On any disconnect, refetch the affected entity from the
primary API before resubscribing.

A few client-side rules of thumb worth carrying into integration:

- Never read row data from the SSE event for authorization
  decisions on the client — the field whitelist is enforced inside
  Walera, but the client cannot trust that the whitelist matches
  the user's full product permissions. Drive authorization from the
  same source the primary API uses.
- Treat the SSE stream as a hint to refresh, not a replacement for
  reading. State that survives a page reload must come from the
  primary API.
- Use jittered exponential backoff on reconnect. A fleet-wide
  reconnect storm after a deploy will otherwise hit the primary
  API as a synchronous spike.

### Load testing

A standalone SSE load generator lives at `cmd/loadgen`; an
orchestrator at `scripts/bench.sh` captures heap, CPU, and goroutine
pprof snapshots alongside Prometheus output. Both are opt-in and
gated behind explicit flags.

```bash
LOADGEN_AUTH_TOKEN="<bearer-token>" ./loadgen \
  --target-url http://127.0.0.1:8080 \
  --concurrency 1000 \
  --channels orders/all,users/all \
  --duration 5m \
  --ramp-up 30s \
  --http-addr 127.0.0.1:9200
```

The opt-in pprof listener is configured via `http.pprof_addr` or
`WALERA_HTTP_PPROF_ADDR` and binds to loopback by default. See
[Operations](./docs/operations.md) for the full benchmarking
workflow.

## License

Walera is licensed under the [MIT License](./LICENSE) (SPDX
identifier: `MIT`). See the `LICENSE` file at the repository root
for the full text.

For third-party license attributions, see
[docs/licenses.md](./docs/licenses.md).
