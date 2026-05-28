# Walera

> Stream PostgreSQL row changes to authenticated SSE subscribers without
> standing up Kafka or Debezium. A single Go binary — ~10k concurrent
> connections at ~5k tx/s on a 4-CPU pod.

```
PostgreSQL WAL ──▶ Walera ──▶ SSE clients
                      │
                      └──▶ your auth backend
```

> AI-assisted reference implementation. Review before production use.

---

## When it fits

You already have:

- A PostgreSQL database that owns your domain state.
- An auth backend that knows what each user is allowed to see.
- Clients that want **real-time per-entity push** — not analytics, not
  audit logs.

Walera adds an SSE layer between them. No new message broker. No
schema-change pipeline. No client-acked queue. The product backend
keeps owning the source of truth; Walera just streams what already
landed in the WAL.

---

## Subscribe in 30 seconds

```bash
docker run --rm -p 8080:8080 \
  -e WALERA_DATABASE_URL='postgres://walera:secret@host:5432/app?sslmode=disable' \
  -e WALERA_AUTH_BACKEND_URL='https://auth.example.com' \
  ghcr.io/slavnycoder/walera:latest
```

```bash
curl -N -H "Authorization: Bearer alice-token" \
  http://localhost:8080/sse/v1/orders/42

event: tx
id: 735
data: {"tx_id":735,"commit_ts":"2026-05-18T08:30:12Z","changes":[
  {"op":"update","table":"orders","pk":"42","data":{"status":"paid"}}
]}
```

Two endpoints, that's the whole API:

| Endpoint                       | Streams                              |
| ------------------------------ | ------------------------------------ |
| `GET /sse/v1/{table}/{pk}`     | Changes to one row.                  |
| `GET /sse/v1/{table}/all`      | Changes to every row in the table.   |

The URL is versioned (`/sse/v1/`). Breaking changes will be served from
`/sse/v2/` rather than mutating `v1`.

---

## Events are diffs

Each SSE event corresponds to **exactly one Postgres transaction**. The
`data` field is a primary key plus the columns that changed:

```ts
type WaleraTx = {
  tx_id: number;
  commit_ts: string;
  changes: Array<{
    op: "insert" | "update" | "delete";
    table: string;
    pk: string;
    // insert: full new row
    // update: only modified columns (absence ≠ null)
    // delete: absent
    data?: Record<string, unknown>;
  }>;
};
```

Apply each event to a local mirror — IndexedDB, a Redux/Zustand store,
a `Map` in memory — and render from the mirror. Stop hitting REST on
every event:

```js
for (const change of tx.changes) {
  if (change.op === "insert")      store.set(change.pk, change.data);
  else if (change.op === "update") store.merge(change.pk, change.data);
  else if (change.op === "delete") store.delete(change.pk);
}
```

REST stays in the loop for two things and two things only:

- **Bootstrap.** Load initial state when the page opens.
- **Gap-closing.** Refetch after SSE reconnect — Walera does not
  replay across the disconnect window.

A worked example using Dexie/IndexedDB as the mirror, with
`liveQuery`-driven re-render and optimistic-update rollback, lives in
[`walera-demo`](https://github.com/slavnycoder/walera-demo).

Other event types Walera emits:

| Event          | When it fires                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------------ |
| `initial_data` | Optional first frame, only if the auth backend returns an `initial_data` field. Opaque JSON.     |
| `error`        | Permission revoked, payload too large, etc. Includes reason.                                     |
| `shutdown`     | Service is restarting; the client should reconnect.                                              |
| `:` comment    | Heartbeat. Not visible to browser JS.                                                            |

---

## Writer-side discipline

This is the part that asks something of you. Walera routes by the root
row(s) touched in a transaction; the broker is **not** FK-aware. Three
rules govern how your write path must behave so subscribers see clean,
atomic events.

### Rule 1 — One root row per transaction

A transaction that mutates a root table (a table users subscribe to)
must touch **exactly one** PK of that root.

```sql
-- ✓ OK
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 42;
COMMIT;

-- ✗ DROPPED for affected subscribers (multi-root violation):
BEGIN;
  UPDATE orders SET reordered_at = now();   -- touches every order!
COMMIT;
```

This rule is **broker-enforced**. Multi-root transactions are silently
dropped per subscriber (`walera_tx_dropped_total{reason="multi_root"}`
increments, a warn-level log fires, the connection stays open). Split
bulk operations across roots in your application layer — one
transaction per root.

### Rule 2 — Co-write children with their root

When a transaction modifies a child row, the same transaction must
touch the corresponding root row. The root touch is the routing
anchor; without it, subscribers don't see the child change at all.

```sql
BEGIN;
  UPDATE line_items SET qty = 3 WHERE id = 17;
  -- root anchor (a trigger usually does this for you):
  UPDATE orders SET updated_at = now() WHERE id = 42;
COMMIT;
```

A common pattern is `updated_at`-bump triggers on every child table, so
child writes automatically anchor their root. The
[demo schema](https://github.com/slavnycoder/walera-demo/blob/master/db/002_schema.sql)
wires this up through three FK levels (`todo_lists ← tasks ← subtasks`)
and a single subtask write produces one Walera event containing all
three layers.

### Rule 3 — Don't mix children of different roots

A transaction that writes children of two different roots — even with
each root anchored — leaks across subscribers and is **not** detected
by the broker.

```sql
-- ✗ Caller-enforced: this leaks. Split it.
BEGIN;
  UPDATE line_items SET qty = 3 WHERE id = 17;  -- belongs to order 42
  UPDATE line_items SET qty = 1 WHERE id = 88;  -- belongs to order 99
  UPDATE orders SET updated_at = now() WHERE id IN (42, 99);
COMMIT;
```

Walera cannot distinguish "child of my root" from "child of someone
else's root" without FK-aware scope declarations, which are out of
scope for the current model. If you need this stricter isolation,
please file an issue.

---

## Auth model

Walera does not authenticate users. It forwards the bearer token from
each SSE open to an auth backend **you operate**, and receives back a
per-user whitelist of tables and columns. Field filtering is enforced
inside Walera before any event reaches the wire.

The contract is a single GET:

```http
GET /auth/permissions?channel=orders%3A42
Authorization: Bearer <user-token>
```

```json
{
  "user_id": "alice",
  "tables": {
    "orders": ["id", "status", "total_cents", "updated_at"]
  },
  "roots": ["orders"],
  "ttl_seconds": 60
}
```

The full contract — status-code handling, refresh semantics, wildcard
policy, circuit-breaker posture — lives in [`docs/auth.md`](./docs/auth.md).
A minimal Django reference implementation is in that doc; the
[demo backend](https://github.com/slavnycoder/walera-demo/blob/master/backend/app.py)
shows a FastAPI version that doubles as the product API.

When the auth backend goes sideways, Walera trips a circuit breaker
and takes a **bounded fail-open** posture for established subscribers
(events continue until each subscriber's permission TTL expires) and a
**fail-closed** posture for new opens. This stops an auth outage from
becoming a fleet-wide reconnect storm.

---

## Frontend integration

Native `EventSource` can't send `Authorization`. Use
[`@microsoft/fetch-event-source`](https://www.npmjs.com/package/@microsoft/fetch-event-source):

```js
import { fetchEventSource } from "https://esm.sh/@microsoft/fetch-event-source";

fetchEventSource("http://localhost:8080/sse/v1/orders/42", {
  headers: { Authorization: "Bearer alice-token" },
  onmessage(msg) {
    if (msg.event !== "tx") return;
    const tx = JSON.parse(msg.data);
    for (const c of tx.changes) applyToStore(c);
  },
  onerror() {
    // Walera does not replay across reconnect — refetch from REST
    // before resubscribing, then come back.
    return 15000;
  },
});
```

Rules of thumb:

- **Events are diffs, not refresh hints.** Steady-state UI updates
  should make zero further network calls.
- **Never authorise on event data alone.** Walera filters fields at
  fan-out, but the client cannot assume the whitelist matches your
  full product permissions. Drive auth from the same source REST uses.
- **Jittered exponential backoff on reconnect.** A fleet-wide
  reconnect storm after a deploy will otherwise hit your primary API
  as a synchronous spike.

---

## Configuration

Two env vars get you off the ground:

```bash
WALERA_DATABASE_URL=postgres://walera:secret@host:5432/app?sslmode=disable
WALERA_AUTH_BACKEND_URL=https://auth.example.com
```

YAML config and the full env reference (CORS, trusted-proxy parsing,
pprof, etc.) live in [`docs/operations.md`](./docs/operations.md#configuration).

### PostgreSQL prerequisites

- Version ≥ 14, `wal_level = logical`.
- A DBA-owned publication enumerating the tables Walera may decode.
- A replication user with the `REPLICATION` attribute.
- **No PgBouncer in the replication path.** PgBouncer does not speak
  the replication protocol — connect directly to PostgreSQL.

The replication slot is temporary: created on boot, dropped by
PostgreSQL when the connection closes. No manual cleanup is needed
when the deployment is decommissioned. A restart causes a brief
delivery gap; clients reconnect and resync through the primary API.

---

## Run the full demo stack

PostgreSQL + Walera + mock auth + load writer + Prometheus + Grafana +
a browser UI, all wired up:

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

For a showcase application built **on top of** Walera — Dexie diff
source, optimistic UI, bulk transactional ops, failure-injection
rollback — see [`walera-demo`](https://github.com/slavnycoder/walera-demo).

---

## What Walera does not guarantee

Walera is intentionally narrow:

- **No durable event store.** Events are not persisted past fan-out.
- **No `Last-Event-ID` resume.** No replay across reconnect.
- **No across-restart delivery to disconnected clients.** Clients
  resync state from the primary API.
- **No client-side filtering.** The per-user field whitelist is
  enforced inside Walera, not negotiated by the client.
- **No client acknowledgement protocol.**

If durable, replayable, exactly-once-style delivery is what you need,
Walera is not the right tool — that scope belongs to a different
product with a different failure model. The full delivery posture is
documented in [`docs/delivery-semantics.md`](./docs/delivery-semantics.md).

---

## Operations

| Endpoint        | Purpose                                                |
| --------------- | ------------------------------------------------------ |
| `GET /healthz`  | Liveness. `200` when the WAL reader is connected.      |
| `GET /readyz`   | Readiness. `200` when PostgreSQL and auth are healthy. |
| `GET /metrics`  | Prometheus metrics.                                    |

Key metrics: WAL lag histogram, PG slot lag gauge, connected-subscriber
gauge, auth-failure counter (by status + breaker state), per-event
fan-out latency histogram, slow-client disconnect counter, slot
connection-state gauge.

Subscribers that cannot drain at WAL pace are disconnected rather
than buffered indefinitely, keeping the per-subscriber memory
footprint predictable at the ~10,000-subscriber target.

Deployment targets a single Kubernetes pod per environment:
2 CPU / 4 GiB requests, 4 CPU / 8 GiB limits,
`terminationGracePeriodSeconds: 30`. Full deployment manifests, the
slow-client policy, and the upgrade procedure live in
[`docs/operations.md`](./docs/operations.md).

---

## Development

```bash
git clone https://github.com/slavnycoder/walera.git
cd walera
make build               # ./cdc-sse
make test                # unit tests with -race
make test-integration    # testcontainers-go + PostgreSQL
make deps-check          # internal/ import-graph gate
./cdc-sse --config ./config.yaml --dev-log
```

Go 1.22 or later is required. The test suite must pass under `-race`
on every change; coverage target is > 85% lines. Integration tests use
`testcontainers-go` to spin up a real PostgreSQL with
`wal_level=logical` and run the WAL pipeline end-to-end.

The package layout, the runtime component breakdown, the failure
model, and the operational assumptions are in
[`docs/architecture.md`](./docs/architecture.md).

A standalone SSE load generator (`cmd/loadgen`) and a benchmark
orchestrator (`scripts/bench.sh`) capture heap, CPU, and goroutine
pprof snapshots alongside Prometheus output — see
[`docs/operations.md`](./docs/operations.md) for the full workflow.

---

## License

MIT — see [`LICENSE`](./LICENSE). Third-party attributions in
[`docs/licenses.md`](./docs/licenses.md).
