# 10. Resources and deployment

## 10.1. Container sizing

```yaml
resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"
```

Expected baseline:
- CPU: 1.5–2 cores under steady load; bursts up to 4.
- Memory: 1.5–3 GB under steady load; safety margin to 8 GB.

## 10.2. Kernel / OS

- `ulimit -n 32768` (file descriptors).
- `net.core.somaxconn = 4096`.
- `net.ipv4.tcp_max_syn_backlog = 4096`.

## 10.3. Go runtime

- `GOMAXPROCS`: detect via `automaxprocs` package (correct in containers).
- `GOGC=200` (less frequent GC when memory is available).
- `GOMEMLIMIT=6500MiB` (~80% of container limit).

## 10.4. PostgreSQL prerequisites

- Version ≥ 14.
- `wal_level = logical`.
- `max_replication_slots` — sized per the formula below. **Hard requirement**: undersizing breaks rollouts (new pod cannot create its slot, walreplicator enters crash-loop). PG default is `10`, which is often already partially consumed by neighbors (Debezium, audit pipelines, standby replication). Changing this value requires a **PG restart** — must be done as planned maintenance, not reactively.

  Formula: `max_replication_slots ≥ ceil(N_instances × 1.25) + reserved_for_other_consumers + 2`

  Where:
  - `N_instances` — peak count of walreplicator pods, including rolling-deploy surge.
  - `× 1.25` — covers `maxSurge=25%` during rolling deploy (old + new pod overlap).
  - `+ 2` — headroom for stuck/orphaned slots that PG hasn't cleaned up yet (typically released within `wal_sender_timeout`, but a brief overlap is possible).
  - `reserved_for_other_consumers` — other replication users on the same PG cluster.

  Concrete minimums:
  - **MVP (N=1):** `max_replication_slots ≥ 4`. Cheap, leaves room for one additional consumer.
  - **Future scale-out (N=4):** `≥ 8`. Set this **now if scale-out is on the roadmap** — increasing it later requires PG restart.

  Owner: DBA reviews this value whenever the instance count changes or a new replication-protocol consumer joins the cluster.
- `max_wal_senders` ≥ `max_replication_slots` (each connected slot consumes one wal_sender process).
- A replication user with the `REPLICATION` attribute. SELECT on data tables is NOT required.
- **Publication pre-created by DBA** ([§1.2](01-data-source-and-wal.md#12-publication)). The replication slot is NOT pre-created — the service creates a temporary slot on each connect ([§1.4](01-data-source-and-wal.md#14-replication-slot)).
- If a connection pooler (PgBouncer) is in front of PG, the replication connection must bypass it — PgBouncer does not support the replication protocol.

## 10.5. k8s probes and lifecycle

```yaml
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
  periodSeconds: 5
  failureThreshold: 3
livenessProbe:
  httpGet: { path: /healthz, port: 8080 }
  periodSeconds: 2
  failureThreshold: 3
terminationGracePeriodSeconds: 30
```

Effective windows:
- **Readiness:** ~15s without recovery → pod removed from Service endpoints. Used for routing, not lifecycle.
- **Liveness:** ~5s (4–6s depending on probe phase) without PG connectivity → kubelet kills the pod. The aggressive timing is deliberate — it caps the silent-gap window during PG flaps. See [§7.4](07-operational-concerns.md#74-pg-reconnection) for rationale.

If the cluster forbids `periodSeconds < 5`, use `periodSeconds=5, failureThreshold=1` → ~5s (with the trade-off that a single transient probe failure also kills the pod).

## 10.6. Topology

Single instance for MVP. Rolling update causes a few seconds of downtime between old shutdown and new accept; clients reconnect automatically.

Future scale-out paths (not in MVP, but the broadcaster/router interfaces should be designed for swap-out):
- **Active-passive HA:** standby instance acquires slot on leader failure. Needs distributed lock (etcd, Consul).
- **N-instance fan-out:** one WAL reader publishes to a message bus (NATS, Redis Streams); each instance reads from the bus and holds its share of SSE connections.
