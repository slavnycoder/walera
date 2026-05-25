# 7. Operational concerns

## 7.1. Startup sequence

1. Load and validate config.
2. Open admin DB connection (used for current-WAL-LSN polling and other diagnostics).
3. Open replication connection and create a **temporary** logical slot (`CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL pgoutput`). PG returns the slot's starting LSN — adopt it as `lastCommittedLSN`. If the publication does not exist, **fail fast**.
4. Start the pipeline: reader → router.
5. Start the standby-ack ticker (advances `confirmed_flush_lsn` while connected; see [§1.5](01-data-source-and-wal.md#15-lsn-acknowledgment)).
6. Start the HTTP server.

There is no "WAL backed up after downtime" scenario to recover from — the temporary slot ([§1.4](01-data-source-and-wal.md#14-replication-slot)) means PG retained no WAL on our behalf during downtime, and the new slot starts at the current LSN.

## 7.2. Restart behavior

Restart is treated as a fresh start: a new temporary slot begins at the current WAL LSN. Events that occurred while the service was down are not in our stream and are not delivered. This is by design — the snapshot-on-reconnect contract ([§3](03-subscriptions-and-sse.md), [§1.4](01-data-source-and-wal.md#14-replication-slot)) makes catch-up unnecessary: every reconnecting client refetches state via the snapshot backend before resubscribing, which covers the gap.

New subscribers arriving after the service is up get a `start_lsn` equal to the current `lastCommittedLSN` and enter the realtime stream from the next COMMIT.

## 7.3. Graceful shutdown

Triggered by SIGTERM (or SIGINT). Sequence:

1. Set a 10-second deadline for the entire shutdown.
2. `httpServer.Shutdown(ctx)` — stops accepting new connections. Existing connections continue.
3. Signal the reader to stop at the next safe point (after current Commit, or immediately if outside a tx).
4. Wait for the reader to exit; the router drains `txCh`.
5. For each active subscriber: send `event: shutdown\ndata: {"reason": "service_restart"}`, flush, then close.
6. Wait for all writer goroutines to exit (or until deadline).
7. Close the replication connection. PG drops the temporary slot automatically; no final ack is required for correctness (the slot is going away). The standby-ack ticker may send one last update for clean metrics if it fires before close, but this is best-effort.
8. Close the admin connection.
9. Exit with code 0.

If the 10-second deadline expires, exit anyway. Clients will reconnect to a new instance and resync via snapshot.

k8s should set `terminationGracePeriodSeconds: 30` (more than our 10s deadline, with margin).

## 7.4. PG reconnection

PG disconnect is handled by a **two-track policy**: the service tries to recover fast and silently for short blips; k8s probes evict the pod for sustained outages. The crossover point is ~5 seconds.

**Service-side recovery (fast path).**
Reconnect with exponential backoff: 1s, 2s, 4s, ..., cap at 30s. On each attempt, repeat the slot-creation step from [§7.1](#71-startup-sequence) — a fresh temporary slot at the current WAL LSN. Increment `pg_reconnects_total`. Active SSE subscribers are **not closed** by the service. Their writer goroutines keep emitting heartbeats; tx events resume from the new slot's first COMMIT after reconnect.

**k8s-side eviction (slow path).**
The moment the PG connection drops, the service flips `pg_connection_status = 0`, which causes both probes to fail immediately:
- `/readyz` → 503: the Service controller removes this pod from endpoints. No new SSE handshakes arrive here.
- `/healthz` → 503: the kubelet `livenessProbe` (configured `periodSeconds=2, failureThreshold=3`, see [§10.5](10-resources-and-deployment.md#105-k8s-probes-and-lifecycle)) trips after ~5s and kills the pod. All TCP connections RST; clients hit `EventSource.onerror`, refetch a snapshot, and resubscribe (possibly on a different pod once one is ready).

**Resulting worst-case behavior.**
- **PG flap < 5s:** subscribers see a silent gap. In practice this gap usually contains 0 commits (the disconnect itself often coincides with PG being unable to commit). Pod survives, no client-visible impact.
- **PG outage ≥ 5s:** pod is killed, all clients drop into snapshot-and-resubscribe. Correct, and handled by infrastructure rather than app code.

The bounded silent-gap window is **~5 seconds** by design. If a stricter contract is ever required (zero silent gap), the service can emit `event: error\ndata: {"reason": "stream_reset"}` to every subscriber on PG disconnect before relying on probes; this is **not** the MVP behavior and should be added behind a config flag if needed.

## 7.5. Backpressure and lag handling

With a temporary slot, slot bloat across downtime is structurally impossible — the slot vanishes with the connection. What remains is **in-session lag**: the service is up but the router/writers can't drain WAL fast enough, so `confirmed_flush_lsn` trails `pg_current_wal_lsn()`. PG will retain WAL for our slot up to its own configured limits while we're connected.

Recovery is automatic in the normal case: backpressure ([§4.5](04-routing-and-wal-pipeline.md#45-backpressure)) pushes slow consumers out, freeing the router. If lag remains high after slow-consumer eviction, the bottleneck is upstream (router CPU, network) — capacity question, not an operational incident.

Operator-driven recovery (force-resync) is **never required** with a temporary slot: restarting the service drops the slot, releases all retained WAL, and clients refetch a snapshot on reconnect.
