# Operations

## Running

Walera ships as a distroless, non-root image that listens on port 8080:

```bash
docker run --rm -p 8080:8080 \
  -e WALERA_DATABASE_URL='postgres://walera:secret@db.internal:5432/app' \
  -e WALERA_AUTH_URL='https://api.example.com/internal' \
  ghcr.io/slavnycoder/walera:latest
```

All configuration is in environment variables (see the
[README](../README.md#configuration)). Walera also ignores the variables
Kubernetes injects for a Service named `walera` (`WALERA_SERVICE_HOST`,
`WALERA_SERVICE_PORT…`, `WALERA_PORT…`); every other unknown `WALERA_*`
variable stops startup.

Instances are independent: each holds its own replication slot (and WAL
sender) and serves only its own subscribers.

The listener speaks HTTP/1.1 and unencrypted HTTP/2 (h2c) and has no TLS.
Put a reverse proxy in front for TLS, and disable response buffering for
`/sse/` there (Walera sends `X-Accel-Buffering: no` for nginx). Keep the
proxy's read timeout above the 15 s heartbeat interval.

Two flags exist besides the environment:

- `--healthcheck` — request `/healthz` on the local port (from
  `WALERA_ADDR`) and exit 0 on `200`, 1 otherwise. The image has no shell or
  curl, so use this for container health checks:

  ```yaml
  healthcheck:
    test: ["CMD", "/cdc-sse", "--healthcheck"]
    interval: 10s
  ```

- `--dev-log` — human-readable console logs instead of JSON.

## Health and readiness

| Endpoint | `200` | `503` |
| --- | --- | --- |
| `GET /healthz` | Replication connection is up. | Body `pg disconnected`. |
| `GET /readyz` | `{"status":"ok"}` | `{"reason":"pg disconnected","checked_at":"…"}` (or `not yet probed` right after start). |

Both report PostgreSQL only. `/readyz` serves the result of a probe that
runs every 5 s; `/healthz` checks on each request. Neither calls the auth
backend, so an auth outage does not take Walera out of rotation — it refuses
new streams with `503` instead.

When the replication connection drops, Walera reconnects by itself with
backoff (1 s doubling to 30 s, with jitter) and the endpoints turn `503`
meanwhile.

## Metrics

`GET /metrics` serves these, plus the standard Go runtime and process
collectors:

| Metric | Type | Labels |
| --- | --- | --- |
| `walera_subscribers_active` | gauge | `type`: `exact`, `wildcard` |
| `walera_events_sent_total` | counter | `type`: `exact`, `wildcard` — `tx` frames written |
| `walera_subscriber_disconnects_total` | counter | `reason`: `client_closed`, `slow_consumer`, `tx_too_large`, `shutdown`, `auth_revoked`, `auth_unavailable` |
| `walera_auth_requests_total` | counter | `result`: `ok`, `unauthorized`, `forbidden`, `not_found`, `unavailable` (handshakes and refreshes) |
| `walera_pg_connection_status` | gauge | `1` connected, `0` disconnected (updated every 5 s) |

Useful queries:

```promql
sum by (type) (walera_subscribers_active)
sum by (type) (rate(walera_events_sent_total[5m]))
sum by (reason) (rate(walera_subscriber_disconnects_total[5m]))
sum by (result) (rate(walera_auth_requests_total[5m]))
```

Example alerting rules:

```yaml
groups:
  - name: walera
    rules:
      - alert: WaleraPostgresDisconnected
        expr: walera_pg_connection_status == 0
        for: 1m
        annotations:
          summary: Walera has no replication connection; no changes are streamed.
      - alert: WaleraAuthBackendFailing
        expr: |
          sum(rate(walera_auth_requests_total{result="unavailable"}[5m]))
            / sum(rate(walera_auth_requests_total[5m])) > 0.05
        for: 5m
        annotations:
          summary: More than 5% of auth backend calls fail; streams are refused or dropped.
      - alert: WaleraSlowConsumers
        expr: increase(walera_subscriber_disconnects_total{reason="slow_consumer"}[15m]) > 10
        annotations:
          summary: Clients are being dropped for not keeping up.
```

## Shutdown

On `SIGTERM` or `SIGINT`, at the same time:

- the WAL reader stops and closes the replication connection, which drops
  the temporary slot; no further transactions are routed;
- the HTTP server stops accepting connections and waits up to 9 s for open
  streams to finish;
- every stream gets `event: shutdown` (`{"reason":"service_restart"}`) and
  closes, so clients reconnect elsewhere or later.

Finally the admin connection to PostgreSQL is closed. This normally takes well under a second. If it is not done after 10 s,
Walera exits with status 1. Give the container at least 10 s to stop (the
`docker stop` default).

## Fixed limits

Only `WALERA_MAX_CONNECTIONS` is configurable.

| Limit | Value | When exceeded |
| --- | --- | --- |
| Concurrent connections | `WALERA_MAX_CONNECTIONS` (50,000) | `503`, `Retry-After: 5` |
| Streams per `user_id` | 10 | `429` |
| Queued frames per row stream | 64 | stream ends with `slow_consumer` |
| Queued frames per table stream | 512 | stream ends with `slow_consumer` |
| Time to write a frame to a client | 5 s | stream ends with `slow_consumer` |
| Changes per transaction, per subscriber (after filtering) | 10,000 | stream ends with `tx_too_large` |
| Frame size | 10 MiB | stream ends with `tx_too_large`; an oversized `initial_data` is skipped |
| Heartbeat interval | 15 s | |
| Auth backend timeout | 2 s | handshake `503`; refresh retried |
| Auth response body | 64 KiB | treated as unavailable |
| Auth refresh retries | 1 s, 3 s, 9 s | stream ends with `auth_unavailable` |
| Request headers | 16 KiB | `431` |
| Shutdown | 10 s | exit status 1 |

## Logs

JSON lines on stderr. Logs carry identifiers — user ID, primary key, channel,
subscriber ID, client IP — but never row data, tokens, cookies, forwarded
header values or `initial_data`. Messages worth watching:

| Message | Level | Meaning |
| --- | --- | --- |
| `WAL reader: transient error; reconnecting` | warn | Replication connection lost. Changes until it is back are not delivered. |
| `standby status update failed` | warn | Could not confirm the WAL position to PostgreSQL; repeated, PostgreSQL may drop the connection. |
| `relation skipped: PK constraint not met` | warn | A published table has no usable primary key (see [docs/postgres.md](postgres.md#tables)); its changes are skipped. |
| `HandleInsert error; change skipped` (also `HandleUpdate`, `HandleDelete`) | warn | A change could not be decoded, usually because of the line above. |
| `subscriber dropped: slow_consumer` | warn | A client's queue filled up. |
| `subscriber dropped: tx_too_large` | warn | Too many changes in one transaction for this subscriber. |
| `subscriber dropped: tx_too_large (payload cap)` | warn | Frame over 10 MiB. |
| `subscriber tx dropped: multi_root (writer-side discipline violation)` | warn | A transaction touched two rows of a subscribed table; row subscribers skip it (see the [README](../README.md#writer-side-discipline)). |
| `auth permission refresh failed` | warn | A refresh attempt failed; `result` says how. |
| `auth: unexpected backend status` | warn | The auth backend answered something other than 200/401/403/404. |
| `auth: HTTP dispatch failed` | warn | The auth backend was unreachable or timed out. |
| `auth: response parse failed` | warn | A `200` body was not valid JSON or had no `user_id`. |
| `bootstrap: publication membership differs from WALERA_TABLES; reconcile manually via ALTER PUBLICATION` | warn | The publication and `WALERA_TABLES` disagree. |
| `WARNING: low replication slot headroom — risk of slot exhaustion` | warn | Fewer than 2 free replication slots at startup. |
| `shutdown: hard cap reached; forced exit` | error | Shutdown took longer than 10 s. |

`WALERA_LOG_LEVEL=debug` adds notes on ignored WAL messages (type, origin,
truncate) and cancelled standby updates; `info` (default) logs
each subscriber's connect and disconnect.
