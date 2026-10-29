# Walera

Walera streams PostgreSQL row changes to browsers over Server-Sent Events:
one stream per row (or per table), authorized by your own backend, with
every row filtered down to the columns the user may see.

```
PostgreSQL ──(logical replication)──▶ Walera ──(SSE)──▶ clients
                                        │
                                        └──▶ your auth backend (POST /auth/sessions)
```

It is a single Go binary with no broker and no extra storage. It targets small
projects that want live updates without building change-data-capture
plumbing, and it fails closed: no grant, no data.

## Quick start

You need Docker and Python 3.9+.

1. Start the example auth backend from [docs/auth.md](docs/auth.md#example-backend):
   save it as `auth.py` and run `python3 auth.py`. It listens on port 9000
   and accepts the token `alice-token`.

2. Start PostgreSQL (with a demo table `items`) and Walera:

   ```bash
   docker compose up
   ```

3. Subscribe to row 1 of `items`:

   ```bash
   curl -N -H "Authorization: Bearer alice-token" localhost:8080/sse/v1/items/1
   ```

4. In another terminal, write to the row:

   ```bash
   docker compose exec postgres psql -U walera -c "INSERT INTO items (title) VALUES ('buy milk')"
   docker compose exec postgres psql -U walera -c "UPDATE items SET done = true WHERE id = 1"
   ```

   The `curl` window prints:

   ```
   retry: 15000

   event: tx
   id: 756
   data: {"tx_id":756,"commit_ts":"2026-10-29T19:10:33.780755Z","changes":[{"op":"insert","table":"items","pk":"1","data":{"done":false,"id":1,"title":"buy milk"}}]}

   event: tx
   id: 757
   data: {"tx_id":757,"commit_ts":"2026-10-29T19:10:33.862668Z","changes":[{"op":"update","table":"items","pk":"1","data":{"done":true,"id":1,"title":"buy milk"}}]}
   ```

The compose file points Walera at `http://host.docker.internal:9000`. Set
`WALERA_AUTH_URL` (and `WALERA_TABLES` for your own tables) in your shell to
change that.

## Configuration

Walera reads environment variables only. There is no config file and no
command-line configuration.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `WALERA_DATABASE_URL` | yes | | PostgreSQL URL (`postgres://` or `postgresql://`; the key=value form is not accepted), e.g. `postgres://walera:secret@db:5432/app`. Walera adds `replication=database` itself for the replication connection. |
| `WALERA_AUTH_URL` | yes | | Base URL of your auth backend; Walera calls `<url>/auth/sessions`. Must be `https://` unless `WALERA_AUTH_ALLOW_PLAINTEXT=1`. |
| `WALERA_ADDR` | no | `:8080` | Listen address for SSE, health and metrics. |
| `WALERA_PUBLICATION` | no | `walera_pub` | Name of the publication to stream. An unquoted identifier (`[A-Za-z_][A-Za-z0-9_]{0,62}`), not `walera` (the slot name prefix). |
| `WALERA_TABLES` | no | empty | Comma-separated `schema.table` list. Used to create the publication when it does not exist; compared against it (warning only) when it does. |
| `WALERA_CORS_ORIGINS` | no | empty | Comma-separated origins allowed to read streams cross-origin, e.g. `https://app.example`. Empty means no CORS headers. |
| `WALERA_AUTH_TTL` | no | `60s` | How often a live stream re-checks its permissions. Go duration, at least `1s`. |
| `WALERA_AUTH_FORWARD_COOKIES` | no | empty | Comma-separated cookie names forwarded from the client to the auth backend. |
| `WALERA_AUTH_FORWARD_HEADERS` | no | empty | Comma-separated header names forwarded from the client to the auth backend. |
| `WALERA_AUTH_ALLOW_PLAINTEXT` | no | `0` | `1` allows an `http://` auth URL. For local development only. |
| `WALERA_MAX_CONNECTIONS` | no | `50000` | Maximum concurrent SSE connections; beyond it new ones get `503`. |
| `WALERA_LOG_LEVEL` | no | `info` | `debug`, `info`, `warn` or `error`. |

Startup fails on any other `WALERA_*` variable, so a typo or an outdated
name never silently falls back to a default. Configuration errors name the
variable but never print its value.

## Subscribing

| Endpoint | Streams |
| --- | --- |
| `GET /sse/v1/{table}/{pk}` | Transactions that touch one row. |
| `GET /sse/v1/{table}/all` | Transactions that touch any row of the table. |

`{table}` is a table in the `public` schema (lowercase, `[a-z_][a-z0-9_]*`,
at most 64 characters); `{pk}` is the primary-key value as text (at most 256
characters). A row whose primary key is literally `all` cannot be subscribed
to: that path is the table stream. The table must be in the publication and needs a single-column
primary key of type `smallint`, `integer`, `bigint`, `uuid` or `text`.

Request options:

- `Authorization: Bearer <token>` — passed to your auth backend. It may be
  omitted when a forwarded cookie or header carries the credentials
  ([docs/auth.md](docs/auth.md#forwarding-cookies-and-headers)).
- `X-Request-ID` — optional, up to 128 characters of `[A-Za-z0-9._-]`;
  forwarded to the auth backend. Walera generates one when absent.
- `?since_lsn=16/B374D848` — skip transactions committed at or before this
  WAL position. Default: the last position Walera has processed. It filters
  only; it cannot replay older changes.
- CORS: origins listed in `WALERA_CORS_ORIGINS` get
  `Access-Control-Allow-Origin` and `Access-Control-Allow-Credentials: true`.
  `OPTIONS` preflights are answered for both endpoints.

Browsers' `EventSource` cannot send an `Authorization` header. Use a
fetch-based client, or forward a session cookie with
`WALERA_AUTH_FORWARD_COOKIES`.

### Frames

The stream starts with `retry: 15000` (reconnect after 15 s). Then:

| Frame | Meaning |
| --- | --- |
| `event: initial_data` | Sent first, only if the auth backend returned `initial_data`. Its JSON value, compacted onto one line. |
| `event: tx` | One committed transaction. `id:` is the PostgreSQL transaction ID. |
| `event: error` | The stream is ending. `data: {"reason":"…"}`, see below. |
| `event: shutdown` | Walera is stopping. `data: {"reason":"service_restart"}`. Reconnect. |
| `:` | Heartbeat comment after 15 s without other frames. |

A `tx` frame:

```json
{
  "tx_id": 757,
  "commit_ts": "2026-10-29T19:10:33.862668Z",
  "changes": [
    {"op": "update", "table": "items", "pk": "1", "data": {"done": true, "id": 1, "title": "buy milk"}}
  ]
}
```

- `op` is `insert`, `update` or `delete`. `pk` is always a string.
- `data` holds only the columns the auth backend allowed, plus the primary
  key. For `update` it is the new row; columns whose large (TOASTed) value
  did not change are left out. `delete` has no `data`.
- Value types: `smallint`/`integer`/`real`/`double precision` become JSON
  numbers, `boolean` becomes a boolean, `json`/`jsonb` are embedded as JSON,
  timestamps become RFC 3339 UTC strings, `bytea` becomes base64, supported
  arrays become JSON arrays, and everything else (including `bigint` and
  `numeric`, to keep precision) is a string.
- An `update` that leaves no allowed column besides the primary key is not
  sent.

Error reasons:

| Reason | Cause |
| --- | --- |
| `auth_revoked` | A permission refresh denied the stream (401/403/404, table gone from the grant, or a different `user_id`). |
| `auth_unavailable` | A permission refresh kept failing after retries. |
| `slow_consumer` | The client did not keep up (its queue filled up). |
| `tx_too_large` | A transaction had more than 10,000 changes for this subscriber, or its frame exceeded 10 MiB. |

### Handshake errors

| Status | When |
| --- | --- |
| `400` | Invalid table or primary key (`{"error":"invalid_channel"}`), `since_lsn` (`invalid_since_lsn`) or `X-Request-ID` (`invalid_request_id`). |
| `401` | No credentials, or the auth backend answered `401` (its body is passed through). |
| `403` | The auth backend answered `403` (body passed through), or its grant does not include the table (`{"reason":"not_allowed"}`). |
| `404` | The auth backend answered `404` (body passed through). |
| `429` | The user already has 10 open streams. |
| `503` | `WALERA_MAX_CONNECTIONS` reached, or the auth backend is unreachable or answered anything else. Sent with `Retry-After: 5`. |

## Delivery guarantees

- **Atomic.** One `tx` frame per committed transaction, containing all of
  that transaction's changes the subscriber may see — never part of one.
- **Ordered.** Frames arrive in commit order. Nothing is delivered twice.
- **No resume.** Walera stores nothing. There is no `Last-Event-ID`
  replay; changes committed while a client is disconnected are lost to it.
  After every reconnect, reload state from your API, then apply new frames.
- **Gaps on replication reconnect.** If the replication connection drops,
  Walera reconnects with a fresh temporary slot. Transactions committed in
  between are not delivered, and open streams are not told. Watch
  `walera_pg_connection_status`.

A row subscriber receives a transaction when it touches that row. The frame
then also carries the transaction's other changes in the `public` schema that
the user may see — this is how child rows (an order's line items) reach the
order's subscribers.

### Writer-side discipline

1. **One root row per transaction.** A transaction that changes two
   different rows of the subscribed table (say `orders` 42 and 99) is not
   delivered to row subscribers of that table at all; Walera logs a
   `multi_root` warning and keeps the stream open. Table (`/all`)
   subscribers still receive it.
2. **Touch the root with its children.** A change to a child row reaches the
   root's subscribers only if the same transaction also changes the root row
   (an `updated_at` trigger on the child table does this).
3. **Do not mix children of different roots.** Walera does not follow
   foreign keys: in a transaction that touches `orders` 42 and a line item of
   order 99, the line item reaches order 42's subscribers. Split such
   writes.

## Authorization

Walera never decides access itself. On every subscription, and again every
`WALERA_AUTH_TTL` while it is open, it calls `POST <WALERA_AUTH_URL>/auth/sessions`
with the client's credentials and the channel (`items:1`). Your backend
answers with the user ID and, per table, the columns that user may see.
Anything else — a denial, an error, a timeout — closes or refuses the
stream. Contract and example backend: [docs/auth.md](docs/auth.md).

Table streams (`/all`) reveal every primary key that changes; grant them
only for publicly enumerable tables.

## PostgreSQL requirements

- PostgreSQL 14 or newer with `wal_level = logical`, connected directly (not
  through PgBouncer).
- A role with the `REPLICATION` attribute. Walera itself needs no `SELECT` on
  the published tables (it does not copy initial data).
- A publication listing tables explicitly (`FOR TABLE …`), or `WALERA_TABLES`
  so Walera creates one. `FOR ALL TABLES` and `FOR TABLES IN SCHEMA` are
  refused.

Details: [docs/postgres.md](docs/postgres.md).

## Health and metrics

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | `200` while the replication connection is up, otherwise `503`. |
| `GET /readyz` | Same check, cached and refreshed every 5 s. Does not call the auth backend. |
| `GET /metrics` | Prometheus metrics. |

Metrics: `walera_subscribers_active`, `walera_events_sent_total`,
`walera_subscriber_disconnects_total`, `walera_auth_requests_total`,
`walera_pg_connection_status`, plus the Go runtime and process collectors.
Alerts, limits, logs and shutdown: [docs/operations.md](docs/operations.md).
How it works inside: [docs/architecture.md](docs/architecture.md).

## Development

Go 1.26 and Docker (for integration tests).

```bash
make test                                       # unit tests with -race
make testbin/cdc-sse && make test-integration   # end-to-end against PostgreSQL in Docker
make lint                                       # golangci-lint
```

## License

MIT — see [LICENSE](LICENSE).
