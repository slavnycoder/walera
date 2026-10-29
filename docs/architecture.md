# Architecture

## Pipeline

```
PostgreSQL
    │  logical replication (pgoutput, temporary slot)
    ▼
┌──────────────────────────────┐
│ WAL reader          internal/wal
│  one goroutine owns the      │
│  replication connection:     │──── standby status every 5 s
│  receive, decode, build tx   │
└──────────────┬───────────────┘
               │ committed transactions (channel, 128)
               ▼
┌──────────────────────────────┐
│ Router           internal/router
│  find subscribers by         │
│  table:pk and table,         │
│  filter by whitelist,        │
│  encode frame, enqueue       │
└──────┬───────────────┬───────┘
       │ non-blocking  │
       ▼               ▼
┌─────────────┐  ┌─────────────┐
│ stream loop │  │ stream loop │   internal/sse, one per client
│ queue → SSE │  │ queue → SSE │   (the HTTP handler goroutine)
└─────────────┘  └─────────────┘
       ▲
       │ handshake and refresh: POST /auth/sessions   internal/auth
```

1. **WAL reader.** A single goroutine connects with `replication=database`,
   creates a temporary slot and receives `pgoutput` messages. It assembles
   `BEGIN … COMMIT` into one transaction, decodes values into JSON types and
   hands the committed transaction to the router.
2. **Router.** For each transaction it collects every row subscriber
   (`public.items:1`) and table subscriber (`public.items`) touched by any
   change, builds the subscriber's own copy (filtered by its whitelist),
   encodes one `tx` frame and offers it to the subscriber's queue. Offering
   never blocks.
3. **Stream loop.** Each client's HTTP handler goroutine owns its response.
   It writes queued frames (several at once when they pile up), heartbeats
   when idle, and the final `error` or `shutdown` frame.

## One owner for the replication connection

`pgconn.PgConn` is not safe for concurrent use, so only the reader goroutine
touches it. It wakes up at least every 5 s to send a standby status update.
When the router is busy and the hand-off channel is full, the reader keeps
sending those updates while it waits, so PostgreSQL does not hit
`wal_sender_timeout` and cut the connection. Back-pressure therefore slows
reading instead of losing the connection.

If the connection fails, the reader reconnects with backoff (1 s up to 30 s)
and a new temporary slot. Transactions committed in between are not
replayed.

## Filtering

The handshake answer is a whitelist: table → allowed columns. For every
subscriber and transaction the router:

- skips changes in other schemas and in tables the whitelist does not list;
- keeps only allowed columns, plus the primary key;
- drops an update left with no allowed column besides the key;
- sends nothing unless the subscriber's own row (or, for table streams, a
  row of its table) survived filtering;
- for row subscribers, sends nothing when the transaction changed two
  different rows of the subscribed table (`multi_root`), because the
  other row's children would otherwise reach this subscriber.

Each subscriber gets its own filtered copy; one subscriber's grant never
shapes another's frame.

## Refresh

Every TTL, each stream replays its handshake request with the client's
stored credentials. A successful answer replaces the whitelist; it applies
to transactions committed after the swap, while transactions already read
keep the previous grant. A denial, a different `user_id` or the table missing
from the grant ends the stream with `auth_revoked`. Transport failures are
retried three times (1 s, 3 s, 9 s) and then end it with `auth_unavailable`.
Frames still queued when a stream ends are discarded, so nothing sent under
an old grant leaks after a revocation.

## Slow consumers

A client gets a bounded queue (64 frames for a row stream, 512 for a table
stream) and a 5 s deadline for each write. A full queue or a missed deadline
ends that stream with `slow_consumer`. Nothing else waits for it: the router
never blocks on a client, and the WAL reader never waits on a single
subscriber.

## Shutdown

`SIGTERM` cancels the context every long-running goroutine shares, so the
WAL reader and the router's ingest loop stop at once; the replication
connection closes and the slot is dropped. In parallel the HTTP server stops
accepting connections and the router marks every subscriber `shutdown`
(later arrivals too); each stream loop writes `event: shutdown` and returns.
Then the admin connection closes. A 10 s timer forces exit if anything
hangs.

## Security decisions

- **Fail closed.** No credentials, no grant, an unreachable or confused
  auth backend, an unknown table: the stream is refused or ended. There is
  no cached or default grant and no circuit breaker that opens access.
- **Credentials do not linger.** After the handshake, `Authorization`,
  `Cookie` and forwarded headers are removed from the request; only the
  copy needed for refresh is kept. Redirects from the auth backend are not
  followed, so credentials never travel to another host. Plain `http://`
  to the backend needs an explicit opt-in.
- **Nothing sensitive in logs or errors.** Row data, tokens, cookies and
  secrets are never logged; primary keys and user IDs are. Configuration
  errors name the variable, not its value, and database URLs are redacted.
- **No broad publications.** `FOR ALL TABLES` and `FOR TABLES IN SCHEMA`
  are refused at startup: every streamed table is listed on purpose.
- **Strict configuration.** Unknown `WALERA_*` variables stop startup.
- **Bounded resources.** Connection limits, per-user limits, queue sizes,
  frame and response size caps keep one client from exhausting the process.

## Packages

| Package | Role |
| --- | --- |
| `cmd/cdc-sse` | Entry point, `--healthcheck`, `--dev-log`. |
| `internal/app` | Environment configuration, PostgreSQL startup checks, wiring, shutdown. |
| `internal/wal` | Replication reader, `pgoutput` decoding, value mapping. |
| `internal/walconn` | Admin connection and DSN types. |
| `internal/router` | Subscriber index, per-transaction routing and filtering. |
| `internal/sse` | HTTP handlers, handshake, stream loop, frame encoding, CORS. |
| `internal/auth` | Auth backend client, whitelist, refresh loop. |
| `internal/limits` | Global and per-user connection limits. |
| `internal/health` | `/healthz`, `/readyz`, `/metrics`. |
| `internal/metrics` | Prometheus registry. |
| `internal/config` | Error formatting and DSN redaction. |

`make deps-check` keeps the import graph one-way: `router` does not import
`auth` or `sse`, and `wal` does not import `sse`.
