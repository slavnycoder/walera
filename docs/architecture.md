# Architecture

This document captures Walera's runtime architecture and the design
posture that follows from it. For the user-facing summary, start with
the project [README](../README.md); this file is the deeper reference
for operators and contributors.

## Purpose

Walera is a Go service that delivers authorized, field-filtered,
transactionally-atomic Postgres row changes to clients over Server-Sent
Events. A client subscribes to a channel of the form `entity_name:id`;
Walera authorizes the subscription against an external auth backend
(which returns a per-user whitelist of accessible tables and fields),
tails the WAL via `pgoutput` logical replication, and delivers only the
relevant changes filtered down to the allowed fields.

The target audience is internal product teams that need real-time
per-entity push without writing bespoke CDC plumbing, operating Kafka,
or running a Debezium cluster. A single Walera pod is designed to sustain
roughly 5,000 WAL transactions per second and roughly 10,000 concurrent
SSE subscribers on a 4-CPU instance, with no client-visible duplicate or
out-of-order events.

Walera is a notification gateway, not a durable event store. It is a
delivery layer, not an identity provider. Both boundaries are deliberate
and load-bearing — see [Non-goals](#non-goals) below.

## Non-goals

Walera intentionally does not do the following. These boundaries keep
the runtime small enough to fit on a single pod and keep the failure
model honest.

- **No durable event store.** Walera does not persist events. Once an
  event has been fanned out to subscribers, it is gone.
- **No replay of historical events.** There is no `Last-Event-ID`
  resume, no per-subscriber spool, no in-memory ring buffer with
  bounded replay. Reconnecting clients resync via the primary API.
- **No guaranteed delivery to disconnected clients.** Subscribers that
  are not currently connected do not receive events that occur during
  the disconnect.
- **No client-side filtering.** Field whitelisting is enforced at
  fan-out time inside Walera. Clients cannot opt out of the whitelist
  or request fields not granted to them.
- **No identity provider behavior.** Walera does not issue tokens,
  manage users, or store credentials. Authorization is delegated to an
  external HTTP backend operated by the same team that owns the data.
- **No multi-instance scale-out in this revision.** Broadcaster and
  router interfaces are designed so a NATS or Redis Streams
  implementation can replace the in-process fan-out, but the shipped
  implementation runs as a single Kubernetes pod.
- **No PgBouncer in the replication path.** PgBouncer does not support
  the PostgreSQL replication protocol; Walera must connect directly to
  PostgreSQL.
- **No `wal2json` or Debezium.** Walera uses the built-in `pgoutput`
  logical decoding plugin via `pglogrepl`, which works on managed
  Postgres offerings without server-side extension installation.

## Delivery semantics

Walera delivers each Postgres row change to each authorized subscriber
at most once. Changes from a single Postgres transaction are grouped
into a single SSE event, so within a transaction subscribers see
WAL-order updates atomically. Across transactions, ordering reflects
the WAL's commit order.

The runtime never replays events: clients that lose their connection
miss the events that occur during the outage and are expected to
recover by re-reading the affected entity from the primary product API
after reconnect.

See [Delivery semantics](./delivery-semantics.md) for the complete
specification, including what Walera guarantees, what it does not
guarantee, and the expected client recovery pattern.

## Failure model

Walera's runtime fails in three places: the auth backend, the
PostgreSQL replication connection, and the per-subscriber send path.
Each has a defined posture.

- **Auth backend unavailable.** A circuit breaker around the auth
  client trips when the failure rate over a sliding window crosses a
  threshold. While the breaker is open, Walera takes a bounded
  fail-open posture for already-established subscriptions (existing
  streams continue to receive events as long as their last successful
  permission map is still within its TTL) and a fail-closed posture for
  new subscription attempts (new SSE opens are rejected with `503`).
  See [ADR 0002: Auth Model](./adr/0002-auth-model.md) for the
  rationale.
- **PostgreSQL replication slot dropped or unreachable.** Walera uses a
  temporary replication slot that is created at startup and dropped
  automatically when the replication connection closes. If the
  connection is lost, Walera reconnects, creates a fresh slot, and
  resumes from the current WAL position. Subscribers experience a brief
  delivery gap and recover by resyncing through the primary API. See
  [ADR 0004: Replication Slot Policy](./adr/0004-replication-slot-policy.md).
- **Subscriber falls behind.** Each subscriber has a small bounded
  send queue. A subscriber that cannot drain at WAL pace is
  disconnected rather than buffered indefinitely. After disconnect, the
  client is expected to reconnect and resync from the primary API. A
  Prometheus counter exposes the slow-client disconnect rate. See
  [ADR 0003: Slow Client Policy](./adr/0003-slow-client-policy.md).

In all three cases the failure mode is explicit: subscribers either
receive the event or are disconnected with a defined reason. There is
no silent partial delivery, no out-of-order replay, and no per-client
spool that could leak memory under sustained backpressure.

## Security model

Walera does not authenticate users. Each SSE request carries an
operator-issued bearer token; Walera forwards that token to an external
auth backend on every subscription open and receives back a per-user
whitelist of accessible tables and fields. Only whitelisted fields are
forwarded to the subscriber, and whitelisting is enforced at fan-out
time inside Walera — clients cannot bypass it.

Tokens and row payloads are never logged. Primary-key values are logged
when they are needed to identify a subscription (they are identifiers,
not content), but column values are not.

Wildcard streams (subscriptions of the form `entity:*`) bypass the
per-row identifier check and are governed by a separate, more
restrictive posture documented in [Auth](./auth.md#wildcard-streams).
That document is the canonical location for the wildcard-stream policy;
it is not duplicated here.

For the full auth backend contract, status codes, circuit-breaker
behavior, and an example backend implementation, see
[Auth](./auth.md).

## Runtime components

A single Walera pod runs the following components in-process:

- **Replication consumer.** Connects to PostgreSQL with the
  replication protocol, creates a temporary slot, and decodes
  `pgoutput` messages (`RelationMessage`, `InsertMessage`,
  `UpdateMessage`, `DeleteMessage`) using `pglogrepl`. Maintains a
  relation cache for schema mapping.
- **Router / broadcaster.** Maps decoded changes to subscriber sets
  through a sharded subscription index keyed on `entity:id`. Hashing
  uses `cespare/xxhash/v2` for low-allocation shard selection. Designed
  to be swappable for an out-of-process broadcaster (NATS / Redis
  Streams) without changing the producer or consumer surfaces.
- **SSE handler.** A `net/http` stdlib mux serves `/sse/v1/{table}/{pk}`
  and `/sse/v1/{table}/all`. Each connection runs a small bounded-queue
  writer that applies the per-subscriber field whitelist and frames
  events for SSE transport.
- **Auth client.** Calls the external auth backend on subscription
  open and refreshes permission maps periodically. Wrapped in a custom
  circuit-breaker FSM that distinguishes new opens from established
  streams.
- **Metrics endpoint.** Exposes Prometheus counters, gauges, and
  histograms (subscriber count, WAL lag, auth failure rate, fan-out
  latency, slow-client drops, etc.) on `/metrics`.
- **Health endpoints.** `GET /healthz` (liveness, WAL connected) and
  `GET /readyz` (PostgreSQL + auth healthy).

Production dependency versions are pinned in `go.mod`. The locked
choices are summarized in the project root `CLAUDE.md` under
"Production Dependencies"; that table is the source of truth for
upstream library selection.

## Composition Root — Hand-Wired vs Codegen DI

The singleton graph that runs inside a Walera pod is constructed by a
single hand-written function, `app.InitializeApp`, defined in
`internal/app/initialize.go`. There is no `wire_gen.go`, no `//go:build
wireinject` stub, and no provider set. The construction order is
exactly the order of the lines in that file. This section records why
v2.3 chose this shape, what discipline from the v2.2 wire era it
preserves anyway, and the concrete thresholds that would justify
returning to compile-time DI codegen.

### Decision

v2.3 replaced `google/wire` with a hand-wired composition root because
the team's primary axis is readability: `InitializeApp` now reads
top-to-bottom in one file (~200 LOC, with the runnables-and-servers
sub-machinery factored into `internal/app/runnables.go` to keep the
wiring spine under the soft 200 LOC ceiling). A reader does not need
to know about build tags, jump between a `wire.go` panic stub and a
`DO NOT EDIT` `wire_gen.go`, or hold the provider-set graph in their
head — they read the file. Wire's compile-time gate did pay off in
v2.2: it forced the named-type discipline now living in
`internal/walconn`, drove the DI-03 cycle break between `auth.Client`
and `auth.Breaker`, and locked in the adapter-at-consumer pattern
(`sse.PoolMetricsAdapter`). Once that discipline had crystallised in
the code, wire's ongoing cognitive cost — two files per symbol, a
generated indirection layer, wire-tax types like `MainHTTPServer` and
`walBundle` that existed only as wire keys, a hidden cycle break
inside `NewBreaker`, and a reserved-for-future `ctx` parameter on
`InitializeApp` — outweighed the gate it provided.

The DI-03 cycle now appears as three explicit, sequential lines at
the call site:

```go
authClient := auth.New(cfg.Auth, auth.Deps{Breaker: nil, ...})
breaker := auth.NewBreaker(cfg.Auth.Breaker, auth.BreakerDeps{Prober: authClient, ...})
authClient.SetBreaker(breaker) // init-only; guarded by sync.Once
```

The cycle is no longer hidden inside a constructor's post-init hook —
it is visible to anyone reading `initialize.go`.

Two contract changes that previously lived as `WR-03` / `WR-05`
comment markers in the wire era are now encoded directly in the
`InitializeApp` signature:

- **`AppConfig` is passed by value, not by pointer.** The signature is
  `func InitializeApp(cfg AppConfig, ...) (*App, func(), error)`. A
  by-value parameter cannot be mutated by the caller after the call
  returns; the read-only contract that wire-era code documented in
  prose is now a property of the type system. `PrepareDatabase` still
  takes `*AppConfig` because it runs DB I/O against the caller's
  config; `InitializeApp` captures a snapshot at call time.
- **No `ctx context.Context` parameter.** The wire-era signature
  reserved a `ctx` for hypothetical future constructors. No
  constructor in the graph consumes it today, and reserved-for-future
  parameters violate the project's "don't design for hypothetical
  requirements" rule. When a constructor actually needs cancellation
  it will be added at that point.

### Wire-era discipline preserved

Several patterns that v2.2 wire pressure introduced are good Go
regardless of DI strategy, and v2.3 keeps every one of them. They are
not wire artefacts; they are semantic-boundary discipline that
survives wire's removal.

- **Named types in `internal/walconn`.** `AdminConn`, `ReplicationConn`,
  `AdminDSN`, `ReplicationDSN`, `SlotName`, and `PublicationName`
  remain distinct types. They were originally introduced to give wire
  unambiguous keys when two `*pgx.Conn` values had different roles;
  they stay because they encode a semantic boundary in the type
  system.
- **`auth.Prober` interface and the two-step cycle break.** The
  `auth.New` / `auth.NewBreaker(BreakerDeps{Prober: client, ...})` /
  `client.SetBreaker(breaker)` sequence (DI-03) is now three explicit
  lines in `InitializeApp` instead of one hidden hook inside
  `NewBreaker`. The interface stays — it documents that the breaker
  only needs the `CheckAuth` capability of the client, not the full
  client surface.
- **`sse.PoolMetricsAdapter` at the consumer.** DI-04 placed the
  metrics adapter inside `internal/sse/` (the package that needs it),
  not in the metrics producer. That remains the right shape: the
  adapter lives next to the interface it adapts to.
- **`[]Runnable` lifecycle and explicit cleanup chain.** The runnable
  abstraction (`runnable.go`) and the 5-step shutdown wave
  (`lifecycle.go`) are independent of the DI strategy; they composed
  cleanly under wire and compose cleanly under hand-wired
  construction. The 5 `safego.Go` call sites inside `internal/app/`
  remain the audited concurrency surface.

### Exit criteria for returning to codegen DI

Hand-wired composition is a deliberate bet on the current shape of
the graph. If the graph grows in specific ways, codegen DI becomes
worth its cost again. The following are the concrete signals at
which `internal/app/` should re-evaluate.

- **`internal/app/initialize.go` body grows past ~150 LOC.** The file
  is 197 LOC today (200 LOC is the soft ceiling). The runnables and
  HTTP-server factories were extracted to `runnables.go` (254 LOC)
  specifically to keep the wiring spine readable in one screen.
  Sustained drift of the `InitializeApp` body itself above ~150 LOC,
  even after reasonable extraction, is the first signal that the
  graph has outgrown a single-file hand-wired root.
- **Singleton graph node count exceeds ~25.** The graph today has
  19 long-lived nodes (counted from `InitializeApp`'s constructor
  calls). Past ~25 the by-hand topological sort starts to be a
  liability rather than a feature, and the compile-time check codegen
  DI provides starts to be worth its weight again.
- **A second injector appears.** Today the only construction site is
  `InitializeApp` itself; unit tests in `internal/app/*_test.go` use
  direct construction rather than a parallel test injector. If an
  `apptest/` subpackage emerges that duplicates the construction
  logic for test scaffolding, the duplication is the kind of problem
  codegen DI solves well, and the return-on-investment for compile-
  time wiring changes.

Until those signals fire, `internal/app/initialize.go` is the
composition root, full stop.

## Operational assumptions

Walera is designed against a deliberate, narrow set of operational
assumptions. Deviations should be treated as configuration errors and
fixed at the platform layer, not papered over in Walera.

- **PostgreSQL ≥ 14.** Older versions lack the `pgoutput` features
  Walera relies on.
- **`wal_level = logical`.** Walera verifies this at startup and
  refuses to run if it is not set.
- **DBA-owned publication.** The publication is created and maintained
  by the database team; Walera reads it. Bootstrap helpers exist for
  development environments.
- **Replication user with `REPLICATION` attribute.** Required by the
  replication protocol.
- **No PgBouncer in the replication path.** PgBouncer cannot proxy the
  replication protocol; Walera must connect directly.
- **Single Kubernetes instance.** One pod per environment. Requests
  are 2 CPU / 4 GiB, limits 4 CPU / 8 GiB, with
  `terminationGracePeriodSeconds: 30`. Liveness probe runs every 2
  seconds with `failureThreshold: 3`.
- **Race-clean.** Walera ships only after `go test -race ./...` is
  clean; the runtime is verified concurrency-safe at build time.
- **Stateless across restarts.** Walera does not persist runtime
  state. A restart drops the temporary replication slot and reconnects
  fresh; subscribers reconnect and resync through the primary API.

## See also

- [Delivery semantics](./delivery-semantics.md)
- [Auth model](./auth.md)
- [Operations](./operations.md)
