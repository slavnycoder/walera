# Delivery semantics

This document specifies what Walera guarantees about event delivery to
SSE subscribers, what it does not guarantee, and the recovery pattern
clients are expected to implement.

## Summary

Walera delivers Postgres row changes to SSE subscribers at most once
per event. There is no client acknowledgement, no automatic retry, no
persistent event store, and no replay. Events that occur while a
subscriber is disconnected are not delivered to that subscriber on
reconnect. Clients recover by re-reading the affected entity from the
primary product API.

## At-most-once delivery

Every change emitted to a subscriber appears at most once. There is no
deduplication required on the client side, but there is also no
mechanism for redelivery: once Walera has written an event to a
subscriber's SSE connection, the event is no longer retained anywhere
addressable to that subscriber.

There is no acknowledgement protocol between Walera and the
subscriber. Walera writes the event into the SSE framing and then
discards it. If the subscriber is connected, the event is delivered;
if the subscriber is not connected (TCP closed, slow-client disconnect,
process gone), the event is lost from that subscriber's perspective.

There is no retry. Walera does not attempt to redeliver an event that
failed to write, and there is no out-of-band channel for catch-up.

## What we guarantee

- **Per-transaction atomicity.** Each Postgres transaction produces
  exactly one SSE event. All row changes from that transaction appear
  in the event's `changes` array; partial transactions are not
  emitted.
- **Ordering within a transaction.** Row changes within a single
  transaction are listed in WAL order inside the SSE event.
- **Ordering across transactions.** Events arrive at a subscriber in
  Postgres commit order. There is no reordering across transactions.
- **Field whitelisting at fan-out.** The subscriber's permission map
  is applied inside Walera before the event is written. Only
  whitelisted columns appear in the payload.
- **At-most-once delivery to currently-connected subscribers.** A
  given event is written to a given subscriber's connection no more
  than once.

## What we do not guarantee

- **No durable event store.** Walera does not persist events to disk
  or to any external store.
- **No replay.** There is no `Last-Event-ID` resume, no per-subscriber
  spool, and no in-memory ring buffer with bounded replay.
- **No across-restart delivery.** A Walera restart drops the temporary
  replication slot and reconnects fresh; events committed while
  Walera was down are not backfilled.
- **No delivery to disconnected subscribers.** Subscribers that are
  not currently connected miss events that occur during the
  disconnect.
- **No client acknowledgement protocol.** Walera does not know whether
  a given event was actually rendered by the client; it only knows
  whether the write to the SSE connection succeeded.
- **No guaranteed delivery during backpressure.** Subscribers that
  cannot drain at WAL pace are disconnected per the slow-client
  policy; events written after the bounded queue fills are not
  buffered indefinitely.

## Reconnection and resync

Walera does not replay missed events to disconnected clients. Clients must resynchronize through the primary API after reconnect.

This is the load-bearing posture of the entire delivery model. The
operational implication for client teams is straightforward but
important: any time the SSE connection drops — network blip,
slow-client disconnect, server restart, deploy — the client is
expected to re-read the affected entity from the same product API
that wrote the data. The SSE stream is an event-driven supplement to
the primary API, not a replacement for it.

The recommended client pattern is:

1. On initial mount, load current state from the product API.
2. Open the Walera SSE stream and apply incoming events to that state.
3. On any SSE disconnect or `shutdown` event, repeat step 1 before
   resubscribing — do not assume the local state is current.

Clients should implement exponential backoff on reconnect to avoid
fleet-wide reconnect storms after a Walera restart or a network
event.

## Initial data frame

If the auth backend's open-time permission response includes an
`initial_data` field, Walera emits its raw JSON value to the subscriber
as a single `event: initial_data` SSE frame **before** any `tx` events.
The frame is optional, opaque to Walera, and emitted at most once per
SSE connection (open-time map only — not re-emitted on permission
refresh). A payload exceeding `max_payload_bytes` is dropped with a
warning and the stream opens normally without the frame.

See [Auth — Initial data payload](./auth.md#initial-data-payload) for
the contract.

## Ordering within a transaction

A single Postgres transaction is delivered as a single SSE event with
a `changes` array. Row changes inside that array appear in WAL order
(the order in which Postgres recorded them in the transaction). The
event's `commit_ts` identifies the transaction's commit wall-clock;
the `tx_id` matches Postgres's internal transaction identifier. The
Postgres commit LSN is intentionally not exposed on the wire — it is
a physical WAL offset with no client-visible semantics (Walera does
not honour `Last-Event-ID` on reconnect).

This per-transaction grouping is the transactionally-atomic delivery
contract: a subscriber that receives the event has received a
self-consistent snapshot of the changes from that transaction. There
is no scenario in which a subscriber sees half a transaction.

## Transaction-scoped delivery (v2.4)

### Anchor matching and co-transactional delivery

When a committed Postgres transaction contains a change that matches a
subscriber's channel (an exact `schema.table:pk` match or a wildcard
`schema.table` match), that subscriber's channel is said to be
*anchored* to that transaction. Walera then delivers all whitelisted
changes from the entire matched transaction to that subscriber in a
single, commit-ordered SSE event.

This is called *transaction-scoped delivery*: the unit of delivery is
the whole Postgres transaction, not the individual row change. The
`changes` array in the SSE payload contains every whitelisted row
change from the transaction, in WAL order.

### Contract points (D-04)

**1. Relatedness is the application's responsibility.**
Table relatedness is the application's responsibility — Walera does
not infer relationships between tables. If a transaction contains
changes to both `todo_list` and `tasks`, they are delivered together
only because the application co-wrote them in a single Postgres
transaction. To receive related row changes in one atomic event,
co-write them in the same `BEGIN` / `COMMIT` block. Walera delivers
what Postgres committed together; it cannot synthesize atomicity
across separate transactions.

**2. The whitelist is the sole authorization gate.**
A co-transactional table is delivered only if that table is present
in the subscriber's per-user field whitelist. The anchor table itself
must produce at least one post-filter anchor change to authorize the
channel. If a raw channel match is later removed by field filtering
(for example, an UPDATE changed only hidden columns), Walera silently
skips the whole event for that subscriber, even if other tables in the
transaction are whitelisted. Authorization is enforced inside Walera at
fan-out time; no other gate exists.

**3. Walera does not route by foreign keys.**
There is no foreign-key resolution, join inference, or relationship
traversal. Walera does not follow FK references between tables and
does not route by foreign keys. The only routing signal is the set of
`schema.table:pk` and `schema.table` patterns that a subscriber's
channel matches. Any relational structure implied by FK constraints in
the schema is invisible to Walera at dispatch time.

### Worked example

A `todo_list:42` subscriber whose whitelist grants access to both
`todo_list` and `tasks` receives the following when a single Postgres
transaction updates `todo_list` row 42 and inserts a new `tasks` row
in the same `BEGIN` / `COMMIT`:

```json
{
  "tx_id": 735,
  "commit_ts": "2026-05-18T08:30:12.123456Z",
  "changes": [
    {"op": "update", "table": "todo_list", "pk": "42", "data": {"status": "active"}},
    {"op": "insert", "table": "tasks",     "pk": "17", "data": {"todo_id": 42, "title": "Draft spec"}}
  ]
}
```

The `todo_list:42` change anchors the transaction to this subscriber.
The `tasks` INSERT is co-transactional: it is in the same committed
transaction, and `tasks` is present in the subscriber's whitelist, so
it is included in the same atomic event. A `todo_list:42` subscriber
whose whitelist does not include `tasks` would receive only the
`todo_list:42` change.

This is the originating Phase 1 scenario (integration test
`Test16TxScopedDelivery / CoTxTasksDeliveredWithAnchor`).

### Observability

Two Prometheus metrics expose the co-transactional delivery volume:

- **`walera_tx_fan_out_work`** (histogram): per-transaction sum of
  post-filter delivered changes across all eligible subscribers.
  Captures the total fan-out work distribution per transaction.
- **`walera_co_tx_beyond_anchor_total`** (counter): cumulative count
  of changes delivered to subscribers from matched transactions beyond
  the subscriber's own anchor-matched key(s). This is the incremental
  delivery volume that whole-transaction delivery adds on top of what
  per-change matching alone would have delivered. Observe-only —
  Walera does not enforce a hard cap.

Both metrics are pre-touched to 0 at startup so the series is visible
from the first Prometheus scrape even before the first transaction
is delivered.

## See also

See [ADR 0001: Delivery Semantics](./adr/0001-delivery-semantics.md) for the rationale.

- [Architecture](./architecture.md)
- [Auth model](./auth.md)
- [Operations](./operations.md)
