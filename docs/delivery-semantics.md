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

## Ordering within a transaction

A single Postgres transaction is delivered as a single SSE event with
a `changes` array. Row changes inside that array appear in WAL order
(the order in which Postgres recorded them in the transaction). The
event's `commit_lsn` and `commit_ts` identify the transaction at the
WAL level; the `tx_id` matches Postgres's internal transaction
identifier.

This per-transaction grouping is the transactionally-atomic delivery
contract: a subscriber that receives the event has received a
self-consistent snapshot of the changes from that transaction. There
is no scenario in which a subscriber sees half a transaction.

## See also

See [ADR 0001: Delivery Semantics](./adr/0001-delivery-semantics.md) for the rationale.

- [Architecture](./architecture.md)
- [Auth model](./auth.md)
- [Operations](./operations.md)
