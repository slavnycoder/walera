# ADR 0001: Delivery Semantics

**Status:** Accepted
**Date:** 2026-05-22

## Decision

Walera delivers Postgres row changes to SSE subscribers at most once
per event. There is no client acknowledgement, no automatic retry, no
persistent event store, and no replay mechanism.

Walera does not replay missed events to disconnected clients. Clients must resynchronize through the primary API after reconnect.

## Context

Walera's scale target is ~10,000 concurrent SSE subscribers and ~5,000
WAL transactions per second on a single 4-CPU Kubernetes pod. At that
shape any per-subscriber durability — disk spool, in-memory ring,
external event log — would either dominate the memory budget or
require a separate infrastructure tier. The service is single-instance
greenfield and does not need to support cross-restart delivery to be
useful.

Clients of Walera already have a primary product API that owns the
data being streamed. They can recover from any delivery gap by
re-reading the affected entity. This is the natural recovery path and
does not require Walera to retain history.

## Options Considered

- **Persistent replication slot + `Last-Event-ID` resume.** Rejected.
  A persistent slot accumulates WAL during downtime; on restart Walera
  would replay accumulated changes to zero connected subscribers,
  which provides no client value while consuming PostgreSQL disk.
- **External durable event log (Kafka / Redis Streams) with replay.**
  Rejected. Adds an entire new infrastructure tier for a
  single-instance service whose target deployment is one
  Kubernetes pod. The complexity is out of proportion to the value.
- **In-memory ring buffer with bounded replay window.** Rejected.
  Gives clients ambiguous semantics — some events are replayable,
  others are not, depending on buffer occupancy. The
  resync-via-primary-API pattern is simpler and unambiguous.

## Consequences

- **Positive.** Operational simplicity. There is no durable event
  store to operate, back up, or scale alongside Walera.
- **Positive.** Memory footprint stays bounded. No per-subscriber
  replay buffer means the 10,000-subscriber target fits inside the
  pod's memory limit.
- **Negative.** Clients must implement reconnect + primary-API
  re-read logic. This is documented in `../delivery-semantics.md` and
  `../auth.md` so client teams know what to build.
- **Negative.** A fleet-wide reconnect storm after a Walera restart
  produces a synchronous load spike on the primary product API.
  Clients should mitigate with exponential backoff on reconnect.
- **Operational.** SSE clients must reconnect with jitter; the
  expected client pattern is documented in the delivery-semantics
  reference.

## See also

- [Delivery semantics](../delivery-semantics.md)
- [Architecture overview](../architecture.md)
