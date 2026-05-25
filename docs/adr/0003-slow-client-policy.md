# ADR 0003: Slow Client Policy

**Status:** Accepted
**Date:** 2026-05-22

## Decision

SSE subscribers that cannot drain at WAL pace are disconnected. Each
subscriber has a small bounded send queue; there is no per-subscriber
buffering beyond that. After disconnect, clients are expected to
reconnect and resync through the primary API per ADR 0001.

## Context

Walera targets ~10,000 concurrent SSE subscribers and ~5,000 WAL
transactions per second on a 2 CPU / 4 GiB Kubernetes pod. With that
ratio, any per-subscriber unbounded buffer would exhaust memory in
seconds during a client-side backpressure event (network throttle,
sluggish render loop, paused tab). The bounded-queue plus
disconnect-on-overflow posture keeps the per-subscriber memory
footprint predictable.

The explicit disconnect is also the clearest possible signal to the
client. A subscriber that is silently dropping events has no way to
know it is no longer current; a subscriber that has been disconnected
knows unambiguously that it must resync.

## Options Considered

- **Unbounded per-subscriber buffer.** Rejected. Out-of-memory risk
  under any sustained backpressure; the pod's memory limit would be
  breached well before the subscriber recovered.
- **Persistent per-subscriber spool to disk.** Rejected. Turns
  Walera into a durable event store, which violates the posture
  established in [ADR 0001](./0001-delivery-semantics.md) and forces
  Walera to manage per-subscriber disk lifecycle.
- **Drop oldest events silently.** Rejected. Gives clients partial
  or out-of-order data with no signal that they have missed events.
  An explicit disconnect is honest; silent dropping is not.

## Consequences

- **Positive.** Memory footprint per subscriber is bounded and
  predictable; the 10,000-subscriber target stays inside the pod's
  memory budget.
- **Positive.** The failure mode is explicit. Clients always know
  whether their stream is current or whether they need to resync.
- **Negative.** Slow clients (mobile networks, throttled CPUs,
  background tabs) reconnect frequently, increasing primary-API
  resync load.
- **Operational.** A Prometheus counter exposes the slow-client
  disconnect rate. A sustained increase indicates either a
  client-side regression or a WAL pace that has outgrown the
  per-subscriber bandwidth budget.

## See also

- [Auth](../auth.md)
- [Operations](../operations.md)
- [ADR 0001: Delivery Semantics](./0001-delivery-semantics.md)
