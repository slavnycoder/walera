# ADR 0004: Replication Slot Policy

**Status:** Accepted
**Date:** 2026-05-22

## Decision

Walera uses a temporary PostgreSQL replication slot
(`CREATE_REPLICATION_SLOT ... TEMPORARY`) that is created when the
replication connection opens and dropped automatically when that
connection closes. No persistent slot is created across restarts.

## Context

A persistent slot would accumulate WAL on the PostgreSQL side
whenever Walera is down. On restart, Walera would then replay all
accumulated changes — but the delivery-semantics posture established
in [ADR 0001](./0001-delivery-semantics.md) means subscribers do not
benefit from replayed events (they have already resynced via the
primary API). The replay would be pure PostgreSQL disk consumption
with no client value.

Temporary slots drop cleanly on disconnect, so there is no
operational artifact to clean up if a Walera deployment is removed.
This matches the stateless-across-restarts shape of the service.

## Options Considered

- **Persistent replication slot.** Rejected. WAL accumulation during
  Walera downtime; manual slot cleanup required when Walera is
  decommissioned; replay-on-restart provides no client value given
  the delivery-semantics posture in ADR 0001.
- **Logical decoding via SQL function calls
  (`pg_logical_slot_get_changes`).** Rejected. Cannot meet the 5,000
  transactions per second throughput target; the replication protocol
  via `pglogrepl` is required for performance.
- **`wal2json` or Debezium.** Rejected. `wal2json` requires a
  PostgreSQL extension that is unavailable on managed Postgres
  offerings; Debezium adds a Kafka cluster and a JVM service to a
  single-instance Go runtime. Both are explicitly out of scope for
  this project.

## Consequences

- **Positive.** No WAL accumulation during Walera downtime.
  PostgreSQL disk usage stays bounded regardless of Walera uptime.
- **Positive.** Slot cleanup is automatic. Operators do not need to
  manually drop slots if Walera is decommissioned or replaced.
- **Negative.** A Walera restart causes a brief delivery gap.
  Subscribers reconnect and resync per ADR 0001.
- **Operational.** The replication user must have the `REPLICATION`
  attribute, and Walera must connect directly to PostgreSQL — no
  PgBouncer in the replication path.
- **Operational.** WAL lifecycle tests verify the slot reuse and
  reconnect behavior end-to-end against a real PostgreSQL instance.

## See also

- [Operations](../operations.md)
- [Architecture overview](../architecture.md)
- [ADR 0001: Delivery Semantics](./0001-delivery-semantics.md)
