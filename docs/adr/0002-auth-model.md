# ADR 0002: Auth Model

**Status:** Accepted
**Date:** 2026-05-22

## Decision

Walera delegates authorization to an external HTTP backend supplied
by the operator. Each subscription open triggers one call to that
backend, which returns an allow / deny decision plus a per-user
whitelist of accessible tables and fields. Walera does not
authenticate users itself; it only enforces the whitelist at fan-out
time.

## Context

Walera is a CDC delivery layer, not an identity provider. The target
audience is internal product teams whose authentication and
authorization systems already exist — JWT issuers, session stores,
RBAC engines, SSO integrations. Hard-coding an auth model in Walera
would force every adopter to either fork the service or wrap it in a
shim, both of which defeat the goal of a drop-in CDC gateway.

The auth backend is the single source of truth for "who can see
what." Walera's job is to honor that decision precisely, including
field-level whitelisting on the outgoing event stream.

## Options Considered

- **In-process JWT validation.** Rejected. Forces a single
  signing-key model and couples Walera to one identity provider. Any
  team using a different auth scheme would have to fork.
- **Internal RBAC store inside Walera.** Rejected. Out of scope —
  adds state to an otherwise stateless service and duplicates a
  system the operator already runs.
- **No auth (trust the network boundary).** Rejected. Row-level data
  is sensitive and the field-whitelist guarantee is a core value of
  the project. Trusting the network would make Walera unusable for
  any multi-tenant deployment.

## Consequences

- **Positive.** Operators integrate Walera with whatever auth they
  already run.
- **Positive.** The per-user field whitelist is enforced inside
  Walera at fan-out, so clients cannot bypass it by filtering only on
  the client side.
- **Negative.** Every subscription open costs one round trip to the
  auth backend, and every TTL-driven refresh costs another. Backend
  latency directly contributes to SSE time-to-first-event.
- **Operational.** Backend outages need a defined posture; that
  posture lives in [ADR 0003](./0003-slow-client-policy.md) and the
  circuit-breaker behavior described in [Auth](../auth.md#circuit-breaker-behavior).
- **Operational.** The auth backend is a hard production dependency
  and must be sized against the SSE-open rate it will see.

## See also

- [Auth](../auth.md)
- [Architecture overview](../architecture.md)
- [ADR 0003: Slow Client Policy](./0003-slow-client-policy.md)
