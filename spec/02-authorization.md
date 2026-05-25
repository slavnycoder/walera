# 2. Authorization

## 2.1. Trust model

Clients send `Authorization: Bearer <token>` on SSE open. The service does NOT validate the token locally — it forwards the token verbatim to the auth backend, which is the source of truth for both identity and authorization.

JWT with local claims validation is rejected because:
- The field map depends on roles and runtime context — too large for claims.
- Revocation is not possible with stateless JWT.

## 2.2. Request contract

```
GET /auth/permissions?channel=users%3A42
Host: auth.internal
Authorization: Bearer <token>
X-Request-ID: <correlation id>
```

The channel is passed in the query string in full (e.g., `users:42` or `users:*`). The auth backend distinguishes exact and wildcard subscriptions and decides allowance per channel.

## 2.3. Response contract

**200 OK** — authorized. Single-entity view:
```json
{
  "user_id": "u_abc123",
  "tables": {
    "users": ["id", "name", "email"]
  },
  "roots": ["users"],
  "ttl_seconds": 60
}
```

Composite view (multi-table logical entity):
```json
{
  "user_id": "u_abc123",
  "tables": {
    "todo_list": ["id", "title", "updated_at"],
    "tasks":     ["id", "title", "done", "todo_list_id"]
  },
  "roots": ["todo_list"],
  "ttl_seconds": 60
}
```

- `user_id` is required. Used for per-user limits, metrics, logs. NEVER hash the token as a substitute.
- `tables` is the whitelist map for **all** tables visible to this user (root + children).
- `roots` is the subset of `tables` keys this user may **subscribe on**. Channels target root tables only ([§1.6](01-data-source-and-wal.md#16-entity-model), [§3.2](03-subscriptions-and-sse.md#32-subscription-handshake-handler-logic)). Children appear in `tables` so their fields can be filtered for delivery, but a handshake whose channel table is not in `roots` returns 403. Empty or missing `roots` is invalid — auth backend must declare at least one root if any subscription is allowed.
- `ttl_seconds` is optional. Overrides the default refresh interval.

**401 Unauthorized** — invalid token.
**403 Forbidden** — token valid, but no access to this channel: `{"reason": "not_allowed", "details": "..."}`.
**404 Not Found** — user deleted (optional; backend may return 401 instead).
**5xx** — retry with backoff.

## 2.4. HTTP client configuration

- Timeout: 2 seconds.
- Use a shared `http.Client` with tuned transport: `MaxIdleConns: 100`, `IdleConnTimeout: 90s`. Keep-alive avoids TLS handshake per request.
- mTLS recommended if both services are inside the same VPC.

## 2.5. Lifecycle

**On SSE open:** synchronous auth call. Translates to HTTP response of the SSE handshake:
- 401 from auth → 401 to client.
- 403 from auth → 403 to client.
- 5xx from auth → 503 to client with `Retry-After`.

**Periodic refresh:** ticker per subscriber, period = `ttl_seconds` (default 60s).
- **Explicit rejection (401/403/404):** disconnect immediately. Send `event: error\ndata: {"reason": "auth_revoked"}` if possible, then close TCP.
- **Network error / 5xx:** retry 2-3 times with exponential backoff (1s, 3s, 9s). On final failure, disconnect with `event: error\ndata: {"reason": "auth_unavailable"}`.

**Per-subscriber fail-open on refresh failure is forbidden.** If a specific subscriber's refresh exhausts its retry budget, that subscriber is disconnected — regardless of whether other subscribers are operating normally. This closes the revocation race for the individual subscriber and is the default safety posture.

A separate, **bounded** fail-open exists for systemic auth-backend outages, governed by the circuit breaker ([§2.6](#26-circuit-breaker)). The two cases are deliberately asymmetric:

| Failure shape | Policy | Rationale |
|---|---|---|
| Per-subscriber refresh failure (auth backend healthy) | **Fail-closed**: disconnect this subscriber | Revocation race is fully within our control; no system-wide harm in disconnecting one user |
| Systemic auth-backend outage (>50% failure rate) | **Bounded fail-open**: suspend refreshes globally; existing subscribers keep their current maps | Disconnecting 10k subscribers during an auth incident creates a thundering-herd on snapshot+auth backends, turning a partial outage into a total one. Explicit time-bound on stale permissions makes the trade-off accountable (see §2.6) |
| New connection during outage | **Fail-closed**: 503 to client | New opens are naturally rate-limited; no reason to accept them without auth confirmation |

## 2.6. Circuit breaker

The breaker bounds the systemic-outage fail-open in time. Track failure rate to the auth backend in a sliding window (e.g., 30s). If failure rate exceeds 50%, open the breaker:

- **Suspend per-subscriber refresh attempts.** Subscribers continue streaming with their **current** auth maps.
- **New SSE opens still attempt auth.** Their load is naturally bounded by client reconnect behavior, and an outage should not silently accept fresh, unauthenticated connections.
- **Cooldown** (e.g., 30s) → half-open: single trial request → on success, close the breaker.
- **On breaker close, trigger an immediate refresh** for every subscriber whose last successful refresh is older than 1× `ttl_seconds`, instead of waiting for their individual tickers. This compresses the residual stale-permissions window to roughly the auth-backend's recovery time, not the worst-case TTL.

### Trade-off: bounded stale-permissions window

A user whose access is revoked during a breaker-open window continues receiving events until either the breaker closes and their refresh runs, or their TCP connection drops for unrelated reasons.

**Worst-case stale window** (single-incident, no breaker re-trip):
`sliding_window + cooldown + half_open_probe_time + max(ttl_seconds) ≈ 30s + 30s + ~1s + 60s ≈ 2 minutes`.

This is the **explicit, documented cost** of preserving service availability during an auth incident. Per-subscriber fail-open during normal operation is still forbidden (§2.5); this is the only fail-open path, and it is bounded by the breaker FSM rather than indefinite.

### Observability

- `auth_circuit_breaker_state` (gauge 0/1, already in [§8.1](08-observability.md#81-prometheus-metrics)).
- `auth_breaker_stale_subscribers` (gauge): count of subscribers whose last successful refresh is older than `1.5 × ttl_seconds`. Should be 0 outside breaker-open windows; non-zero outside means refresh logic has a bug.

## 2.7. Field map semantics

The map is a **whitelist**. Deny-by-default.

Rules:
- **Table absent from map → 403** at subscription time; **disconnect** if a previously-present table is removed at refresh time.
- **PK is always included in event payload**, even if not in the whitelist. Without it, events cannot be correlated to client-side state and become useless.
  - For **exact subscriptions** (`users:42`) this is tautological — the client already knows the PK from the channel name. No new information disclosed.
  - For **wildcard subscriptions** (`users:*`) this **does** disclose the set of PKs in the table to the subscriber. This is **by design** — wildcards are a higher-trust mode intended for publicly-enumerable entities (currency rates, public catalogs, status feeds). The auth backend is the sole gatekeeper: approving an `entity:*` channel is an explicit acknowledgment that the table's PK space is safe to enumerate. See [§3.4](03-subscriptions-and-sse.md#34-wildcard-subscriptions).
- **Update where ALL changed columns are filtered out → drop the event silently** for this subscriber. Informing the client "something changed but you can't see what" is itself a leak.

## 2.8. Applying refreshed maps

The auth map is held per-subscriber as `atomic.Pointer[AuthMap]`. The refresh goroutine `Store`s the new pointer atomically. The router `Load`s once per transaction.

A given transaction is filtered by exactly one map version. Switching mid-transaction is not allowed; the router reads the pointer once and uses that snapshot for all changes in the tx.

If the new map makes the subscription impossible (table removed, or wildcard no longer allowed), close the connection with `event: error\ndata: {"reason": "auth_revoked"}`.
