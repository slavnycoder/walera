# Auth

Walera delegates all authorization decisions to an external HTTP
backend that the operator supplies. This document describes the
contract Walera expects from that backend, the request and response
formats, status-code handling, the wildcard-stream policy, and a
minimal example implementation.

## Overview

Walera is not an identity provider and does not authenticate users on
its own. Each SSE subscription request carries a bearer token in the
`Authorization` header; Walera forwards that token (and the requested
channel) to the configured auth backend and waits for a permission
decision before opening the stream.

The permission decision returns a per-user whitelist of accessible
tables and fields. Walera enforces the whitelist at fan-out time inside
the SSE writer — only whitelisted fields are forwarded to the
subscriber. Clients cannot opt out of the whitelist or request fields
they were not granted.

## Backend contract

The auth backend is a plain HTTP service operated by the same team
that owns the underlying data. Walera calls it once per subscription
open and again periodically while the subscription is live (to refresh
the permission map). The contract is intentionally minimal: one
endpoint, one request shape, one response shape.

Walera sends:

- The bearer token the client supplied (`Authorization: Bearer …`),
  when present.
- The requested channel, encoded as `entity:id` or `entity:all` for
  wildcard streams.
- A request identifier and `Accept: application/json`.
- Optionally, a configured allowlist of the client's cookies and
  headers forwarded verbatim from the SSE handshake — see
  [Forwarding cookies and headers](#forwarding-cookies-and-headers).

The backend returns:

- `200` with a permission map (allow), or
- `401` / `403` / `404` (deny — with semantics described under
  [Status codes](#status-codes)), or
- `5xx` or a timeout (treated as a transport failure; see
  [Circuit breaker behavior](#circuit-breaker-behavior)).

## Forwarding cookies and headers

Some auth backends key their decision on credentials the client carries
outside the bearer token — a session cookie, a tenant header, an
`X-Forwarded-For` chain. Walera can thread a configured allowlist of the
client's cookies and headers from the SSE handshake into the
**session-open** call to the backend.

Two config keys control this, both **name allowlists** (not values):

| Key                       | Type       | Meaning                                                  |
| ------------------------- | ---------- | -------------------------------------------------------- |
| `auth.forwarded_cookies`  | `[]string` | Cookie names copied from the handshake to the open call. |
| `auth.forwarded_headers`  | `[]string` | Header names copied from the handshake to the open call. |

Behavior:

- **Empty means off.** When a list is unset (or empty), nothing of that
  kind is forwarded. There is no default allowlist — forwarding is
  strictly opt-in per deployment.
- **Open only, never refresh.** Forwarding happens only on the session
  **open** call (`POST /auth/sessions`). The periodic permission
  refresh (`POST /auth/permissions`) is HMAC-signed by Walera and never
  carries forwarded client credentials.
- **Bearer is optional when a credential is forwarded.** Historically
  the open required `Authorization: Bearer …`. With forwarding enabled,
  the open proceeds as long as **at least one** credential is present —
  a bearer, or an allowlisted cookie, or an allowlisted header. The open
  is rejected with `401`
  (`{"reason":"missing_credentials"}`) **only when no credential at all**
  is supplied. When a bearer is present it is still sent; when it is
  absent Walera omits the `Authorization` header entirely and relies on
  the forwarded cookie/header.
- **Name matching.** Cookie names match exactly and are
  case-sensitive (per RFC 6265). Header names match case-insensitively
  (canonicalized like all HTTP field names). Names must be valid field
  tokens — letters, digits, and ``! # $ % & ' * + - . ^ _ | ~ ` `` —
  otherwise config validation rejects them at startup.
- **Reserved names can never be forwarded.** The following headers are
  set and managed by Walera on every backend call and are rejected if
  they appear in `auth.forwarded_headers` (any casing):

  `Authorization`, `Host`, `Content-Length`, `Content-Type`, `Accept`,
  `Connection`, `Transfer-Encoding`, `Cookie`, `X-Request-Id`,
  `X-Walera-Sig`, `X-Walera-Kid`.

  Forwarded headers are applied first; Walera's own headers always win.
- **Values are never logged.** Forwarded cookie and header *values* are
  treated as secrets/PII and never appear in logs. Only allowlisted
  *names* are part of static config.

On a session open, Walera applies the forwarded headers and cookies to
the `POST /auth/sessions` request, then sets `Content-Type`, `Accept`,
and `X-Request-ID`, and sets `Authorization: Bearer …` only when a
bearer was supplied.

## Request format

Walera uses two endpoints, both `POST` with a JSON body, both rooted at
`<WALERA_AUTH_BACKEND_URL>`.

On every subscription **open**, Walera calls `POST /auth/sessions`:

```http
POST /auth/sessions
Authorization: Bearer <user-token>
Content-Type: application/json
Accept: application/json
X-Request-ID: <request-id>

{"channel":"orders:42"}
```

- For `/sse/v1/orders/all`, the channel is `orders:all`.
- `Authorization` is sent only when the client supplied a bearer; with
  cookie/header forwarding it may be absent (see
  [Forwarding cookies and headers](#forwarding-cookies-and-headers)).

While a subscription is live, Walera periodically **refreshes** the
permission map via `POST /auth/permissions`. The refresh is authenticated
by Walera itself with an HMAC signature over the `user_id` (headers
`X-Walera-Sig` / `X-Walera-Kid`), not by the client's bearer, and never
carries forwarded client credentials:

```http
POST /auth/permissions
Content-Type: application/json
Accept: application/json
X-Walera-Sig: <hmac>
X-Walera-Kid: <key-id>
X-Request-ID: <request-id>

{"user_id":"alice","channel":"orders:42","ts":1718000000,"nonce":"…"}
```

- For Walera's own readiness probes the `user_id` / channel is `_health`;
  the probe rides the same HMAC-signed refresh path and carries no bearer.

## Response format

A successful authorization returns `200` with a JSON body of the
following shape:

```json
{
  "user_id": "alice",
  "tables": {
    "orders": ["id", "status", "total_cents", "updated_at"]
  },
  "ttl_seconds": 60,
  "initial_data": {
    "snapshot_ts": "2026-05-18T08:30:00Z",
    "cursor": "abc123"
  }
}
```

| Field          | Meaning                                                                                |
| -------------- | -------------------------------------------------------------------------------------- |
| `user_id`      | Stable identifier used for rate limits and structured logs.                            |
| `tables`       | Per-table list of columns the user may receive.                                        |
| `ttl_seconds`  | Refresh interval Walera uses for this permission map.                                  |
| `initial_data` | Optional. Arbitrary JSON delivered to the client as the first SSE frame after open.    |

Field-whitelist semantics: only the columns named in `tables[<table>]`
are forwarded to the subscriber. Other columns are filtered out before
the SSE event is written.

The table from the SSE URL must appear in `tables`; otherwise Walera
rejects the request.

## Initial data payload

If the auth backend includes an `initial_data` field in the open-time
permission response, Walera emits its raw JSON value to the subscriber
as a single `event: initial_data` SSE frame, written before any `tx`
events:

```
event: initial_data
data: {"snapshot_ts":"2026-05-18T08:30:00Z","cursor":"abc123"}
```

Notes:

- The field is **optional**. When omitted (or set to JSON `null`), no
  `initial_data` frame is emitted and the stream begins directly with
  `tx` / `error` / heartbeat frames.
- The value is opaque to Walera. It is compacted (whitespace stripped
  so the SSE framing is not broken) and otherwise forwarded verbatim —
  any JSON value is allowed (object, array, scalar). Schema and
  semantics are between the auth backend and the client.
- The frame is subject to the same `max_payload_bytes` cap as `tx`
  events. If the compacted payload exceeds the cap, Walera logs a
  warning (`sse initial_data exceeds max_payload_bytes; skipping`) and
  drops the frame; the stream still opens and `tx` delivery proceeds
  normally.
- The frame is emitted only on the **open-time** permission map. It is
  not re-emitted on background permission refreshes — `initial_data`
  from refresh responses is ignored.
- The payload is never logged (treat it as PII by default).

Typical uses: a snapshot cursor / ETag the client should use when
reconciling against the primary API before applying live events, a
seed of server-derived state the client cannot compute on its own, or
a per-session correlation identifier.

## Status codes

| Status            | Meaning                                                                                         |
| ----------------- | ----------------------------------------------------------------------------------------------- |
| `200`             | Authorized. Body is the permission map.                                                         |
| `401`             | Token invalid or expired. Walera rejects the open.                                              |
| `403`             | Token valid but not allowed for this channel.                                                   |
| `404`             | Treated as revoked / not found.                                                                 |
| `429`             | Backend rate limiting; treated as a transient denial. Surfaced to the client.                   |
| `5xx` or timeout  | Treated as a transport failure. On open, returns `503`. On refresh, behavior depends on state.  |

On a background permission refresh, a `401`, `403`, `404`, or a `200`
that drops the previously-allowed table disconnects the established
stream with `auth_revoked`. This keeps the runtime contract honest:
permission changes propagate to active subscribers, not just to new
opens.

## Wildcard streams

Wildcard streams (subscriptions of the form `/sse/v1/{table}/all`)
bypass the per-row primary-key check. A subscriber to a wildcard
stream receives changes for every row in the table — which inherently
reveals which primary keys are changing. Wildcard streams are
**intended for publicly-enumerable entities only**.

If a table's primary keys are themselves sensitive (for example, user
identifiers, order numbers from which volume could be inferred, or any
identifier whose enumeration leaks business information), do not enable
wildcard subscriptions for that table. The auth backend should refuse
wildcard channels for tables whose enumeration is not already public.

The wildcard-stream policy is intentionally restrictive. A backend that
allows wildcard subscriptions for sensitive tables would defeat the
field-whitelist guarantee, because the row identifiers themselves
become a side channel.

## Circuit breaker behavior

Walera wraps the auth client in a circuit breaker so that a
misbehaving backend cannot cascade into a runtime-wide outage. When
the failure rate over a sliding window crosses a threshold (above 50%
sustained), the breaker opens and Walera shifts posture:

- **Established subscriptions** continue to receive events as long as
  their last successful permission map is within its TTL. This is a
  bounded fail-open posture for existing streams.
- **New subscription attempts** are rejected with `503` while the
  breaker is open. This is a fail-closed posture for new opens.

When the failure rate drops back below the threshold the breaker
returns to closed and normal authorization resumes. The breaker design
trades a bounded amount of stale-permission delivery against the
operational reality that an auth-backend outage should not produce a
fleet-wide reconnect storm.

See [ADR 0002: Auth Model](./adr/0002-auth-model.md) for the
delegation rationale and [ADR 0003: Slow Client Policy](./adr/0003-slow-client-policy.md)
for the subscriber-side counterpart.

## Example backend (Django)

A minimal Django implementation suitable for development and testbench
use. Replace the in-memory token map with the real session or JWT
lookup that the rest of the product uses.

```python
# views.py
from django.http import JsonResponse
from django.views.decorators.http import require_GET

TOKENS = {
    "alice-token": {
        "user_id": "alice",
        "tables": {"orders": ["id", "status", "total_cents", "updated_at"]},
    },
    "bob-token": {
        "user_id": "bob",
        "tables": {"orders": ["id", "status"]},
    },
    "service-token": {
        "user_id": "walera-service",
        "tables": {"_health": ["id"]},
    },
}


def bearer_token(request):
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return ""
    return header.removeprefix("Bearer ").strip()


@require_GET
def walera_permissions(request):
    channel = request.GET.get("channel", "")
    token = bearer_token(request)
    permissions = TOKENS.get(token)

    if permissions is None:
        return JsonResponse({"reason": "unauthorized"}, status=401)

    if channel == "_health":
        if "_health" in permissions["tables"]:
            return JsonResponse({**permissions, "ttl_seconds": 60})
        return JsonResponse({"reason": "forbidden"}, status=403)

    table, sep, pk = channel.partition(":")
    if not sep or not table or not pk:
        return JsonResponse({"reason": "invalid_channel"}, status=404)

    if table not in permissions["tables"]:
        return JsonResponse({"reason": "not_allowed"}, status=403)

    return JsonResponse({**permissions, "ttl_seconds": 60})
```

```python
# urls.py
from django.urls import path
from .views import walera_permissions

urlpatterns = [
    path("auth/permissions", walera_permissions),
]
```

Production checklist for a real backend:

- Never log bearer tokens or row payloads.
- Keep the endpoint fast — Walera's default auth timeout is two
  seconds. Slow auth directly increases SSE time-to-first-event.
- Treat wildcard channels (`<table>:all`) as a distinct permission
  decision. The default should be deny.
- To revoke an active stream, return `401` / `403` / `404` — or a map
  that no longer contains the subscribed table — on the next refresh.

## See also

- [ADR 0002: Auth Model](./adr/0002-auth-model.md)
- [ADR 0003: Slow Client Policy](./adr/0003-slow-client-policy.md)
- [Architecture](./architecture.md)
