# Auth backend contract

Walera does not authenticate users or store permissions. It asks your
backend, through one endpoint, and enforces the answer. Every unclear
outcome closes or refuses the stream.

## When Walera calls it

- **Handshake.** Once per subscription, before the stream opens. The client
  waits for the answer (2 s timeout).
- **Refresh.** While the stream is open, every TTL. The TTL is the
  `ttl_seconds` of the handshake response, or `WALERA_AUTH_TTL` (default
  `60s`) when it is absent or zero. The first refresh is delayed by a random
  extra 0 – TTL/2 so streams opened together do not refresh together.

A refresh is the same request as the handshake, with the same credentials, so
logout, token expiry and permission changes reach streams that are already
open.

## Request

```http
POST /auth/sessions HTTP/1.1
Authorization: Bearer alice-token
Content-Type: application/json
Accept: application/json
X-Request-ID: 3f2a9c0d1e4b5a6978c0d1e2f3a4b5c6

{"channel":"items:1"}
```

- The URL is `WALERA_AUTH_URL` + `/auth/sessions`; a base path such as
  `https://api.example/internal` is kept. Give the URL without a trailing
  slash.
- `channel` is `<table>:<pk>`, or `<table>:all` for a table stream.
- `Authorization` is the client's own `Authorization: Bearer …` header. It
  is omitted when the client sent none.
- `X-Request-ID` is the client's header on the handshake (Walera generates
  one when absent) and a fresh random ID on each refresh.
- Allowlisted cookies and headers are forwarded too, see below.

A client that sends neither a bearer token nor a forwarded cookie or header
gets `401` from Walera without a backend call.

## Response

`200` with JSON:

```json
{
  "user_id": "alice",
  "tables": {
    "items": ["id", "title", "done"],
    "item_tags": ["id", "item_id", "tag"]
  },
  "ttl_seconds": 60,
  "initial_data": {"cursor": "abc123"}
}
```

| Field | Meaning |
| --- | --- |
| `user_id` | Required, non-empty. Used for the per-user limit of 10 streams and in logs. |
| `tables` | Table name (no schema) → columns the user may receive. The subscribed table must be present, otherwise the handshake gets `403 {"reason":"not_allowed"}`. Changes to tables not listed here are never sent; the primary key is always sent. |
| `ttl_seconds` | Optional refresh interval for this stream. Read from the handshake only. |
| `initial_data` | Optional, any JSON. Sent to the client as the first frame (`event: initial_data`), handshake only. Skipped if the frame would exceed 10 MiB. |

Walera reads at most 64 KiB of the response body. A body it cannot parse,
or one without `user_id`, counts as unavailable.

## Status codes

| Backend answer | On handshake | On refresh |
| --- | --- | --- |
| `200` | Stream opens. | New column set applies to transactions committed from now on. If `user_id` changed or the table is gone from `tables`, the stream ends with `auth_revoked`. |
| `401`, `403`, `404` | Same status to the client, with your response body. | Stream ends with `auth_revoked`. |
| Anything else, including redirects (never followed), `429`, `5xx`, timeouts, connection errors | `503` with `Retry-After: 5`. | Retried after 1 s, 3 s and 9 s; if all four attempts fail, the stream ends with `auth_unavailable`. |

Deny bodies reach the client verbatim with `Content-Type: application/json`,
so do not put anything there the client must not see.

A revocation therefore takes effect at the next refresh: within one TTL, or
1.5 TTL for a stream's first refresh. During a backend outage a stream
outlives a failed refresh by at most about 21 s (13 s of backoff plus 2 s
timeouts).

## Forwarding cookies and headers

Browsers' `EventSource` cannot set `Authorization`, but it sends cookies.
To use a session cookie (or a custom header) instead of a bearer token, list
the names to forward:

```bash
WALERA_AUTH_FORWARD_COOKIES=sessionid
WALERA_AUTH_FORWARD_HEADERS=X-Tenant
```

- Both lists are empty by default: nothing is forwarded unless named.
- Cookie names match exactly (case-sensitive); header names match
  case-insensitively. Names must be valid HTTP tokens.
- Forwarded headers are added to the CORS preflight `Access-Control-Allow-Headers`.
- These headers are managed by Walera and cannot be forwarded (startup fails):
  `Authorization`, `Host`, `Content-Length`, `Content-Type`, `Accept`,
  `Connection`, `Transfer-Encoding`, `Cookie`, `X-Request-Id`, `Origin`.
- The same cookies and headers are sent on every refresh.

## Plain HTTP

`WALERA_AUTH_URL` must use `https://`: the request carries the user's
credentials. `WALERA_AUTH_ALLOW_PLAINTEXT=1` allows `http://` for local
development, such as the quick start.

## Credential handling

Tokens, cookies, forwarded header values and `initial_data` are never logged.
After the handshake Walera removes `Authorization`, `Cookie` and forwarded
headers from the request it keeps, holding only the copy each stream needs to
refresh.

## Table streams

`/sse/v1/{table}/all` (channel `<table>:all`) delivers every row of the table
the user may see, which reveals every primary key that changes. Grant table
streams only for publicly enumerable tables; refuse `:all` channels
otherwise, as the example below does.

## Example backend

A minimal backend for local development, standard library only (Python
3.9+). It accepts one hard-coded token, grants three columns of `items`, and
refuses table streams.

```python
# Minimal Walera auth backend for local development. Not for production.
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

TOKENS = {"alice-token": "alice"}           # bearer token -> user id
GRANTS = {"items": ["id", "title", "done"]}  # table -> columns the user may see


class AuthHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/auth/sessions":
            return self.reply(404, {"reason": "not_found"})
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        if token not in TOKENS:
            return self.reply(401, {"reason": "invalid_token"})
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        table, _, pk = body["channel"].partition(":")
        # Wildcards (pk "all") reveal every key: grant them for public tables only.
        if table not in GRANTS or pk == "all":
            return self.reply(403, {"reason": "forbidden"})
        self.reply(200, {"user_id": TOKENS[token], "tables": GRANTS, "ttl_seconds": 60})

    def reply(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


HTTPServer(("0.0.0.0", 9000), AuthHandler).serve_forever()
```

It listens on all interfaces so the Walera container can reach it; do not run
it on a public network. Check it with curl:

```bash
curl -i -X POST localhost:9000/auth/sessions \
  -H "Authorization: Bearer alice-token" -H "Content-Type: application/json" \
  -d '{"channel":"items:1"}'
# HTTP/1.0 200 OK ... {"user_id": "alice", "tables": {"items": ["id", "title", "done"]}, "ttl_seconds": 60}
```

A real backend looks the token or session up the same way the rest of your
API does, and returns only the tables and columns that user may read.
