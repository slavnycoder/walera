#!/usr/bin/env python3
"""Walera testbench mock-auth backend (HMAC-refresh wire).

Replaces the v1.x bearer-on-refresh model. Walera now exchanges the user's
bearer for a session whitelist exactly once (POST /auth/sessions). Subsequent
refreshes use HMAC-signed service-to-service requests (POST /auth/permissions)
authenticated by WALERA_AUTH_SIGNING_SECRET — Walera no longer stores the
user token in memory beyond handshake.

Endpoints:
  POST /auth/sessions
    - Header: Authorization: Bearer <user_token>
    - Body:   {"channel": "ENT:ID"}
    - 200 with whitelist body (user_id/tables/ttl_seconds) on known token.
    - 401 on missing/unknown/revoked token.
    - 500 when FAIL_MODE is on.

  POST /auth/permissions
    - Headers: X-Walera-Sig (HMAC-SHA256 hex), X-Walera-Kid
    - Body:   {"user_id", "channel", "ts", "nonce"}
    - HMAC payload (canonical): user_id||\n||channel||\n||ts||\n||nonce
    - 200 with whitelist body for that user_id.
    - 401 on bad signature, stale ts (±60s window), or replayed nonce.
    - 404 if user_id is unknown.
    - 403 if user_id is administratively revoked.
    - 500 when FAIL_MODE is on (FAIL_MODE applies to non-admin paths only;
      sentinel user_id="_health" still honors FAIL_MODE so the breaker-trip
      demo continues to work).

  GET  /_health
    - Liveness probe for docker healthcheck. Honors FAIL_MODE.
    - 200 "ok" / 500 "failing". No auth required.

  POST /_admin/revoke?subject=USER_ID
  POST /_admin/fail-on / fail-off

Sentinel _health channel (HMAC-required):
  When user_id == "_health" arrives via POST /auth/permissions with a valid
  HMAC, the response is the synthetic service whitelist used by Walera's
  CheckAuth liveness path. FAIL_MODE is honored — toggling fail-on flips the
  container's docker-reported health AND breaks Walera's auth health check.

PII / logging discipline (CLAUDE.md):
  log_message prints only the access-log line (method/path/status). Bearer
  tokens, HMAC signatures, full table payloads, and seed contents are NEVER
  logged.
"""
import hashlib
import hmac
import http.server
import json
import os
import sys
import threading
import time
import urllib.parse
from collections import OrderedDict

REVOKED = set()
FAIL_MODE = {"on": False}
LOCK = threading.Lock()

NONCE_CACHE_MAX = 4096
NONCE_TTL_SECONDS = 300
TS_WINDOW_SECONDS = 60

# OrderedDict for poor-man's LRU: key=nonce, value=insertion ts. Pruned on
# every insert by both size and age. Single-process mock — no clustering.
NONCE_CACHE = OrderedDict()


def _signing_secret():
    s = os.environ.get("WALERA_AUTH_SIGNING_SECRET", "")
    if len(s.encode("utf-8")) < 32:
        sys.stderr.write(
            "[mock-auth] FATAL: WALERA_AUTH_SIGNING_SECRET must be set "
            "and at least 32 bytes (got %d)\n" % len(s.encode("utf-8"))
        )
        sys.exit(1)
    return s.encode("utf-8")


def _signing_kid():
    return os.environ.get("WALERA_AUTH_SIGNING_KID", "v1")


SECRET = _signing_secret()
KID = _signing_kid()

try:
    MAPS = json.load(open("/app/seed.json"))
except (OSError, json.JSONDecodeError) as e:
    sys.stderr.write("[mock-auth] FATAL: could not load /app/seed.json: %s\n" % e)
    sys.exit(1)

USERS_BY_ID = {v["user_id"]: v for v in MAPS.values()}


def _sign(user_id, channel, ts, nonce):
    payload = (
        user_id.encode("utf-8")
        + b"\n"
        + channel.encode("utf-8")
        + b"\n"
        + ("%d" % ts).encode("utf-8")
        + b"\n"
        + nonce.encode("utf-8")
    )
    return hmac.new(SECRET, payload, hashlib.sha256).hexdigest()


def _check_nonce(nonce):
    """Return True if nonce is fresh (not seen recently). Prunes by size + age."""
    now = time.time()
    with LOCK:
        cutoff = now - NONCE_TTL_SECONDS
        while NONCE_CACHE:
            oldest_nonce = next(iter(NONCE_CACHE))
            if NONCE_CACHE[oldest_nonce] >= cutoff:
                break
            NONCE_CACHE.popitem(last=False)
        if nonce in NONCE_CACHE:
            return False
        NONCE_CACHE[nonce] = now
        while len(NONCE_CACHE) > NONCE_CACHE_MAX:
            NONCE_CACHE.popitem(last=False)
        return True


def _read_json_body(handler):
    length = int(handler.headers.get("Content-Length") or "0")
    if length <= 0 or length > 64 * 1024:
        return None
    raw = handler.rfile.read(length)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _send(handler, status, body=b"", content_type="application/json"):
    handler.send_response(status)
    if body:
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    if body:
        handler.wfile.write(body)


HEALTH_WHITELIST_BODY = json.dumps(
    {
        "user_id": "_health",
        "tables": {"_health": ["id"]},
        "ttl_seconds": 60,
    }
).encode()


class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("[mock-auth] " + (fmt % args) + "\n")

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/_health":
            with LOCK:
                if FAIL_MODE["on"]:
                    _send(self, 500, b"failing", "text/plain")
                    return
            _send(self, 200, b"ok", "text/plain")
            return
        if u.path.startswith("/_admin/"):
            self.send_response(405)
            self.send_header("Allow", "POST")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/auth/sessions":
            self._handle_open_session()
            return
        if u.path == "/auth/permissions":
            self._handle_refresh()
            return
        if u.path.startswith("/_admin/"):
            self._handle_admin(u)
            return
        self.send_response(404)
        self.end_headers()

    def _handle_open_session(self):
        with LOCK:
            if FAIL_MODE["on"]:
                _send(self, 500, b"failing", "text/plain")
                return

        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            _send(self, 401, b'{"reason":"missing_bearer"}')
            return
        token = auth[7:]
        if token == "" or token == "bad":
            _send(self, 401, b'{"reason":"unauthorized"}')
            return

        m = MAPS.get(token)
        if m is None:
            _send(self, 401, b'{"reason":"unknown_token"}')
            return

        with LOCK:
            if m["user_id"] in REVOKED:
                _send(self, 401, b'{"reason":"revoked"}')
                return

        # Body is informational — channel is logged but does not scope the
        # returned whitelist (full multi-table view, same shape as the old
        # GET endpoint). Walera filters per-channel client-side.
        _ = _read_json_body(self)

        body = json.dumps(m).encode()
        _send(self, 200, body)

    def _handle_refresh(self):
        sig = self.headers.get("X-Walera-Sig", "")
        kid = self.headers.get("X-Walera-Kid", "")
        if kid != KID:
            _send(self, 401, b'{"reason":"unknown_kid"}')
            return
        if sig == "":
            _send(self, 401, b'{"reason":"missing_sig"}')
            return

        body = _read_json_body(self)
        if body is None or not isinstance(body, dict):
            _send(self, 401, b'{"reason":"bad_body"}')
            return

        user_id = body.get("user_id", "")
        channel = body.get("channel", "")
        ts = body.get("ts", 0)
        nonce = body.get("nonce", "")
        if not isinstance(user_id, str) or not isinstance(channel, str):
            _send(self, 401, b'{"reason":"bad_types"}')
            return
        if not isinstance(ts, int) or not isinstance(nonce, str) or nonce == "":
            _send(self, 401, b'{"reason":"bad_types"}')
            return

        now = int(time.time())
        if abs(now - ts) > TS_WINDOW_SECONDS:
            _send(self, 401, b'{"reason":"ts_window"}')
            return

        expected = _sign(user_id, channel, ts, nonce)
        if not hmac.compare_digest(expected, sig):
            _send(self, 401, b'{"reason":"bad_sig"}')
            return

        if not _check_nonce(nonce):
            _send(self, 401, b'{"reason":"replay"}')
            return

        # Sentinel health probe — honors FAIL_MODE for breaker-trip demo.
        if user_id == "_health":
            with LOCK:
                if FAIL_MODE["on"]:
                    _send(self, 500, b"failing", "text/plain")
                    return
            _send(self, 200, HEALTH_WHITELIST_BODY)
            return

        with LOCK:
            if FAIL_MODE["on"]:
                _send(self, 500, b"failing", "text/plain")
                return

        m = USERS_BY_ID.get(user_id)
        if m is None:
            _send(self, 404, b'{"reason":"unknown_user"}')
            return

        with LOCK:
            if user_id in REVOKED:
                _send(self, 403, b'{"reason":"revoked"}')
                return

        out = json.dumps(m).encode()
        _send(self, 200, out)

    def _handle_admin(self, u):
        q = urllib.parse.parse_qs(u.query)
        with LOCK:
            if u.path == "/_admin/revoke":
                subject = q.get("subject", [""])[0]
                REVOKED.add(subject)
                body = b"ok"
            elif u.path == "/_admin/fail-on":
                FAIL_MODE["on"] = True
                body = b"failing"
            elif u.path == "/_admin/fail-off":
                FAIL_MODE["on"] = False
                body = b"healthy"
            else:
                self.send_response(404)
                self.end_headers()
                return
        _send(self, 200, body, "text/plain")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    http.server.HTTPServer(("0.0.0.0", port), H).serve_forever()
