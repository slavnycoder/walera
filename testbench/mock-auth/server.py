#!/usr/bin/env python3
"""Walera testbench mock-auth backend.

Walera calls POST /auth/sessions on open and again every TTL with the
client's own bearer/cookies; revoke a token via the admin endpoint to end
live streams.

Endpoints:
  POST /auth/sessions
    - Header: Authorization: Bearer <user_token>
    - Body:   {"channel": "ENT:ID"}
    - 200 with whitelist body (user_id/tables/ttl_seconds) on known token.
    - 401 on missing/unknown/revoked token.
    - 500 when FAIL_MODE is on.

  GET  /_health
    - Liveness probe for docker healthcheck. Honors FAIL_MODE.
    - 200 "ok" / 500 "failing". No auth required.

  POST /_admin/revoke?subject=USER_ID
  POST /_admin/fail-on / fail-off

PII / logging discipline (CLAUDE.md):
  log_message prints only the access-log line (method/path/status). Bearer
  tokens, full table payloads, and seed contents are NEVER logged.
"""
import http.server
import json
import os
import sys
import threading
import urllib.parse

REVOKED = set()
FAIL_MODE = {"on": False}
LOCK = threading.Lock()

try:
    MAPS = json.load(open("/app/seed.json"))
except (OSError, json.JSONDecodeError) as e:
    sys.stderr.write("[mock-auth] FATAL: could not load /app/seed.json: %s\n" % e)
    sys.exit(1)

USERS_BY_ID = {v["user_id"]: v for v in MAPS.values()}


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
