#!/usr/bin/env python3
"""Walera v1.1 testbench mock-auth backend.

Promoted from /tmp/mock-auth.py (v1.0 ad-hoc smoke fixture) into a first-class
testbench service. Loads its user catalog from `seed.json` at boot; serves the
v1.0 AUTH-01 wire format (`user_id`, `tables`, `roots`, `ttl_seconds`).

Three demo users (deliberately distinct — see Pitfall P8):
  demo-alice  -> u_demo_alice, full whitelist (orders/devices/articles/line_items)
  demo-bob    -> u_demo_bob,   narrow (orders id+status only)
  demo-eve    -> u_demo_eve,   wildcard-only (articles root)

Endpoints (per MOCK-03; method-strict):
  GET  /auth/permissions?channel=ENT:ID  -> 200 with v1.0 AUTH-01 payload,
                                            401 on empty/unknown/revoked token,
                                            500 when FAIL_MODE is on.
                                            POST returns 405 (GET-only).
  POST /_admin/revoke?subject=USER_ID    -> next call for that user_id returns 401
  POST /_admin/fail-on                   -> flip into 500 mode for all non-admin calls
  POST /_admin/fail-off                  -> restore normal operation
                                            GET on /_admin/* returns 405 (POST-only).

`_health` channel:
  Behaves like a normal /auth/permissions call but with a baked-in synthetic
  payload (`u_service`, roots=["_health"]). It HONORS FAIL_MODE on purpose:
  flipping FAIL_MODE flips the container's docker-reported health, which is
  exactly what Phase 06's AUTH-04 breaker-trip demo relies on.

PII / logging discipline (CLAUDE.md):
  log_message prints only the access-log line (method/path/status). Bearer
  tokens, full table payloads, and seed contents are NEVER logged.
"""
import http.server, json, sys, urllib.parse, threading

REVOKED = set()
FAIL_MODE = {"on": False}
LOCK = threading.Lock()

# Load declarative whitelist at boot; fail-fast if missing/corrupt so we never
# silently serve an empty MAPS (which would 401 every demo user).
try:
    MAPS = json.load(open('/app/seed.json'))
except (OSError, json.JSONDecodeError) as e:
    sys.stderr.write("[mock-auth] FATAL: could not load /app/seed.json: %s\n" % e)
    sys.exit(1)


class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("[mock-auth] " + (fmt % args) + "\n")

    def do_GET(self):
        # Per MOCK-03, /_admin/* is POST-only and /auth/permissions is GET-only.
        # Reject GET on admin paths with 405 so state-mutating GETs cannot be
        # triggered by accidental URL fetches (cache prefetch, log paste, etc.).
        u = urllib.parse.urlparse(self.path)
        if u.path.startswith("/_admin/"):
            self.send_response(405); self.send_header("Allow", "POST"); self.end_headers(); return
        if u.path != "/auth/permissions":
            self.send_response(404); self.end_headers(); return
        self._handle_permissions(u)

    def do_POST(self):
        # Per MOCK-03, /_admin/* admin endpoints are POST-only. /auth/permissions
        # is GET-only on the v1.0 wire; reject POST there with 405.
        u = urllib.parse.urlparse(self.path)
        if not u.path.startswith("/_admin/"):
            self.send_response(405); self.send_header("Allow", "GET"); self.end_headers(); return
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
                self.send_response(404); self.end_headers(); return
        self.send_response(200); self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def _handle_permissions(self, u):
        q = urllib.parse.parse_qs(u.query)
        ch = q.get("channel", [""])[0]
        auth = self.headers.get("Authorization", "")
        token = ""
        if auth.startswith("Bearer "):
            token = auth[7:]

        # _health channel: honors FAIL_MODE so toggling failure injection flips
        # container health (required for AUTH-04 breaker-trip demo, phase 06+).
        if ch == "_health":
            with LOCK:
                if FAIL_MODE["on"]:
                    self.send_response(500); self.end_headers(); self.wfile.write(b"failing"); return
            body = json.dumps({"user_id": "u_service", "tables": {"_health": ["id"]}, "roots": ["_health"], "ttl_seconds": 60}).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            return

        with LOCK:
            if FAIL_MODE["on"]:
                self.send_response(500); self.end_headers(); self.wfile.write(b"failing"); return

        if token == "" or token == "bad":
            self.send_response(401); self.end_headers(); self.wfile.write(b'{"reason":"unauthorized"}'); return

        m = MAPS.get(token)
        if m is None:
            self.send_response(401); self.end_headers(); return

        with LOCK:
            if m["user_id"] in REVOKED:
                self.send_response(401); self.end_headers(); self.wfile.write(b'{"reason":"revoked"}'); return

        body = json.dumps(m).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    http.server.HTTPServer(("0.0.0.0", port), H).serve_forever()
