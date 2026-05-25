//go:build integration

// Package integration — mock_auth.go is the Go re-implementation of the
// Phase-3 Python smoke mock (/tmp/mock-auth.py). It exposes the auth-backend
// contract that internal/auth/client.go talks to:
//
//	GET /auth/permissions?channel=<URL-encoded channel>
//	  Authorization: Bearer <token>
//	  X-Request-ID:  <uuid>
//	→ 200 application/json {"user_id":..., "tables":..., "ttl_seconds":...}
//	→ 401 / 403 / 404 forwarded verbatim
//	→ 500 / 503 → Walera Client classifies as ErrUnavailable → breaker
//
// Tokens map 1-to-1 to user_ids. The default mock fixture stocks "test-token"
// → user "test-user" with the users-only Map; SetMap on a fresh MockAuth
// replaces that fixture.
//
// State helpers:
//   - SetMap(userID, tables, fields) — install/replace a user's Map.
//   - Revoke(userID)                  — next refresh returns 401.
//   - FailMode(on)                    — every call returns 503.
//   - RequestCount()                  — atomic counter for assertions.
//   - URL()                           — base URL for config wiring.
//
// All shared maps are RWMutex-protected; the request counter is atomic; the
// failMode flag is atomic. The handler is safe for concurrent calls from
// arbitrary numbers of goroutines (the Walera process plus the test's own
// admin requests during scenario setup).
package integration

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// permissionResponse mirrors the wireMap shape consumed by
// internal/auth/client.go's ParseWhitelist.
type permissionResponse struct {
	UserID     string              `json:"user_id"`
	Tables     map[string][]string `json:"tables"`
	TTLSeconds int                 `json:"ttl_seconds"`
}

// MockAuth wraps an httptest.Server plus mutable per-user state. The zero
// value is NOT usable — construct via NewMockAuth(t).
type MockAuth struct {
	server *httptest.Server

	mu      sync.RWMutex
	perms   map[string]permissionResponse // token → permission map
	tokUser map[string]string             // token → user_id (for revoke routing)
	revoked map[string]bool               // user_id → revoked flag

	failMode atomic.Bool
	requests atomic.Int64
	// permissionsRequests counts only NON-health-channel requests to
	// /auth/permissions, i.e., real user-permission lookups. Health-channel
	// probes (channel=_health) are excluded so tests asserting "the binary
	// did NOT forward a request to the auth backend" remain stable in the
	// presence of background health pings from the auth breaker / future
	// warm-up paths. WR-03 review-fix anchor.
	permissionsRequests atomic.Int64
}

// NewMockAuth starts a new httptest.Server bound to the loopback interface
// and registers t.Cleanup for teardown. The server's URL is exposed via
// (*MockAuth).URL() and threaded into walera-test.yaml as auth.backend_url.
//
// The default state contains NO permissions: every call returns 404 until
// the test installs a Map via SetMap. This mirrors the production
// expectation that a Walera handshake for an unknown user falls through to
// 404.
func NewMockAuth(t *testing.T) *MockAuth {
	t.Helper()
	m := &MockAuth{
		perms:   make(map[string]permissionResponse),
		tokUser: make(map[string]string),
		revoked: make(map[string]bool),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/permissions", m.servePermissions)
	mux.HandleFunc("/_admin/revoke", m.serveAdminRevoke) // optional, for parity with Python mock
	m.server = httptest.NewServer(mux)
	t.Cleanup(func() {
		m.server.Close()
	})
	return m
}

// URL returns the base URL of the mock server (e.g., "http://127.0.0.1:PORT").
func (m *MockAuth) URL() string {
	return m.server.URL
}

// SetMap installs (or replaces) the Map returned by the mock for `token`. The
// userID is recorded so Revoke(userID) can route to this token's response
// without callers needing to know the token-to-userID binding.
//
// `fields` maps table-name → list of allowed column names.
func (m *MockAuth) SetMap(token, userID string, tables []string, fields map[string][]string) {
	// Ensure every requested table has a fields entry (even if empty) — the
	// permission contract is "the table is in the map; fields can be empty
	// (PK-only)".
	tbl := make(map[string][]string, len(tables))
	for _, t := range tables {
		if cols, ok := fields[t]; ok {
			tbl[t] = append([]string(nil), cols...)
		} else {
			tbl[t] = []string{}
		}
	}
	resp := permissionResponse{
		UserID:     userID,
		Tables:     tbl,
		TTLSeconds: 1, // 1s default: scenarios that need fast refresh override via SetTTL.
	}
	m.mu.Lock()
	m.perms[token] = resp
	m.tokUser[token] = userID
	// Setting a fresh map clears any prior revoke flag for the user.
	delete(m.revoked, userID)
	m.mu.Unlock()
}

// SetTTL overrides the ttl_seconds value returned for `token`. Useful for
// scenarios that exercise refresh cadence (e.g., scenario 04 revoke-mid-
// stream wants a short TTL so the refresh fires within the test budget).
func (m *MockAuth) SetTTL(token string, ttlSeconds int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.perms[token]
	if !ok {
		return errors.New("mock auth: SetTTL: token has no map; call SetMap first")
	}
	p.TTLSeconds = ttlSeconds
	m.perms[token] = p
	return nil
}

// Revoke marks userID as revoked. Subsequent /auth/permissions calls that
// resolve to this user_id (via the token-to-userID map installed by SetMap)
// return 401 application/json {"reason":"revoked"}. Per spec §3 the running
// Walera subscriber's background refresh ticker (plan 03-03) translates this
// 401 into a Drop("auth_revoked") on the subscriber.
func (m *MockAuth) Revoke(userID string) {
	m.mu.Lock()
	m.revoked[userID] = true
	m.mu.Unlock()
}

// FailMode toggles a global "every call returns 503" mode. Used by the
// breaker scenario; harmless for the other scenarios.
func (m *MockAuth) FailMode(on bool) {
	m.failMode.Store(on)
}

// RequestCount returns the cumulative number of /auth/permissions requests
// the mock has observed, INCLUDING health-channel probes (channel=_health).
// Used by scenarios that want to assert e.g. "background refresh fired at
// least once within 2 seconds".
//
// Prefer PermissionsRequestCount when asserting "the binary did NOT forward
// a USER-permission request to the auth backend" — RequestCount counts
// health pings that fire on the breaker's background loop, which can
// race with the assertion window.
func (m *MockAuth) RequestCount() int64 {
	return m.requests.Load()
}

// PermissionsRequestCount returns the cumulative number of NON-health-channel
// requests to /auth/permissions (i.e., real user-permission lookups; channel
// != "_health"). Use this counter in tests asserting that a particular HTTP
// request did NOT trigger an upstream auth-backend call — it is robust
// against breaker health pings and any future warm-up tick on the health
// channel.
//
// WR-03 anchor; TEST-09.2 supporting helper.
func (m *MockAuth) PermissionsRequestCount() int64 {
	return m.permissionsRequests.Load()
}

// servePermissions implements GET /auth/permissions per the contract
// documented in internal/auth/client.go.
func (m *MockAuth) servePermissions(w http.ResponseWriter, r *http.Request) {
	m.requests.Add(1)

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if m.failMode.Load() {
		// 503 → Walera classifies as *ErrUnavailable → breaker counts.
		http.Error(w, `{"reason":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}

	authHdr := r.Header.Get("Authorization")
	token, hasBearer := strings.CutPrefix(authHdr, "Bearer ")
	if !hasBearer || token == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"reason":"missing_bearer"}`))
		return
	}

	channel := r.URL.Query().Get("channel")
	// Bump the user-permission-specific counter for non-health requests
	// (WR-03 anchor). Health-channel probes are intentionally excluded
	// so test assertions like "the rejected handshake did NOT forward
	// to /auth/permissions" remain stable when the auth breaker's
	// background health loop fires asynchronously.
	if channel != "_health" {
		m.permissionsRequests.Add(1)
	}
	// The health channel is always 200 — handles auth.Client.CheckAuth()
	// probe plus the breaker's background probe.
	if channel == "_health" {
		body := mustJSON(permissionResponse{
			UserID:     "u_service",
			Tables:     map[string][]string{"_health": {"id"}},
			TTLSeconds: 60,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}

	m.mu.RLock()
	resp, ok := m.perms[token]
	userID := m.tokUser[token]
	revoked := userID != "" && m.revoked[userID]
	m.mu.RUnlock()

	if revoked {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"reason":"revoked"}`))
		return
	}
	if !ok {
		// Unknown token — Walera SSE handler forwards 404 to the client.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"reason":"unknown_token"}`))
		return
	}

	body := mustJSON(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// serveAdminRevoke supports `GET /_admin/revoke?user=USER` for parity with the
// Phase-3 Python mock — handy when manually probing the running test binary
// with curl. Tests should prefer the typed Revoke() method.
func (m *MockAuth) serveAdminRevoke(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.Revoke(user)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// mustJSON encodes v or panics — only invoked on shapes we control.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("mock auth: json.Marshal: " + err.Error())
	}
	return b
}
