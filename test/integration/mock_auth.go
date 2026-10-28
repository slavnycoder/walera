//go:build integration

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
	"time"
)

type permissionResponse struct {
	UserID     string              `json:"user_id"`
	Tables     map[string][]string `json:"tables"`
	TTLSeconds int                 `json:"ttl_seconds"`
}

type MockAuth struct {
	server *httptest.Server

	mu         sync.RWMutex
	permsByTok map[string]permissionResponse
	permsByUID map[string]permissionResponse
	tokUser    map[string]string
	revoked    map[string]bool

	// cookieAuth maps a session-cookie name -> cookie value -> permissions, so
	// an OpenSession with no bearer but a known cookie still resolves a user.
	cookieAuth map[string]map[string]permissionResponse

	// lastOpenCookies / lastOpenHeaders capture exactly what the most recent
	// /auth/sessions call carried, so tests can assert allowlisted credentials
	// reached the backend and non-allowlisted ones were stripped.
	lastOpenCookies []*http.Cookie
	lastOpenHeaders http.Header

	failMode    atomic.Bool
	openDelayNs atomic.Int64
	requests    atomic.Int64
}

func NewMockAuth(t *testing.T) *MockAuth {
	t.Helper()
	m := &MockAuth{
		permsByTok: make(map[string]permissionResponse),
		permsByUID: make(map[string]permissionResponse),
		tokUser:    make(map[string]string),
		revoked:    make(map[string]bool),
		cookieAuth: make(map[string]map[string]permissionResponse),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/sessions", m.serveOpenSession)
	mux.HandleFunc("/_admin/revoke", m.serveAdminRevoke)
	m.server = httptest.NewServer(mux)
	t.Cleanup(func() {
		m.server.Close()
	})
	return m
}

func (m *MockAuth) URL() string { return m.server.URL }

func (m *MockAuth) SetMap(token, userID string, tables []string, fields map[string][]string) {
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
		TTLSeconds: 1,
	}
	m.mu.Lock()
	m.permsByTok[token] = resp
	m.permsByUID[userID] = resp
	m.tokUser[token] = userID
	delete(m.revoked, userID)
	m.mu.Unlock()
}

// SetCookieMap registers a session cookie (name=value) that resolves to the
// given user and table/field whitelist. An OpenSession with no bearer but a
// matching cookie authenticates as this user. The user is also registered in
// permsByUID; refresh replays the same cookie to /auth/sessions, so this keeps
// the subscription alive.
func (m *MockAuth) SetCookieMap(cookieName, cookieValue, userID string, tables []string, fields map[string][]string) {
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
		TTLSeconds: 1,
	}
	m.mu.Lock()
	if m.cookieAuth[cookieName] == nil {
		m.cookieAuth[cookieName] = make(map[string]permissionResponse)
	}
	m.cookieAuth[cookieName][cookieValue] = resp
	m.permsByUID[userID] = resp
	delete(m.revoked, userID)
	m.mu.Unlock()
}

// LastOpenCookies returns the cookies the most recent /auth/sessions call
// carried (a copy, safe to read after the call).
func (m *MockAuth) LastOpenCookies() []*http.Cookie {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]*http.Cookie(nil), m.lastOpenCookies...)
}

// LastOpenHeaders returns the headers the most recent /auth/sessions call
// carried (a clone, safe to read after the call).
func (m *MockAuth) LastOpenHeaders() http.Header {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastOpenHeaders.Clone()
}

func (m *MockAuth) SetTTL(token string, ttlSeconds int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.permsByTok[token]
	if !ok {
		return errors.New("mock auth: SetTTL: token has no map; call SetMap first")
	}
	p.TTLSeconds = ttlSeconds
	m.permsByTok[token] = p
	if uid := m.tokUser[token]; uid != "" {
		m.permsByUID[uid] = p
	}
	return nil
}

func (m *MockAuth) Revoke(userID string) {
	m.mu.Lock()
	m.revoked[userID] = true
	m.mu.Unlock()
}

func (m *MockAuth) FailMode(on bool) { m.failMode.Store(on) }

// SetOpenDelay injects an artificial latency into serveOpenSession so tests can
// exercise the handshake's auth-call timeout and the global-slot release path.
func (m *MockAuth) SetOpenDelay(d time.Duration) { m.openDelayNs.Store(int64(d)) }

func (m *MockAuth) RequestCount() int64 { return m.requests.Load() }

func (m *MockAuth) serveOpenSession(w http.ResponseWriter, r *http.Request) {
	m.requests.Add(1)

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if d := m.openDelayNs.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	if m.failMode.Load() {
		http.Error(w, `{"reason":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}

	// Record exactly what reached the backend so tests can assert allowlisted
	// credentials were forwarded and non-allowlisted ones were stripped.
	m.mu.Lock()
	m.lastOpenCookies = r.Cookies()
	m.lastOpenHeaders = r.Header.Clone()
	m.mu.Unlock()

	authHdr := r.Header.Get("Authorization")
	token, hasBearer := strings.CutPrefix(authHdr, "Bearer ")

	// Bearer path: resolve the token against the registered token map.
	if hasBearer && token != "" {
		m.mu.RLock()
		resp, ok := m.permsByTok[token]
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
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"reason":"unknown_token"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(mustJSON(resp))
		return
	}

	// Bearer-less path: resolve via a registered session cookie.
	resp, userID, ok := m.resolveCookieAuth(r)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"reason":"missing_credentials"}`))
		return
	}
	m.mu.RLock()
	revoked := userID != "" && m.revoked[userID]
	m.mu.RUnlock()
	if revoked {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"reason":"revoked"}`))
		return
	}

	body := mustJSON(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// resolveCookieAuth looks for any request cookie whose name+value was
// registered via SetCookieMap and returns its permissions.
func (m *MockAuth) resolveCookieAuth(r *http.Request) (permissionResponse, string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, ck := range r.Cookies() {
		byValue, ok := m.cookieAuth[ck.Name]
		if !ok {
			continue
		}
		if resp, ok := byValue[ck.Value]; ok {
			return resp, resp.UserID, true
		}
	}
	return permissionResponse{}, "", false
}

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

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("mock auth: json.Marshal: " + err.Error())
	}
	return b
}
