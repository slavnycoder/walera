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
)

type permissionResponse struct {
	UserID     string              `json:"user_id"`
	Tables     map[string][]string `json:"tables"`
	TTLSeconds int                 `json:"ttl_seconds"`
}

type MockAuth struct {
	server *httptest.Server

	mu      sync.RWMutex
	perms   map[string]permissionResponse
	tokUser map[string]string
	revoked map[string]bool

	failMode atomic.Bool
	requests atomic.Int64

	permissionsRequests atomic.Int64
}

func NewMockAuth(t *testing.T) *MockAuth {
	t.Helper()
	m := &MockAuth{
		perms:   make(map[string]permissionResponse),
		tokUser: make(map[string]string),
		revoked: make(map[string]bool),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/permissions", m.servePermissions)
	mux.HandleFunc("/_admin/revoke", m.serveAdminRevoke)
	m.server = httptest.NewServer(mux)
	t.Cleanup(func() {
		m.server.Close()
	})
	return m
}

func (m *MockAuth) URL() string {
	return m.server.URL
}

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
	m.perms[token] = resp
	m.tokUser[token] = userID

	delete(m.revoked, userID)
	m.mu.Unlock()
}

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

func (m *MockAuth) Revoke(userID string) {
	m.mu.Lock()
	m.revoked[userID] = true
	m.mu.Unlock()
}

func (m *MockAuth) FailMode(on bool) {
	m.failMode.Store(on)
}

func (m *MockAuth) RequestCount() int64 {
	return m.requests.Load()
}

func (m *MockAuth) PermissionsRequestCount() int64 {
	return m.permissionsRequests.Load()
}

func (m *MockAuth) servePermissions(w http.ResponseWriter, r *http.Request) {
	m.requests.Add(1)

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if m.failMode.Load() {

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

	if channel != "_health" {
		m.permissionsRequests.Add(1)
	}

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
