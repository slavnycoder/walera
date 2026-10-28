package sse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
)

func colSet(cols ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		m[c] = struct{}{}
	}
	return m
}

// registerLiveSub builds an auth.Subscriber (no TTL loop) and adds it to the
// handler's registry, mimicking what stream.go does for a live SSE connection.
func registerLiveSub(t *testing.T, kit *testHandlerKit, userID, channel, table string, cols ...string) *auth.Subscriber {
	t.Helper()
	rsub := router.NewSubscriber(
		router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: table, PK: "42", BufferCap: 4},
		router.SubscriberDeps{Parent: context.Background()},
	)
	wl := &auth.Whitelist{
		UserID:     userID,
		Tables:     map[string]map[string]struct{}{table: colSet(cols...)},
		TTLSeconds: 60,
	}
	asub := auth.NewSubscriber(
		auth.SubscriberConfig{InitialMap: wl, UserID: userID, Channel: channel, DefaultTTL: 0},
		auth.SubscriberDeps{
			Sub:     rsub,
			Client:  kit.authClient,
			Breaker: kit.breaker,
			Logger:  zerolog.Nop(),
			Metrics: metrics.New(),
		},
	)
	kit.authReg.Add(asub)
	t.Cleanup(func() { kit.authReg.Remove(asub.ID()) })
	return asub
}

// refreshBackend scripts the fake auth backend to answer OpenSession
// (/auth/sessions) with sessionTables and RefreshPermissions (/auth/permissions)
// with permTables (or permStatus when it is a non-200 code). All maps are for
// user "u1".
func refreshBackend(b *fakeAuthBackend, sessionTables, permTables map[string][]string, permStatus int) {
	b.SetResp(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/permissions" {
			if permStatus != 0 && permStatus != http.StatusOK {
				w.WriteHeader(permStatus)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(permMapJSON("u1", permTables, 60))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", sessionTables, 60))
	})
}

func postRefresh(h *Handler, body string, withAuth bool) (*httptest.ResponseRecorder, *http.Request) {
	req := httptest.NewRequest(http.MethodPost, "/sse/v1/refresh", strings.NewReader(body))
	if withAuth {
		req.Header.Set("Authorization", "Bearer valid")
	}
	w := httptest.NewRecorder()
	h.serveRefresh(w, req)
	return w, req
}

func TestRefresh_HappyPath_200(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, map[string][]string{"users": {"id", "name"}}, 200)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	var resp struct {
		Refreshed int `json:"refreshed"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Refreshed != 1 {
		t.Errorf("refreshed = %d; want 1", resp.Refreshed)
	}
	if _, ok := sub.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("whitelist not swapped: users.name missing")
	}
}

func TestRefresh_NoLiveSubscriber_404(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, map[string][]string{"users": {"id"}}, 200)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404", w.Code)
	}
	if got := reasonOf(t, w); got != "no_live_subscriber" {
		t.Errorf("reason = %q; want no_live_subscriber", got)
	}
}

func TestRefresh_OwnScope_OtherUserUntouched(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	// Caller authenticates as u1, but the only live sub on the channel is u2's.
	other := registerLiveSub(t, kit, "u2", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, map[string][]string{"users": {"id", "name"}}, 200)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404 (caller has no own sub)", w.Code)
	}
	if _, ok := other.AuthMap.Load().Tables["users"]["name"]; ok {
		t.Errorf("another user's subscriber was refreshed; want untouched")
	}
}

func TestRefresh_WildcardNotCrossMatched(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := registerLiveSub(t, kit, "u1", "users:all", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, map[string][]string{"users": {"id", "name"}}, 200)

	// Exact channel must NOT match the wildcard subscriber.
	if w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true); w.Code != http.StatusNotFound {
		t.Fatalf("exact channel status = %d; want 404", w.Code)
	}
	// Wildcard channel matches.
	if w, _ := postRefresh(kit.h, `{"channel":"users:all"}`, true); w.Code != http.StatusOK {
		t.Fatalf("wildcard channel status = %d; want 200", w.Code)
	}
	if _, ok := sub.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("wildcard subscriber not swapped")
	}
}

func TestRefresh_NotAllowed_403(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	// Caller authorized only for orders, not users.
	refreshBackend(kit.backend, map[string][]string{"orders": {"id"}}, map[string][]string{"users": {"id"}}, 200)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d; want 403", w.Code)
	}
	if got := reasonOf(t, w); got != "not_allowed" {
		t.Errorf("reason = %q; want not_allowed", got)
	}
}

func TestRefresh_Revoked_403_DropsSub(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	// Caller authorized at session time, but the re-fetch revokes the table.
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, map[string][]string{"orders": {"id"}}, 200)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d; want 403", w.Code)
	}
	if got := reasonOf(t, w); got != "auth_revoked" {
		t.Errorf("reason = %q; want auth_revoked", got)
	}
	if r := sub.Sub.Reason(); r != "auth_revoked" {
		t.Errorf("sub drop reason = %q; want auth_revoked", r)
	}
}

func TestRefresh_Unavailable_503_KeepsStream(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, nil, http.StatusInternalServerError)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q; want 5", got)
	}
	if got := reasonOf(t, w); got != "auth_unavailable" {
		t.Errorf("reason = %q; want auth_unavailable", got)
	}
	if r := sub.Sub.Reason(); r != "" {
		t.Errorf("sub dropped with reason %q; want kept alive (best-effort)", r)
	}
}

func TestRefresh_MissingCredentials_401(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, false)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d; want 401", w.Code)
	}
}

func TestRefresh_BadBody_400(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	w, _ := postRefresh(kit.h, `{not json`, true)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400", w.Code)
	}
	if got := errorOf(t, w); got != "invalid_request" {
		t.Errorf("error = %q; want invalid_request", got)
	}
}

func TestRefresh_BadChannel_400(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	for _, ch := range []string{"nope", "Bad-Table:1", "", "users:"} {
		w, _ := postRefresh(kit.h, `{"channel":"`+ch+`"}`, true)
		if w.Code != http.StatusBadRequest {
			t.Errorf("channel %q: status = %d; want 400", ch, w.Code)
			continue
		}
		if got := errorOf(t, w); got != "invalid_channel" {
			t.Errorf("channel %q: error = %q; want invalid_channel", ch, got)
		}
	}
}

func TestRefresh_CredentialScrub(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}}, map[string][]string{"users": {"id", "name"}}, 200)

	req := httptest.NewRequest(http.MethodPost, "/sse/v1/refresh", strings.NewReader(`{"channel":"users:42"}`))
	req.Header.Set("Authorization", "Bearer valid")
	req.Header.Set("Cookie", "session=secret")
	w := httptest.NewRecorder()
	kit.h.serveRefresh(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", w.Code)
	}
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("Authorization not scrubbed: %q", got)
	}
	if got := req.Header.Get("Cookie"); got != "" {
		t.Errorf("Cookie not scrubbed: %q", got)
	}
}

func TestRefresh_Preflight_204(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, []string{"https://app.example"}, nil)
	srv := newTestServer(t, kit.h)

	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/sse/v1/refresh", nil)
	req.Header.Set("Origin", "https://app.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("OPTIONS: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d; want 204", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "POST, OPTIONS" {
		t.Errorf("Allow-Methods = %q; want %q", got, "POST, OPTIONS")
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("nosniff missing: %q", got)
	}
}

func TestRefresh_GetMethodNotAllowed_405(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	srv := newTestServer(t, kit.h)

	resp, err := http.Get(srv.URL + "/sse/v1/refresh")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d; want 405", resp.StatusCode)
	}
}

func reasonOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var b struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode reason: %v (body=%s)", err, w.Body.String())
	}
	return b.Reason
}

func errorOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var b struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode error: %v (body=%s)", err, w.Body.String())
	}
	return b.Error
}
