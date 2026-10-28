package sse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/limits"
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

// addLiveSub builds an auth.Subscriber (no TTL loop) and adds it to the given
// registry, mimicking what stream.go does for a live SSE connection.
func addLiveSub(t *testing.T, reg *auth.Subscribers, c *auth.Client, bk *auth.Breaker, userID, channel, table string, cols ...string) *auth.Subscriber {
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
		auth.SubscriberDeps{Sub: rsub, Client: c, Breaker: bk, Logger: zerolog.Nop(), Metrics: metrics.New()},
	)
	reg.Add(asub)
	t.Cleanup(func() { reg.Remove(asub.ID()) })
	return asub
}

func registerLiveSub(t *testing.T, kit *testHandlerKit, userID, channel, table string, cols ...string) *auth.Subscriber {
	t.Helper()
	return addLiveSub(t, kit.authReg, kit.authClient, kit.breaker, userID, channel, table, cols...)
}

// refreshBackend scripts OpenSession (/auth/sessions) for user "u1" to return
// tables; POST /sse/v1/refresh swaps exactly this whitelist.
func refreshBackend(b *fakeAuthBackend, tables map[string][]string) {
	b.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", tables, 60))
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
	refreshBackend(kit.backend, map[string][]string{"users": {"id", "name"}})

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
	if got := gatherCounterValue(t, kit.h.metrics, "walera_refresh_ondemand_total", "result", "swapped"); got != 1 {
		t.Errorf("swapped metric = %v; want 1", got)
	}
}

func TestRefresh_NoLiveSubscriber_404(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	refreshBackend(kit.backend, map[string][]string{"users": {"id"}})

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404", w.Code)
	}
	if got := reasonOf(t, w); got != "no_live_subscriber" {
		t.Errorf("reason = %q; want no_live_subscriber", got)
	}
	if got := gatherCounterValue(t, kit.h.metrics, "walera_refresh_ondemand_total", "result", "no_live_subscriber"); got != 1 {
		t.Errorf("no_live_subscriber metric = %v; want 1", got)
	}
}

func TestRefresh_OwnScope_OtherUserUntouched(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	// Caller authenticates as u1, but the only live sub on the channel is u2's.
	other := registerLiveSub(t, kit, "u2", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id", "name"}})

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
	refreshBackend(kit.backend, map[string][]string{"users": {"id", "name"}})

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
	refreshBackend(kit.backend, map[string][]string{"orders": {"id"}})

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d; want 403", w.Code)
	}
	if got := reasonOf(t, w); got != "not_allowed" {
		t.Errorf("reason = %q; want not_allowed", got)
	}
	if got := gatherCounterValue(t, kit.h.metrics, "walera_refresh_ondemand_total", "result", "not_allowed"); got != 1 {
		t.Errorf("not_allowed metric = %v; want 1", got)
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
	refreshBackend(kit.backend, map[string][]string{"users": {"id", "name"}})

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

func TestRefresh_MultipleOwnSubs_FanOut_200(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	// Same user, same channel, two live subscribers (two tabs).
	a := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	b := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id", "name"}})

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	var resp struct {
		Refreshed int `json:"refreshed"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Refreshed != 2 {
		t.Errorf("refreshed = %d; want 2", resp.Refreshed)
	}
	for i, s := range []*auth.Subscriber{a, b} {
		if _, ok := s.AuthMap.Load().Tables["users"]["name"]; !ok {
			t.Errorf("sub %d not swapped", i)
		}
	}
}

// sessionFails scripts the backend so OpenSession (/auth/sessions) fails with the
// given status/body and records whether /auth/permissions was ever contacted.
func sessionFails(b *fakeAuthBackend, status int, body string) *atomic.Bool {
	var permHit atomic.Bool
	b.SetResp(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/permissions" {
			permHit.Store(true)
			return
		}
		w.WriteHeader(status)
		if body != "" {
			_, _ = io.WriteString(w, body)
		}
	})
	return &permHit
}

func TestRefresh_OpenSessionForbidden_403(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	permHit := sessionFails(kit.backend, http.StatusForbidden, `{"reason":"backend_denied"}`)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d; want 403", w.Code)
	}
	// OpenSession 403 forwards the backend's own body, NOT {"reason":"not_allowed"}.
	if got := reasonOf(t, w); got != "backend_denied" {
		t.Errorf("reason = %q; want backend_denied (backend body forwarded)", got)
	}
	if permHit.Load() {
		t.Errorf("/auth/permissions was contacted after OpenSession failure")
	}
}

func TestRefresh_OpenSessionUnauthorized_401(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	permHit := sessionFails(kit.backend, http.StatusUnauthorized, `{"reason":"bad_token"}`)

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d; want 401", w.Code)
	}
	if permHit.Load() {
		t.Errorf("/auth/permissions contacted after OpenSession 401")
	}
}

func TestRefresh_OpenSessionUnavailable_503(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	permHit := sessionFails(kit.backend, http.StatusInternalServerError, "")

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q; want 5", got)
	}
	if permHit.Load() {
		t.Errorf("/auth/permissions contacted after OpenSession 5xx")
	}
}

func TestRefresh_Busy_409_KeepsStream(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")

	// A background RefreshOnce holds refreshMu while blocked inside its backend
	// fetch (the FIRST /auth/permissions call). RefreshOwnByChannel fetches
	// off-lock and only then TryLocks each sub, so the POST's own fetch must return
	// while the background still holds the lock — hence only the first
	// /auth/permissions call blocks; later calls answer immediately, letting the
	// POST reach a TryLock that deterministically fails → RefreshBusy → 409.
	var permCalls atomic.Int32
	inflight := make(chan struct{})
	proceed := make(chan struct{})
	kit.backend.SetResp(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/permissions" {
			if permCalls.Add(1) == 1 {
				close(inflight)
				<-proceed
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id"}}, 60))
	})

	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		sub.RefreshOnce(context.Background(), "bg-refresh")
	}()
	<-inflight // background RefreshOnce now holds refreshMu inside the backend call

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d; want 409 (refresh in progress)", w.Code)
	}
	if got := reasonOf(t, w); got != "refresh_in_progress" {
		t.Errorf("reason = %q; want refresh_in_progress", got)
	}
	if got := w.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q; want 1", got)
	}
	if r := sub.Sub.Reason(); r != "" {
		t.Errorf("sub dropped with reason %q; want kept alive while busy", r)
	}
	if got := gatherCounterValue(t, kit.h.metrics, "walera_refresh_ondemand_total", "result", "refresh_in_progress"); got != 1 {
		t.Errorf("refresh_in_progress metric = %v; want 1", got)
	}

	close(proceed)
	bg.Wait()
}

func TestRefresh_InvalidRequestID_400(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/sse/v1/refresh", strings.NewReader(`{"channel":"users:42"}`))
	req.Header.Set("Authorization", "Bearer valid")
	req.Header.Set("X-Request-ID", "bad id with spaces")
	w := httptest.NewRecorder()
	kit.h.serveRefresh(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400", w.Code)
	}
	if got := errorOf(t, w); got != "invalid_request_id" {
		t.Errorf("error = %q; want invalid_request_id", got)
	}
}

func TestRefresh_OversizedBody_400(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	big := `{"channel":"users:42","pad":"` + strings.Repeat("x", 2048) + `"}`
	w, _ := postRefresh(kit.h, big, true)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400 (oversized body)", w.Code)
	}
	if got := errorOf(t, w); got != "invalid_request" {
		t.Errorf("error = %q; want invalid_request", got)
	}
}

// newRefreshFwdHandler builds a handler whose auth client forwards the given
// header allowlist, so the forwarded-credential scrub path can be exercised.
func newRefreshFwdHandler(t *testing.T, fwdHeaders []string) (*Handler, *fakeAuthBackend, *auth.Subscribers, *auth.Client, *auth.Breaker) {
	t.Helper()
	backend := newFakeAuthBackend()
	t.Cleanup(backend.Close)
	logger := zerolog.Nop()
	m := metrics.New()
	authCfg := auth.Config{
		BackendURL:        backend.srv.URL,
		DefaultTTLSeconds: 60,
		RequestTimeout:    2 * time.Second,
		ForwardedHeaders:  fwdHeaders,
		Breaker: auth.BreakerConfig{
			WindowBuckets: 30, BucketSeconds: 1, FailureRateThreshold: 0.5,
			DebounceFloor: 20, Cooldown: 30 * time.Second, StaleRefreshJitter: 5 * time.Second,
		},
		Signing: auth.SigningConfig{Secret: strings.Repeat("k", 64), Kid: "v1"},
	}
	breaker := auth.NewBreaker(authCfg.Breaker, auth.BreakerDeps{
		Prober: sseTestProber(func(_ context.Context) error { return nil }), Logger: logger, Metrics: m,
	})
	authClient := auth.New(authCfg, auth.Deps{Logger: logger, Breaker: breaker, Metrics: m})
	lim := limits.New(limits.Config{GlobalConcurrent: 1024, PerUserConcurrentMax: 10}, limits.Deps{Logger: logger, Metrics: m})
	authReg := auth.NewSubscribers(auth.SubscribersDeps{Logger: logger, Metrics: m})
	enc := NewEncoder(10 << 20)
	pool := NewPool(PoolConfig{
		PoolFactor: 1, SubQueueSize: 8, MaxWaitMs: 2, WriteTimeout: 5 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond, DrainThresholdSubs: 1,
	}, PoolDeps{Encoder: enc, Metrics: newFakeMetrics(), Logger: logger})
	t.Cleanup(func() { _ = pool.Shutdown(context.Background()) })
	cfg := Config{
		MaxPayloadBytes: 10 << 20, WriteTimeout: 5 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
		Router: router.Config{ExactBuffer: 16, WildcardBuffer: 32, MaxChangesPerTx: 10000, HeartbeatInterval: 200 * time.Millisecond},
		Auth:   authCfg,
	}
	h := NewHandler(cfg, Deps{
		Broadcaster: &fakeBroadcaster{},
		Auth:        AuthDeps{Client: authClient, Subscribers: authReg, Breaker: breaker},
		Limits:      lim, Pool: pool, Logger: logger, Metrics: m,
	})
	return h, backend, authReg, authClient, breaker
}

func TestRefresh_CredentialScrub_ForwardedHeaders(t *testing.T) {
	t.Parallel()
	h, backend, reg, client, breaker := newRefreshFwdHandler(t, []string{"X-Tenant"})
	addLiveSub(t, reg, client, breaker, "u1", "users:42", "users", "id")

	backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
	})

	req := httptest.NewRequest(http.MethodPost, "/sse/v1/refresh", strings.NewReader(`{"channel":"users:42"}`))
	req.Header.Set("Authorization", "Bearer valid")
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("X-Tenant", "acme")
	w := httptest.NewRecorder()
	h.serveRefresh(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	// The request header map is scrubbed of all forwarded credentials.
	for _, hdr := range []string{"Authorization", "Cookie", "X-Tenant"} {
		if got := req.Header.Get(hdr); got != "" {
			t.Errorf("%s not scrubbed from request: %q", hdr, got)
		}
	}
}

func TestRefresh_GlobalLimitExhausted_503(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, &limits.Config{GlobalConcurrent: 1, PerUserConcurrentMax: 10})
	// Occupy the only global slot so the refresh's AcquireGlobal fails before any
	// auth work happens.
	if !kit.limits.AcquireGlobal() {
		t.Fatal("precondition: could not acquire the single global slot")
	}
	defer kit.limits.ReleaseGlobal()

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503 (global limit)", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q; want 5", got)
	}
}

func TestRefresh_PerUserSlotsFull_StillRefreshes(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, &limits.Config{GlobalConcurrent: 1024, PerUserConcurrentMax: 2})
	sub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	refreshBackend(kit.backend, map[string][]string{"users": {"id", "name"}})

	// u1 holds one of two slots (its live stream). Refresh must not consume the
	// other one, and must not fail when all are taken.
	if ok, _ := kit.limits.AcquirePerUser("u1"); !ok {
		t.Fatal("precondition: acquire slot 1")
	}
	defer kit.limits.ReleasePerUser("u1")

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 with a free slot (body=%s)", w.Code, w.Body.String())
	}
	if ok, _ := kit.limits.AcquirePerUser("u1"); !ok {
		t.Fatal("refresh leaked a per-user slot")
	}
	defer kit.limits.ReleasePerUser("u1")

	// Both slots now held, as with PerUserConcurrentMax live streams.
	w, _ = postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 with all per-user slots full (body=%s)", w.Code, w.Body.String())
	}
	if _, ok := sub.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("whitelist not swapped: users.name missing")
	}
}

func TestRefresh_PartialSuccess_SwappedBeatsBusy_200(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	// Two live subs for the same (user, channel): one is held busy by a background
	// refresh, the other swaps. Swapped must win over a concurrent Busy → 200.
	busySub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	freeSub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")

	var permCalls atomic.Int32
	inflight := make(chan struct{})
	proceed := make(chan struct{})
	kit.backend.SetResp(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/permissions" {
			if permCalls.Add(1) == 1 {
				close(inflight)
				<-proceed
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
	})

	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		busySub.RefreshOnce(context.Background(), "bg-refresh")
	}()
	<-inflight // busySub's refreshMu is now held by the blocked background refresh

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (partial success beats busy) body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Refreshed int `json:"refreshed"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Refreshed != 1 {
		t.Errorf("refreshed = %d; want 1 (only the free sub swapped)", resp.Refreshed)
	}
	if _, ok := freeSub.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("free sub not swapped")
	}
	if got := gatherCounterValue(t, kit.h.metrics, "walera_refresh_ondemand_total", "result", "swapped"); got != 1 {
		t.Errorf("swapped metric = %v; want 1", got)
	}

	close(proceed)
	bg.Wait()
}

func TestRefresh_Preflight_AdvertisesForwardedHeader(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newRefreshFwdHandler(t, []string{"X-Tenant"})

	req := httptest.NewRequest(http.MethodOptions, "/sse/v1/refresh", nil)
	req.Header.Set("Origin", "https://app.example")
	w := httptest.NewRecorder()
	h.serveRefreshPreflight(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d; want 204", w.Code)
	}
	got := w.Header().Get("Access-Control-Allow-Headers")
	want := "Authorization, X-Request-ID, Content-Type, X-Tenant"
	if got != want {
		t.Errorf("Access-Control-Allow-Headers = %q; want %q (forwarded header must be advertised)", got, want)
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

func TestRefresh_SwapsOpenSessionWhitelist_NoPermissionsCall(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := registerLiveSub(t, kit, "u1", "users:42", "users", "id")
	var permCalls atomic.Int32
	kit.backend.SetResp(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/permissions" {
			permCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
	})

	w, _ := postRefresh(kit.h, `{"channel":"users:42"}`, true)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body=%s)", w.Code, w.Body.String())
	}
	if n := permCalls.Load(); n != 0 {
		t.Errorf("/auth/permissions calls = %d; want 0 (OpenSession already returned the whitelist)", n)
	}
	if _, ok := sub.AuthMap.Load().Tables["users"]["name"]; !ok {
		t.Errorf("whitelist not swapped: users.name missing")
	}
}
