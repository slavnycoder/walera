// Package sse — handler_test.go covers the four SSE routes end-to-end via
// httptest.NewServer.
// httptest.NewServer + a real *http.Client is preferred over
// httptest.ResponseRecorder because the recorder does NOT honour Flush
// semantics nor support reading partial streaming body bytes — both
// requirements for SSE-style tests (note 4).
// This suite covers the handshake sequence tests end-to-end. Each test
// stands up a small fake auth backend via
// httptest and threads its URL into a real *auth.Client, then constructs the
// Handler with real *limits.Limits / *auth.Subscribers / *auth.Breaker — the
// integration shape matches production.
package sse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// sseTestProber is a file-local adapter that wraps a func(ctx) error closure
// to satisfy the auth.Prober interface required by auth.BreakerDeps.Prober.
// Per RESEARCH.md §"Decision on cross-package test adapter", auth does not
// export a ProberFunc adapter; each cross-package test that needs the
// adapter declares its own. Shared between handler_test.go and
// handler_panic_test.go (both files are in `package sse`).
type sseTestProber func(ctx context.Context) error

// CheckAuth satisfies the auth.Prober interface by invoking the wrapped
// closure.
func (f sseTestProber) CheckAuth(ctx context.Context) error { return f(ctx) }

// fakeBroadcaster is a test-only stub satisfying the package-local
// broadcaster interface. It captures registered subscribers for later
// inspection by the test.
type fakeBroadcaster struct {
	mu             sync.Mutex
	subs           []*router.Subscriber
	deregisterHits int
}

func (f *fakeBroadcaster) Register(sub *router.Subscriber) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subs = append(f.subs, sub)
}

func (f *fakeBroadcaster) Deregister(sub *router.Subscriber) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deregisterHits++
}

func (f *fakeBroadcaster) firstSub(t *testing.T) *router.Subscriber {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		if len(f.subs) > 0 {
			s := f.subs[0]
			f.mu.Unlock()
			return s
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("fakeBroadcaster: no subscriber registered within 2s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// fakeAuthBackend is a tiny httptest-backed auth backend. Each test installs
// its own response function via SetResp; nextResp is invoked on every
// request so a test can return a Whitelist first and then *ErrUnauthorized on
// subsequent calls (auth-revoked mid-stream).
type fakeAuthBackend struct {
	srv    *httptest.Server
	hits   atomic.Int64
	mu     sync.Mutex
	respFn func(w http.ResponseWriter, r *http.Request)
}

func newFakeAuthBackend() *fakeAuthBackend {
	b := &fakeAuthBackend{}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/permissions", func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		b.mu.Lock()
		fn := b.respFn
		b.mu.Unlock()
		if fn == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fn(w, r)
	})
	b.srv = httptest.NewServer(mux)
	return b
}

func (b *fakeAuthBackend) Close() { b.srv.Close() }

func (b *fakeAuthBackend) SetResp(fn func(w http.ResponseWriter, r *http.Request)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.respFn = fn
}

// permMapJSON serialises a Whitelist-shaped JSON body for the fake backend's 200
// response.
func permMapJSON(userID string, tables map[string][]string, ttl int) []byte {
	body := map[string]any{
		"user_id":     userID,
		"tables":      tables,
		"ttl_seconds": ttl,
	}
	out, _ := json.Marshal(body)
	return out
}

// testHandlerKit bundles everything a test needs.
type testHandlerKit struct {
	h          *Handler
	bc         *fakeBroadcaster
	backend    *fakeAuthBackend
	limits     *limits.Limits
	authReg    *auth.Subscribers
	breaker    *auth.Breaker
	authClient *auth.Client
	pool       *WriterPool
}

// newTestHandler builds a Handler with real Phase-3 dependencies pointed at
// a fake auth backend. cors is the allowed-origin list; lim allows tests to
// override the limits.Config (pass nil for defaults).
func newTestHandler(t *testing.T, cors []string, lcfg *limits.Config) *testHandlerKit {
	t.Helper()
	backend := newFakeAuthBackend()
	t.Cleanup(backend.Close)

	bc := &fakeBroadcaster{}
	rcfg := router.Config{
		ExactBuffer:       16,
		WildcardBuffer:    32,
		MaxChangesPerTx:   10000,
		HeartbeatInterval: 200 * time.Millisecond,
	}
	cfg := Config{
		Addr:              ":0",
		CORSOrigins:       cors,
		HeartbeatInterval: 200 * time.Millisecond,
		MaxPayloadBytes:   10 * 1024 * 1024,
		// SEC-01: a sane WriteTimeout so the per-frame
		// deadline path does not fire on the test scaffold.
		WriteTimeout: 5 * time.Second,
	}
	logger := zerolog.Nop()
	m := metrics.New()

	authCfg := auth.Config{
		BackendURL:        backend.srv.URL,
		ServiceToken:      "svc-tok",
		DefaultTTLSeconds: 60,
		RequestTimeout:    2 * time.Second,
		Breaker: auth.BreakerConfig{
			WindowBuckets:        30,
			BucketSeconds:        1,
			FailureRateThreshold: 0.5,
			DebounceFloor:        20,
			Cooldown:             30 * time.Second,
			StaleRefreshJitter:   5 * time.Second,
		},
	}
	breaker := auth.NewBreaker(authCfg.Breaker, auth.BreakerDeps{
		Prober:  sseTestProber(func(_ context.Context) error { return nil }),
		Logger:  logger,
		Metrics: m,
	})
	authClient := auth.New(authCfg, auth.Deps{
		Logger:  logger,
		Breaker: breaker,
		Metrics: m,
	})

	if lcfg == nil {
		lcfg = &limits.Config{
			GlobalConcurrent:     1024,
			PerUserConcurrentMax: 10,
			PerUserRatePerSecond: 100,
			PerUserBurst:         100,
			PreAuthRatePerSecond: 100,
			PreAuthBurst:         100,
			SweepInterval:        60 * time.Second,
			SweepIdleThreshold:   5 * time.Minute,
		}
	}
	lim := limits.New(*lcfg, limits.Deps{Logger: logger, Metrics: m})
	authReg := auth.NewSubscribers(auth.SubscribersDeps{
		Logger:  logger,
		Metrics: m,
	})

	// Every Handler test gets a real *WriterPool. A small
	// config keeps the worker count low (factor 1; on a 4-core machine
	// that's 4 workers per test). The test pool uses NewEncoder(maxPayload)
	// for parity with production and fakeMetrics from pool_test.go to
	// avoid registering Prometheus collectors twice across tests. Pool is
	// shut down on test cleanup so worker goroutines do not outlive the
	// test (race-detector would flag otherwise).
	enc := NewEncoder(cfg.MaxPayloadBytes)
	pool := NewPool(PoolConfig{
		PoolFactor:   1,
		SubQueueSize: 8,
		MaxWaitMs:    2,
		WriteTimeout: cfg.WriteTimeout,
		// HeartbeatInterval inherited from cfg.HeartbeatInterval
		// (~200ms in tests) so the per-worker
		// heartbeat ticker emits `:\n\n` frames within the test's
		// 1.5s window. The worker-driven sweep is the production
		// heartbeat path; this test scaffolding exercises it
		// end-to-end (TestHandler_HeartbeatArrivesWithinHeartbeatInterval).
		HeartbeatInterval:  cfg.HeartbeatInterval,
		DrainThresholdSubs: 1, // drain the heartbeat frame immediately
	}, PoolDeps{
		Encoder: enc,
		Metrics: newFakeMetrics(),
		Logger:  logger,
	})
	t.Cleanup(func() { _ = pool.Shutdown(context.Background()) })

	// Handler Config embeds the router and auth configs so NewHandler
	// takes a single Config bag.
	cfg.Router = rcfg
	cfg.Auth = authCfg
	h := NewHandler(cfg, Deps{
		Broadcaster: bc,
		Auth: AuthDeps{
			Client:      authClient,
			Subscribers: authReg,
			Breaker:     breaker,
		},
		Limits:  lim,
		Pool:    pool,
		Logger:  logger,
		Metrics: m,
	})
	return &testHandlerKit{
		h:          h,
		bc:         bc,
		backend:    backend,
		limits:     lim,
		authReg:    authReg,
		breaker:    breaker,
		authClient: authClient,
		pool:       pool,
	}
}

// newTestServer builds an httptest.NewServer with the handler's routes
// registered. Returns the server URL and a teardown.
func newTestServer(t *testing.T, h *Handler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	h.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// validMapBackend installs a fake-backend response that always returns 200
// with a Whitelist for users:* having tables=users[id,name].
func validMapBackend(b *fakeAuthBackend) {
	b.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
	})
}

// validRequest builds a GET request with a valid bearer token attached.
func validRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer valid")
	return req
}

// readUntil reads from r until it has read at least n bytes OR the deadline
// expires. Returns whatever was accumulated.
// Uses a shared []byte protected by a mutex so the caller sees the partial
// accumulator on timeout, not just the final assignment after the goroutine
// completes.
func readUntil(t *testing.T, r io.Reader, n int, deadline time.Duration) []byte {
	t.Helper()
	var mu sync.Mutex
	buf := make([]byte, 0, n)
	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(r)
		for {
			mu.Lock()
			haveEnough := len(buf) >= n
			mu.Unlock()
			if haveEnough {
				return
			}
			chunk := make([]byte, 256)
			nr, rerr := br.Read(chunk)
			if nr > 0 {
				mu.Lock()
				buf = append(buf, chunk[:nr]...)
				mu.Unlock()
			}
			if rerr != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(deadline):
	}
	mu.Lock()
	out := make([]byte, len(buf))
	copy(out, buf)
	mu.Unlock()
	return out
}

// --- Phase-2 invariant regression tests (kept unchanged in spirit) ---

func TestHandler_ExactRoute_Headers(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
	hdr := resp.Header
	if got := hdr.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q; want %q", got, "text/event-stream")
	}
	if got := hdr.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q; want %q", got, "no-cache")
	}
	// writeSSEHeaders sets `Transfer-Encoding: identity` so
	// net/http suppresses its auto-chunked encoder and emits
	// `Connection: close` framing instead. The handler-set
	// `Connection: keep-alive` is overridden by net/http when the
	// response is identity-encoded — accept either to remain robust
	// across Go versions, but assert NO Transfer-Encoding: chunked
	// on the wire.
	if got := hdr.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q; want %q", got, "no")
	}
	for _, te := range resp.TransferEncoding {
		if te == "chunked" {
			t.Errorf("Transfer-Encoding contains %q; want no chunked framing ()", te)
		}
	}
	if !containsCSV(hdr.Values("Vary"), "Origin") {
		t.Errorf("Vary headers = %v; want one to include %q", hdr.Values("Vary"), "Origin")
	}

	// Now artificially close the subscriber and assert the response closes.
	sub := kit.bc.firstSub(t)
	if sub.Kind() != router.KindExact {
		t.Errorf("sub.Kind = %q; want %q", sub.Kind(), router.KindExact)
	}
	if sub.Table() != "users" {
		t.Errorf("sub.Table = %q; want %q", sub.Table(), "users")
	}
	if sub.PK() != "42" {
		t.Errorf("sub.PK = %q; want %q", sub.PK(), "42")
	}
	if sub.Filter == nil {
		t.Error("router.Subscriber.Filter is nil; expected FilterWithLSN to be wired")
	}
	sub.Drop("test")

	closed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Errorf("response did not close within 2s after Drop")
	}
}

func TestHandler_WildcardRoute_Headers(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/all", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("Content-Type = %q; want %q", resp.Header.Get("Content-Type"), "text/event-stream")
	}
	if !containsCSV(resp.Header.Values("Vary"), "Origin") {
		t.Errorf("Vary headers = %v; want one to include %q", resp.Header.Values("Vary"), "Origin")
	}

	sub := kit.bc.firstSub(t)
	if sub.Kind() != router.KindWildcard {
		t.Errorf("sub.Kind = %q; want %q", sub.Kind(), router.KindWildcard)
	}
	if sub.Table() != "users" {
		t.Errorf("sub.Table = %q; want %q", sub.Table(), "users")
	}
	if sub.PK() != "" {
		t.Errorf("sub.PK = %q; want empty (wildcard)", sub.PK())
	}
	sub.Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestHandler_InvalidTable_400(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp, err := http.Get(srv.URL + "/sse/v1/USERS/42")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q; want %q (no SSE headers on 400)", got, "application/json")
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"invalid_channel"`) {
		t.Errorf("body = %q; want to contain %q", body, `"invalid_channel"`)
	}
	if !containsCSV(resp.Header.Values("Vary"), "Origin") {
		t.Errorf("Vary headers = %v; want one to include %q (must be set even on error)", resp.Header.Values("Vary"), "Origin")
	}
}

func TestHandler_PK_TooLong_400(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	longPK := strings.Repeat("x", 257)
	resp, err := http.Get(srv.URL + "/sse/v1/users/" + longPK)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"invalid_channel"`) {
		t.Errorf("body = %q; want to contain %q", body, `"invalid_channel"`)
	}
}

func TestHandler_PKAll_OnExactRoute_400(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)

	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/all", nil)
	r.SetPathValue("table", "users")
	r.SetPathValue("pk", "all")
	w := httptest.NewRecorder()

	kit.h.serveExact(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"invalid_channel"`) {
		t.Errorf("body = %q; want to contain %q", body, `"invalid_channel"`)
	}
}

func TestHandler_InvalidSinceLSN_400(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp, err := http.Get(srv.URL + "/sse/v1/users/42?since_lsn=garbage")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"invalid_since_lsn"`) {
		t.Errorf("body = %q; want to contain %q", body, `"invalid_since_lsn"`)
	}
}

func TestHandler_DefaultStartLSN_UsesCurrentLSN(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	wantLSN := wal.CurrentLSN()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	sub := kit.bc.firstSub(t)
	gotLSN := sub.StartLSN()
	if gotLSN < wantLSN {
		t.Errorf("StartLSN = %s; want >= wal.CurrentLSN()=%s", gotLSN.String(), wantLSN.String())
	}

	sub.Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestHandler_OptionsPreflight_204(t *testing.T) {
	t.Parallel()

	allowed := "https://allowed.example"
	kit := newTestHandler(t, []string{allowed}, nil)
	srv := newTestServer(t, kit.h)

	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Origin", allowed)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d; want 204", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != allowed {
		t.Errorf("Access-Control-Allow-Origin = %q; want %q", got, allowed)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET, OPTIONS" {
		t.Errorf("Access-Control-Allow-Methods = %q; want %q", got, "GET, OPTIONS")
	}
}

// TestServePreflight_ReflectsAllowedOrigin locks the full CORS preflight
// wire contract: an OPTIONS request from an Origin in cors_origins receives
// 204 + ACAO + Vary: Origin + Allow-Methods (GET, OPTIONS) +
// Allow-Headers including Authorization.
func TestServePreflight_ReflectsAllowedOrigin(t *testing.T) {
	t.Parallel()

	const allowed = "http://localhost:8081"
	kit := newTestHandler(t, []string{allowed}, nil)
	srv := newTestServer(t, kit.h)

	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/sse/v1/orders/1", nil)
	req.Header.Set("Origin", allowed)
	req.Header.Set("Access-Control-Request-Method", "GET")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d; want 204", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != allowed {
		t.Errorf("Access-Control-Allow-Origin = %q; want %q", got, allowed)
	}
	if !containsCSV(resp.Header.Values("Vary"), "Origin") {
		t.Errorf("Vary headers = %v; want one to include %q", resp.Header.Values("Vary"), "Origin")
	}
	allowMethods := resp.Header.Get("Access-Control-Allow-Methods")
	if !strings.Contains(allowMethods, "GET") || !strings.Contains(allowMethods, "OPTIONS") {
		t.Errorf("Access-Control-Allow-Methods = %q; want to contain both GET and OPTIONS", allowMethods)
	}
	if !strings.Contains(resp.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Errorf("Access-Control-Allow-Headers = %q; want to contain Authorization", resp.Header.Get("Access-Control-Allow-Headers"))
	}
}

// TestServeExact_ReflectsAllowedOriginOnSSEResponse locks the SSE-response
// side of the same wire contract: a cross-origin GET from an Origin in
// cors_origins receives Access-Control-Allow-Origin and Vary: Origin on
// the streaming response.
func TestServeExact_ReflectsAllowedOriginOnSSEResponse(t *testing.T) {
	t.Parallel()

	const allowed = "http://localhost:8081"
	kit := newTestHandler(t, []string{allowed}, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	req.Header.Set("Origin", allowed)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != allowed {
		t.Errorf("Access-Control-Allow-Origin = %q; want %q (cross-origin GET must reflect allowlist match)", got, allowed)
	}
	if !containsCSV(resp.Header.Values("Vary"), "Origin") {
		t.Errorf("Vary headers = %v; want one to include %q", resp.Header.Values("Vary"), "Origin")
	}

	// Close the subscriber cleanly to release goroutines.
	kit.bc.firstSub(t).Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
}

// TestHandler_HeartbeatArrivesWithinHeartbeatInterval verifies the
// heartbeat contract end-to-end through the handler: a sub that sends
// zero data frames observes a `:\n\n` heartbeat within
// HeartbeatInterval. The worker-driven sweep is wired in production;
// this test restores the byte-level heartbeat assertion that had
// dropped earlier.
// The handler is constructed with HeartbeatInterval inherited from
// cfg.HeartbeatInterval — newTestHandler sets that to a small value
// (~200ms) so the test window is tight without being flaky.
func TestHandler_HeartbeatArrivesWithinHeartbeatInterval(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	// Read enough bytes to include the prelude (14 bytes) plus at
	// least one full heartbeat (3 bytes). Allow 1.5s — handler test
	// HeartbeatInterval is ~200ms so a heartbeat arrives well within
	// budget.
	const prelude = "retry: 15000\n\n"
	const hb = ":\n\n"
	buf := readUntil(t, resp.Body, len(prelude)+len(hb), 1500*time.Millisecond)
	body := string(buf)
	if !strings.HasPrefix(body, prelude) {
		t.Errorf("body does not begin with prelude %q; got first bytes = %q", prelude, body)
	}
	if !strings.Contains(body, hb) {
		t.Errorf("body does not contain heartbeat %q within HeartbeatInterval; got %q", hb, body)
	}

	kit.bc.firstSub(t).Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
}

// TestHandler_PreludeArrivesAtConnectionOpen asserts WIRE-02: the
// `retry: 15000\n\n` prelude is the first thing on the wire after a
// successful handshake. The prelude is emitted by pool.Attach (not
// the deleted writer.go). The heartbeat assertion lives in
// TestHandler_HeartbeatArrivesWithinHeartbeatInterval.
func TestHandler_PreludeArrivesAtConnectionOpen(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	// The 14-byte WALERA-01 retry prelude ("retry: 15000\n\n") is the
	// first bytes pool.Attach writes after the handler hijacks the conn.
	const prelude = "retry: 15000\n\n"
	buf := readUntil(t, resp.Body, len(prelude), 1500*time.Millisecond)
	if len(buf) < len(prelude) {
		t.Fatalf("did not receive prelude within 1.5s; got %d bytes (%q)", len(buf), buf)
	}
	if !strings.HasPrefix(string(buf), prelude) {
		t.Errorf("body does not begin with WALERA-01 prelude %q; got first %d bytes = %q", prelude, len(prelude), buf[:len(prelude)])
	}

	kit.bc.firstSub(t).Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
}

// TestHandler_RegistersSubscriberAndWireSendFuncIsCallable asserts that
// after the handler completes its handshake the registered
// router.Subscriber accepts a WireSendFunc closure call without panicking.
// The closure is what `pool.Attach` wires for production; the wire-byte
// parity is covered by the golden-fixture replay test.
func TestHandler_RegistersSubscriberAndWireSendFuncIsCallable(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	defer resp.Body.Close()

	sub := kit.bc.firstSub(t)

	// Contract: WireSendFunc accepts a closure and the
	// subscriber's surface is non-nil. We do not exercise the unexported
	// router.Subscriber.send() method here — the per-sub send path is
	// covered by internal/router unit tests in the same package.
	var recordedMu sync.Mutex
	var recorded [][]byte
	sub.WireSendFunc(func(frame []byte) bool {
		recordedMu.Lock()
		defer recordedMu.Unlock()
		fc := make([]byte, len(frame))
		copy(fc, frame)
		recorded = append(recorded, fc)
		return true
	})

	// Document the per-sub Event shape the router still synthesises before
	// encoding (the values are intentionally unused — kept here so the
	// test reviewer can see that wal.Tx / router.Event are still part of
	// the routing pipeline post-16-01).
	_ = wal.Tx{
		ID:        12345,
		CommitLSN: pglogrepl.LSN(0x100),
		CommitTS:  time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC),
		Changes: []wal.Change{
			{Schema: "public", Table: "users", Op: wal.OpInsert, PK: "42", PKCol: "id", Data: map[string]any{"id": "42"}},
		},
	}
	_ = router.Event{}

	if sub.ID() == "" {
		t.Errorf("registered subscriber has empty ID")
	}
	if got := sub.Kind(); got != router.KindExact {
		t.Errorf("Kind = %q; want %q", got, router.KindExact)
	}

	sub.Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)

	// Silence unused-variable lint for the recorder (the wire-byte
	// assertions return in 16-04). Keeping the recorder in source proves
	// the surface compiles + accepts the closure shape the
	// pool wires in 16-03.
	recordedMu.Lock()
	_ = len(recorded)
	recordedMu.Unlock()
}

// --- Phase-3 handshake sequence tests ---

// TestHandshake_GlobalSemaphoreExhausted — gate 1 fires before gate 3 (no
// auth call). Pre-acquire the global slot to cap = 1.
func TestHandshake_GlobalSemaphoreExhausted(t *testing.T) {
	t.Parallel()

	lcfg := &limits.Config{
		GlobalConcurrent:     1,
		PerUserConcurrentMax: 10,
		PerUserRatePerSecond: 100, PerUserBurst: 100,
		PreAuthRatePerSecond: 100, PreAuthBurst: 100,
		SweepInterval:      60 * time.Second,
		SweepIdleThreshold: 5 * time.Minute,
	}
	kit := newTestHandler(t, nil, lcfg)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	// Saturate the global semaphore from outside the handler.
	if !kit.limits.AcquireGlobal() {
		t.Fatal("pre-acquire failed; cap may be wrong")
	}
	defer kit.limits.ReleaseGlobal()

	hits0 := kit.backend.hits.Load()

	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do(): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q; want %q", got, "5")
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Errorf("body = %q; want empty", body)
	}
	if got := kit.backend.hits.Load(); got != hits0 {
		t.Errorf("auth backend hits delta = %d; want 0 (gate 1 must fire before gate 3)", got-hits0)
	}
}

// TestHandshake_PreAuthRateExceeded — gate 2 fires after exhausting the
// per-IP token bucket.
func TestHandshake_PreAuthRateExceeded(t *testing.T) {
	t.Parallel()

	lcfg := &limits.Config{
		GlobalConcurrent:     1024,
		PerUserConcurrentMax: 10,
		PerUserRatePerSecond: 100, PerUserBurst: 100,
		PreAuthRatePerSecond: 0.0001, PreAuthBurst: 1, // effectively single-use
		SweepInterval:      60 * time.Second,
		SweepIdleThreshold: 5 * time.Minute,
	}
	kit := newTestHandler(t, nil, lcfg)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	// Drain the bucket from outside the handler. The fake-httptest client's
	// IP is 127.0.0.1, the same key the handler will use.
	for i := 0; i < 5; i++ {
		_ = kit.limits.AllowPreAuthRate("127.0.0.1")
	}

	hits0 := kit.backend.hits.Load()
	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do(): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d; want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q; want %q", got, "1")
	}
	if got := kit.backend.hits.Load(); got != hits0 {
		t.Errorf("auth backend hits delta = %d; want 0", got-hits0)
	}
}

// TestHandshake_MissingBearerReturns401 — handler-internal 401 before any
// auth call (the auth client is never invoked without a token).
func TestHandshake_MissingBearerReturns401(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	hits0 := kit.backend.hits.Load()
	resp, err := http.Get(srv.URL + "/sse/v1/users/42") // no Authorization header
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", resp.StatusCode)
	}
	if got := kit.backend.hits.Load(); got != hits0 {
		t.Errorf("auth backend hits delta = %d; want 0", got-hits0)
	}
}

// TestHandshake_AuthBackend401Forwarded: the auth backend's 401 body
// must be forwarded verbatim.
func TestHandshake_AuthBackend401Forwarded(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	kit.backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("revoked"))
	})
	srv := newTestServer(t, kit.h)

	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "revoked" {
		t.Errorf("body = %q; want %q", body, "revoked")
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q; want %q", got, "application/json")
	}

	// AcquireGlobal must have been released on the error path — pre-acquire
	// up to cap and confirm none of the slots are still pinned.
	if !kit.limits.AcquireGlobal() {
		t.Error("global semaphore not released on 401 path")
	}
	kit.limits.ReleaseGlobal()
}

func TestHandshake_AuthBackend403Forwarded(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"reason":"upstream-forbidden"}`))
	})
	srv := newTestServer(t, kit.h)
	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d; want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "upstream-forbidden") {
		t.Errorf("body = %q; want to contain upstream body", body)
	}
}

func TestHandshake_AuthBackend404Forwarded(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"reason":"channel-gone"}`))
	})
	srv := newTestServer(t, kit.h)
	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d; want 404", resp.StatusCode)
	}
}

func TestHandshake_AuthBackend5xxReturns503(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	srv := newTestServer(t, kit.h)
	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d; want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q; want %q", got, "5")
	}
}

// TestHandshake_PerUserConcurrentExceeded — gate 4. Pre-load the per-user
// counter to cap; the next handshake gets 429.
func TestHandshake_PerUserConcurrentExceeded(t *testing.T) {
	t.Parallel()
	lcfg := &limits.Config{
		GlobalConcurrent:     1024,
		PerUserConcurrentMax: 1,
		PerUserRatePerSecond: 100, PerUserBurst: 100,
		PreAuthRatePerSecond: 100, PreAuthBurst: 100,
		SweepInterval:      60 * time.Second,
		SweepIdleThreshold: 5 * time.Minute,
	}
	kit := newTestHandler(t, nil, lcfg)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	if ok, _ := kit.limits.AcquirePerUser("u1"); !ok {
		t.Fatal("pre-acquire per-user failed")
	}
	defer kit.limits.ReleasePerUser("u1")

	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d; want 429", resp.StatusCode)
	}
}

// TestHandshake_LegacyRootsIgnored — a backend that still emits the legacy
// `roots` field (whose value is unrelated to Tables) must not affect the
// authorization decision. The requested table comes from the URL/channel and
// is authorized solely by its presence in Tables.
func TestHandshake_LegacyRootsIgnored(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user_id":"u1","roots":["orders"],"tables":{"users":["id"]},"ttl_seconds":60}`))
	})
	srv := newTestServer(t, kit.h)

	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}
	kit.bc.firstSub(t).Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
}

// TestHandshake_TableNotInWhitelistReturns403 — requested table must be in
// Tables. roots is ignored.
func TestHandshake_TableNotInWhitelistReturns403(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(permMapJSON("u1", map[string][]string{}, 60))
	})
	srv := newTestServer(t, kit.h)

	resp, err := http.DefaultClient.Do(validRequest(t, srv.URL+"/sse/v1/users/42"))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d; want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"reason":"not_allowed"}`+"\n" {
		t.Errorf("body = %q; want %q", body, `{"reason":"not_allowed"}`+"\n")
	}
}

// TestHandshake_HappyPath_RegistersSubscriberWithFilter — full happy-path
// shape: 200 SSE, sub.Filter non-nil, auth.Subscribers contains one entry.
func TestHandshake_HappyPath_RegistersSubscriberWithFilter(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}

	sub := kit.bc.firstSub(t)
	if sub.Filter == nil {
		t.Error("sub.Filter is nil after registration")
	}
	if got := kit.authReg.Len(); got != 1 {
		t.Errorf("auth.Subscribers.Len() = %d; want 1", got)
	}
	sub.Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)

	// After the writer exits and the deferred Remove fires, the registry
	// should drain back to zero.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if kit.authReg.Len() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("auth.Subscribers.Len() did not drain to 0; still %d", kit.authReg.Len())
}

// TestHandshake_HappyPathReleasesLimitsOnExit — after the connection closes,
// the per-user counter and the global slot are released.
func TestHandshake_HappyPathReleasesLimitsOnExit(t *testing.T) {
	t.Parallel()
	lcfg := &limits.Config{
		GlobalConcurrent:     1,
		PerUserConcurrentMax: 1,
		PerUserRatePerSecond: 100, PerUserBurst: 100,
		PreAuthRatePerSecond: 100, PreAuthBurst: 100,
		SweepInterval:      60 * time.Second,
		SweepIdleThreshold: 5 * time.Minute,
	}
	kit := newTestHandler(t, nil, lcfg)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}

	sub := kit.bc.firstSub(t)
	sub.Drop("test")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Wait briefly for the writer goroutine to unwind the defer chain.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if kit.limits.AcquireGlobal() {
			kit.limits.ReleaseGlobal()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !kit.limits.AcquireGlobal() {
		t.Error("global semaphore not released after writer exit")
	} else {
		kit.limits.ReleaseGlobal()
	}

	if ok, _ := kit.limits.AcquirePerUser("u1"); !ok {
		t.Error("per-user counter not released after writer exit")
	} else {
		kit.limits.ReleasePerUser("u1")
	}
}

// TestHandshake_AuthRevokedMidStream: backend returns a valid
// Whitelist first, then 401 on every subsequent call. With a short TTL the
// refresh ticker eventually drops the subscriber with reason=auth_revoked
// and the client receives the event:error frame.
func TestHandshake_AuthRevokedMidStream(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	srv := newTestServer(t, kit.h)

	// Override auth.Config to use a fast refresh interval so the test
	// doesn't wait 60s for the first refresh tick.
	// Build a fresh kit with shorter TTL.
	backend := kit.backend
	first := atomic.Bool{}
	backend.SetResp(func(w http.ResponseWriter, _ *http.Request) {
		if !first.Swap(true) {
			// First call → valid Whitelist with TTL = 1s for fast refresh.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 1))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"reason":"revoked"}`))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	// Read body bytes until we see event: error (with reason=auth_revoked)
	// or hit the deadline. With initial jitter [0, 500ms) and a 1s
	// ticker, the second auth call should land within ~1.5s.
	buf := readUntil(t, resp.Body, 1024, 4*time.Second)
	body := string(buf)
	if !strings.Contains(body, "event: error") {
		t.Errorf("body did not contain %q; got %q", "event: error", body)
	}
	if !strings.Contains(body, `"reason":"auth_revoked"`) {
		t.Errorf("body did not contain %q; got %q", `"reason":"auth_revoked"`, body)
	}
}

// TestResponses_NoSniffHeaderEverywhere closure: every
// response written by the SSE handler — SSE 200 stream, JSON error bodies
// (invalid_channel, invalid_since_lsn), JSON reason bodies (not_allowed),
// preflight 204, and 401 status-only — must carry
// X-Content-Type-Options: nosniff. The header is injected by the single
// writeNoSniff(w) helper at every response surface in internal/sse.
func TestResponses_NoSniffHeaderEverywhere(t *testing.T) {
	t.Parallel()

	// Helper: build a kit configured for each sub-case. The 200 SSE path
	// needs a valid backend Whitelist; the 401 path uses no Authorization header
	// (so the handler short-circuits before hitting the backend); the
	// not_allowed case installs a Whitelist without "users" in Tables.
	type setup struct {
		name       string
		backendFn  func(*fakeAuthBackend)
		method     string
		path       string
		setHeaders func(req *http.Request)
		wantStatus int
		// closeBody: for SSE 200 the test must Drop the subscriber after
		// reading headers so the writer returns and the body closes.
		isSSE bool
	}

	cases := []setup{
		{
			name:       "invalid_channel_400",
			backendFn:  validMapBackend,
			method:     http.MethodGet,
			path:       "/sse/v1/USERS/42", // uppercase table -> invalid
			setHeaders: nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_since_lsn_400",
			backendFn:  validMapBackend,
			method:     http.MethodGet,
			path:       "/sse/v1/users/42?since_lsn=garbage",
			setHeaders: nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_allowed_403",
			backendFn: func(b *fakeAuthBackend) {
				b.SetResp(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(permMapJSON("u1", map[string][]string{"orders": {"id"}}, 60))
				})
			},
			method: http.MethodGet,
			path:   "/sse/v1/users/42",
			setHeaders: func(req *http.Request) {
				req.Header.Set("Authorization", "Bearer valid")
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:      "preflight_204",
			backendFn: validMapBackend,
			method:    http.MethodOptions,
			path:      "/sse/v1/users/42",
			setHeaders: func(req *http.Request) {
				req.Header.Set("Origin", "http://example")
				req.Header.Set("Access-Control-Request-Method", "GET")
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "unauthorized_401",
			backendFn:  validMapBackend,
			method:     http.MethodGet,
			path:       "/sse/v1/users/42",
			setHeaders: nil, // no Authorization header -> 401 status-only
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:      "sse_200_stream",
			backendFn: validMapBackend,
			method:    http.MethodGet,
			path:      "/sse/v1/users/42",
			setHeaders: func(req *http.Request) {
				req.Header.Set("Authorization", "Bearer valid")
			},
			wantStatus: http.StatusOK,
			isSSE:      true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kit := newTestHandler(t, []string{"http://example"}, nil)
			tc.backendFn(kit.backend)
			srv := newTestServer(t, kit.h)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, tc.method, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if tc.setHeaders != nil {
				tc.setHeaders(req)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Errorf("status = %d; want %d", resp.StatusCode, tc.wantStatus)
			}
			if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q; want %q", got, "nosniff")
			}

			if tc.isSSE {
				// Close the writer loop so the test does not hang on the
				// 200-stream case.
				sub := kit.bc.firstSub(t)
				sub.Drop("test")
				_, _ = io.Copy(io.Discard, resp.Body)
			}
		})
	}
}

// TestHandshake_InvalidRequestID_400 closure: when a
// client sends a malformed X-Request-ID (too long, contains spaces, or
// contains characters outside ^[A-Za-z0-9._-]+$) the handler rejects with
// 400 invalid_request_id BEFORE any auth-backend call. The auth backend
// hit counter MUST be 0 for the rejected handshake.
func TestHandshake_InvalidRequestID_400(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		requestID string
	}{
		{"too_long_129_chars", strings.Repeat("A", 129)},
		{"has_spaces", "has spaces"},
		{"has_tabs", "value\twith\ttab"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kit := newTestHandler(t, nil, nil)
			validMapBackend(kit.backend)
			srv := newTestServer(t, kit.h)

			hitsBefore := kit.backend.hits.Load()

			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
			req.Header.Set("Authorization", "Bearer valid")
			req.Header.Set("X-Request-ID", tc.requestID)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d; want 400", resp.StatusCode)
			}
			body, _ := io.ReadAll(resp.Body)
			if string(body) != `{"error":"invalid_request_id"}`+"\n" {
				t.Errorf("body = %q; want %q", body, `{"error":"invalid_request_id"}`+"\n")
			}
			hitsAfter := kit.backend.hits.Load()
			if hitsAfter != hitsBefore {
				t.Errorf("auth backend was called %d time(s) for rejected handshake; want 0", hitsAfter-hitsBefore)
			}
		})
	}
}

// TestHandshake_ValidRequestID_PassedThrough happy path: a
// client-supplied X-Request-ID matching the full charset spectrum is
// forwarded verbatim to the auth backend, and an empty X-Request-ID is
// replaced by a generated 32-char hex ID.
func TestHandshake_ValidRequestID_PassedThrough(t *testing.T) {
	t.Parallel()

	t.Run("full_charset_passed_verbatim", func(t *testing.T) {
		t.Parallel()
		kit := newTestHandler(t, nil, nil)

		var gotRequestID string
		var mu sync.Mutex
		kit.backend.SetResp(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			gotRequestID = r.Header.Get("X-Request-ID")
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
		})
		srv := newTestServer(t, kit.h)

		const wantID = "abc.123-DEF_xyz"

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
		req.Header.Set("Authorization", "Bearer valid")
		req.Header.Set("X-Request-ID", wantID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d; want 200", resp.StatusCode)
		}
		sub := kit.bc.firstSub(t)
		sub.Drop("test")
		_, _ = io.Copy(io.Discard, resp.Body)
		mu.Lock()
		got := gotRequestID
		mu.Unlock()
		if got != wantID {
			t.Errorf("backend saw X-Request-ID = %q; want %q", got, wantID)
		}
	})

	t.Run("empty_generates_hex_id", func(t *testing.T) {
		t.Parallel()
		kit := newTestHandler(t, nil, nil)

		var gotRequestID string
		var mu sync.Mutex
		kit.backend.SetResp(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			gotRequestID = r.Header.Get("X-Request-ID")
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(permMapJSON("u1", map[string][]string{"users": {"id", "name"}}, 60))
		})
		srv := newTestServer(t, kit.h)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/sse/v1/users/42", nil)
		req.Header.Set("Authorization", "Bearer valid")
		// No X-Request-ID header.
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d; want 200", resp.StatusCode)
		}
		sub := kit.bc.firstSub(t)
		sub.Drop("test")
		_, _ = io.Copy(io.Discard, resp.Body)

		mu.Lock()
		got := gotRequestID
		mu.Unlock()
		hexRe := regexp.MustCompile(`^[a-f0-9]{32}$`)
		if !hexRe.MatchString(got) {
			t.Errorf("backend saw X-Request-ID = %q; want 32-char hex (^[a-f0-9]{32}$)", got)
		}
	})
}

// TestWriteJSONReason_EscapesCallerString closure:
// writeJSONReason must produce strictly-valid JSON for arbitrary caller-
// supplied reason strings, including byte sequences that the prior
// byte-string concatenation implementation would have corrupted (e.g.,
// `"; alert(1); //`). Round-tripping through json.Unmarshal proves both
// validity and exact-string preservation. The trailing newline is the
// documented, intentional wire-format change (Encoder semantics).
func TestWriteJSONReason_EscapesCallerString(t *testing.T) {
	t.Parallel()
	cases := []string{
		`"; alert(1); //`,
		"newline\nin\nstring",
		"\"double\\quotes\\\"",
		"unicode—😀—chars",
		"",
		strings.Repeat("a", 1024),
	}
	for _, in := range cases {
		in := in
		t.Run(fmt.Sprintf("input=%q", in), func(t *testing.T) {
			t.Parallel()
			kit := newTestHandler(t, nil, nil)
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
			kit.h.writeJSONReason(rr, req, 400, in)

			var got reasonBody
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
				t.Fatalf("body is not valid JSON: %v (body=%q)", err, rr.Body.String())
			}
			if got.Reason != in {
				t.Errorf("Reason round-trip mismatch: got %q want %q", got.Reason, in)
			}
			if !bytes.HasSuffix(rr.Body.Bytes(), []byte("\n")) {
				t.Errorf("body missing trailing newline: %q", rr.Body.String())
			}
		})
	}
}

// -- (clientIP + trusted-proxy XFF parsing) ---

func mkClientIPHandler(t *testing.T, proxies []string) *Handler {
	t.Helper()
	lcfg := &limits.Config{
		GlobalConcurrent:     100,
		PerUserConcurrentMax: 10,
		PerUserRatePerSecond: 100,
		PerUserBurst:         100,
		PreAuthRatePerSecond: 100,
		PreAuthBurst:         100,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
		TrustedProxies:       proxies,
	}
	kit := newTestHandler(t, nil, lcfg)
	return kit.h
}

func TestClientIP_EmptyAllowlist_ReturnsPeer(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, nil)
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "192.0.2.1:1234"
	r.Header.Set("X-Forwarded-For", "8.8.8.8") // ignored because allowlist empty
	if got := h.clientIP(r); got != "192.0.2.1" {
		t.Errorf("clientIP = %q; want %q", got, "192.0.2.1")
	}
}

func TestClientIP_PeerNotTrusted_IgnoresXFF(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "203.0.113.10:1234"
	r.Header.Set("X-Forwarded-For", "8.8.8.8")
	if got := h.clientIP(r); got != "203.0.113.10" {
		t.Errorf("clientIP = %q; want %q", got, "203.0.113.10")
	}
}

func TestClientIP_PeerTrusted_NoXFF(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	if got := h.clientIP(r); got != "10.0.0.1" {
		t.Errorf("clientIP = %q; want %q", got, "10.0.0.1")
	}
}

func TestClientIP_PeerTrusted_SingleHop(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.5")
	if got := h.clientIP(r); got != "203.0.113.5" {
		t.Errorf("clientIP = %q; want %q", got, "203.0.113.5")
	}
}

func TestClientIP_PeerTrusted_ChainAllTrusted(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "10.1.0.1, 10.2.0.1")
	// Entire chain trusted → fallback to leftmost (claimed client).
	if got := h.clientIP(r); got != "10.1.0.1" {
		t.Errorf("clientIP = %q; want %q", got, "10.1.0.1")
	}
}

func TestClientIP_PeerTrusted_TwoHops_RightToLeft(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.5, 10.2.0.1")
	// Right-to-left: skip 10.2.0.1 trusted, take 203.0.113.5 untrusted.
	if got := h.clientIP(r); got != "203.0.113.5" {
		t.Errorf("clientIP = %q; want %q", got, "203.0.113.5")
	}
}

func TestClientIP_IPv6Peer(t *testing.T) {
	t.Parallel()
	// IN-02: fc00::/7 is the canonical IPv6 ULA range (covers fc00::/8
	// and fd00::/8). The earlier fd00::/8 fixture worked because fd00::1
	// is inside it, but a copy-paste of that prefix into a production
	// configmap would silently trust four times the IPv6 space (the
	// entire f000::/8 block).
	h := mkClientIPHandler(t, []string{"fc00::/7"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "[fd00::1]:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.5")
	if got := h.clientIP(r); got != "203.0.113.5" {
		t.Errorf("clientIP = %q; want %q", got, "203.0.113.5")
	}
}

func TestClientIP_MalformedXFFEntry(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "not-an-ip, 10.2.0.1")
	// Malformed entry must NOT be returned verbatim. Fall back to
	// the peer host (trusted, bounded) so attacker-controlled garbage
	// cannot poison the per-IP rate-limit map.
	if got := h.clientIP(r); got != "10.0.0.1" {
		t.Errorf("clientIP = %q; want %q (peer host fallback)", got, "10.0.0.1")
	}
}

// TestClientIP_MalformedXFFEntry_RotatingTailsCollapse asserts that
// two distinct attacker-controlled malformed tails collapse onto a
// single rate-limit key (the peer host), defeating the cache-busting
// attack against the per-IP rate-limit map.
func TestClientIP_MalformedXFFEntry_RotatingTailsCollapse(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r1 := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r1.RemoteAddr = "10.0.0.1:1234"
	r1.Header.Set("X-Forwarded-For", "real-client, ${rand1}")
	r2 := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r2.RemoteAddr = "10.0.0.1:5678"
	r2.Header.Set("X-Forwarded-For", "real-client, ${rand2}")
	g1, g2 := h.clientIP(r1), h.clientIP(r2)
	if g1 != g2 {
		t.Errorf("rotating malformed tails produced distinct keys: %q vs %q (rate-limit bypass)", g1, g2)
	}
	if g1 != "10.0.0.1" {
		t.Errorf("clientIP = %q; want %q (peer host fallback)", g1, "10.0.0.1")
	}
}

// TestClientIP_IPv6XFFEntry_BracketsStripped verifies bracket-wrapped
// IPv6 entries in XFF are accepted and returned in canonical
// net.IP.String() form.
func TestClientIP_IPv6XFFEntry_BracketsStripped(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "[2001:db8::1]")
	if got := h.clientIP(r); got != "2001:db8::1" {
		t.Errorf("clientIP = %q; want %q (canonical IPv6, brackets stripped)", got, "2001:db8::1")
	}
}

// TestClientIP_MixedValidInvalidXFF exercises a mixed XFF chain with
// one malformed entry. The right-to-left scan hits
// the malformed entry before reaching the untrusted hop, so the peer
// host fallback engages.
func TestClientIP_MixedValidInvalidXFF(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.5, garbage, 10.2.0.1")
	// Right-to-left: 10.2.0.1 trusted (skip), "garbage" malformed → fall
	// back to peer host.
	if got := h.clientIP(r); got != "10.0.0.1" {
		t.Errorf("clientIP = %q; want %q (peer host fallback on malformed)", got, "10.0.0.1")
	}
}

// TestClientIP_SingleEntryXFF: a single untrusted entry is returned
// in canonical form.
func TestClientIP_SingleEntryXFF(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.5")
	if got := h.clientIP(r); got != "203.0.113.5" {
		t.Errorf("clientIP = %q; want %q", got, "203.0.113.5")
	}
}

// TestClientIP_EmptyXFFHeader: an empty XFF header value (header
// present but empty string) falls back to peer host.
func TestClientIP_EmptyXFFHeader(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "")
	if got := h.clientIP(r); got != "10.0.0.1" {
		t.Errorf("clientIP = %q; want %q", got, "10.0.0.1")
	}
}

// TestClientIP_MultipleXFFHeaders.
// RFC 7230 §3.2.2 permits the same header multiple times. r.Header.Get
// returns only the first, silently dropping subsequent entries. Verify
// the implementation gathers ALL values via r.Header.Values and joins
// them so the right-to-left scan walks the full chain.
func TestClientIP_MultipleXFFHeaders(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	// Two separate XFF header instances — note Add not Set.
	r.Header.Add("X-Forwarded-For", "203.0.113.5")
	r.Header.Add("X-Forwarded-For", "10.2.0.1")
	// After Values+Join the chain is "203.0.113.5,10.2.0.1". Right-to-left:
	// 10.2.0.1 trusted (skip), 203.0.113.5 untrusted → return it.
	if got := h.clientIP(r); got != "203.0.113.5" {
		t.Errorf("clientIP = %q; want %q (must walk BOTH XFF headers)", got, "203.0.113.5")
	}
}

// TestClientIP_AllMalformedEntries: an XFF chain entirely composed of
// malformed entries falls back to peer host (never returns garbage).
func TestClientIP_AllMalformedEntries(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "foo, bar, baz")
	if got := h.clientIP(r); got != "10.0.0.1" {
		t.Errorf("clientIP = %q; want %q (peer fallback)", got, "10.0.0.1")
	}
}

func TestClientIP_XFFWhitespace(t *testing.T) {
	t.Parallel()
	h := mkClientIPHandler(t, []string{"10.0.0.0/8"})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "  203.0.113.5  ,  10.2.0.1  ")
	if got := h.clientIP(r); got != "203.0.113.5" {
		t.Errorf("clientIP = %q; want %q", got, "203.0.113.5")
	}
}

// -- (CORS canonicalisation handlers) ---

func TestCORS_CaseInsensitiveOriginMatch(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "https://EXAMPLE.com")
	w := httptest.NewRecorder()
	allowed, _ := handleCORS(w, r, []string{"https://example.com"})
	if !allowed {
		t.Errorf("allowed = false; want true")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://EXAMPLE.com" {
		t.Errorf("ACAO = %q; want %q (reflected ORIGINAL)", got, "https://EXAMPLE.com")
	}
}

func TestCORS_TrailingSlashTolerance(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "https://example.com/")
	w := httptest.NewRecorder()
	allowed, _ := handleCORS(w, r, []string{"https://example.com"})
	if !allowed {
		t.Errorf("allowed = false; want true")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com/" {
		t.Errorf("ACAO = %q; want %q (reflected ORIGINAL incl trailing slash)", got, "https://example.com/")
	}
}

func TestCORS_PortPreservedAsTyped(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "https://example.com:8080")
	w := httptest.NewRecorder()
	allowed, _ := handleCORS(w, r, []string{"https://example.com"})
	if allowed {
		t.Errorf("allowed = true; want false (port preserved as-typed; no default-port normalisation)")
	}
}

func TestCORS_ReflectedOriginUsesRequestForm(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "HTTPS://Example.COM/Some/Path")
	w := httptest.NewRecorder()
	allowed, _ := handleCORS(w, r, []string{"https://example.com"})
	if !allowed {
		t.Errorf("allowed = false; want true (canonicalisation should match)")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "HTTPS://Example.COM/Some/Path" {
		t.Errorf("ACAO = %q; want %q (reflected original byte-for-byte)", got, "HTTPS://Example.COM/Some/Path")
	}
}

func TestCORS_MalformedOrigin_NoMatch(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "not-a-url")
	w := httptest.NewRecorder()
	allowed, origin := handleCORS(w, r, []string{"https://example.com"})
	if allowed {
		t.Errorf("allowed = true; want false (malformed Origin)")
	}
	if origin != "not-a-url" {
		t.Errorf("origin = %q; want %q (literal request Origin)", origin, "not-a-url")
	}
	if w.Header().Get("Vary") != "Origin" {
		t.Errorf("Vary = %q; want %q (must be set unconditionally)", w.Header().Get("Vary"), "Origin")
	}
}

// containsCSV reports whether any of vals contains target (after
// comma-splitting and trimming). net/http represents multi-Vary either as
// multiple header lines or a single comma-joined line; we accept either.
func containsCSV(vals []string, target string) bool {
	for _, v := range vals {
		for _, p := range strings.Split(v, ",") {
			if strings.TrimSpace(p) == target {
				return true
			}
		}
	}
	return false
}

// TestHandler_HijackedTCPConn_WireBytes is the integration-level
// proof that the real *net.TCPConn hijack path produces a valid HTTP/1.1
// SSE response. Connects raw net.Dial to an httptest.NewServer (loopback
// TCP, hijack-capable), drives a minimal SSE GET handshake, parses the
// response off the wire, and asserts:
//   - Status line `HTTP/1.1 200 OK`.
//   - `Content-Type: text/event-stream` present.
//   - `Transfer-Encoding: chunked` ABSENT (the 16-03 hazard — the whole
//     point of wire-correctness).
//   - First 14 body bytes after the header terminator (CRLF CRLF) are
//     exactly `retry: 15000\n\n` (WALERA-01 prelude, emitted by
//     pool.Attach on the hijacked conn).
//
// httptest.ResponseRecorder does NOT support Hijack — every other handler
// test that uses http.DefaultClient + httptest.NewServer exercises the
// hijack path too (because httptest.NewServer is a real TCP listener); this
// test additionally inspects the raw wire bytes so the chunked-encoding
// invariant is asserted directly.  / WIRE-02.
func TestHandler_HijackedTCPConn_WireBytes(t *testing.T) {
	t.Parallel()

	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// 2s deadline covers the full handshake (auth backend, limits gates,
	// pool.Attach prelude write). On expiry the read returns whatever
	// was accumulated so the test fails with a useful message instead
	// of hanging.
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	host := srv.Listener.Addr().String()
	req := "GET /sse/v1/users/42 HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Authorization: Bearer valid\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}

	// Parse status line + headers off the raw wire. Cannot use
	// http.ReadResponse here because Go's response parser would consume
	// the body bytes through its Transfer-Encoding decoder; we want the
	// raw post-header bytes.
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	statusLine = strings.TrimRight(statusLine, "\r\n")
	if statusLine != "HTTP/1.1 200 OK" {
		t.Fatalf("status line = %q; want %q", statusLine, "HTTP/1.1 200 OK")
	}

	var (
		gotContentType   string
		sawChunkedTE     bool
		sawContentTypeTE bool
	)
	for {
		line, lerr := br.ReadString('\n')
		if lerr != nil {
			t.Fatalf("read header line: %v", lerr)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break // header terminator
		}
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			t.Fatalf("malformed header line: %q", line)
		}
		name := strings.TrimSpace(line[:colon])
		val := strings.TrimSpace(line[colon+1:])
		switch strings.ToLower(name) {
		case "content-type":
			gotContentType = val
			sawContentTypeTE = true
		case "transfer-encoding":
			for _, te := range strings.Split(val, ",") {
				if strings.TrimSpace(strings.ToLower(te)) == "chunked" {
					sawChunkedTE = true
				}
			}
		}
	}
	if !sawContentTypeTE || gotContentType != "text/event-stream" {
		t.Errorf("Content-Type = %q; want %q", gotContentType, "text/event-stream")
	}
	if sawChunkedTE {
		t.Errorf("Transfer-Encoding contains chunked; want absent ( / WIRE-02)")
	}

	// First 14 bytes after header terminator must be the WALERA-01 prelude.
	const prelude = "retry: 15000\n\n"
	buf := make([]byte, len(prelude))
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("read prelude: %v", err)
	}
	if !bytes.Equal(buf, []byte(prelude)) {
		t.Errorf("prelude = %q; want %q", buf, prelude)
	}

	// Drop the subscriber so the handler returns and the deferred
	// tcpConn.Close + Drop fire cleanly. Closing the client side here
	// races the handler; cleaner to ask the broadcaster to Drop.
	kit.bc.firstSub(t).Drop("test")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	// Drain any final frames (e.g. event: error) and EOF.
	_, _ = io.Copy(io.Discard, br)
}
