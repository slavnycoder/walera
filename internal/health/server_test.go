// Package health — server_test.go covers /healthz, /readyz, /metrics handlers
// plus the background readyz prober. All tests use httptest.NewRecorder +
// http.NewServeMux to exercise the registered routes end-to-end, plus stub
// PgChecker/AuthChecker values to simulate PG/auth state transitions
// deterministically. Stdlib testing only.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/metrics"
)

// ---------------------------------------------------------------------
// Test stubs
// ---------------------------------------------------------------------

// stubReader satisfies the PgChecker interface defined in server.go.
// Toggle PG state mid-flight via connected.Store(true|false).
type stubReader struct {
	connected atomic.Bool
}

func (s *stubReader) CheckPG(_ context.Context) error {
	if s.connected.Load() {
		return nil
	}
	return errors.New("pg disconnected")
}

// stubAuthClient satisfies the AuthChecker interface defined in server.go.
// Counts CheckAuth calls (for TestHealthz_NeverCallsAuthBackend) and
// returns a swappable error.
type stubAuthClient struct {
	hits atomic.Int64
	err  atomic.Pointer[error]
}

func (s *stubAuthClient) CheckAuth(_ context.Context) error {
	s.hits.Add(1)
	if e := s.err.Load(); e != nil {
		return *e
	}
	return nil
}

func (s *stubAuthClient) setErr(e error) {
	if e == nil {
		s.err.Store(nil)
		return
	}
	s.err.Store(&e)
}

// newTestServer builds a Server with the given stubs + a default 50ms probe
// interval (callers override via the cfg argument when needed).
func newTestServer(t *testing.T, reader PgChecker, ac AuthChecker, cfg Config) *Server {
	t.Helper()
	return newServerInternal(reader, ac, metrics.New(), cfg, zerolog.Nop())
}

// ---------------------------------------------------------------------
// /healthz tests
// ---------------------------------------------------------------------

func TestHealthz_200WhenReaderConnected(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	if rr.Body.Len() != 0 {
		t.Errorf("body = %q; want empty", rr.Body.String())
	}
}

func TestHealthz_503WhenReaderDisconnected(t *testing.T) {
	t.Parallel()
	r := &stubReader{} // connected=false
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q; want text/plain*", ct)
	}
	if got, want := rr.Body.String(), "pg disconnected"; got != want {
		t.Errorf("body = %q; want %q", got, want)
	}
}

// Critical invariant: /healthz NEVER touches the auth backend, even across
// many calls. If it did, an auth outage would trigger k8s liveness failure
// and pod restart — defeating the two-track health/readiness policy.
func TestHealthz_NeverCallsAuthBackend(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	// Make auth "fail" so a buggy implementation that calls CheckAuth()
	// would be tempted to flip the response — but the test asserts the call
	// counter remains zero regardless.
	ac.setErr(errors.New("auth backend on fire"))

	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})
	mux := http.NewServeMux()
	s.Routes(mux)

	for i := 0; i < 5; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("iter %d: status = %d; want 200 (reader connected)", i, rr.Code)
		}
	}
	if got := ac.hits.Load(); got != 0 {
		t.Fatalf("AuthChecker.CheckAuth() hits = %d; want 0 — /healthz must NOT touch auth", got)
	}
}

// ---------------------------------------------------------------------
// /readyz tests
// ---------------------------------------------------------------------

func TestReadyz_200WhenBothHealthy(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})
	// Prime the cache with a healthy state so we don't depend on the
	// background goroutine running.
	s.readyCache.Store(&readyState{healthy: true, reason: "", checkedAt: time.Now()})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v; body=%q", err, rr.Body.String())
	}
	if body["status"] != "ok" {
		t.Errorf("body[status] = %v; want \"ok\"", body["status"])
	}
}

func TestReadyz_503WhenPGDown(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})
	checked := time.Now()
	s.readyCache.Store(&readyState{healthy: false, reason: "pg disconnected", checkedAt: checked})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v; body=%q", err, rr.Body.String())
	}
	if body["reason"] != "pg disconnected" {
		t.Errorf("reason = %v; want \"pg disconnected\"", body["reason"])
	}
	if _, ok := body["checked_at"].(string); !ok {
		t.Errorf("checked_at missing or wrong type; body=%v", body)
	}
}

func TestReadyz_503WhenAuthDown(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})
	s.readyCache.Store(&readyState{
		healthy:   false,
		reason:    "auth backend unavailable: dial tcp 10.0.0.5:443: connection refused",
		checkedAt: time.Now(),
	})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503", rr.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	reason, _ := body["reason"].(string)
	if !strings.Contains(reason, "auth backend unavailable") {
		t.Errorf("reason = %q; want contains \"auth backend unavailable\"", reason)
	}
}

func TestReadyz_503BeforeFirstProbe(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})
	// Do NOT prime the cache.

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want 503 (no probe yet)", rr.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	reason, _ := body["reason"].(string)
	if !strings.Contains(reason, "not yet probed") {
		t.Errorf("reason = %q; want contains \"not yet probed\"", reason)
	}
}

// ---------------------------------------------------------------------
// /metrics tests
// ---------------------------------------------------------------------

func TestMetrics_200ReturnsPrometheusContentType(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	mc := metrics.New()
	// Pre-touch a metric family so it's visible in the scrape output. The
	// auth.Client constructor does this in production wiring; here we touch
	// directly to keep the test self-contained.
	mc.AuthRequests("ok").Add(0)
	mc.AuthRequests("unauthorized").Add(0)
	s := newServerInternal(r, ac, mc, Config{ReadyzProbeInterval: time.Hour}, zerolog.Nop())

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") && !strings.HasPrefix(ct, "application/openmetrics-text") {
		t.Errorf("Content-Type = %q; want text/plain* or application/openmetrics-text*", ct)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "walera_auth_requests_total") {
		t.Errorf("body missing walera_auth_requests_total; got %d bytes", len(body))
	}
}

func TestMetrics_ContainsPhase3Metrics(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	mc := metrics.New()
	// Pre-touch counter-vector label values so they appear in Gather().
	mc.AuthRequests("ok").Add(0)
	mc.LimitRejected("global_concurrent").Add(0)
	s := newServerInternal(r, ac, mc, Config{ReadyzProbeInterval: time.Hour}, zerolog.Nop())

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatal(err)
	}
	bodyStr := string(body)

	want := []string{
		"walera_auth_requests_total",
		"walera_auth_request_duration_seconds",
		"walera_auth_circuit_breaker_state",
		"walera_auth_breaker_stale_subscribers",
		"walera_limit_rejected_total",
		"walera_pg_connection_status",
	}
	for _, name := range want {
		if !strings.Contains(bodyStr, name) {
			t.Errorf("body missing metric family %q", name)
		}
	}
}

// ---------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------

func TestRoutes_MountsAllThreeRoutes(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})
	s.readyCache.Store(&readyState{healthy: true, checkedAt: time.Now()})

	mux := http.NewServeMux()
	s.Routes(mux)

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		mux.ServeHTTP(rr, req)
		if rr.Code == http.StatusNotFound {
			t.Errorf("path %s: got 404 — route not mounted", path)
		}
	}
}

// ---------------------------------------------------------------------
// StartReadinessProbe tests
// ---------------------------------------------------------------------

func TestStartReadinessProbe_UpdatesCacheOverTime(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{} // CheckAuth returns nil
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartReadinessProbe(ctx)

	// Wait for the initial-shot probe + at least one tick.
	waitFor(t, 200*time.Millisecond, func() bool {
		st := s.readyCache.Load()
		return st != nil && st.healthy
	})

	st := s.readyCache.Load()
	if st == nil || !st.healthy {
		t.Fatalf("cache state = %+v; want healthy", st)
	}

	// Cancel and assert the goroutine stops calling CheckAuth.
	cancel()
	hitsAtCancel := ac.hits.Load()
	time.Sleep(50 * time.Millisecond)
	hitsAfter := ac.hits.Load()
	// Allow at most one straggler call that was already in-flight.
	if hitsAfter-hitsAtCancel > 1 {
		t.Errorf("probe goroutine still running: %d additional hits after cancel", hitsAfter-hitsAtCancel)
	}
}

func TestStartReadinessProbe_DetectsAuthFailure(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	ac.setErr(&auth.ErrUnavailable{Cause: errors.New("boom")})

	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartReadinessProbe(ctx)

	waitFor(t, 200*time.Millisecond, func() bool {
		st := s.readyCache.Load()
		return st != nil && !st.healthy
	})

	st := s.readyCache.Load()
	if st == nil {
		t.Fatal("readyCache is nil after probe interval")
	}
	if st.healthy {
		t.Errorf("healthy = true; want false (auth down)")
	}
	if !strings.Contains(st.reason, "auth") {
		t.Errorf("reason = %q; want contains \"auth\"", st.reason)
	}
}

func TestStartReadinessProbe_DetectsPGFailure(t *testing.T) {
	t.Parallel()
	r := &stubReader{} // connected=false
	ac := &stubAuthClient{}

	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartReadinessProbe(ctx)

	waitFor(t, 200*time.Millisecond, func() bool {
		st := s.readyCache.Load()
		return st != nil && !st.healthy
	})

	st := s.readyCache.Load()
	if st == nil {
		t.Fatal("readyCache is nil after probe interval")
	}
	if st.healthy {
		t.Errorf("healthy = true; want false (PG down)")
	}
	if !strings.Contains(st.reason, "pg") {
		t.Errorf("reason = %q; want contains \"pg\"", st.reason)
	}
}

// waitFor polls cond up to timeout; fails the test if timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("waitFor: condition not met within %s", timeout)
}

// ---------------------------------------------------------------------
// Gauge mirror — probe writes pg_connection_status
// ---------------------------------------------------------------------

// TestServer_ProbeMirrorsPGConnectionStatus verifies the mirror of
// reader.CheckPG() into the walera_pg_connection_status gauge. The
// probe is invoked twice with the stub reader's connected state flipped
// between calls; the gauge must read 0 then 1 from the underlying registry.
func TestServer_ProbeMirrorsPGConnectionStatus(t *testing.T) {
	t.Parallel()

	r := &stubReader{} // connected=false initially
	ac := &stubAuthClient{}
	mc := metrics.New()
	s := newServerInternal(r, ac, mc, Config{ReadyzProbeInterval: time.Hour}, zerolog.Nop())

	// Pass 1: pg disconnected → gauge==0.
	s.probe(context.Background())
	if got := gatherGaugeValue(t, mc, "walera_pg_connection_status"); got != 0 {
		t.Errorf("after probe with disconnected reader: gauge=%v; want 0", got)
	}

	// Pass 2: flip reader to connected → gauge==1.
	r.connected.Store(true)
	s.probe(context.Background())
	if got := gatherGaugeValue(t, mc, "walera_pg_connection_status"); got != 1 {
		t.Errorf("after probe with connected reader: gauge=%v; want 1", got)
	}

	// Pass 3: flip back to disconnected — verify the mirror is 0/1 toggle
	// (not just a "monotonic-up" property).
	r.connected.Store(false)
	s.probe(context.Background())
	if got := gatherGaugeValue(t, mc, "walera_pg_connection_status"); got != 0 {
		t.Errorf("after re-disconnection: gauge=%v; want 0", got)
	}
}

// gatherGaugeValue scans the registry for the named gauge family and returns
// its current value. Returns 0 if absent.
func gatherGaugeValue(t *testing.T, m *metrics.Registry, name string) float64 {
	t.Helper()
	mfs, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, mt := range mf.GetMetric() {
			if g := mt.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	return 0
}

// ---------------------------------------------------------------------
// CORS / Timing-Allow-Origin
// ---------------------------------------------------------------------

// TestHealthz_CORS_TimingAllowOriginReflectedOnMatch asserts that a
// browser-origin GET /healthz with an allowlisted Origin receives both
// Access-Control-Allow-Origin AND Timing-Allow-Origin reflecting the request
// Origin. TAO lets the h2c probe read
// PerformanceResourceTiming.nextHopProtocol cross-origin.
func TestHealthz_CORS_TimingAllowOriginReflectedOnMatch(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	s := newTestServer(t, r, &stubAuthClient{}, Config{ReadyzProbeInterval: time.Hour})
	s.SetCORSOrigins([]string{"http://localhost:8081"})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("ACAO = %q, want %q", got, "http://localhost:8081")
	}
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("ACA-Credentials = %q, want true", got)
	}
	if got := rr.Header().Get("Timing-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("Timing-Allow-Origin = %q, want %q", got, "http://localhost:8081")
	}
	if got := rr.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want Origin", got)
	}
}

func TestHealthz_CORS_NoHeadersForUnknownOrigin(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	s := newTestServer(t, r, &stubAuthClient{}, Config{ReadyzProbeInterval: time.Hour})
	s.SetCORSOrigins([]string{"http://localhost:8081"})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://evil.example")
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty (origin denied)", got)
	}
	if got := rr.Header().Get("Timing-Allow-Origin"); got != "" {
		t.Errorf("Timing-Allow-Origin = %q, want empty (origin denied)", got)
	}
	if got := rr.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want Origin (CORS configured → Vary discipline)", got)
	}
}

func TestMetrics_CORS_TimingAllowOriginReflectedOnMatch(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	s := newTestServer(t, r, &stubAuthClient{}, Config{ReadyzProbeInterval: time.Hour})
	s.SetCORSOrigins([]string{"http://localhost:8081"})

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("ACAO = %q, want %q", got, "http://localhost:8081")
	}
	if got := rr.Header().Get("Timing-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("Timing-Allow-Origin = %q, want %q", got, "http://localhost:8081")
	}
}

func TestHealthz_CORS_DisabledByDefault(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	s := newTestServer(t, r, &stubAuthClient{}, Config{ReadyzProbeInterval: time.Hour})
	// Note: no SetCORSOrigins → empty allowlist → CORS disabled.

	mux := http.NewServeMux()
	s.Routes(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty (CORS disabled by default)", got)
	}
	if got := rr.Header().Get("Vary"); got != "" {
		t.Errorf("Vary = %q, want empty (CORS disabled → no Vary discipline)", got)
	}
}
