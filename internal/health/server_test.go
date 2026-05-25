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

type stubReader struct {
	connected atomic.Bool
}

func (s *stubReader) CheckPG(_ context.Context) error {
	if s.connected.Load() {
		return nil
	}
	return errors.New("pg disconnected")
}

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

func newTestServer(t *testing.T, reader PgChecker, ac AuthChecker, cfg Config) *Server {
	t.Helper()
	return newServerInternal(reader, ac, metrics.New(), cfg, zerolog.Nop())
}

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
	r := &stubReader{}
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

func TestHealthz_NeverCallsAuthBackend(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}

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

func TestReadyz_200WhenBothHealthy(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: time.Hour})

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

func TestMetrics_200ReturnsPrometheusContentType(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	mc := metrics.New()

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

func TestStartReadinessProbe_UpdatesCacheOverTime(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	ac := &stubAuthClient{}
	s := newTestServer(t, r, ac, Config{ReadyzProbeInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartReadinessProbe(ctx)

	waitFor(t, 200*time.Millisecond, func() bool {
		st := s.readyCache.Load()
		return st != nil && st.healthy
	})

	st := s.readyCache.Load()
	if st == nil || !st.healthy {
		t.Fatalf("cache state = %+v; want healthy", st)
	}

	cancel()
	hitsAtCancel := ac.hits.Load()
	time.Sleep(50 * time.Millisecond)
	hitsAfter := ac.hits.Load()

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
	r := &stubReader{}
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

func TestServer_ProbeMirrorsPGConnectionStatus(t *testing.T) {
	t.Parallel()

	r := &stubReader{}
	ac := &stubAuthClient{}
	mc := metrics.New()
	s := newServerInternal(r, ac, mc, Config{ReadyzProbeInterval: time.Hour}, zerolog.Nop())

	s.probe(context.Background())
	if got := gatherGaugeValue(t, mc, "walera_pg_connection_status"); got != 0 {
		t.Errorf("after probe with disconnected reader: gauge=%v; want 0", got)
	}

	r.connected.Store(true)
	s.probe(context.Background())
	if got := gatherGaugeValue(t, mc, "walera_pg_connection_status"); got != 1 {
		t.Errorf("after probe with connected reader: gauge=%v; want 1", got)
	}

	r.connected.Store(false)
	s.probe(context.Background())
	if got := gatherGaugeValue(t, mc, "walera_pg_connection_status"); got != 0 {
		t.Errorf("after re-disconnection: gauge=%v; want 0", got)
	}
}

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
