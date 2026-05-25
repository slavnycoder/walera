// Package writer — server_test.go covers the /healthz, /metrics, /control
// HTTP routes. Tests use httptest.NewRecorder or ServeHTTP directly against
// the *http.Server's Handler so no listener is bound.
package writer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

// helperDeps builds a ServerDeps for tests. Initial scenario is "smoke" at
// 5 tx/s, 1 row/tx, targets={orders,devices,articles}.
func helperDeps(t *testing.T) ServerDeps {
	t.Helper()
	reg := NewRegistry()
	reg.SetActiveScenario("smoke")
	reg.SetCommitRate("smoke", 5)

	lim := rate.NewLimiter(rate.Limit(5), 1)

	var ptr atomic.Pointer[scenarioState]
	ptr.Store(NewScenarioState(NewSmokeScenario(5, 1), time.Now(), 5, 1, []string{"orders", "devices", "articles"}))

	return ServerDeps{
		Limiter:     lim,
		ScenarioPtr: &ptr,
		Registry:    reg,
		Logger:      zerolog.Nop(),
		Targets:     []string{"orders", "devices", "articles"},
	}
}

// helperServe builds an *http.Server and serves the supplied request
// against its handler in-process. Returns the response.
func helperServe(t *testing.T, srv *http.Server, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	return rec
}

func TestHealthz_OK(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got healthzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body.String())
	}
	if got.Status != "ok" {
		t.Errorf("status = %q, want ok", got.Status)
	}
	if got.UptimeSeconds < 0 {
		t.Errorf("uptime_seconds = %v, want >=0", got.UptimeSeconds)
	}
	if got.Scenario != "smoke" {
		t.Errorf("scenario = %q, want smoke", got.Scenario)
	}
}

func TestMetrics_ReturnsPromText(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q, want text/plain*", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "writer_errors_total") {
		t.Errorf("body missing writer_errors_total; first 500=%s", body[:min(500, len(body))])
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Errorf("body missing go_goroutines")
	}
}

func TestControl_FullUpdate(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{
		CommitRate: floatPtr(50),
		RowsPerTx:  intPtr(2),
		Scenario:   strPtr("steady"),
	})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got controlResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.CommitRate != 50 || got.RowsPerTx != 2 || got.Scenario != "steady" {
		t.Errorf("response = %+v, want {50,2,steady}", got)
	}
	if got, want := float64(deps.Limiter.Limit()), 50.0; got != want {
		t.Errorf("limiter = %v, want %v", got, want)
	}
	st := deps.ScenarioPtr.Load()
	if st.Scenario.Name() != "steady" {
		t.Errorf("scenario name = %q, want steady", st.Scenario.Name())
	}
	if st.RowsPerTx != 2 {
		t.Errorf("rows_per_tx = %d, want 2", st.RowsPerTx)
	}
	if v, ok := metricValueByLabels(t, deps.Registry, "writer_scenario",
		map[string]string{"scenario": "steady"}); !ok || v != 1 {
		t.Errorf("writer_scenario{steady} = %v (ok=%v), want 1", v, ok)
	}
}

func TestControl_PartialUpdate_OnlyCommitRate(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(25)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got controlResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.CommitRate != 25 || got.RowsPerTx != 1 || got.Scenario != "smoke" {
		t.Errorf("response = %+v, want {25,1,smoke}", got)
	}
	if float64(deps.Limiter.Limit()) != 25 {
		t.Errorf("limiter = %v, want 25", float64(deps.Limiter.Limit()))
	}
	st := deps.ScenarioPtr.Load()
	if st.Scenario.Name() != "smoke" {
		t.Errorf("scenario changed unexpectedly to %q", st.Scenario.Name())
	}
	if st.RowsPerTx != 1 {
		t.Errorf("rows_per_tx changed unexpectedly to %d", st.RowsPerTx)
	}
}

func TestControl_PartialUpdate_OnlyScenario(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{Scenario: strPtr("spike")})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	st := deps.ScenarioPtr.Load()
	if st.Scenario.Name() != "spike" {
		t.Errorf("scenario = %q, want spike", st.Scenario.Name())
	}
	// Rate and rows preserved.
	if float64(deps.Limiter.Limit()) != 5 {
		t.Errorf("limiter changed to %v, want 5", float64(deps.Limiter.Limit()))
	}
	if st.RowsPerTx != 1 {
		t.Errorf("rows_per_tx changed to %d, want 1", st.RowsPerTx)
	}
}

func TestControl_InvalidScenario(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	priorScenario := deps.ScenarioPtr.Load().Scenario.Name()
	priorRate := float64(deps.Limiter.Limit())

	body, _ := json.Marshal(controlRequest{Scenario: strPtr("unknown")})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if got := deps.ScenarioPtr.Load().Scenario.Name(); got != priorScenario {
		t.Errorf("scenario mutated to %q, want unchanged %q", got, priorScenario)
	}
	if float64(deps.Limiter.Limit()) != priorRate {
		t.Errorf("limiter mutated to %v, want unchanged %v", float64(deps.Limiter.Limit()), priorRate)
	}
}

func TestControl_InvalidJSON(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodPost, "/control", strings.NewReader("not-json{{{"))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestControl_NegativeRate(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(-1)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestControl_ZeroRowsPerTx(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{RowsPerTx: intPtr(0)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestControl_MethodNotAllowed(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodGet, "/control", nil)
	rec := helperServe(t, srv, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

// TestControl_EmptyJSON exercises the partial-update no-op path: a POST
// with `{}` is a valid request that mutates nothing and returns 200 with
// the current effective config.
func TestControl_EmptyJSON(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodPost, "/control", strings.NewReader("{}"))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got controlResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.CommitRate != 5 || got.RowsPerTx != 1 || got.Scenario != "smoke" {
		t.Errorf("response = %+v, want {5,1,smoke}", got)
	}
}

// TestControl_BodyTooLarge exercises the MaxBytesReader guard (T-07-06).
// A body larger than 1KB returns 400 (the JSON decoder surfaces the
// MaxBytesReader error which we translate to invalid-json).
func TestControl_BodyTooLarge(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	// 2KB padding inside a string field — exceeds the 1KB cap.
	pad := strings.Repeat("x", 2048)
	body := `{"scenario":"` + pad + `"}`
	req := httptest.NewRequest(http.MethodPost, "/control", strings.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for oversized body; got body=%s",
			rec.Code, truncate(rec.Body.String(), 200))
	}
}

// TestControl_CR01_RateOverrideSurvivesScenarioTick is the regression test
// for the rate-override-survives-tick contract. The previous /control
// handler built the new Scenario via Registry()[name], whose hard-coded
// baseline rate (steady=100) then got re-asserted by the commit-loop
// scenario evaluator goroutine ~100ms later, silently overwriting the
// operator's commit_rate=250. The fix is to route scenario construction
// through BuildScenario so Tick() returns the operator's rate.
//
// Test method: POST /control {scenario:"steady", commit_rate:250}, then
// directly invoke the scenario's Tick() with a synthetic elapsed of 250ms
// (the same call the cmd/writer evaluator goroutine makes on its 100ms
// ticker). Assert Tick() returns 250 — proving the operator's rate is
// baked into the scenario rather than discarded.
//
// We test Tick() directly rather than spinning up the full evaluator
// goroutine so the assertion is deterministic (no sleep, no flake).
func TestControl_CR01_RateOverrideSurvivesScenarioTick(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{
		Scenario:   strPtr("steady"),
		CommitRate: floatPtr(250),
	})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	st := deps.ScenarioPtr.Load()
	if st == nil {
		t.Fatal("scenario state is nil after /control")
	}
	if st.Scenario.Name() != "steady" {
		t.Errorf("scenario name = %q, want steady", st.Scenario.Name())
	}

	// The key assertion: Tick() returns the operator's rate, not the
	// Registry baseline (100).
	gotRate, _ := st.Scenario.Tick(250 * time.Millisecond)
	if gotRate != 250 {
		t.Errorf("Scenario.Tick() returned commit_rate=%v, want 250 (rate-override regression: scenario was built from Registry baseline)", gotRate)
	}

	// Limiter is also set; cmd/writer's evaluator goroutine would compare
	// rate.Limit(gotRate) to lim.Limit(). They must match so the goroutine
	// doesn't try to "fix" the limiter.
	if float64(deps.Limiter.Limit()) != 250 {
		t.Errorf("limiter = %v, want 250", float64(deps.Limiter.Limit()))
	}
}

// TestControl_CR01_RateOverride_OnExistingScenario covers the partial-update
// case: POST {commit_rate: 250} (no scenario) on an existing scenario must
// also produce a scenario whose Tick() returns 250 — otherwise the next
// evaluator tick would re-assert the OLD scenario's baseline rate.
func TestControl_CR01_RateOverride_OnExistingScenario(t *testing.T) {
	deps := helperDeps(t)
	// Pre-seed to steady@100 so the prior scenario is a steady (Registry
	// baseline rate = 100); without the fix, a partial /control commit_rate
	// update would also be vulnerable to re-assertion on the next tick.
	deps.ScenarioPtr.Store(NewScenarioState(
		BuildScenario("steady", 100, 1, 0),
		time.Now(), 100, 1,
		[]string{"orders", "devices", "articles"},
	))
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(250)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	st := deps.ScenarioPtr.Load()
	gotRate, _ := st.Scenario.Tick(250 * time.Millisecond)
	if gotRate != 250 {
		t.Errorf("Scenario.Tick() returned commit_rate=%v, want 250", gotRate)
	}
}

// TestControl_WR01_StartedAtPreservedOnPartialUpdate ensures a partial
// /control update (rate/rows only, no scenario field) does NOT reset
// scenarioState.StartedAt. The ramp-up scenario depends on
// elapsed = time.Since(StartedAt); resetting it would restart the ramp at
// 0%, surprising operators who just bumped commit_rate on a mid-ramp loop.
func TestControl_WR01_StartedAtPreservedOnPartialUpdate(t *testing.T) {
	deps := helperDeps(t)
	// Pre-seed a ramp-up scenario with a StartedAt 2s in the past.
	rampStart := time.Now().Add(-2 * time.Second)
	deps.ScenarioPtr.Store(NewScenarioState(
		BuildScenario("ramp-up", 100, 1, 10*time.Second),
		rampStart, 100, 1,
		[]string{"orders", "devices", "articles"},
	))
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(150)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	st := deps.ScenarioPtr.Load()
	if !st.StartedAt.Equal(rampStart) {
		t.Errorf("StartedAt = %v, want preserved %v (WR-01 regression: partial update reset ramp progress)",
			st.StartedAt, rampStart)
	}
}

// TestControl_WR01_StartedAtResetsOnScenarioSwitch confirms the inverse:
// when the request DOES change scenario, StartedAt resets to now() so the
// new scenario starts fresh (ramp-up resumes from 0%, spike enters its
// burst window at offset 0).
func TestControl_WR01_StartedAtResetsOnScenarioSwitch(t *testing.T) {
	deps := helperDeps(t)
	// Pre-seed with a StartedAt 5s in the past.
	oldStart := time.Now().Add(-5 * time.Second)
	deps.ScenarioPtr.Store(NewScenarioState(
		BuildScenario("smoke", 5, 1, 0),
		oldStart, 5, 1,
		[]string{"orders", "devices", "articles"},
	))
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	before := time.Now()
	body, _ := json.Marshal(controlRequest{Scenario: strPtr("steady"), CommitRate: floatPtr(50)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	rec := helperServe(t, srv, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	after := time.Now()

	st := deps.ScenarioPtr.Load()
	if st.StartedAt.Before(before) || st.StartedAt.After(after) {
		t.Errorf("StartedAt = %v, want freshly set between %v and %v on scenario switch",
			st.StartedAt, before, after)
	}
}

// ── writer CORS tests ──────────────────────────────────────────────────
//
// The /control endpoint gains a withCORS middleware + an OPTIONS preflight
// responder. Behaviour mirrors internal/sse/cors.go EXACTLY:
//   - empty allowlist → no ACA-* headers, no Vary (CORS disabled)
//   - non-empty allowlist + matching Origin → ACAO + ACA-Credentials +
//     Vary: Origin reflected
//   - non-empty allowlist + non-matching Origin → Vary: Origin only,
//     request still served (browser blocks the JS read)
//   - OPTIONS /control always returns 204; ACAM/ACAH/Max-Age set only on
//     matching Origin

// depsWithCORS builds a ServerDeps with the supplied allowlist.
func depsWithCORS(t *testing.T, allowed []string) ServerDeps {
	t.Helper()
	d := helperDeps(t)
	d.CORSOrigins = allowed
	return d
}

func TestControl_PreflightReturns204WithReflectedOrigin(t *testing.T) {
	deps := depsWithCORS(t, []string{"http://localhost:8081"})
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodOptions, "/control", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "content-type")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	hdr := rec.Header()
	if got := hdr.Get("Access-Control-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("ACAO = %q, want %q", got, "http://localhost:8081")
	}
	if got := hdr.Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("ACA-Credentials = %q, want %q", got, "true")
	}
	if got := hdr.Get("Access-Control-Allow-Methods"); got != "POST, OPTIONS" {
		t.Errorf("ACA-Methods = %q, want %q", got, "POST, OPTIONS")
	}
	if got := hdr.Get("Access-Control-Allow-Headers"); got != "Content-Type" {
		t.Errorf("ACA-Headers = %q, want %q", got, "Content-Type")
	}
	if got := hdr.Get("Access-Control-Max-Age"); got != "86400" {
		t.Errorf("ACA-Max-Age = %q, want %q", got, "86400")
	}
	if got := hdr.Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want %q", got, "Origin")
	}
}

func TestControl_PreflightDeniesUnknownOrigin(t *testing.T) {
	deps := depsWithCORS(t, []string{"http://localhost:8081"})
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodOptions, "/control", nil)
	req.Header.Set("Origin", "http://evil.example")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	hdr := rec.Header()
	if got := hdr.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty (origin denied)", got)
	}
	// Preflight on a denied origin omits the method/header advertisement
	// — the absent ACAO is the gate, the absent ACAM is correctness.
	if got := hdr.Get("Access-Control-Allow-Methods"); got != "" {
		t.Errorf("ACA-Methods = %q, want empty for denied origin", got)
	}
	if got := hdr.Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want %q (Vary set whenever CORS is configured)", got, "Origin")
	}
}

func TestControl_POSTReflectsAllowedOrigin(t *testing.T) {
	deps := depsWithCORS(t, []string{"http://localhost:8081"})
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(25)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8081")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("ACAO = %q, want %q", got, "http://localhost:8081")
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("ACA-Credentials = %q, want %q", got, "true")
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want %q", got, "Origin")
	}
	// Body still parsed & limiter still mutated — the middleware does not
	// short-circuit on CORS denial; it just omits the headers.
	if float64(deps.Limiter.Limit()) != 25 {
		t.Errorf("limiter = %v, want 25 (POST processed regardless of CORS)", float64(deps.Limiter.Limit()))
	}
}

func TestControl_POSTOmitsCORSForUnknownOrigin(t *testing.T) {
	deps := depsWithCORS(t, []string{"http://localhost:8081"})
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(33)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://evil.example")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty (origin denied)", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want %q", got, "Origin")
	}
	// Request body still applied — browser blocks the JS read, but the
	// server's job is just to handle the request as normal.
	if float64(deps.Limiter.Limit()) != 33 {
		t.Errorf("limiter = %v, want 33", float64(deps.Limiter.Limit()))
	}
}

func TestControl_NoCORSWhenAllowlistEmpty(t *testing.T) {
	deps := depsWithCORS(t, nil)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	// POST with an Origin header but empty allowlist — must emit zero
	// CORS headers (preserves pre-08-04 behaviour byte-for-byte).
	body, _ := json.Marshal(controlRequest{CommitRate: floatPtr(7)})
	req := httptest.NewRequest(http.MethodPost, "/control", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8081")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty (CORS disabled)", got)
	}
	if got := rec.Header().Get("Vary"); got != "" {
		t.Errorf("Vary = %q, want empty (CORS disabled — no Vary discipline either)", got)
	}

	// OPTIONS preflight with empty allowlist: still returns 204 (the
	// route exists thanks to OPTIONS /control registration) but emits
	// no ACA-* headers.
	preReq := httptest.NewRequest(http.MethodOptions, "/control", nil)
	preReq.Header.Set("Origin", "http://localhost:8081")
	preRec := helperServe(t, srv, preReq)
	if preRec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", preRec.Code)
	}
	if got := preRec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("preflight ACAO = %q, want empty (CORS disabled)", got)
	}
}

// TestControl_MethodNotAllowedStillReturnsAfterMiddleware confirms the
// withCORS wrapper does not break stdlib method-routing: a GET to
// /control still returns 405 even with an allowed Origin header set.
func TestControl_MethodNotAllowedStillReturnsAfterMiddleware(t *testing.T) {
	deps := depsWithCORS(t, []string{"http://localhost:8081"})
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	req := httptest.NewRequest(http.MethodGet, "/control", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

// floatPtr / intPtr / strPtr — JSON-pointer helpers for the optional fields.
func floatPtr(v float64) *float64 { return &v }
func intPtr(v int) *int           { return &v }
func strPtr(v string) *string     { return &v }

// truncate returns at most n characters of s.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
