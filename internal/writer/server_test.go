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

func TestControl_BodyTooLarge(t *testing.T) {
	deps := helperDeps(t)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

	pad := strings.Repeat("x", 2048)
	body := `{"scenario":"` + pad + `"}`
	req := httptest.NewRequest(http.MethodPost, "/control", strings.NewReader(body))
	rec := helperServe(t, srv, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for oversized body; got body=%s",
			rec.Code, truncate(rec.Body.String(), 200))
	}
}

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

	gotRate, _ := st.Scenario.Tick(250 * time.Millisecond)
	if gotRate != 250 {
		t.Errorf("Scenario.Tick() returned commit_rate=%v, want 250 (rate-override regression: scenario was built from Registry baseline)", gotRate)
	}

	if float64(deps.Limiter.Limit()) != 250 {
		t.Errorf("limiter = %v, want 250", float64(deps.Limiter.Limit()))
	}
}

func TestControl_CR01_RateOverride_OnExistingScenario(t *testing.T) {
	deps := helperDeps(t)

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

func TestControl_WR01_StartedAtPreservedOnPartialUpdate(t *testing.T) {
	deps := helperDeps(t)

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

func TestControl_WR01_StartedAtResetsOnScenarioSwitch(t *testing.T) {
	deps := helperDeps(t)

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

	if float64(deps.Limiter.Limit()) != 33 {
		t.Errorf("limiter = %v, want 33", float64(deps.Limiter.Limit()))
	}
}

func TestControl_NoCORSWhenAllowlistEmpty(t *testing.T) {
	deps := depsWithCORS(t, nil)
	srv := NewServer(ServerConfig{Addr: ":0"}, deps)

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

func floatPtr(v float64) *float64 { return &v }
func intPtr(v int) *int           { return &v }
func strPtr(v string) *string     { return &v }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
