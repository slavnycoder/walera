package writer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

const controlBodyMaxBytes = 1024

const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 5 * time.Second
	httpWriteTimeout      = 5 * time.Second
	httpIdleTimeout       = 60 * time.Second
)

type ServerDeps struct {
	Limiter      *rate.Limiter
	ScenarioPtr  *atomic.Pointer[scenarioState]
	Registry     *WriterRegistry
	Logger       zerolog.Logger
	Targets      []string
	RampDuration time.Duration
	CORSOrigins  []string
}

type controlRequest struct {
	CommitRate *float64 `json:"commit_rate,omitempty"`
	RowsPerTx  *int     `json:"rows_per_tx,omitempty"`
	Scenario   *string  `json:"scenario,omitempty"`
}

type controlResponse struct {
	CommitRate float64 `json:"commit_rate"`
	RowsPerTx  int     `json:"rows_per_tx"`
	Scenario   string  `json:"scenario"`
}

type healthzResponse struct {
	Status        string  `json:"status"`
	UptimeSeconds float64 `json:"uptime_seconds"`
	Scenario      string  `json:"scenario"`
}

type ServerConfig struct {
	Addr string
}

func validateServerDeps(d ServerDeps) {
	if d.Limiter == nil {
		panic("writer.NewServer: Deps.Limiter is required")
	}
	if d.ScenarioPtr == nil {
		panic("writer.NewServer: Deps.ScenarioPtr is required")
	}
	if d.Registry == nil {
		panic("writer.NewServer: Deps.Registry is required")
	}
}

func NewServer(cfg ServerConfig, deps ServerDeps) *http.Server {
	validateServerDeps(deps)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthzHandler(deps))
	mux.Handle("GET /metrics", promhttp.HandlerFor(
		deps.Registry.Gatherer(),
		promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError},
	))
	mux.HandleFunc("POST /control", withCORS(controlHandler(deps), deps.CORSOrigins))
	mux.HandleFunc("OPTIONS /control", preflightHandler(deps.CORSOrigins))

	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

func healthzHandler(deps ServerDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		st := deps.ScenarioPtr.Load()
		scenarioName := ""
		if st != nil {
			scenarioName = st.Scenario.Name()
		}
		body := healthzResponse{
			Status:        "ok",
			UptimeSeconds: deps.Registry.Uptime().Seconds(),
			Scenario:      scenarioName,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(body)
	}
}

func decodeControlRequest(w http.ResponseWriter, r *http.Request) (controlRequest, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, controlBodyMaxBytes)

	var req controlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %s", err.Error()))
		return req, false
	}
	if req.CommitRate != nil && *req.CommitRate <= 0 {
		writeError(w, http.StatusBadRequest, "commit_rate must be > 0")
		return req, false
	}
	if req.RowsPerTx != nil && *req.RowsPerTx < 1 {
		writeError(w, http.StatusBadRequest, "rows_per_tx must be >= 1")
		return req, false
	}
	if req.Scenario != nil && !isKnownScenario(*req.Scenario) {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown scenario: %s", *req.Scenario))
		return req, false
	}
	return req, true
}

type controlMerge struct {
	prev         *scenarioState
	prevName     string
	newName      string
	newRate      float64
	newRowsPerTx int
}

func resolveControlMutation(deps ServerDeps, w http.ResponseWriter, req controlRequest) (controlMerge, bool) {
	prev := deps.ScenarioPtr.Load()
	m := controlMerge{prev: prev}
	if prev != nil {
		m.prevName = prev.Scenario.Name()
		m.newRate = prev.CommitRate
		m.newRowsPerTx = prev.RowsPerTx
	}
	m.newName = m.prevName
	if req.Scenario != nil {
		m.newName = *req.Scenario
	}
	if req.CommitRate != nil {
		m.newRate = *req.CommitRate
	}
	if req.RowsPerTx != nil {
		m.newRowsPerTx = *req.RowsPerTx
	}
	if m.newName == "" {
		writeError(w, http.StatusBadRequest, "no active scenario; supply scenario field")
		return m, false
	}
	return m, true
}

func applyControlMutation(deps ServerDeps, w http.ResponseWriter, req controlRequest, m controlMerge) bool {
	activeScenario := BuildScenario(m.newName, m.newRate, m.newRowsPerTx, deps.RampDuration)
	if activeScenario == nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown scenario: %s", m.newName))
		return false
	}

	startedAt := time.Now()
	if req.Scenario == nil && m.prev != nil {
		startedAt = m.prev.StartedAt
	}

	next := NewScenarioState(activeScenario, startedAt, m.newRate, m.newRowsPerTx, deps.Targets)
	deps.ScenarioPtr.Store(next)
	deps.Limiter.SetLimit(rate.Limit(m.newRate))

	if req.Scenario != nil && m.newName != m.prevName {
		deps.Registry.SetActiveScenario(m.newName)
		deps.Logger.Info().
			Str("from", m.prevName).
			Str("to", m.newName).
			Msg("scenario changed")
	}
	deps.Registry.SetCommitRate(m.newName, m.newRate)
	return true
}

func respondControl(w http.ResponseWriter, m controlMerge) {
	resp := controlResponse{
		CommitRate: m.newRate,
		RowsPerTx:  m.newRowsPerTx,
		Scenario:   m.newName,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func controlHandler(deps ServerDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeControlRequest(w, r)
		if !ok {
			return
		}
		m, ok := resolveControlMutation(deps, w, req)
		if !ok {
			return
		}
		if !applyControlMutation(deps, w, req, m) {
			return
		}
		respondControl(w, m)
	}
}

func applyCORSHeaders(w http.ResponseWriter, r *http.Request, allowed []string) bool {
	if len(allowed) == 0 {
		return false
	}
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	for _, a := range allowed {
		if a == origin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			return true
		}
	}
	return false
}

func withCORS(h http.HandlerFunc, allowed []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		applyCORSHeaders(w, r, allowed)
		h(w, r)
	}
}

func preflightHandler(allowed []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matched := applyCORSHeaders(w, r, allowed)
		if matched {
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
