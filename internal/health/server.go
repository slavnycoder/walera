// Package health — server.go implements the operational HTTP routes
// (/healthz, /readyz, /metrics).
//
// The Server type holds references to the three external surfaces it observes
// behind consumer-owned interfaces:
//   - PgChecker (for /healthz liveness + /readyz prober).
//   - AuthChecker (for the /readyz auth-reachability prober).
//   - *metrics.Registry (for Gatherer — backs /metrics via promhttp).
//
// Constructed in cmd/cdc-sse/main.go AFTER the PgChecker / AuthChecker
// implementations exist (production: *wal.Reader and *auth.Client). The
// Routes(mux) call mounts the three operational routes BEFORE the SSE
// handler routes so an /healthz preflight cannot collide with SSE patterns.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// PgChecker is the consumer-owned interface health uses to probe the
// primary PostgreSQL replication connection. Production wires *wal.Reader
// (which crosses the PostgreSQL network boundary); server_test.go uses
// stubReader to drive the readyz state machine without a real PG. The
// contract: return nil when the replication connection is open, a non-nil
// error otherwise. IFACE-01 (b)+(c).
type PgChecker interface {
	CheckPG(ctx context.Context) error
}

// AuthChecker is the consumer-owned interface health uses to probe the
// auth backend's reachability from the background /readyz prober.
// Production wires *auth.Client (which crosses the HTTP boundary to the
// auth backend); server_test.go uses stubAuthClient to drive prober and
// CORS scenarios without a real backend. The contract: return nil when
// the backend ANSWERED (including non-2xx HTTP), a non-nil error only on
// transport-level unavailability. IFACE-01 (b)+(c).
type AuthChecker interface {
	CheckAuth(ctx context.Context) error
}

// Server hosts the three operational HTTP routes. The readyCache pointer is
// populated by the background prober spawned via StartReadinessProbe; HTTP
// handlers are a sub-millisecond atomic.Pointer load on the hot path.
type Server struct {
	reader     PgChecker
	authClient AuthChecker
	metricsReg *metrics.Registry
	cfg        Config
	readyCache atomic.Pointer[readyState]
	log        zerolog.Logger
	// corsOrigins is the optional allowlist applied to the three
	// operational routes. Wired via the SetCORSOrigins setter from
	// cmd/cdc-sse/main.go so the existing constructor signature stays
	// stable. Empty (default) → no CORS headers (the SSE routes already
	// have their own CORS via handleCORS).
	corsOrigins []string
}

// SetCORSOrigins configures the cross-origin allowlist for /healthz,
// /readyz, /metrics. Must be called BEFORE Routes(mux); a no-op after
// routes are mounted (the wrapper closure captures the slice value).
// Empty / nil disables CORS handling entirely.
func (s *Server) SetCORSOrigins(allowed []string) {
	s.corsOrigins = allowed
}

// Deps bundles the collaborators health.New requires at construction time.
// Required fields panic on nil; Logger is the value-type exception
// (zerolog.Logger zero value is a usable Nop logger).
type Deps struct {
	// Logger is the structured logger; zero value is a usable Nop logger so
	// this field has no nil-check.
	Logger zerolog.Logger
	// PgChecker probes the primary PG replication connection. Required —
	// /healthz dereferences this on every request. Production passes
	// *wal.Reader.
	PgChecker PgChecker
	// AuthChecker probes the auth backend's reachability. Required — the
	// background /readyz prober dereferences this. Production passes
	// *auth.Client.
	AuthChecker AuthChecker
	// Metrics is the typed Prometheus registry. Required — /metrics is
	// served by promhttp.HandlerFor over this registry's Gatherer().
	Metrics *metrics.Registry
}

// validateDeps panics with the canonical message format when any required
// Deps field is nil. Logger is exempt (value-type with usable zero value).
func validateDeps(d Deps) {
	if d.PgChecker == nil {
		panic("health.New: Deps.PgChecker is required")
	}
	if d.AuthChecker == nil {
		panic("health.New: Deps.AuthChecker is required")
	}
	if d.Metrics == nil {
		panic("health.New: Deps.Metrics is required")
	}
}

// New constructs a Server. The interfaces are consumer-owned; any
// implementation satisfying PgChecker / AuthChecker can be wired in.
// Production passes *wal.Reader and *auth.Client (which satisfy the
// interfaces implicitly via CheckPG / CheckAuth).
//
// Construct in cmd/cdc-sse/main.go AFTER the concrete implementations
// and metrics.Registry exist. Call StartReadinessProbe(ctx) before
// mounting Routes(mux) so /readyz has a primed cache by the time k8s
// scrapes start.
func New(cfg Config, deps Deps) *Server {
	validateDeps(deps)
	return newServerInternal(deps.PgChecker, deps.AuthChecker, deps.Metrics, cfg, deps.Logger)
}

// newServerInternal is the constructor body shared between the exported
// New and the unit tests (which pass stubs). Kept unexported so callers
// outside the package go through New. The unit-test fast path skips the
// Deps nil-check because stubs are constructed by the test fixture.
func newServerInternal(reader PgChecker, authClient AuthChecker, metricsReg *metrics.Registry, cfg Config, log zerolog.Logger) *Server {
	return &Server{
		reader:     reader,
		authClient: authClient,
		metricsReg: metricsReg,
		cfg:        cfg,
		log:        log,
	}
}

// Metrics returns the registry this Server publishes counters into. Exposed
// so the composition-root singleton-identity test can compare the pointer
// every consumer received against the registry the composition root built.
func (s *Server) Metrics() *metrics.Registry { return s.metricsReg }

// Routes registers the three operational routes on the supplied mux.
// Call BEFORE sseHandler.Routes(mux) — see cmd/cdc-sse/main.go.
//
// The /metrics handler wraps the PRIVATE metrics.Registry gatherer; the
// global prometheus.DefaultGatherer is NOT exposed.
func (s *Server) Routes(mux *http.ServeMux) {
	metricsHandler := promhttp.HandlerFor(
		s.metricsReg.Gatherer(),
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
			ErrorHandling:     promhttp.ContinueOnError,
			// Registry: nil — we deliberately do not instrument the /metrics
			// handler itself (avoiding recursive observability).
		},
	)

	mux.HandleFunc("GET /healthz", s.withCORS(s.serveHealthz))
	mux.HandleFunc("GET /readyz", s.withCORS(s.serveReadyz))
	mux.HandleFunc("GET /metrics", s.withCORS(metricsHandler.ServeHTTP))
}

// withCORS wraps an operational-route handler with the same CORS reflection
// semantics as internal/sse/cors.go's handleCORS — including the
// Timing-Allow-Origin emission that lets the browser h2c probe read
// PerformanceResourceTiming.nextHopProtocol cross-origin. When corsOrigins
// is empty the wrapper is a pass-through (no headers added, no Vary
// discipline) so the operational routes are CORS-free in production where
// CORS for /metrics is irrelevant.
func (s *Server) withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.corsOrigins) > 0 {
			w.Header().Add("Vary", "Origin")
			if origin := r.Header.Get("Origin"); origin != "" {
				for _, a := range s.corsOrigins {
					if a == origin {
						w.Header().Set("Access-Control-Allow-Origin", origin)
						w.Header().Set("Access-Control-Allow-Credentials", "true")
						// TAO lets the h2c probe read nextHopProtocol on
						// /healthz cross-origin.
						w.Header().Set("Timing-Allow-Origin", origin)
						break
					}
				}
			}
		}
		h(w, r)
	}
}

// serveHealthz implements /healthz.
//
// CRITICAL INVARIANT: this handler NEVER calls the auth backend. An auth
// outage must NOT trigger k8s liveness failure and pod restart — that
// defeats the two-track health/readiness policy. The unit test
// TestHealthz_NeverCallsAuthBackend enforces this.
func (s *Server) serveHealthz(w http.ResponseWriter, r *http.Request) {
	if err := s.reader.CheckPG(r.Context()); err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("pg disconnected"))
}

// serveReadyz implements /readyz.
//
// Reads the cached readyState pointer populated by the background prober
// (StartReadinessProbe). If the cache is empty (no probe yet), returns 503
// with a "not yet probed" reason — safer default than 200 before the first
// probe completes.
func (s *Server) serveReadyz(w http.ResponseWriter, _ *http.Request) {
	state := s.readyCache.Load()
	if state == nil {
		state = &readyState{healthy: false, reason: "not yet probed", checkedAt: time.Time{}}
	}

	w.Header().Set("Content-Type", "application/json")
	if state.healthy {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
	// json.Marshal on a small map[string]any cannot fail in practice; the
	// errcheck-friendly pattern is to ignore the (impossible) error.
	body, _ := json.Marshal(map[string]any{
		"reason":     state.reason,
		"checked_at": state.checkedAt.UTC().Format(time.RFC3339Nano),
	})
	_, _ = w.Write(body)
}
