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

type PgChecker interface {
	CheckPG(ctx context.Context) error
}

type AuthChecker interface {
	CheckAuth(ctx context.Context) error
}

type Server struct {
	reader     PgChecker
	authClient AuthChecker
	metricsReg *metrics.Registry
	cfg        Config
	readyCache atomic.Pointer[readyState]
	log        zerolog.Logger

	corsOrigins []string
}

func (s *Server) SetCORSOrigins(allowed []string) {
	s.corsOrigins = allowed
}

type Deps struct {
	Logger zerolog.Logger

	PgChecker PgChecker

	AuthChecker AuthChecker

	Metrics *metrics.Registry
}

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

func New(cfg Config, deps Deps) *Server {
	validateDeps(deps)
	return newServerInternal(deps.PgChecker, deps.AuthChecker, deps.Metrics, cfg, deps.Logger)
}

func newServerInternal(reader PgChecker, authClient AuthChecker, metricsReg *metrics.Registry, cfg Config, log zerolog.Logger) *Server {
	return &Server{
		reader:     reader,
		authClient: authClient,
		metricsReg: metricsReg,
		cfg:        cfg,
		log:        log,
	}
}

func (s *Server) Metrics() *metrics.Registry { return s.metricsReg }

func (s *Server) Routes(mux *http.ServeMux) {
	metricsHandler := promhttp.HandlerFor(
		s.metricsReg.Gatherer(),
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
			ErrorHandling:     promhttp.ContinueOnError,
		},
	)

	mux.HandleFunc("GET /healthz", s.withCORS(s.serveHealthz))
	mux.HandleFunc("GET /readyz", s.withCORS(s.serveReadyz))
	mux.HandleFunc("GET /metrics", s.withCORS(metricsHandler.ServeHTTP))
}

func (s *Server) withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.corsOrigins) > 0 {
			w.Header().Add("Vary", "Origin")
			if origin := r.Header.Get("Origin"); origin != "" {
				for _, a := range s.corsOrigins {
					if a == origin {
						w.Header().Set("Access-Control-Allow-Origin", origin)
						w.Header().Set("Access-Control-Allow-Credentials", "true")

						w.Header().Set("Timing-Allow-Origin", origin)
						break
					}
				}
			}
		}
		h(w, r)
	}
}

func (s *Server) serveHealthz(w http.ResponseWriter, r *http.Request) {
	if err := s.reader.CheckPG(r.Context()); err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("pg disconnected"))
}

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

	body, _ := json.Marshal(map[string]any{
		"reason":     state.reason,
		"checked_at": state.checkedAt.UTC().Format(time.RFC3339Nano),
	})
	_, _ = w.Write(body)
}
