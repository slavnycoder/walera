// Package health — readyz.go implements the background /readyz prober that
// keeps Server.readyCache populated with the most-recent (pgOK ∧ authOK) state.
//
// The prober runs in a dedicated safego.Go("readyz-probe", ...) goroutine
// spawned by StartReadinessProbe. The HTTP /readyz handler (serveReadyz in
// server.go) is a simple atomic.Pointer.Load on the hot path; all probing
// happens off the request thread.
//
// Each probe is bounded by a 2s context timeout (mirrors the auth backend
// timeout); the ticker period (default 5s) is longer than the timeout so
// probes cannot stack.
package health

import (
	"context"
	"time"

	"github.com/walera/walera/internal/safego"
)

// readyState is the immutable cached probe result published into the Server's
// atomic.Pointer. Each probe builds a fresh value and Store()s it; readers
// (serveReadyz) Load() it without locks.
//
// Fields:
//   - healthy: true iff pgOK ∧ authOK at the last probe.
//   - reason: operator-facing description when healthy=false. Empty string
//     when healthy=true. The reason field may carry internal error strings
//     (e.g., "auth backend unavailable: dial tcp 10.0.0.5:443:
//     connection refused"); /readyz is intended to be cluster-internal so the
//     small disclosure is accepted for MVP.
//   - checkedAt: wall-clock time the probe completed. Serialised in the
//     503 JSON body so operators can spot a stale cache.
type readyState struct {
	healthy   bool
	reason    string
	checkedAt time.Time
}

// StartReadinessProbe spawns the background prober. It returns immediately;
// the goroutine runs until ctx is cancelled. Call exactly once per Server
// instance (production wiring lives in cmd/cdc-sse/main.go).
//
// The first probe runs immediately (before the first ticker tick) so the
// cache is primed within ~probe-RTT after Start; otherwise /readyz would
// return "not yet probed" for the entire first cfg.ReadyzProbeInterval
// window.
func (s *Server) StartReadinessProbe(ctx context.Context) {
	interval := s.cfg.ReadyzProbeInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	safego.Go("readyz-probe", func() {
		// First probe — primes the cache before the first tick.
		s.probe(ctx)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.probe(ctx)
			}
		}
	})
}

// probe performs a single PG-and-auth health observation and atomically
// publishes the resulting readyState. Bounded by a 2s context timeout
// (mirrors the auth backend client timeout).
//
// Composition rule:
//   - If PG is disconnected: reason = "pg disconnected" (even if auth would
//     also fail — the operator's first remediation is the PG side).
//   - Else if auth backend is unavailable: reason = "auth backend
//     unavailable: <cause>".
//   - Else: healthy=true.
func (s *Server) probe(ctx context.Context) {
	now := time.Now()

	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	pgErr := s.reader.CheckPG(probeCtx)
	pgOK := pgErr == nil

	// Mirror CheckPG result into the walera_pg_connection_status gauge for
	// Prometheus scrapers. The k8s liveness probe reads CheckPG directly
	// via /healthz at 2s cadence; this gauge updates at the slower
	// readyz-probe cadence (default 5s), sufficient for the
	// WaleraPGDisconnected alert's `for: 1m` window.
	if pgOK {
		s.metricsReg.PGConnectionStatus().Set(1)
	} else {
		s.metricsReg.PGConnectionStatus().Set(0)
	}

	authErr := s.authClient.CheckAuth(probeCtx)

	var state *readyState
	switch {
	case !pgOK:
		state = &readyState{healthy: false, reason: "pg disconnected", checkedAt: now}
	case authErr != nil:
		state = &readyState{
			healthy:   false,
			reason:    "auth backend unavailable: " + authErr.Error(),
			checkedAt: now,
		}
	default:
		state = &readyState{healthy: true, reason: "", checkedAt: now}
	}
	s.readyCache.Store(state)
}
