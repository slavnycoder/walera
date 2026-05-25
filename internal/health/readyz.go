package health

import (
	"context"
	"time"

	"github.com/walera/walera/internal/safego"
)

type readyState struct {
	healthy   bool
	reason    string
	checkedAt time.Time
}

func (s *Server) StartReadinessProbe(ctx context.Context) {
	interval := s.cfg.ReadyzProbeInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	safego.Go("readyz-probe", func() {

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

func (s *Server) probe(ctx context.Context) {
	now := time.Now()

	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	pgErr := s.reader.CheckPG(probeCtx)
	pgOK := pgErr == nil

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
