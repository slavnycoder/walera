// Package limits — sweeper.go owns the GC sweeper for the per-IP and
// per-user rate-limiter maps.
//
// Without this sweeper, a Walera instance that sees many distinct IPs over
// its lifetime would accumulate one *rateEntry per ever-seen IP forever.
// At ~10k unique IPs/day that becomes a slow memory leak. The sweeper runs
// every cfg.SweepInterval (default 60s) and deletes entries idle for more
// than cfg.SweepIdleThreshold (default 5m).
package limits

import (
	"context"
	"sync"
	"time"
)

// RunSweeper is a long-lived goroutine that periodically sweeps idle entries
// from the per-IP and per-user rate-limiter maps. Spawn shape from
// cmd/cdc-sse/main.go:
//
//	safego.Go("limits-sweeper", func() { l.RunSweeper(ctx) })
//
// Exits cleanly on ctx.Done.
func (l *Limits) RunSweeper(ctx context.Context) {
	ticker := time.NewTicker(l.cfg.SweepInterval)
	defer ticker.Stop()
	thresholdNs := int64(l.cfg.SweepIdleThreshold)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cutoff := now.UnixNano() - thresholdNs
			l.sweep(&l.preAuthRate, cutoff)
			l.sweep(&l.perUserRate, cutoff)
		}
	}
}

// sweep walks the given map and deletes every rateEntry whose lastSeen is
// older than cutoff. sync.Map.Range is safe to call concurrently with
// Delete; the Range callback may observe Delete from other goroutines.
func (l *Limits) sweep(m *sync.Map, cutoff int64) {
	m.Range(func(k, v any) bool {
		if e, ok := v.(*rateEntry); ok && e.lastSeen.Load() < cutoff {
			m.Delete(k)
		}
		return true
	})
}
