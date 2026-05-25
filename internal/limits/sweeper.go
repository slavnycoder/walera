package limits

import (
	"context"
	"sync"
	"time"
)

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

func (l *Limits) sweep(m *sync.Map, cutoff int64) {
	m.Range(func(k, v any) bool {
		if e, ok := v.(*rateEntry); ok && e.lastSeen.Load() < cutoff {
			m.Delete(k)
		}
		return true
	})
}
