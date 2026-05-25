package sse

import (
	"github.com/walera/walera/internal/metrics"
)

type PoolMetricsAdapter struct {
	r *metrics.Registry
}

func NewPoolMetricsAdapter(r *metrics.Registry) *PoolMetricsAdapter {
	if r == nil {
		panic("sse.NewPoolMetricsAdapter: registry is required")
	}
	return &PoolMetricsAdapter{r: r}
}

func (a *PoolMetricsAdapter) Registry() *metrics.Registry { return a.r }

func (a *PoolMetricsAdapter) EventsSentInc(kind string)  { a.r.EventsSent(kind).Inc() }
func (a *PoolMetricsAdapter) TxDroppedInc(reason string) { a.r.TxDropped(reason).Inc() }
func (a *PoolMetricsAdapter) SubscriberDisconnectsInc(reason string) {
	a.r.SubscriberDisconnects(reason).Inc()
}
func (a *PoolMetricsAdapter) SubscriberLifetimeObserve(seconds float64) {
	a.r.SubscriberLifetime().Observe(seconds)
}

func (a *PoolMetricsAdapter) PoolWorkerDirtySubsInc(workerID string) {
	a.r.PoolWorkerDirtySubs(workerID).Inc()
}
func (a *PoolMetricsAdapter) PoolWorkerDirtySubsDec(workerID string) {
	a.r.PoolWorkerDirtySubs(workerID).Dec()
}
func (a *PoolMetricsAdapter) PoolWorkerDirtySubsSet(workerID string, v float64) {
	a.r.PoolWorkerDirtySubs(workerID).Set(v)
}
func (a *PoolMetricsAdapter) PoolDrainBatchSizeObserve(n float64) {
	a.r.PoolDrainBatchSize().Observe(n)
}
func (a *PoolMetricsAdapter) PoolDrainDurationObserve(seconds float64) {
	a.r.PoolDrainDuration().Observe(seconds)
}

func (a *PoolMetricsAdapter) SlowClientDropsInc() {
	a.r.SlowClientDrops().Inc()
}

var _ metricsIface = (*PoolMetricsAdapter)(nil)
