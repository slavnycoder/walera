// Package sse — PoolMetricsAdapter, the production adapter that bridges
// *metrics.Registry's typed-counter `Xxx(label).Inc()` surface to the
// flat `XxxInc(label)` shape that metricsIface (pool.go) consumes on the
// per-frame hot path. Behavior is byte-identical to the pre-relocation
// poolMetricsShim previously declared in cmd/cdc-sse/main.go.
package sse

import (
	"github.com/walera/walera/internal/metrics"
)

// PoolMetricsAdapter forwards metricsIface calls to a shared
// *metrics.Registry. One pointer + ten one-line forwarders — no
// allocation on the per-frame path.
type PoolMetricsAdapter struct {
	r *metrics.Registry
}

// NewPoolMetricsAdapter returns a *PoolMetricsAdapter wrapping r. Panics
// if r is nil — same Deps + init-time nil-check convention used by
// validatePoolDeps and auth.NewBreaker.
func NewPoolMetricsAdapter(r *metrics.Registry) *PoolMetricsAdapter {
	if r == nil {
		panic("sse.NewPoolMetricsAdapter: registry is required")
	}
	return &PoolMetricsAdapter{r: r}
}

// Registry returns the registry this adapter wraps. Exposed so the
// composition-root singleton-identity test can compare pointers.
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

// SlowClientDropsInc forwards to walera_slow_client_drops_total. Called
// sibling-to SubscriberDisconnectsInc("slow_consumer") inside the drain
// path so both counters move in lockstep on every slow-client drop.
func (a *PoolMetricsAdapter) SlowClientDropsInc() {
	a.r.SlowClientDrops().Inc()
}

// Compile-time assertion that *PoolMetricsAdapter satisfies metricsIface.
// Placed in this production file (NOT a _test.go) so a future signature
// drift breaks `go build` at CI-gate time.
var _ metricsIface = (*PoolMetricsAdapter)(nil)
