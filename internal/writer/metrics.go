// Package writer — metrics.go defines WriterRegistry, the private
// prometheus.Registry that owns the writer's metric set. The Gatherer()
// handle backs the /metrics HTTP route. See INVARIANTS.md for the
// scenario-Reset and pre-touch contracts.
package writer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// WriterRegistry is the writer binary's private metrics registry.
// Callers go through typed accessors (TxTotal, RowsTotal, …). Construct
// exactly one per process in cmd/writer.
type WriterRegistry struct {
	reg *prometheus.Registry

	// Per-tx counters and gauges.
	txTotal             *prometheus.CounterVec // writer_tx_total{scenario,target}
	rowsTotal           *prometheus.CounterVec // writer_rows_total{scenario,target,op}
	commitRate          *prometheus.GaugeVec   // writer_commit_rate{scenario}
	errorsTotal         *prometheus.CounterVec // writer_errors_total{reason}
	scenarioGauge       *prometheus.GaugeVec   // writer_scenario{scenario}
	overloadEventsTotal prometheus.Counter     // writer_overload_events_total
	poolBusy            prometheus.Gauge       // writer_pool_busy
	poolIdle            prometheus.Gauge       // writer_pool_idle

	startedAt time.Time
}

// NewRegistry constructs a WriterRegistry with every metric family plus
// the standard Go runtime + process collectors on a private
// prometheus.Registry. The global DefaultRegisterer is NOT touched.
// See INVARIANTS.md for the metric inventory.
func NewRegistry() *WriterRegistry {
	reg := prometheus.NewRegistry()
	r := &WriterRegistry{reg: reg, startedAt: time.Now()}

	r.txTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writer_tx_total",
			Help: "Total successful commits by scenario and target table. Increments ONLY after tx.Commit returns nil (parity invariant — see INVARIANTS.md).",
		},
		[]string{"scenario", "target"},
	)
	r.rowsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writer_rows_total",
			Help: "Total rows written by scenario, target table, and op (insert|update|delete). v1.1 emits insert only.",
		},
		[]string{"scenario", "target", "op"},
	)
	r.commitRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "writer_commit_rate",
			Help: "Target commit rate in tx/s for the active scenario. SetActiveScenario resets the family so only the current scenario series is present.",
		},
		[]string{"scenario"},
	)
	r.errorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writer_errors_total",
			Help: "Total commit failures by classified reason (pg_conn|pg_constraint|pg_other|tx_timeout). A failed tx is counted here and NOT in writer_tx_total.",
		},
		[]string{"reason"},
	)
	r.scenarioGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "writer_scenario",
			Help: "Active scenario indicator (1 for the current scenario, others removed via Reset on switch). Prometheus enum-pattern.",
		},
		[]string{"scenario"},
	)
	r.overloadEventsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "writer_overload_events_total",
		Help: "Total overload events observed by the stress scenario when a per-tx context exceeds its deadline.",
	})
	r.poolBusy = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "writer_pool_busy",
		Help: "pgxpool acquired-connections count (sampled every 1s). Debug aid (CONTEXT discretion).",
	})
	r.poolIdle = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "writer_pool_idle",
		Help: "pgxpool idle-connections count (sampled every 1s). Debug aid (CONTEXT discretion).",
	})

	reg.MustRegister(
		r.txTotal,
		r.rowsTotal,
		r.commitRate,
		r.errorsTotal,
		r.scenarioGauge,
		r.overloadEventsTotal,
		r.poolBusy,
		r.poolIdle,
	)

	// Standard Go runtime + process collectors on the SAME private registry.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Pre-touch the known writer_errors_total reasons so each labelled
	// series is visible from t=0 (Prometheus only emits CounterVec children
	// once WithLabelValues materialises them).
	for _, reason := range []string{"pg_conn", "pg_constraint", "pg_other", "tx_timeout"} {
		r.errorsTotal.WithLabelValues(reason).Add(0)
	}

	return r
}

// Gatherer returns the underlying prometheus.Gatherer for /metrics and
// unit-test inspection via Gather().
func (r *WriterRegistry) Gatherer() prometheus.Gatherer { return r.reg }

// Uptime returns the duration since NewRegistry was called.
func (r *WriterRegistry) Uptime() time.Duration { return time.Since(r.startedAt) }

// TxTotal increments writer_tx_total{scenario,target} by 1. MUST be
// called only after a successful tx.Commit. See INVARIANTS.md.
func (r *WriterRegistry) TxTotal(scenario, target string) {
	r.txTotal.WithLabelValues(scenario, target).Inc()
}

// RowsTotal adds n to writer_rows_total{scenario,target,op}.
func (r *WriterRegistry) RowsTotal(scenario, target, op string, n int) {
	r.rowsTotal.WithLabelValues(scenario, target, op).Add(float64(n))
}

// SetCommitRate sets writer_commit_rate{scenario} to rate.
func (r *WriterRegistry) SetCommitRate(scenario string, rate float64) {
	r.commitRate.WithLabelValues(scenario).Set(rate)
}

// Errors increments writer_errors_total{reason} by 1.
// Recognized reasons: pg_conn, pg_constraint, pg_other, tx_timeout.
func (r *WriterRegistry) Errors(reason string) {
	r.errorsTotal.WithLabelValues(reason).Inc()
}

// Overload increments writer_overload_events_total by 1.
func (r *WriterRegistry) Overload() {
	r.overloadEventsTotal.Inc()
}

// SetPoolStats sets writer_pool_busy and writer_pool_idle to the supplied
// counts (sampled once per second by the pool sampler in cmd/writer).
func (r *WriterRegistry) SetPoolStats(busy, idle int) {
	r.poolBusy.Set(float64(busy))
	r.poolIdle.Set(float64(idle))
}

// SetActiveScenario marks the named scenario as active. Both the
// scenarioGauge and commitRate families are Reset so only the active
// scenario survives in the gather output. See INVARIANTS.md.
func (r *WriterRegistry) SetActiveScenario(scenario string) {
	r.scenarioGauge.Reset()
	r.commitRate.Reset()
	r.scenarioGauge.WithLabelValues(scenario).Set(1)
}
