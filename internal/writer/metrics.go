package writer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type WriterRegistry struct {
	reg *prometheus.Registry

	txTotal             *prometheus.CounterVec
	rowsTotal           *prometheus.CounterVec
	commitRate          *prometheus.GaugeVec
	errorsTotal         *prometheus.CounterVec
	scenarioGauge       *prometheus.GaugeVec
	overloadEventsTotal prometheus.Counter
	poolBusy            prometheus.Gauge
	poolIdle            prometheus.Gauge

	startedAt time.Time
}

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

	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	for _, reason := range []string{"pg_conn", "pg_constraint", "pg_other", "tx_timeout"} {
		r.errorsTotal.WithLabelValues(reason).Add(0)
	}

	return r
}

func (r *WriterRegistry) Gatherer() prometheus.Gatherer { return r.reg }

func (r *WriterRegistry) Uptime() time.Duration { return time.Since(r.startedAt) }

func (r *WriterRegistry) TxTotal(scenario, target string) {
	r.txTotal.WithLabelValues(scenario, target).Inc()
}

func (r *WriterRegistry) RowsTotal(scenario, target, op string, n int) {
	r.rowsTotal.WithLabelValues(scenario, target, op).Add(float64(n))
}

func (r *WriterRegistry) SetCommitRate(scenario string, rate float64) {
	r.commitRate.WithLabelValues(scenario).Set(rate)
}

func (r *WriterRegistry) Errors(reason string) {
	r.errorsTotal.WithLabelValues(reason).Inc()
}

func (r *WriterRegistry) Overload() {
	r.overloadEventsTotal.Inc()
}

func (r *WriterRegistry) SetPoolStats(busy, idle int) {
	r.poolBusy.Set(float64(busy))
	r.poolIdle.Set(float64(idle))
}

func (r *WriterRegistry) SetActiveScenario(scenario string) {
	r.scenarioGauge.Reset()
	r.commitRate.Reset()
	r.scenarioGauge.WithLabelValues(scenario).Set(1)
}
