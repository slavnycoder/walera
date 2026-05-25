package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Registry struct {
	reg                   *prometheus.Registry
	subscribersActive     *prometheus.GaugeVec
	eventsSentTotal       *prometheus.CounterVec
	txDroppedTotal        *prometheus.CounterVec
	subscriberDisconnects *prometheus.CounterVec
	routeLookupDuration   prometheus.Histogram

	authRequestsTotal    *prometheus.CounterVec
	authRequestDuration  prometheus.Histogram
	authBreakerState     prometheus.Gauge
	authBreakerStaleSubs prometheus.Gauge
	limitRejectedTotal   *prometheus.CounterVec
	pgConnectionStatus   prometheus.Gauge

	pgReconnectsTotal prometheus.Counter
	walLSNLagBytes    prometheus.Gauge
	walTxSizeChanges  prometheus.Histogram
	walDecodeDuration prometheus.Histogram

	walStandbyACKFailures prometheus.Counter

	routingFanOut        prometheus.Histogram
	routingIndexSize     *prometheus.GaugeVec
	subscriberQueueDepth *prometheus.HistogramVec
	subscriberLifetime   prometheus.Histogram

	authRefreshTotal *prometheus.CounterVec

	poolWorkerDirtySubs *prometheus.GaugeVec
	poolDrainBatchSize  prometheus.Histogram
	poolDrainDuration   prometheus.Histogram

	slowClientDrops prometheus.Counter
}

func New() *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{reg: reg}

	newSSEMetrics(r)
	newAuthMetrics(r)
	newLimitsMetrics(r)
	newWALMetrics(r)
	newRouterMetrics(r)

	reg.MustRegister(
		r.subscribersActive,
		r.eventsSentTotal,
		r.txDroppedTotal,
		r.subscriberDisconnects,
		r.routeLookupDuration,
		r.authRequestsTotal,
		r.authRequestDuration,
		r.authBreakerState,
		r.authBreakerStaleSubs,
		r.limitRejectedTotal,
		r.pgConnectionStatus,
		r.pgReconnectsTotal,
		r.walStandbyACKFailures,
		r.walLSNLagBytes,
		r.walTxSizeChanges,
		r.walDecodeDuration,
		r.routingFanOut,
		r.routingIndexSize,
		r.subscriberQueueDepth,
		r.subscriberLifetime,
		r.authRefreshTotal,
		r.poolWorkerDirtySubs,
		r.poolDrainBatchSize,
		r.poolDrainDuration,
		r.slowClientDrops,
	)

	reg.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
	))
	reg.MustRegister(collectors.NewProcessCollector(
		collectors.ProcessCollectorOpts{

			ReportErrors: false,
		},
	))

	r.routingIndexSize.WithLabelValues("exact").Add(0)
	r.routingIndexSize.WithLabelValues("wildcard").Add(0)
	r.subscriberQueueDepth.WithLabelValues("exact").Observe(0)
	r.subscriberQueueDepth.WithLabelValues("wildcard").Observe(0)
	for _, result := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		r.authRefreshTotal.WithLabelValues(result).Add(0)
	}

	for _, reason := range []string{"slow_consumer", "tx_too_large", "client_closed", "shutdown"} {
		r.subscriberDisconnects.WithLabelValues(reason).Add(0)
	}

	return r
}

func newSSEMetrics(r *Registry) {
	r.subscribersActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "walera_subscribers_active",
			Help: "Active SSE subscribers by kind (exact|wildcard).",
		},
		[]string{"type"},
	)
	r.eventsSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "walera_events_sent_total",

			Help: "Total SSE frames sent to subscribers by kind (exact|wildcard). Includes both tx-data frames and per-sub heartbeat (`:` comment) frames; for a tx-only rate subtract heartbeat cadence × active subs.",
		},
		[]string{"type"},
	)
	r.txDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "walera_tx_dropped_total",
			Help: "Total transactions dropped before delivery by reason (slow_consumer|tx_too_large|multi_root).",
		},
		[]string{"reason"},
	)
	r.subscriberDisconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "walera_subscriber_disconnects_total",
			Help: "Total subscriber disconnects by reason (slow_consumer|tx_too_large|client_closed|shutdown).",
		},
		[]string{"reason"},
	)
	r.routeLookupDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "walera_route_lookup_duration_seconds",
			Help:    "Per-tx routing lookup duration (exact + wildcard combined).",
			Buckets: prometheus.DefBuckets,
		},
	)

	r.poolWorkerDirtySubs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "walera_pool_worker_dirty_subs",
		Help: "Per-worker count of subscribers in the dirty list awaiting drain.",
	}, []string{"worker_id"})
	r.poolDrainBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "walera_pool_drain_batch_size",
		Help:    "Number of dirty subscribers drained per drainAll cycle.",
		Buckets: []float64{1, 4, 16, 64, 256, 1024},
	})
	r.poolDrainDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "walera_pool_drain_duration_seconds",
		Help:    "Wall-clock duration of each drainAll cycle in seconds.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
	})

	r.slowClientDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "walera_slow_client_drops_total",
		Help: "Total SSE subscribers dropped because their per-client queue could not keep pace (slow-client policy). Complements walera_subscriber_disconnects_total{reason=\"slow_consumer\"}, which remains the general disconnect-by-reason surface; both counters move in lockstep.",
	})
}

func newAuthMetrics(r *Registry) {

	r.authRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "walera_auth_requests_total",
			Help: "Auth backend requests by result (ok|unauthorized|forbidden|not_found|unavailable).",
		},
		[]string{"result"},
	)
	r.authRequestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "walera_auth_request_duration_seconds",
			Help:    "Auth backend request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)
	r.authBreakerState = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "walera_auth_circuit_breaker_state",
			Help: "Auth circuit breaker state (0=Closed, 1=Open, 2=HalfOpen).",
		},
	)
	r.authBreakerStaleSubs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "walera_auth_breaker_stale_subscribers",
			Help: "Subscribers with last_refresh > 1.5×ttl_seconds (sampled by registry every 30s).",
		},
	)
	r.authRefreshTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "walera_auth_refresh_total",
		Help: "Per-subscriber auth refresh attempts by result (separates from handshake auth_requests_total).",
	}, []string{"result"})
}

func newLimitsMetrics(r *Registry) {
	r.limitRejectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "walera_limit_rejected_total",
			Help: "Admission-control rejections by kind (global_concurrent|per_user_concurrent|pre_auth_rate|per_user_rate).",
		},
		[]string{"kind"},
	)
}

func newWALMetrics(r *Registry) {
	r.pgConnectionStatus = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "walera_pg_connection_status",
			Help: "PG replication connection status (0=disconnected, 1=connected).",
		},
	)

	r.pgReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "walera_pg_reconnects_total",
		Help: "Total PG replication-connection reconnect attempts.",
	})
	r.walStandbyACKFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "walera_wal_standby_ack_failures_total",
		Help: "Total SendStandbyStatusUpdate errors from the wal-standby-ticker goroutine. Incremented on every transient ACK failure; the ticker continues running so this counter drives the WaleraStandbyAckFailures alert rather than gating any control-plane behaviour.",
	})
	r.walLSNLagBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "walera_wal_lsn_lag_bytes",
		Help: "PG WAL lag: pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) — sampled every 5s.",
	})
	r.walTxSizeChanges = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "walera_wal_tx_size_changes",
		Help:    "Number of changes per decoded WAL transaction.",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000},
	})
	r.walDecodeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "walera_wal_decode_duration_seconds",
		Help:    "Per-WAL-message decode duration in seconds (µs..1s range).",
		Buckets: []float64{1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 0.1, 1},
	})
}

func newRouterMetrics(r *Registry) {
	r.routingFanOut = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "walera_routing_fan_out",
		Help:    "Number of subscribers matched per tx (per-tx fan-out).",
		Buckets: []float64{1, 5, 25, 100, 500, 2500, 10000},
	})
	r.routingIndexSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "walera_routing_index_size",
		Help: "Subscribers registered per index kind, sampled every 30s.",
	}, []string{"index_kind"})
	r.subscriberQueueDepth = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "walera_subscriber_queue_depth",
		Help:    "Per-subscriber buffered-channel len(), sampled every 30s.",
		Buckets: []float64{1, 4, 16, 64, 256, 1024, 4096},
	}, []string{"type"})
	r.subscriberLifetime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "walera_subscriber_lifetime_seconds",
		Help:    "Connection lifetime of disconnected subscribers in seconds (business-range buckets).",
		Buckets: []float64{1, 10, 60, 600, 3600, 21600, 86400},
	})
}

func (r *Registry) SubscribersActive(kind string) prometheus.Gauge {
	return r.subscribersActive.WithLabelValues(kind)
}

func (r *Registry) EventsSent(kind string) prometheus.Counter {
	return r.eventsSentTotal.WithLabelValues(kind)
}

func (r *Registry) TxDropped(reason string) prometheus.Counter {
	return r.txDroppedTotal.WithLabelValues(reason)
}

func (r *Registry) SubscriberDisconnects(reason string) prometheus.Counter {
	return r.subscriberDisconnects.WithLabelValues(reason)
}

func (r *Registry) RouteLookupDuration() prometheus.Histogram {
	return r.routeLookupDuration
}

func (r *Registry) AuthRequests(result string) prometheus.Counter {
	return r.authRequestsTotal.WithLabelValues(result)
}

func (r *Registry) AuthRequestDuration() prometheus.Histogram {
	return r.authRequestDuration
}

func (r *Registry) AuthBreakerState() prometheus.Gauge {
	return r.authBreakerState
}

func (r *Registry) AuthBreakerStaleSubs() prometheus.Gauge {
	return r.authBreakerStaleSubs
}

func (r *Registry) LimitRejected(kind string) prometheus.Counter {
	return r.limitRejectedTotal.WithLabelValues(kind)
}

func (r *Registry) PGConnectionStatus() prometheus.Gauge {
	return r.pgConnectionStatus
}

func (r *Registry) PGReconnects() prometheus.Counter { return r.pgReconnectsTotal }

func (r *Registry) WALStandbyACKFailures() prometheus.Counter { return r.walStandbyACKFailures }

func (r *Registry) WALLSNLagBytes() prometheus.Gauge { return r.walLSNLagBytes }

func (r *Registry) WALTxSizeChanges() prometheus.Histogram { return r.walTxSizeChanges }

func (r *Registry) WALDecodeDuration() prometheus.Histogram { return r.walDecodeDuration }

func (r *Registry) RoutingFanOut() prometheus.Histogram { return r.routingFanOut }

func (r *Registry) RoutingIndexSize(kind string) prometheus.Gauge {
	return r.routingIndexSize.WithLabelValues(kind)
}

func (r *Registry) SubscriberLifetime() prometheus.Histogram { return r.subscriberLifetime }

func (r *Registry) AuthRefresh(result string) prometheus.Counter {
	return r.authRefreshTotal.WithLabelValues(result)
}

func (r *Registry) PoolWorkerDirtySubs(workerID string) prometheus.Gauge {
	return r.poolWorkerDirtySubs.WithLabelValues(workerID)
}

func (r *Registry) PoolDrainBatchSize() prometheus.Histogram {
	return r.poolDrainBatchSize
}

func (r *Registry) PoolDrainDuration() prometheus.Histogram {
	return r.poolDrainDuration
}

func (r *Registry) SlowClientDrops() prometheus.Counter {
	return r.slowClientDrops
}

func (r *Registry) Gatherer() prometheus.Gatherer {
	return r.reg
}
