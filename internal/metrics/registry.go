// Package metrics — registry.go defines the Registry struct that owns the
// Walera Prometheus collectors and exposes typed accessors.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry wraps a private prometheus.Registry plus the metric families. The
// fields are unexported by design — callers must go through the typed
// accessors which forward to WithLabelValues. This shape eliminates a class
// of bugs where a label is misspelled or a metric is double-registered.
//
// Construct exactly one Registry per process in main, then thread it through
// each package's constructor as a *Registry parameter (no package-level
// singletons).
type Registry struct {
	reg                   *prometheus.Registry
	subscribersActive     *prometheus.GaugeVec
	eventsSentTotal       *prometheus.CounterVec
	txDroppedTotal        *prometheus.CounterVec
	subscriberDisconnects *prometheus.CounterVec
	routeLookupDuration   prometheus.Histogram
	// Auth + limits + health metric families.
	authRequestsTotal    *prometheus.CounterVec
	authRequestDuration  prometheus.Histogram
	authBreakerState     prometheus.Gauge
	authBreakerStaleSubs prometheus.Gauge
	limitRejectedTotal   *prometheus.CounterVec
	pgConnectionStatus   prometheus.Gauge

	// WAL/PG metric families.
	pgReconnectsTotal prometheus.Counter
	walLSNLagBytes    prometheus.Gauge
	walTxSizeChanges  prometheus.Histogram
	walDecodeDuration prometheus.Histogram

	// Standby-ticker ACK failure counter.
	walStandbyACKFailures prometheus.Counter

	// Routing metric families.
	routingFanOut        prometheus.Histogram
	routingIndexSize     *prometheus.GaugeVec
	subscriberQueueDepth *prometheus.HistogramVec
	subscriberLifetime   prometheus.Histogram

	// Auth refresh metric family.
	authRefreshTotal *prometheus.CounterVec

	// SSE pool metric families.
	poolWorkerDirtySubs *prometheus.GaugeVec
	poolDrainBatchSize  prometheus.Histogram
	poolDrainDuration   prometheus.Histogram

	// Spec-named slow-client counter (SSE-02). Unlabelled. Complements
	// walera_subscriber_disconnects_total{reason="slow_consumer"}, which
	// remains the general disconnect-by-reason surface. Both counters
	// move in lockstep on every slow-client drop.
	slowClientDrops prometheus.Counter
}

// New constructs a Registry with all metrics pre-registered.
//
// Metric inventory:
//   - walera_subscribers_active{type}             (Gauge)
//   - walera_events_sent_total{type}              (Counter)
//   - walera_tx_dropped_total{reason}             (Counter)
//   - walera_subscriber_disconnects_total{reason} (Counter)
//   - walera_route_lookup_duration_seconds        (Histogram, DefBuckets)
//
// Uses reg.MustRegister(...) — collector-registration failures indicate a
// programmer error (duplicate name or label set) and should fail-fast at
// startup, not at first observation. The registry is inspected via
// Gatherer().Gather() and backs the /metrics scrape endpoint.
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

	// Go runtime + process collectors on the SAME private registry.
	// MetricsAll = regexp.MustCompile("/.*") — enables the full runtime/metrics
	// catalog (goroutines, GC pauses, heap, scheduler). The "no leakage to
	// prometheus.DefaultRegisterer" invariant is preserved: both collectors
	// are registered on `reg`, never on the default global.
	reg.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
	))
	reg.MustRegister(collectors.NewProcessCollector(
		collectors.ProcessCollectorOpts{
			// No Namespace: keep canonical names (process_open_fds, ...) so
			// the FDPressure alert query matches without re-namespacing.
			ReportErrors: false,
		},
	))

	// Pre-touch label series so each labelled family is visible in /metrics
	// from t=0 — Prometheus does not emit series for CounterVec/HistogramVec
	// children until WithLabelValues materialises them. This is required for
	// the alert rules that reference specific label values (e.g.,
	// reason="slow_consumer").
	r.routingIndexSize.WithLabelValues("exact").Add(0)
	r.routingIndexSize.WithLabelValues("wildcard").Add(0)
	r.subscriberQueueDepth.WithLabelValues("exact").Observe(0)
	r.subscriberQueueDepth.WithLabelValues("wildcard").Observe(0)
	for _, result := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		r.authRefreshTotal.WithLabelValues(result).Add(0)
	}
	// Pre-touch every reason documented in the
	// walera_subscriber_disconnects_total Help string so dashboards
	// keyed on `rate(...{reason="X"}[5m])` see zero (not "no data") from
	// process start — most importantly reason="shutdown", which only fires
	// on a graceful shutdown event (which may never happen in long-lived
	// pods). The other three reasons are likewise pre-touched here to keep
	// "every documented reason is pre-touched" invariant straightforward.
	for _, reason := range []string{"slow_consumer", "tx_too_large", "client_closed", "shutdown"} {
		r.subscriberDisconnects.WithLabelValues(reason).Add(0)
	}

	return r
}

// newSSEMetrics constructs the SSE-facing metric families and writes them into r.
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
			// Help string is reconciled with the locked behaviour captured
			// in scripts/golden/metrics_v13.json. The pool's per-frame
			// counter loop (pool.go drainSubDeadline:
			// `for range st.buffer { EventsSentInc(kind) }`) increments on
			// every frame in the buffer, INCLUDING the `:\n\n` heartbeat
			// frames appended by sweepHeartbeats. The metric therefore
			// counts "frames sent" not "tx events sent" — phrase the Help
			// string accordingly so operators querying the metric see the
			// correct unit. For a tx-only rate, subtract the heartbeat
			// cadence (one per active sub per HeartbeatInterval).
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

	// SSE pool metric families.
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

	// Spec-named slow-client counter (SSE-02). Unlabelled — Prometheus
	// emits unlabelled counters as zero from registration, so no
	// pre-touch is required (cf. the labelled SubscriberDisconnects
	// pre-touch loop below). Complements
	// walera_subscriber_disconnects_total{reason="slow_consumer"}: both
	// counters increment together on every slow-client drop.
	r.slowClientDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "walera_slow_client_drops_total",
		Help: "Total SSE subscribers dropped because their per-client queue could not keep pace (slow-client policy). Complements walera_subscriber_disconnects_total{reason=\"slow_consumer\"}, which remains the general disconnect-by-reason surface; both counters move in lockstep.",
	})
}

// newAuthMetrics constructs the auth metric families and writes them into r.
func newAuthMetrics(r *Registry) {
	// Auth + limits + health metric families.
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

// newLimitsMetrics constructs the admission-control metric families and writes them into r.
func newLimitsMetrics(r *Registry) {
	r.limitRejectedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "walera_limit_rejected_total",
			Help: "Admission-control rejections by kind (global_concurrent|per_user_concurrent|pre_auth_rate|per_user_rate).",
		},
		[]string{"kind"},
	)
}

// newWALMetrics constructs the WAL/PG metric families and writes them into r.
func newWALMetrics(r *Registry) {
	r.pgConnectionStatus = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "walera_pg_connection_status",
			Help: "PG replication connection status (0=disconnected, 1=connected).",
		},
	)

	// WAL/PG metric families.

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

// newRouterMetrics constructs the routing metric families and writes them into r.
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

// SubscribersActive returns the gauge for the given subscriber kind
// ("exact" or "wildcard").
func (r *Registry) SubscribersActive(kind string) prometheus.Gauge {
	return r.subscribersActive.WithLabelValues(kind)
}

// EventsSent returns the counter for events sent to subscribers of the given
// kind ("exact" or "wildcard").
func (r *Registry) EventsSent(kind string) prometheus.Counter {
	return r.eventsSentTotal.WithLabelValues(kind)
}

// TxDropped returns the counter for transactions dropped by the given reason
// ("slow_consumer", "tx_too_large", or "multi_root").
func (r *Registry) TxDropped(reason string) prometheus.Counter {
	return r.txDroppedTotal.WithLabelValues(reason)
}

// SubscriberDisconnects returns the counter for subscriber disconnects by the
// given reason ("slow_consumer", "tx_too_large", or "client_closed").
func (r *Registry) SubscriberDisconnects(reason string) prometheus.Counter {
	return r.subscriberDisconnects.WithLabelValues(reason)
}

// RouteLookupDuration returns the histogram observing per-tx routing lookup
// duration in seconds (combined exact + wildcard lookups).
func (r *Registry) RouteLookupDuration() prometheus.Histogram {
	return r.routeLookupDuration
}

// AuthRequests returns the counter for the walera_auth_requests_total family
// with the given result label ("ok"|"unauthorized"|"forbidden"|
// "not_found"|"unavailable").
func (r *Registry) AuthRequests(result string) prometheus.Counter {
	return r.authRequestsTotal.WithLabelValues(result)
}

// AuthRequestDuration returns the histogram observing auth backend request
// duration in seconds (walera_auth_request_duration_seconds).
func (r *Registry) AuthRequestDuration() prometheus.Histogram {
	return r.authRequestDuration
}

// AuthBreakerState returns the gauge for the auth circuit-breaker state
// (walera_auth_circuit_breaker_state: 0=Closed, 1=Open, 2=HalfOpen).
func (r *Registry) AuthBreakerState() prometheus.Gauge {
	return r.authBreakerState
}

// AuthBreakerStaleSubs returns the gauge for subscribers whose last refresh
// LSN-age has exceeded 1.5×ttl_seconds
// (walera_auth_breaker_stale_subscribers; sampled by the breaker package
// every 30s).
func (r *Registry) AuthBreakerStaleSubs() prometheus.Gauge {
	return r.authBreakerStaleSubs
}

// LimitRejected returns the counter for admission-control rejections by the
// given kind ("global_concurrent"|"per_user_concurrent"|"pre_auth_rate"|
// "per_user_rate").
func (r *Registry) LimitRejected(kind string) prometheus.Counter {
	return r.limitRejectedTotal.WithLabelValues(kind)
}

// PGConnectionStatus returns the gauge for the PostgreSQL replication
// connection status (walera_pg_connection_status: 0=disconnected,
// 1=connected). Written by health.Server from wal.Reader.IsConnected().
func (r *Registry) PGConnectionStatus() prometheus.Gauge {
	return r.pgConnectionStatus
}

// PGReconnects returns the counter for walera_pg_reconnects_total.
// Incremented by wal.Reader.Run on every transient-error retry.
func (r *Registry) PGReconnects() prometheus.Counter { return r.pgReconnectsTotal }

// WALStandbyACKFailures returns the counter for
// walera_wal_standby_ack_failures_total. Incremented inside the
// wal/reader.go standby-ticker goroutine on every SendStandbyStatusUpdate
// error; the ticker continues running on transient failures (behaviour
// preserved). Drives the WaleraStandbyAckFailures alert in
// deploy/prometheus/alerts.yaml.
func (r *Registry) WALStandbyACKFailures() prometheus.Counter { return r.walStandbyACKFailures }

// WALLSNLagBytes returns the gauge for walera_wal_lsn_lag_bytes.
// Set by the lag sampler every 5s from pg_wal_lsn_diff().
func (r *Registry) WALLSNLagBytes() prometheus.Gauge { return r.walLSNLagBytes }

// WALTxSizeChanges returns the histogram observing per-tx change count.
// Observed by wal.Reader inside the Commit branch of processWALMessage.
func (r *Registry) WALTxSizeChanges() prometheus.Histogram { return r.walTxSizeChanges }

// WALDecodeDuration returns the histogram observing per-WAL-message decode
// duration (idiom: `prometheus.NewTimer(...).ObserveDuration()`).
func (r *Registry) WALDecodeDuration() prometheus.Histogram { return r.walDecodeDuration }

// RoutingFanOut returns the histogram observing subscribers-matched per tx.
// Observed by router.Broadcaster after exact+wildcard lookup.
func (r *Registry) RoutingFanOut() prometheus.Histogram { return r.routingFanOut }

// RoutingIndexSize returns the gauge for the given index kind
// ("exact" | "wildcard"). Sampled every 30s by the metrics sampler.
func (r *Registry) RoutingIndexSize(kind string) prometheus.Gauge {
	return r.routingIndexSize.WithLabelValues(kind)
}

// SubscriberLifetime returns the histogram observing per-subscriber connection
// lifetime in seconds. Observed by the SSE writer's defer on disconnect.
func (r *Registry) SubscriberLifetime() prometheus.Histogram { return r.subscriberLifetime }

// AuthRefresh returns the counter for walera_auth_refresh_total with the given
// result label ("ok"|"unauthorized"|"forbidden"|"not_found"|"unavailable").
// Pre-touched in New() so every result series is visible from t=0.
func (r *Registry) AuthRefresh(result string) prometheus.Counter {
	return r.authRefreshTotal.WithLabelValues(result)
}

// PoolWorkerDirtySubs returns the gauge for walera_pool_worker_dirty_subs
// with the given worker_id label. Per-worker count of subscribers in the
// dirty list awaiting drain. The pool worker calls Inc on the
// clean→dirty transition, Dec on drain/evict, and Set(0) after drainAll
// completes to re-sync against any Inc/Dec accounting drift.
func (r *Registry) PoolWorkerDirtySubs(workerID string) prometheus.Gauge {
	return r.poolWorkerDirtySubs.WithLabelValues(workerID)
}

// PoolDrainBatchSize returns the histogram observing the number of dirty
// subscribers drained per drainAll cycle. Buckets are
// [1, 4, 16, 64, 256, 1024].
func (r *Registry) PoolDrainBatchSize() prometheus.Histogram {
	return r.poolDrainBatchSize
}

// PoolDrainDuration returns the histogram observing the wall-clock duration
// of each drainAll cycle in seconds. Buckets are
// [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0].
func (r *Registry) PoolDrainDuration() prometheus.Histogram {
	return r.poolDrainDuration
}

// SlowClientDrops returns the counter for walera_slow_client_drops_total
// (SSE-02). Incremented inside the SSE drain path whenever a subscriber is
// dropped because its bounded per-client queue could not keep pace. The
// labelled walera_subscriber_disconnects_total{reason="slow_consumer"}
// remains the general disconnect-by-reason surface; both counters move in
// lockstep on every slow-client drop. The counter is unlabelled, so
// Prometheus emits it as zero from registration — no pre-touch needed.
func (r *Registry) SlowClientDrops() prometheus.Counter {
	return r.slowClientDrops
}

// Gatherer returns the underlying prometheus.Gatherer for unit-test inspection
// (via Gather()) and for the /metrics HTTP endpoint.
func (r *Registry) Gatherer() prometheus.Gatherer {
	return r.reg
}
