package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Registry struct {
	reg                   *prometheus.Registry
	subscribersActive     *prometheus.GaugeVec
	eventsSentTotal       *prometheus.CounterVec
	subscriberDisconnects *prometheus.CounterVec
	authRequestsTotal     *prometheus.CounterVec
	pgConnectionStatus    prometheus.Gauge
}

func New() *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{
		reg: reg,
		subscribersActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walera_subscribers_active",
				Help: "Active SSE subscribers by kind (exact|wildcard).",
			},
			[]string{"type"},
		),
		eventsSentTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "walera_events_sent_total",
				Help: "Total SSE frames sent to subscribers by kind (exact|wildcard). Counts tx frames only (not heartbeats or initial_data).",
			},
			[]string{"type"},
		),
		subscriberDisconnects: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "walera_subscriber_disconnects_total",
				Help: "Total subscriber disconnects by reason (slow_consumer|tx_too_large|client_closed|shutdown|auth_revoked|auth_unavailable).",
			},
			[]string{"reason"},
		),
		authRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "walera_auth_requests_total",
				Help: "Auth backend requests by result (ok|unauthorized|forbidden|not_found|unavailable).",
			},
			[]string{"result"},
		),
		pgConnectionStatus: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walera_pg_connection_status",
				Help: "PG replication connection status (0=disconnected, 1=connected).",
			},
		),
	}

	reg.MustRegister(
		r.subscribersActive,
		r.eventsSentTotal,
		r.subscriberDisconnects,
		r.authRequestsTotal,
		r.pgConnectionStatus,
	)

	reg.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
	))
	reg.MustRegister(collectors.NewProcessCollector(
		collectors.ProcessCollectorOpts{
			ReportErrors: false,
		},
	))

	for _, reason := range []string{"slow_consumer", "tx_too_large", "client_closed", "shutdown"} {
		r.subscriberDisconnects.WithLabelValues(reason).Add(0)
	}

	return r
}

func (r *Registry) SubscribersActive(kind string) prometheus.Gauge {
	return r.subscribersActive.WithLabelValues(kind)
}

func (r *Registry) EventsSent(kind string) prometheus.Counter {
	return r.eventsSentTotal.WithLabelValues(kind)
}

func (r *Registry) SubscriberDisconnects(reason string) prometheus.Counter {
	return r.subscriberDisconnects.WithLabelValues(reason)
}

func (r *Registry) AuthRequests(result string) prometheus.Counter {
	return r.authRequestsTotal.WithLabelValues(result)
}

func (r *Registry) PGConnectionStatus() prometheus.Gauge {
	return r.pgConnectionStatus
}

func (r *Registry) Gatherer() prometheus.Gatherer {
	return r.reg
}
