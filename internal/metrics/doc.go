// Package metrics provides the Prometheus metric registry for Walera.
//
// Design:
//   - A private *prometheus.Registry is held inside the package's Registry struct;
//     there is no package-level singleton. Each binary constructs exactly one
//     Registry via New() in main and passes it to each constructor that needs to
//     observe metrics.
//   - Five metric families are registered at construction time:
//     walera_subscribers_active{type}            (Gauge)
//     walera_events_sent_total{type}             (Counter)
//     walera_tx_dropped_total{reason}            (Counter)
//     walera_subscriber_disconnects_total{reason} (Counter)
//     walera_route_lookup_duration_seconds       (Histogram, DefBuckets)
//   - Typed accessors (SubscribersActive, EventsSent, TxDropped,
//     SubscriberDisconnects, RouteLookupDuration) replace stringly-typed lookups
//     so callers cannot fat-finger a label or metric name. The accessors return
//     the underlying prometheus types (Gauge / Counter / Histogram).
//   - The Gatherer() accessor exposes the registry to unit tests and to the
//     /metrics HTTP route alongside /healthz and /readyz.
//
// Import discipline: this package depends only on the standard library and
// github.com/prometheus/client_golang. It does not import any other Walera
// package, mirroring the dependency-free posture of internal/wal/types.go and
// internal/safego.
package metrics
