// Package health hosts the operational HTTP routes: /healthz (liveness),
// /readyz (readiness), /metrics (Prometheus scrape). The Server is constructed
// in cmd/cdc-sse/main.go and its Routes are mounted on the shared
// http.ServeMux BEFORE the SSE handler routes.
//
// Two-track PG-failure policy:
//   - /healthz flips immediately when the WAL replication connection drops.
//     It NEVER calls the auth backend — an auth outage must not kill the pod
//     via the k8s liveness probe.
//   - /readyz flips when EITHER PG OR auth is unhealthy. Sampled by k8s
//     readiness with longer intervals. Backed by a cached state populated by
//     a background prober (safego.Go("readyz-probe", ...)) that runs every
//     cfg.ReadyzProbeInterval (default 5s).
//
// /metrics exposes the private metrics registry through promhttp.HandlerFor
// with EnableOpenMetrics=true.
package health
