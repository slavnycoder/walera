// registry_bench_test.go — BenchmarkMetricsNew pins the per-call cost of
// metrics.New() so any future decomposition (BENCH-01 baseline; covers
// DECOMP-02 of the v2.4 readability sweep) cannot silently regress
// allocs/op or ns/op for the registry-assembly hot path.
//
// Why this is safe to iterate in a tight loop:
//   - metrics.New() calls prometheus.NewRegistry() as its FIRST statement,
//     so every iteration produces a fully fresh registry and the
//     MustRegister calls below it cannot panic on duplicate registration.
//   - The Go-runtime and process collectors registered inside New() are
//     filesystem-free at registration time; their /proc reads happen at
//     scrape time, which the bench never triggers.
//   - No global state is touched; the bench is single-goroutine and
//     leak-clean.
package metrics_test

import (
	"testing"

	"github.com/walera/walera/internal/metrics"
)

// BenchmarkMetricsNew measures one full metrics.New() assembly per
// iteration. No sub-benchmarks — the constructor takes no arguments and
// has no parameterised shape.
func BenchmarkMetricsNew(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		r := metrics.New()
		_ = r
	}
}
