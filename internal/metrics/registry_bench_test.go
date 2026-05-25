package metrics_test

import (
	"testing"

	"github.com/walera/walera/internal/metrics"
)

func BenchmarkMetricsNew(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		r := metrics.New()
		_ = r
	}
}
