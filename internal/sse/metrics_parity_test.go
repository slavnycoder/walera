package sse

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func TestMetricsParity_PoolPathMatchesV13Deltas(t *testing.T) {
	t.Parallel()

	fix := loadMetricsFixture(t)

	reg, err := runMetricsV13ScenarioParity()
	if err != nil {
		t.Fatalf("runMetricsV13ScenarioParity: %v", err)
	}

	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	for _, want := range fix.Deltas {
		actual, found := readCounterValue(mfs, want.Name, want.Labels)
		if !found {
			t.Fatalf("metric=%s labels=%v expected=%v actual=<not found>",
				want.Name, want.Labels, want.Delta)
		}

		if math.Abs(actual-want.Delta) > 1e-9 {
			t.Fatalf("metric=%s labels=%v expected=%v actual=%v",
				want.Name, want.Labels, want.Delta, actual)
		}
	}

	for _, want := range fix.Histograms {
		actual, found := readHistogramSampleCount(mfs, want.Name)
		if !found {
			t.Fatalf("histogram=%s expected=>=%d actual=<not found>",
				want.Name, want.MinCount)
		}
		if actual < want.MinCount {
			t.Fatalf("histogram=%s expected=>=%d actual=%d",
				want.Name, want.MinCount, actual)
		}
	}
}

func loadMetricsFixture(t *testing.T) fixture {
	t.Helper()
	root := metricsParityRepoRoot(t)
	path := filepath.Join(root, "scripts", "golden", "metrics_v13.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read metrics fixture %q: %v\n(regenerate via: go test -tags=golden_capture ./internal/sse -run TestCaptureMetricsV13Fixture)", path, err)
	}
	var fix fixture
	if err := json.Unmarshal(b, &fix); err != nil {
		t.Fatalf("parse metrics fixture %q: %v", path, err)
	}
	if fix.ScenarioName != "v13-single-sub-100-1-1" {
		t.Fatalf("metrics fixture scenario_name = %q; want %q (per CONTEXT.md verbatim per checker W-2)",
			fix.ScenarioName, "v13-single-sub-100-1-1")
	}
	if len(fix.Deltas) == 0 {
		t.Fatalf("metrics fixture has zero deltas — regenerate")
	}
	return fix
}

func metricsParityRepoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("metricsParityRepoRoot: walked past filesystem root from %q without finding go.mod", cwd)
		}
		dir = parent
	}
}

type metricsParityShimDefault struct{ r *metrics.Registry }

func (s *metricsParityShimDefault) EventsSentInc(kind string) {
	s.r.EventsSent(kind).Inc()
}
func (s *metricsParityShimDefault) TxDroppedInc(reason string) {
	s.r.TxDropped(reason).Inc()
}
func (s *metricsParityShimDefault) SubscriberLifetimeObserve(seconds float64) {
	s.r.SubscriberLifetime().Observe(seconds)
}
func (s *metricsParityShimDefault) SubscriberDisconnectsInc(reason string) {
	s.r.SubscriberDisconnects(reason).Inc()
}
func (s *metricsParityShimDefault) PoolWorkerDirtySubsInc(workerID string) {
	s.r.PoolWorkerDirtySubs(workerID).Inc()
}
func (s *metricsParityShimDefault) PoolWorkerDirtySubsDec(workerID string) {
	s.r.PoolWorkerDirtySubs(workerID).Dec()
}
func (s *metricsParityShimDefault) PoolWorkerDirtySubsSet(workerID string, v float64) {
	s.r.PoolWorkerDirtySubs(workerID).Set(v)
}
func (s *metricsParityShimDefault) PoolDrainBatchSizeObserve(n float64) {
	s.r.PoolDrainBatchSize().Observe(n)
}
func (s *metricsParityShimDefault) PoolDrainDurationObserve(seconds float64) {
	s.r.PoolDrainDuration().Observe(seconds)
}
func (s *metricsParityShimDefault) SlowClientDropsInc() {
	s.r.SlowClientDrops().Inc()
}

const (
	scenarioFrameCount  = 100
	scenarioHeartbeatMs = 50
)

func scenarioPoolConfigParity() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          32,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1000,
		HeartbeatInterval:     scenarioHeartbeatMs * time.Millisecond,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
		MaxBatchBytesPerSub:   1024 * 1024,
	}
}

func runMetricsV13ScenarioParity() (*metrics.Registry, error) {
	reg := metrics.New()
	shim := &metricsParityShimDefault{r: reg}
	enc := NewEncoder(10 * 1024 * 1024)
	p := NewPool(scenarioPoolConfigParity(), PoolDeps{Encoder: enc, Metrics: shim, Logger: zerolog.Nop()})

	sub := newFixtureSub("metrics-v13-sub-001", "exact")
	rw := &goldenRespWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		return reg, err
	}

	frame := []byte("event: tx\ndata: {\"k\":1}\n\n")
	for i := 0; i < scenarioFrameCount; i++ {
		for !sub.SendForTest(frame) {
			time.Sleep(time.Millisecond)
		}
	}

	if werr := waitForCounterValue(reg, "walera_events_sent_total", "type", "exact", float64(scenarioFrameCount), 2*time.Second); werr != nil {
		return reg, werr
	}

	time.Sleep(scenarioPoolConfigParity().HeartbeatInterval + 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if serr := p.Shutdown(ctx); serr != nil {
		return reg, serr
	}

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return reg, errShutdownTimeout
	}

	return reg, nil
}

func waitForCounterValue(reg *metrics.Registry, name, labelName, labelValue string, want float64, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		mfs, err := reg.Gatherer().Gather()
		if err == nil {
			for _, mf := range mfs {
				if mf.GetName() != name {
					continue
				}
				for _, m := range mf.GetMetric() {
					if metricHasLabel(m, labelName, labelValue) {
						if c := m.GetCounter(); c != nil && c.GetValue() >= want {
							return nil
						}
					}
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	return errMetricsParityTimeout
}

var errMetricsParityTimeout = &parityError{msg: "metrics_parity: counter did not reach target within budget"}

type parityError struct{ msg string }

func (e *parityError) Error() string { return e.msg }

func metricHasLabel(m *dto.Metric, name, value string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name && lp.GetValue() == value {
			return true
		}
	}
	return false
}

func readCounterValue(mfs []*dto.MetricFamily, name string, want map[string]string) (float64, bool) {
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatchExact(m.GetLabel(), want) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func readHistogramSampleCount(mfs []*dto.MetricFamily, name string) (uint64, bool) {
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				return h.GetSampleCount(), true
			}
		}
	}
	return 0, false
}

func labelsMatchExact(got []*dto.LabelPair, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, lp := range got {
		wantV, ok := want[lp.GetName()]
		if !ok {
			return false
		}
		if wantV != lp.GetValue() {
			return false
		}
	}
	return true
}
