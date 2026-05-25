//go:build golden_capture

package sse

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

const (
	captureScenarioName = "v13-single-sub-100-1-1"
	captureDescription  = "1 sub (exact kind), 100 events, exactly 1 heartbeat, 1 graceful shutdown disconnect"
	captureFrameCount   = 100
)

func TestCaptureMetricsV13Fixture(t *testing.T) {

	reg, _, err := runMetricsV13Scenario()
	if err != nil {
		t.Fatalf("runMetricsV13Scenario: %v", err)
	}

	fix := fixture{
		ScenarioName: captureScenarioName,
		Description:  captureDescription,
		Deltas:       extractCounterDeltas(t, reg),
		Histograms:   extractHistogramAsserts(t, reg),
	}

	root := metricsFixtureRepoRoot(t)
	outDir := filepath.Join(root, "scripts", "golden")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", outDir, err)
	}
	outPath := filepath.Join(outDir, "metrics_v13.json")

	buf, err := json.MarshalIndent(fix, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent: %v", err)
	}

	buf = append(buf, '\n')

	if err := os.WriteFile(outPath, buf, 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", outPath, err)
	}
	t.Logf("wrote %d bytes to %s (scenario=%s, %d deltas, %d histograms)",
		len(buf), outPath, fix.ScenarioName, len(fix.Deltas), len(fix.Histograms))
}

func metricsFixtureRepoRoot(t *testing.T) string {
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
			t.Fatalf("metricsFixtureRepoRoot: walked past filesystem root from %q without finding go.mod", cwd)
		}
		dir = parent
	}
}

type metricsParityShim struct{ r *metrics.Registry }

func (s *metricsParityShim) EventsSentInc(kind string) { s.r.EventsSent(kind).Inc() }
func (s *metricsParityShim) TxDroppedInc(reason string) {
	s.r.TxDropped(reason).Inc()
}
func (s *metricsParityShim) SubscriberLifetimeObserve(seconds float64) {
	s.r.SubscriberLifetime().Observe(seconds)
}
func (s *metricsParityShim) SubscriberDisconnectsInc(reason string) {
	s.r.SubscriberDisconnects(reason).Inc()
}
func (s *metricsParityShim) PoolWorkerDirtySubsInc(workerID string) {
	s.r.PoolWorkerDirtySubs(workerID).Inc()
}
func (s *metricsParityShim) PoolWorkerDirtySubsDec(workerID string) {
	s.r.PoolWorkerDirtySubs(workerID).Dec()
}
func (s *metricsParityShim) PoolWorkerDirtySubsSet(workerID string, v float64) {
	s.r.PoolWorkerDirtySubs(workerID).Set(v)
}
func (s *metricsParityShim) PoolDrainBatchSizeObserve(n float64) {
	s.r.PoolDrainBatchSize().Observe(n)
}
func (s *metricsParityShim) PoolDrainDurationObserve(seconds float64) {
	s.r.PoolDrainDuration().Observe(seconds)
}
func (s *metricsParityShim) SlowClientDropsInc() {
	s.r.SlowClientDrops().Inc()
}

func runMetricsV13Scenario() (*metrics.Registry, *goldenRespWriter, error) {
	reg := metrics.New()
	shim := &metricsParityShim{r: reg}
	enc := NewEncoder(10 * 1024 * 1024)
	p := NewPool(scenarioPoolConfig(), PoolDeps{Encoder: enc, Metrics: shim, Logger: zerolog.Nop()})

	sub := newFixtureSub("metrics-v13-sub-001", "exact")
	rw := &goldenRespWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		return reg, rw, err
	}

	frame := []byte("event: tx\ndata: {\"k\":1}\n\n")
	for i := 0; i < captureFrameCount; i++ {

		for !sub.SendForTest(frame) {
			time.Sleep(time.Millisecond)
		}
	}

	if werr := waitForCounter(reg, "walera_events_sent_total", "type", "exact", float64(captureFrameCount), 2*time.Second); werr != nil {
		return reg, rw, werr
	}

	time.Sleep(scenarioPoolConfig().HeartbeatInterval + 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if serr := p.Shutdown(ctx); serr != nil {
		return reg, rw, serr
	}

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return reg, rw, errShutdownTimeout
	}

	return reg, rw, nil
}

func scenarioPoolConfig() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          32,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1000,
		HeartbeatInterval:     50 * time.Millisecond,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
		MaxBatchBytesPerSub:   1024 * 1024,
	}
}

func waitForCounter(reg *metrics.Registry, name, labelName, labelValue string, want float64, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		mfs, err := reg.Gatherer().Gather()
		if err == nil {
			for _, mf := range mfs {
				if mf.GetName() != name {
					continue
				}
				for _, m := range mf.GetMetric() {
					if labelsContain(m, labelName, labelValue) {
						if c := m.GetCounter(); c != nil && c.GetValue() >= want {
							return nil
						}
					}
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	return errCounterWaitTimeout
}

var errCounterWaitTimeout = &captureError{msg: "metrics_v13_capture: counter did not reach target within budget"}

type captureError struct{ msg string }

func (e *captureError) Error() string { return e.msg }

func labelsContain(m *dto.Metric, name, value string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name && lp.GetValue() == value {
			return true
		}
	}
	return false
}

func extractCounterDeltas(t *testing.T, reg *metrics.Registry) []counterDelta {
	t.Helper()
	targets := map[string]bool{
		"walera_events_sent_total":            true,
		"walera_subscriber_disconnects_total": true,
		"walera_tx_dropped_total":             true,
	}
	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var out []counterDelta
	for _, mf := range mfs {
		name := mf.GetName()
		if !targets[name] {
			continue
		}
		for _, m := range mf.GetMetric() {
			c := m.GetCounter()
			if c == nil {
				continue
			}
			v := c.GetValue()
			if v == 0 {

				continue
			}
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			out = append(out, counterDelta{Name: name, Labels: labels, Delta: v})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return labelMapKey(out[i].Labels) < labelMapKey(out[j].Labels)
	})
	return out
}

func extractHistogramAsserts(t *testing.T, reg *metrics.Registry) []histogramAssert {
	t.Helper()
	targets := map[string]bool{
		"walera_subscriber_lifetime_seconds": true,
	}
	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var out []histogramAssert
	for _, mf := range mfs {
		name := mf.GetName()
		if !targets[name] {
			continue
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			if h == nil {
				continue
			}
			if h.GetSampleCount() == 0 {
				continue
			}
			out = append(out, histogramAssert{Name: name, MinCount: h.GetSampleCount()})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func labelMapKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out string
	for _, k := range keys {
		out += k + "=" + m[k] + ";"
	}
	return out
}
