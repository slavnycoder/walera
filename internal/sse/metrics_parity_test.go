// Package sse — metrics_parity_test.go pins the contract: every
// pre-existing subscriber/event counter still increments end-to-end from
// the worker goroutine under the v1.4 pool path, AND the
// reason="shutdown" label introduced in plan 18-01 fires exactly once
// for the sub that received the shutdown frame.
// This is the default-build companion to golden_metrics_capture_test.go
// (which is build-tagged `golden_capture` and only re-runs when the
// v1.3 contract intentionally changes). The capture writes the
// scripts/golden/metrics_v13.json fixture; this test replays the same
// scenario and asserts every (counter_name, label_set, delta) tuple
// matches the fixture byte-for-byte.
// Failure mode (CONTEXT.md §"Metrics-Parity Test Q3.4"): structured diff
// — t.Fatalf prints `metric=%s labels=%v expected=%v actual=%v` so a
// regression pinpoints exactly which counter drifted, which label set,
// and by how much.
// Test isolation: each invocation constructs a fresh *metrics.Registry
// via metrics.New() so counter values are scenario-scoped, not
// process-scoped. No DefaultRegisterer pollution.
// This plan does NOT touch internal/metrics/registry.go (per checker
// B-2 — that file is owned by plan 18-03).
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

// TestMetricsParity_PoolPathMatchesV13Deltas loads
// scripts/golden/metrics_v13.json, replays the same single-sub
// deterministic scenario through the production pool path, and asserts
// every captured counter delta + histogram floor matches the fixture
// exactly.
// Single-sub scenario per checker W-2 (CONTEXT.md verbatim):
//   - 1 sub of kind "exact"
//   - 100 data frames
//   - exactly 1 heartbeat (HeartbeatInterval=50ms; wait
//     HeartbeatInterval+50ms after the last data frame)
//   - 1 graceful shutdown — drainShutdown emits one
//     SubscriberDisconnectsInc("shutdown") + one
//     SubscriberLifetimeObserve.
//
// On a forced fixture mismatch (e.g. manually editing the delta to 99)
// the failure message names the counter + label map + expected vs
// actual — operator-readable, not just a diff blob.
func TestMetricsParity_PoolPathMatchesV13Deltas(t *testing.T) {
	t.Parallel()

	// (1) Load the golden fixture from disk.
	fix := loadMetricsFixture(t)

	// (2) + (3) Run the deterministic single-sub scenario through a
	// fresh per-test registry. Returns the registry so we can introspect
	// the resulting counter / histogram values.
	reg, err := runMetricsV13ScenarioParity()
	if err != nil {
		t.Fatalf("runMetricsV13ScenarioParity: %v", err)
	}

	// (4) Walk the gatherer and assert each fixture delta matches.
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
		// Float-safe equality (counter values are integral in practice,
		// but use a small epsilon to be defensive against drift).
		if math.Abs(actual-want.Delta) > 1e-9 {
			t.Fatalf("metric=%s labels=%v expected=%v actual=%v",
				want.Name, want.Labels, want.Delta, actual)
		}
	}

	// (5) For each fixture histogram, assert actual SampleCount >=
	// MinCount.
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

// loadMetricsFixture reads scripts/golden/metrics_v13.json from the repo
// root. On missing/unparseable file the test FAILs with an instruction
// to regenerate.
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

// metricsParityRepoRoot walks up from the test working dir
// (internal/sse) to find the directory containing go.mod.
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

// metricsParityShimDefault is the default-build twin of the
// metricsParityShim defined in golden_metrics_capture_test.go (which is
// only compiled under -tags=golden_capture). Both shims have identical
// shape — the duplication exists because the build-tagged file is
// invisible to the default build. Any divergence between the two is a
// parity bug; if you change one, change the other.
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

// scenarioFrameCount + heartbeatInterval are the parity-test mirror of
// the capture harness's constants. Held here (not imported from the
// build-tagged file) because the capture harness is invisible to the
// default build.
const (
	scenarioFrameCount  = 100
	scenarioHeartbeatMs = 50
)

// scenarioPoolConfigParity is the default-build twin of the capture
// harness's scenarioPoolConfig. Identical shape — see metricsParityShim
// duplication note above. Held here to keep the scenario byte-identical
// between capture and replay.
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

// runMetricsV13ScenarioParity is the default-build twin of the capture
// harness's runMetricsV13Scenario. Single sub, 100 frames, one
// heartbeat, graceful shutdown. Returns the registry so the test can
// introspect counter values.
// SCENARIO DRIVER (single source of truth — keep in sync with
// golden_metrics_capture_test.go's runMetricsV13Scenario).
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

	// Wait for all data frames to drain.
	if werr := waitForCounterValue(reg, "walera_events_sent_total", "type", "exact", float64(scenarioFrameCount), 2*time.Second); werr != nil {
		return reg, werr
	}

	// Wait for exactly one heartbeat to fire.
	time.Sleep(scenarioPoolConfigParity().HeartbeatInterval + 50*time.Millisecond)

	// Graceful shutdown.
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

// waitForCounterValue polls reg.Gatherer().Gather() until the named
// counter family's (labelName=labelValue) child reaches `want`. Returns
// nil on success, an error on timeout.
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

// errMetricsParityTimeout fires when waitForCounterValue exhausts its
// budget.
var errMetricsParityTimeout = &parityError{msg: "metrics_parity: counter did not reach target within budget"}

// parityError is a local sentinel-style error type.
type parityError struct{ msg string }

func (e *parityError) Error() string { return e.msg }

// metricHasLabel returns true when the metric's labels include
// (name=value).
func metricHasLabel(m *dto.Metric, name, value string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name && lp.GetValue() == value {
			return true
		}
	}
	return false
}

// readCounterValue walks the gather output for the named counter
// family, returns the value of the metric child whose label set EXACTLY
// matches `want` (every key and value in want must be present, and the
// metric must have no extra labels beyond want's keys).
// "Exact match" semantics: rejecting metrics with extra labels prevents
// a regression where someone adds a new label dimension to an existing
// counter without updating the fixture — that would silently make the
// label-subset check pass while emitting under a new label space.
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

// readHistogramSampleCount walks the gather output for the named
// histogram family. For histograms the family is unlabelled, so
// we take the first metric child.
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

// labelsMatchExact returns true when the metric's labels are an EXACT
// match for want — every key in want is present with the matching
// value, AND the metric has no extra labels beyond want's keys.
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
