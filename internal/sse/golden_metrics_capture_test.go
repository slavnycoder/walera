//go:build golden_capture

// Package sse — golden_metrics_capture_test.go regenerates the checked-in
// metrics-parity fixture at scripts/golden/metrics_v13.json.
// Build-tagged with `golden_capture` so it is INVISIBLE to the normal
// `go test./...` run (mirrors the golden_capture_test.go
// pattern for wire fixtures). The fixture is the CONTRACT; the parity
// test (metrics_parity_test.go) replays the same scenario in the
// default build and asserts every (name, labels, delta) tuple matches
// the JSON on disk.
// The capture must be deterministic: SINGLE-SUB scenario per checker
// W-2 (CONTEXT.md §"Metrics-Parity Test Q3.4" verbatim — 1 sub, 100
// frames, exactly 1 heartbeat, 1 graceful shutdown disconnect). Single-
// sub means exactly one heartbeat fires per heartbeat tick and exactly
// one disconnect frame is emitted on shutdown — matching CONTEXT.md
// verbatim.
// Re-run ONLY when the v1.3 counter-delta contract intentionally
// changes:
//
//	go test -tags=golden_capture./internal/sse \
//	  -run TestCaptureMetricsV13Fixture -count=1 -timeout=30s
//
// The harness does NOT touch internal/metrics/registry.go (per checker
// B-2 — that file is owned by plan 18-03; the Help-string update +
// reason="shutdown" pre-touch shipped in plan 18-03 task 1).
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

// captureScenarioName + captureDescription are the on-disk identifiers
// for the fixture. CONTEXT.md §"Metrics-Parity Test Q3.4" + checker W-2
// pin them verbatim — do NOT change without updating the parity test's
// assertions and the SUMMARY documentation.
const (
	captureScenarioName = "v13-single-sub-100-1-1"
	captureDescription  = "1 sub (exact kind), 100 events, exactly 1 heartbeat, 1 graceful shutdown disconnect"
	captureFrameCount   = 100
)

// TestCaptureMetricsV13Fixture runs the deterministic single-sub
// scenario and writes scripts/golden/metrics_v13.json with the resulting
// counter deltas + histogram floor counts.
// On a fresh `prometheus.NewRegistry()` the captured absolute values ARE
// the deltas (no prior state) — same semantic the default-build parity
// test asserts against.
func TestCaptureMetricsV13Fixture(t *testing.T) {
	// Step 1-6: drive the scenario through a fresh registry.
	reg, _, err := runMetricsV13Scenario()
	if err != nil {
		t.Fatalf("runMetricsV13Scenario: %v", err)
	}

	// Step 7: walk the gatherer and extract the four target families.
	fix := fixture{
		ScenarioName: captureScenarioName,
		Description:  captureDescription,
		Deltas:       extractCounterDeltas(t, reg),
		Histograms:   extractHistogramAsserts(t, reg),
	}

	// Step 8: write the JSON to scripts/golden/metrics_v13.json.
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
	// Append a trailing newline so editors and `git diff` are happy.
	buf = append(buf, '\n')

	if err := os.WriteFile(outPath, buf, 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", outPath, err)
	}
	t.Logf("wrote %d bytes to %s (scenario=%s, %d deltas, %d histograms)",
		len(buf), outPath, fix.ScenarioName, len(fix.Deltas), len(fix.Histograms))
}

// metricsFixtureRepoRoot walks up from the test working dir
// (internal/sse) to find the directory containing go.mod. Mirrors
// repoRoot in golden_capture_test.go but renamed to avoid a duplicate
// symbol under the golden_capture tag (both files compile together
// when that tag is set).
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

// metricsParityShim adapts a *metrics.Registry to the unexported
// metricsIface that sse.NewPool consumes. Mirrors the poolMetricsShim in
// cmd/cdc-sse/main.go (we cannot import that — it lives in package main
// and would create a circular import anyway). The shape is identical
// for byte-for-byte parity of emission paths.
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

// runMetricsV13Scenario constructs a fresh *metrics.Registry, builds a
// single-sub pool, sends captureFrameCount frames, waits for exactly one
// heartbeat to fire, then graceful-shuts the pool. Returns the registry
// so the caller (capture harness or parity test) can introspect counter
// values via Gatherer().Gather().
// IMPORTANT: this helper compiles under BOTH the golden_capture tag and
// the default build. The capture harness lives in this file (tagged) but
// metrics_parity_test.go (default build) cannot reach it. We therefore
// duplicate the helper in metrics_parity_test.go's default-build
// companion file — BUT to keep the two implementations in lockstep, the
// canonical implementation is the body inlined into BOTH locations from
// a shared comment block (see "SCENARIO DRIVER" below). Any divergence
// between the two is a parity bug.
// SCENARIO DRIVER (single source of truth — keep in sync with
// metrics_parity_test.go's runMetricsV13Scenario):
//  1. reg = metrics.New(); shim = &metricsParityShim{r: reg}
//  2. pool = NewPool(scenarioPoolConfig(), NewEncoder, shim, nop)
//  3. sub = newFixtureSub("metrics-v13-sub-001", "exact")
//  4. rw = &goldenRespWriter{}; rc = http.NewResponseController(rw)
//  5. doneCh, _ = pool.Attach(sub, nil, rw, rc)
//  6. for i in [0, captureFrameCount): sub.SendForTest(deterministicFrame)
//  7. wait until events_sent_total{exact} == captureFrameCount
//     (the data-drain completion gate — uses Gather() polling)
//  8. wait until subscriber_lifetime_seconds.SampleCount has NOT yet
//     advanced AND HeartbeatInterval + 20ms has elapsed so the worker
//     sweep fires exactly once for the sub
//  9. pool.Shutdown(ctx with 1s budget)
//
// 10. wait for doneCh to close (drainShutdown completed)
// 11. return reg
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

	// Deterministic 24-byte frame payload — content is irrelevant for
	// counter parity, what matters is the per-frame EventsSentInc.
	frame := []byte("event: tx\ndata: {\"k\":1}\n\n")
	for i := 0; i < captureFrameCount; i++ {
		// SendForTest pushes through the wired pool sendFunc. Returns
		// false on queue full — we slow down briefly and retry, since
		// the queue size is 32 and we are sending 100 frames.
		for !sub.SendForTest(frame) {
			time.Sleep(time.Millisecond)
		}
	}

	// Wait for all 100 data frames to drain (events_sent_total advances).
	if werr := waitForCounter(reg, "walera_events_sent_total", "type", "exact", float64(captureFrameCount), 2*time.Second); werr != nil {
		return reg, rw, werr
	}

	// Wait for the worker's heartbeat ticker to fire exactly once. The
	// scenarioPoolConfig sets HeartbeatInterval=50ms; sleep
	// HeartbeatInterval+50ms (heartbeat fires once around 50ms after
	// attach, well before the 100ms wake-up). The wait is bounded by the
	// next-frame margin — we do NOT send any further frames after this
	// point, so the heartbeat sweep finds lastWriteAt stale.
	time.Sleep(scenarioPoolConfig().HeartbeatInterval + 50*time.Millisecond)

	// Graceful shutdown. The single sub receives the shutdown frame and
	// drainShutdown emits SubscriberLifetimeObserve + SubscriberDisconnectsInc(shutdown).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if serr := p.Shutdown(ctx); serr != nil {
		return reg, rw, serr
	}

	// Wait for doneCh to confirm drainShutdown completed.
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return reg, rw, errShutdownTimeout
	}

	return reg, rw, nil
}

// scenarioPoolConfig is the PoolConfig used by BOTH the capture harness
// and the default-build parity test. Held in one place so the two
// scenarios are byte-identical by construction.
func scenarioPoolConfig() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          32,
		MaxWaitMs:             2,
		DrainThresholdSubs:    1000, // force timer-driven drain (single sub)
		HeartbeatInterval:     50 * time.Millisecond,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
		MaxBatchBytesPerSub:   1024 * 1024, // generous to keep 100 frames in one buffer
	}
}

// waitForCounter polls reg.Gatherer().Gather() every 1ms until the named
// counter family's matching (label_name=label_value) child reaches
// `want`. Returns nil on success, an error on timeout.
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

// errCounterWaitTimeout fires when waitForCounter exhausts its budget.
var errCounterWaitTimeout = &captureError{msg: "metrics_v13_capture: counter did not reach target within budget"}

// captureError is a local sentinel-style error type so the harness can
// distinguish its own timeouts from other errors during root-cause
// analysis.
type captureError struct{ msg string }

func (e *captureError) Error() string { return e.msg }

// labelsContain returns true when the metric's labels include
// (name=value).
func labelsContain(m *dto.Metric, name, value string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name && lp.GetValue() == value {
			return true
		}
	}
	return false
}

// extractCounterDeltas walks the gatherer for the four counter
// families and emits one counterDelta per (family, label-set) child
// with a non-zero value. The output is sorted (by name, then by label
// pairs) for byte-stable JSON.
// Tracked families:
//   - walera_events_sent_total{type}
//   - walera_subscriber_disconnects_total{reason}
//   - walera_tx_dropped_total{reason}
//
// (walera_subscriber_lifetime_seconds is a Histogram, not a Counter — it
// goes into extractHistogramAsserts.)
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
				// Skip pre-touched zero series — the fixture locks
				// non-zero deltas only.
				continue
			}
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			out = append(out, counterDelta{Name: name, Labels: labels, Delta: v})
		}
	}
	// Sort for byte-stable JSON output (name → label pairs).
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return labelMapKey(out[i].Labels) < labelMapKey(out[j].Labels)
	})
	return out
}

// extractHistogramAsserts emits one histogramAssert per target histogram
// family with `MinCount = sample_count` from the scenario. The parity
// test asserts `actual >= MinCount` — providing a floor rather than an
// exact match because histogram timing is wall-clock-dependent and
// would flake on slow CI runners.
// Tracked histogram family:
//   - walera_subscriber_lifetime_seconds (one observation per disconnect)
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

// labelMapKey returns a sortable canonical string of the label map.
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
