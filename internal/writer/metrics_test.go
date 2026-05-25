// Package writer — metrics_test.go covers the private WriterRegistry that
// owns the Prometheus metric set. Tests assert counter / gauge values via
// the gatherer's Gather() output and prometheus/testutil helpers without
// touching the global default registry.
package writer

import (
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// helperFamilies returns metric-family names from the registry in sorted
// order for stable assertions.
func helperFamilies(t *testing.T, r *WriterRegistry) []string {
	t.Helper()
	mf, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make([]string, 0, len(mf))
	for _, m := range mf {
		names = append(names, m.GetName())
	}
	sort.Strings(names)
	return names
}

// findFamily looks up a metric family by name in the gathered output.
// Returns nil when not present.
func findFamily(t *testing.T, r *WriterRegistry, name string) *dto.MetricFamily {
	t.Helper()
	mf, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, m := range mf {
		if m.GetName() == name {
			return m
		}
	}
	return nil
}

// metricValueByLabels returns the float value of a metric in family `name`
// matching every label in `want`. Returns (-1, false) when no series matches.
func metricValueByLabels(t *testing.T, r *WriterRegistry, name string, want map[string]string) (float64, bool) {
	t.Helper()
	fam := findFamily(t, r, name)
	if fam == nil {
		return -1, false
	}
NEXT:
	for _, m := range fam.GetMetric() {
		labels := map[string]string{}
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		for k, v := range want {
			if labels[k] != v {
				continue NEXT
			}
		}
		switch fam.GetType() {
		case dto.MetricType_COUNTER:
			return m.GetCounter().GetValue(), true
		case dto.MetricType_GAUGE:
			return m.GetGauge().GetValue(), true
		default:
			return -1, false
		}
	}
	return -1, false
}

func TestNewRegistry_RegistersExpectedMetrics(t *testing.T) {
	r := NewRegistry()

	// Materialise the Vec families that don't have statically-known label
	// values so the gather output includes them. Prometheus does not emit
	// CounterVec/GaugeVec children until WithLabelValues materialises them
	// (the pre-touch pattern used in internal/metrics for known-label series).
	r.TxTotal("steady", "orders")
	r.RowsTotal("steady", "orders", "insert", 0)
	r.SetActiveScenario("steady")
	r.SetCommitRate("steady", 1)

	names := helperFamilies(t, r)

	// All writer_* families must be present in the gather output now.
	want := []string{
		"writer_tx_total",
		"writer_rows_total",
		"writer_commit_rate",
		"writer_errors_total", // pre-touched in NewRegistry for known reasons.
		"writer_scenario",
		"writer_overload_events_total",
		"writer_pool_busy",
		"writer_pool_idle",
	}
	have := map[string]bool{}
	for _, n := range names {
		have[n] = true
	}
	for _, w := range want {
		if !have[w] {
			t.Errorf("metric family %q not registered (have: %v)", w, names)
		}
	}

	// Smoke-check that the Go runtime collector is registered.
	if !have["go_goroutines"] {
		t.Errorf("go_goroutines missing — Go runtime collector not registered")
	}
}

// TestNewRegistry_PreTouchesErrorReasons ensures every classify() label
// (pg_conn|pg_constraint|pg_other|tx_timeout) is visible in /metrics from
// t=0 without waiting for the first error — required for dashboards / alert
// rules that reference specific reason labels.
func TestNewRegistry_PreTouchesErrorReasons(t *testing.T) {
	r := NewRegistry()
	for _, reason := range []string{"pg_conn", "pg_constraint", "pg_other", "tx_timeout"} {
		if v, ok := metricValueByLabels(t, r, "writer_errors_total",
			map[string]string{"reason": reason}); !ok || v != 0 {
			t.Errorf("reason=%s not pre-touched (got %v, ok=%v)", reason, v, ok)
		}
	}
}

func TestTxTotal_Labels(t *testing.T) {
	r := NewRegistry()
	r.TxTotal("steady", "orders")
	r.TxTotal("steady", "orders")
	r.TxTotal("steady", "orders")
	r.TxTotal("smoke", "devices")

	if v, ok := metricValueByLabels(t, r, "writer_tx_total",
		map[string]string{"scenario": "steady", "target": "orders"}); !ok || v != 3 {
		t.Errorf("steady/orders = %v (ok=%v), want 3", v, ok)
	}
	if v, ok := metricValueByLabels(t, r, "writer_tx_total",
		map[string]string{"scenario": "smoke", "target": "devices"}); !ok || v != 1 {
		t.Errorf("smoke/devices = %v (ok=%v), want 1", v, ok)
	}
}

func TestRowsTotal_Labels(t *testing.T) {
	r := NewRegistry()
	r.RowsTotal("steady", "orders", "insert", 5)

	if v, ok := metricValueByLabels(t, r, "writer_rows_total",
		map[string]string{"scenario": "steady", "target": "orders", "op": "insert"}); !ok || v != 5 {
		t.Errorf("rows_total = %v (ok=%v), want 5", v, ok)
	}
}

// TestSetCommitRate_ResetsOnScenarioSwitch documents the CONTEXT-locked
// decision: SetActiveScenario calls commitRate.Reset() so that the old-scenario
// series is REMOVED from the gather output (not merely set to 0). The next
// SetCommitRate(new, value) call recreates the series for the new scenario.
func TestSetCommitRate_ResetsOnScenarioSwitch(t *testing.T) {
	r := NewRegistry()
	r.SetCommitRate("smoke", 5)
	if v, ok := metricValueByLabels(t, r, "writer_commit_rate",
		map[string]string{"scenario": "smoke"}); !ok || v != 5 {
		t.Fatalf("smoke = %v (ok=%v), want 5", v, ok)
	}

	r.SetActiveScenario("steady")
	r.SetCommitRate("steady", 100)

	// After Reset() + SetCommitRate for steady, the smoke series should be
	// gone from the gather output entirely.
	if v, ok := metricValueByLabels(t, r, "writer_commit_rate",
		map[string]string{"scenario": "smoke"}); ok {
		t.Errorf("smoke series still present after Reset() — value=%v (expected removed)", v)
	}
	if v, ok := metricValueByLabels(t, r, "writer_commit_rate",
		map[string]string{"scenario": "steady"}); !ok || v != 100 {
		t.Errorf("steady = %v (ok=%v), want 100", v, ok)
	}
}

func TestSetActiveScenario(t *testing.T) {
	r := NewRegistry()

	r.SetActiveScenario("steady")
	if v, ok := metricValueByLabels(t, r, "writer_scenario",
		map[string]string{"scenario": "steady"}); !ok || v != 1 {
		t.Errorf("steady = %v (ok=%v), want 1", v, ok)
	}

	r.SetActiveScenario("spike")
	// Old steady series removed (Reset semantics).
	if _, ok := metricValueByLabels(t, r, "writer_scenario",
		map[string]string{"scenario": "steady"}); ok {
		t.Errorf("steady series still present after switch to spike")
	}
	if v, ok := metricValueByLabels(t, r, "writer_scenario",
		map[string]string{"scenario": "spike"}); !ok || v != 1 {
		t.Errorf("spike = %v (ok=%v), want 1", v, ok)
	}
}

func TestErrors_Labels(t *testing.T) {
	r := NewRegistry()
	r.Errors("pg_conn")
	r.Errors("pg_conn")

	if v, ok := metricValueByLabels(t, r, "writer_errors_total",
		map[string]string{"reason": "pg_conn"}); !ok || v != 2 {
		t.Errorf("pg_conn = %v (ok=%v), want 2", v, ok)
	}
}

func TestOverload(t *testing.T) {
	r := NewRegistry()
	r.Overload()
	r.Overload()
	r.Overload()

	fam := findFamily(t, r, "writer_overload_events_total")
	if fam == nil {
		t.Fatalf("family writer_overload_events_total not found")
	}
	got := fam.GetMetric()[0].GetCounter().GetValue()
	if got != 3 {
		t.Errorf("overload = %v, want 3", got)
	}
}

func TestSetPoolStats(t *testing.T) {
	r := NewRegistry()
	r.SetPoolStats(7, 1)

	if got := findFamily(t, r, "writer_pool_busy").GetMetric()[0].GetGauge().GetValue(); got != 7 {
		t.Errorf("pool_busy = %v, want 7", got)
	}
	if got := findFamily(t, r, "writer_pool_idle").GetMetric()[0].GetGauge().GetValue(); got != 1 {
		t.Errorf("pool_idle = %v, want 1", got)
	}
}

func TestUptime(t *testing.T) {
	r := NewRegistry()
	time.Sleep(10 * time.Millisecond)
	if got := r.Uptime(); got < 10*time.Millisecond {
		t.Errorf("uptime = %v, want >= 10ms", got)
	}
}

// TestNewRegistry_HelpStringsPresent ensures every writer_* family has a
// non-empty Help string (operator-readability invariant).
func TestNewRegistry_HelpStringsPresent(t *testing.T) {
	r := NewRegistry()
	mf, _ := r.Gatherer().Gather()
	for _, m := range mf {
		if !strings.HasPrefix(m.GetName(), "writer_") {
			continue
		}
		if strings.TrimSpace(m.GetHelp()) == "" {
			t.Errorf("%s missing Help string", m.GetName())
		}
	}
}
