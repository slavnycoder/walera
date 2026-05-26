package metrics

import (
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// TestRegistry_TxFanOutWork_PreTouched asserts that walera_tx_fan_out_work is
// present in Gather() output from a freshly constructed Registry (pre-touch
// confirmed — no gap from t=0) and that TxFanOutWork() returns a non-nil
// prometheus.Histogram on which Observe can be called.
func TestRegistry_TxFanOutWork_PreTouched(t *testing.T) {
	t.Parallel()
	r := New()

	h := r.TxFanOutWork()
	if h == nil {
		t.Fatal("TxFanOutWork() returned nil")
	}
	// Verify the accessor returns a usable histogram (should not panic).
	h.Observe(42)

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var found *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "walera_tx_fan_out_work" {
			found = mf
			break
		}
	}
	if found == nil {
		t.Fatal("walera_tx_fan_out_work not in Gather() output — pre-touch or registration missing")
	}

	// The series must be present even before any caller-driven Observe (pre-touch).
	// After our Observe(42) above the histogram should have at least one sample.
	ms := found.GetMetric()
	if len(ms) == 0 {
		t.Fatal("walera_tx_fan_out_work: no Metric children")
	}
	if ms[0].GetHistogram() == nil {
		t.Fatal("walera_tx_fan_out_work: not a Histogram")
	}
}

// TestRegistry_TxFanOutWork_Buckets asserts the histogram uses the expected
// extended bucket set aligned with the plan spec (tail extended to 50000 to
// cover work = changes × subscribers which can exceed pure fan-out).
func TestRegistry_TxFanOutWork_Buckets(t *testing.T) {
	t.Parallel()
	r := New()

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var found *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "walera_tx_fan_out_work" {
			found = mf
			break
		}
	}
	if found == nil {
		t.Fatal("walera_tx_fan_out_work not in Gather() output")
	}
	ms := found.GetMetric()
	if len(ms) == 0 {
		t.Fatal("walera_tx_fan_out_work: no Metric children")
	}
	h := ms[0].GetHistogram()
	if h == nil {
		t.Fatal("walera_tx_fan_out_work: not a Histogram")
	}

	want := []float64{1, 5, 25, 100, 500, 2500, 10000, 50000}
	got := make([]float64, 0, len(h.GetBucket()))
	for _, b := range h.GetBucket() {
		got = append(got, b.GetUpperBound())
	}
	if len(got) != len(want) {
		t.Fatalf("walera_tx_fan_out_work bucket count: got %d %v; want %d %v",
			len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("walera_tx_fan_out_work bucket[%d]: got %v; want %v", i, got[i], want[i])
		}
	}
}

func TestRegistry_GatherIncludesAllMetrics(t *testing.T) {
	t.Parallel()

	r := New()

	r.SubscribersActive("exact").Set(1)
	r.SubscribersActive("wildcard").Inc()
	r.EventsSent("exact").Inc()
	r.EventsSent("wildcard").Inc()
	r.TxDropped("slow_consumer").Inc()
	r.TxDropped("tx_too_large").Inc()
	r.TxDropped("multi_root").Inc()
	r.SubscriberDisconnects("slow_consumer").Inc()
	r.SubscriberDisconnects("client_closed").Inc()
	r.RouteLookupDuration().Observe(0.001)

	r.AuthRequests("ok").Inc()
	r.AuthRequestDuration().Observe(0.001)
	r.AuthBreakerState().Set(0)
	r.AuthBreakerStaleSubs().Set(0)
	r.LimitRejected("global_concurrent").Inc()
	r.PGConnectionStatus().Set(0)

	r.WALStandbyACKFailures().Inc()
	r.PoolWorkerDirtySubs("0").Set(0)

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() returned error: %v", err)
	}

	got := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		got = append(got, mf.GetName())
	}
	sort.Strings(got)

	wantWalera := []string{
		"walera_auth_breaker_stale_subscribers",
		"walera_auth_circuit_breaker_state",
		"walera_auth_refresh_total",
		"walera_auth_request_duration_seconds",
		"walera_auth_requests_total",
		"walera_events_sent_total",
		"walera_limit_rejected_total",
		"walera_pg_connection_status",
		"walera_pg_reconnects_total",

		"walera_pool_drain_batch_size",
		"walera_pool_drain_duration_seconds",
		"walera_pool_worker_dirty_subs",
		"walera_route_lookup_duration_seconds",
		"walera_routing_fan_out",
		"walera_routing_index_size",
		"walera_slow_client_drops_total",
		"walera_subscriber_disconnects_total",
		"walera_subscriber_lifetime_seconds",
		"walera_subscriber_queue_depth",
		"walera_subscribers_active",
		"walera_tx_dropped_total",
		"walera_tx_fan_out_work",
		"walera_wal_decode_duration_seconds",
		"walera_wal_lsn_lag_bytes",
		"walera_wal_standby_ack_failures_total",
		"walera_wal_tx_size_changes",
	}
	sort.Strings(wantWalera)

	gotWalera := make([]string, 0, len(got))
	for _, n := range got {
		if strings.HasPrefix(n, "walera_") {
			gotWalera = append(gotWalera, n)
		}
	}
	sort.Strings(gotWalera)

	if len(gotWalera) != len(wantWalera) {
		t.Fatalf("walera_* metric family count: got %d %v; want %d %v",
			len(gotWalera), gotWalera, len(wantWalera), wantWalera)
	}
	for i := range wantWalera {
		if gotWalera[i] != wantWalera[i] {
			t.Errorf("walera_* metric family[%d]: got %q; want %q", i, gotWalera[i], wantWalera[i])
		}
	}
}

func TestRegistry_OBS01Inventory(t *testing.T) {
	t.Parallel()

	r := New()
	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	phase4Required := []string{
		"walera_pg_reconnects_total",
		"walera_pg_connection_status",
		"walera_wal_lsn_lag_bytes",
		"walera_wal_tx_size_changes",
		"walera_wal_decode_duration_seconds",
		"walera_routing_fan_out",
		"walera_routing_index_size",
		"walera_subscriber_queue_depth",
		"walera_subscriber_lifetime_seconds",
		"walera_auth_refresh_total",
	}
	for _, n := range phase4Required {
		if !names[n] {
			t.Errorf("metric family missing from Gather(): %q", n)
		}
	}

	foundGo := false
	for n := range names {
		if strings.HasPrefix(n, "go_") {
			foundGo = true
			break
		}
	}
	if !foundGo {
		t.Error("no go_* metric found — collectors.NewGoCollector not registered")
	}

	foundProcess := false
	for n := range names {
		if strings.HasPrefix(n, "process_") {
			foundProcess = true
			break
		}
	}
	if !foundProcess {
		t.Error("no process_* metric found — collectors.NewProcessCollector not registered")
	}

	for _, want := range []string{"exact", "wildcard"} {
		_ = r.RoutingIndexSize(want)
		_ = r.subscriberQueueDepth.WithLabelValues(want)
	}
	for _, want := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		_ = r.AuthRefresh(want)
	}
}

func TestRegistry_WALStandbyACKFailures_Registered(t *testing.T) {
	t.Parallel()
	r := New()
	if r.WALStandbyACKFailures() == nil {
		t.Fatal("WALStandbyACKFailures() returned nil")
	}
	r.WALStandbyACKFailures().Inc()
	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	found := false
	for _, fam := range families {
		if fam.GetName() == "walera_wal_standby_ack_failures_total" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("series walera_wal_standby_ack_failures_total not in Gather() output")
	}
}

func TestRegistry_PoolMetricsRegistered(t *testing.T) {
	t.Parallel()
	r := New()

	r.PoolWorkerDirtySubs("0").Set(0)

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	byName := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		byName[mf.GetName()] = mf
	}

	for _, name := range []string{
		"walera_pool_worker_dirty_subs",
		"walera_pool_drain_batch_size",
		"walera_pool_drain_duration_seconds",
	} {
		if _, ok := byName[name]; !ok {
			t.Errorf("metric family %q missing from Gather() output", name)
		}
	}

	checkBuckets := func(t *testing.T, name string, want []float64) {
		t.Helper()
		mf, ok := byName[name]
		if !ok {
			t.Fatalf("family %q missing", name)
		}
		ms := mf.GetMetric()
		if len(ms) == 0 {
			t.Fatalf("family %q has no Metric children (histogram should surface from registration)", name)
		}
		h := ms[0].GetHistogram()
		if h == nil {
			t.Fatalf("family %q is not a Histogram", name)
		}
		got := make([]float64, 0, len(h.GetBucket()))
		for _, b := range h.GetBucket() {
			got = append(got, b.GetUpperBound())
		}
		if len(got) != len(want) {
			t.Fatalf("family %q bucket count: got %d %v; want %d %v",
				name, len(got), got, len(want), want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("family %q bucket[%d]: got %v; want %v", name, i, got[i], want[i])
			}
		}
	}
	checkBuckets(t, "walera_pool_drain_batch_size",
		[]float64{1, 4, 16, 64, 256, 1024})
	checkBuckets(t, "walera_pool_drain_duration_seconds",
		[]float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0})
}

func TestRegistry_DisconnectsShutdownLabelPreTouched(t *testing.T) {
	t.Parallel()
	r := New()

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var family *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "walera_subscriber_disconnects_total" {
			family = mf
			break
		}
	}
	if family == nil {
		t.Fatal("walera_subscriber_disconnects_total family missing from Gather() output")
	}

	foundShutdown := false
	for _, m := range family.GetMetric() {
		labels := m.GetLabel()
		if len(labels) != 1 || labels[0].GetName() != "reason" {
			continue
		}
		if labels[0].GetValue() != "shutdown" {
			continue
		}
		foundShutdown = true
		if got := m.GetCounter().GetValue(); got != 0 {
			t.Errorf("disconnects_total{reason=shutdown}: got value %v; want 0", got)
		}
		break
	}
	if !foundShutdown {

		var present []string
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" {
					present = append(present, l.GetValue())
				}
			}
		}
		t.Errorf("disconnects_total{reason=shutdown} not pre-touched (present reasons: %v)", present)
	}

	wantReasons := map[string]bool{
		"slow_consumer": false,
		"tx_too_large":  false,
		"client_closed": false,
		"shutdown":      false,
	}
	for _, m := range family.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "reason" {
				if _, ok := wantReasons[l.GetValue()]; ok {
					wantReasons[l.GetValue()] = true
				}
			}
		}
	}
	for reason, present := range wantReasons {
		if !present {
			t.Errorf("documented disconnect reason %q not pre-touched in New()", reason)
		}
	}
}
