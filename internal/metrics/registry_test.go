package metrics

import (
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// TestRegistry_GatherIncludesAllMetrics constructs a Registry, exercises every
// typed accessor with a representative label so the WithLabelValues child is
// materialized, then asserts that Gatherer().Gather() returns exactly the five
// Phase-2 metric families.
//
// This is the unit gate that locks the metric names and the fact that they
// are observable through the registry's Gather() pipeline (which the
// /metrics endpoint reuses).
func TestRegistry_GatherIncludesAllMetrics(t *testing.T) {
	t.Parallel()

	r := New()

	// Force WithLabelValues registration of one child per family + a Histogram
	// observation. Touching every accessor here is the assertion that the
	// names compile and dispatch as advertised; the Gather() check below
	// covers visibility.
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
	// Touch new CounterVec families so Gather() exposes them.
	// Plain Gauges (state, stale subs, pg conn) and the Histogram are visible
	// from registration without a touch; CounterVec families require at least
	// one WithLabelValues child to appear.
	r.AuthRequests("ok").Inc()
	r.AuthRequestDuration().Observe(0.001)
	r.AuthBreakerState().Set(0)
	r.AuthBreakerStaleSubs().Set(0)
	r.LimitRejected("global_concurrent").Inc()
	r.PGConnectionStatus().Set(0)
	// SEC-11 / F-P2-08 — touch the new no-label counter so the family
	// surfaces in Gather() output.
	r.WALStandbyACKFailures().Inc()

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() returned error: %v", err)
	}

	got := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		got = append(got, mf.GetName())
	}
	sort.Strings(got)

	// walera_* families — these must always be present after New().
	// Go-runtime / process collectors are asserted separately below.
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
		// Pool metric families. The two histograms surface in Gather() from
		// registration; the GaugeVec (walera_pool_worker_dirty_subs) does
		// NOT until a worker_id label is materialised — that pre-touch
		// happens in sse.NewPool, not here (asserted by internal/sse tests
		// + the dedicated registration test below).
		"walera_pool_drain_batch_size",
		"walera_pool_drain_duration_seconds",
		"walera_route_lookup_duration_seconds",
		"walera_routing_fan_out",
		"walera_routing_index_size",
		"walera_slow_client_drops_total",
		"walera_subscriber_disconnects_total",
		"walera_subscriber_lifetime_seconds",
		"walera_subscriber_queue_depth",
		"walera_subscribers_active",
		"walera_tx_dropped_total",
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

// TestRegistry_OBS01Inventory verifies the full observability inventory is
// gathered, including:
//   - all 9 walera_* WAL/routing/auth families,
//   - at least one go_* runtime metric (proves NewGoCollector wired),
//   - at least one process_* metric (proves NewProcessCollector wired).
//
// Critical invariant: NONE of the Go-runtime or process collectors leak to
// prometheus.DefaultRegisterer — the Gatherer() under test is the PRIVATE
// registry; the test would fail if collectors were mis-registered globally
// because they'd be absent here.
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

	// Every name MUST be present after New() (pre-touch in New()
	// materialises labelled-vector children).
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

	// At least one go_* runtime metric must appear (NewGoCollector wired).
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

	// At least one process_* metric must appear (NewProcessCollector wired).
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

	// Pre-touched labels must materialise their series so alert queries that
	// filter on label values (e.g., reason="slow_consumer") have something
	// to scrape from t=0.
	for _, want := range []string{"exact", "wildcard"} {
		_ = r.RoutingIndexSize(want)                     // should not panic
		_ = r.subscriberQueueDepth.WithLabelValues(want) // should not panic
	}
	for _, want := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		_ = r.AuthRefresh(want)
	}
}

// TestRegistry_WALStandbyACKFailures_Registered asserts the no-label
// counter is reachable via WALStandbyACKFailures() and that the series
// surfaces in Gather() output after an increment.
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

// TestRegistry_PoolMetricsRegistered asserts the pool metric families are
// registered with the locked bucket boundaries.
//
//   - walera_pool_worker_dirty_subs (GaugeVec, label worker_id) — surfaces in
//     Gather() only after a worker_id child is materialised. The test
//     materialises one ("0") via the typed accessor before asserting; the
//     production pre-touch lives in sse.NewPool (verified by internal/sse
//     tests).
//   - walera_pool_drain_batch_size (Histogram) — buckets exactly
//     [1, 4, 16, 64, 256, 1024].
//   - walera_pool_drain_duration_seconds (Histogram) — buckets exactly
//     [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0].
func TestRegistry_PoolMetricsRegistered(t *testing.T) {
	t.Parallel()
	r := New()

	// Materialise the GaugeVec child so it appears in Gather() (the prod
	// pre-touch lives in sse.NewPool — out of scope for this package).
	r.PoolWorkerDirtySubs("0").Set(0)

	mfs, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	byName := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		byName[mf.GetName()] = mf
	}

	// Family presence.
	for _, name := range []string{
		"walera_pool_worker_dirty_subs",
		"walera_pool_drain_batch_size",
		"walera_pool_drain_duration_seconds",
	} {
		if _, ok := byName[name]; !ok {
			t.Errorf("metric family %q missing from Gather() output", name)
		}
	}

	// Bucket-list deep equality (CONTEXT-LOCKED; any drift is a regression).
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

// TestRegistry_DisconnectsShutdownLabelPreTouched verifies the B-2
// consolidation: after metrics.New() returns, the walera_subscriber_
// disconnects_total{reason="shutdown"} series is present in Gather() with
// value 0. Dashboards keyed on rate(...{reason="shutdown"}[5m]) must see
// zero (not "no data") before the first shutdown event.
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
		// Surface every present reason for easier debugging.
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

	// Sibling reasons should also be pre-touched (Help string documents
	// the full set: slow_consumer|tx_too_large|client_closed|shutdown).
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
