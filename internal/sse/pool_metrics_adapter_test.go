package sse

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/walera/walera/internal/metrics"
)

func TestNewPoolMetricsAdapter_NilPanics(t *testing.T) {
	t.Parallel()
	const want = "sse.NewPoolMetricsAdapter: registry is required"
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic with %q; got no panic", want)
		}
		got, ok := r.(string)
		if !ok {
			t.Fatalf("panic value: got %v (%T); want string %q", r, r, want)
		}
		if got != want {
			t.Fatalf("panic message: got %q; want %q", got, want)
		}
	}()
	_ = NewPoolMetricsAdapter(nil)
}

func TestPoolMetricsAdapter_ForwardsCounterIncrements(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	a := NewPoolMetricsAdapter(reg)

	a.EventsSentInc("exact")
	a.TxDroppedInc("slow_consumer")
	a.SubscriberDisconnectsInc("shutdown")
	a.SlowClientDropsInc()

	cases := []struct {
		family    string
		labelName string
		labelVal  string
		want      float64
	}{
		{"walera_events_sent_total", "type", "exact", 1},
		{"walera_tx_dropped_total", "reason", "slow_consumer", 1},
		{"walera_subscriber_disconnects_total", "reason", "shutdown", 1},

		{"walera_slow_client_drops_total", "", "", 1},
	}
	for _, c := range cases {
		t.Run(c.family, func(t *testing.T) {
			got := gatherCounterValue(t, reg, c.family, c.labelName, c.labelVal)
			if got != c.want {
				t.Fatalf("%s{%s=%q}: got %v; want %v", c.family, c.labelName, c.labelVal, got, c.want)
			}
		})
	}
}

func TestPoolMetricsAdapter_ForwardsGaugesAndHistograms(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	a := NewPoolMetricsAdapter(reg)

	a.PoolWorkerDirtySubsInc("w-0")
	a.PoolWorkerDirtySubsInc("w-0")
	a.PoolWorkerDirtySubsDec("w-0")
	if got := gatherGaugeValue(t, reg, "walera_pool_worker_dirty_subs", "worker_id", "w-0"); got != 1 {
		t.Fatalf("walera_pool_worker_dirty_subs{worker_id=\"w-0\"}: got %v; want 1", got)
	}

	a.PoolWorkerDirtySubsSet("w-0", 0)
	if got := gatherGaugeValue(t, reg, "walera_pool_worker_dirty_subs", "worker_id", "w-0"); got != 0 {
		t.Fatalf("walera_pool_worker_dirty_subs{worker_id=\"w-0\"}: after Set(0) got %v; want 0", got)
	}

	a.PoolDrainBatchSizeObserve(7)
	a.PoolDrainDurationObserve(0.001)
	a.SubscriberLifetimeObserve(2.5)

	hists := []string{
		"walera_pool_drain_batch_size",
		"walera_pool_drain_duration_seconds",
		"walera_subscriber_lifetime_seconds",
	}
	for _, name := range hists {
		t.Run(name, func(t *testing.T) {
			if got := gatherHistogramSampleCount(t, reg, name); got != 1 {
				t.Fatalf("%s: SampleCount got %d; want 1", name, got)
			}
		})
	}
}

func gatherCounterValue(t *testing.T, reg *metrics.Registry, family, labelName, labelVal string) float64 {
	t.Helper()
	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelName == "" {
				if m.GetCounter() == nil {
					t.Fatalf("%s: child has no counter", family)
				}
				return m.GetCounter().GetValue()
			}
			if labelsContain(m.GetLabel(), labelName, labelVal) {
				if m.GetCounter() == nil {
					t.Fatalf("%s{%s=%q}: child has no counter", family, labelName, labelVal)
				}
				return m.GetCounter().GetValue()
			}
		}
		t.Fatalf("%s: no child with %s=%q", family, labelName, labelVal)
	}
	t.Fatalf("metric family %q absent from gather output", family)
	return 0
}

func gatherGaugeValue(t *testing.T, reg *metrics.Registry, family, labelName, labelVal string) float64 {
	t.Helper()
	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsContain(m.GetLabel(), labelName, labelVal) {
				if m.GetGauge() == nil {
					t.Fatalf("%s{%s=%q}: child has no gauge", family, labelName, labelVal)
				}
				return m.GetGauge().GetValue()
			}
		}
		t.Fatalf("%s: no child with %s=%q", family, labelName, labelVal)
	}
	t.Fatalf("metric family %q absent from gather output", family)
	return 0
}

func gatherHistogramSampleCount(t *testing.T, reg *metrics.Registry, family string) uint64 {
	t.Helper()
	mfs, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		ms := mf.GetMetric()
		if len(ms) != 1 {
			t.Fatalf("%s: expected 1 unlabeled child; got %d", family, len(ms))
		}
		if ms[0].GetHistogram() == nil {
			t.Fatalf("%s: child has no histogram", family)
		}
		return ms[0].GetHistogram().GetSampleCount()
	}
	t.Fatalf("metric family %q absent from gather output", family)
	return 0
}

func labelsContain(pairs []*dto.LabelPair, name, value string) bool {
	for _, p := range pairs {
		if p.GetName() == name && p.GetValue() == value {
			return true
		}
	}
	return false
}
