package metrics

import (
	"slices"
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestRegistry_ExactlyFiveFamilies(t *testing.T) {
	r := New()
	r.SubscribersActive("exact").Set(1)
	r.EventsSent("exact").Inc()
	r.SubscriberDisconnects("client_closed").Inc()
	r.AuthRequests("ok").Inc()
	r.PGConnectionStatus().Set(1)
	fams, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var walera []string
	for _, f := range fams {
		if strings.HasPrefix(f.GetName(), "walera_") {
			walera = append(walera, f.GetName())
		}
	}
	sort.Strings(walera)
	want := []string{
		"walera_auth_requests_total",
		"walera_events_sent_total",
		"walera_pg_connection_status",
		"walera_subscriber_disconnects_total",
		"walera_subscribers_active",
	}
	if !slices.Equal(walera, want) {
		t.Fatalf("walera families = %v; want %v", walera, want)
	}
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
