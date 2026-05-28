package limits

import (
	"bytes"
	"net"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func gatherCounter(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func gatherHasSeries(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) bool {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return true
			}
		}
	}
	return false
}

func matchLabel(m *dto.Metric, key, val string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key && lp.GetValue() == val {
			return true
		}
	}
	return false
}

func mkLimits(t *testing.T, globalCap, perUserCap int) *Limits {
	t.Helper()
	mc := metrics.New()
	return New(Config{
		GlobalConcurrent:     globalCap,
		PerUserConcurrentMax: perUserCap,
	}, Deps{Logger: zerolog.Nop(), Metrics: mc})
}

func mkLimitsWithLogCapture(t *testing.T, globalCap, perUserCap int) (*Limits, *bytes.Buffer) {
	t.Helper()
	mc := metrics.New()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	return New(Config{
		GlobalConcurrent:     globalCap,
		PerUserConcurrentMax: perUserCap,
	}, Deps{Logger: logger, Metrics: mc}), &buf
}

func TestLimits_AcquireGlobal_Succeeds_BelowCap(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 3, 2)
	for i := 0; i < 3; i++ {
		if !l.AcquireGlobal() {
			t.Fatalf("AcquireGlobal #%d: got false; want true", i)
		}
	}
	if v := gatherCounter(t, l.mc, "walera_limit_rejected_total", "kind", "global_concurrent"); v != 0 {
		t.Errorf("limit_rejected_total{kind=global_concurrent}: got %v; want 0", v)
	}
}

func TestLimits_AcquireGlobal_FailsAtCap(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 2, 2)
	for i := 0; i < 2; i++ {
		if !l.AcquireGlobal() {
			t.Fatalf("AcquireGlobal #%d: got false; want true", i)
		}
	}
	if l.AcquireGlobal() {
		t.Fatal("AcquireGlobal #3: got true; want false (cap exhausted)")
	}
	if v := gatherCounter(t, l.mc, "walera_limit_rejected_total", "kind", "global_concurrent"); v != 1 {
		t.Errorf("limit_rejected_total{kind=global_concurrent}: got %v; want 1", v)
	}
}

func TestLimits_AcquireGlobal_RoundTrip(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 1, 2)
	if !l.AcquireGlobal() {
		t.Fatal("first acquire failed")
	}
	l.ReleaseGlobal()
	if !l.AcquireGlobal() {
		t.Fatal("second acquire (after release) failed — slot not reused")
	}
}

func TestLimits_AcquirePerUser_BelowCap(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 2)
	ok, n := l.AcquirePerUser("u1")
	if !ok || n != 1 {
		t.Fatalf("first AcquirePerUser: got (%v, %d); want (true, 1)", ok, n)
	}
	ok, n = l.AcquirePerUser("u1")
	if !ok || n != 2 {
		t.Fatalf("second AcquirePerUser: got (%v, %d); want (true, 2)", ok, n)
	}
}

func TestLimits_AcquirePerUser_AtCap(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 2)
	_, _ = l.AcquirePerUser("u1")
	_, _ = l.AcquirePerUser("u1")
	ok, n := l.AcquirePerUser("u1")
	if ok || n != 2 {
		t.Fatalf("overflow AcquirePerUser: got (%v, %d); want (false, 2)", ok, n)
	}
	if v := gatherCounter(t, l.mc, "walera_limit_rejected_total", "kind", "per_user_concurrent"); v != 1 {
		t.Errorf("limit_rejected_total{kind=per_user_concurrent}: got %v; want 1", v)
	}
}

func TestLimits_AcquirePerUser_DifferentUsersIndependent(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 1)
	if ok, _ := l.AcquirePerUser("u1"); !ok {
		t.Fatal("u1 first acquire failed")
	}
	if ok, _ := l.AcquirePerUser("u2"); !ok {
		t.Fatal("u2 first acquire failed (counters not independent)")
	}
}

func TestLimits_ReleasePerUser_Decrements(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 2)
	_, _ = l.AcquirePerUser("u1")
	_, _ = l.AcquirePerUser("u1")
	l.ReleasePerUser("u1")

	ok, n := l.AcquirePerUser("u1")
	if !ok || n != 2 {
		t.Fatalf("acquire after release: got (%v, %d); want (true, 2)", ok, n)
	}
}

func TestLimits_PreTouchesLimitRejectedSeries(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	for _, k := range []string{"global_concurrent", "per_user_concurrent"} {
		if !gatherHasSeries(t, l.mc, "walera_limit_rejected_total", "kind", k) {
			t.Errorf("walera_limit_rejected_total{kind=%s} missing from Gather() — pre-touch regressed", k)
		}
	}
}

func TestReleaseGlobal_WithoutAcquire_LogsWarn(t *testing.T) {
	t.Parallel()
	l, buf := mkLimitsWithLogCapture(t, 10, 5)

	l.ReleaseGlobal()

	if !strings.Contains(buf.String(), "ReleaseGlobal without prior Acquire") {
		t.Errorf("expected Warn log; got %q", buf.String())
	}

	if !l.AcquireGlobal() {
		t.Error("AcquireGlobal failed after defensive ReleaseGlobal — counter went negative?")
	}
}

func TestLimits_ReleasePerUser_DefensiveWarn(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)

	l.ReleasePerUser("never-seen-user")
}

func mkLimitsWithTrustedProxies(t *testing.T, proxies []string) *Limits {
	t.Helper()
	mc := metrics.New()
	return New(Config{
		GlobalConcurrent:     100,
		PerUserConcurrentMax: 10,
		TrustedProxies:       proxies,
	}, Deps{Logger: zerolog.Nop(), Metrics: mc})
}

func TestLimits_IsTrustedProxy_EmptyAllowlist(t *testing.T) {
	t.Parallel()
	l := mkLimitsWithTrustedProxies(t, nil)
	for _, ip := range []string{"10.0.0.1", "203.0.113.5", "fd00::1"} {
		if l.IsTrustedProxy(net.ParseIP(ip)) {
			t.Errorf("IsTrustedProxy(%q) = true; want false (empty allowlist)", ip)
		}
	}
}

func TestLimits_IsTrustedProxy_IPv4InRange(t *testing.T) {
	t.Parallel()
	l := mkLimitsWithTrustedProxies(t, []string{"10.0.0.0/8"})
	for _, ip := range []string{"10.0.0.5", "10.255.255.255"} {
		if !l.IsTrustedProxy(net.ParseIP(ip)) {
			t.Errorf("IsTrustedProxy(%q) = false; want true", ip)
		}
	}
}

func TestLimits_IsTrustedProxy_IPv4OutOfRange(t *testing.T) {
	t.Parallel()
	l := mkLimitsWithTrustedProxies(t, []string{"10.0.0.0/8"})
	for _, ip := range []string{"203.0.113.5", "11.0.0.1"} {
		if l.IsTrustedProxy(net.ParseIP(ip)) {
			t.Errorf("IsTrustedProxy(%q) = true; want false (out of range)", ip)
		}
	}
}

func TestLimits_IsTrustedProxy_IPv6InRange(t *testing.T) {
	t.Parallel()
	l := mkLimitsWithTrustedProxies(t, []string{"fd00::/8"})
	if !l.IsTrustedProxy(net.ParseIP("fd00::1")) {
		t.Errorf("IsTrustedProxy(fd00::1) = false; want true")
	}
}

func TestLimits_IsTrustedProxy_NilIP(t *testing.T) {
	t.Parallel()
	l := mkLimitsWithTrustedProxies(t, []string{"10.0.0.0/8"})
	if l.IsTrustedProxy(nil) {
		t.Error("IsTrustedProxy(nil) = true; want false")
	}
}

func TestLimits_New_SkipsMalformedCIDR(t *testing.T) {
	t.Parallel()
	mc := metrics.New()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	l := New(Config{
		GlobalConcurrent:     100,
		PerUserConcurrentMax: 10,
		TrustedProxies:       []string{"not-a-cidr", "10.0.0.0/8"},
	}, Deps{Logger: logger, Metrics: mc})
	if !strings.Contains(buf.String(), "trusted_proxies entry failed to parse") {
		t.Errorf("expected Warn log; got %q", buf.String())
	}
	if !l.IsTrustedProxy(net.ParseIP("10.0.0.5")) {
		t.Error("valid CIDR should still parse despite earlier malformed entry")
	}
	if l.IsTrustedProxy(net.ParseIP("203.0.113.5")) {
		t.Error("out-of-range IP should not be trusted")
	}
}
