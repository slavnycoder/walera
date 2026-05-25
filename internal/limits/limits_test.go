package limits

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// --- helpers ---

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

// mkLimits constructs a Limits with caps suited to small unit tests.
func mkLimits(t *testing.T, globalCap, perUserCap int) *Limits {
	t.Helper()
	mc := metrics.New()
	return New(Config{
		GlobalConcurrent:     globalCap,
		PerUserConcurrentMax: perUserCap,
		PerUserRatePerSecond: 1,
		PerUserBurst:         2,
		PreAuthRatePerSecond: 1,
		PreAuthBurst:         2,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
	}, Deps{Logger: zerolog.Nop(), Metrics: mc})
}

// mkLimitsWithLogCapture constructs a Limits whose logger writes to a
// bytes.Buffer the test can inspect — used by SEC-10 / F-P2-07 tests that
// assert on the Warn log line emitted by ReleaseGlobal's default branch.
func mkLimitsWithLogCapture(t *testing.T, globalCap, perUserCap int) (*Limits, *bytes.Buffer) {
	t.Helper()
	mc := metrics.New()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	return New(Config{
		GlobalConcurrent:     globalCap,
		PerUserConcurrentMax: perUserCap,
		PerUserRatePerSecond: 1,
		PerUserBurst:         2,
		PreAuthRatePerSecond: 1,
		PreAuthBurst:         2,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
	}, Deps{Logger: logger, Metrics: mc}), &buf
}

// --- tests ---

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
	// Should be able to acquire again now (was at cap=2 after two acquires).
	ok, n := l.AcquirePerUser("u1")
	if !ok || n != 2 {
		t.Fatalf("acquire after release: got (%v, %d); want (true, 2)", ok, n)
	}
}

func TestLimits_AllowPreAuthRate_TokenBucket(t *testing.T) {
	t.Parallel()
	// rate=1/s, burst=2 → first two succeed; third (within same instant) fails.
	l := mkLimits(t, 100, 100)
	if !l.AllowPreAuthRate("1.1.1.1") {
		t.Fatal("first AllowPreAuthRate: got false; want true")
	}
	if !l.AllowPreAuthRate("1.1.1.1") {
		t.Fatal("second AllowPreAuthRate: got false; want true")
	}
	if l.AllowPreAuthRate("1.1.1.1") {
		t.Fatal("third AllowPreAuthRate: got true; want false (burst exhausted)")
	}
	if v := gatherCounter(t, l.mc, "walera_limit_rejected_total", "kind", "pre_auth_rate"); v != 1 {
		t.Errorf("limit_rejected_total{kind=pre_auth_rate}: got %v; want 1", v)
	}
}

func TestLimits_AllowPerUserRate_TokenBucket(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	if !l.AllowPerUserRate("u1") {
		t.Fatal("first AllowPerUserRate: got false; want true")
	}
	if !l.AllowPerUserRate("u1") {
		t.Fatal("second AllowPerUserRate: got false; want true")
	}
	if l.AllowPerUserRate("u1") {
		t.Fatal("third AllowPerUserRate: got true; want false")
	}
	if v := gatherCounter(t, l.mc, "walera_limit_rejected_total", "kind", "per_user_rate"); v != 1 {
		t.Errorf("limit_rejected_total{kind=per_user_rate}: got %v; want 1", v)
	}
}

func TestLimits_AllowPreAuthRate_DifferentIPsIndependent(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	for _, ip := range []string{"1.1.1.1", "2.2.2.2"} {
		if !l.AllowPreAuthRate(ip) {
			t.Errorf("%s: first call got false; want true", ip)
		}
		if !l.AllowPreAuthRate(ip) {
			t.Errorf("%s: second call got false; want true", ip)
		}
		if l.AllowPreAuthRate(ip) {
			t.Errorf("%s: third call got true; want false", ip)
		}
	}
}

func TestLimits_RunSweeper_RemovesIdleEntries(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	// Seed an entry.
	_ = l.AllowPreAuthRate("1.1.1.1")
	v, ok := l.preAuthRate.Load("1.1.1.1")
	if !ok {
		t.Fatal("seeded entry missing after AllowPreAuthRate")
	}
	v.(*rateEntry).lastSeen.Store(0) // ancient timestamp

	// Cutoff is "now" — any entry with lastSeen<now is removed (which
	// includes the one we just zeroed).
	l.sweep(&l.preAuthRate, time.Now().UnixNano())

	if _, ok := l.preAuthRate.Load("1.1.1.1"); ok {
		t.Fatal("idle entry survived sweep")
	}
}

func TestLimits_RunSweeper_KeepsActiveEntries(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	_ = l.AllowPreAuthRate("1.1.1.1")
	// Cutoff = far in the past → no entries are stale.
	l.sweep(&l.preAuthRate, 0)
	if _, ok := l.preAuthRate.Load("1.1.1.1"); !ok {
		t.Fatal("active entry incorrectly removed by sweep")
	}
}

func TestLimits_PreTouchesLimitRejectedSeries(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	for _, k := range []string{"global_concurrent", "per_user_concurrent", "pre_auth_rate", "per_user_rate"} {
		if !gatherHasSeries(t, l.mc, "walera_limit_rejected_total", "kind", k) {
			t.Errorf("walera_limit_rejected_total{kind=%s} missing from Gather() — pre-touch regressed", k)
		}
	}
}

func TestLimits_PreAuthRetryAfter_Defaults(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	// Absent entry → 1s default.
	if got := l.preAuthRetryAfter("never-seen"); got != time.Second {
		t.Errorf("PreAuthRetryAfter(absent): got %s; want 1s", got)
	}
}

func TestLimits_PerUserRateRetryAfter(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	// Absent entry → default.
	if got := l.perUserRateRetryAfter("never-seen"); got != time.Second {
		t.Errorf("PerUserRateRetryAfter(absent): got %s; want 1s", got)
	}
	// Populate via Allow then call RetryAfter — should be non-negative.
	_ = l.AllowPerUserRate("u1")
	d := l.perUserRateRetryAfter("u1")
	if d < 0 {
		t.Errorf("PerUserRateRetryAfter(populated): got %s; want >= 0", d)
	}
}

func TestLimits_PreAuthRetryAfter_Populated(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	// Exhaust burst to ensure RetryAfter returns a non-zero delay.
	_ = l.AllowPreAuthRate("ip1")
	_ = l.AllowPreAuthRate("ip1")
	_ = l.AllowPreAuthRate("ip1") // rejected
	d := l.preAuthRetryAfter("ip1")
	if d < 0 {
		t.Errorf("PreAuthRetryAfter(exhausted): got %s; want >= 0", d)
	}
}

// TestReleaseGlobal_WithoutAcquire_LogsWarn — SEC-10 / F-P2-07 closure:
// ReleaseGlobal invoked without a prior successful AcquireGlobal logs a
// Warn (mirrors ReleasePerUser's existing Warn at limits.go:123). No
// panic, no negative counter — AcquireGlobal still succeeds afterwards.
func TestReleaseGlobal_WithoutAcquire_LogsWarn(t *testing.T) {
	t.Parallel()
	l, buf := mkLimitsWithLogCapture(t, 10, 5)

	l.ReleaseGlobal() // no prior Acquire

	if !strings.Contains(buf.String(), "ReleaseGlobal without prior Acquire") {
		t.Errorf("expected Warn log; got %q", buf.String())
	}
	// Underflow contract preserved: AcquireGlobal still succeeds.
	if !l.AcquireGlobal() {
		t.Error("AcquireGlobal failed after defensive ReleaseGlobal — counter went negative?")
	}
}

func TestLimits_ReleasePerUser_DefensiveWarn(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 100, 100)
	// Releasing an unknown user must not panic; just logs a Warn.
	l.ReleasePerUser("never-seen-user")
}

// --- Trusted-proxy allowlist ---

// mkLimitsWithTrustedProxies constructs a Limits whose Config carries the
// supplied trusted_proxies CIDRs. Mirrors mkLimits in shape.
func mkLimitsWithTrustedProxies(t *testing.T, proxies []string) *Limits {
	t.Helper()
	mc := metrics.New()
	return New(Config{
		GlobalConcurrent:     100,
		PerUserConcurrentMax: 10,
		PerUserRatePerSecond: 1,
		PerUserBurst:         2,
		PreAuthRatePerSecond: 1,
		PreAuthBurst:         2,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
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
		PerUserRatePerSecond: 1,
		PerUserBurst:         2,
		PreAuthRatePerSecond: 1,
		PreAuthBurst:         2,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
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

func TestLimits_RunSweeper_ExitsOnCtxCancel(t *testing.T) {
	t.Parallel()
	mc := metrics.New()
	l := New(Config{
		GlobalConcurrent:     10,
		PerUserConcurrentMax: 2,
		PerUserRatePerSecond: 1,
		PerUserBurst:         2,
		PreAuthRatePerSecond: 1,
		PreAuthBurst:         2,
		SweepInterval:        20 * time.Millisecond,
		SweepIdleThreshold:   10 * time.Millisecond,
	}, Deps{Logger: zerolog.Nop(), Metrics: mc})

	// Seed an idle entry that the sweeper should remove on its first tick.
	_ = l.AllowPreAuthRate("idle.ip")
	if v, ok := l.preAuthRate.Load("idle.ip"); ok {
		v.(*rateEntry).lastSeen.Store(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.RunSweeper(ctx)
		close(done)
	}()

	// Give the sweeper at least one tick.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("RunSweeper did not exit within 200ms of ctx cancel")
	}

	// Sweeper should have removed the idle entry.
	if _, ok := l.preAuthRate.Load("idle.ip"); ok {
		t.Error("idle entry not removed by RunSweeper across at least one tick")
	}
}
