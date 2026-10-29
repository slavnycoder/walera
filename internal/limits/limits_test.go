package limits

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func mkLimits(t *testing.T, globalCap, perUserCap int) *Limits {
	t.Helper()
	return New(Config{
		GlobalConcurrent:     globalCap,
		PerUserConcurrentMax: perUserCap,
	}, Deps{Logger: zerolog.Nop()})
}

func mkLimitsWithLogCapture(t *testing.T, globalCap, perUserCap int) (*Limits, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	return New(Config{
		GlobalConcurrent:     globalCap,
		PerUserConcurrentMax: perUserCap,
	}, Deps{Logger: logger}), &buf
}

func TestLimits_AcquireGlobal_Succeeds_BelowCap(t *testing.T) {
	t.Parallel()
	l := mkLimits(t, 3, 2)
	for i := 0; i < 3; i++ {
		if !l.AcquireGlobal() {
			t.Fatalf("AcquireGlobal #%d: got false; want true", i)
		}
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
