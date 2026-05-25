package limits

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func validLimitsDeps() Deps {
	return Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	}
}

func validLimitsConfig() Config {
	return Config{
		GlobalConcurrent:     10,
		PerUserConcurrentMax: 2,
		PerUserRatePerSecond: 1,
		PerUserBurst:         2,
		PreAuthRatePerSecond: 1,
		PreAuthBurst:         2,
		SweepInterval:        60 * time.Second,
		SweepIdleThreshold:   5 * time.Minute,
	}
}

func TestNewLimits_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *Deps)
		wantMsg string
	}{
		{
			name:    "Metrics",
			mutate:  func(d *Deps) { d.Metrics = nil },
			wantMsg: "limits.New: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validLimitsDeps()
			tc.mutate(&deps)
			assertLimitsPanicsWithValue(t, tc.wantMsg, func() {
				_ = New(validLimitsConfig(), deps)
			})
		})
	}
}

func assertLimitsPanicsWithValue(t *testing.T, want any, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic with value %v; got no panic", want)
		}
		if r != want {
			t.Fatalf("panic value: got %v (%T); want %v (%T)", r, r, want, want)
		}
	}()
	fn()
}
