package auth

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func validBreakerDeps() BreakerDeps {
	return BreakerDeps{
		Prober:  proberFunc(func(_ context.Context) error { return nil }),
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	}
}

func validBreakerConfig() BreakerConfig {
	return BreakerConfig{
		WindowBuckets:        30,
		BucketSeconds:        1,
		FailureRateThreshold: 0.5,
		DebounceFloor:        20,
	}
}

func TestNewBreaker_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *BreakerDeps)
		wantMsg string
	}{
		{
			name:    "Prober",
			mutate:  func(d *BreakerDeps) { d.Prober = nil },
			wantMsg: "auth.NewBreaker: Deps.Prober is required",
		},
		{
			name:    "Metrics",
			mutate:  func(d *BreakerDeps) { d.Metrics = nil },
			wantMsg: "auth.NewBreaker: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validBreakerDeps()
			tc.mutate(&deps)
			assertPanicsWithValue(t, tc.wantMsg, func() {
				_ = NewBreaker(validBreakerConfig(), deps)
			})
		})
	}
}

func assertPanicsWithValue(t *testing.T, want any, fn func()) {
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
