package router

import (
	"testing"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func validBroadcasterDeps() Deps {
	return Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
		Encoder: &stubEncoder{},
	}
}

func validBroadcasterConfig() Config {
	return Config{
		ExactBuffer:     16,
		WildcardBuffer:  16,
		MaxChangesPerTx: 10000,
	}
}

func TestNewBroadcaster_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *Deps)
		wantMsg string
	}{
		{
			name:    "Metrics",
			mutate:  func(d *Deps) { d.Metrics = nil },
			wantMsg: "router.New: Deps.Metrics is required",
		},
		{
			name:    "Encoder",
			mutate:  func(d *Deps) { d.Encoder = nil },
			wantMsg: "router.New: Deps.Encoder is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validBroadcasterDeps()
			tc.mutate(&deps)
			assertPanicsWithValue(t, tc.wantMsg, func() {
				_ = New(validBroadcasterConfig(), deps)
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
