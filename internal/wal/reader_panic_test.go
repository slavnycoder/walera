package wal

import (
	"testing"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func validReaderDeps() Deps {
	return Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
	}
}

func TestNewReader_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *Deps)
		wantMsg string
	}{
		{
			name:    "Metrics",
			mutate:  func(d *Deps) { d.Metrics = nil },
			wantMsg: "wal.New: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validReaderDeps()
			tc.mutate(&deps)
			assertWalPanicsWithValue(t, tc.wantMsg, func() {
				_, _ = New(Config{}, deps)
			})
		})
	}
}

func assertWalPanicsWithValue(t *testing.T, want any, fn func()) {
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
