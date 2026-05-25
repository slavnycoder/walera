// Package sse — pool_panic_test.go covers the NewPool construction
// gate: each required Deps field must panic with the exact format
// "sse.NewPool: Deps.<Field> is required".
package sse

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestNewPool_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *PoolDeps)
		wantMsg string
	}{
		{
			name:    "Encoder",
			mutate:  func(d *PoolDeps) { d.Encoder = nil },
			wantMsg: "sse.NewPool: Deps.Encoder is required",
		},
		{
			name:    "Metrics",
			mutate:  func(d *PoolDeps) { d.Metrics = nil },
			wantMsg: "sse.NewPool: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := PoolDeps{
				Encoder: fakeEncoder{},
				Metrics: newFakeMetrics(),
				Logger:  zerolog.Nop(),
			}
			tc.mutate(&deps)
			assertSSEPanic(t, tc.wantMsg, func() {
				_ = NewPool(PoolConfig{PoolFactor: 1, SubQueueSize: 4}, deps)
			})
		})
	}
}

// assertSSEPanic mirrors require.PanicsWithValue without pulling in
// testify. Shared with handler_panic_test.go via package-local scope.
func assertSSEPanic(t *testing.T, want any, fn func()) {
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
