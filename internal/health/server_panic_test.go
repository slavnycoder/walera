package health

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

type panicTestPgChecker struct{}

func (panicTestPgChecker) CheckPG(_ context.Context) error { return errors.New("stub") }

func validHealthDeps() Deps {
	return Deps{
		Logger:    zerolog.Nop(),
		PgChecker: panicTestPgChecker{},
		Metrics:   metrics.New(),
	}
}

func TestNewHealth_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *Deps)
		wantMsg string
	}{
		{
			name:    "PgChecker",
			mutate:  func(d *Deps) { d.PgChecker = nil },
			wantMsg: "health.New: Deps.PgChecker is required",
		},
		{
			name:    "Metrics",
			mutate:  func(d *Deps) { d.Metrics = nil },
			wantMsg: "health.New: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validHealthDeps()
			tc.mutate(&deps)
			assertHealthPanicsWithValue(t, tc.wantMsg, func() {
				_ = New(Config{}, deps)
			})
		})
	}
}

func assertHealthPanicsWithValue(t *testing.T, want any, fn func()) {
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
