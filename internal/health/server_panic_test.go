// Package health — server_panic_test.go covers the health.New construction
// gate: every required Deps field panics with the exact message
// "health.New: Deps.<Field> is required" when nil.
package health

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// stubPgChecker / stubAuthChecker implement PgChecker / AuthChecker without
// any side effects — only their non-nil identity matters for the panic
// gate.
type panicTestPgChecker struct{}

func (panicTestPgChecker) CheckPG(_ context.Context) error { return errors.New("stub") }

type panicTestAuthChecker struct{}

func (panicTestAuthChecker) CheckAuth(_ context.Context) error { return nil }

// validHealthDeps returns a fully-populated Deps so each per-field test
// only nils one field.
func validHealthDeps() Deps {
	return Deps{
		Logger:      zerolog.Nop(),
		PgChecker:   panicTestPgChecker{},
		AuthChecker: panicTestAuthChecker{},
		Metrics:     metrics.New(),
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
			name:    "AuthChecker",
			mutate:  func(d *Deps) { d.AuthChecker = nil },
			wantMsg: "health.New: Deps.AuthChecker is required",
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

// assertHealthPanicsWithValue runs fn and asserts that it panicked with a
// value equal to want. Mirrors testify's require.PanicsWithValue without
// taking on the testify dep.
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
