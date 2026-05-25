// Package auth — client_panic_test.go covers the auth.New (Client)
// construction gate: every required Deps field panics with the exact
// message "auth.New: Deps.<Field> is required" when nil. Breaker is
// intentionally NOT required (auth.New substitutes nopBreaker{} for nil)
// — see Deps docstring in client.go.
package auth

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// validClientDeps returns a fully-populated Deps so each per-field test
// only nils one field.
func validClientDeps() Deps {
	return Deps{
		Logger:  zerolog.Nop(),
		Breaker: nopBreaker{},
		Metrics: metrics.New(),
	}
}

func validClientConfig() Config {
	return Config{
		BackendURL:     "http://127.0.0.1:1",
		ServiceToken:   "svc",
		RequestTimeout: time.Second,
	}
}

func TestNewClient_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *Deps)
		wantMsg string
	}{
		{
			name:    "Metrics",
			mutate:  func(d *Deps) { d.Metrics = nil },
			wantMsg: "auth.New: Deps.Metrics is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validClientDeps()
			tc.mutate(&deps)
			// assertPanicsWithValue is the package-local helper defined in
			// breaker_panic_test.go.
			assertPanicsWithValue(t, tc.wantMsg, func() {
				_ = New(validClientConfig(), deps)
			})
		})
	}
}
