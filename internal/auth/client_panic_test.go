package auth

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

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

			assertPanicsWithValue(t, tc.wantMsg, func() {
				_ = New(validClientConfig(), deps)
			})
		})
	}
}
