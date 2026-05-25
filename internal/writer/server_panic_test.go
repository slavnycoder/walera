package writer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

func validServerDeps() ServerDeps {
	reg := NewRegistry()
	lim := rate.NewLimiter(rate.Limit(1), 1)
	var ptr atomic.Pointer[scenarioState]
	ptr.Store(NewScenarioState(NewSmokeScenario(1, 1), time.Now(), 1, 1, []string{"orders"}))
	return ServerDeps{
		Limiter:     lim,
		ScenarioPtr: &ptr,
		Registry:    reg,
		Logger:      zerolog.Nop(),
		Targets:     []string{"orders"},
	}
}

func TestNewServer_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(d *ServerDeps)
		wantMsg string
	}{
		{
			name:    "Limiter",
			mutate:  func(d *ServerDeps) { d.Limiter = nil },
			wantMsg: "writer.NewServer: Deps.Limiter is required",
		},
		{
			name:    "ScenarioPtr",
			mutate:  func(d *ServerDeps) { d.ScenarioPtr = nil },
			wantMsg: "writer.NewServer: Deps.ScenarioPtr is required",
		},
		{
			name:    "Registry",
			mutate:  func(d *ServerDeps) { d.Registry = nil },
			wantMsg: "writer.NewServer: Deps.Registry is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deps := validServerDeps()
			tc.mutate(&deps)
			assertWriterPanicsWithValue(t, tc.wantMsg, func() {
				_ = NewServer(ServerConfig{Addr: ":0"}, deps)
			})
		})
	}
}

func assertWriterPanicsWithValue(t *testing.T, want any, fn func()) {
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
