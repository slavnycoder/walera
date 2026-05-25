package app

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/wal"
)

func TestHTTPServerConstructors(t *testing.T) {
	cfg := AppConfig{}
	cfg.HTTP.Addr = "127.0.0.1:0"
	cfg.HTTP.MaxHeaderBytes = 12345
	cfg.HTTP.H2CEnabled = true

	main := newMainHTTPServer(cfg, http.NewServeMux(), zerolog.Nop())
	if main.Addr != cfg.HTTP.Addr || main.MaxHeaderBytes != cfg.HTTP.MaxHeaderBytes || main.Protocols == nil {
		t.Fatalf("main server = addr %q maxHeader %d protocols %v", main.Addr, main.MaxHeaderBytes, main.Protocols)
	}

	cfg.HTTP.H2CEnabled = false
	plain := newMainHTTPServer(cfg, http.NewServeMux(), zerolog.Nop())
	if plain.Protocols != nil {
		t.Fatalf("plain protocols = %v, want nil", plain.Protocols)
	}

	if srv := newPProfHTTPServer(cfg, zerolog.Nop()); srv != nil {
		t.Fatalf("disabled pprof server = %#v", srv)
	}
	cfg.HTTP.PProfAddr = "127.0.0.1:0"
	pprofSrv := newPProfHTTPServer(cfg, zerolog.Nop())
	if pprofSrv == nil || pprofSrv.Addr != cfg.HTTP.PProfAddr || pprofSrv.Handler == nil {
		t.Fatalf("pprof server = %#v", pprofSrv)
	}
}

func TestBuildRunnablesRunnableClosures(t *testing.T) {
	cfg := newSingletonTestConfig(t)
	cfg.HTTP.PProfAddr = "127.0.0.1:0"
	cfg.Limits.SweepInterval = time.Second
	cfg.Limits.SweepIdleThreshold = time.Second
	a, cleanup, err := InitializeApp(*cfg, zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("InitializeApp: %v", err)
	}
	defer cleanup()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = a.SSEPool.Shutdown(ctx)
	})

	closedTx := make(chan wal.Tx)
	close(closedTx)
	a.TxCh = closedTx
	a.HTTPServer = &http.Server{Addr: "127.0.0.1:0"}
	a.PProfServer = &http.Server{Addr: "127.0.0.1:0"}

	cancelCalls := 0
	a.cancel = func() { cancelCalls++ }

	runs := buildRunnables(a, "slot")
	byName := make(map[string]Runnable, len(runs))
	for _, r := range runs {
		byName[r.Name] = r
		if r.OnError != nil {
			r.OnError(errors.New("boom"))
		}
	}
	if cancelCalls != 4 {
		t.Fatalf("cancelCalls = %d, want 4", cancelCalls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, name := range []string{"auth-breaker-fsm", "limits-sweeper", "auth-stale-watcher", "router-ingest", "metrics-sampler"} {
		r, ok := byName[name]
		if !ok {
			t.Fatalf("missing runnable %q", name)
		}
		if err := r.Run(ctx); err != nil && name != "router-ingest" {
			t.Fatalf("%s Run: %v", name, err)
		}
	}

	runServerRunnable(t, byName["http-server"], a.HTTPServer)
	runServerRunnable(t, byName["pprof-server"], a.PProfServer)
}

func runServerRunnable(t *testing.T, r Runnable, srv *http.Server) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	_ = srv.Shutdown(context.Background())
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s Run: %v", r.Name, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not stop", r.Name)
	}
}
