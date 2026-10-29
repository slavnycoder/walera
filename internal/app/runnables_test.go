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
}

func TestBuildRunnablesRunnableClosures(t *testing.T) {
	cfg := newSingletonTestConfig(t)
	a, cleanup, err := InitializeApp(*cfg, zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("InitializeApp: %v", err)
	}
	defer cleanup()

	closedTx := make(chan wal.Tx)
	close(closedTx)
	a.TxCh = closedTx
	a.HTTPServer = &http.Server{Addr: "127.0.0.1:0"}

	cancelCalls := 0
	a.cancel = func() { cancelCalls++ }

	runs := buildRunnables(a)
	byName := make(map[string]Runnable, len(runs))
	for _, r := range runs {
		byName[r.Name] = r
		if r.OnError != nil {
			r.OnError(errors.New("boom"))
		}
	}
	if cancelCalls != 3 {
		t.Fatalf("cancelCalls = %d, want 3", cancelCalls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ingest, ok := byName["router-ingest"]
	if !ok {
		t.Fatal("missing runnable router-ingest")
	}
	_ = ingest.Run(ctx)

	runServerRunnable(t, byName["http-server"], a.HTTPServer)
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
