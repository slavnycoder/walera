package sse

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
)

func (f *fakeBroadcaster) waitSubs(t *testing.T, n int) []*router.Subscriber {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		if len(f.subs) >= n {
			out := append([]*router.Subscriber(nil), f.subs[:n]...)
			f.mu.Unlock()
			return out
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("fakeBroadcaster: %d subscribers not registered within 2s", n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func openStream(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

const testPrelude = "retry: 15000\n\n"

func TestStream_DeliversSentFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	sub := kit.bc.firstSub(t)
	frame := "event: tx\ndata: {\"k\":1}\n\n"
	if !sub.Send([]byte(frame)) {
		t.Fatal("Send returned false on an empty queue")
	}

	body := string(readUntil(t, resp.Body, len(testPrelude)+len(frame), 2*time.Second))
	if !strings.Contains(body, frame) {
		t.Fatalf("body = %q; want it to contain %q", body, frame)
	}
	sub.Drop("test")
}

// A client that stops reading must stall only its own stream.
func TestStream_SlowClientDoesNotStallOthers(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	slow, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = slow.Close() })
	_, _ = slow.Write([]byte("GET /sse/v1/users/1 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer valid\r\n\r\n"))
	slowSub := kit.bc.waitSubs(t, 1)[0]

	fastResp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/2")
	fastSub := kit.bc.waitSubs(t, 2)[1]

	// Fill the slow client's socket buffers and queue until Send keeps refusing:
	// loopback buffers can absorb megabytes, so one refusal may be transient.
	big := []byte("data: " + strings.Repeat("x", 64*1024) + "\n\n")
	filled := false
	var refusedSince time.Time
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if slowSub.Send(big) {
			refusedSince = time.Time{}
			continue
		}
		if refusedSince.IsZero() {
			refusedSince = time.Now()
		} else if time.Since(refusedSince) > 200*time.Millisecond {
			filled = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !filled {
		t.Fatal("slow client's queue never filled")
	}

	frame := "event: tx\ndata: {\"fast\":true}\n\n"
	if !fastSub.Send([]byte(frame)) {
		t.Fatal("fast Send returned false")
	}
	body := string(readUntil(t, fastResp.Body, len(testPrelude)+len(frame), time.Second))
	if !strings.Contains(body, frame) {
		t.Fatalf("fast client body = %q; want %q within 1s while another client is stuck", body, frame)
	}

	select {
	case <-slowSub.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("slow client was never dropped")
	}
	// Done can fire via the request context (net/http cancels it on the failed
	// write) a moment before the handler records the reason.
	for deadline := time.Now().Add(time.Second); slowSub.Reason() == "" && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if got := slowSub.Reason(); got != "slow_consumer" {
		t.Fatalf("slow Reason = %q; want slow_consumer", got)
	}
	fastSub.Drop("test")
}

func TestStream_ServesOverTLS(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		http2 bool
	}{{"http1", false}, {"h2", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kit := newTestHandler(t, nil, nil)
			validMapBackend(kit.backend)
			mux := http.NewServeMux()
			kit.h.Routes(mux)
			srv := httptest.NewUnstartedServer(mux)
			srv.EnableHTTP2 = tc.http2
			srv.StartTLS()
			t.Cleanup(srv.Close)

			resp := openStream(t, srv.Client(), srv.URL+"/sse/v1/users/42")
			if got := resp.ProtoMajor; got != map[bool]int{false: 1, true: 2}[tc.http2] {
				t.Fatalf("ProtoMajor = %d", got)
			}
			body := string(readUntil(t, resp.Body, len(testPrelude), 2*time.Second))
			if !strings.HasPrefix(body, testPrelude) {
				t.Fatalf("body = %q; want prelude %q", body, testPrelude)
			}
			kit.bc.firstSub(t).Drop("test")
		})
	}
}

// gatedWriter blocks Write while gate is armed so frames can pile up in the queue.
type gatedWriter struct {
	mu      sync.Mutex
	buf     strings.Builder
	hdr     http.Header
	gate    chan struct{}
	blocked chan struct{}
	flushes int
}

func (g *gatedWriter) Header() http.Header { return g.hdr }
func (g *gatedWriter) WriteHeader(int)     {}
func (g *gatedWriter) Flush() {
	g.mu.Lock()
	g.flushes++
	g.mu.Unlock()
}

func (g *gatedWriter) flushCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.flushes
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	if gate != nil {
		close(g.blocked)
		<-gate
		g.mu.Lock()
		g.gate = nil
		g.mu.Unlock()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

func (g *gatedWriter) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.String()
}

func TestStream_NoFramesAfterRevokeErrorFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	gw := &gatedWriter{hdr: http.Header{}, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	done := make(chan struct{})
	go func() { defer close(done); kit.h.stream(gw, r, sub, nil) }()
	kit.bc.firstSub(t)

	gate := make(chan struct{})
	gw.mu.Lock()
	gw.gate = gate
	gw.mu.Unlock()
	if !sub.Send([]byte("event: tx\ndata: {\"ok\":true}\n\n")) {
		t.Fatal("Send returned false")
	}
	<-gw.blocked
	for i := 0; i < 8; i++ {
		if !sub.Send([]byte("event: tx\ndata: {\"leak\":true}\n\n")) {
			t.Fatalf("leak Send %d returned false", i)
		}
	}
	sub.Drop("auth_revoked")
	close(gate)
	<-done

	body := gw.String()
	if !strings.Contains(body, `"reason":"auth_revoked"`) {
		t.Fatalf("body = %q; want auth_revoked error frame", body)
	}
	if strings.Contains(body, "leak") {
		t.Fatalf("frame queued before the revoke was delivered: %q", body)
	}
}

// On HTTP/2 a leftover write deadline resets an idle stream.
func TestStream_H2IdleLongerThanWriteTimeout(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	kit.h.cfg.WriteTimeout = 300 * time.Millisecond
	kit.h.cfg.HeartbeatInterval = 5 * time.Second
	mux := http.NewServeMux()
	kit.h.Routes(mux)
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	resp := openStream(t, srv.Client(), srv.URL+"/sse/v1/users/42")
	if resp.ProtoMajor != 2 {
		t.Fatalf("ProtoMajor = %d; want 2", resp.ProtoMajor)
	}
	sub := kit.bc.firstSub(t)
	_ = readUntil(t, resp.Body, len(testPrelude), 2*time.Second)

	time.Sleep(time.Second)
	frame := "event: tx\ndata: {\"k\":1}\n\n"
	if !sub.Send([]byte(frame)) {
		t.Fatalf("Send returned false after idle; Reason = %q", sub.Reason())
	}
	body := string(readUntil(t, resp.Body, len(frame), 2*time.Second))
	if !strings.Contains(body, frame) {
		t.Fatalf("body = %q; want %q after idle", body, frame)
	}
	if got := sub.Reason(); got != "" {
		t.Fatalf("Reason = %q; want empty (stream alive)", got)
	}
	sub.Drop("test")
}

// Without hijack, Server.Shutdown waits for SSE handlers; a shutdown drop must release them.
func TestStream_ServerShutdownCompletesAfterDrop(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	sub := kit.bc.firstSub(t)
	_ = readUntil(t, resp.Body, len(testPrelude), 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Config.Shutdown(ctx) }()
	sub.Drop("shutdown")

	if err := <-errCh; err != nil {
		t.Fatalf("Shutdown: %v; want nil within 2s after the shutdown drop", err)
	}
	rest, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(rest), "event: shutdown") {
		t.Fatalf("body = %q; want shutdown frame", rest)
	}
}

// A stream registering after Broadcaster.Shutdown's snapshot must still be released.
func TestStream_OpenAfterBroadcasterShutdown_GetsShutdownFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	bc := router.New(kit.h.routerCfg, router.Deps{
		Logger:  zerolog.Nop(),
		Metrics: metrics.New(),
		Encoder: kit.h.enc,
	})
	kit.h.bc = bc
	srv := newTestServer(t, kit.h)

	bc.Shutdown()

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "event: shutdown") {
		t.Fatalf("body = %q; want shutdown frame", body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Config.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v; want nil within 2s", err)
	}
}

func TestStream_BatchesQueuedFramesIntoOneFlush(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	gw := &gatedWriter{hdr: http.Header{}, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	done := make(chan struct{})
	go func() { defer close(done); kit.h.stream(gw, r, sub, nil) }()
	kit.bc.firstSub(t)

	gate := make(chan struct{})
	gw.mu.Lock()
	gw.gate = gate
	gw.mu.Unlock()
	if !sub.Send([]byte("data: 0\n\n")) {
		t.Fatal("Send 0 returned false")
	}
	<-gw.blocked
	for i := 1; i <= 5; i++ {
		if !sub.Send([]byte(fmt.Sprintf("data: %d\n\n", i))) {
			t.Fatalf("Send %d returned false", i)
		}
	}
	before := gw.flushCount()
	close(gate)

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(gw.String(), "data: 5\n\n") {
		if time.Now().After(deadline) {
			t.Fatalf("frames not delivered: %q", gw.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
	// frame 0 + one batch for 1..5; a heartbeat may add one more.
	if got := gw.flushCount() - before; got > 3 {
		t.Fatalf("flushes for 6 frames = %d; want <= 3 (queued frames batched)", got)
	}
	body := gw.String()
	for i := 0; i < 5; i++ {
		if strings.Index(body, fmt.Sprintf("data: %d\n", i)) > strings.Index(body, fmt.Sprintf("data: %d\n", i+1)) {
			t.Fatalf("frames out of order: %q", body)
		}
	}
	sub.Drop("test")
	<-done
}

// Once the client has the response, it must already be routable: a row it
// writes right after connecting is never missed.
func TestStream_RegistersBeforeFirstFlush(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	gate := make(chan struct{})
	gw := &gatedWriter{hdr: http.Header{}, gate: gate, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	done := make(chan struct{})
	go func() { defer close(done); kit.h.stream(gw, r, sub, nil) }()

	<-gw.blocked // the first write is parked: nothing has reached the client yet
	kit.bc.mu.Lock()
	registered := len(kit.bc.subs)
	kit.bc.mu.Unlock()
	close(gate)
	sub.Drop("test")
	<-done
	if registered != 1 {
		t.Fatalf("registered subscribers at first write = %d; want 1", registered)
	}
}

// shutdownBroadcaster mimics a closed router.Broadcaster: Register drops the
// late registrant with "shutdown".
type shutdownBroadcaster struct{ fakeBroadcaster }

func (s *shutdownBroadcaster) Register(sub *router.Subscriber) {
	s.fakeBroadcaster.Register(sub)
	sub.Drop("shutdown")
}

// A handshake racing service shutdown still gets the prelude and then an
// explicit shutdown frame, not a bare disconnect.
func TestStream_LateRegistrantGetsShutdownFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.h.bc = &shutdownBroadcaster{}
	gw := &gatedWriter{hdr: http.Header{}, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	kit.h.stream(gw, r, sub, nil)

	body := gw.String()
	if !strings.HasPrefix(body, testPrelude) {
		t.Fatalf("body = %q; want prelude first", body)
	}
	if !strings.Contains(body, "event: shutdown") {
		t.Fatalf("body = %q; want a shutdown frame", body)
	}
}
