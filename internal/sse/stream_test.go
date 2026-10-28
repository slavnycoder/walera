package sse

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestStream_NoFramesAfterRevokeErrorFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	sub := kit.bc.firstSub(t)
	_ = readUntil(t, resp.Body, len(testPrelude), 2*time.Second)

	sub.Drop("auth_revoked")
	_ = sub.Send([]byte("event: tx\ndata: {\"leak\":true}\n\n"))

	rest, _ := io.ReadAll(resp.Body)
	body := string(rest)
	if !strings.Contains(body, `"reason":"auth_revoked"`) {
		t.Fatalf("body = %q; want auth_revoked error frame", body)
	}
	if strings.Contains(body[strings.Index(body, "auth_revoked"):], "leak") {
		t.Fatalf("frame delivered after the revoke error frame: %q", body)
	}
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
