//go:build integration

// Package integration — sse_client_internal_test.go exercises the typed
// *HTTPError extraction path that the Test04 reject sub-tests rely on
// (TEST-07 / ROADMAP §10 SC #3). It does NOT require the full harness
// (no PG container, no Walera binary) — it constructs a Client directly
// against an httptest.Server that returns 403 with a JSON body.
package integration

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestSSEClient_HTTPError verifies that Client.Connect surfaces the HTTP
// status (and body) of the initial handshake non-200 response as a typed
// *HTTPError on errCh, before closing the events channel. Callers extract
// the value via errors.As.
func TestSSEClient_HTTPError(t *testing.T) {
	t.Parallel()

	const wantStatus = http.StatusForbidden
	const wantBody = `{"reason":"not_allowed"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(wantStatus)
		_, _ = w.Write([]byte(wantBody))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events, errCh, closeFn := c.Connect(ctx, "users/42", "irrelevant-token")
	defer closeFn()

	// Connect must surface *HTTPError on errCh BEFORE events is closed-and-empty.
	// Because Go's select randomizes among ready cases and `<-events` on a
	// closed channel is immediately ready, we may pick the events branch first;
	// in that case the error MUST already be queued on errCh (cap 1, the send
	// happens before close(events) in Connect), so re-poll with a short budget.
	assertHTTPErr := func(err error) {
		t.Helper()
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) {
			t.Fatalf("expected *HTTPError, got %T: %v", err, err)
		}
		if httpErr.Status != wantStatus {
			t.Fatalf("Status = %d, want %d", httpErr.Status, wantStatus)
		}
		if !strings.Contains(string(httpErr.Body), "not_allowed") {
			t.Fatalf("Body missing 'not_allowed': %q", httpErr.Body)
		}
		// HTTPError.Error() must include the status — preserves backwards
		// compatibility with any caller that still string-matches the message.
		if !strings.Contains(httpErr.Error(), "403") {
			t.Fatalf("Error() missing status: %q", httpErr.Error())
		}
	}

	select {
	case err := <-errCh:
		assertHTTPErr(err)
	case ev, ok := <-events:
		if ok {
			t.Fatalf("unexpected event on rejected connect: %+v", ev)
		}
		// events closed; the error MUST be queued on errCh — re-poll.
		select {
		case err := <-errCh:
			assertHTTPErr(err)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("events closed without errCh delivering *HTTPError")
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for *HTTPError")
	}
}
