//go:build integration

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
