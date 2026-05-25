//go:build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func Test_SEC02_OversizedRequestID_Rejected(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hitsBefore := h.Auth.PermissionsRequestCount()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		h.Binary.BaseURL()+"/sse/v1/users/42", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Accept", "text/event-stream")

	req.Header.Set("X-Request-ID", strings.Repeat("A", 256))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v; stderr:\n%s", err, h.Binary.Stderr())
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (stderr:\n%s)", resp.StatusCode, h.Binary.Stderr())
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q; want prefix \"application/json\" (stderr:\n%s)", ct, h.Binary.Stderr())
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Contains(body, []byte("invalid_request_id")) {
		t.Errorf("body = %q; want substring \"invalid_request_id\" (stderr:\n%s)", string(body), h.Binary.Stderr())
	}

	time.Sleep(50 * time.Millisecond)

	hitsAfter := h.Auth.PermissionsRequestCount()
	if hitsAfter != hitsBefore {
		t.Errorf("mock auth backend received %d user-permission call(s) for rejected handshake; want 0 (stderr:\n%s)",
			hitsAfter-hitsBefore, h.Binary.Stderr())
	}
}
