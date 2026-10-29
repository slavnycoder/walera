package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClient_OpenSession_OK(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Method = %s; want POST", r.Method)
		}
		if r.URL.Path != "/auth/sessions" {
			t.Errorf("Path = %s; want /auth/sessions", r.URL.Path)
		}
		if got, want := r.Header.Get("Authorization"), "Bearer the-token"; got != want {
			t.Errorf("Authorization: got %q; want %q", got, want)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["channel"] != "users:42" {
			t.Errorf("body.channel = %q; want users:42", body["channel"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, mc := newTestClient(t, srv.URL, 2*time.Second)
	m, err := c.OpenSession(context.Background(), "the-token", ForwardedAuth{}, "users:42", "req-open-1")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if m.UserID != "u1" {
		t.Errorf("UserID = %q; want u1", m.UserID)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "ok"); v != 1 {
		t.Errorf("ok counter: got %v; want 1", v)
	}
}

func TestClient_OpenSession_RejectsMissingCredentials(t *testing.T) {
	t.Parallel()
	c, _ := newTestClient(t, "http://unused", time.Second)
	_, err := c.OpenSession(context.Background(), "", ForwardedAuth{}, "users:1", "req-x")
	var ue *ErrUnauthorized
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v (%T); want *ErrUnauthorized", err, err)
	}
	if string(ue.Body) != `{"reason":"missing_credentials"}` {
		t.Errorf("body = %q; want missing_credentials JSON", ue.Body)
	}
}
