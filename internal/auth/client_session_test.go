package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

func newSignedTestClient(t *testing.T, baseURL string, timeout time.Duration) (*Client, *fakeBreaker, *metrics.Registry) {
	t.Helper()
	fb := &fakeBreaker{}
	mc := metrics.New()
	cfg := Config{
		BackendURL:     baseURL,
		RequestTimeout: timeout,
		Signing: SigningConfig{
			Secret: strings.Repeat("k", 64),
			Kid:    "v1",
		},
	}
	return New(cfg, Deps{Logger: zerolog.Nop(), Breaker: fb, Metrics: mc}), fb, mc
}

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

	c, fb, mc := newSignedTestClient(t, srv.URL, 2*time.Second)
	m, err := c.OpenSession(context.Background(), "the-token", "users:42", "req-open-1")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if m.UserID != "u1" {
		t.Errorf("UserID = %q; want u1", m.UserID)
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "ok"); v != 1 {
		t.Errorf("ok counter: got %v; want 1", v)
	}
}

func TestClient_OpenSession_RejectsEmptyBearer(t *testing.T) {
	t.Parallel()
	c, _, _ := newSignedTestClient(t, "http://unused", time.Second)
	_, err := c.OpenSession(context.Background(), "", "users:1", "req-x")
	var ue *ErrUnauthorized
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v (%T); want *ErrUnauthorized", err, err)
	}
}

func TestClient_OpenSession_401PropagatesBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("bad-bearer"))
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newSignedTestClient(t, srv.URL, time.Second)
	_, err := c.OpenSession(context.Background(), "any", "ch:1", "req")
	var ue *ErrUnauthorized
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v; want ErrUnauthorized", err)
	}
	if string(ue.Body) != "bad-bearer" {
		t.Errorf("body = %q; want bad-bearer", ue.Body)
	}
}

func TestClient_RefreshPermissions_OK_SignatureVerified(t *testing.T) {
	t.Parallel()

	const secret = "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"
	const kid = "v1"

	var seenSig, seenKid string
	var seenPayload refreshRequest
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		seenSig = r.Header.Get("X-Walera-Sig")
		seenKid = r.Header.Get("X-Walera-Kid")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &seenPayload)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newSignedTestClient(t, srv.URL, 2*time.Second)
	_, err := c.RefreshPermissions(context.Background(), "u1", "users:42", "req-refresh-1")
	if err != nil {
		t.Fatalf("RefreshPermissions: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if seenKid != kid {
		t.Errorf("X-Walera-Kid = %q; want %q", seenKid, kid)
	}

	signer, _ := NewSigner([]byte(secret), kid)
	if !signer.Verify(seenPayload.UserID, seenPayload.Channel, seenPayload.TS, seenPayload.Nonce, seenSig) {
		t.Errorf("server-side HMAC verify failed: payload=%+v sig=%q", seenPayload, seenSig)
	}
	if seenPayload.UserID != "u1" || seenPayload.Channel != "users:42" {
		t.Errorf("payload identity wrong: %+v", seenPayload)
	}
	if seenPayload.Nonce == "" || len(seenPayload.Nonce) != 32 {
		t.Errorf("nonce shape wrong: %q", seenPayload.Nonce)
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "ok"); v != 1 {
		t.Errorf("ok counter: got %v; want 1", v)
	}
}

func TestClient_RefreshPermissions_FreshNonceEachCall(t *testing.T) {
	t.Parallel()

	var nonces []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p refreshRequest
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		nonces = append(nonces, p.Nonce)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newSignedTestClient(t, srv.URL, 2*time.Second)
	for i := 0; i < 4; i++ {
		_, err := c.RefreshPermissions(context.Background(), "u1", "ch:1", "req")
		if err != nil {
			t.Fatalf("RefreshPermissions: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	seen := make(map[string]struct{}, len(nonces))
	for _, n := range nonces {
		if _, dup := seen[n]; dup {
			t.Errorf("duplicate nonce across calls: %q (all=%v)", n, nonces)
		}
		seen[n] = struct{}{}
	}
}

func TestClient_RefreshPermissions_FailsWithoutSigner(t *testing.T) {
	t.Parallel()

	mc := metrics.New()
	c := New(Config{
		BackendURL:     "http://unused",
		RequestTimeout: time.Second,
	}, Deps{Logger: zerolog.Nop(), Metrics: mc})

	_, err := c.RefreshPermissions(context.Background(), "u1", "ch:1", "req")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v (%T); want *ErrUnavailable", err, err)
	}
	if !strings.Contains(ue.Cause.Error(), "signer not configured") {
		t.Errorf("cause = %v; want 'signer not configured'", ue.Cause)
	}
}

func TestClient_RefreshPermissions_RejectsEmptyUserID(t *testing.T) {
	t.Parallel()
	c, _, _ := newSignedTestClient(t, "http://unused", time.Second)
	_, err := c.RefreshPermissions(context.Background(), "", "ch:1", "req")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v; want ErrUnavailable", err)
	}
	if !strings.Contains(ue.Cause.Error(), "empty user_id") {
		t.Errorf("cause = %v; want 'empty user_id'", ue.Cause)
	}
}

func TestClient_RefreshPermissions_BackendUnavailable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c, fb, _ := newSignedTestClient(t, srv.URL, time.Second)
	_, err := c.RefreshPermissions(context.Background(), "u1", "ch:1", "req")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v; want ErrUnavailable", err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker last: got (%v,%v); want (false,true) (5xx = backend down)", last, ok)
	}
}

func TestClient_RefreshPermissions_TimestampWithinTwoSeconds(t *testing.T) {
	t.Parallel()

	var seenTS int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p refreshRequest
		_ = json.NewDecoder(r.Body).Decode(&p)
		seenTS = p.TS
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newSignedTestClient(t, srv.URL, time.Second)
	before := time.Now().Unix()
	_, err := c.RefreshPermissions(context.Background(), "u1", "ch:1", "req")
	after := time.Now().Unix()
	if err != nil {
		t.Fatalf("RefreshPermissions: %v", err)
	}
	if seenTS < before || seenTS > after {
		t.Errorf("ts = %d; want in [%d,%d]", seenTS, before, after)
	}
}
