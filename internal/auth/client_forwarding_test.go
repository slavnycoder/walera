package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// newForwardingTestClient mirrors newSignedTestClient but wires the forwarded
// cookie/header allowlists so ForwardedFromRequest and OpenSession exercise the
// forwarding path.
func newForwardingTestClient(t *testing.T, baseURL string, cookies, headers []string) (*Client, *fakeBreaker, *metrics.Registry) {
	t.Helper()
	fb := &fakeBreaker{}
	mc := metrics.New()
	cfg := Config{
		BackendURL:     baseURL,
		RequestTimeout: 2 * time.Second,
		Signing: SigningConfig{
			Secret: strings.Repeat("k", 64),
			Kid:    "v1",
		},
		ForwardedCookies: cookies,
		ForwardedHeaders: headers,
	}
	return New(cfg, Deps{Logger: zerolog.Nop(), Breaker: fb, Metrics: mc}), fb, mc
}

// capturedRequest records what the backend saw, guarded for the race detector.
type capturedRequest struct {
	mu      sync.Mutex
	cookies map[string]string
	headers http.Header
	called  bool
}

func newCaptureServer(t *testing.T) (*httptest.Server, *capturedRequest) {
	t.Helper()
	cap := &capturedRequest{cookies: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.mu.Lock()
		cap.called = true
		for _, ck := range r.Cookies() {
			cap.cookies[ck.Name] = ck.Value
		}
		cap.headers = r.Header.Clone()
		cap.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)
	return srv, cap
}

func (c *capturedRequest) cookie(name string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.cookies[name]
	return v, ok
}

func (c *capturedRequest) header(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headers.Get(name)
}

func (c *capturedRequest) wasCalled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.called
}

// TestClient_OpenSession_ForwardsAllowlistedCookies drives the full path
// (ForwardedFromRequest -> OpenSession): allowlisted cookies reach the backend,
// a non-allowlisted cookie does NOT.
func TestClient_OpenSession_ForwardsAllowlistedCookies(t *testing.T) {
	t.Parallel()
	srv, cap := newCaptureServer(t)

	c, _, _ := newForwardingTestClient(t, srv.URL, []string{"session", "csrf"}, nil)

	r, _ := http.NewRequest(http.MethodGet, "http://example/sse", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: "sess-abc"})
	r.AddCookie(&http.Cookie{Name: "csrf", Value: "csrf-xyz"})
	r.AddCookie(&http.Cookie{Name: "tracking", Value: "should-not-appear"})
	fwd := c.ForwardedFromRequest(r)

	if _, err := c.OpenSession(context.Background(), "the-token", fwd, "users:42", "req-fwd-1"); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if v, ok := cap.cookie("session"); !ok || v != "sess-abc" {
		t.Errorf("session cookie: got (%q,%v); want (sess-abc,true)", v, ok)
	}
	if v, ok := cap.cookie("csrf"); !ok || v != "csrf-xyz" {
		t.Errorf("csrf cookie: got (%q,%v); want (csrf-xyz,true)", v, ok)
	}
	if v, ok := cap.cookie("tracking"); ok {
		t.Errorf("tracking cookie forwarded (=%q); want it filtered out", v)
	}
}

// TestClient_OpenSession_ForwardsAllowlistedHeaders drives the full path
// (ForwardedFromRequest -> OpenSession): allowlisted headers reach the backend,
// a non-allowlisted header does NOT.
func TestClient_OpenSession_ForwardsAllowlistedHeaders(t *testing.T) {
	t.Parallel()
	srv, cap := newCaptureServer(t)

	c, _, _ := newForwardingTestClient(t, srv.URL, nil, []string{"X-Tenant-Id", "X-Trace"})

	r, _ := http.NewRequest(http.MethodGet, "http://example/sse", nil)
	r.Header.Set("X-Tenant-Id", "tenant-7")
	r.Header.Set("X-Trace", "trace-9")
	r.Header.Set("X-Not-Allowed", "should-not-appear")
	fwd := c.ForwardedFromRequest(r)

	if _, err := c.OpenSession(context.Background(), "the-token", fwd, "users:42", "req-fwd-2"); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if got := cap.header("X-Tenant-Id"); got != "tenant-7" {
		t.Errorf("X-Tenant-Id: got %q; want tenant-7", got)
	}
	if got := cap.header("X-Trace"); got != "trace-9" {
		t.Errorf("X-Trace: got %q; want trace-9", got)
	}
	if got := cap.header("X-Not-Allowed"); got != "" {
		t.Errorf("X-Not-Allowed: got %q; want empty (not allowlisted)", got)
	}
}

// TestClient_OpenSession_ReservedHeadersNeverOverridden proves that even if a
// ForwardedAuth somehow carries reserved headers (e.g. validation bypassed),
// OpenSession's defensive skip prevents them from overriding Walera's own
// Authorization / Content-Type / Accept.
func TestClient_OpenSession_ReservedHeadersNeverOverridden(t *testing.T) {
	t.Parallel()
	srv, cap := newCaptureServer(t)

	c, _, _ := newForwardingTestClient(t, srv.URL, nil, []string{"X-Tenant-Id"})
	fwd := ForwardedAuth{
		Headers: http.Header{
			"X-Tenant-Id":   []string{"tenant-7"},
			"Authorization": []string{"Bearer ATTACKER"},
			"Content-Type":  []string{"text/evil"},
			"Accept":        []string{"text/evil"},
		},
	}
	if _, err := c.OpenSession(context.Background(), "the-token", fwd, "users:42", "req-fwd-3"); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	if got := cap.header("Authorization"); got != "Bearer the-token" {
		t.Errorf("Authorization: got %q; want Bearer the-token (reserved must not be overridden)", got)
	}
	if got := cap.header("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type: got %q; want application/json", got)
	}
	if got := cap.header("Accept"); got != "application/json" {
		t.Errorf("Accept: got %q; want application/json", got)
	}
	if got := cap.header("X-Tenant-Id"); got != "tenant-7" {
		t.Errorf("X-Tenant-Id: got %q; want tenant-7", got)
	}
}

// TestClient_OpenSession_EmptyBearerWithCookieIsAllowed: an allowlisted cookie
// is a sufficient credential — the backend IS called, returns OK, and sees NO
// Authorization header (bearer was empty).
func TestClient_OpenSession_EmptyBearerWithCookieIsAllowed(t *testing.T) {
	t.Parallel()
	srv, cap := newCaptureServer(t)

	c, fb, mc := newForwardingTestClient(t, srv.URL, []string{"session"}, nil)
	fwd := ForwardedAuth{
		Cookies: []*http.Cookie{{Name: "session", Value: "sess-abc"}},
	}
	m, err := c.OpenSession(context.Background(), "", fwd, "users:42", "req-fwd-4")
	if err != nil {
		t.Fatalf("OpenSession: %v; want success on cookie-only credential", err)
	}
	if m.UserID != "u1" {
		t.Errorf("UserID = %q; want u1", m.UserID)
	}
	if !cap.wasCalled() {
		t.Error("backend was not called; want a backend call on cookie-only credential")
	}
	if got := cap.header("Authorization"); got != "" {
		t.Errorf("Authorization: got %q; want empty (bearer was empty)", got)
	}
	if v, ok := cap.cookie("session"); !ok || v != "sess-abc" {
		t.Errorf("session cookie: got (%q,%v); want (sess-abc,true)", v, ok)
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "ok"); v != 1 {
		t.Errorf("ok counter: got %v; want 1", v)
	}
}

// TestClient_OpenSession_EmptyBearerWithHeaderIsAllowed: an allowlisted header
// is also a sufficient credential.
func TestClient_OpenSession_EmptyBearerWithHeaderIsAllowed(t *testing.T) {
	t.Parallel()
	srv, cap := newCaptureServer(t)

	c, _, _ := newForwardingTestClient(t, srv.URL, nil, []string{"X-Tenant-Id"})
	fwd := ForwardedAuth{
		Headers: http.Header{"X-Tenant-Id": []string{"tenant-7"}},
	}
	if _, err := c.OpenSession(context.Background(), "", fwd, "users:42", "req-fwd-5"); err != nil {
		t.Fatalf("OpenSession: %v; want success on header-only credential", err)
	}
	if !cap.wasCalled() {
		t.Error("backend was not called; want a backend call on header-only credential")
	}
	if got := cap.header("Authorization"); got != "" {
		t.Errorf("Authorization: got %q; want empty (bearer was empty)", got)
	}
}

// TestClient_OpenSession_EmptyBearerNoCreds: no bearer and no forwarded creds
// -> ErrUnauthorized (missing credentials) and NO backend call.
func TestClient_OpenSession_EmptyBearerNoCreds(t *testing.T) {
	t.Parallel()
	srv, cap := newCaptureServer(t)

	c, fb, mc := newForwardingTestClient(t, srv.URL, []string{"session"}, []string{"X-Tenant-Id"})
	_, err := c.OpenSession(context.Background(), "", ForwardedAuth{}, "users:42", "req-fwd-6")
	var ue *ErrUnauthorized
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v (%T); want *ErrUnauthorized", err, err)
	}
	if string(ue.Body) != `{"reason":"missing_credentials"}` {
		t.Errorf("body = %q; want missing_credentials JSON", ue.Body)
	}
	if cap.wasCalled() {
		t.Error("backend WAS called; want no backend call when credentials are missing")
	}
	// Missing-credential short-circuit records a successful breaker result.
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unauthorized"); v != 1 {
		t.Errorf("unauthorized counter: got %v; want 1", v)
	}
}

func TestClient_ForwardedFromRequest_FiltersCookies(t *testing.T) {
	t.Parallel()

	// Cookie names are case-sensitive: "Session" must NOT match allowlist "session".
	c, _, _ := newForwardingTestClient(t, "http://unused", []string{"session", "csrf"}, nil)

	r, _ := http.NewRequest(http.MethodGet, "http://example/sse", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: "sess-abc"})
	r.AddCookie(&http.Cookie{Name: "csrf", Value: "csrf-xyz"})
	r.AddCookie(&http.Cookie{Name: "tracking", Value: "nope"})
	r.AddCookie(&http.Cookie{Name: "Session", Value: "wrong-case"})

	fwd := c.ForwardedFromRequest(r)

	got := map[string]string{}
	for _, ck := range fwd.Cookies {
		got[ck.Name] = ck.Value
	}
	if got["session"] != "sess-abc" {
		t.Errorf("session = %q; want sess-abc", got["session"])
	}
	if got["csrf"] != "csrf-xyz" {
		t.Errorf("csrf = %q; want csrf-xyz", got["csrf"])
	}
	if _, ok := got["tracking"]; ok {
		t.Error("tracking cookie forwarded; want it filtered out")
	}
	if _, ok := got["Session"]; ok {
		t.Error("Session (wrong case) forwarded; cookie names are case-sensitive")
	}
	if len(fwd.Cookies) != 2 {
		t.Errorf("forwarded cookie count = %d; want 2", len(fwd.Cookies))
	}
}

func TestClient_ForwardedFromRequest_FiltersHeaders(t *testing.T) {
	t.Parallel()

	// Header names are case-insensitive: a request header "x-tenant-id" must
	// match allowlist entry "X-Tenant-Id".
	c, _, _ := newForwardingTestClient(t, "http://unused", nil, []string{"X-Tenant-Id"})

	r, _ := http.NewRequest(http.MethodGet, "http://example/sse", nil)
	r.Header.Set("x-tenant-id", "tenant-7") // different casing in
	r.Header.Set("X-Other", "nope")

	fwd := c.ForwardedFromRequest(r)

	if got := fwd.Headers.Get("X-Tenant-Id"); got != "tenant-7" {
		t.Errorf("X-Tenant-Id = %q; want tenant-7 (case-insensitive match)", got)
	}
	if got := fwd.Headers.Get("X-Other"); got != "" {
		t.Errorf("X-Other = %q; want empty (not allowlisted)", got)
	}
}

func TestClient_ForwardedFromRequest_FeatureOffYieldsEmpty(t *testing.T) {
	t.Parallel()

	// No allowlists configured -> ForwardedFromRequest returns Empty().
	c, _, _ := newForwardingTestClient(t, "http://unused", nil, nil)

	r, _ := http.NewRequest(http.MethodGet, "http://example/sse", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: "sess-abc"})
	r.Header.Set("X-Tenant-Id", "tenant-7")

	fwd := c.ForwardedFromRequest(r)
	if !fwd.Empty() {
		t.Errorf("fwd = %+v; want Empty() when feature off", fwd)
	}
}
