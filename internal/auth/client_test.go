package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// --- helpers ---

type fakeBreaker struct {
	mu      sync.Mutex
	results []bool
}

func (f *fakeBreaker) RecordResult(b bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, b)
}

func (f *fakeBreaker) Allow() bool { return true }

func (f *fakeBreaker) Last() (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.results) == 0 {
		return false, false
	}
	return f.results[len(f.results)-1], true
}

// gatherCounter returns the counter value at <name>{labelKey=labelVal}, or 0
// if absent.
func gatherCounter(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// gatherHistogramSampleCount returns the sample count for the given histogram
// family (0 if absent).
func gatherHistogramSampleCount(t *testing.T, reg *metrics.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range fam.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				total += h.GetSampleCount()
			}
		}
		return total
	}
	return 0
}

func matchLabel(m *dto.Metric, key, val string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key && lp.GetValue() == val {
			return true
		}
	}
	return false
}

// newTestClient bundles the boilerplate for constructing a Client pointing at
// a httptest server.
func newTestClient(t *testing.T, baseURL string, timeout time.Duration) (*Client, *fakeBreaker, *metrics.Registry) {
	t.Helper()
	fb := &fakeBreaker{}
	mc := metrics.New()
	cfg := Config{
		BackendURL:     baseURL,
		RequestTimeout: timeout,
	}
	return New(cfg, Deps{Logger: zerolog.Nop(), Breaker: fb, Metrics: mc}), fb, mc
}

// validBody is the canonical 200 response body used by the OK tests.
const validBody = `{"user_id":"u1","tables":{"users":["id","name"]},"ttl_seconds":60}`

// --- tests ---

func TestClient_Permissions_OK(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer good-token"; got != want {
			t.Errorf("Authorization: got %q; want %q", got, want)
		}
		if got, want := r.Header.Get("X-Request-ID"), "req-1"; got != want {
			t.Errorf("X-Request-ID: got %q; want %q", got, want)
		}
		if got, want := r.URL.Query().Get("channel"), "users:42"; got != want {
			t.Errorf("query channel: got %q; want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
	m, err := c.Permissions(context.Background(), "good-token", "users:42", "req-1")
	if err != nil {
		t.Fatalf("Permissions: %v", err)
	}
	if m.UserID != "u1" {
		t.Errorf("UserID: got %q; want %q", m.UserID, "u1")
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last result: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "ok"); v != 1 {
		t.Errorf("auth_requests_total{result=ok}: got %v; want 1", v)
	}
	if v := gatherHistogramSampleCount(t, mc, "walera_auth_request_duration_seconds"); v == 0 {
		t.Errorf("auth_request_duration_seconds sample count: got 0; want >=1")
	}
}

func TestClient_Permissions_ChannelURLEscaped(t *testing.T) {
	t.Parallel()

	var gotChannel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotChannel = r.URL.Query().Get("channel")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "orders/items:99", "req-x")
	if err != nil {
		t.Fatalf("Permissions: %v", err)
	}
	if got, want := gotChannel, "orders/items:99"; got != want {
		t.Errorf("channel after server unescape: got %q; want %q", got, want)
	}
}

func TestClient_Permissions_401(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("forbidden"))
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-2")
	var ue *ErrUnauthorized
	if !errors.As(err, &ue) {
		t.Fatalf("err: got %v (%T); want *ErrUnauthorized", err, err)
	}
	if string(ue.Body) != "forbidden" {
		t.Errorf("Body: got %q; want %q", string(ue.Body), "forbidden")
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last result: got (%v,%v); want (true,true) (401 proves reachable)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unauthorized"); v != 1 {
		t.Errorf("auth_requests_total{result=unauthorized}: got %v; want 1", v)
	}
}

func TestClient_Permissions_403(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("nope"))
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-3")
	var fe *ErrForbidden
	if !errors.As(err, &fe) {
		t.Fatalf("err: got %v (%T); want *ErrForbidden", err, err)
	}
	if string(fe.Body) != "nope" {
		t.Errorf("Body: got %q; want %q", string(fe.Body), "nope")
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last result: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "forbidden"); v != 1 {
		t.Errorf("auth_requests_total{result=forbidden}: got %v; want 1", v)
	}
}

func TestClient_Permissions_404(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing"))
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-4")
	var ne *ErrNotFound
	if !errors.As(err, &ne) {
		t.Fatalf("err: got %v (%T); want *ErrNotFound", err, err)
	}
	if string(ne.Body) != "missing" {
		t.Errorf("Body: got %q; want %q", string(ne.Body), "missing")
	}
	if last, ok := fb.Last(); !ok || !last {
		t.Errorf("breaker last result: got (%v,%v); want (true,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "not_found"); v != 1 {
		t.Errorf("auth_requests_total{result=not_found}: got %v; want 1", v)
	}
}

func TestClient_Permissions_5xx(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-5")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err: got %v (%T); want *ErrUnavailable", err, err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker last result: got (%v,%v); want (false,true) (5xx = backend down)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unavailable"); v != 1 {
		t.Errorf("auth_requests_total{result=unavailable}: got %v; want 1", v)
	}
}

func TestClient_Permissions_NetworkError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := srv.URL
	srv.Close() // close immediately so connect refuses

	c, fb, mc := newTestClient(t, url, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-6")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err: got %v (%T); want *ErrUnavailable", err, err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker last result: got (%v,%v); want (false,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unavailable"); v != 1 {
		t.Errorf("auth_requests_total{result=unavailable}: got %v; want 1", v)
	}
}

func TestClient_Permissions_TimeoutHonored(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, fb, _ := newTestClient(t, srv.URL, 50*time.Millisecond)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-7")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err: got %v (%T); want *ErrUnavailable (timeout)", err, err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker last result: got (%v,%v); want (false,true)", last, ok)
	}
}

func TestClient_Permissions_MalformedJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(srv.Close)

	c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-8")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err: got %v (%T); want *ErrUnavailable", err, err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker last result: got (%v,%v); want (false,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unavailable"); v != 1 {
		t.Errorf("auth_requests_total{result=unavailable}: got %v; want 1", v)
	}
}

func TestClient_Permissions_InvalidShape(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_id":""}`))
	}))
	t.Cleanup(srv.Close)

	c, fb, _ := newTestClient(t, srv.URL, 2*time.Second)
	_, err := c.Permissions(context.Background(), "tok", "users:42", "req-9")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err: got %v (%T); want *ErrUnavailable", err, err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker last result: got (%v,%v); want (false,true)", last, ok)
	}
}

func TestClient_CheckAuth_Reachable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q; want empty for health probe", got)
		}
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newTestClient(t, srv.URL, 2*time.Second)
	if err := c.CheckAuth(context.Background()); err != nil {
		t.Fatalf("CheckAuth: got %v; want nil", err)
	}
}

func TestClient_CheckAuth_BackendReachableViaNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newTestClient(t, srv.URL, 2*time.Second)
	if err := c.CheckAuth(context.Background()); err != nil {
		t.Fatalf("CheckAuth: got %v; want nil (401 = reachable)", err)
	}
}

func TestClient_CheckAuth_NetworkError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := srv.URL
	srv.Close()

	c, _, _ := newTestClient(t, url, 2*time.Second)
	err := c.CheckAuth(context.Background())
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("CheckAuth err: got %v (%T); want *ErrUnavailable", err, err)
	}
}

func TestErrors_ErrorAndUnwrap(t *testing.T) {
	t.Parallel()

	if (&ErrUnauthorized{}).Error() != "auth: unauthorized" {
		t.Errorf("ErrUnauthorized.Error: got %q", (&ErrUnauthorized{}).Error())
	}
	if (&ErrForbidden{}).Error() != "auth: forbidden" {
		t.Errorf("ErrForbidden.Error: got %q", (&ErrForbidden{}).Error())
	}
	if (&ErrNotFound{}).Error() != "auth: not found" {
		t.Errorf("ErrNotFound.Error: got %q", (&ErrNotFound{}).Error())
	}
	if (&ErrUnavailable{}).Error() != "auth: unavailable" {
		t.Errorf("ErrUnavailable.Error (nil cause): got %q", (&ErrUnavailable{}).Error())
	}
	cause := errors.New("network down")
	ue := &ErrUnavailable{Cause: cause}
	if got := ue.Error(); got != "auth: unavailable: network down" {
		t.Errorf("ErrUnavailable.Error (with cause): got %q", got)
	}
	if !errors.Is(ue, cause) {
		t.Error("errors.Is(ErrUnavailable, cause): got false; want true (Unwrap path)")
	}
}

func TestNopBreaker_AllowAndRecord(t *testing.T) {
	t.Parallel()
	var b nopBreaker
	b.RecordResult(true)
	b.RecordResult(false)
	if !b.Allow() {
		t.Error("nopBreaker.Allow: got false; want true")
	}
}

func TestMap_Allowed_Lookup(t *testing.T) {
	t.Parallel()
	m := mkMap("u1", map[string][]string{"users": {"id", "name"}})
	if !m.Allowed("users", "id") {
		t.Error("Allowed(users,id): got false; want true")
	}
	if m.Allowed("users", "secret") {
		t.Error("Allowed(users,secret): got true; want false")
	}
	if m.Allowed("orders", "id") {
		t.Error("Allowed(orders,id): got true; want false (table not in map)")
	}
	var nilm *Whitelist
	if nilm.Allowed("users", "id") {
		t.Error("nil Whitelist.Allowed: got true; want false")
	}
}

// TestPermissions_OutboundRequestID_InvalidSubstituted — SEC-02 / F-P1-02
// defense-in-depth: when an invalid X-Request-ID reaches auth.Client.Permissions
// (programmer-contract violation; the SSE handler should normally have
// rejected it earlier), the client substitutes a freshly-generated 32-char
// hex ID, emits a Warn log, and proceeds. The backend MUST see the
// substitute ID, not the malformed input.
func TestPermissions_OutboundRequestID_InvalidSubstituted(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	var gotRequestID string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotRequestID = r.Header.Get("X-Request-ID")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c := New(Config{
		BackendURL:     srv.URL,
		RequestTimeout: 2 * time.Second,
	}, Deps{Logger: logger, Metrics: metrics.New()})

	_, err := c.Permissions(context.Background(), "client-tok", "users:42", "has spaces and \t tabs")
	if err != nil {
		t.Fatalf("Permissions returned error: %v", err)
	}

	mu.Lock()
	got := gotRequestID
	mu.Unlock()
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(got) {
		t.Errorf("backend did not see substituted ID: got %q", got)
	}
	logOut := buf.String()
	if !strings.Contains(logOut, "outbound X-Request-ID failed validation") {
		t.Errorf("expected Warn log; got %q", logOut)
	}
	// Warn log must include invalid_len, invalid_id_truncated, and
	// substitute_id so operators can correlate the substitution.
	if !strings.Contains(logOut, `"invalid_len":`) {
		t.Errorf("WR-02: Warn log missing invalid_len field; got %q", logOut)
	}
	if !strings.Contains(logOut, `"invalid_id_truncated":`) {
		t.Errorf("WR-02: Warn log missing invalid_id_truncated field; got %q", logOut)
	}
	if !strings.Contains(logOut, `"substitute_id":"`+got+`"`) {
		t.Errorf("WR-02: Warn log substitute_id field must match backend-seen ID %q; got %q", got, logOut)
	}
}

// TestPermissions_OutboundRequestID_Truncated_LogsBoundedOriginal asserts
// that when a malformed X-Request-ID is longer than the 16-byte log hygiene
// cap, the Warn log records only the first 16 bytes followed by "..." in
// invalid_id_truncated. invalid_len retains the full length verbatim so the
// size signal is preserved.
func TestPermissions_OutboundRequestID_Truncated_LogsBoundedOriginal(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c := New(Config{
		BackendURL:     srv.URL,
		RequestTimeout: 2 * time.Second,
	}, Deps{Logger: logger, Metrics: metrics.New()})

	// 200-byte malformed ID — contains a space (regex miss) AND exceeds
	// the 128-byte length cap. Either alone would trigger substitution.
	bad := strings.Repeat("A", 100) + " " + strings.Repeat("B", 99)
	_, err := c.Permissions(context.Background(), "client-tok", "users:42", bad)
	if err != nil {
		t.Fatalf("Permissions returned error: %v", err)
	}

	logOut := buf.String()
	// invalid_len must be the full byte length (200), not the truncated 16.
	wantLen := fmt.Sprintf(`"invalid_len":%d`, len(bad))
	if !strings.Contains(logOut, wantLen) {
		t.Errorf("WR-02: invalid_len must record full length %d; got %q", len(bad), logOut)
	}
	// invalid_id_truncated must be the first 16 bytes + "...".
	wantTrunc := `"invalid_id_truncated":"` + strings.Repeat("A", 16) + `..."`
	if !strings.Contains(logOut, wantTrunc) {
		t.Errorf("WR-02: invalid_id_truncated must be 16-byte prefix+\"...\"; got %q", logOut)
	}
}

// TestTruncateForLog covers the truncation helper directly.
func TestTruncateForLog(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"empty", "", 16, ""},
		{"short", "abc", 16, "abc"},
		{"exact", "1234567890123456", 16, "1234567890123456"},
		{"long", "1234567890123456X", 16, "1234567890123456..."},
		{"very_long", strings.Repeat("Z", 200), 16, strings.Repeat("Z", 16) + "..."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := truncateForLog(tc.in, tc.max); got != tc.want {
				t.Errorf("truncateForLog(%q,%d) = %q; want %q", tc.in, tc.max, got, tc.want)
			}
		})
	}
}

// TestPermissions_OutboundRequestID_ValidPassedThrough — SEC-02 happy
// path: a valid X-Request-ID is forwarded verbatim and produces NO Warn
// log line.
func TestPermissions_OutboundRequestID_ValidPassedThrough(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	var gotRequestID string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotRequestID = r.Header.Get("X-Request-ID")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c := New(Config{
		BackendURL:     srv.URL,
		RequestTimeout: 2 * time.Second,
	}, Deps{Logger: logger, Metrics: metrics.New()})

	const wantID = "valid-id.123_DEF"
	_, err := c.Permissions(context.Background(), "client-tok", "users:42", wantID)
	if err != nil {
		t.Fatalf("Permissions returned error: %v", err)
	}

	mu.Lock()
	got := gotRequestID
	mu.Unlock()
	if got != wantID {
		t.Errorf("backend saw X-Request-ID = %q; want %q (verbatim)", got, wantID)
	}
	if strings.Contains(buf.String(), "outbound X-Request-ID failed validation") {
		t.Errorf("unexpected Warn log for valid request ID: %q", buf.String())
	}
}

// TestValidOutboundRequestID — table-driven cases for the boundary branches
// in validOutboundRequestID (length cap, empty string, regex misses, the
// valid charset spectrum). Locks the coverage of every branch in the
// helper so the per-package gate stays above 85 % even without the
// substitute-fresh-ID path being exercised end-to-end.
func TestValidOutboundRequestID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want bool
	}{
		{"", false},                       // empty
		{"abc", true},                     // basic alnum
		{"abc.123-DEF_xyz", true},         // full charset
		{strings.Repeat("a", 128), true},  // exactly at cap
		{strings.Repeat("a", 129), false}, // over cap
		{"has spaces", false},             // regex miss
		{"has\ttab", false},               // regex miss
		{"unicode—😀—chars", false},        // non-ASCII
		{`"; alert(1); //`, false},        // adversarial
	}
	for _, tc := range cases {
		if got := validOutboundRequestID(tc.in); got != tc.want {
			t.Errorf("validOutboundRequestID(%q) = %v; want %v", tc.in, got, tc.want)
		}
	}
}

func TestClient_New_PreTouchesAllResultLabels(t *testing.T) {
	t.Parallel()
	mc := metrics.New()
	_ = New(Config{BackendURL: "http://x", RequestTimeout: time.Second}, Deps{Logger: zerolog.Nop(), Metrics: mc})
	for _, label := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		// gatherCounter returns 0 for both "missing" and "zero". A series
		// must be present after pre-touch — verify via Gather() families.
		families, err := mc.Gatherer().Gather()
		if err != nil {
			t.Fatalf("Gather: %v", err)
		}
		found := false
		for _, fam := range families {
			if fam.GetName() != "walera_auth_requests_total" {
				continue
			}
			for _, m := range fam.GetMetric() {
				if matchLabel(m, "result", label) {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("walera_auth_requests_total{result=%s} series missing — pre-touch regressed", label)
		}
	}
}

// TestClient_SetBreaker_NilSubstitutesNop asserts SetBreaker(nil) installs
// nopBreaker{} (not a literal nil) so subsequent c.bk.* calls remain
// non-panicking. Asserted via the BreakerHook contract surface rather than
// pointer-equality (interface satisfaction is the contract).
func TestClient_SetBreaker_NilSubstitutesNop(t *testing.T) {
	t.Parallel()
	// Construct a Client with a real fakeBreaker initially, then overwrite
	// via SetBreaker(nil). The post-condition: c.bk.RecordResult must not
	// panic and c.bk.Allow() must return true (nopBreaker's contract).
	fb := &fakeBreaker{}
	c := New(Config{
		BackendURL:     "http://x",
		RequestTimeout: time.Second,
	}, Deps{Logger: zerolog.Nop(), Breaker: fb, Metrics: metrics.New()})

	c.SetBreaker(nil)

	// Behavior assertion: nopBreaker contract.
	c.bk.RecordResult(false) // must not panic
	c.bk.RecordResult(true)  // must not panic
	if got := c.bk.Allow(); !got {
		t.Errorf("c.bk.Allow() after SetBreaker(nil) = %v; want true (nopBreaker)", got)
	}
	// And the previously-installed fakeBreaker must NOT have seen the calls.
	if _, ok := fb.Last(); ok {
		t.Errorf("fakeBreaker was unexpectedly called after SetBreaker(nil): results=%v", fb.results)
	}
}

// TestClient_SetBreaker_InstallsHook asserts SetBreaker(hook) installs the
// hook so subsequent Client paths route through it. The Client is built with
// Deps.Breaker: nil (substituted to nopBreaker{} by auth.New) so the
// assertion proves the swap happens.
func TestClient_SetBreaker_InstallsHook(t *testing.T) {
	t.Parallel()
	c := New(Config{
		BackendURL:     "http://x",
		RequestTimeout: time.Second,
	}, Deps{Logger: zerolog.Nop(), Breaker: nil, Metrics: metrics.New()})

	fb := &fakeBreaker{}
	c.SetBreaker(fb)

	// Drive a direct RecordResult through the installed hook to prove the
	// swap. (Going through Permissions would require a httptest server +
	// network round-trip; the BreakerHook contract is what SetBreaker
	// installs, so asserting via that surface is the minimal proof.)
	c.bk.RecordResult(true)
	c.bk.RecordResult(false)

	last, ok := fb.Last()
	if !ok {
		t.Fatalf("fakeBreaker.Last(): got (_, false); want a recorded result")
	}
	if last { // last RecordResult was false
		t.Errorf("fakeBreaker last result: got %v; want false", last)
	}
}
