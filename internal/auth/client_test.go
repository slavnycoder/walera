package auth

import (
	"bytes"
	"context"
	"encoding/json"
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

func newTestClient(t *testing.T, baseURL string, timeout time.Duration) (*Client, *fakeBreaker, *metrics.Registry) {
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

const validBody = `{"user_id":"u1","tables":{"users":["id","name"]},"ttl_seconds":60}`

func TestClient_RefreshPermissions_StatusPropagation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		status       int
		body         string
		wantLabel    string
		wantReachable bool
		check        func(t *testing.T, err error)
	}{
		{
			name:          "401",
			status:        http.StatusUnauthorized,
			body:          "forbidden",
			wantLabel:     "unauthorized",
			wantReachable: true,
			check: func(t *testing.T, err error) {
				var ue *ErrUnauthorized
				if !errors.As(err, &ue) {
					t.Fatalf("err = %v (%T); want *ErrUnauthorized", err, err)
				}
				if string(ue.Body) != "forbidden" {
					t.Errorf("Body = %q; want %q", ue.Body, "forbidden")
				}
			},
		},
		{
			name:          "403",
			status:        http.StatusForbidden,
			body:          "nope",
			wantLabel:     "forbidden",
			wantReachable: true,
			check: func(t *testing.T, err error) {
				var fe *ErrForbidden
				if !errors.As(err, &fe) {
					t.Fatalf("err = %v (%T); want *ErrForbidden", err, err)
				}
				if string(fe.Body) != "nope" {
					t.Errorf("Body = %q; want %q", fe.Body, "nope")
				}
			},
		},
		{
			name:          "404",
			status:        http.StatusNotFound,
			body:          "missing",
			wantLabel:     "not_found",
			wantReachable: true,
			check: func(t *testing.T, err error) {
				var ne *ErrNotFound
				if !errors.As(err, &ne) {
					t.Fatalf("err = %v (%T); want *ErrNotFound", err, err)
				}
				if string(ne.Body) != "missing" {
					t.Errorf("Body = %q; want %q", ne.Body, "missing")
				}
			},
		},
		{
			name:          "5xx",
			status:        http.StatusInternalServerError,
			wantLabel:     "unavailable",
			wantReachable: false,
			check: func(t *testing.T, err error) {
				var ue *ErrUnavailable
				if !errors.As(err, &ue) {
					t.Fatalf("err = %v (%T); want *ErrUnavailable", err, err)
				}
			},
		},
		{
			name:          "malformed_json",
			status:        http.StatusOK,
			body:          "not json",
			wantLabel:     "unavailable",
			wantReachable: false,
			check: func(t *testing.T, err error) {
				var ue *ErrUnavailable
				if !errors.As(err, &ue) {
					t.Fatalf("err = %v; want ErrUnavailable", err)
				}
			},
		},
		{
			name:          "invalid_shape",
			status:        http.StatusOK,
			body:          `{"user_id":""}`,
			wantLabel:     "unavailable",
			wantReachable: false,
			check: func(t *testing.T, err error) {
				var ue *ErrUnavailable
				if !errors.As(err, &ue) {
					t.Fatalf("err = %v; want ErrUnavailable", err)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
				}
			}))
			t.Cleanup(srv.Close)

			c, fb, mc := newTestClient(t, srv.URL, 2*time.Second)
			_, err := c.RefreshPermissions(context.Background(), "u1", "users:42", "req-"+tc.name)
			tc.check(t, err)

			last, ok := fb.Last()
			if !ok {
				t.Fatalf("breaker not recorded")
			}
			if last != tc.wantReachable {
				t.Errorf("breaker last = %v; want %v", last, tc.wantReachable)
			}
			if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", tc.wantLabel); v != 1 {
				t.Errorf("counter{result=%s} = %v; want 1", tc.wantLabel, v)
			}
		})
	}
}

func TestClient_RefreshPermissions_NetworkError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := srv.URL
	srv.Close()

	c, fb, mc := newTestClient(t, url, 2*time.Second)
	_, err := c.RefreshPermissions(context.Background(), "u1", "users:42", "req-net")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v; want ErrUnavailable", err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker = (%v,%v); want (false,true)", last, ok)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unavailable"); v != 1 {
		t.Errorf("counter: got %v; want 1", v)
	}
}

func TestClient_RefreshPermissions_TimeoutHonored(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c, fb, _ := newTestClient(t, srv.URL, 50*time.Millisecond)
	_, err := c.RefreshPermissions(context.Background(), "u1", "ch:1", "req-timeout")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v; want ErrUnavailable (timeout)", err)
	}
	if last, ok := fb.Last(); !ok || last {
		t.Errorf("breaker = (%v,%v); want (false,true)", last, ok)
	}
}

func TestClient_CheckAuth_ReachableViaSentinel(t *testing.T) {
	t.Parallel()

	var sawUserID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/permissions" {
			t.Errorf("CheckAuth hit path %q; want /auth/permissions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("CheckAuth must not send Authorization; got %q", got)
		}
		if r.Header.Get("X-Walera-Sig") == "" {
			t.Errorf("CheckAuth must send X-Walera-Sig")
		}

		var body refreshRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		sawUserID = body.UserID
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user_id":"_health","tables":{"_health":["id"]},"ttl_seconds":60}`))
	}))
	t.Cleanup(srv.Close)

	c, _, _ := newTestClient(t, srv.URL, 2*time.Second)
	if err := c.CheckAuth(context.Background()); err != nil {
		t.Fatalf("CheckAuth: %v", err)
	}
	if sawUserID != HealthSentinelUserID {
		t.Errorf("sentinel user_id = %q; want %q", sawUserID, HealthSentinelUserID)
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

func newSigningOnlyClient(t *testing.T, baseURL string, logger zerolog.Logger) *Client {
	t.Helper()
	return New(Config{
		BackendURL:     baseURL,
		RequestTimeout: 2 * time.Second,
		Signing: SigningConfig{
			Secret: strings.Repeat("k", 64),
			Kid:    "v1",
		},
	}, Deps{Logger: logger, Metrics: metrics.New()})
}

func TestRefreshPermissions_OutboundRequestID_InvalidSubstituted(t *testing.T) {
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

	c := newSigningOnlyClient(t, srv.URL, logger)
	_, err := c.RefreshPermissions(context.Background(), "u1", "users:42", "has spaces and \t tabs")
	if err != nil {
		t.Fatalf("RefreshPermissions: %v", err)
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
	if !strings.Contains(logOut, `"invalid_len":`) {
		t.Errorf("WR-02: missing invalid_len; got %q", logOut)
	}
	if !strings.Contains(logOut, `"invalid_id_truncated":`) {
		t.Errorf("WR-02: missing invalid_id_truncated; got %q", logOut)
	}
	if !strings.Contains(logOut, `"substitute_id":"`+got+`"`) {
		t.Errorf("WR-02: substitute_id mismatch with backend-seen %q; got %q", got, logOut)
	}
}

func TestRefreshPermissions_OutboundRequestID_Truncated_LogsBoundedOriginal(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)

	c := newSigningOnlyClient(t, srv.URL, logger)
	bad := strings.Repeat("A", 100) + " " + strings.Repeat("B", 99)
	_, err := c.RefreshPermissions(context.Background(), "u1", "users:42", bad)
	if err != nil {
		t.Fatalf("RefreshPermissions: %v", err)
	}

	logOut := buf.String()
	wantLen := fmt.Sprintf(`"invalid_len":%d`, len(bad))
	if !strings.Contains(logOut, wantLen) {
		t.Errorf("WR-02: invalid_len must record full length %d; got %q", len(bad), logOut)
	}
	wantTrunc := `"invalid_id_truncated":"` + strings.Repeat("A", 16) + `..."`
	if !strings.Contains(logOut, wantTrunc) {
		t.Errorf("WR-02: invalid_id_truncated must be 16-byte prefix+\"...\"; got %q", logOut)
	}
}

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

func TestRefreshPermissions_OutboundRequestID_ValidPassedThrough(t *testing.T) {
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

	c := newSigningOnlyClient(t, srv.URL, logger)
	const wantID = "valid-id.123_DEF"
	_, err := c.RefreshPermissions(context.Background(), "u1", "users:42", wantID)
	if err != nil {
		t.Fatalf("RefreshPermissions: %v", err)
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

func TestValidOutboundRequestID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"abc", true},
		{"abc.123-DEF_xyz", true},
		{strings.Repeat("a", 128), true},
		{strings.Repeat("a", 129), false},
		{"has spaces", false},
		{"has\ttab", false},
		{"unicode—😀—chars", false},
		{`"; alert(1); //`, false},
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

func TestClient_SetBreaker_NilSubstitutesNop(t *testing.T) {
	t.Parallel()

	fb := &fakeBreaker{}
	c := New(Config{
		BackendURL:     "http://x",
		RequestTimeout: time.Second,
	}, Deps{Logger: zerolog.Nop(), Breaker: fb, Metrics: metrics.New()})

	c.SetBreaker(nil)

	c.bk.RecordResult(false)
	c.bk.RecordResult(true)
	if got := c.bk.Allow(); !got {
		t.Errorf("c.bk.Allow() after SetBreaker(nil) = %v; want true (nopBreaker)", got)
	}

	if _, ok := fb.Last(); ok {
		t.Errorf("fakeBreaker was unexpectedly called after SetBreaker(nil): results=%v", fb.results)
	}
}

func TestClient_SetBreaker_InstallsHook(t *testing.T) {
	t.Parallel()
	c := New(Config{
		BackendURL:     "http://x",
		RequestTimeout: time.Second,
	}, Deps{Logger: zerolog.Nop(), Breaker: nil, Metrics: metrics.New()})

	fb := &fakeBreaker{}
	c.SetBreaker(fb)

	c.bk.RecordResult(true)
	c.bk.RecordResult(false)

	last, ok := fb.Last()
	if !ok {
		t.Fatalf("fakeBreaker.Last(): got (_, false); want a recorded result")
	}
	if last {
		t.Errorf("fakeBreaker last result: got %v; want false", last)
	}
}
