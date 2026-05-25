package sse

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORS_NoOrigin_VaryStillSet(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	w := httptest.NewRecorder()

	allowed, origin := handleCORS(w, r, []string{"https://app.example"})
	if allowed {
		t.Errorf("allowed = true; want false (no Origin header on request)")
	}
	if origin != "" {
		t.Errorf("origin = %q; want %q", origin, "")
	}
	if got := w.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q; want %q (must be set unconditionally)", got, "Origin")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q; want empty (no Origin → no ACAO)", got)
	}

	if got := w.Header().Get("Timing-Allow-Origin"); got != "" {
		t.Errorf("Timing-Allow-Origin = %q; want empty (no Origin → no TAO)", got)
	}
}

func TestCORS_OriginMatching_SetsACAOAndACAC(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "https://app.example")
	w := httptest.NewRecorder()

	allowed, origin := handleCORS(w, r, []string{"https://app.example", "https://admin.example"})
	if !allowed {
		t.Errorf("allowed = false; want true")
	}
	if origin != "https://app.example" {
		t.Errorf("origin = %q; want %q", origin, "https://app.example")
	}
	if got := w.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q; want %q", got, "Origin")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Errorf("Access-Control-Allow-Origin = %q; want %q", got, "https://app.example")
	}
	if got := w.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Access-Control-Allow-Credentials = %q; want %q", got, "true")
	}

	if got := w.Header().Get("Timing-Allow-Origin"); got != "https://app.example" {
		t.Errorf("Timing-Allow-Origin = %q; want %q", got, "https://app.example")
	}
}

func TestCORS_OriginNotInAllowlist_VaryOnly(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()

	allowed, origin := handleCORS(w, r, []string{"https://app.example"})
	if allowed {
		t.Errorf("allowed = true; want false (Origin not in allowlist)")
	}
	if origin != "https://evil.example" {
		t.Errorf("origin = %q; want literal request Origin", origin)
	}
	if got := w.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q; want %q", got, "Origin")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q; want empty (origin not in allowlist)", got)
	}

	if got := w.Header().Get("Timing-Allow-Origin"); got != "" {
		t.Errorf("Timing-Allow-Origin = %q; want empty (origin not in allowlist)", got)
	}
}

func TestHandleCORS_TimingAllowOrigin_EnablesH2cProbe(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.Header.Set("Origin", "http://localhost:8081")
	w := httptest.NewRecorder()

	allowed, _ := handleCORS(w, r, []string{"http://localhost:8081"})
	if !allowed {
		t.Fatalf("allowed = false; want true (UI-11 prerequisite: allowlist match)")
	}
	if got := w.Header().Get("Timing-Allow-Origin"); got != "http://localhost:8081" {
		t.Errorf("Timing-Allow-Origin = %q; want %q (UI-11: h2c probe needs TAO to read nextHopProtocol)",
			got, "http://localhost:8081")
	}
}

func TestPreflight_OptionsWithMatchingOrigin_204(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodOptions, "/sse/v1/users/42", nil)
	r.Header.Set("Origin", "https://app.example")
	w := httptest.NewRecorder()

	handled := handlePreflight(w, r, []string{"https://app.example"})
	if !handled {
		t.Fatalf("handled = false; want true for OPTIONS request")
	}
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d; want %d", w.Code, http.StatusNoContent)
	}
	hdr := w.Header()
	if hdr.Get("Vary") != "Origin" {
		t.Errorf("Vary = %q; want %q", hdr.Get("Vary"), "Origin")
	}
	if hdr.Get("Access-Control-Allow-Origin") != "https://app.example" {
		t.Errorf("Access-Control-Allow-Origin = %q; want %q", hdr.Get("Access-Control-Allow-Origin"), "https://app.example")
	}
	if hdr.Get("Access-Control-Allow-Methods") != "GET, OPTIONS" {
		t.Errorf("Access-Control-Allow-Methods = %q; want %q", hdr.Get("Access-Control-Allow-Methods"), "GET, OPTIONS")
	}
	if hdr.Get("Access-Control-Allow-Headers") == "" {
		t.Errorf("Access-Control-Allow-Headers = empty; want non-empty")
	}
	if hdr.Get("Access-Control-Max-Age") != "86400" {
		t.Errorf("Access-Control-Max-Age = %q; want %q", hdr.Get("Access-Control-Max-Age"), "86400")
	}
}

func TestPreflight_OptionsNoOrigin_204VaryOnly(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodOptions, "/sse/v1/users/42", nil)
	w := httptest.NewRecorder()

	handled := handlePreflight(w, r, []string{"https://app.example"})
	if !handled {
		t.Fatalf("handled = false; want true for OPTIONS request")
	}
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d; want %d", w.Code, http.StatusNoContent)
	}
	if w.Header().Get("Vary") != "Origin" {
		t.Errorf("Vary = %q; want %q", w.Header().Get("Vary"), "Origin")
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("Access-Control-Allow-Origin = %q; want empty (no Origin)", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestPreflight_NonOptions_NotHandled(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	w := httptest.NewRecorder()

	handled := handlePreflight(w, r, []string{"https://app.example"})
	if handled {
		t.Errorf("handled = true; want false for GET request")
	}

	if w.Header().Get("Vary") != "" {
		t.Errorf("Vary = %q; want empty (preflight should not set headers on non-OPTIONS)", w.Header().Get("Vary"))
	}
}
