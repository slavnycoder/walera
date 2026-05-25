// Package sse — Origin allowlist + Vary: Origin + OPTIONS preflight.
// See doc.go invariant 4.
package sse

import (
	"net/http"
	"net/url"
	"strings"
)

// canonicalOrigin returns lowercase-scheme + "://" + lowercase-host
// and ok=true, or ("", false) when s has no scheme or host.
// Port-default normalisation (":80"/":443") is NOT performed — operators
// match port-to-port (documented in README.md).
func canonicalOrigin(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), true
}

// handleCORS sets Vary: Origin unconditionally; when r.Header["Origin"]
// canonicalises to an entry in allowedOrigins, reflects the original
// (byte-for-byte) Origin via ACAO + ACAC + Timing-Allow-Origin.
// allowedOrigins is pre-canonicalised at config-load time. Returns
// (allowed, origin) — origin is the literal incoming header value
// regardless of match. MUST be called on every SSE response so Vary
// is always present.
func handleCORS(w http.ResponseWriter, r *http.Request, allowedOrigins []string) (allowed bool, origin string) {
	// Add (not Set) preserves any prior Vary value.
	w.Header().Add("Vary", "Origin")

	origin = r.Header.Get("Origin")
	if origin == "" {
		return false, ""
	}
	canon, ok := canonicalOrigin(origin)
	if !ok {
		return false, origin
	}
	for _, a := range allowedOrigins {
		if a == canon {
			// Reflect the ORIGINAL request Origin byte-for-byte —
			// canonicalisation is for membership only.
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			// Timing-Allow-Origin lets the cross-origin h2c probe read
			// PerformanceResourceTiming.nextHopProtocol.
			w.Header().Set("Timing-Allow-Origin", origin)
			return true, origin
		}
	}
	return false, origin
}

// writeNoSniff sets X-Content-Type-Options: nosniff. Defense-in-depth
// against MIME-sniff XSS on JSON error bodies.
func writeNoSniff(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

// handlePreflight handles OPTIONS preflight: handleCORS + the three
// preflight-specific headers + 204. Returns true on OPTIONS; false
// otherwise (caller continues normal handling).
func handlePreflight(w http.ResponseWriter, r *http.Request, allowedOrigins []string) bool {
	if r.Method != http.MethodOptions {
		return false
	}
	handleCORS(w, r, allowedOrigins)
	writeNoSniff(w)
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, X-Request-ID, Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400")
	w.WriteHeader(http.StatusNoContent)
	return true
}
