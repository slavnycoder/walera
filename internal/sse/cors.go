package sse

import (
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
)

func canonicalOrigin(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), true
}

func handleCORS(w http.ResponseWriter, r *http.Request, allowedOrigins []string) (allowed bool, origin string) {

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

			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")

			w.Header().Set("Timing-Allow-Origin", origin)
			return true, origin
		}
	}
	return false, origin
}

func writeNoSniff(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func handlePreflight(w http.ResponseWriter, r *http.Request, allowedOrigins []string, allowHeaders string) bool {
	return handlePreflightMethods(w, r, allowedOrigins, "GET, OPTIONS", allowHeaders)
}

// handlePreflightMethods writes a CORS preflight response advertising the given
// Access-Control-Allow-Methods and Access-Control-Allow-Headers strings. The SSE
// GET routes pass "GET, OPTIONS"; the POST /sse/v1/refresh route passes
// "POST, OPTIONS". Both pass h.corsAllowHeaders. All other preflight headers are
// identical so both routes share one source of truth.
func handlePreflightMethods(w http.ResponseWriter, r *http.Request, allowedOrigins []string, methods, allowHeaders string) bool {
	if r.Method != http.MethodOptions {
		return false
	}
	handleCORS(w, r, allowedOrigins)
	writeNoSniff(w)
	w.Header().Set("Access-Control-Allow-Methods", methods)
	w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
	w.Header().Set("Access-Control-Max-Age", "86400")
	w.WriteHeader(http.StatusNoContent)
	return true
}

// buildAllowHeaders builds the Access-Control-Allow-Headers value for the CORS
// preflight: the base set every route needs plus any configured forwarded auth
// headers (auth.forwarded_headers, e.g. X-Tenant), so a browser fetch on
// POST /sse/v1/refresh can actually send them. Forwarded headers are canonicalized
// via textproto.CanonicalMIMEHeaderKey and deduped against the base set (and each
// other). With no forwarded headers configured the result is the base string
// unchanged, so existing deployments see no difference.
func buildAllowHeaders(forwarded []string) string {
	const base = "Authorization, X-Request-ID, Content-Type"
	if len(forwarded) == 0 {
		return base
	}
	seen := map[string]struct{}{
		textproto.CanonicalMIMEHeaderKey("Authorization"): {},
		textproto.CanonicalMIMEHeaderKey("X-Request-ID"):  {},
		textproto.CanonicalMIMEHeaderKey("Content-Type"):  {},
	}
	parts := []string{base}
	for _, h := range forwarded {
		canon := textproto.CanonicalMIMEHeaderKey(h)
		if canon == "" {
			continue
		}
		if _, dup := seen[canon]; dup {
			continue
		}
		seen[canon] = struct{}{}
		parts = append(parts, canon)
	}
	return strings.Join(parts, ", ")
}
