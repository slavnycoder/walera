package sse

import (
	"net/http"
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
