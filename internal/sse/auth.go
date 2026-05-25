// Package sse — SSE handshake gate sequence (see INVARIANTS.md §10) and
// the auth-flow helpers that translate gate failures into HTTP responses.
package sse

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/walera/walera/internal/auth"
)

// reasonBody is the body shape for writeJSONReason: {"reason":"<code>"}\n.
type reasonBody struct {
	Reason string `json:"reason"`
}

// requestIDRe matches the charset for X-Request-ID values: ASCII
// alphanumerics plus period, underscore, hyphen. Pre-compiled at init;
// prevents log + auth-backend amplification from arbitrarily large
// client-supplied IDs.
var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// validRequestID reports whether s is a well-formed X-Request-ID: non-
// empty, <= 128 bytes, and matches requestIDRe.
func validRequestID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	return requestIDRe.MatchString(s)
}

// newRequestID generates a 32-char hex X-Request-ID when the client did
// not supply one. crypto/rand failure on Linux is unrecoverable.
func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("sse: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}

// clientIP returns the effective client IP for rate-limiting. When the
// immediate peer is in limits.trusted_proxies the X-Forwarded-For
// header is parsed right-to-left; the first IP NOT in the allowlist
// is returned (the real client behind a trusted-proxy chain).
// Malformed XFF entries fall back to the peer host (trusted, bounded
// as a rate-limit key) so attacker-controlled garbage cannot poison the
// per-IP rate-limit map. Entire-chain-trusted falls back to the
// leftmost parsed IP. Returned IPs are emitted in canonical
// net.IP.String() form so map keys are normalised.
func (h *Handler) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peerIP := net.ParseIP(host)
	if peerIP == nil || !h.limits.IsTrustedProxy(peerIP) {
		return host
	}
	// RFC 7230 §3.2.2 allows the same header sent multiple times;
	// r.Header.Values preserves all instances. r.Header.Get returns only
	// the first, which would silently drop subsequent entries.
	xffValues := r.Header.Values("X-Forwarded-For")
	if len(xffValues) == 0 {
		return host
	}
	xff := strings.Join(xffValues, ",")
	if xff == "" {
		return host
	}
	parts := strings.Split(xff, ",")
	// Right-to-left: skip trusted hops, return the first untrusted IP.
	// Track the leftmost parsed IP as claimed-client fallback.
	var leftmostParsed net.IP
	for i := len(parts) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(parts[i])
		if candidate == "" {
			continue
		}
		// Strip surrounding brackets for IPv6 ([2001:db8::1] → 2001:db8::1).
		if len(candidate) >= 2 && candidate[0] == '[' && candidate[len(candidate)-1] == ']' {
			candidate = candidate[1 : len(candidate)-1]
		}
		ip := net.ParseIP(candidate)
		if ip == nil {
			// Malformed entry — do NOT return attacker-controlled string;
			// fall back to peer host.
			return host
		}
		leftmostParsed = ip
		if !h.limits.IsTrustedProxy(ip) {
			return ip.String()
		}
	}
	if leftmostParsed != nil {
		return leftmostParsed.String()
	}
	return host
}

// writeStatusOnlyError writes a status-code-only response (no body) with
// optional Retry-After header. Used by gates 1, 2, 4 and the 5xx auth
// response. Vary: Origin is set unconditionally via handleCORS.
func (h *Handler) writeStatusOnlyError(w http.ResponseWriter, r *http.Request, status, retryAfterSeconds int) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	if retryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	w.WriteHeader(status)
}

// writeAuthError translates a typed auth error into the HTTP response:
// ErrUnauthorized → 401, ErrForbidden → 403, ErrNotFound → 404 (each
// forwards the upstream body verbatim), ErrUnavailable / unknown → 503 +
// Retry-After:5 with no body. The upstream body is bounded to ≤ 64 KiB
// by the auth client's LimitReader; forwarding it cannot exhaust client
// buffers.
func (h *Handler) writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	switch e := err.(type) {
	case *auth.ErrUnauthorized:
		handleCORS(w, r, h.cfg.CORSOrigins)
		writeNoSniff(w)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write(e.Body)
	case *auth.ErrForbidden:
		handleCORS(w, r, h.cfg.CORSOrigins)
		writeNoSniff(w)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(e.Body)
	case *auth.ErrNotFound:
		handleCORS(w, r, h.cfg.CORSOrigins)
		writeNoSniff(w)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(e.Body)
	default:
		// Any ErrUnavailable / unknown — backend unavailable; breaker has
		// already counted. Client retries with Retry-After.
		_ = e // shut up exhaustive
		h.writeStatusOnlyError(w, r, http.StatusServiceUnavailable, 5)
	}
}

// writeJSONReason writes an application/json error body of the form
// {"reason":"..."}. Used by gates 5 and 6 (and the auth-revoked SSE
// error frame mirrors this shape on the wire). Typed struct +
// json.NewEncoder eliminates byte-string concatenation.
func (h *Handler) writeJSONReason(w http.ResponseWriter, r *http.Request, status int, reason string) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(reasonBody{Reason: reason})
}

// handshakeResult carries the values produced by a successful runHandshake
// pass to the post-gate writer loop. Only valid when runHandshake returns
// ok=true.
type handshakeResult struct {
	authMap         *auth.Whitelist
	token           string
	requestID       string
	clientIP        string
	userID          string
	globalAcquired  bool
	perUserAcquired bool
}

// runHandshake executes the five SSE handshake gates in order (see
// INVARIANTS.md §10) and writes the matching HTTP error response on the
// first failure. On success (ok=true) the caller releases limits via
// ReleaseGlobal / ReleasePerUser using the flags returned in
// handshakeResult — runHandshake never releases on the success path. On
// partial-success failure (e.g. gate 4 fails after gate 1 succeeded),
// the relevant *Acquired flag is still set so deferred releases fire.
func (h *Handler) runHandshake(w http.ResponseWriter, r *http.Request, table, channelStr string) (handshakeResult, bool) {
	var res handshakeResult

	// --- Gate 1: global semaphore ---
	if !h.limits.AcquireGlobal() {
		h.writeStatusOnlyError(w, r, http.StatusServiceUnavailable, 5)
		return res, false
	}
	res.globalAcquired = true

	// --- Gate 2: pre-auth per-IP rate ---
	ip := h.clientIP(r)
	res.clientIP = ip
	if !h.limits.AllowPreAuthRate(ip) {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 1)
		return res, false
	}

	// --- Gate 3: auth backend ---
	authHdr := r.Header.Get("Authorization")
	token, hasBearer := strings.CutPrefix(authHdr, "Bearer ")
	if !hasBearer || token == "" {
		// Missing bearer: respond 401 with no upstream body.
		h.writeStatusOnlyError(w, r, http.StatusUnauthorized, 0)
		return res, false
	}
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = newRequestID()
	} else if !validRequestID(requestID) {
		// Reject malformed client-supplied IDs BEFORE any logging or
		// auth-backend forwarding to prevent amplification.
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_request_id")
		return res, false
	}

	authMap, err := h.authClient.Permissions(r.Context(), token, channelStr, requestID)
	if err != nil {
		h.writeAuthError(w, r, err)
		return res, false
	}

	// --- Gate 4: per-user concurrent ---
	userID := authMap.UserID
	res.userID = userID
	if ok, _ := h.limits.AcquirePerUser(userID); !ok {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 0)
		return res, false
	}
	res.perUserAcquired = true

	// --- Gate 5: table in whitelist ---
	if _, ok := authMap.Tables[table]; !ok {
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return res, false
	}

	res.authMap = authMap
	res.token = token
	res.requestID = requestID
	return res, true
}
