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

type reasonBody struct {
	Reason string `json:"reason"`
}

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validRequestID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	return requestIDRe.MatchString(s)
}

func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("sse: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}

// clientIP is the TCP peer; it only feeds the client_ip log field.
func (h *Handler) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (h *Handler) writeStatusOnlyError(w http.ResponseWriter, r *http.Request, status, retryAfterSeconds int) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	if retryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	w.WriteHeader(status)
}

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

		_ = e
		h.writeStatusOnlyError(w, r, http.StatusServiceUnavailable, 5)
	}
}

func (h *Handler) writeJSONReason(w http.ResponseWriter, r *http.Request, status int, reason string) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(reasonBody{Reason: reason})
}

type handshakeResult struct {
	authMap         *auth.Whitelist
	requestID       string
	bearer          string
	fwd             auth.ForwardedAuth
	clientIP        string
	userID          string
	globalAcquired  bool
	perUserAcquired bool
}

// authenticate writes the error response on failure. Admission limits and table
// authorization are the caller's job.
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request, channelStr string, res *handshakeResult) bool {
	authHdr := r.Header.Get("Authorization")
	token := ""
	if t, ok := strings.CutPrefix(authHdr, "Bearer "); ok {
		token = t
	}
	fwd := h.authClient.ForwardedFromRequest(r)
	// The subscriber holds the copies it replays; the request should not keep
	// a second copy, whichever way the handshake ends.
	defer func() {
		r.Header.Del("Authorization")
		r.Header.Del("Cookie")
		for name := range fwd.Headers {
			r.Header.Del(name)
		}
	}()
	if token == "" && fwd.Empty() {

		h.writeStatusOnlyError(w, r, http.StatusUnauthorized, 0)
		return false
	}
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = newRequestID()
	} else if !validRequestID(requestID) {

		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_request_id")
		return false
	}

	authMap, err := h.authClient.OpenSession(r.Context(), token, fwd, channelStr, requestID)
	if err != nil {
		h.writeAuthError(w, r, err)
		return false
	}
	res.authMap = authMap
	res.requestID = requestID
	res.bearer = token
	res.fwd = fwd
	return true
}

func (h *Handler) runHandshake(w http.ResponseWriter, r *http.Request, table, channelStr string) (handshakeResult, bool) {
	var res handshakeResult

	if !h.limits.AcquireGlobal() {
		h.writeStatusOnlyError(w, r, http.StatusServiceUnavailable, 5)
		return res, false
	}
	res.globalAcquired = true

	ip := h.clientIP(r)
	res.clientIP = ip

	if !h.authenticate(w, r, channelStr, &res) {
		return res, false
	}

	userID := res.authMap.UserID
	res.userID = userID
	if ok, _ := h.limits.AcquirePerUser(userID); !ok {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 0)
		return res, false
	}
	res.perUserAcquired = true

	if _, ok := res.authMap.Tables[table]; !ok {
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return res, false
	}
	return res, true
}
