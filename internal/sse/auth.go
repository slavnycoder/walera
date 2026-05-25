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

func (h *Handler) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peerIP := net.ParseIP(host)
	if peerIP == nil || !h.limits.IsTrustedProxy(peerIP) {
		return host
	}

	xffValues := r.Header.Values("X-Forwarded-For")
	if len(xffValues) == 0 {
		return host
	}
	xff := strings.Join(xffValues, ",")
	if xff == "" {
		return host
	}
	parts := strings.Split(xff, ",")

	var leftmostParsed net.IP
	for i := len(parts) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(parts[i])
		if candidate == "" {
			continue
		}

		if len(candidate) >= 2 && candidate[0] == '[' && candidate[len(candidate)-1] == ']' {
			candidate = candidate[1 : len(candidate)-1]
		}
		ip := net.ParseIP(candidate)
		if ip == nil {

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
	token           string
	requestID       string
	clientIP        string
	userID          string
	globalAcquired  bool
	perUserAcquired bool
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
	if !h.limits.AllowPreAuthRate(ip) {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 1)
		return res, false
	}

	authHdr := r.Header.Get("Authorization")
	token, hasBearer := strings.CutPrefix(authHdr, "Bearer ")
	if !hasBearer || token == "" {

		h.writeStatusOnlyError(w, r, http.StatusUnauthorized, 0)
		return res, false
	}
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = newRequestID()
	} else if !validRequestID(requestID) {

		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_request_id")
		return res, false
	}

	authMap, err := h.authClient.Permissions(r.Context(), token, channelStr, requestID)
	if err != nil {
		h.writeAuthError(w, r, err)
		return res, false
	}

	userID := authMap.UserID
	res.userID = userID
	if ok, _ := h.limits.AcquirePerUser(userID); !ok {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 0)
		return res, false
	}
	res.perUserAcquired = true

	if !h.limits.AllowPerUserRate(userID) {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 1)
		return res, false
	}

	if _, ok := authMap.Tables[table]; !ok {
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return res, false
	}

	res.authMap = authMap
	res.token = token
	res.requestID = requestID
	return res, true
}
