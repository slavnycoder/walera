package sse

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/walera/walera/internal/auth"
)

// refreshBodyMaxBytes bounds the POST /sse/v1/refresh request body. The body is
// a tiny {"channel":"..."} object, so 1 KiB is generous.
const refreshBodyMaxBytes = 1024

type refreshRequest struct {
	Channel string `json:"channel"`
}

type refreshResponse struct {
	Refreshed int `json:"refreshed"`
}

// serveRefreshPreflight answers the CORS preflight for POST /sse/v1/refresh.
func (h *Handler) serveRefreshPreflight(w http.ResponseWriter, r *http.Request) {
	if !handlePreflightMethods(w, r, h.cfg.CORSOrigins, "POST, OPTIONS") {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// serveRefresh handles POST /sse/v1/refresh. The frontend sends a channel name
// in the JSON body; Walera authenticates the caller (the same identity path as
// the SSE handshake), authorizes them for the channel, and then synchronously
// re-fetches the channel's permissions from the auth backend and atomically
// swaps the whitelist for the caller's OWN live subscriber(s) on that channel —
// mid-stream, no reconnect. The actual refresh delegates to the same routine the
// TTL loop uses (auth.Subscriber.RefreshOnce -> the shared refreshAttempt), so it
// inherits the identical swap, revocation-drop, and breaker semantics; it runs a
// single attempt (no TTL backoff) and returns a hard result.
//
// Status codes:
//   - 200 {"refreshed":N}     the caller's N subscriber(s) were re-authorized and swapped
//   - 400 {"error":...}       malformed body / channel / request id
//   - 401                     no credentials presented
//   - 403 {"reason":...}      caller not authorized for the channel (not_allowed),
//     or the re-fetch revoked access and the subscriber was dropped (auth_revoked)
//   - 404 {"reason":"no_live_subscriber"}  the caller has no live stream on the channel
//   - 503 {"reason":"auth_unavailable"}    the backend re-fetch failed (Retry-After: 5)
func (h *Handler) serveRefresh(w http.ResponseWriter, r *http.Request) {
	// 1. Parse and validate the request body.
	r.Body = http.MaxBytesReader(w, r.Body, refreshBodyMaxBytes)
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	table, channelStr, ok := parseRefreshChannel(req.Channel)
	if !ok {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_channel")
		return
	}

	// 2. Authenticate the caller and authorize them for the channel. This mirrors
	//    runHandshake's identity subset exactly (Bearer cut + forwarded creds +
	//    OpenSession), including the immediate credential scrub.
	authHdr := r.Header.Get("Authorization")
	token := ""
	if t, found := strings.CutPrefix(authHdr, "Bearer "); found {
		token = t
	}
	fwd := h.authClient.ForwardedFromRequest(r)
	if token == "" && fwd.Empty() {
		h.writeStatusOnlyError(w, r, http.StatusUnauthorized, 0)
		return
	}
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = newRequestID()
	} else if !validRequestID(requestID) {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_request_id")
		return
	}

	authMap, err := h.authClient.OpenSession(r.Context(), token, fwd, channelStr, requestID)
	// Scrub forwarded credentials from our scope and the request header map ASAP,
	// for the same reason runHandshake does (auth.go): the refresh re-fetch
	// authenticates with HMAC over user_id, never these credentials.
	r.Header.Del("Authorization")
	r.Header.Del("Cookie")
	for name := range fwd.Headers {
		r.Header.Del(name)
	}
	token = ""
	fwd = auth.ForwardedAuth{}
	if err != nil {
		h.writeAuthError(w, r, err)
		return
	}
	if _, allowed := authMap.Tables[table]; !allowed {
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return
	}

	// 3. Synchronously refresh the caller's own live subscriber(s) on the channel.
	sum := h.authRegistry.RefreshOwnByChannel(r.Context(), channelStr, authMap.UserID)
	switch {
	case sum.Matched == 0:
		h.writeJSONReason(w, r, http.StatusNotFound, "no_live_subscriber")
	case sum.Revoked > 0:
		// The re-fetch determined the caller is no longer entitled to the channel;
		// the subscriber was dropped (auth_revoked), mirroring the TTL path.
		h.writeJSONReason(w, r, http.StatusForbidden, "auth_revoked")
	case sum.Unavailable > 0 || sum.Busy > 0:
		// Transient backend failure (or a refresh already in flight): the stream is
		// kept on its current whitelist; the caller may retry.
		h.writeReasonRetry(w, r, http.StatusServiceUnavailable, 5, "auth_unavailable")
	default:
		h.writeRefreshOK(w, r, sum.Swapped)
	}
}

// parseRefreshChannel validates a POST-body channel string and returns the
// authorization table plus the canonical channel string. It mirrors the GET-route
// rules: "table:pk" (exact) or "table:all" (wildcard). The returned channelStr is
// the exact string used to match live subscribers and to authorize via OpenSession.
func parseRefreshChannel(channel string) (table, channelStr string, ok bool) {
	tbl, id, found := strings.Cut(channel, ":")
	if !found || !validTableName(tbl) {
		return "", "", false
	}
	if id == "all" {
		return tbl, channel, true
	}
	if id == "" || len(id) > 256 {
		return "", "", false
	}
	return tbl, channel, true
}

func (h *Handler) writeRefreshOK(w http.ResponseWriter, r *http.Request, refreshed int) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(refreshResponse{Refreshed: refreshed})
}

func (h *Handler) writeReasonRetry(w http.ResponseWriter, r *http.Request, status, retryAfterSeconds int, reason string) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	if retryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(reasonBody{Reason: reason})
}
