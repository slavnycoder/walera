package sse

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
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
	if !handlePreflightMethods(w, r, h.cfg.CORSOrigins, "POST, OPTIONS", h.corsAllowHeaders) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// serveRefresh handles POST /sse/v1/refresh {"channel":"..."}: authenticates the
// caller via OpenSession (same path as the SSE handshake) and swaps the returned
// whitelist into the caller's own live subscriber(s) on that channel, mid-stream.
//
// Admission: only the global semaphore. The per-user counter caps concurrent
// streams and is held for a stream's lifetime, so taking it here would 429 a
// user with a full set of streams.
//
// Status codes:
//   - 200 {"refreshed":N}                  N subscriber(s) swapped
//   - 400 {"error":...}                    malformed body / channel / request id
//   - 401, 403, 503                        OpenSession failure, as in the handshake
//   - 403 {"reason":"not_allowed"}         channel's table not granted
//   - 403 {"reason":"auth_revoked"}        fresh whitelist revokes the channel; sub dropped
//   - 404 {"reason":"no_live_subscriber"}  no live stream on the channel
//   - 409 {"reason":"refresh_in_progress"} a TTL refresh holds the lock (Retry-After: 1)
//   - 503 (status only)                    global concurrency exhausted (Retry-After: 5)
func (h *Handler) serveRefresh(w http.ResponseWriter, r *http.Request) {
	// 0. Global admission, before any auth work, so an accept-storm of refreshes
	//    can't blow up goroutines or hammer the backend. Released on return.
	if !h.limits.AcquireGlobal() {
		h.writeStatusOnlyError(w, r, http.StatusServiceUnavailable, 5)
		return
	}
	defer h.limits.ReleaseGlobal()

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

	// 2. Authenticate the caller via the shared identity path (the same helper the
	//    SSE handshake uses), including the immediate credential scrub.
	authMap, _, ok := h.authenticate(w, r, channelStr)
	if !ok {
		return
	}

	if _, allowed := authMap.Tables[table]; !allowed {
		h.metrics.RefreshOnDemand("not_allowed").Inc()
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return
	}

	// 3. Apply the whitelist OpenSession just returned to the caller's own live
	//    subscriber(s). Revocation beats a swap; a swap beats a concurrent Busy.
	sum := h.authRegistry.RefreshOwnByChannel(channelStr, authMap.UserID, authMap)
	switch {
	case sum.Matched == 0:
		h.metrics.RefreshOnDemand("no_live_subscriber").Inc()
		h.writeJSONReason(w, r, http.StatusNotFound, "no_live_subscriber")
	case sum.Revoked > 0:
		h.metrics.RefreshOnDemand("auth_revoked").Inc()
		h.writeJSONReason(w, r, http.StatusForbidden, "auth_revoked")
	case sum.Swapped > 0:
		h.metrics.RefreshOnDemand("swapped").Inc()
		h.writeRefreshOK(w, r, sum.Swapped)
	default:
		// Only Busy: a TTL refresh holds the swap lock; the stream is healthy.
		h.metrics.RefreshOnDemand("refresh_in_progress").Inc()
		h.writeReasonRetry(w, r, http.StatusConflict, 1, "refresh_in_progress")
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

// writeReasonRetry writes a {"reason":...} body with an optional Retry-After
// header. The header must be set before WriteHeader, so it is set here and the
// body is then delegated to writeJSONReason (the single source of truth for the
// reason-body response).
func (h *Handler) writeReasonRetry(w http.ResponseWriter, r *http.Request, status, retryAfterSeconds int, reason string) {
	if retryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	h.writeJSONReason(w, r, status, reason)
}
