package sse

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const refreshBodyMaxBytes = 1024

type refreshRequest struct {
	Channel string `json:"channel"`
}

type refreshResponse struct {
	Refreshed int `json:"refreshed"`
}

func (h *Handler) serveRefreshPreflight(w http.ResponseWriter, r *http.Request) {
	if !handlePreflight(w, r, h.cfg.CORSOrigins, "POST, OPTIONS", h.corsAllowHeaders) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// serveRefresh handles POST /sse/v1/refresh {"channel":"..."}: authenticates the
// caller via OpenSession (same path as the SSE handshake) and swaps the returned
// whitelist into the caller's own live subscriber(s) on that channel, mid-stream.
//
// No per-user slot: that counter is held for a stream's lifetime, so taking it
// here would 429 a user with a full set of streams.
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
	if !h.limits.AcquireGlobal() {
		h.writeStatusOnlyError(w, r, http.StatusServiceUnavailable, 5)
		return
	}
	defer h.limits.ReleaseGlobal()

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

	fetchedAt := time.Now()
	authMap, _, ok := h.authenticate(w, r, channelStr)
	if !ok {
		return
	}

	if _, allowed := authMap.Tables[table]; !allowed {
		h.metrics.RefreshOnDemand("not_allowed").Inc()
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return
	}

	// Revocation beats a swap; a swap beats a concurrent Busy.
	sum := h.authRegistry.RefreshOwnByChannel(channelStr, authMap.UserID, authMap, fetchedAt)
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

// parseRefreshChannel mirrors the GET-route rules: "table:pk" or "table:all".
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
	if retryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	}
	h.writeJSONReason(w, r, status, reason)
}
