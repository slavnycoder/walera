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

// serveRefresh handles POST /sse/v1/refresh. The frontend sends a channel name
// in the JSON body; Walera authenticates the caller (the same identity path as
// the SSE handshake, via the shared authenticate helper), authorizes them for the
// channel, and then synchronously re-fetches the channel's permissions from the
// auth backend and atomically swaps the whitelist for the caller's OWN live
// subscriber(s) on that channel — mid-stream, no reconnect. The refresh delegates
// to auth.Subscribers.RefreshOwnByChannel, which shares the fetch/apply primitives
// with the TTL loop (so it inherits the identical swap, revocation-drop, and
// breaker semantics) but issues a SINGLE backend fetch for the whole group and
// returns an aggregate result; there is no TTL backoff.
//
// Admission control mirrors the handshake but is symmetric within this one short
// request: AcquireGlobal first (before auth), AcquirePerUser after OpenSession
// yields the user_id, both released on return.
//
// Status codes:
//   - 200 {"refreshed":N}     the caller's N subscriber(s) were re-authorized and swapped
//   - 400 {"error":...}       malformed body / channel / request id
//   - 401                     no credentials presented
//   - 403 {"reason":...}      caller not authorized for the channel (not_allowed),
//     or the re-fetch revoked access and the subscriber was dropped (auth_revoked)
//   - 404 {"reason":"no_live_subscriber"}  the caller has no live stream on the channel
//   - 409 {"reason":"refresh_in_progress"} a refresh is already in flight (Retry-After: 1)
//   - 429                     per-user refresh concurrency exceeded
//   - 503 {"reason":"auth_unavailable"}    the backend re-fetch failed (Retry-After: 5)
//   - 503 (status only)       global concurrency exhausted (Retry-After: 5)
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
	authMap, requestID, ok := h.authenticate(w, r, channelStr)
	if !ok {
		return
	}

	// 3. Per-user admission, after OpenSession yields the user_id (mirrors the
	//    handshake order: global before auth, per-user after). Released on return.
	if ok, _ := h.limits.AcquirePerUser(authMap.UserID); !ok {
		h.writeStatusOnlyError(w, r, http.StatusTooManyRequests, 0)
		return
	}
	defer h.limits.ReleasePerUser(authMap.UserID)

	if _, allowed := authMap.Tables[table]; !allowed {
		h.metrics.RefreshOnDemand("not_allowed").Inc()
		h.writeJSONReason(w, r, http.StatusForbidden, "not_allowed")
		return
	}

	// 4. Synchronously refresh the caller's own live subscriber(s) on the channel.
	//    Branch precedence keeps a partial success from hiding behind a concurrent
	//    failure: an authoritative revocation beats everything, then any swap beats
	//    Busy/Unavailable, then a real backend failure, then in-flight (Busy).
	sum := h.authRegistry.RefreshOwnByChannel(r.Context(), channelStr, authMap.UserID, requestID)
	switch {
	case sum.Matched == 0:
		h.metrics.RefreshOnDemand("no_live_subscriber").Inc()
		h.writeJSONReason(w, r, http.StatusNotFound, "no_live_subscriber")
	case sum.Revoked > 0:
		// The re-fetch determined the caller is no longer entitled to the channel;
		// the subscriber was dropped (auth_revoked), mirroring the TTL path.
		h.metrics.RefreshOnDemand("auth_revoked").Inc()
		h.writeJSONReason(w, r, http.StatusForbidden, "auth_revoked")
	case sum.Swapped > 0:
		// Partial or full success: at least one of the caller's subscribers was
		// re-authorized and swapped (a concurrent Busy/Unavailable does not mask it).
		h.metrics.RefreshOnDemand("swapped").Inc()
		h.writeRefreshOK(w, r, sum.Swapped)
	case sum.Unavailable > 0:
		// A real transient backend failure: the stream(s) are kept on their current
		// whitelist; the caller may retry. This is the SINGLE on-demand log site —
		// the shared fetchPermissions no longer logs (only the TTL refreshAttempt
		// does), so an explicitly requested on-demand failure is observable at any
		// ttl. Channel/user_id are identifiers, not row data.
		h.logger.Warn().
			Str("channel", channelStr).
			Str("user_id", authMap.UserID).
			Int("matched", sum.Matched).
			Int("unavailable", sum.Unavailable).
			Msg("on-demand refresh did not complete")
		h.metrics.RefreshOnDemand("auth_unavailable").Inc()
		h.writeReasonRetry(w, r, http.StatusServiceUnavailable, 5, "auth_unavailable")
	default:
		// Only Busy remains: a refresh (a TTL tick, breaker fan-out, or a concurrent
		// POST) already holds refreshMu — the stream is healthy and in-flight, so
		// this is a 409, not a 503. No warn: nothing failed.
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
