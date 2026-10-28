package sse

import (
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/safego"
)

func (h *Handler) writeSSEHeaders(w http.ResponseWriter, r *http.Request) {

	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)

	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Accel-Buffering", "no")
	hdr.Set("Transfer-Encoding", "identity")

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) runWriter(
	w http.ResponseWriter,
	r *http.Request,
	table, pk, channelStr string,
	kind router.Kind,
	startLSN pglogrepl.LSN,
	hs handshakeResult,
) {
	sub, authSub, err := h.headersAndPreamble(w, r, table, pk, channelStr, kind, startLSN, hs)
	if err != nil {
		return
	}
	defer h.authRegistry.Remove(authSub.ID())

	var initialFrame []byte
	if len(hs.authMap.InitialData) > 0 {
		frame, overflow := h.enc.EncodeInitialData(hs.authMap.InitialData)
		if overflow {
			h.logger.Warn().
				Str("subscriber_id", sub.ID()).
				Int("initial_data_bytes", len(hs.authMap.InitialData)).
				Msg("sse initial_data exceeds max_payload_bytes; skipping")
		} else {
			initialFrame = frame
		}
	}

	doneCh, tcpConn, ok := h.mainLoop(w, r, sub, initialFrame)

	if tcpConn != nil {
		defer func() { _ = tcpConn.Close() }()
	}
	if !ok {
		return
	}
	defer h.bc.Deregister(sub)

	h.finalizeError(r, sub, doneCh)
}

func (h *Handler) headersAndPreamble(
	w http.ResponseWriter,
	r *http.Request,
	table, pk, channelStr string,
	kind router.Kind,
	startLSN pglogrepl.LSN,
	hs handshakeResult,
) (sub *router.Subscriber, authSub *auth.Subscriber, err error) {
	bufCap := h.routerCfg.ExactBuffer
	if kind == router.KindWildcard {
		bufCap = h.routerCfg.WildcardBuffer
	}
	sub = router.NewSubscriber(
		router.SubscriberConfig{
			Kind:      kind,
			Schema:    "public",
			Table:     table,
			PK:        pk,
			StartLSN:  startLSN,
			BufferCap: bufCap,
		},
		router.SubscriberDeps{Parent: r.Context()},
	)

	ttl := time.Duration(h.authCfg.DefaultTTLSeconds) * time.Second
	authSub = auth.NewSubscriber(
		auth.SubscriberConfig{
			InitialMap: hs.authMap,
			UserID:     hs.userID,
			Channel:    channelStr,
			DefaultTTL: ttl,
		},
		auth.SubscriberDeps{
			Sub:     sub,
			Client:  h.authClient,
			Breaker: h.breaker,
			Logger:  h.logger,
			Metrics: h.metrics,
		},
	)
	sub.Filter = authSub.FilterWithLSN

	h.authRegistry.Add(authSub)

	safego.Go("auth-refresh-"+sub.ID(), func() {
		authSub.RefreshLoop(r.Context())
	})

	h.writeSSEHeaders(w, r)

	h.logger.Info().
		Str("subscriber_id", sub.ID()).
		Str("kind", string(kind)).
		Str("channel", channelStr).
		Str("table", table).
		Str("pk", pk).
		Str("start_lsn", startLSN.String()).
		Str("user_id", hs.authMap.UserID).
		Str("auth_request_id", hs.requestID).
		Str("client_ip", hs.clientIP).
		Msg("sse subscriber connected")

	return sub, authSub, nil
}

func (h *Handler) mainLoop(
	w http.ResponseWriter,
	r *http.Request,
	sub *router.Subscriber,
	initialFrame []byte,
) (doneCh <-chan struct{}, tcpConn *net.TCPConn, ok bool) {
	rc := http.NewResponseController(w)
	tcpConn, hijackErr := hijackTCPConn(rc)
	if hijackErr != nil {

		h.logger.Warn().Err(hijackErr).
			Str("subscriber_id", sub.ID()).
			Msg("sse hijack failed; cannot serve")
		h.metrics.SubscriberDisconnects("client_closed").Inc()
		return nil, nil, false
	}

	if len(initialFrame) > 0 {
		if !h.writeInitialFrame(sub, tcpConn, w, rc, initialFrame) {
			return nil, tcpConn, false
		}
	}

	doneCh, attachErr := h.pool.Attach(sub, tcpConn, w, rc)
	if attachErr != nil {

		h.logger.Warn().Err(attachErr).
			Str("subscriber_id", sub.ID()).
			Msg("sse pool Attach failed")
		reason := "shutdown"
		if !errors.Is(attachErr, errPoolClosed) {
			reason = "client_closed"
		}
		h.metrics.SubscriberDisconnects(reason).Inc()
		return nil, tcpConn, false
	}
	h.bc.Register(sub)
	return doneCh, tcpConn, true
}

// writeInitialFrame writes the auth-supplied initial_data SSE frame
// synchronously, before the pool worker takes over the connection. Mirrors
// the prelude write in WriterPool.Attach: prefers the hijacked tcpConn,
// falls back to respWriter+rc when hijack returned (nil, nil). Returns
// false on any write/flush error after recording a client_closed disconnect.
// Content is never logged (PII); only byte count on failure.
func (h *Handler) writeInitialFrame(sub *router.Subscriber, tcpConn *net.TCPConn, w http.ResponseWriter, rc *http.ResponseController, frame []byte) bool {
	if tcpConn != nil {
		_ = tcpConn.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout))
		if _, werr := tcpConn.Write(frame); werr != nil {
			_ = tcpConn.SetWriteDeadline(time.Time{})
			h.logger.Warn().Err(werr).
				Str("subscriber_id", sub.ID()).
				Int("frame_bytes", len(frame)).
				Msg("sse initial_data write failed")
			h.metrics.SubscriberDisconnects("client_closed").Inc()
			return false
		}
		_ = tcpConn.SetWriteDeadline(time.Time{})
		return true
	}

	if w == nil {
		return true
	}
	if rc != nil {
		_ = rc.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout))
	}
	if _, werr := w.Write(frame); werr != nil {
		h.logger.Warn().Err(werr).
			Str("subscriber_id", sub.ID()).
			Int("frame_bytes", len(frame)).
			Msg("sse initial_data write failed")
		h.metrics.SubscriberDisconnects("client_closed").Inc()
		return false
	}
	if rc != nil {
		if ferr := rc.Flush(); ferr != nil {
			h.logger.Warn().Err(ferr).
				Str("subscriber_id", sub.ID()).
				Int("frame_bytes", len(frame)).
				Msg("sse initial_data flush failed")
			h.metrics.SubscriberDisconnects("client_closed").Inc()
			return false
		}
		_ = rc.SetWriteDeadline(time.Time{})
	}
	return true
}

func (h *Handler) finalizeError(r *http.Request, sub *router.Subscriber, doneCh <-chan struct{}) {

	select {
	case <-doneCh:
	case <-r.Context().Done():

		sub.Drop("client_closed")
		select {
		case <-doneCh:
		case <-time.After(h.cfg.WriteTimeout):
		}
	}

	sub.Drop("client_closed")
}
