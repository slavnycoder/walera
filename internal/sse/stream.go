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

	doneCh, tcpConn, ok := h.mainLoop(w, r, sub)

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
			Token:      hs.token,
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
