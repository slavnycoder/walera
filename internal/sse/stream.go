// Package sse — post-gate SSE writer loop: response header emission,
// subscriber construction, pool.Attach + broadcaster.Register wiring,
// and the doneCh-vs-context-Done select that ends the handler.
// Heartbeat ticker emission lives in the writer pool
// (worker_loop.go's sweepHeartbeats). See INVARIANTS.md §1, §2.
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

// writeSSEHeaders sets standard SSE response headers, Vary: Origin,
// and matched CORS reflection headers. Transfer-Encoding: identity
// suppresses net/http's auto-chunked framing — after hijack raw SSE
// bytes flow without framing transform. Side effect: wire Connection
// header becomes "close" (not "keep-alive"); benign — EventSource
// auto-reconnects. MUST NOT be called before all validation passes.
func (h *Handler) writeSSEHeaders(w http.ResponseWriter, r *http.Request) {
	// handleCORS unconditionally adds Vary: Origin.
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)

	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Accel-Buffering", "no")
	hdr.Set("Transfer-Encoding", "identity")

	w.WriteHeader(http.StatusOK)
}

// runWriter is invoked by runHandshakeAndWriter after all five gates
// pass. Builds router.Subscriber + auth.Subscriber, wires the auth
// refresh ticker, emits SSE headers, hijacks the TCP conn (or falls
// back to respWriter+rc on TLS/h2c), attaches to the WriterPool,
// registers with the broadcaster, and blocks on doneCh vs
// r.Context().Done().
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
	// Once we own the hijacked TCP conn we are responsible for closing
	// it. Close runs LAST (LIFO) so deferred Drop + Deregister + pool
	// teardown can flush the final frame before the socket is torn
	// down. Fallback path: tcpConn == nil and net/http closes.
	if tcpConn != nil {
		defer func() { _ = tcpConn.Close() }()
	}
	if !ok {
		return
	}
	defer h.bc.Deregister(sub)

	h.finalizeError(r, sub, doneCh)
}

// headersAndPreamble builds the router + auth subscribers, wires
// Filter, registers authSub, spawns the refresh ticker, and emits SSE
// response headers + the connection log line. The caller installs
// deferred authRegistry.Remove on success.
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

	// auth.Subscriber wraps the router subscriber and runs the refresh
	// ticker. FilterWithLSN applies the back-buffer rule on dispatch.
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

	// Bind the refresh ticker to the request context so client
	// disconnect also exits the loop.
	safego.Go("auth-refresh-"+sub.ID(), func() {
		authSub.RefreshLoop(r.Context())
	})

	// Headers FIRST on the live ResponseWriter (before hijack) so the
	// 200 status line + SSE headers reach the client via the normal
	// net/http write path. The prelude ("retry: 15000\n\n") is then
	// emitted by pool.Attach as the first bytes on the hijacked conn.
	// Reversing this order is the most common SSE-hijack bug.
	h.writeSSEHeaders(w, r)
	// Token MUST NOT appear in any log field (INVARIANTS.md §11).
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

// mainLoop hijacks the TCP conn, attaches the subscriber to the
// writer pool, and registers with the broadcaster. Returns tcpConn
// (for the caller's deferred Close, even on error paths) and ok=false
// on hijack/attach failure. Order: Attach → Register → block on
// doneCh — reversing would let the router dispatch a frame before
// sendFunc is wired (spurious slow_consumer drop).
func (h *Handler) mainLoop(
	w http.ResponseWriter,
	r *http.Request,
	sub *router.Subscriber,
) (doneCh <-chan struct{}, tcpConn *net.TCPConn, ok bool) {
	rc := http.NewResponseController(w)
	tcpConn, hijackErr := hijackTCPConn(rc)
	if hijackErr != nil {
		// Real hijack error (not ErrNotSupported). errHijackedConnNotTCP
		// closes the conn itself.
		h.logger.Warn().Err(hijackErr).
			Str("subscriber_id", sub.ID()).
			Msg("sse hijack failed; cannot serve")
		h.metrics.SubscriberDisconnects("client_closed").Inc()
		return nil, nil, false
	}

	// Defer Deregister AFTER successful Attach so failed Attach does
	// not double-deregister.
	doneCh, attachErr := h.pool.Attach(sub, tcpConn, w, rc)
	if attachErr != nil {
		// Most likely errPoolClosed (shutdown in progress).
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

// finalizeError blocks on doneCh, handles client-disconnect via
// sub.Drop("client_closed") + bounded WriteTimeout wait, and runs the
// belt-and-suspenders inline Drop so LIFO defers in runWriter can
// flush+Drop+Deregister+Close cleanly.
func (h *Handler) finalizeError(r *http.Request, sub *router.Subscriber, doneCh <-chan struct{}) {
	// Block on doneCh (closed by the worker after drain + final frame
	// + detach) OR r.Context().Done() (client TCP closed; respWriter
	// about to be torn down). On router-side Drop (auth_revoked / ...)
	// we deliberately do NOT exit on sub.Done() — evictDone observes
	// Done(), emits the error frame, and closes doneCh; the handler
	// then exits via the doneCh arm with the frame already flushed.
	select {
	case <-doneCh:
	case <-r.Context().Done():
		// Trigger Drop synchronously, then wait for drain bounded by
		// WriteTimeout. Drop is sync.Once-guarded.
		sub.Drop("client_closed")
		select {
		case <-doneCh:
		case <-time.After(h.cfg.WriteTimeout):
		}
	}
	// Belt-and-suspenders: if we exited via doneCh first, Drop has
	// not fired (the pool already tore the sub down). Calling it now
	// is a no-op for sync.Once-guarded Drop. Must run BEFORE the
	// deferred tcpConn.Close() — placed inline (not as a defer) so
	// LIFO order does not reverse it.
	sub.Drop("client_closed")
}
