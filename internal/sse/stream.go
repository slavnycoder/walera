package sse

import (
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/router"
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

var preludeFrame = []byte("retry: 15000\n\n")

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

	h.stream(w, r, sub, initialFrame)
}

func (h *Handler) headersAndPreamble(
	w http.ResponseWriter,
	r *http.Request,
	table, pk, channelStr string,
	kind router.Kind,
	startLSN pglogrepl.LSN,
	hs handshakeResult,
) (sub *router.Subscriber, authSub *auth.Subscriber, err error) {
	sub = router.NewSubscriber(
		router.SubscriberConfig{
			Kind:     kind,
			Schema:   "public",
			Table:    table,
			PK:       pk,
			StartLSN: startLSN,
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
			Logger:  h.logger,
			Metrics: h.metrics,
		},
	)
	sub.Filter = authSub.FilterWithLSN

	h.authRegistry.Add(authSub)

	go authSub.RefreshLoop(r.Context())

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

// stream owns the response on the handler's goroutine for the subscriber's
// lifetime. The router only enqueues (non-blocking), so a slow client stalls
// nobody but itself and is dropped by the write deadline.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request, sub *router.Subscriber, initialFrame []byte) {
	rc := http.NewResponseController(w)
	connectedAt := time.Now()
	kind := sub.KindString()

	queueCap := h.routerCfg.ExactBuffer
	if sub.Kind() == router.KindWildcard {
		queueCap = h.routerCfg.WildcardBuffer
	}
	queue := make(chan []byte, queueCap)
	sub.WireSendFunc(func(frame []byte) bool {
		select {
		case queue <- frame:
			return true
		default:
			return false
		}
	})

	write := func(frames ...[]byte) error {
		_ = rc.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout))
		for _, f := range frames {
			if _, err := w.Write(f); err != nil {
				return err
			}
		}
		if err := rc.Flush(); err != nil {
			return err
		}
		// A pending deadline on HTTP/2 resets the stream even while idle.
		_ = rc.SetWriteDeadline(time.Time{})
		return nil
	}

	reason := "client_closed"
	defer func() {
		h.metrics.SubscriberLifetime().Observe(time.Since(connectedAt).Seconds())
		h.metrics.SubscriberDisconnects(reason).Inc()
		if reason == "slow_consumer" {
			h.metrics.SlowClientDrops().Inc()
		}
	}()

	if err := write(preludeFrame, initialFrame); err != nil {
		reason = writeFailureReason(err)
		sub.Drop(reason)
		return
	}
	h.bc.Register(sub)
	defer h.bc.Deregister(sub)

	// Queued frames are discarded on drop: after a revoke they carry the old grant.
	finish := func() {
		switch reason = sub.Reason(); reason {
		case "", "client_closed":
			reason = "client_closed"
		case "shutdown":
			_ = write(h.enc.EncodeShutdown())
		default:
			_ = write(h.enc.EncodeError(reason))
		}
	}

	hb := time.NewTicker(h.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case frame := <-queue:
			select {
			case <-sub.Done():
				finish()
				return
			default:
			}
			frames := [][]byte{frame}
		batch:
			for {
				select {
				case f := <-queue:
					frames = append(frames, f)
				default:
					break batch
				}
			}
			select {
			case <-sub.Done():
				finish()
				return
			default:
			}
			if err := write(frames...); err != nil {
				reason = writeFailureReason(err)
				sub.Drop(reason)
				return
			}
			h.metrics.EventsSent(kind).Add(float64(len(frames)))
			hb.Reset(h.cfg.HeartbeatInterval)
		case <-hb.C:
			if err := write(h.enc.EncodeHeartbeat()); err != nil {
				reason = writeFailureReason(err)
				sub.Drop(reason)
				return
			}
		case <-r.Context().Done():
			sub.Drop(reason)
			return
		case <-sub.Done():
			finish()
			return
		}
	}
}

func writeFailureReason(err error) string {
	var ne net.Error
	if errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "slow_consumer"
	}
	return "client_closed"
}
