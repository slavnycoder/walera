package sse

import (
	"net"
	"net/http"
	"time"
)

type subState struct {
	sub subscriber

	queue      chan []byte
	conn       *net.TCPConn
	respWriter http.ResponseWriter
	rc         *http.ResponseController

	lastWriteAt time.Time

	buffer   [][]byte
	bufBytes int

	inDirty bool

	dropReason string

	done chan struct{}

	connectedAt time.Time

	inDisconnected bool
}

func (p *WriterPool) Attach(sub subscriber, conn *net.TCPConn, respWriter http.ResponseWriter, rc *http.ResponseController) (doneCh <-chan struct{}, err error) {
	if p.closed.Load() {
		return nil, errPoolClosed
	}

	w := p.pickWorker(sub.ID())

	st := &subState{
		sub:        sub,
		queue:      make(chan []byte, p.cfg.SubQueueSize),
		conn:       conn,
		respWriter: respWriter,
		rc:         rc,
		done:       make(chan struct{}),
	}

	prelude := []byte("retry: 15000\n\n")
	if conn != nil {
		_ = conn.SetWriteDeadline(time.Now().Add(p.cfg.WriteTimeout))
		if _, werr := conn.Write(prelude); werr != nil {
			_ = conn.SetWriteDeadline(time.Time{})
			return nil, werr
		}
		_ = conn.SetWriteDeadline(time.Time{})
	} else if respWriter != nil {

		if rc != nil {
			_ = rc.SetWriteDeadline(time.Now().Add(p.cfg.WriteTimeout))
		}
		if _, werr := respWriter.Write(prelude); werr != nil {
			return nil, werr
		}
		if rc != nil {
			if ferr := rc.Flush(); ferr != nil {
				return nil, ferr
			}
			_ = rc.SetWriteDeadline(time.Time{})
		}
	}
	now := time.Now()
	st.connectedAt = now

	st.lastWriteAt = now

	sub.WireSendFunc(func(frame []byte) bool {
		select {
		case st.queue <- frame:
			return true
		default:
			return false
		}
	})

	select {
	case w.attachCh <- st:
	case <-w.shutdownCh:
		return nil, errPoolClosed
	}

	return st.done, nil
}

func (w *poolWorker) attachSub(st *subState) {
	w.subs = append(w.subs, st)
	w.thresholdDirty = true
}

var _ = func() *subState { return &subState{} }
