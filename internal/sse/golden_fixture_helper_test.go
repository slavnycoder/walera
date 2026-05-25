package sse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

const goldenCommitLSN pglogrepl.LSN = 0x16B374A

const goldenXID uint32 = 12345

var goldenCommitTS = time.Date(2026, time.May, 20, 0, 0, 0, 0, time.UTC)

var errSendFailed = errors.New("golden_fixture: sendFunc returned false")

var errShutdownTimeout = errors.New("golden_fixture: shutdown timed out")

func buildGoldenTx() wal.Tx {
	return wal.Tx{
		ID:        goldenXID,
		CommitLSN: goldenCommitLSN,
		CommitTS:  goldenCommitTS,
		Changes: []wal.Change{
			{
				Schema: "public",
				Table:  "users",
				Op:     wal.OpInsert,
				PK:     "42",
				PKCol:  "user_id",

				Data: map[string]any{"name": "alice"},
			},
			{
				Schema:  "public",
				Table:   "users",
				Op:      wal.OpUpdate,
				PK:      "42",
				PKCol:   "user_id",
				Changed: map[string]any{"name": "bob"},
			},
			{
				Schema: "public",
				Table:  "users",
				Op:     wal.OpDelete,
				PK:     "42",
				PKCol:  "user_id",
			},
		},
	}
}

type fixtureSub struct {
	id   string
	kind string

	mu     sync.Mutex
	send   func(frame []byte) bool
	done   chan struct{}
	reason string
}

func newFixtureSub(id, kind string) *fixtureSub {
	return &fixtureSub{id: id, kind: kind, done: make(chan struct{})}
}

func (s *fixtureSub) ID() string         { return s.id }
func (s *fixtureSub) KindString() string { return s.kind }

func (s *fixtureSub) WireSendFunc(send func(frame []byte) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.send = send
}

func (s *fixtureSub) SendForTest(frame []byte) bool {
	s.mu.Lock()
	fn := s.send
	s.mu.Unlock()
	if fn == nil {
		return false
	}
	return fn(frame)
}

func (s *fixtureSub) Done() <-chan struct{} { return s.done }
func (s *fixtureSub) Reason() string        { return s.reason }

type goldenRespWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *goldenRespWriter) Header() http.Header { return http.Header{} }
func (w *goldenRespWriter) WriteHeader(int)     {}
func (w *goldenRespWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (w *goldenRespWriter) Flush() {}

func (w *goldenRespWriter) snapshot() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]byte, w.buf.Len())
	copy(out, w.buf.Bytes())
	return out
}

func fixturePoolConfig() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          8,
		MaxWaitMs:             2,
		DrainThresholdSubs:    100,
		HeartbeatInterval:     250 * time.Millisecond,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
		MaxBatchBytesPerSub:   64 * 1024,
	}
}

func runSyntheticTxThroughPoolViaRespWriter(logger zerolog.Logger) (*goldenRespWriter, error) {
	enc := NewEncoder(10 * 1024 * 1024)
	p := NewPool(fixturePoolConfig(), PoolDeps{Encoder: enc, Metrics: newFakeMetrics(), Logger: logger})

	sub := newFixtureSub("golden-sub-001", "exact")
	rw := &goldenRespWriter{}
	rc := http.NewResponseController(rw)
	doneCh, err := p.Attach(sub, nil, rw, rc)
	if err != nil {
		return nil, err
	}

	ev := router.Event{
		Tx:             buildGoldenTx(),
		MatchedIndices: []int{0, 1, 2},
	}
	frame, overflow := enc.Encode(ev)
	if overflow {
		return nil, errors.New("golden_fixture: synthetic tx overflowed encoder cap")
	}

	if !sub.SendForTest(frame) {
		return nil, errSendFailed
	}

	if err := waitFor(200*time.Millisecond, func() bool {
		got := rw.snapshot()

		return bytes.Contains(got, []byte("event: tx\n")) && bytes.HasSuffix(got, []byte("\n\n"))
	}); err != nil {
		return nil, err
	}

	hb := enc.EncodeHeartbeat()
	if _, werr := rw.Write(hb); werr != nil {
		return nil, werr
	}

	_ = p.Shutdown(context.Background())

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return nil, errShutdownTimeout
	}

	return rw, nil
}

func runSyntheticTxThroughPoolViaTCPConn(logger zerolog.Logger) ([]byte, error) {
	enc := NewEncoder(10 * 1024 * 1024)
	p := NewPool(fixturePoolConfig(), PoolDeps{Encoder: enc, Metrics: newFakeMetrics(), Logger: logger})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer ln.Close()

	type accepted struct {
		conn *net.TCPConn
		err  error
	}
	acceptCh := make(chan accepted, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			acceptCh <- accepted{nil, aerr}
			return
		}
		acceptCh <- accepted{c.(*net.TCPConn), nil}
	}()

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	cliRaw, derr := dialer.Dial("tcp", ln.Addr().String())
	if derr != nil {
		return nil, derr
	}
	defer cliRaw.Close()
	cli := cliRaw.(*net.TCPConn)

	acc := <-acceptCh
	if acc.err != nil {
		return nil, acc.err
	}
	srvConn := acc.conn

	sub := newFixtureSub("golden-sub-001", "exact")
	doneCh, err := p.Attach(sub, srvConn, nil, nil)
	if err != nil {
		return nil, err
	}

	ev := router.Event{
		Tx:             buildGoldenTx(),
		MatchedIndices: []int{0, 1, 2},
	}
	frame, overflow := enc.Encode(ev)
	if overflow {
		return nil, errors.New("golden_fixture: synthetic tx overflowed encoder cap")
	}
	if !sub.SendForTest(frame) {
		return nil, errSendFailed
	}

	expectedPreludeAndTxLen := len("retry: 15000\n\n") + len(frame)
	collected := make([]byte, 0, expectedPreludeAndTxLen+1024)
	readBuf := make([]byte, 4096)
	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	for len(collected) < expectedPreludeAndTxLen {
		n, rerr := cli.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return nil, rerr
		}
	}

	if _, werr := srvConn.Write(enc.EncodeHeartbeat()); werr != nil {
		return nil, werr
	}

	_ = p.Shutdown(context.Background())

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return nil, errShutdownTimeout
	}

	_ = cli.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		n, rerr := cli.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {

			break
		}
	}
	return collected, nil
}

func waitFor(budget time.Duration, cond func() bool) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("golden_fixture: wait-for condition timed out")
}
