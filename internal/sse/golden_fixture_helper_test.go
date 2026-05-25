// Package sse — golden_fixture_helper_test.go houses the deterministic
// synthetic-transaction builder + the pool driver shared by the two
// wire-parity test bodies (golden_parity_test.go) AND the fixture
// generator (golden_capture_test.go, build-tagged).
// No build tag — the helpers compile on every `go test./...` run so
// golden_parity_test.go can call them.
// Determinism contract (CONTEXT.md Q4 + 16-04 plan Risk #5):
//   - `CommitLSN = 0/16B374A` (fixed pglogrepl.LSN literal).
//   - `CommitTS = 2026-05-20T00:00:00Z` (fixed wall-clock).
//   - `XID = 12345`.
//   - 3 changes: insert + update + delete on `users.user_id=42`, with
//     fixed integer / string column values. Maps in `Data` / `Changed`
//     have ONE key each so encoding/json's map-key randomization cannot
//     produce different bytes.
//   - subscriber = synthetic fixtureSub (KindString "exact"), no Filter.
//   - Pool config = PoolFactor 1, SubQueueSize 8, MaxWaitMs 2.
//     DrainThresholdSubs 100 so the timer arms and drains within ~2ms.
//     HeartbeatInterval 250ms — long enough that the worker's hb ticker
//     does not interfere with the synthetic tx capture window; the
//     fixture's heartbeat frame is written inline via EncodeHeartbeat
//     for deterministic ordering.
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

// goldenCommitLSN is the deterministic CommitLSN. Encoded as "0/16B374A"
// by pglogrepl.LSN.String.
const goldenCommitLSN pglogrepl.LSN = 0x16B374A

// goldenXID is the deterministic transaction ID.
const goldenXID uint32 = 12345

// goldenCommitTS is the deterministic commit timestamp — fixed wall-clock
// in UTC so encoder.txToEvent's `CommitTS.UTC().Format(time.RFC3339Nano)`
// yields a byte-stable string.
var goldenCommitTS = time.Date(2026, time.May, 20, 0, 0, 0, 0, time.UTC)

// errSendFailed is returned by the fixture driver when the wired sendFunc
// rejects the synthetic frame (queue full). Should never fire in
// practice — the synthetic tx is one frame and SubQueueSize is 8.
var errSendFailed = errors.New("golden_fixture: sendFunc returned false")

// errShutdownTimeout is returned when pool.Shutdown does not close the
// sub's doneCh within the helper's 1-second budget.
var errShutdownTimeout = errors.New("golden_fixture: shutdown timed out")

// buildGoldenTx returns the deterministic synthetic 3-change transaction
// (insert + update + delete on public.users, user_id=42). Encoded form
// is locked into scripts/golden/sse_v13_handshake.txt.
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
				// One key only → encoding/json map-key order is irrelevant.
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

// fixtureSub is a subscriber suitable for the wire-parity tests. Exposes
// SendForTest, which the helper uses to push the encoded frame through
// the wired pool sendFunc — replacing the router.Broadcaster routeTx
// call (which would otherwise own the encode→send sequence).
// Implements sse.subscriber: ID, KindString, WireSendFunc, Done, Reason.
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

// SendForTest invokes the wired pool sendFunc directly, returning its
// bool. Used in place of router.Broadcaster.routeTx → sub.send.
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

// goldenRespWriter is a byte-recording http.ResponseWriter used by the
// fixture capture and the respWriter-path parity test.
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

// snapshot returns a copy of the recorded bytes. Safe to call after the
// pool has been shut down.
func (w *goldenRespWriter) snapshot() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]byte, w.buf.Len())
	copy(out, w.buf.Bytes())
	return out
}

// fixturePoolConfig returns the deterministic pool config used for every
// fixture capture and parity test variant.
func fixturePoolConfig() PoolConfig {
	return PoolConfig{
		PoolFactor:            1,
		SubQueueSize:          8,
		MaxWaitMs:             2,
		DrainThresholdSubs:    100, // force timer drain (single sub)
		HeartbeatInterval:     250 * time.Millisecond,
		WriteTimeout:          time.Second,
		drainShutdownDeadline: 50 * time.Millisecond,
		MaxBatchBytesPerSub:   64 * 1024,
	}
}

// runSyntheticTxThroughPoolViaRespWriter drives the deterministic
// synthetic tx through the production encoder + pool via the
// respWriter-fallback path (conn == nil). Returns the populated
// goldenRespWriter once Shutdown has completed, so the captured bytes
// include the prelude, the tx frame, an inline-emitted heartbeat, and
// the shutdown frame.
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

	// Encode the synthetic tx via the same encoder that production wires.
	ev := router.Event{
		Tx:             buildGoldenTx(),
		MatchedIndices: []int{0, 1, 2},
	}
	frame, overflow := enc.Encode(ev)
	if overflow {
		return nil, errors.New("golden_fixture: synthetic tx overflowed encoder cap")
	}
	// Deliver to the sub's queue via the wired pool sendFunc — exactly
	// the path the router takes after filtering.
	if !sub.SendForTest(frame) {
		return nil, errSendFailed
	}

	// Wait for the worker timer (MaxWaitMs=2) to drain. 200ms is well
	// over the timer ceiling + scheduler overhead.
	if err := waitFor(200*time.Millisecond, func() bool {
		got := rw.snapshot()
		// Drained when the tx frame's terminating "\n\n" is present
		// after the prelude.
		return bytes.Contains(got, []byte("event: tx\n")) && bytes.HasSuffix(got, []byte("\n\n"))
	}); err != nil {
		return nil, err
	}

	// Append the heartbeat frame inline (phase-16 pool has no worker
	// heartbeat dispatch yet; produced via EncodeHeartbeat for the same
	// bytes phase 17's worker will emit).
	hb := enc.EncodeHeartbeat()
	if _, werr := rw.Write(hb); werr != nil {
		return nil, werr
	}

	// Shutdown emits the spec §3.5 shutdown frame via drainShutdown.
	// To trigger drainShutdown, the sub must still be attached when
	// Shutdown is called.
	_ = p.Shutdown(context.Background())

	// Wait for doneCh to confirm drainShutdown completed.
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return nil, errShutdownTimeout
	}

	return rw, nil
}

// runSyntheticTxThroughPoolViaTCPConn drives the deterministic synthetic
// tx through the production encoder + pool via the hijacked-conn path
// (conn != nil). Uses a real loopback TCP pair so net.Buffers.WriteTo
// hits the production writev(2) syscall path. Returns the bytes read
// from the client side.
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

	// Inline heartbeat AFTER the tx frame has drained. We read the prelude
	// + tx frame from the client first to ensure the worker has flushed
	// (otherwise an inline conn.Write here could land BEFORE the worker's
	// pending writev on a different goroutine, breaking determinism).
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

	// Now inline-write the heartbeat to the server side; the client
	// reads it after this point.
	if _, werr := srvConn.Write(enc.EncodeHeartbeat()); werr != nil {
		return nil, werr
	}

	// Shutdown — emits the §3.5 shutdown frame via drainShutdown.
	_ = p.Shutdown(context.Background())

	// Wait for doneCh + drain shutdown frame from client.
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		return nil, errShutdownTimeout
	}

	// Read everything still pending on the client side (heartbeat +
	// shutdown frame). Use a short read deadline; we keep reading until
	// the conn closes or 200ms passes with no data.
	_ = cli.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		n, rerr := cli.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {
			// EOF or timeout — either is the natural end of the stream.
			break
		}
	}
	return collected, nil
}

// waitFor polls cond every 1ms until it returns true or the budget
// expires. Returns nil on success or a context-deadline-style error on
// timeout. Avoids tight spin loops by sleeping 1ms per iteration.
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
