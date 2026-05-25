package wal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// --- fake replication connection harness ---

// fakeReplConn implements the replConn interface for unit testing.
// It feeds a pre-configured queue of messages to ReceiveMessage callers, and records
// all SendACK calls.
type fakeReplConn struct {
	mu       sync.Mutex
	messages []pgproto3.BackendMessage // queue of messages to return from ReceiveMessage
	msgIdx   int
	ackCalls []pglogrepl.StandbyStatusUpdate
	ackMu    sync.Mutex

	// errAfterIdx, if >= 0, causes ReceiveMessage to return errOnErr after msgIdx reaches it.
	errAfterIdx int
	errOnErr    error

	// sendACKErr, if non-nil, causes SendACK to return this error instead of
	// nil (used by SEC-11 standby-ticker tests to exercise the counter
	// increment branch).
	sendACKErr error
}

func newFakeConn(msgs []pgproto3.BackendMessage) *fakeReplConn {
	return &fakeReplConn{
		messages:    msgs,
		errAfterIdx: -1, // disabled
	}
}

func newFakeConnWithError(msgs []pgproto3.BackendMessage, afterIdx int, err error) *fakeReplConn {
	fc := newFakeConn(msgs)
	fc.errAfterIdx = afterIdx
	fc.errOnErr = err
	return fc
}

func (f *fakeReplConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.errAfterIdx >= 0 && f.msgIdx >= f.errAfterIdx {
		return nil, f.errOnErr
	}

	if f.msgIdx >= len(f.messages) {
		// Block until context is cancelled (simulate idle connection).
		f.mu.Unlock()
		<-ctx.Done()
		f.mu.Lock()
		return nil, ctx.Err()
	}

	msg := f.messages[f.msgIdx]
	f.msgIdx++
	return msg, nil
}

func (f *fakeReplConn) SendACK(_ context.Context, ssu pglogrepl.StandbyStatusUpdate) error {
	f.ackMu.Lock()
	defer f.ackMu.Unlock()
	f.ackCalls = append(f.ackCalls, ssu)
	return f.sendACKErr
}

func (f *fakeReplConn) AckCalls() []pglogrepl.StandbyStatusUpdate {
	f.ackMu.Lock()
	defer f.ackMu.Unlock()
	result := make([]pglogrepl.StandbyStatusUpdate, len(f.ackCalls))
	copy(result, f.ackCalls)
	return result
}

// waitForACK blocks until at least one ACK is recorded or timeout expires.
func (f *fakeReplConn) waitForACK(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(f.AckCalls()) > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// --- CopyData helper builders ---

// buildKeepaliveMsg builds a pgproto3.CopyData carrying a PrimaryKeepaliveMessage.
func buildKeepaliveMsg(replyRequested bool) *pgproto3.CopyData {
	// PrimaryKeepaliveMessage wire format (17 bytes after the byte ID):
	//   byte[0]    = 'k' (PrimaryKeepaliveMessageByteID)
	//   byte[1-8]  = ServerWALEnd (uint64 big-endian)
	//   byte[9-16] = ServerTime (int64 big-endian, microseconds since 2000-01-01)
	//   byte[17]   = ReplyRequested (0 or 1)
	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	// ServerWALEnd = 0, ServerTime = 0
	if replyRequested {
		data[17] = 1
	}
	return &pgproto3.CopyData{Data: data}
}

// buildXLogData builds a pgproto3.CopyData carrying an XLogData message with the given WAL payload.
func buildXLogData(walData []byte) *pgproto3.CopyData {
	// XLogData wire format (24 bytes header):
	//   byte[0]    = 'w' (XLogDataByteID)
	//   byte[1-8]  = WALStart (uint64 big-endian) = 0
	//   byte[9-16] = ServerWALEnd (uint64 big-endian) = 0
	//   byte[17-24]= ServerTime (int64 big-endian) = 0
	//   byte[25+]  = WALData
	data := make([]byte, 25+len(walData))
	data[0] = pglogrepl.XLogDataByteID
	copy(data[25:], walData)
	return &pgproto3.CopyData{Data: data}
}

// --- WAL message encoders ---

// encodeBeginMsg encodes a pglogrepl.BeginMessage into pgoutput wire bytes.
// Format: 'B' + FinalLSN (8) + CommitTime (8) + Xid (4)
func encodeBeginMsg(xid uint32, finalLSN uint64) []byte {
	b := make([]byte, 21)
	b[0] = 'B'
	// FinalLSN
	b[1] = byte(finalLSN >> 56)
	b[2] = byte(finalLSN >> 48)
	b[3] = byte(finalLSN >> 40)
	b[4] = byte(finalLSN >> 32)
	b[5] = byte(finalLSN >> 24)
	b[6] = byte(finalLSN >> 16)
	b[7] = byte(finalLSN >> 8)
	b[8] = byte(finalLSN)
	// CommitTime (8 bytes, zero)
	// Xid (4 bytes, big-endian)
	b[17] = byte(xid >> 24)
	b[18] = byte(xid >> 16)
	b[19] = byte(xid >> 8)
	b[20] = byte(xid)
	return b
}

// encodeCommitMsg encodes a pglogrepl.CommitMessage into pgoutput wire bytes.
// Format: 'C' + Flags (1) + CommitLSN (8) + TransactionEndLSN (8) + CommitTime (8)
func encodeCommitMsg(commitLSN uint64) []byte {
	b := make([]byte, 26)
	b[0] = 'C'
	// Flags = 0
	// CommitLSN
	b[2] = byte(commitLSN >> 56)
	b[3] = byte(commitLSN >> 48)
	b[4] = byte(commitLSN >> 40)
	b[5] = byte(commitLSN >> 32)
	b[6] = byte(commitLSN >> 24)
	b[7] = byte(commitLSN >> 16)
	b[8] = byte(commitLSN >> 8)
	b[9] = byte(commitLSN)
	// TransactionEndLSN (same as CommitLSN for simplicity)
	b[10] = byte(commitLSN >> 56)
	b[11] = byte(commitLSN >> 48)
	b[12] = byte(commitLSN >> 40)
	b[13] = byte(commitLSN >> 32)
	b[14] = byte(commitLSN >> 24)
	b[15] = byte(commitLSN >> 16)
	b[16] = byte(commitLSN >> 8)
	b[17] = byte(commitLSN)
	// CommitTime (8 bytes, zero)
	return b
}

// encodeRelationMsg encodes a RelationMessage for a simple single-PK table.
// This is the minimal encoding needed for the reader test.
func encodeRelationMsg(relID uint32, schema, table string, columns []testColumn) []byte {
	// Format: 'R' + RelationID (4) + Namespace + '\0' + RelationName + '\0' + ReplicaIdentity (1) + ColumnCount (2) + columns
	var buf []byte
	buf = append(buf, 'R')
	buf = appendUint32(buf, relID)
	buf = append(buf, []byte(schema)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(table)...)
	buf = append(buf, 0)
	buf = append(buf, 'd') // ReplicaIdentity DEFAULT
	buf = appendUint16(buf, uint16(len(columns)))
	for _, col := range columns {
		buf = append(buf, col.flags)
		buf = append(buf, []byte(col.name)...)
		buf = append(buf, 0)
		buf = appendUint32(buf, col.dataType)
		buf = appendInt32(buf, -1) // TypeModifier
	}
	return buf
}

// encodeInsertMsg encodes an InsertMessage.
// Format: 'I' + RelationID (4) + 'N' + TupleData
func encodeInsertMsg(relID uint32, values []string) []byte {
	var buf []byte
	buf = append(buf, 'I')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N') // NewTuple indicator
	buf = append(buf, encodeTupleData(values)...)
	return buf
}

func encodeTupleData(values []string) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(values)))
	for _, v := range values {
		buf = append(buf, 't') // text
		buf = appendUint32(buf, uint32(len(v)))
		buf = append(buf, []byte(v)...)
	}
	return buf
}

type testColumn struct {
	flags    uint8
	name     string
	dataType uint32
}

func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendInt32(buf []byte, v int32) []byte {
	return appendUint32(buf, uint32(v))
}

// --- TestReplyRequestedACK ---

// TestReplyRequestedACK verifies that handleKeepaliveMsg with ReplyRequested=true
// calls the sendACK callback immediately, and that the elapsed time is < 50ms.
// Success criterion #2, D-21.
func TestReplyRequestedACK(t *testing.T) {
	var called atomic.Bool
	var elapsed time.Duration

	sendACK := func(ctx context.Context) error {
		called.Store(true)
		return nil
	}

	startedAt := time.Now()
	pkm := pglogrepl.PrimaryKeepaliveMessage{ReplyRequested: true}
	result := handleKeepaliveMsg(pkm, sendACK)
	elapsed = time.Since(startedAt)

	if !result {
		t.Error("handleKeepaliveMsg should return true when ReplyRequested=true")
	}
	if !called.Load() {
		t.Error("sendACK was not called for ReplyRequested=true")
	}
	// The time.Since assertion is a contract: ACK must happen synchronously within 50ms.
	if elapsed >= 50*time.Millisecond {
		t.Errorf("ACK took %v, expected < 50ms", elapsed)
	}
}

// TestReplyNotRequestedNoACK verifies that handleKeepaliveMsg with ReplyRequested=false
// does NOT call the sendACK callback.
func TestReplyNotRequestedNoACK(t *testing.T) {
	var called atomic.Bool
	sendACK := func(ctx context.Context) error {
		called.Store(true)
		return nil
	}
	pkm := pglogrepl.PrimaryKeepaliveMessage{ReplyRequested: false}
	result := handleKeepaliveMsg(pkm, sendACK)
	if result {
		t.Error("handleKeepaliveMsg should return false when ReplyRequested=false")
	}
	if called.Load() {
		t.Error("sendACK should NOT be called when ReplyRequested=false")
	}
}

// TestReplyRequestedACKTimingInRun drives the real reader loop via the fake conn
// harness. It injects a PrimaryKeepaliveMessage{ReplyRequested:true} and asserts that
// a SendACK is recorded on the fake conn within 50ms wall-clock.
func TestReplyRequestedACKTimingInRun(t *testing.T) {
	// Build a message queue: one keepalive with ReplyRequested=true, then block.
	msgs := []pgproto3.BackendMessage{
		buildKeepaliveMsg(true),
	}
	fc := newFakeConn(msgs)

	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	startedAt := time.Now()
	// Run reader in background; it will block on ReceiveMessage after processing the keepalive.
	go func() {
		r.runLoop(ctx) //nolint:errcheck
	}()

	// Wait for ACK to be recorded on fake conn within 50ms.
	if !fc.waitForACK(100 * time.Millisecond) {
		t.Fatal("SendACK not observed within 100ms after ReplyRequested keepalive")
	}
	elapsed := time.Since(startedAt)
	// The 50ms contract: ACK must be observed within 50ms wall-clock.
	if elapsed >= 50*time.Millisecond {
		t.Errorf("ACK observed after %v, expected < 50ms", elapsed)
	}
}

// TestRunResetsTxBufferOnErrorReturn verifies that a failed Run() (aborted mid-transaction)
// does not carry forward partial state into a subsequent Run() call.
//
// Protocol:
//  1. First runLoop call: feed Begin + Relation + Insert; then inject ReceiveMessage error.
//     Expect: runLoop returns the injected error; no Tx emitted on txCh.
//  2. Second runLoop call (new fake conn): feed Begin + Relation + Insert + Commit.
//     Expect: exactly one Tx emitted on txCh with the correct ID.
func TestRunResetsTxBufferOnErrorReturn(t *testing.T) {
	// Encode the relation and insert messages.
	relWAL := encodeRelationMsg(42, "public", "events", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
		{flags: 0x00, name: "payload", dataType: OIDText},
	})
	insWAL := encodeInsertMsg(42, []string{"1", "hello"})

	sentinelErr := errors.New("simulated connection error")

	// First run: Begin + Relation + Insert, then error.
	msgs1 := []pgproto3.BackendMessage{
		buildXLogData(encodeBeginMsg(101, 1000)),
		buildXLogData(relWAL),
		buildXLogData(insWAL),
	}
	fc1 := newFakeConnWithError(msgs1, len(msgs1), sentinelErr)

	logger := zerolog.Nop()
	r := newReaderForTest(fc1, logger)

	ctx := context.Background()
	err1 := r.runLoop(ctx)
	if err1 == nil {
		t.Fatal("expected first runLoop to return error, got nil")
	}
	if !errors.Is(err1, sentinelErr) {
		t.Errorf("expected sentinelErr, got %v", err1)
	}

	// No Tx should have been emitted during the aborted run.
	select {
	case tx := <-r.txCh:
		t.Errorf("unexpected Tx on txCh after aborted run: ID=%d", tx.ID)
	default:
		// Good — no partial Tx.
	}

	// Second run: clean Begin + Relation + Insert + Commit.
	// Encode commit with LSN 1001.
	commitWAL := encodeCommitMsg(1001)
	msgs2 := []pgproto3.BackendMessage{
		buildXLogData(encodeBeginMsg(202, 2000)),
		buildXLogData(relWAL),
		buildXLogData(insWAL),
		buildXLogData(commitWAL),
	}
	fc2 := newFakeConn(msgs2)
	r.replConn = fc2 // replace the conn for second run

	// Run in a goroutine; it will block after emitting the Tx.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- r.runLoop(ctx2)
	}()

	// Collect the Tx from txCh.
	var gotTx *Tx
	select {
	case tx := <-r.txCh:
		gotTx = &tx
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Tx from second run")
	}
	cancel2()
	<-runErrCh

	if gotTx.ID != 202 {
		t.Errorf("expected Tx.ID=202, got %d", gotTx.ID)
	}
	if len(gotTx.Changes) != 1 {
		t.Errorf("expected 1 change in second Tx, got %d", len(gotTx.Changes))
	}
}

// TestProcessWALMessageAllTypes verifies that processWALMessage correctly dispatches
// all WAL message types without error and that Commit emits a Tx.
func TestProcessWALMessageAllTypes(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Relation message for OID 11.
	relWAL := encodeRelationMsg(11, "public", "items", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
		{flags: 0x00, name: "name", dataType: OIDText},
	})
	if err := r.processWALMessage(ctx, relWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Relation): %v", err)
	}
	if _, ok := relCache.Get(11); !ok {
		t.Error("relation 11 should be in cache after RelationMessage")
	}

	// BeginMessage.
	if err := r.processWALMessage(ctx, encodeBeginMsg(55, 5000), txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Begin): %v", err)
	}

	// InsertMessage.
	insWAL := encodeInsertMsg(11, []string{"42", "thing"})
	if err := r.processWALMessage(ctx, insWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Insert): %v", err)
	}

	// CommitMessage — should emit a Tx on txCh.
	if err := r.processWALMessage(ctx, encodeCommitMsg(5001), txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Commit): %v", err)
	}
	select {
	case tx := <-r.txCh:
		if tx.ID != 55 {
			t.Errorf("expected Tx.ID=55, got %d", tx.ID)
		}
		if len(tx.Changes) != 1 {
			t.Errorf("expected 1 change, got %d", len(tx.Changes))
		}
	default:
		t.Error("expected Tx on txCh after Commit, got none")
	}

	// Unknown message type — should not error.
	_ = r.processWALMessage(ctx, []byte{'Z', 0, 0, 0, 0}, txBld, relCache)
}

// TestProcessWALMessageRelationError verifies that a relation with an unsupported PK
// logs a warning but does not return a fatal error.
func TestProcessWALMessageRelationError(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Encode a relation with a composite PK (both columns flagged as PK).
	relWAL := encodeRelationMsg(77, "public", "bad_table", []testColumn{
		{flags: 0x01, name: "a", dataType: OIDInt4},
		{flags: 0x01, name: "b", dataType: OIDInt4},
	})
	err := r.processWALMessage(ctx, relWAL, txBld, relCache)
	if err != nil {
		t.Errorf("processWALMessage with bad relation should not return error, got: %v", err)
	}
	// Table should not be in cache (rejected by Update).
	if _, ok := relCache.Get(77); ok {
		t.Error("bad relation should not be in cache")
	}
}

// TestReaderIsConnectedAtConstruction verifies that a freshly-constructed
// Reader reports IsConnected()==false before Run is invoked. The lifecycle
// toggle (Store(true) inside Run after replConn assignment, deferred
// Store(false) on exit) is exercised by the health.Server tests; this
// test only locks the at-rest state.
func TestReaderIsConnectedAtConstruction(t *testing.T) {
	cfg := Config{
		PostgresDSN:     "postgres://localhost/test",
		ReplicationDSN:  "postgres://localhost/test",
		PublicationName: "testpub",
		SlotNamePrefix:  "walera",
	}
	r, _ := New(cfg, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})
	if r.IsConnected() {
		t.Error("IsConnected() at construction: got true; want false")
	}
}

// TestReaderCheckPG verifies the PgChecker bridge: CheckPG returns nil iff
// IsConnected() reports true; ErrNotConnected otherwise. The method is
// expected to satisfy internal/health.PgChecker without taking a context
// dependency in the body today.
func TestReaderCheckPG(t *testing.T) {
	cfg := Config{
		PostgresDSN:     "postgres://localhost/test",
		ReplicationDSN:  "postgres://localhost/test",
		PublicationName: "testpub",
		SlotNamePrefix:  "walera",
	}
	r, _ := New(cfg, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	// At construction: not connected.
	if err := r.CheckPG(context.Background()); !errors.Is(err, ErrNotConnected) {
		t.Errorf("CheckPG at construction: got %v; want %v", err, ErrNotConnected)
	}

	// Simulate replication active.
	r.connected.Store(true)
	if err := r.CheckPG(context.Background()); err != nil {
		t.Errorf("CheckPG when connected: got %v; want nil", err)
	}

	// Simulate Run exit.
	r.connected.Store(false)
	if err := r.CheckPG(context.Background()); !errors.Is(err, ErrNotConnected) {
		t.Errorf("CheckPG after disconnect: got %v; want %v", err, ErrNotConnected)
	}
}

// TestNewReader verifies that New() returns a non-nil Reader and readable channel.
func TestNewReader(t *testing.T) {
	cfg := Config{
		PostgresDSN:     "postgres://localhost/test",
		ReplicationDSN:  "postgres://localhost/test",
		PublicationName: "testpub",
		SlotNamePrefix:  "walera",
	}
	logger := zerolog.Nop()
	reader, ch := New(cfg, Deps{Logger: logger, Metrics: metrics.New()})
	if reader == nil {
		t.Error("New returned nil reader")
	}
	if ch == nil {
		t.Error("New returned nil channel")
	}
}

// TestCurrentLSN verifies that setLSN and CurrentLSN round-trip correctly.
func TestCurrentLSN(t *testing.T) {
	initial := CurrentLSN()
	testLSN := pglogrepl.LSN(0xDEAD0000BEEF)
	setLSN(testLSN)
	if got := CurrentLSN(); got != testLSN {
		t.Errorf("CurrentLSN() = %v, want %v", got, testLSN)
	}
	setLSN(initial) // restore
}

// TestPgConnAdapterInterface verifies that pgConnAdapter satisfies replConn.
func TestPgConnAdapterInterface(t *testing.T) {
	// Compile-time assertion: pgConnAdapter must implement replConn.
	var _ replConn = (*pgConnAdapter)(nil)
}

// TestFakeConnInterface verifies that fakeReplConn satisfies replConn (sanity check).
func TestFakeConnInterface(t *testing.T) {
	var _ replConn = (*fakeReplConn)(nil)
}

// TestProcessWALMessageUpdateAndDelete adds coverage for Update and Delete dispatch.
func TestProcessWALMessageUpdateAndDelete(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Set up relation.
	relWAL := encodeRelationMsg(22, "public", "products", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
		{flags: 0x00, name: "price", dataType: OIDText},
	})
	_ = r.processWALMessage(ctx, relWAL, txBld, relCache)

	// Begin.
	_ = r.processWALMessage(ctx, encodeBeginMsg(66, 6000), txBld, relCache)

	// UpdateMessage — NewTuple has both columns.
	updateWAL := encodeUpdateMsg(22, []string{"5", "19.99"})
	if err := r.processWALMessage(ctx, updateWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Update): %v", err)
	}

	// DeleteMessage.
	deleteWAL := encodeDeleteMsg(22, []string{"5", ""})
	if err := r.processWALMessage(ctx, deleteWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Delete): %v", err)
	}

	// Commit.
	_ = r.processWALMessage(ctx, encodeCommitMsg(6001), txBld, relCache)

	select {
	case tx := <-r.txCh:
		if len(tx.Changes) != 2 {
			t.Errorf("expected 2 changes (update+delete), got %d", len(tx.Changes))
		}
		if tx.Changes[0].Op != OpUpdate {
			t.Errorf("expected first change OpUpdate, got %q", tx.Changes[0].Op)
		}
		if tx.Changes[1].Op != OpDelete {
			t.Errorf("expected second change OpDelete, got %q", tx.Changes[1].Op)
		}
	default:
		t.Error("expected Tx on txCh")
	}
}

// TestProcessWALMessageTruncateAndIgnored adds coverage for Truncate, Type, and Origin messages.
func TestProcessWALMessageTruncateAndIgnored(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Truncate message (byte 'T').
	truncWAL := encodeTruncateMsg([]uint32{42})
	err := r.processWALMessage(ctx, truncWAL, txBld, relCache)
	if err != nil {
		t.Errorf("TruncateMessage should not error: %v", err)
	}

	// Type message (byte 'Y').
	typeWAL := encodeTypeMsg()
	err = r.processWALMessage(ctx, typeWAL, txBld, relCache)
	if err != nil {
		t.Errorf("TypeMessage should not error: %v", err)
	}

	// Origin message (byte 'O').
	origWAL := encodeOriginMsg()
	err = r.processWALMessage(ctx, origWAL, txBld, relCache)
	if err != nil {
		t.Errorf("OriginMessage should not error: %v", err)
	}
}

// TestRunLoopKeepaliveNoReply verifies that a keepalive with ReplyRequested=false
// does not trigger an ACK.
func TestRunLoopKeepaliveNoReply(t *testing.T) {
	msgs := []pgproto3.BackendMessage{
		buildKeepaliveMsg(false), // no reply requested
	}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go r.runLoop(ctx) //nolint:errcheck

	// Wait for context to expire, then check no ACK was sent.
	<-ctx.Done()
	time.Sleep(10 * time.Millisecond) // let goroutine settle
	if len(fc.AckCalls()) > 0 {
		t.Errorf("expected no ACK for ReplyRequested=false, got %d", len(fc.AckCalls()))
	}
}

// TestRunLoopNonCopyData verifies that non-CopyData messages are skipped.
func TestRunLoopNonCopyData(t *testing.T) {
	msgs := []pgproto3.BackendMessage{
		&pgproto3.NotificationResponse{}, // not a CopyData
	}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Should not panic or error on non-CopyData messages.
	go r.runLoop(ctx) //nolint:errcheck
	<-ctx.Done()
}

// TestRunLoopBadXLogData verifies that malformed XLogData does not crash the loop.
func TestRunLoopBadXLogData(t *testing.T) {
	// XLogData with too-short data (< 24 bytes after the 'w' byte).
	badXLog := &pgproto3.CopyData{Data: []byte{pglogrepl.XLogDataByteID, 0, 1}}
	msgs := []pgproto3.BackendMessage{badXLog}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go r.runLoop(ctx) //nolint:errcheck
	<-ctx.Done()
}

// TestRunLoopBadKeepalive verifies that a malformed keepalive message does not crash the loop.
func TestRunLoopBadKeepalive(t *testing.T) {
	// Keepalive with wrong length (should be 17 bytes after the 'k' byte).
	badKeepalive := &pgproto3.CopyData{Data: []byte{pglogrepl.PrimaryKeepaliveMessageByteID, 0, 1}}
	msgs := []pgproto3.BackendMessage{badKeepalive}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go r.runLoop(ctx) //nolint:errcheck
	<-ctx.Done()
}

// TestRunLoopEmptyCopyData verifies that a CopyData with empty Data is skipped.
func TestRunLoopEmptyCopyData(t *testing.T) {
	msgs := []pgproto3.BackendMessage{
		&pgproto3.CopyData{Data: []byte{}},
	}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go r.runLoop(ctx) //nolint:errcheck
	<-ctx.Done()
}

// --- additional WAL message encoders ---

// encodeUpdateMsg encodes a minimal UpdateMessage (no OldTuple, NewTuple only).
// Format: 'U' + RelationID (4) + 'N' + TupleData
func encodeUpdateMsg(relID uint32, values []string) []byte {
	var buf []byte
	buf = append(buf, 'U')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N') // NewTuple indicator
	buf = append(buf, encodeTupleData(values)...)
	return buf
}

// encodeDeleteMsg encodes a minimal DeleteMessage with OldTuple.
// Format: 'D' + RelationID (4) + 'O' + TupleData
func encodeDeleteMsg(relID uint32, values []string) []byte {
	var buf []byte
	buf = append(buf, 'D')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'O') // OldTuple type 'O' (full row)
	buf = append(buf, encodeTupleData(values)...)
	return buf
}

// encodeTruncateMsg encodes a minimal TruncateMessage.
// Format: 'T' + RelationNum (4) + Options (1) + RelationID (4) per relation
func encodeTruncateMsg(relIDs []uint32) []byte {
	var buf []byte
	buf = append(buf, 'T')
	buf = appendUint32(buf, uint32(len(relIDs)))
	buf = append(buf, 0) // Options
	for _, id := range relIDs {
		buf = appendUint32(buf, id)
	}
	return buf
}

// encodeTypeMsg encodes a minimal TypeMessage.
// Format: 'Y' + OID (4) + Namespace + '\0' + Name + '\0'
func encodeTypeMsg() []byte {
	var buf []byte
	buf = append(buf, 'Y')
	buf = appendUint32(buf, 1) // OID
	buf = append(buf, 0)       // namespace (empty)
	buf = append(buf, 0)       // name (empty)
	return buf
}

// encodeOriginMsg encodes a minimal OriginMessage.
// Format: 'O' + CommitLSN (8) + Name + '\0'
func encodeOriginMsg() []byte {
	var buf []byte
	buf = append(buf, 'O')
	// CommitLSN (8 bytes, zero)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	buf = append(buf, []byte("origin")...)
	buf = append(buf, 0) // null terminator
	return buf
}

// TestProcessWALMessageBadWALData verifies that a parse error for malformed WAL
// data logs a warning and does not return a fatal error.
func TestProcessWALMessageBadWALData(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Empty WAL data will fail pglogrepl.Parse.
	err := r.processWALMessage(ctx, []byte{}, txBld, relCache)
	// Should not return an error (just logs a warning).
	if err != nil {
		t.Errorf("bad WAL data should not return fatal error, got: %v", err)
	}
}

// TestProcessWALMessageCommitCtxCancelled verifies that a cancelled context
// causes processWALMessage to return the context error during Commit.
func TestProcessWALMessageCommitCtxCancelled(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)

	// Use a pre-cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	txBld := newTxBuilder()
	relCache := newRelationCache()

	// Drain the buffered txCh to ensure it blocks.
	// newReaderForTest uses cap=8, drain it.
	for i := 0; i < 8; i++ {
		r.txCh <- Tx{} // fill the channel
	}

	// Begin a transaction.
	_ = r.processWALMessage(ctx, encodeBeginMsg(99, 9000), txBld, relCache)
	// Set up a relation and insert so the Commit has at least one change.
	relWAL := encodeRelationMsg(33, "public", "z", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
	})
	_ = r.processWALMessage(ctx, relWAL, txBld, relCache)

	insWAL := encodeInsertMsg(33, []string{"1"})
	_ = r.processWALMessage(ctx, insWAL, txBld, relCache)

	// Commit with cancelled context — should return ctx.Err().
	err := r.processWALMessage(ctx, encodeCommitMsg(9001), txBld, relCache)
	if err == nil {
		t.Error("expected context error from processWALMessage(Commit) with cancelled ctx")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestRunLoopProcessWALError verifies that runLoop exits and returns an error when
// processWALMessage returns a non-nil error (e.g., ctx cancelled during Commit).
func TestRunLoopProcessWALError(t *testing.T) {
	// Setup: relation + begin + insert + commit, with txCh full so Commit blocks on ctx.Done.
	relWAL := encodeRelationMsg(55, "public", "things", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
	})
	msgs := []pgproto3.BackendMessage{
		buildXLogData(relWAL),
		buildXLogData(encodeBeginMsg(999, 99000)),
		buildXLogData(encodeInsertMsg(55, []string{"1"})),
		buildXLogData(encodeCommitMsg(99001)),
	}
	fc := newFakeConn(msgs)

	logger := zerolog.Nop()
	// Create reader with cap-0 txCh to force ctx.Done() branch on Commit.
	r := &Reader{
		log:      logger,
		txCh:     make(chan Tx, 0), // zero capacity — forces ctx.Done path
		replConn: fc,
		metrics:  metrics.New(),
	}

	// Use a pre-cancelled context so Commit's ctx.Done fires.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.runLoop(ctx)
	if err == nil {
		t.Error("expected error when ctx cancelled during Commit, got nil")
	}
}

// TestRunLoopContextCancelledOnReceive verifies that runLoop exits cleanly when
// the context is cancelled while waiting for a message.
func TestRunLoopContextCancelledOnReceive(t *testing.T) {
	// Empty message queue — ReceiveMessage will block until ctx cancelled.
	fc := newFakeConn(nil)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := r.runLoop(ctx)
	// Should return a non-nil error (context.Canceled wrapped in "wal: ReceiveMessage").
	if err == nil {
		t.Error("expected error from runLoop when ctx cancelled, got nil")
	}
}

// --- standby-ticker ACK failure counter ---

// gatherCounterNoLabel walks the Gather() output for the named counter family
// and returns the single child's value (no labels). Returns 0 if absent.
func gatherCounterNoLabel(t *testing.T, r *Reader, name string) float64 {
	t.Helper()
	fams, err := r.metrics.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		ms := fam.GetMetric()
		if len(ms) == 0 {
			return 0
		}
		return ms[0].GetCounter().GetValue()
	}
	return 0
}

// TestStandbyTicker_SendACKError_IncrementsCounter — SEC-11 / F-P2-08.
// Drives the standby-ticker body directly (via the test-seam tickStandby
// method) so we exercise the SendACK error branch without waiting on the
// production 5-second ticker. Asserts the counter increments by exactly
// one and that the ticker body returns cleanly (does not panic, does not
// exit).
func TestStandbyTicker_SendACKError_IncrementsCounter(t *testing.T) {
	fc := newFakeConn(nil)
	fc.sendACKErr = errors.New("fake send-ack error")
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Fatalf("counter pre-tick = %v; want 0", got)
	}
	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 1 {
		t.Errorf("counter after one failing tick = %v; want 1", got)
	}
	// Second failing tick — counter should advance to 2 (proves the ticker
	// body did not become poisoned by the first error).
	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 2 {
		t.Errorf("counter after two failing ticks = %v; want 2", got)
	}
}

// TestStandbyTicker_SendACKSuccess_NoCounter — SEC-11 / F-P2-08.
// Asserts the counter is NOT incremented when SendACK returns nil.
func TestStandbyTicker_SendACKSuccess_NoCounter(t *testing.T) {
	fc := newFakeConn(nil)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Errorf("counter after successful tick = %v; want 0", got)
	}
	// And we observed an ACK call.
	if len(fc.AckCalls()) != 1 {
		t.Errorf("ack call count = %d; want 1", len(fc.AckCalls()))
	}
}

// TestStandbyTicker_ContextCanceled_DoesNotIncrementCounter — WR-01.
// Asserts that when SendACK returns context.Canceled (graceful shutdown
// or reconnect tickerCancel()), the SEC-11 counter is NOT incremented.
// This prevents the WaleraStandbyAckFailures P1 alert from firing on
// benign reconnect runs.
func TestStandbyTicker_ContextCanceled_DoesNotIncrementCounter(t *testing.T) {
	fc := newFakeConn(nil)
	fc.sendACKErr = context.Canceled
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Fatalf("counter pre-tick = %v; want 0", got)
	}
	// Pass a cancelled context so the err is genuinely a ctx-error from
	// the caller's perspective (defense-in-depth: even if SendACK didn't
	// already return ctx.Err, the cancelled parent ctx would trigger it
	// in production).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.tickStandby(ctx)
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Errorf("counter after ctx-cancelled tick = %v; want 0 (WR-01: ctx errors are not ACK failures)", got)
	}
}

// TestStandbyTicker_ContextDeadlineExceeded_DoesNotIncrementCounter — WR-01.
// Same as above for context.DeadlineExceeded.
func TestStandbyTicker_ContextDeadlineExceeded_DoesNotIncrementCounter(t *testing.T) {
	fc := newFakeConn(nil)
	fc.sendACKErr = context.DeadlineExceeded
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Errorf("counter after deadline-exceeded tick = %v; want 0 (WR-01)", got)
	}
}

// TestStandbyTicker_WrappedContextError_DoesNotIncrementCounter — WR-01.
// Asserts the errors.Is check unwraps wrapped ctx errors (defense
// against future code that wraps SendACK's err with additional context).
func TestStandbyTicker_WrappedContextError_DoesNotIncrementCounter(t *testing.T) {
	fc := newFakeConn(nil)
	fc.sendACKErr = errors.Join(errors.New("wrapper"), context.Canceled)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Errorf("counter after wrapped-ctx-cancel tick = %v; want 0 (WR-01: errors.Is unwraps)", got)
	}
}
