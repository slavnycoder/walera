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

type fakeReplConn struct {
	mu       sync.Mutex
	messages []pgproto3.BackendMessage
	msgIdx   int
	ackCalls []pglogrepl.StandbyStatusUpdate
	ackMu    sync.Mutex

	errAfterIdx int
	errOnErr    error

	sendACKErr error
}

func newFakeConn(msgs []pgproto3.BackendMessage) *fakeReplConn {
	return &fakeReplConn{
		messages:    msgs,
		errAfterIdx: -1,
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

func buildKeepaliveMsg(replyRequested bool) *pgproto3.CopyData {

	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID

	if replyRequested {
		data[17] = 1
	}
	return &pgproto3.CopyData{Data: data}
}

func buildXLogData(walData []byte) *pgproto3.CopyData {

	data := make([]byte, 25+len(walData))
	data[0] = pglogrepl.XLogDataByteID
	copy(data[25:], walData)
	return &pgproto3.CopyData{Data: data}
}

func encodeBeginMsg(xid uint32, finalLSN uint64) []byte {
	b := make([]byte, 21)
	b[0] = 'B'

	b[1] = byte(finalLSN >> 56)
	b[2] = byte(finalLSN >> 48)
	b[3] = byte(finalLSN >> 40)
	b[4] = byte(finalLSN >> 32)
	b[5] = byte(finalLSN >> 24)
	b[6] = byte(finalLSN >> 16)
	b[7] = byte(finalLSN >> 8)
	b[8] = byte(finalLSN)

	b[17] = byte(xid >> 24)
	b[18] = byte(xid >> 16)
	b[19] = byte(xid >> 8)
	b[20] = byte(xid)
	return b
}

func encodeCommitMsg(commitLSN uint64) []byte {
	b := make([]byte, 26)
	b[0] = 'C'

	b[2] = byte(commitLSN >> 56)
	b[3] = byte(commitLSN >> 48)
	b[4] = byte(commitLSN >> 40)
	b[5] = byte(commitLSN >> 32)
	b[6] = byte(commitLSN >> 24)
	b[7] = byte(commitLSN >> 16)
	b[8] = byte(commitLSN >> 8)
	b[9] = byte(commitLSN)

	b[10] = byte(commitLSN >> 56)
	b[11] = byte(commitLSN >> 48)
	b[12] = byte(commitLSN >> 40)
	b[13] = byte(commitLSN >> 32)
	b[14] = byte(commitLSN >> 24)
	b[15] = byte(commitLSN >> 16)
	b[16] = byte(commitLSN >> 8)
	b[17] = byte(commitLSN)

	return b
}

func encodeRelationMsg(relID uint32, schema, table string, columns []testColumn) []byte {

	var buf []byte
	buf = append(buf, 'R')
	buf = appendUint32(buf, relID)
	buf = append(buf, []byte(schema)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(table)...)
	buf = append(buf, 0)
	buf = append(buf, 'd')
	buf = appendUint16(buf, uint16(len(columns)))
	for _, col := range columns {
		buf = append(buf, col.flags)
		buf = append(buf, []byte(col.name)...)
		buf = append(buf, 0)
		buf = appendUint32(buf, col.dataType)
		buf = appendInt32(buf, -1)
	}
	return buf
}

func encodeInsertMsg(relID uint32, values []string) []byte {
	var buf []byte
	buf = append(buf, 'I')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N')
	buf = append(buf, encodeTupleData(values)...)
	return buf
}

func encodeTupleData(values []string) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(values)))
	for _, v := range values {
		buf = append(buf, 't')
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

	if elapsed >= 50*time.Millisecond {
		t.Errorf("ACK took %v, expected < 50ms", elapsed)
	}
}

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

func TestReplyRequestedACKTimingInRun(t *testing.T) {

	msgs := []pgproto3.BackendMessage{
		buildKeepaliveMsg(true),
	}
	fc := newFakeConn(msgs)

	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	startedAt := time.Now()

	go func() {
		r.runLoop(ctx) //nolint:errcheck
	}()

	if !fc.waitForACK(100 * time.Millisecond) {
		t.Fatal("SendACK not observed within 100ms after ReplyRequested keepalive")
	}
	elapsed := time.Since(startedAt)

	if elapsed >= 50*time.Millisecond {
		t.Errorf("ACK observed after %v, expected < 50ms", elapsed)
	}
}

func TestRunResetsTxBufferOnErrorReturn(t *testing.T) {

	relWAL := encodeRelationMsg(42, "public", "events", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
		{flags: 0x00, name: "payload", dataType: OIDText},
	})
	insWAL := encodeInsertMsg(42, []string{"1", "hello"})

	sentinelErr := errors.New("simulated connection error")

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

	select {
	case tx := <-r.txCh:
		t.Errorf("unexpected Tx on txCh after aborted run: ID=%d", tx.ID)
	default:

	}

	commitWAL := encodeCommitMsg(1001)
	msgs2 := []pgproto3.BackendMessage{
		buildXLogData(encodeBeginMsg(202, 2000)),
		buildXLogData(relWAL),
		buildXLogData(insWAL),
		buildXLogData(commitWAL),
	}
	fc2 := newFakeConn(msgs2)
	r.replConn = fc2

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- r.runLoop(ctx2)
	}()

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

func TestProcessWALMessageAllTypes(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

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

	if err := r.processWALMessage(ctx, encodeBeginMsg(55, 5000), txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Begin): %v", err)
	}

	insWAL := encodeInsertMsg(11, []string{"42", "thing"})
	if err := r.processWALMessage(ctx, insWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Insert): %v", err)
	}

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

	_ = r.processWALMessage(ctx, []byte{'Z', 0, 0, 0, 0}, txBld, relCache)
}

func TestProcessWALMessageRelationError(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	relWAL := encodeRelationMsg(77, "public", "bad_table", []testColumn{
		{flags: 0x01, name: "a", dataType: OIDInt4},
		{flags: 0x01, name: "b", dataType: OIDInt4},
	})
	err := r.processWALMessage(ctx, relWAL, txBld, relCache)
	if err != nil {
		t.Errorf("processWALMessage with bad relation should not return error, got: %v", err)
	}

	if _, ok := relCache.Get(77); ok {
		t.Error("bad relation should not be in cache")
	}
}

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

func TestReaderCheckPG(t *testing.T) {
	cfg := Config{
		PostgresDSN:     "postgres://localhost/test",
		ReplicationDSN:  "postgres://localhost/test",
		PublicationName: "testpub",
		SlotNamePrefix:  "walera",
	}
	r, _ := New(cfg, Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	if err := r.CheckPG(context.Background()); !errors.Is(err, ErrNotConnected) {
		t.Errorf("CheckPG at construction: got %v; want %v", err, ErrNotConnected)
	}

	r.connected.Store(true)
	if err := r.CheckPG(context.Background()); err != nil {
		t.Errorf("CheckPG when connected: got %v; want nil", err)
	}

	r.connected.Store(false)
	if err := r.CheckPG(context.Background()); !errors.Is(err, ErrNotConnected) {
		t.Errorf("CheckPG after disconnect: got %v; want %v", err, ErrNotConnected)
	}
}

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

func TestCurrentLSN(t *testing.T) {
	initial := CurrentLSN()
	testLSN := pglogrepl.LSN(0xDEAD0000BEEF)
	setLSN(testLSN)
	if got := CurrentLSN(); got != testLSN {
		t.Errorf("CurrentLSN() = %v, want %v", got, testLSN)
	}
	setLSN(initial)
}

func TestPgConnAdapterInterface(t *testing.T) {

	var _ replConn = (*pgConnAdapter)(nil)
}

func TestFakeConnInterface(t *testing.T) {
	var _ replConn = (*fakeReplConn)(nil)
}

func TestProcessWALMessageUpdateAndDelete(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	relWAL := encodeRelationMsg(22, "public", "products", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
		{flags: 0x00, name: "price", dataType: OIDText},
	})
	_ = r.processWALMessage(ctx, relWAL, txBld, relCache)

	_ = r.processWALMessage(ctx, encodeBeginMsg(66, 6000), txBld, relCache)

	updateWAL := encodeUpdateMsg(22, []string{"5", "19.99"})
	if err := r.processWALMessage(ctx, updateWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Update): %v", err)
	}

	deleteWAL := encodeDeleteMsg(22, []string{"5", ""})
	if err := r.processWALMessage(ctx, deleteWAL, txBld, relCache); err != nil {
		t.Fatalf("processWALMessage(Delete): %v", err)
	}

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

func TestProcessWALMessageTruncateAndIgnored(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	truncWAL := encodeTruncateMsg([]uint32{42})
	err := r.processWALMessage(ctx, truncWAL, txBld, relCache)
	if err != nil {
		t.Errorf("TruncateMessage should not error: %v", err)
	}

	typeWAL := encodeTypeMsg()
	err = r.processWALMessage(ctx, typeWAL, txBld, relCache)
	if err != nil {
		t.Errorf("TypeMessage should not error: %v", err)
	}

	origWAL := encodeOriginMsg()
	err = r.processWALMessage(ctx, origWAL, txBld, relCache)
	if err != nil {
		t.Errorf("OriginMessage should not error: %v", err)
	}
}

func TestRunLoopKeepaliveNoReply(t *testing.T) {
	msgs := []pgproto3.BackendMessage{
		buildKeepaliveMsg(false),
	}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go r.runLoop(ctx) //nolint:errcheck

	<-ctx.Done()
	time.Sleep(10 * time.Millisecond)
	if len(fc.AckCalls()) > 0 {
		t.Errorf("expected no ACK for ReplyRequested=false, got %d", len(fc.AckCalls()))
	}
}

func TestRunLoopNonCopyData(t *testing.T) {
	msgs := []pgproto3.BackendMessage{
		&pgproto3.NotificationResponse{},
	}
	fc := newFakeConn(msgs)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go r.runLoop(ctx) //nolint:errcheck
	<-ctx.Done()
}

func TestRunLoopBadXLogData(t *testing.T) {

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

func TestRunLoopBadKeepalive(t *testing.T) {

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

func encodeUpdateMsg(relID uint32, values []string) []byte {
	var buf []byte
	buf = append(buf, 'U')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N')
	buf = append(buf, encodeTupleData(values)...)
	return buf
}

func encodeDeleteMsg(relID uint32, values []string) []byte {
	var buf []byte
	buf = append(buf, 'D')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'O')
	buf = append(buf, encodeTupleData(values)...)
	return buf
}

func encodeTruncateMsg(relIDs []uint32) []byte {
	var buf []byte
	buf = append(buf, 'T')
	buf = appendUint32(buf, uint32(len(relIDs)))
	buf = append(buf, 0)
	for _, id := range relIDs {
		buf = appendUint32(buf, id)
	}
	return buf
}

func encodeTypeMsg() []byte {
	var buf []byte
	buf = append(buf, 'Y')
	buf = appendUint32(buf, 1)
	buf = append(buf, 0)
	buf = append(buf, 0)
	return buf
}

func encodeOriginMsg() []byte {
	var buf []byte
	buf = append(buf, 'O')

	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	buf = append(buf, []byte("origin")...)
	buf = append(buf, 0)
	return buf
}

func TestProcessWALMessageBadWALData(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)
	ctx := context.Background()
	txBld := newTxBuilder()
	relCache := newRelationCache()

	err := r.processWALMessage(ctx, []byte{}, txBld, relCache)

	if err != nil {
		t.Errorf("bad WAL data should not return fatal error, got: %v", err)
	}
}

func TestProcessWALMessageCommitCtxCancelled(t *testing.T) {
	logger := zerolog.Nop()
	fc := newFakeConn(nil)
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	txBld := newTxBuilder()
	relCache := newRelationCache()

	for i := 0; i < 8; i++ {
		r.txCh <- Tx{}
	}

	_ = r.processWALMessage(ctx, encodeBeginMsg(99, 9000), txBld, relCache)

	relWAL := encodeRelationMsg(33, "public", "z", []testColumn{
		{flags: 0x01, name: "id", dataType: OIDInt4},
	})
	_ = r.processWALMessage(ctx, relWAL, txBld, relCache)

	insWAL := encodeInsertMsg(33, []string{"1"})
	_ = r.processWALMessage(ctx, insWAL, txBld, relCache)

	err := r.processWALMessage(ctx, encodeCommitMsg(9001), txBld, relCache)
	if err == nil {
		t.Error("expected context error from processWALMessage(Commit) with cancelled ctx")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRunLoopProcessWALError(t *testing.T) {

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

	r := &Reader{
		log:      logger,
		txCh:     make(chan Tx),
		replConn: fc,
		metrics:  metrics.New(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.runLoop(ctx)
	if err == nil {
		t.Error("expected error when ctx cancelled during Commit, got nil")
	}
}

func TestRunLoopContextCancelledOnReceive(t *testing.T) {

	fc := newFakeConn(nil)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := r.runLoop(ctx)

	if err == nil {
		t.Error("expected error from runLoop when ctx cancelled, got nil")
	}
}

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

	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 2 {
		t.Errorf("counter after two failing ticks = %v; want 2", got)
	}
}

func TestStandbyTicker_SendACKSuccess_NoCounter(t *testing.T) {
	fc := newFakeConn(nil)
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	r.tickStandby(context.Background())
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Errorf("counter after successful tick = %v; want 0", got)
	}

	if len(fc.AckCalls()) != 1 {
		t.Errorf("ack call count = %d; want 1", len(fc.AckCalls()))
	}
}

func TestStandbyTicker_ContextCanceled_DoesNotIncrementCounter(t *testing.T) {
	fc := newFakeConn(nil)
	fc.sendACKErr = context.Canceled
	logger := zerolog.Nop()
	r := newReaderForTest(fc, logger)

	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Fatalf("counter pre-tick = %v; want 0", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.tickStandby(ctx)
	if got := gatherCounterNoLabel(t, r, "walera_wal_standby_ack_failures_total"); got != 0 {
		t.Errorf("counter after ctx-cancelled tick = %v; want 0 (WR-01: ctx errors are not ACK failures)", got)
	}
}

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
