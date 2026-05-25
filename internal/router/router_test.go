// Package router — router_test.go exercises Broadcaster: fan-out, per-tx
// atomicity, start_lsn filter, tx_too_large cap, slow-consumer disconnect,
// Ingest exit shapes, gauge decrement, and the multi-root sentinel.
//
// All tests use t.Parallel() and the stdlib `testing` package only — no
// testify. Metric assertions go through reg.Gatherer().Gather().
//
// Tests live in `package router` (not `router_test`) so they can poke at
// unexported identifiers if needed; the public API is exercised exclusively.
package router

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/wal"
)

// stubEncoder is the package-private encoderIface stub used by every router
// test that constructs a Broadcaster via New. It captures the most-recent
// Event handed in via Encode and exposes it via lastEvent() so the recorder
// sendFunc closure can immediately pair the incoming []byte frame back with
// the Event that produced it.
//
// Single-tx test pattern (the vast majority of the suite):
//   - routeTx encodes ev_subA (stubEncoder stores ev_subA in lastEvent)
//   - routeTx calls subA.send(frame) → recorder appends frame + reads
//     lastEvent into events[]
//   - rinse-repeat per matched sub. Because routeTx runs in a single
//     goroutine, the "lastEvent slot" is a safe per-call hand-off.
//
// On overflow=true Encode returns (nil, true) WITHOUT capturing the Event
// (a real overflow path drops the frame before the wire ever sees it; the
// router still calls Drop("tx_too_large") which the test asserts).
type stubEncoder struct {
	overflow bool
	mu       sync.Mutex
	last     Event
}

func (e *stubEncoder) Encode(ev Event) ([]byte, bool) {
	if e.overflow {
		return nil, true
	}
	e.mu.Lock()
	e.last = ev
	e.mu.Unlock()
	pks := make([]string, 0, len(ev.MatchedIndices))
	for _, i := range ev.MatchedIndices {
		if i >= 0 && i < len(ev.Tx.Changes) {
			pks = append(pks, ev.Tx.Changes[i].PK)
		}
	}
	body, _ := json.Marshal(map[string]any{
		"tx_id":      ev.Tx.ID,
		"commit_lsn": ev.Tx.CommitLSN.String(),
		"pks":        pks,
	})
	return body, false
}

// lastEvent returns the most recently encoded Event. Caller is responsible
// for ordering — read it immediately after the recorder appends a frame.
func (e *stubEncoder) lastEvent() Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last
}

// recordedSub bundles a Subscriber with a recorder slice + mutex. Tests
// that previously drained from sub.Channel() drain from rec.frames instead.
// The recorder also tracks the per-sub Event sequence the encoder fed into
// this sub's sendFunc — populated via the shared lastEvent cursor on
// stubEncoder. Call rec.useEncoder(enc) BEFORE Register so the sendFunc
// closure sees the binding.
type recordedSub struct {
	sub    *Subscriber
	mu     sync.Mutex
	frames [][]byte
	events []Event
	limit  int          // 0 = unlimited; >0 means the (limit+1)th frame returns false (BP-01 sim)
	enc    *stubEncoder // set via useEncoder; nil → events[] not populated
}

// useEncoder binds the stubEncoder so the next frames recorded also capture
// the Event the encoder just saw. Tests that need to inspect MatchedIndices
// / Tx.Changes call this after mkBroadcaster + before Register.
func (r *recordedSub) useEncoder(enc *stubEncoder) {
	r.mu.Lock()
	r.enc = enc
	r.mu.Unlock()
}

// --- fixture helpers ---

// mkChange builds a wal.Change for tests. INSERT carries Data, UPDATE carries
// Changed, DELETE carries neither (per wal.types invariants).
func mkChange(op wal.Op, schema, table, pk string) wal.Change {
	c := wal.Change{
		Schema: schema,
		Table:  table,
		Op:     op,
		PK:     pk,
		PKCol:  "id",
	}
	switch op {
	case wal.OpInsert:
		c.Data = map[string]any{"id": pk}
	case wal.OpUpdate:
		c.Changed = map[string]any{"id": pk}
	}
	return c
}

// mkTx builds a wal.Tx with the supplied changes and a fixed commit time.
func mkTx(commitLSN pglogrepl.LSN, txID uint32, changes ...wal.Change) wal.Tx {
	return wal.Tx{
		ID:        txID,
		CommitLSN: commitLSN,
		CommitTS:  time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
		Changes:   changes,
	}
}

// subRecorders is a process-global map from *Subscriber → recorded frames.
// Tests retrieve their per-sub recorder via recordedFor(sub). The map is
// guarded by recordersMu; entries are written exactly once at construction
// time (so reads outside the mutex are race-clean as long as the test does
// not also share the *Subscriber across goroutines without synchronisation).
var (
	recordersMu sync.Mutex
	recorders   = map[*Subscriber]*recordedSub{}
)

// mkExactSub constructs an exact-kind Subscriber with the requested
// (informational) buffer cap and start LSN, wires a recording sendFunc, and
// registers the recorder so tests can drain via recordedFor(sub). When
// bufCap > 0 it is interpreted as the recorder's BP-01 limit (the
// (bufCap+1)th send returns false → router slow_consumer path); bufCap == 0
// means unlimited. Parent context is background — tests cancel via Drop
// when asserting disconnect.
func mkExactSub(schema, table, pk string, startLSN pglogrepl.LSN, bufCap int) *Subscriber {
	rec := newRecordedExactSubWithPK(schema, table, pk, startLSN, bufCap)
	return rec.sub
}

// mkWildcardSub constructs a wildcard-kind Subscriber with a recording
// sendFunc wired in. Semantics for bufCap match mkExactSub.
func mkWildcardSub(schema, table string, startLSN pglogrepl.LSN, bufCap int) *Subscriber {
	rec := newRecordedWildcardSubWithCap(schema, table, startLSN, bufCap)
	return rec.sub
}

// newRecordedExactSubWithPK is the actual constructor used by mkExactSub —
// it builds the Subscriber and the recorder together so the recorder is
// available immediately.
func newRecordedExactSubWithPK(schema, table, pk string, startLSN pglogrepl.LSN, limit int) *recordedSub {
	s := NewSubscriber(
		SubscriberConfig{
			Kind:     KindExact,
			Schema:   schema,
			Table:    table,
			PK:       pk,
			StartLSN: startLSN,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	rec := &recordedSub{sub: s, limit: limit}
	s.WireSendFunc(recordingSendFunc(rec))
	recordersMu.Lock()
	recorders[s] = rec
	recordersMu.Unlock()
	return rec
}

// newRecordedWildcardSubWithCap is the wildcard analogue.
func newRecordedWildcardSubWithCap(schema, table string, startLSN pglogrepl.LSN, limit int) *recordedSub {
	s := NewSubscriber(
		SubscriberConfig{
			Kind:     KindWildcard,
			Schema:   schema,
			Table:    table,
			StartLSN: startLSN,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	rec := &recordedSub{sub: s, limit: limit}
	s.WireSendFunc(recordingSendFunc(rec))
	recordersMu.Lock()
	recorders[s] = rec
	recordersMu.Unlock()
	return rec
}

// recordingSendFunc builds the closure wired into the Subscriber via
// WireSendFunc. The closure appends each frame onto rec.frames and (when
// rec.enc is set via useEncoder) also captures the encoder's lastEvent so
// drainOne / inspectEvent helpers can recover the per-sub Event.
func recordingSendFunc(rec *recordedSub) func(frame []byte) bool {
	return func(frame []byte) bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if rec.limit > 0 && len(rec.frames) >= rec.limit {
			return false
		}
		fc := make([]byte, len(frame))
		copy(fc, frame)
		rec.frames = append(rec.frames, fc)
		if rec.enc != nil {
			rec.events = append(rec.events, rec.enc.lastEvent())
		}
		return true
	}
}

// recordedFor returns the recorder previously registered for sub by
// mkExactSub / mkWildcardSub. Test-helper; fails the test if no recorder
// exists (e.g., the sub was constructed via NewSubscriber directly).
func recordedFor(t *testing.T, sub *Subscriber) *recordedSub {
	t.Helper()
	recordersMu.Lock()
	rec, ok := recorders[sub]
	recordersMu.Unlock()
	if !ok {
		t.Fatalf("recordedFor: no recorder for subscriber %s — was the sub built via NewSubscriber directly?", sub.ID())
	}
	return rec
}

// mkBroadcaster builds a Broadcaster with the supplied unified per-tx cap,
// default buffer/heartbeat values, and a non-overflowing stub encoder.
func mkBroadcaster(maxChanges int) (*Broadcaster, *metrics.Registry) {
	m := metrics.New()
	b := New(Config{
		ExactBuffer:       64,
		WildcardBuffer:    512,
		MaxChangesPerTx:   maxChanges,
		HeartbeatInterval: 15 * time.Second,
	}, Deps{
		Logger:  zerolog.Nop(),
		Metrics: m,
		Encoder: &stubEncoder{},
	})
	return b, m
}

// gatherCounter returns the counter value at <name>{labelKey=labelVal}, or 0
// if the family or matching series is not present.
func gatherCounter(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// gatherGauge returns the gauge value at <name>{labelKey=labelVal}, or 0 if
// the family or matching series is not present.
func gatherGauge(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// gatherHasSeries reports whether <name>{labelKey=labelVal} exists in the
// gathered output (regardless of its value). Used by the multi-root
// sentinel test to confirm pre-registration at zero.
func gatherHasSeries(t *testing.T, reg *metrics.Registry, name, labelKey, labelVal string) bool {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabel(m, labelKey, labelVal) {
				return true
			}
		}
	}
	return false
}

// matchLabel reports whether the given metric carries a label pair with the
// requested key and value.
func matchLabel(m *dto.Metric, key, val string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key && lp.GetValue() == val {
			return true
		}
	}
	return false
}

// drainOne waits up to timeout for the next frame the router delivers to
// sub.send, and returns the matching captured Event. The caller MUST have
// bound the broadcaster's encoder via recordedFor(t, sub).useEncoder(...)
// before Register so the recorder records both frame and Event in lock-step.
//
// If no event-capture binding was made, this still waits for a frame and
// returns a synthetic Event reconstructed from the frame's JSON payload
// (commit_lsn + matched-index count derived from pks). Tests that assert
// Tx.Changes shape or backing-array identity MUST bind useEncoder.
func drainOne(t *testing.T, sub *Subscriber, timeout time.Duration) Event {
	t.Helper()
	rec := recordedFor(t, sub)
	deadline := time.Now().Add(timeout)
	for {
		rec.mu.Lock()
		framesLen := len(rec.frames)
		eventsLen := len(rec.events)
		rec.mu.Unlock()
		// Drain in FIFO order — pop the head of frames + events.
		if framesLen > 0 {
			rec.mu.Lock()
			var ev Event
			if eventsLen > 0 {
				ev = rec.events[0]
				rec.events = rec.events[1:]
			}
			frame := rec.frames[0]
			rec.frames = rec.frames[1:]
			rec.mu.Unlock()
			if eventsLen > 0 {
				return ev
			}
			// No encoder-binding: synthesize a minimal Event from the
			// frame's JSON envelope so legacy assertions on CommitLSN /
			// len(MatchedIndices) still pass.
			return synthesizeEvent(t, frame)
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscriber %s: no event within %v", sub.ID(), timeout)
			return Event{}
		}
		time.Sleep(time.Millisecond)
	}
}

// synthesizeEvent decodes the stubEncoder JSON envelope back into an Event
// with as much shape as the encoder preserved (tx_id, commit_lsn, pks-derived
// MatchedIndices). Tests that need Tx.Changes / backing-array identity must
// bind a real encoder capture; this fallback handles only the simpler
// "delivered LSN" assertions.
func synthesizeEvent(t *testing.T, frame []byte) Event {
	t.Helper()
	var m struct {
		TxID      uint32   `json:"tx_id"`
		CommitLSN string   `json:"commit_lsn"`
		PKs       []string `json:"pks"`
	}
	if err := json.Unmarshal(frame, &m); err != nil {
		t.Fatalf("synthesizeEvent: unmarshal: %v (raw=%q)", err, frame)
	}
	lsn, err := pglogrepl.ParseLSN(m.CommitLSN)
	if err != nil {
		t.Fatalf("synthesizeEvent: ParseLSN(%q): %v", m.CommitLSN, err)
	}
	indices := make([]int, len(m.PKs))
	for i := range m.PKs {
		indices[i] = i
	}
	return Event{
		Tx:             wal.Tx{ID: m.TxID, CommitLSN: lsn},
		MatchedIndices: indices,
	}
}

// expectNoFrame asserts that no frame arrives on sub within timeout. The
// channel-based analogue used to do `select { case <-sub.Channel(): t.Error
// ... case <-time.After(timeout): }` — here we just poll the recorder.
func expectNoFrame(t *testing.T, sub *Subscriber, timeout time.Duration) {
	t.Helper()
	rec := recordedFor(t, sub)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		got := len(rec.frames)
		rec.mu.Unlock()
		if got > 0 {
			t.Errorf("subscriber %s: unexpected frame delivered (%d total)", sub.ID(), got)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// runIngest spawns Ingest in a goroutine and returns a done channel that
// closes when Ingest returns, plus the captured return error (after done
// closes). Raw `go` is permitted in tests per 02-PATTERNS §"Test layout".
func runIngest(b *Broadcaster, ctx context.Context, txCh <-chan wal.Tx) (<-chan struct{}, *error) {
	done := make(chan struct{})
	var retErr error
	go func() {
		retErr = b.Ingest(ctx, txCh)
		close(done)
	}()
	return done, &retErr
}

// sendTx pushes a tx onto a buffered channel with a 100ms timeout.
func sendTx(t *testing.T, txCh chan<- wal.Tx, tx wal.Tx) {
	t.Helper()
	select {
	case txCh <- tx:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout pushing tx %d onto txCh", tx.ID)
	}
}

// --- tests ---

// TestBroadcaster_ExactFanOut_SingleMatch verifies the happy path: one exact
// subscriber receives exactly one Event for a tx with one matching change.
func TestBroadcaster_ExactFanOut_SingleMatch(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42"))
	sendTx(t, txCh, tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.MatchedIndices), 1; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}
	if len(ev.MatchedIndices) > 0 && ev.MatchedIndices[0] != 0 {
		t.Errorf("MatchedIndices[0]: got %d; want 0", ev.MatchedIndices[0])
	}
	if ev.Tx.CommitLSN != tx.CommitLSN {
		t.Errorf("Tx.CommitLSN: got %s; want %s", ev.Tx.CommitLSN, tx.CommitLSN)
	}

	// No drops should have occurred.
	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}

	cancel()
	<-done
}

// TestBroadcaster_ExactFanOut_MultipleSubscribersSameRow verifies exact
// subscriptions support more than one client on the same schema.table:pk and
// deregister by subscriber pointer rather than deleting the whole key.
func TestBroadcaster_ExactFanOut_MultipleSubscribersSameRow(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	a := mkExactSub("public", "users", "42", 0, 4)
	c := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, a).useEncoder(b.enc.(*stubEncoder))
	recordedFor(t, c).useEncoder(b.enc.(*stubEncoder))
	b.Register(a)
	b.Register(c)

	if got, want := b.ExactLen(), 2; got != want {
		t.Fatalf("ExactLen after two exact subscribers: got %d; want %d", got, want)
	}
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 2 {
		t.Fatalf("subscribers_active{type=exact} after two Registers: got %v; want 2", v)
	}

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x110), 11, mkChange(wal.OpInsert, "public", "users", "42")))

	for _, sub := range []*Subscriber{a, c} {
		ev := drainOne(t, sub, 200*time.Millisecond)
		if got, want := ev.MatchedIndices, []int{0}; !equalInts(got, want) {
			t.Errorf("subscriber %s MatchedIndices: got %v; want %v", sub.ID(), got, want)
		}
	}

	b.Deregister(a)
	if got, want := b.ExactLen(), 1; got != want {
		t.Fatalf("ExactLen after deregistering one exact subscriber: got %d; want %d", got, want)
	}
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 1 {
		t.Fatalf("subscribers_active{type=exact} after one Deregister: got %v; want 1", v)
	}

	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x120), 12, mkChange(wal.OpUpdate, "public", "users", "42")))

	expectNoFrame(t, a, 50*time.Millisecond)
	ev := drainOne(t, c, 200*time.Millisecond)
	if got, want := ev.MatchedIndices, []int{0}; !equalInts(got, want) {
		t.Errorf("remaining subscriber MatchedIndices: got %v; want %v", got, want)
	}

	b.Deregister(c)
	if got, want := b.ExactLen(), 0; got != want {
		t.Errorf("ExactLen after deregistering both exact subscribers: got %d; want %d", got, want)
	}
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 0 {
		t.Errorf("subscribers_active{type=exact} after both Deregisters: got %v; want 0", v)
	}

	cancel()
	<-done
}

// TestBroadcaster_WildcardFanOut_MultiChange confirms that a wildcard
// subscriber receives ONE Event aggregating ALL matching changes in a tx
// (transactional atomicity preserved via per-subscriber accumulation).
func TestBroadcaster_WildcardFanOut_MultiChange(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	sub := mkWildcardSub("public", "users", 0, 8)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x200), 2,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpInsert, "public", "users", "2"),
		mkChange(wal.OpInsert, "public", "users", "3"),
		mkChange(wal.OpInsert, "public", "orders", "999"), // non-matching
	)
	sendTx(t, txCh, tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	want := []int{0, 1, 2}
	if got := ev.MatchedIndices; !equalInts(got, want) {
		t.Errorf("MatchedIndices: got %v; want %v", got, want)
	}

	// Exactly one Event — recorder should hold no further frames.
	expectNoFrame(t, sub, 50*time.Millisecond)

	cancel()
	<-done
}

// TestBroadcaster_ExactAndWildcardSameTx_SingleEventPerSub confirms per-
// subscriber accumulation when both exact and wildcard subs match the same
// table: each subscriber receives exactly one Event, with the indices it
// actually matched.
func TestBroadcaster_ExactAndWildcardSameTx_SingleEventPerSub(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	exact := mkExactSub("public", "users", "42", 0, 8)
	wild := mkWildcardSub("public", "users", 0, 8)
	recordedFor(t, exact).useEncoder(b.enc.(*stubEncoder))
	recordedFor(t, wild).useEncoder(b.enc.(*stubEncoder))
	b.Register(exact)
	b.Register(wild)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x300), 3,
		mkChange(wal.OpInsert, "public", "users", "42"),
		mkChange(wal.OpInsert, "public", "users", "99"),
		mkChange(wal.OpInsert, "public", "users", "100"),
	)
	sendTx(t, txCh, tx)

	exactEv := drainOne(t, exact, 200*time.Millisecond)
	if got, want := exactEv.MatchedIndices, []int{0}; !equalInts(got, want) {
		t.Errorf("exact MatchedIndices: got %v; want %v", got, want)
	}

	wildEv := drainOne(t, wild, 200*time.Millisecond)
	if got, want := wildEv.MatchedIndices, []int{0, 1, 2}; !equalInts(got, want) {
		t.Errorf("wildcard MatchedIndices: got %v; want %v", got, want)
	}

	// No second Events on either sub.
	expectNoFrame(t, exact, 50*time.Millisecond)
	expectNoFrame(t, wild, 50*time.Millisecond)

	cancel()
	<-done
}

// TestBroadcaster_StartLSN_FilterBelow asserts the start_lsn filter:
// tx.CommitLSN <= sub.StartLSN is silently skipped (no metric, no log);
// tx.CommitLSN > sub.StartLSN proceeds.
func TestBroadcaster_StartLSN_FilterBelow(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", pglogrepl.LSN(0x100), 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	txCh := make(chan wal.Tx, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Equal LSN — must be filtered.
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42")))

	// Greater LSN — must be delivered.
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x101), 2, mkChange(wal.OpInsert, "public", "users", "42")))

	ev := drainOne(t, sub, 200*time.Millisecond)
	if ev.Tx.CommitLSN != pglogrepl.LSN(0x101) {
		t.Errorf("delivered tx CommitLSN: got %s; want 0/101", ev.Tx.CommitLSN)
	}

	// No more events.
	expectNoFrame(t, sub, 50*time.Millisecond)

	// The filtered tx must NOT have incremented any drop counter.
	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}

	cancel()
	<-done
}

// TestBroadcaster_TxTooLarge_ExactCap verifies that an exact subscriber
// matching more than MaxChangesPerTx changes in one tx is dropped via
// tx_too_large (unified cap applies to exact and wildcard alike).
func TestBroadcaster_TxTooLarge_ExactCap(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(3)
	sub := mkExactSub("public", "users", "42", 0, 16)
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// 4 updates to the same PK in one tx — exceeds cap of 3.
	tx := mkTx(pglogrepl.LSN(0x400), 4,
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
		mkChange(wal.OpUpdate, "public", "users", "42"),
	)
	sendTx(t, txCh, tx)

	// Must be dropped — wait for Done.
	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber not dropped within 500ms")
	}
	if got, want := sub.Reason(), "tx_too_large"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}

	// No Event delivered.
	expectNoFrame(t, sub, 50*time.Millisecond)

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 1 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 1", v)
	}

	cancel()
	<-done
}

// TestBroadcaster_TxTooLarge_WildcardCap verifies the analogous drop for a
// wildcard subscriber against MaxChangesPerTx (unified cap).
func TestBroadcaster_TxTooLarge_WildcardCap(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(2)
	sub := mkWildcardSub("public", "users", 0, 16)
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	tx := mkTx(pglogrepl.LSN(0x500), 5,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpInsert, "public", "users", "2"),
		mkChange(wal.OpInsert, "public", "users", "3"),
	)
	sendTx(t, txCh, tx)

	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wildcard subscriber not dropped within 500ms")
	}
	if got, want := sub.Reason(), "tx_too_large"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}

	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 1 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 1", v)
	}

	cancel()
	<-done
}

// TestBroadcaster_TxTooLarge_ExactRelaxed_GEN03 asserts the GEN-03 relaxation:
// an exact subscriber matching between 1001 and 10000 changes in one tx is
// DELIVERED (was previously dropped at the old 1000 exact cap). The unified
// MaxChangesPerTx=10000 governs both exact and wildcard subscribers.
//
// This test would have FAILED under the old split-cap design (exact=1000, wildcard=10000).
func TestBroadcaster_TxTooLarge_ExactRelaxed_GEN03(t *testing.T) {
	t.Parallel()

	// Unified cap of 10000 — an exact sub matching 1500 changes must be delivered.
	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 0) // unlimited recorder
	b.Register(sub)

	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Build a tx with 1500 changes to the same exact PK — between the old 1000
	// cap and the new 10000 unified cap.
	const nChanges = 1500
	changes := make([]wal.Change, nChanges)
	for i := range changes {
		changes[i] = mkChange(wal.OpUpdate, "public", "users", "42")
	}
	tx := mkTx(pglogrepl.LSN(0x600), 6, changes...)
	sendTx(t, txCh, tx)

	// Event must be delivered — the exact sub should NOT be dropped.
	ev := drainOne(t, sub, 500*time.Millisecond)
	if got, want := len(ev.MatchedIndices), nChanges; got != want {
		t.Errorf("MatchedIndices length: got %d; want %d", got, want)
	}

	// No drops should have occurred.
	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0 (GEN-03 relaxation)", reason, v)
		}
	}

	cancel()
	<-done
}

// TestBroadcaster_SlowConsumerDisconnect is the slow-consumer acceptance
// test: a subscriber whose buffer fills must be dropped with reason
// "slow_consumer" and its Done channel must close. Runs under -race
// (CI default).
func TestBroadcaster_SlowConsumerDisconnect(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 2) // tiny buffer
	b.Register(sub)

	txCh := make(chan wal.Tx, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Three back-to-back txs targeting the subscriber. The recorder's limit
	// is 2 (mkExactSub's bufCap), so the 3rd sub.send call returns false
	// and the router triggers Drop("slow_consumer") via the BP-01 path.
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x200), 2, mkChange(wal.OpInsert, "public", "users", "42")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x300), 3, mkChange(wal.OpInsert, "public", "users", "42")))

	select {
	case <-sub.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber not dropped within 500ms (slow-consumer regression)")
	}
	if got, want := sub.Reason(), "slow_consumer"; got != want {
		t.Errorf("Reason: got %q; want %q", got, want)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "slow_consumer"); v < 1 {
		t.Errorf("tx_dropped_total{reason=slow_consumer}: got %v; want >= 1", v)
	}

	cancel()
	<-done
}

// TestBroadcaster_Ingest_ExitsOnCtxCancel asserts that Ingest returns with
// ctx.Err() promptly when the parent context is cancelled.
func TestBroadcaster_Ingest_ExitsOnCtxCancel(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())

	done, errPtr := runIngest(b, ctx, txCh)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Ingest did not exit within 200ms of ctx cancel")
	}
	if *errPtr != context.Canceled {
		t.Errorf("Ingest return error: got %v; want context.Canceled", *errPtr)
	}
}

// TestBroadcaster_Ingest_ExitsOnTxChClose asserts that Ingest returns nil
// promptly when the txCh is closed by the producer.
func TestBroadcaster_Ingest_ExitsOnTxChClose(t *testing.T) {
	t.Parallel()

	b, _ := mkBroadcaster(10000)
	txCh := make(chan wal.Tx, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done, errPtr := runIngest(b, ctx, txCh)
	close(txCh)

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Ingest did not exit within 200ms of txCh close")
	}
	if *errPtr != nil {
		t.Errorf("Ingest return error: got %v; want nil", *errPtr)
	}
}

// TestBroadcaster_DeregisterDecrementsGauge confirms that Deregister
// decrements walera_subscribers_active for the appropriate kind.
func TestBroadcaster_DeregisterDecrementsGauge(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)

	b.Register(sub)
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 1 {
		t.Errorf("after Register: subscribers_active{type=exact}: got %v; want 1", v)
	}

	b.Deregister(sub)
	if v := gatherGauge(t, reg, "walera_subscribers_active", "type", "exact"); v != 0 {
		t.Errorf("after Deregister: subscribers_active{type=exact}: got %v; want 0", v)
	}
}

// TestBroadcaster_MultiRootCounter_RegisteredAtZero confirms the sentinel:
// the multi_root drop counter series exists in Gather() output immediately
// after New() (value 0). The code path is unreachable by construction; this
// test exists so future regressions don't silently lose the series before
// any future increment site lands.
func TestBroadcaster_MultiRootCounter_RegisteredAtZero(t *testing.T) {
	t.Parallel()

	_, reg := mkBroadcaster(10000)

	if !gatherHasSeries(t, reg, "walera_tx_dropped_total", "reason", "multi_root") {
		t.Fatal("walera_tx_dropped_total{reason=multi_root} series missing from Gather() — sentinel regressed")
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 0 {
		t.Errorf("multi_root counter initial value: got %v; want 0", v)
	}
	// The slow_consumer and tx_too_large series should also be present at
	// zero (router.New pre-touches all three reasons).
	for _, reason := range []string{"slow_consumer", "tx_too_large"} {
		if !gatherHasSeries(t, reg, "walera_tx_dropped_total", "reason", reason) {
			t.Errorf("walera_tx_dropped_total{reason=%s} series missing from Gather()", reason)
		}
	}
}

// --- Filter dispatch in routeTx ---

// TestRouterSubscriberFilterFieldDefaultNil locks the regression
// guarantee: NewSubscriber returns a Subscriber whose Filter field is nil.
// Callers that do not need authorization filtering are unaffected.
func TestRouterSubscriberFilterFieldDefaultNil(t *testing.T) {
	t.Parallel()
	sub := NewSubscriber(
		SubscriberConfig{
			Kind:      KindExact,
			Schema:    "public",
			Table:     "users",
			PK:        "42",
			BufferCap: 4,
		},
		SubscriberDeps{Parent: context.Background()},
	)
	if sub.Filter != nil {
		t.Fatalf("Filter at construction: got non-nil; want nil")
	}
}

// TestRouteTxFilterAllDroppedSilently verifies silent-drop semantics:
// a subscriber whose Filter returns drop=true for every change receives NO
// event and triggers NO drop-metric increment.
func TestRouteTxFilterAllDroppedSilently(t *testing.T) {
	t.Parallel()
	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) { return c, true }
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x100), 1,
		mkChange(wal.OpInsert, "public", "users", "42"),
	)
	b.routeTx(tx)

	// No event should arrive.
	expectNoFrame(t, sub, 100*time.Millisecond)

	// Subscriber should NOT have been dropped (silent skip semantics).
	if sub.Reason() != "" {
		t.Errorf("Reason: got %q; want empty (silent drop must not call sub.Drop)", sub.Reason())
	}

	// No drop metric must have moved.
	for _, reason := range []string{"slow_consumer", "tx_too_large", "multi_root"} {
		if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", reason); v != 0 {
			t.Errorf("tx_dropped_total{reason=%s}: got %v; want 0", reason, v)
		}
	}
}

// TestRouteTxFilterPartialKept verifies that a subscriber whose Filter keeps
// a subset of matched changes receives a single Event whose Tx.Changes
// contains only the kept (possibly modified) changes and whose MatchedIndices
// references the new slice positions starting at 0.
func TestRouteTxFilterPartialKept(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkWildcardSub("public", "users", 0, 8)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	// Redact: keep only the PK column in Data and never drop.
	sub.Filter = func(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) {
		c2 := c
		if c.Data != nil {
			c2.Data = map[string]any{"id": c.Data["id"]}
		}
		return c2, false
	}
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x200), 2,
		mkChange(wal.OpInsert, "public", "users", "1"),
		mkChange(wal.OpInsert, "public", "users", "2"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got, want := len(ev.Tx.Changes), 2; got != want {
		t.Errorf("ev.Tx.Changes length: got %d; want %d", got, want)
	}
	if got, want := len(ev.MatchedIndices), 2; got != want {
		t.Errorf("ev.MatchedIndices length: got %d; want %d", got, want)
	}
	if !equalInts(ev.MatchedIndices, []int{0, 1}) {
		t.Errorf("ev.MatchedIndices: got %v; want [0 1] (clone uses new indices)", ev.MatchedIndices)
	}
	// Cloned slice must NOT share backing array with original tx.Changes
	// (proves a per-subscriber clone was made).
	if reflect.ValueOf(ev.Tx.Changes).Pointer() == reflect.ValueOf(tx.Changes).Pointer() {
		t.Error("ev.Tx.Changes shares backing array with input tx.Changes — Filter path failed to clone")
	}
}

// TestRouteTxPassesCommitLSNToFilter verifies that routeTx forwards
// tx.CommitLSN into the second argument of the Filter callback —
// required for FilterWithLSN to consult PrevWhitelist when a tx committed
// before the latest refresh.
func TestRouteTxPassesCommitLSNToFilter(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)

	var captured pglogrepl.LSN
	sub.Filter = func(c wal.Change, txCommitLSN pglogrepl.LSN) (wal.Change, bool) {
		captured = txCommitLSN
		return c, false
	}
	b.Register(sub)

	want := pglogrepl.LSN(0x4A2)
	tx := mkTx(want, 1, mkChange(wal.OpInsert, "public", "users", "42"))
	b.routeTx(tx)

	// Drain to flush the event so the filter has definitely run.
	_ = drainOne(t, sub, 200*time.Millisecond)

	if captured != want {
		t.Errorf("Filter received txCommitLSN = %s; want %s", captured, want)
	}
}

// TestRouteTxNilFilterUnchangedFastPath verifies that the Phase-2 fast path
// (Filter==nil) preserves slice identity: ev.Tx.Changes shares its backing
// array with the input tx.Changes — zero extra allocation.
func TestRouteTxNilFilterUnchangedFastPath(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	// Do NOT assign sub.Filter — leave nil for the fast path.
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x300), 3,
		mkChange(wal.OpInsert, "public", "users", "42"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if reflect.ValueOf(ev.Tx.Changes).Pointer() != reflect.ValueOf(tx.Changes).Pointer() {
		t.Error("ev.Tx.Changes does NOT share backing array with input tx.Changes — fast path regressed")
	}
}

// gatherHistogramCount returns the SampleCount of the histogram named `name`,
// or 0 if the family is absent. Used by fan-out and lifetime observation
// tests to assert a producer site exists.
func gatherHistogramCount(t *testing.T, reg *metrics.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range fam.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
		return total
	}
	return 0
}

// TestBroadcaster_RoutingFanOut_ObservedPerTx asserts that every routed tx
// records exactly one observation on walera_routing_fan_out, with sample
// value == len(matched). Three txs through the same broadcaster must
// produce SampleCount == 3 — the histogram is non-zero after a
// representative operation.
func TestBroadcaster_RoutingFanOut_ObservedPerTx(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	b.Register(sub)

	txCh := make(chan wal.Tx, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, _ := runIngest(b, ctx, txCh)

	// Three txs — two match the exact subscriber, one does not. Every tx
	// gets one Observe regardless of whether matched is empty.
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x100), 1, mkChange(wal.OpInsert, "public", "users", "42")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x101), 2, mkChange(wal.OpInsert, "public", "orders", "99")))
	sendTx(t, txCh, mkTx(pglogrepl.LSN(0x102), 3, mkChange(wal.OpInsert, "public", "users", "42")))

	// Drain matching events to ensure routeTx has run for both matching txs.
	_ = drainOne(t, sub, 200*time.Millisecond)
	_ = drainOne(t, sub, 200*time.Millisecond)

	// Give the non-matching tx a moment to complete routeTx (no event drains it).
	time.Sleep(20 * time.Millisecond)

	if got := gatherHistogramCount(t, reg, "walera_routing_fan_out"); got != 3 {
		t.Errorf("walera_routing_fan_out SampleCount = %d; want 3", got)
	}

	cancel()
	<-done
}

// --- shared helpers ---

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
