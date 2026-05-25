//go:build integration

// Package integration — scenario 05: start_lsn cutoff (ROUTE-05 / spec §3.3).
//
// A subscriber's handshake captures `start_lsn = wal.CurrentLSN()` at the
// moment Register() runs. Subsequent tx events whose commit_lsn ≤ start_lsn
// MUST be skipped — they happened "in the past" relative to the subscriber.
//
// Test shape:
//  1. INSERT row #1 — its commit_lsn becomes "the past".
//  2. Sleep a short window so the WAL reader observes & advances past tx#1.
//  3. Subscribe to users/all — handshake captures start_lsn > tx#1's lsn.
//  4. INSERT row #2 — its commit_lsn > start_lsn.
//  5. Drain events for 5s — assert exactly ONE event arrives with pk="2".
//
// The wildcard channel `users/all` lets us prove the cutoff is by LSN, not by
// PK routing: an exact subscriber to users:1 would also miss tx#2 (different
// PK), so we'd need wildcard delivery to surface the cutoff itself.
package integration

import (
	"context"
	"testing"
	"time"
)

func Test05StartLSNCutoff(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Step 1: pre-subscribe INSERT. This tx's commit_lsn precedes the
	// subscriber's start_lsn (captured at Register time).
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "pre@example.com", "Pre",
	); err != nil {
		t.Fatalf("pre-subscribe insert: %v", err)
	}

	// Step 2: allow the WAL reader to decode tx#1 and advance its
	// last-committed LSN cursor. 500ms is generous on a developer laptop:
	// the WAL reader's decode loop is microsecond-scale and the bookkeeping
	// is `r.lastCommittedLSN.Store(commitLSN)` (atomic, no allocation).
	time.Sleep(500 * time.Millisecond)

	// Step 3: subscribe AFTER tx#1 is fully observed by the reader. The
	// handshake's Register() call captures start_lsn = wal.CurrentLSN(), so
	// start_lsn > tx#1's commit_lsn.
	events, errCh, closeFn := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn()

	// Step 4: post-subscribe INSERT — must be delivered.
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		2, "post@example.com", "Post",
	); err != nil {
		t.Fatalf("post-subscribe insert: %v", err)
	}

	// Step 5: drain events for 5s. Exactly ONE tx event with pk="2" must
	// arrive. Receiving a second tx event with pk="1" would prove the
	// start_lsn cutoff was bypassed; receiving none would prove the post tx
	// did not route at all (different failure mode).
	deadline := time.Now().Add(5 * time.Second)
	var seenPKs []string
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("events channel closed prematurely")
			}
			if ev.Type != "tx" {
				continue // heartbeat / other — skip
			}
			p := decodeTxPayload(t, ev.Data)
			if len(p.Changes) != 1 {
				t.Fatalf("expected 1 change per tx, got %d (raw=%s)", len(p.Changes), string(ev.Data))
			}
			seenPKs = append(seenPKs, p.Changes[0].PK)
		case err := <-errCh:
			t.Fatalf("client error: %v", err)
		case <-time.After(250 * time.Millisecond):
			// Tick — keep polling until deadline.
		case <-ctx.Done():
			t.Fatalf("ctx done; stderr:\n%s", h.Binary.Stderr())
		}
	}

	if len(seenPKs) == 0 {
		t.Fatalf("no tx events observed within 5s — post-subscribe INSERT did not route; stderr:\n%s", h.Binary.Stderr())
	}
	if len(seenPKs) != 1 {
		t.Fatalf("expected 1 tx event after cutoff, got %d (pks=%v) — pre-subscribe tx leaked through", len(seenPKs), seenPKs)
	}
	if seenPKs[0] != "2" {
		t.Fatalf("expected pk=2 (post-subscribe), got pk=%q — pre-subscribe tx was delivered (start_lsn cutoff broken)", seenPKs[0])
	}
}
