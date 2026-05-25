//go:build integration

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

	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "pre@example.com", "Pre",
	); err != nil {
		t.Fatalf("pre-subscribe insert: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	events, errCh, closeFn := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn()

	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		2, "post@example.com", "Post",
	); err != nil {
		t.Fatalf("post-subscribe insert: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var seenPKs []string
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("events channel closed prematurely")
			}
			if ev.Type != "tx" {
				continue
			}
			p := decodeTxPayload(t, ev.Data)
			if len(p.Changes) != 1 {
				t.Fatalf("expected 1 change per tx, got %d (raw=%s)", len(p.Changes), string(ev.Data))
			}
			seenPKs = append(seenPKs, p.Changes[0].PK)
		case err := <-errCh:
			t.Fatalf("client error: %v", err)
		case <-time.After(250 * time.Millisecond):

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
