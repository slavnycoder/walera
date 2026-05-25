//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func Test09DDLAdaptation(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name", "phone"}},
	)
	if err := h.Auth.SetTTL("test-token", 60); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events, errCh, closeFn := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn()

	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "a@b.c", "Alice",
	); err != nil {
		t.Fatalf("pre-ALTER insert: %v", err)
	}
	preEv := readTxEvent(ctx, t, h, events, errCh)
	if len(preEv.Changes) != 1 {
		t.Fatalf("pre-ALTER: expected 1 change, got %d", len(preEv.Changes))
	}
	if _, present := preEv.Changes[0].Data["phone"]; present {
		t.Errorf("pre-ALTER event unexpectedly has data.phone (column did not exist yet): %v", preEv.Changes[0].Data["phone"])
	}

	if err := h.PG.Exec(ctx, "ALTER TABLE users ADD COLUMN phone text"); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN: %v", err)
	}

	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name, phone) VALUES ($1, $2, $3, $4)",
		2, "c@d.e", "Carol", "+12025550100",
	); err != nil {
		t.Fatalf("post-ALTER insert: %v", err)
	}
	postEv := readTxEvent(ctx, t, h, events, errCh)
	if len(postEv.Changes) != 1 {
		t.Fatalf("post-ALTER: expected 1 change, got %d (raw=%v)", len(postEv.Changes), postEv.Changes)
	}
	c0 := postEv.Changes[0]
	if c0.PK != "2" {
		t.Errorf("post-ALTER pk = %q, want %q", c0.PK, "2")
	}
	got, present := c0.Data["phone"]
	if !present {
		t.Fatalf("post-ALTER event missing data.phone (RelationCache did NOT adapt to ADD COLUMN); data=%v; stderr:\n%s", c0.Data, h.Binary.Stderr())
	}
	if got != "+12025550100" {
		t.Errorf("post-ALTER data.phone = %v, want %q", got, "+12025550100")
	}
}
