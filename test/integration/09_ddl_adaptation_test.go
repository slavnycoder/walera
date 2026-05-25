//go:build integration

// Package integration — scenario 09: passive DDL adaptation.
//
// pgoutput emits a fresh Relation message whenever a relation's tuple shape
// changes (e.g., ADD COLUMN). The first DML on the altered table after the
// ALTER carries a Relation message preceding it; Walera's RelationCache
// rebuilds the cache entry from this Relation, so the new column is
// automatically included in the change payload.
//
// Test shape:
//  1. Subscribe to users/all (wildcard; whitelist includes the
//     to-be-added column).
//  2. INSERT a row with (id, email) only. Assert event arrives with
//     data containing id + email (no phone yet).
//  3. ALTER TABLE users ADD COLUMN phone text.
//  4. INSERT a row with (id, email, phone). Assert event arrives with
//     data.phone populated — proves RelationCache adapted passively.
package integration

import (
	"context"
	"testing"
	"time"
)

func Test09DDLAdaptation(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	// Permissive whitelist — includes the to-be-added `phone` column.
	// Field-level filtering would otherwise mask the new column even if
	// WAL decoded it.
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

	// Step 1-2: pre-ALTER INSERT — id + email only (the schema in
	// testdata/001_publication.sql has id, email, name columns; no phone).
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

	// Step 3: ADD COLUMN. pgoutput emits a Relation message preceding the
	// next DML on this relation.
	if err := h.PG.Exec(ctx, "ALTER TABLE users ADD COLUMN phone text"); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN: %v", err)
	}

	// Step 4: post-ALTER INSERT with the new column populated.
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
