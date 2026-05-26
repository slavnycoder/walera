//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// Test16TxScopedDelivery exercises transaction-scoped delivery end-to-end
// through the real WAL pipeline (pgoutput logical replication → broadcaster →
// SSE encoder).  Tables todo_lists and tasks are defined in
// testdata/002_tx_scoped_tables.sql and published via cdc_sse_streamer.
//
// Sub-scenarios:
//
//  1. co-transactional tasks change arrives in the same single event as the
//     todo_lists anchor change.
//  2. non-whitelisted tasks changes (including PK-only DELETE) are absent from
//     the delivered event.
//  3. multiply-matched subscriber receives exactly one ordered event with all
//     changes.
//  4. subscriber whose anchor entity is absent from a tx receives no event.
func Test16TxScopedDelivery(t *testing.T) {
	t.Parallel()

	// Sub-scenario 1: co-tx tasks delivered with anchor.
	// A subscriber to todo_lists/42 whose whitelist includes both todo_lists
	// and tasks receives the tasks INSERT in the SAME single event as the
	// todo_lists UPDATE.
	t.Run("CoTxTasksDeliveredWithAnchor", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap(
			"tok-1", "usr-1",
			[]string{"todo_lists", "tasks"},
			map[string][]string{
				"todo_lists": {"id", "title"},
				"tasks":      {"id", "title", "todo_list_id"},
			},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Connect FIRST so the subscriber's start_lsn is set before the seed
		// insert.  This ensures the seed event is delivered to us and we can
		// consume it before issuing the co-write.
		events, errCh, closeFn := h.Client.Connect(ctx, "todo_lists/42", "tok-1")
		defer closeFn()

		// Seed todo_lists row 42 in a separate (prior) tx.  We must consume
		// this event so the co-write produces the SECOND event we assert on.
		if err := h.PG.Exec(ctx,
			"INSERT INTO todo_lists (id, title) VALUES ($1, $2)",
			42, "initial",
		); err != nil {
			t.Fatalf("seed todo_list: %v", err)
		}
		_ = readTxEvent(ctx, t, h, events, errCh) // consume seed

		// Co-write: UPDATE todo_lists:42 AND INSERT a tasks row — ONE transaction.
		if err := h.PG.ExecBatch(ctx,
			[]string{
				"UPDATE todo_lists SET title = $1 WHERE id = $2",
				"INSERT INTO tasks (todo_list_id, title) VALUES ($1, $2)",
			},
			[][]any{
				{"updated", 42},
				{42, "first task"},
			},
		); err != nil {
			t.Fatalf("co-write tx: %v", err)
		}

		ev := readTxEvent(ctx, t, h, events, errCh)

		// Must arrive as ONE event.
		if len(ev.Changes) != 2 {
			t.Fatalf("co-tx anchor:expected 2 changes in one event, got %d (raw=%+v)", len(ev.Changes), ev.Changes)
		}

		// Both tables must be represented.
		tables := make(map[string]struct{}, 2)
		for _, c := range ev.Changes {
			tables[c.Table] = struct{}{}
		}
		if _, ok := tables["todo_lists"]; !ok {
			t.Errorf("co-tx anchor:todo_lists change missing from event (changes=%+v)", ev.Changes)
		}
		if _, ok := tables["tasks"]; !ok {
			t.Errorf("co-tx anchor:tasks change missing from event (changes=%+v)", ev.Changes)
		}
	})

	// Sub-scenario 2: non-whitelisted tasks change must not leak.
	// A subscriber whose whitelist excludes tasks receives only the todo_lists
	// change; no tasks change leaks through.
	t.Run("NonWhitelistedTasksNotDelivered", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap(
			"tok-2", "usr-2",
			[]string{"todo_lists"},
			map[string][]string{
				"todo_lists": {"id", "title"},
			},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Connect first, then seed the anchor row.
		events, errCh, closeFn := h.Client.Connect(ctx, "todo_lists/43", "tok-2")
		defer closeFn()

		// Seed todo_lists row 43.
		if err := h.PG.Exec(ctx,
			"INSERT INTO todo_lists (id, title) VALUES ($1, $2)",
			43, "initial",
		); err != nil {
			t.Fatalf("seed todo_list: %v", err)
		}
		_ = readTxEvent(ctx, t, h, events, errCh) // consume seed

		// Co-write: touch both tables in one tx.
		if err := h.PG.ExecBatch(ctx,
			[]string{
				"UPDATE todo_lists SET title = $1 WHERE id = $2",
				"INSERT INTO tasks (todo_list_id, title) VALUES ($1, $2)",
			},
			[][]any{
				{"updated", 43},
				{43, "task not whitelisted"},
			},
		); err != nil {
			t.Fatalf("co-write tx: %v", err)
		}

		ev := readTxEvent(ctx, t, h, events, errCh)

		// Exactly ONE change — the todo_lists update; tasks must be absent.
		if len(ev.Changes) != 1 {
			t.Fatalf("whitelist filter:expected 1 change, got %d (raw=%+v)", len(ev.Changes), ev.Changes)
		}
		if ev.Changes[0].Table != "todo_lists" {
			t.Errorf("whitelist filter:expected todo_lists change, got table=%q", ev.Changes[0].Table)
		}
		for _, c := range ev.Changes {
			if c.Table == "tasks" {
				t.Errorf("whitelist filter:tasks change leaked through whitelist: %+v", c)
			}
		}
	})

	// Sub-scenario 3: delete on non-whitelisted table must not leak.
	// A tx that updates todo_lists:44 AND DELETEs a tasks row must not deliver
	// any tasks change (not even a PK-only DELETE) to a subscriber whose
	// whitelist excludes tasks.
	t.Run("DeleteNonWhitelistedTableNotLeaked", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap(
			"tok-3", "usr-3",
			[]string{"todo_lists"},
			map[string][]string{
				"todo_lists": {"id", "title"},
			},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Connect first to set start_lsn before seed inserts.
		events, errCh, closeFn := h.Client.Connect(ctx, "todo_lists/44", "tok-3")
		defer closeFn()

		// Seed: create todo_lists row 44 and a tasks row to be deleted.
		if err := h.PG.ExecBatch(ctx,
			[]string{
				"INSERT INTO todo_lists (id, title) VALUES ($1, $2)",
				"INSERT INTO tasks (id, todo_list_id, title) VALUES ($1, $2, $3)",
			},
			[][]any{
				{44, "initial"},
				{200, 44, "to be deleted"},
			},
		); err != nil {
			t.Fatalf("seed: %v", err)
		}
		// The seed tx touches todo_lists:44 (whitelisted anchor) — consume it.
		_ = readTxEvent(ctx, t, h, events, errCh)

		// Co-write: update todo_lists AND delete the tasks row — ONE tx.
		if err := h.PG.ExecBatch(ctx,
			[]string{
				"UPDATE todo_lists SET title = $1 WHERE id = $2",
				"DELETE FROM tasks WHERE id = $1",
			},
			[][]any{
				{"updated", 44},
				{200},
			},
		); err != nil {
			t.Fatalf("co-write tx: %v", err)
		}

		ev := readTxEvent(ctx, t, h, events, errCh)

		// Exactly ONE change (todo_lists update); tasks DELETE must be absent.
		if len(ev.Changes) != 1 {
			t.Fatalf("delete-no-leak: expected 1 change, got %d (raw=%+v)", len(ev.Changes), ev.Changes)
		}
		if ev.Changes[0].Table != "todo_lists" {
			t.Errorf("delete-no-leak: expected todo_lists change, got table=%q", ev.Changes[0].Table)
		}
		for _, c := range ev.Changes {
			if c.Table == "tasks" {
				t.Errorf("delete-no-leak: tasks DELETE leaked (PK=%q, op=%q)", c.PK, c.Op)
			}
		}
	})

	// Sub-scenario 4: multiple matches collapse into a single ordered event.
	// A wildcard subscriber on todo_lists/all matched by multiple todo_lists
	// changes in one tx receives exactly ONE event with all changes in commit
	// order; no duplicates.
	t.Run("MultipleMatchesSingleEvent", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap(
			"tok-4", "usr-4",
			[]string{"todo_lists"},
			map[string][]string{
				"todo_lists": {"id", "title"},
			},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		events, errCh, closeFn := h.Client.Connect(ctx, "todo_lists/all", "tok-4")
		defer closeFn()

		// A single transaction that inserts three todo_lists rows.
		if err := h.PG.ExecBatch(ctx,
			[]string{
				"INSERT INTO todo_lists (id, title) VALUES ($1, $2)",
				"INSERT INTO todo_lists (id, title) VALUES ($1, $2)",
				"INSERT INTO todo_lists (id, title) VALUES ($1, $2)",
			},
			[][]any{
				{101, "alpha"},
				{102, "beta"},
				{103, "gamma"},
			},
		); err != nil {
			t.Fatalf("multi-change tx: %v", err)
		}

		ev := readTxEvent(ctx, t, h, events, errCh)

		// Exactly ONE event with exactly 3 changes in commit order.
		if len(ev.Changes) != 3 {
			t.Fatalf("multi-match dedup:expected 3 changes in one event, got %d (raw=%+v)", len(ev.Changes), ev.Changes)
		}
		wantPKs := []string{"101", "102", "103"}
		for i, c := range ev.Changes {
			if c.PK != wantPKs[i] {
				t.Errorf("multi-match dedup:change[%d].pk = %q, want %q", i, c.PK, wantPKs[i])
			}
			if c.Table != "todo_lists" {
				t.Errorf("multi-match dedup:change[%d].table = %q, want %q", i, c.Table, "todo_lists")
			}
		}

		// No second event should arrive within a short window.
		select {
		case extra := <-events:
			if extra.Type == "tx" {
				t.Errorf("multi-match dedup:unexpected second tx event: %+v", extra)
			}
		case <-time.After(500 * time.Millisecond):
			// OK — only one event delivered.
		}
	})

	// Sub-scenario 5: non-matching tx delivers no event.
	// A subscriber on todo_lists/45 receives no event when a transaction
	// touches only tasks (todo_lists:45 is absent from the tx).
	t.Run("NonMatchingTxNoEvent", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap(
			"tok-5", "usr-5",
			[]string{"todo_lists", "tasks"},
			map[string][]string{
				"todo_lists": {"id", "title"},
				"tasks":      {"id", "title", "todo_list_id"},
			},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		events, errCh, closeFn := h.Client.Connect(ctx, "todo_lists/45", "tok-5")
		defer closeFn()

		// A transaction that touches only tasks — no todo_lists:45 row.
		if err := h.PG.Exec(ctx,
			"INSERT INTO tasks (todo_list_id, title) VALUES ($1, $2)",
			99, "unrelated task",
		); err != nil {
			t.Fatalf("tasks-only tx: %v", err)
		}

		// No event should arrive within the window.
		select {
		case ev := <-events:
			if ev.Type == "tx" {
				t.Fatalf("non-matching tx:unexpected event delivered (subscriber not in tx): %+v", ev)
			}
		case err := <-errCh:
			t.Fatalf("non-matching tx:client error: %v", err)
		case <-time.After(500 * time.Millisecond):
			// OK — nothing delivered to a non-matching subscriber.
		}
	})
}
