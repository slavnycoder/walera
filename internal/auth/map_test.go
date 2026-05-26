package auth

import (
	"testing"

	"github.com/walera/walera/internal/wal"
)

func mkMap(userID string, tables map[string][]string) *Whitelist {
	m := &Whitelist{
		UserID: userID,
		Tables: make(map[string]map[string]struct{}, len(tables)),
	}
	for tbl, cols := range tables {
		set := make(map[string]struct{}, len(cols))
		for _, c := range cols {
			set[c] = struct{}{}
		}
		m.Tables[tbl] = set
	}
	return m
}

func assertDroppedChangeSanitized(t *testing.T, name string, out wal.Change) {
	t.Helper()
	if out.Schema != "" || out.Table != "" || out.Op != "" || out.PK != "" || out.PKCol != "" || out.Data != nil || out.Changed != nil {
		t.Errorf("%s: dropped change leaked identity/content: got %+v; want zero Change", name, out)
	}
}

func TestMapFilter_PreservesPK(t *testing.T) {
	t.Parallel()

	m := mkMap("u1", map[string][]string{"users": {"id", "name"}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpUpdate,
		PK: "42", PKCol: "id",
		Changed: map[string]any{"id": "42", "name": "Alice"},
	}
	out, drop := m.Filter(c)
	if drop {
		t.Fatal("drop=true; want false (id is in whitelist and Changed; non-PK `name` survives)")
	}
	if got := out.Changed["id"]; got != "42" {
		t.Errorf("Changed[id]: got %v; want \"42\"", got)
	}
	if got, present := out.Changed["name"]; !present || got != "Alice" {
		t.Errorf("Changed[name]: got %v (present=%v); want \"Alice\" (whitelisted, must survive)", got, present)
	}
}

func TestMapFilter_PKAlwaysIncludedEvenIfNotInWhitelist(t *testing.T) {
	t.Parallel()

	m := mkMap("u1", map[string][]string{"users": {}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpInsert,
		PK: "42", PKCol: "id",
		Data: map[string]any{"id": "42", "name": "Alice"},
	}
	out, drop := m.Filter(c)
	if drop {
		t.Fatal("drop=true; want false (PK preserved on INSERT)")
	}
	if got := out.Data["id"]; got != "42" {
		t.Errorf("Data[id]: got %v; want \"42\"", got)
	}
	if _, present := out.Data["name"]; present {
		t.Errorf("Data[name] should have been filtered out")
	}
}

func TestMapFilter_AllChangedColumnsHiddenIsSilentDrop(t *testing.T) {
	t.Parallel()
	m := mkMap("u1", map[string][]string{"users": {"id"}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpUpdate,
		PK: "42", PKCol: "id",
		Changed: map[string]any{"name": "Alice"},
	}
	out, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (hidden-update-only with PK absent → silent drop)")
	}
	if out.Changed != nil {
		t.Errorf("out.Changed=%v; want nil (must not leak on drop=true)", out.Changed)
	}
	if out.Data != nil {
		t.Errorf("out.Data=%v; want nil (must not leak on drop=true)", out.Data)
	}
	assertDroppedChangeSanitized(t, t.Name(), out)
}

func TestMapFilter_PKAloneInChangedIsSilentDrop(t *testing.T) {
	t.Parallel()

	m := mkMap("u1", map[string][]string{"users": {"id"}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpUpdate,
		PK: "42", PKCol: "id",
		Changed: map[string]any{"id": "42", "secret": "redacted"},
	}
	out, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (only PK survives after filter → silent drop)")
	}
	if out.Changed != nil {
		t.Errorf("out.Changed=%v; want nil (must not leak on drop=true)", out.Changed)
	}
	if out.Data != nil {
		t.Errorf("out.Data=%v; want nil (must not leak on drop=true)", out.Data)
	}
	assertDroppedChangeSanitized(t, t.Name(), out)
}

func TestMapFilter_PGEmitsAllColumnsHiddenUpdateIsSilentDrop(t *testing.T) {
	t.Parallel()

	m := mkMap("u1", map[string][]string{"users": {"id"}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpUpdate,
		PK: "88", PKCol: "id",
		Changed: map[string]any{"id": "88", "email": "old@x", "name": "NewName"},
	}
	out, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (PG-emits-all-cols update with PK-only whitelist must drop)")
	}
	if out.Changed != nil {
		t.Errorf("out.Changed=%v; want nil (must not leak on drop=true)", out.Changed)
	}
	if out.Data != nil {
		t.Errorf("out.Data=%v; want nil (must not leak on drop=true)", out.Data)
	}
	assertDroppedChangeSanitized(t, t.Name(), out)
}

func TestMapFilter_DeleteUntouched(t *testing.T) {
	t.Parallel()
	m := mkMap("u1", map[string][]string{"users": {"id"}})

	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpDelete,
		PK: "42", PKCol: "id",
	}
	out, drop := m.Filter(c)
	if drop {
		t.Fatal("drop=true; want false (DELETE on whitelisted table emits)")
	}
	if out.Schema != "public" || out.Table != "users" || out.PK != "42" || out.PKCol != "id" || out.Op != wal.OpDelete {
		t.Errorf("DELETE Change scalar fields not preserved: %+v", out)
	}
	if out.Data != nil || out.Changed != nil {
		t.Errorf("DELETE Change Data/Changed should be nil: Data=%v Changed=%v", out.Data, out.Changed)
	}
}

func TestMapFilter_TableNotInMapReturnsDrop(t *testing.T) {
	t.Parallel()
	m := mkMap("u1", map[string][]string{"users": {"id"}})
	c := wal.Change{
		Schema: "public", Table: "orders", Op: wal.OpInsert,
		PK: "9", PKCol: "id",
		Data: map[string]any{"id": "9"},
	}
	out, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (orders not in whitelist)")
	}
	assertDroppedChangeSanitized(t, t.Name(), out)
}

func TestParseMap_ValidJSON(t *testing.T) {
	t.Parallel()
	body := []byte(`{"user_id":"u1","tables":{"users":["id","name"]},"ttl_seconds":60}`)
	m, err := ParseWhitelist(body)
	if err != nil {
		t.Fatalf("ParseWhitelist: %v", err)
	}
	if m.UserID != "u1" {
		t.Errorf("UserID: got %q; want %q", m.UserID, "u1")
	}
	if _, ok := m.Tables["users"]["id"]; !ok {
		t.Errorf("Tables[users][id] missing")
	}
	if _, ok := m.Tables["users"]["name"]; !ok {
		t.Errorf("Tables[users][name] missing")
	}
	if m.TTLSeconds != 60 {
		t.Errorf("TTLSeconds: got %d; want 60", m.TTLSeconds)
	}
	if m.RefreshLSN != 0 {
		t.Errorf("RefreshLSN at parse time: got %s; want 0", m.RefreshLSN)
	}
}

func TestParseMap_RejectsEmptyUserID(t *testing.T) {
	t.Parallel()
	body := []byte(`{"user_id":"","tables":{"users":["id"]}}`)
	if _, err := ParseWhitelist(body); err == nil {
		t.Fatal("ParseWhitelist: expected error for empty user_id, got nil")
	}
}

func TestParseMap_IgnoresLegacyRootsField(t *testing.T) {
	t.Parallel()
	body := []byte(`{"user_id":"u1","roots":["users"],"tables":{"users":["id"]}}`)
	m, err := ParseWhitelist(body)
	if err != nil {
		t.Fatalf("ParseWhitelist: %v", err)
	}
	if _, ok := m.Tables["users"]; !ok {
		t.Fatal("Tables[users] missing")
	}
}

func TestParseMap_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	body := []byte(`not json`)
	if _, err := ParseWhitelist(body); err == nil {
		t.Fatal("ParseWhitelist: expected error for malformed JSON, got nil")
	}
}

// TestMapFilter_DeleteNonWhitelistedTableDropped locks in the absent-table delete-leak gate:
// under whole-transaction delivery, every change in a matched tx reaches Filter for every
// eligible subscriber — including DELETE, INSERT, and UPDATE on tables absent from the whitelist.
// The absent-table !ok early-return at map.go:38-41 must drop all ops with no PK or row-content leakage.
func TestMapFilter_DeleteNonWhitelistedTableDropped(t *testing.T) {
	t.Parallel()

	// Whitelist covers only todo_lists; the `tasks` table is intentionally absent.
	m := mkMap("u1", map[string][]string{"todo_lists": {"id", "title"}})

	tests := []struct {
		name string
		c    wal.Change
	}{
		{
			name: "OpDelete on non-whitelisted table",
			c: wal.Change{
				Schema: "public", Table: "tasks", Op: wal.OpDelete,
				PK: "99", PKCol: "id",
			},
		},
		{
			name: "OpInsert on non-whitelisted table",
			c: wal.Change{
				Schema: "public", Table: "tasks", Op: wal.OpInsert,
				PK: "99", PKCol: "id",
				Data: map[string]any{"id": "99", "title": "Buy milk"},
			},
		},
		{
			name: "OpUpdate on non-whitelisted table",
			c: wal.Change{
				Schema: "public", Table: "tasks", Op: wal.OpUpdate,
				PK: "99", PKCol: "id",
				Changed: map[string]any{"id": "99", "title": "Buy milk"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, drop := m.Filter(tc.c)
			if !drop {
				t.Fatalf("%s: drop=false; want true (tasks not in whitelist — must not leak)", tc.name)
			}
			// Even if a caller ignored drop, no row content must leak.
			if out.Data != nil {
				t.Errorf("%s: out.Data=%v; want nil (no row-content leakage when drop=true)", tc.name, out.Data)
			}
			if out.Changed != nil {
				t.Errorf("%s: out.Changed=%v; want nil (no row-content leakage when drop=true)", tc.name, out.Changed)
			}
			assertDroppedChangeSanitized(t, tc.name, out)
		})
	}
}

func TestMapFilter_NilMapReturnsZeroChangeOnDrop(t *testing.T) {
	t.Parallel()

	var m *Whitelist
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpDelete,
		PK: "42", PKCol: "id",
	}
	out, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (nil map is conservative)")
	}
	assertDroppedChangeSanitized(t, t.Name(), out)
}
