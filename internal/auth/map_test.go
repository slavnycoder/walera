package auth

import (
	"testing"

	"github.com/walera/walera/internal/wal"
)

// mkMap builds a *Whitelist literal with the given allowed-column sets.
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

func TestMapFilter_PreservesPK(t *testing.T) {
	t.Parallel()
	// Whitelist includes both `id` (PK) and `name` so that the UPDATE has at
	// least one non-PK whitelisted column survive — the silent-drop predicate
	// fires when an UPDATE yields only the PK after filtering. The assertion
	// intent here is PK preservation alongside a surviving non-PK column.
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
	// Empty allowed-column set — PK must still survive.
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
		Changed: map[string]any{"name": "Alice"}, // PK NOT in Changed; name not allowed
	}
	_, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (hidden-update-only with PK absent → silent drop)")
	}
}

func TestMapFilter_PKAloneInChangedIsSilentDrop(t *testing.T) {
	t.Parallel()
	// Silent-drop predicate: an UPDATE whose filtered Changed map contains
	// ONLY the PK is silently dropped — the row identifier is already known
	// by the subscription channel, so PK-alone carries no new information
	// for a subscriber.
	//   silent-drop fires when len(filtered) == 0 ||
	//   (len(filtered) == 1 && filtered contains only PK without data/changed).
	m := mkMap("u1", map[string][]string{"users": {"id"}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpUpdate,
		PK: "42", PKCol: "id",
		Changed: map[string]any{"id": "42", "secret": "redacted"},
	}
	_, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (only PK survives after filter → silent drop)")
	}
}

func TestMapFilter_PGEmitsAllColumnsHiddenUpdateIsSilentDrop(t *testing.T) {
	t.Parallel()
	// Models PG REPLICA IDENTITY DEFAULT NewTuple where unchanged columns are
	// included in the WAL payload (assembly.buildChangedMap lifts them all).
	// Integration test Test03/HiddenUpdateOnly_SilentDrop covers the wire path
	// for the same scenario. With a PK-only whitelist, every non-PK column is
	// filtered out, leaving the PK alone — must drop.
	m := mkMap("u1", map[string][]string{"users": {"id"}})
	c := wal.Change{
		Schema: "public", Table: "users", Op: wal.OpUpdate,
		PK: "88", PKCol: "id",
		Changed: map[string]any{"id": "88", "email": "old@x", "name": "NewName"},
	}
	_, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (PG-emits-all-cols update with PK-only whitelist must drop)")
	}
}

func TestMapFilter_DeleteUntouched(t *testing.T) {
	t.Parallel()
	m := mkMap("u1", map[string][]string{"users": {"id"}})
	// Phase-1 DELETE has no Data/Changed (per wal.types invariants).
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
	_, drop := m.Filter(c)
	if !drop {
		t.Fatal("drop=false; want true (orders not in whitelist)")
	}
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

// TestParseMap_IgnoresLegacyRootsField — a backend that still emits the old
// `roots` array must continue to parse cleanly. encoding/json silently drops
// unknown fields, so no special handling is required in ParseWhitelist.
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
