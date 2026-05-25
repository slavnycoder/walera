package wal

import "testing"

// TestChange_WildcardKey verifies that Change.WildcardKey() returns the
// schema-qualified table name "<schema>.<table>" used by the Phase-2 router
// as the wildcard-subscription lookup key (ROUTE-03).
func TestChange_WildcardKey(t *testing.T) {
	t.Parallel()
	c := Change{Schema: "public", Table: "users", PK: "42"}
	if got, want := c.WildcardKey(), "public.users"; got != want {
		t.Errorf("WildcardKey() = %q; want %q", got, want)
	}
}

// TestChange_KeyAndWildcardKeyAgree verifies that Key() and WildcardKey()
// share the same schema.table prefix — they differ only by the ":<pk>" suffix.
// This invariant is what allows the router to derive both lookup keys from a
// single change without re-walking the struct.
func TestChange_KeyAndWildcardKeyAgree(t *testing.T) {
	t.Parallel()
	c := Change{Schema: "public", Table: "orders", PK: "abc-123"}
	wantKey := "public.orders:abc-123"
	wantWildcard := "public.orders"
	if got := c.Key(); got != wantKey {
		t.Errorf("Key() = %q; want %q", got, wantKey)
	}
	if got := c.WildcardKey(); got != wantWildcard {
		t.Errorf("WildcardKey() = %q; want %q", got, wantWildcard)
	}
}
