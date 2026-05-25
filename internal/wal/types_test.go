package wal

import "testing"

func TestChange_WildcardKey(t *testing.T) {
	t.Parallel()
	c := Change{Schema: "public", Table: "users", PK: "42"}
	if got, want := c.WildcardKey(), "public.users"; got != want {
		t.Errorf("WildcardKey() = %q; want %q", got, want)
	}
}

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
