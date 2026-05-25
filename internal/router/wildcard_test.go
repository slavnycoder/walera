package router

import (
	"context"
	"testing"
)

func newWildcardTestSubscriber(id string) *Subscriber {
	return NewSubscriber(
		SubscriberConfig{
			ID:        id,
			Kind:      KindWildcard,
			Schema:    "public",
			Table:     "users",
			BufferCap: 1,
		},
		SubscriberDeps{Parent: context.Background()},
	)
}

// TestWildcard_AddLookupRemove verifies the multi-subscriber-per-key contract
// of the wildcard index: Add appends, Lookup returns a copy of the slice,
// Remove removes by pointer identity.
func TestWildcard_AddLookupRemove(t *testing.T) {
	t.Parallel()
	w := newWildcardIndex()
	const key = "public.users"

	a := newWildcardTestSubscriber("a")
	b := newWildcardTestSubscriber("b")

	w.Add(key, a)
	w.Add(key, b)

	subs := w.Lookup(key)
	if len(subs) != 2 {
		t.Fatalf("Lookup after two Adds: got %d subs; want 2", len(subs))
	}
	if subs[0] != a || subs[1] != b {
		t.Errorf("Lookup ordering: got [%p %p]; want [%p %p]", subs[0], subs[1], a, b)
	}
	if got, want := w.Len(), 2; got != want {
		t.Errorf("Len after two Adds: got %d; want %d", got, want)
	}

	// Mutating the returned slice MUST NOT affect the index (copy-before-unlock).
	subs[0] = nil
	if got := w.Lookup(key); got[0] != a {
		t.Errorf("mutating Lookup result leaked into index: got %p; want %p", got[0], a)
	}

	w.Remove(key, a)
	subs = w.Lookup(key)
	if len(subs) != 1 || subs[0] != b {
		t.Errorf("Lookup after removing a: got %v; want [%p]", subs, b)
	}
	if got, want := w.Len(), 1; got != want {
		t.Errorf("Len after removing a: got %d; want %d", got, want)
	}

	w.Remove(key, b)
	if got := w.Lookup(key); got != nil {
		t.Errorf("Lookup after removing both: got %v; want nil", got)
	}
	if got, want := w.Len(), 0; got != want {
		t.Errorf("Len after removing both: got %d; want %d", got, want)
	}
}

// TestWildcard_LookupEmpty asserts that an unknown key returns nil (not an
// empty non-nil slice — the doc comment promises nil).
func TestWildcard_LookupEmpty(t *testing.T) {
	t.Parallel()
	w := newWildcardIndex()
	if got := w.Lookup("public.unknown"); got != nil {
		t.Errorf("Lookup on empty index: got %v; want nil", got)
	}
}

// TestWildcard_RemoveOfUnknownNoOp asserts that Remove on a missing key /
// missing subscriber is a no-op and does not panic.
func TestWildcard_RemoveOfUnknownNoOp(t *testing.T) {
	t.Parallel()
	w := newWildcardIndex()
	w.Remove("public.unknown", newWildcardTestSubscriber("ghost"))
	if got, want := w.Len(), 0; got != want {
		t.Errorf("Len after no-op Remove: got %d; want %d", got, want)
	}

	// Now add one, then try to remove a different pointer with the same key.
	a := newWildcardTestSubscriber("a")
	b := newWildcardTestSubscriber("b")
	w.Add("public.users", a)
	w.Remove("public.users", b)
	if got := w.Lookup("public.users"); len(got) != 1 || got[0] != a {
		t.Errorf("Remove of non-matching pointer mutated slice: got %v; want [%p]", got, a)
	}
}
