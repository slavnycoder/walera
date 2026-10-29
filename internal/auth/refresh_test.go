package auth

import (
	"context"
	"net/http"
	"testing"
)

func usersMap(userID string, cols ...string) *Whitelist {
	set := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		set[c] = struct{}{}
	}
	return &Whitelist{
		UserID:     userID,
		Tables:     map[string]map[string]struct{}{"users": set},
		TTLSeconds: 60,
	}
}

func TestTryRefresh_Revoked403_DropsSub(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	c := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Channel:    "users:42",
		UserID:     "u1",
		InitialMap: usersMap("u1", "id"),
	})

	s.tryRefresh(context.Background())
	if r := s.Sub.Reason(); r != "auth_revoked" {
		t.Errorf("drop reason = %q; want auth_revoked", r)
	}
}
