//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// An idle stream must still report its position: Postgres sees a fresh
// reply_time from Walera's walsender, and no standby write fails.
func Test18StandbyACKReachesPostgres(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, h.PG.DSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background()) //nolint:errcheck

	replyTime := func() time.Time {
		t.Helper()
		var ts *time.Time
		if err := conn.QueryRow(ctx,
			"SELECT reply_time FROM pg_stat_replication ORDER BY reply_time DESC NULLS LAST LIMIT 1",
		).Scan(&ts); err != nil {
			t.Fatalf("pg_stat_replication: %v", err)
		}
		if ts == nil {
			return time.Time{}
		}
		return *ts
	}
	first := replyTime()
	time.Sleep(12 * time.Second) // > 2 standby intervals of idle
	second := replyTime()
	if !second.After(first) {
		t.Fatalf("reply_time did not advance while idle: %v -> %v", first, second)
	}
	if strings.Contains(h.Binary.Stderr(), "standby status update failed") {
		t.Fatalf("standby update failures in logs:\n%s", h.Binary.Stderr())
	}
}
