//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// An idle stream exercises the receive-timeout path; standby updates sent
// concurrently with that timeout used to fail with i/o timeout.
func Test18StandbyACKIdle(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	time.Sleep(21 * time.Second)

	got, err := scrapeMetric(ctx, h.Binary.BaseURL()+"/metrics", "walera_wal_standby_ack_failures_total")
	if err != nil {
		t.Fatalf("scrape walera_wal_standby_ack_failures_total: %v", err)
	}
	if got != 0 {
		t.Fatalf("walera_wal_standby_ack_failures_total = %v after idle; want 0\nstderr:\n%s", got, h.Binary.Stderr())
	}
}
