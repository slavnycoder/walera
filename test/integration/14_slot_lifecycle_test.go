//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func derivedSlotName(prefix, hostname string, pid int) string {
	var b strings.Builder
	b.Grow(len(hostname))
	for _, r := range hostname {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
	}
	return fmt.Sprintf("%s_%s_%d", prefix, b.String(), pid)
}

func waitForSlotPrefix(t *testing.T, p *PG, prefix string, deadline time.Duration) (string, bool, bool, bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		name, temp, act, ok := findSlotByPrefix(t, p, prefix, "")
		if ok {
			return name, temp, act, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", false, false, false
}

func findSlotByPrefix(t *testing.T, p *PG, prefix, skip string) (string, bool, bool, bool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("findSlotByPrefix: connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var (
		name      string
		temporary bool
		active    bool
	)
	q := `SELECT slot_name, temporary, active
	      FROM pg_replication_slots
	      WHERE slot_name LIKE $1 AND slot_name <> $2
	      ORDER BY slot_name LIMIT 1`
	err = conn.QueryRow(ctx, q, prefix+"%", skip).Scan(&name, &temporary, &active)
	if err == pgx.ErrNoRows {
		return "", false, false, false
	}
	if err != nil {
		t.Fatalf("findSlotByPrefix: query: %v", err)
	}
	return name, temporary, active, true
}

func Test14SlotLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("PublicationReuse", func(t *testing.T) {

		h := NewHarness(t)
		h.Auth.SetMap(
			"test-token", "test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		events, errCh, closeFn := h.Client.Connect(ctx, "users/100", "test-token")
		defer closeFn()

		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			100, "reuse@x", "Reuse",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}

		select {
		case ev := <-events:
			if ev.Type != "tx" {
				t.Fatalf("expected tx event, got %q (data=%s)", ev.Type, string(ev.Data))
			}
		case err := <-errCh:
			t.Fatalf("client error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout; stderr:\n%s", h.Binary.Stderr())
		}
	})

	t.Run("PublicationMissing", func(t *testing.T) {

		pg := NewPG(t)
		auth := NewMockAuth(t)
		auth.SetMap(
			"test-token", "test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pg.Exec(ctx, "DROP PUBLICATION IF EXISTS cdc_sse_streamer"); err != nil {
			t.Fatalf("drop publication: %v", err)
		}

		bin := SpawnBinary(t, pg.DSN, pg.ReplicationDSN(), auth.URL())
		client := NewClient(bin.BaseURL())

		events, errCh, closeFn := client.Connect(ctx, "users/200", "test-token")
		defer closeFn()

		if err := pg.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			200, "missing@x", "Missing",
		); err != nil {
			t.Fatalf("insert: %v", err)
		}

		select {
		case ev := <-events:
			if ev.Type != "tx" {
				t.Fatalf("expected tx event, got %q (data=%s)", ev.Type, string(ev.Data))
			}
		case err := <-errCh:
			t.Fatalf("client error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout (auto-bootstrap may not have re-created publication); stderr:\n%s", bin.Stderr())
		}
	})

	t.Run("SlotCreate", func(t *testing.T) {

		h := NewHarness(t)
		h.Auth.SetMap(
			"test-token", "test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)
		slotName, temporary, active, ok := waitForSlotPrefix(t, h.PG, "walera_test_", 10*time.Second)
		if !ok {
			t.Fatalf("walera_test_* slot never appeared; stderr:\n%s", h.Binary.Stderr())
		}
		if !temporary {
			t.Errorf("slot %q temporary=false; expected true (WAL-06 / ADR-04 temporary slot policy)", slotName)
		}
		if !active {
			t.Errorf("slot %q active=false; expected true (Walera holds the slot for the lifetime of the connection)", slotName)
		}
	})

	t.Run("SlotAlreadyExists", func(t *testing.T) {

		pg := NewPG(t)
		auth := NewMockAuth(t)
		auth.SetMap(
			"test-token", "test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		preCreated := derivedSlotName("walera_test", hostname, 99999)
		pg.CreateLogicalSlot(t, preCreated, false)

		bin := SpawnBinary(t, pg.DSN, pg.ReplicationDSN(), auth.URL())

		binSlot, temp, _, ok := findSlotByPrefix(t, pg, "walera_test_", preCreated)
		if !ok {

			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				binSlot, temp, _, ok = findSlotByPrefix(t, pg, "walera_test_", preCreated)
				if ok {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		if !ok {
			t.Fatalf("binary's own slot never appeared next to pre-created %q; stderr:\n%s", preCreated, bin.Stderr())
		}
		if binSlot == preCreated {
			t.Fatalf("internal bug: skip parameter ignored; got %q == %q", binSlot, preCreated)
		}
		if !temp {
			t.Errorf("binary slot %q temporary=false; expected true", binSlot)
		}
	})

	t.Run("TemporarySlotLifecycle", func(t *testing.T) {

		pg := NewPG(t)
		auth := NewMockAuth(t)
		auth.SetMap(
			"test-token", "test-user",
			[]string{"users"},
			map[string][]string{"users": {"id", "email"}},
		)
		bin := SpawnBinary(t, pg.DSN, pg.ReplicationDSN(), auth.URL())

		slotName, temporary, _, ok := waitForSlotPrefix(t, pg, "walera_test_", 10*time.Second)
		if !ok {
			t.Fatalf("slot never appeared; stderr:\n%s", bin.Stderr())
		}
		if !temporary {
			t.Fatalf("WAL-06 / D-10 finding: slot %q is not temporary; fix internal/wal/slot.go::bootstrapSlot to pass Temporary: true", slotName)
		}

		if err := bin.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("SIGTERM binary: %v", err)
		}

		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if !pg.SlotExists(t, slotName) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("slot %q still present 15s after SIGTERM; expected temporary slot to drop on connection close", slotName)
	})
}
