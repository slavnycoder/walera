//go:build integration

// Package integration — scenario 14: replication slot and publication
// lifecycle (WAL-01) plus the WAL-06 temporary-slot assertion that locks
// the doc-to-code contract from docs/adr/0004-replication-slot-policy.md.
//
// Subtests:
//   - PublicationReuse        — start Walera against the fixture publication;
//     INSERTs flow through (binds without re-creating).
//   - PublicationMissing      — DROP the seeded publication before starting
//     Walera; assert the bootstrap.mode="auto" path re-creates it and
//     INSERTs flow through. The default test harness sets bootstrap.mode
//     implicitly via the wal-config defaults (auto / FOR ALL TABLES fallback
//     when wal.bootstrap.tables is empty).
//   - SlotCreate              — fresh boot; query pg_replication_slots for
//     the Walera-derived slot; assert temporary=true, active=true.
//   - SlotAlreadyExists       — pre-create a non-temporary slot whose name
//     does NOT collide with the binary's pid-derived name; assert the
//     binary's own slot is created alongside it (proving the slot policy
//     uniquely names each runtime). Codifies the observable behaviour
//     without depending on a forced collision (the binary's pid is
//     unpredictable; a pid-collision scenario is non-deterministic).
//   - TemporarySlotLifecycle  — D-09 assertion: assert the slot Walera
//     creates is temporary=true; SIGTERM the binary; poll pg_replication_slots
//     until the slot disappears.
//
// Citations: WAL-01, WAL-06.
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

// derivedSlotName mirrors wal.Config.NewSlotName(hostname, pid): lower-cases
// alphabetic runes and rewrites every other rune outside [a-z0-9_] to '_',
// then appends `_<pid>`. Duplicated here (rather than imported from
// internal/wal) because deps-check forbids test/integration from importing
// internal/wal — the integration package observes Walera externally via
// the spawned binary and admin DB.
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

// waitForSlotPrefix polls pg_replication_slots until the first slot whose
// slot_name starts with prefix appears, or the deadline elapses. Returns
// (slot_name, temporary, active, ok). Used because the binary's pid (and
// therefore exact slot suffix) is not directly visible from the test —
// the prefix walera_test_<sanitised-host>_ is sufficient to find it.
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

// findSlotByPrefix returns the first pg_replication_slots row whose slot_name
// starts with prefix and (when skip != "") is not equal to skip. ok=false
// when no such row exists.
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
		// The seeded publication (cdc_sse_streamer) is created by
		// testdata/001_publication.sql before Walera connects. The
		// bootstrap.mode="auto" "publication exists" branch binds it.
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
		// Boot PG (publication seeded), DROP the publication, then spawn
		// Walera. The auto-bootstrap path must re-create the publication.
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
		// Fresh harness; the binary creates one temporary pgoutput slot at
		// startup. Assert the observable shape: temporary=true, active=true.
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
		// Pre-create a non-temporary slot whose name does NOT collide with
		// the binary's pid-derived name (we use synthetic pid 99999 — the
		// spawned binary's real pid will differ). Assert the binary's own
		// slot is created alongside the pre-created one. This codifies the
		// observable behaviour: the slot-name derivation (hostname + pid)
		// is collision-avoiding by construction; the binary does not abort
		// on the presence of unrelated pre-existing slots in the same
		// namespace.
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

		// Walera's own slot appears with a different pid suffix.
		binSlot, temp, _, ok := findSlotByPrefix(t, pg, "walera_test_", preCreated)
		if !ok {
			// Give the reader a moment to bootstrap its slot.
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
		// D-09 assertion: the slot is temporary=true AND vanishes when the
		// binary's replication connection closes. Source of truth for WAL-06.
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

		// SIGTERM the binary to close the replication connection. The
		// harness's t.Cleanup also signals SIGTERM later — idempotent.
		if err := bin.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("SIGTERM binary: %v", err)
		}

		// Poll pg_replication_slots until the slot disappears. Temporary
		// slots are dropped automatically when the replication connection
		// closes (ADR-04 §"Temporary slot policy").
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
