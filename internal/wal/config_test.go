package wal_test

import (
	"strings"
	"testing"
	"time"

	"github.com/walera/walera/internal/wal"
)

func TestDefaults(t *testing.T) {
	cfg := wal.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Defaults().Validate: %v", err)
	}
	if cfg.PublicationName != "walera_pub" {
		t.Errorf("PublicationName = %q; want walera_pub", cfg.PublicationName)
	}
	if cfg.SlotNamePrefix != "walera" {
		t.Errorf("SlotNamePrefix = %q; want walera", cfg.SlotNamePrefix)
	}
	if cfg.SlotHeadroomMin != 2 {
		t.Errorf("SlotHeadroomMin = %d; want 2", cfg.SlotHeadroomMin)
	}
	if !cfg.NaiveTimestampAssumeUTC {
		t.Error("NaiveTimestampAssumeUTC should default to true")
	}
	if cfg.Reconnect.ResetAfterSuccessDuration != 60*time.Second {
		t.Errorf("ResetAfterSuccessDuration = %s; want 60s", cfg.Reconnect.ResetAfterSuccessDuration)
	}
	if len(cfg.Tables) != 0 {
		t.Errorf("Tables = %v; want empty", cfg.Tables)
	}
}

func TestDeriveDSNs(t *testing.T) {
	t.Run("empty rejected", func(t *testing.T) {
		_, _, err := wal.DeriveDSNs("")
		if err == nil || !strings.Contains(err.Error(), "WALERA_DATABASE_URL is required") {
			t.Fatalf("DeriveDSNs(\"\"): err = %v; want 'WALERA_DATABASE_URL is required'", err)
		}
	})

	t.Run("valid preserves sslmode and derives replication", func(t *testing.T) {
		base := "postgres://u:p@h:5432/db?sslmode=disable"
		admin, repl, err := wal.DeriveDSNs(base)
		if err != nil {
			t.Fatalf("DeriveDSNs: unexpected error: %v", err)
		}
		if string(admin) != base {
			t.Errorf("admin = %q; want the base unchanged %q", admin, base)
		}
		rs := string(repl)
		if !strings.Contains(rs, "replication=database") {
			t.Errorf("repl = %q; want replication=database added", rs)
		}
		if !strings.Contains(rs, "sslmode=disable") {
			t.Errorf("repl = %q; want sslmode=disable preserved", rs)
		}
	})

	t.Run("unparseable rejected and redacted", func(t *testing.T) {
		_, _, err := wal.DeriveDSNs("postgres://u:topsecret@h/db?sslmode=bad\x00mode")
		if err == nil {
			t.Fatal("DeriveDSNs: err = nil; want parse error")
		}
		if !strings.Contains(err.Error(), "WALERA_DATABASE_URL") {
			t.Errorf("err = %q; want substring 'WALERA_DATABASE_URL'", err.Error())
		}
		if !strings.Contains(err.Error(), "is not a valid PostgreSQL URL") {
			t.Errorf("err = %q; want substring 'is not a valid PostgreSQL URL'", err.Error())
		}
		if strings.Contains(err.Error(), "topsecret") {
			t.Errorf("err leaks password: %s", err.Error())
		}
	})

	t.Run("base containing replication parameter is normalized", func(t *testing.T) {
		admin, repl, err := wal.DeriveDSNs("postgres://u:p@h/db?sslmode=disable&replication=database")
		if err != nil {
			t.Fatalf("DeriveDSNs: unexpected error: %v", err)
		}
		if strings.Contains(string(admin), "replication=") {
			t.Errorf("admin = %q; want replication parameter stripped", admin)
		}
		if !strings.Contains(string(admin), "sslmode=disable") {
			t.Errorf("admin = %q; want sslmode=disable preserved", admin)
		}
		if !strings.Contains(string(repl), "replication=database") {
			t.Errorf("repl = %q; want canonical replication=database added", repl)
		}
		if !strings.Contains(string(repl), "sslmode=disable") {
			t.Errorf("repl = %q; want sslmode=disable preserved", repl)
		}
	})
}

func TestValidate_Rejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*wal.Config)
		want   string
	}{
		{"publication not an identifier", func(c *wal.Config) { c.PublicationName = "bad-name" }, "WALERA_PUBLICATION"},
		{"publication too long", func(c *wal.Config) { c.PublicationName = wal.PublicationName(strings.Repeat("a", 64)) }, "PG identifier"},
		{"publication injection", func(c *wal.Config) { c.PublicationName = "pub'); DROP" }, "WALERA_PUBLICATION"},
		{"publication equals slot prefix", func(c *wal.Config) { c.PublicationName = "walera" }, "must differ from the replication slot prefix"},
		{"table not schema-qualified", func(c *wal.Config) { c.Tables = []string{"public.ok", "orders"} }, "WALERA_TABLES entry 2"},
		{"slot prefix invalid", func(c *wal.Config) { c.SlotNamePrefix = "bad-prefix" }, "slot name prefix"},
		{"reconnect reset zero", func(c *wal.Config) { c.Reconnect.ResetAfterSuccessDuration = 0 }, "reconnect reset duration must be > 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := wal.Defaults()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate: err = %v; want substring %q", err, tc.want)
			}
		})
	}
}

func TestValidate_Accepts(t *testing.T) {
	for _, name := range []string{"walera_pub", "_x", "A1"} {
		cfg := wal.Defaults()
		cfg.PublicationName = wal.PublicationName(name)
		cfg.Tables = []string{"public.orders", "app.users"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate(%q): %v", name, err)
		}
	}
}
