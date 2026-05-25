package wal_test

import (
	"strings"
	"testing"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/wal"
)

// newK builds a koanf instance with wal defaults applied. Tests then
// override individual keys to exercise specific code paths.
func newK(t *testing.T) *koanf.Koanf {
	t.Helper()
	k := koanf.New(".")
	wal.ApplyDefaults(k)
	return k
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := wal.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
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
	if cfg.Bootstrap.Mode != "auto" {
		t.Errorf("Bootstrap.Mode = %q; want auto", cfg.Bootstrap.Mode)
	}
}

// TestDeriveDSNs exercises the single-URL derivation helper: the base URL is
// the admin DSN; the replication DSN is the base with replication=database
// added; an empty or unparseable base is rejected.
func TestDeriveDSNs(t *testing.T) {
	t.Run("empty rejected", func(t *testing.T) {
		_, _, err := wal.DeriveDSNs("")
		if err == nil || !strings.Contains(err.Error(), "database.url is required") {
			t.Fatalf("DeriveDSNs(\"\"): err = %v; want 'database.url is required'", err)
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
		if !strings.Contains(err.Error(), "database.url") {
			t.Errorf("err = %q; want substring 'database.url'", err.Error())
		}
		if !strings.Contains(err.Error(), "DSN parse failed") {
			t.Errorf("err = %q; want substring 'DSN parse failed'", err.Error())
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

func TestLoadConfig_InvalidBootstrapMode(t *testing.T) {
	k := newK(t)
	_ = k.Set("wal.bootstrap.mode", "bogus")
	_, err := wal.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "wal.bootstrap.mode") {
		t.Fatalf("LoadConfig: err = %v; want bootstrap.mode error", err)
	}
}

func TestLoadConfig_InvalidPublicationName(t *testing.T) {
	k := newK(t)
	_ = k.Set("wal.publication_name", "bad-name")
	_, err := wal.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "wal.publication_name") {
		t.Fatalf("LoadConfig: err = %v; want publication_name error", err)
	}
}

func TestLoadConfig_TablesMustBeSchemaQualified(t *testing.T) {
	k := newK(t)
	_ = k.Set("wal.bootstrap.tables", []string{"orders"})
	_, err := wal.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "wal.bootstrap.tables[0]") {
		t.Fatalf("LoadConfig: err = %v; want schema-qualified error", err)
	}
}

// TestLoadConfig_SchemaValidation — D-12 layer 2 schema rules in
// table-driven form. One row per failure mode.
func TestLoadConfig_SchemaValidation(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(*koanf.Koanf)
		wantSubstrs []string
		// noPassLeak: assert the err does not contain this literal.
		noPassLeak string
	}{
		{
			name: "publication_name invalid identifier",
			setup: func(k *koanf.Koanf) {
				_ = k.Set("wal.publication_name", "bad-name")
			},
			wantSubstrs: []string{"wal.publication_name", "PG identifier"},
		},
		{
			name: "slot_name_prefix too long (>63)",
			setup: func(k *koanf.Koanf) {
				_ = k.Set("wal.slot_name_prefix", strings.Repeat("a", 64))
			},
			wantSubstrs: []string{"wal.slot_name_prefix", "PG identifier"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := newK(t)
			tc.setup(k)
			_, err := wal.LoadConfig(k)
			if err == nil {
				t.Fatalf("LoadConfig: err = nil; want error")
			}
			for _, s := range tc.wantSubstrs {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("err = %q; want substring %q", err.Error(), s)
				}
			}
			if tc.noPassLeak != "" && strings.Contains(err.Error(), tc.noPassLeak) {
				t.Errorf("err leaks %q: %s", tc.noPassLeak, err.Error())
			}
		})
	}
}

func TestLoadConfig_PublicationNameValid(t *testing.T) {
	cases := []string{"walera_pub", "_x", "A1"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			k := newK(t)
			_ = k.Set("wal.publication_name", name)
			if _, err := wal.LoadConfig(k); err != nil {
				t.Fatalf("LoadConfig(%q): %v", name, err)
			}
		})
	}
}

// TestLoadConfig_SlotPrefixCannotEqualPublication — D-12 layer 3
// combination rule.
func TestLoadConfig_SlotPrefixCannotEqualPublication(t *testing.T) {
	k := newK(t)
	_ = k.Set("wal.publication_name", "shared_name")
	_ = k.Set("wal.slot_name_prefix", "shared_name")
	_, err := wal.LoadConfig(k)
	if err == nil {
		t.Fatal("LoadConfig: err = nil; want collision error")
	}
	if !strings.Contains(err.Error(), "wal.slot_name_prefix vs wal.publication_name") {
		t.Errorf("err = %q; want the pair-collision error", err.Error())
	}
}
