package app

import (
	"os"
	"strings"
	"testing"
)

func TestLoad_MalformedYAML(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writeTempYAML(t, "database:\n  url: \"x\"\n  : invalid\n")
	if _, err := LoadAppConfig(path); err == nil {
		t.Fatal("Load() expected YAML parse error, got nil")
	} else if !strings.Contains(err.Error(), "load YAML") && !strings.Contains(err.Error(), "yaml") {
		t.Errorf("Load() err = %v; want YAML-parse error", err)
	}
}

func TestLoad_MissingPathUsesEnvOnly(t *testing.T) {
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "envpub")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load(\"\") returned error: %v", err)
	}
	if cfg.WAL.PublicationName != "envpub" {
		t.Errorf("WAL.PublicationName = %q; want %q", cfg.WAL.PublicationName, "envpub")
	}

	tmp := t.TempDir()
	nonExistent := tmp + string(os.PathSeparator) + "no-such-file.yaml"
	cfg2, err := LoadAppConfig(nonExistent)
	if err != nil {
		t.Fatalf("Load(%q) returned error: %v", nonExistent, err)
	}
	if cfg2.WAL.PublicationName != "envpub" {
		t.Errorf("WAL.PublicationName = %q; want %q", cfg2.WAL.PublicationName, "envpub")
	}
}

func TestValidate_ResetAfterSuccessBounds(t *testing.T) {
	setPhase3RequiredEnv(t)
	tests := []struct {
		name      string
		yaml      string
		wantInErr string
	}{
		{
			name: "reset_after_success_duration zero",
			yaml: `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: p
  reconnect:
    reset_after_success_duration: 0s
`,
			wantInErr: "reset_after_success_duration must be > 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.yaml)
			_, err := LoadAppConfig(path)
			if err == nil {
				t.Fatalf("Load() expected error containing %q, got nil", tc.wantInErr)
			}
			if !strings.Contains(err.Error(), tc.wantInErr) {
				t.Errorf("err = %v; want substring %q", err, tc.wantInErr)
			}
		})
	}
}
