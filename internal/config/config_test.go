package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
)

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writeTempYAML: %v", err)
	}
	return path
}

// TestLoadKoanf_YAMLAndEnvOverlay verifies the primitive's two-layer source
// stack: YAML loaded first, then env vars overlay.
func TestLoadKoanf_YAMLAndEnvOverlay(t *testing.T) {
	path := writeTempYAML(t, `
wal:
  postgres_dsn: "from-yaml"
log:
  level: "warn"
`)
	t.Setenv("WALERA_LOG_LEVEL", "debug")

	k, err := config.LoadKoanf(path, nil)
	if err != nil {
		t.Fatalf("LoadKoanf: %v", err)
	}
	if got := k.String("wal.postgres_dsn"); got != "from-yaml" {
		t.Errorf("wal.postgres_dsn = %q; want from-yaml", got)
	}
	if got := k.String("log.level"); got != "debug" {
		t.Errorf("log.level = %q; want debug (env should override yaml)", got)
	}
}

// TestLoadKoanf_AppliesDefaults verifies the applyDefaults closure runs
// before any source loader so defaults survive an absent YAML+env.
func TestLoadKoanf_AppliesDefaults(t *testing.T) {
	k, err := config.LoadKoanf("", func(k *koanf.Koanf) {
		_ = k.Set("router.exact_buffer", 64)
	})
	if err != nil {
		t.Fatalf("LoadKoanf: %v", err)
	}
	if got := k.Int("router.exact_buffer"); got != 64 {
		t.Errorf("router.exact_buffer = %d; want 64", got)
	}
}

// TestLoadKoanf_EnvTransform_MultiLevelRemap exercises the explicit env
// remap allow-list (wal.bootstrap_* → wal.bootstrap.*).
func TestLoadKoanf_EnvTransform_MultiLevelRemap(t *testing.T) {
	t.Setenv("WALERA_WAL_BOOTSTRAP_MODE", "verify")
	t.Setenv("WALERA_WAL_BOOTSTRAP_CREATE_ROLES", "true")
	k, err := config.LoadKoanf("", nil)
	if err != nil {
		t.Fatalf("LoadKoanf: %v", err)
	}
	if got := k.String("wal.bootstrap.mode"); got != "verify" {
		t.Errorf("wal.bootstrap.mode = %q; want verify", got)
	}
	if got := k.Bool("wal.bootstrap.create_roles"); !got {
		t.Errorf("wal.bootstrap.create_roles = %v; want true", got)
	}
}

// TestLoadKoanf_EnvTransform_SliceCommaSplit verifies the comma-split path
// for slice-valued env keys (http.cors_origins, wal.bootstrap.tables).
func TestLoadKoanf_EnvTransform_SliceCommaSplit(t *testing.T) {
	t.Setenv("WALERA_HTTP_CORS_ORIGINS", "http://a.com,http://b.com")
	t.Setenv("WALERA_WAL_BOOTSTRAP_TABLES", "public.orders, public.invoices")
	k, err := config.LoadKoanf("", nil)
	if err != nil {
		t.Fatalf("LoadKoanf: %v", err)
	}
	gotOrigins := k.Strings("http.cors_origins")
	if len(gotOrigins) != 2 || gotOrigins[0] != "http://a.com" || gotOrigins[1] != "http://b.com" {
		t.Errorf("http.cors_origins = %v; want [http://a.com http://b.com]", gotOrigins)
	}
	gotTables := k.Strings("wal.bootstrap.tables")
	if len(gotTables) != 2 || gotTables[0] != "public.orders" || gotTables[1] != "public.invoices" {
		t.Errorf("wal.bootstrap.tables = %v; want [public.orders public.invoices]", gotTables)
	}
}

// TestLoadKoanf_EnvTransform_EmptyValueIgnored verifies that an empty env
// override is treated as unset so caller defaults survive.
func TestLoadKoanf_EnvTransform_EmptyValueIgnored(t *testing.T) {
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "")
	k, err := config.LoadKoanf("", func(k *koanf.Koanf) {
		_ = k.Set("wal.publication_name", "walera_pub")
	})
	if err != nil {
		t.Fatalf("LoadKoanf: %v", err)
	}
	if got := k.String("wal.publication_name"); got != "walera_pub" {
		t.Errorf("wal.publication_name = %q; want walera_pub (empty env should not override default)", got)
	}
}

// TestLoadKoanf_MalformedYAML drives the YAML-parse-error branch.
func TestLoadKoanf_MalformedYAML(t *testing.T) {
	path := writeTempYAML(t, "wal:\n  postgres_dsn: \"x\"\n  : invalid\n")
	if _, err := config.LoadKoanf(path, nil); err == nil {
		t.Fatal("LoadKoanf expected YAML parse error, got nil")
	} else if !strings.Contains(err.Error(), "load YAML") && !strings.Contains(err.Error(), "yaml") {
		t.Errorf("err = %v; want YAML-parse error", err)
	}
}

// TestLoadKoanf_MissingPathUsesEnvOnly verifies that an empty path or a
// path that does not exist on disk falls back to env-only.
func TestLoadKoanf_MissingPathUsesEnvOnly(t *testing.T) {
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "envpub")

	k, err := config.LoadKoanf("", nil)
	if err != nil {
		t.Fatalf("LoadKoanf(\"\") returned error: %v", err)
	}
	if got := k.String("wal.publication_name"); got != "envpub" {
		t.Errorf("wal.publication_name = %q; want envpub", got)
	}

	tmp := t.TempDir()
	nonExistent := filepath.Join(tmp, "no-such-file.yaml")
	k2, err := config.LoadKoanf(nonExistent, nil)
	if err != nil {
		t.Fatalf("LoadKoanf(%q) returned error: %v", nonExistent, err)
	}
	if got := k2.String("wal.publication_name"); got != "envpub" {
		t.Errorf("wal.publication_name = %q; want envpub", got)
	}
}
