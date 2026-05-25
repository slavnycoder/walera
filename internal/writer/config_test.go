package writer

import (
	"flag"
	"os"
	"strings"
	"testing"
	"time"
)

// resetEnv unsets any WRITER_* env var so tests are isolated. Returns a
// restoration function.
func resetEnv(t *testing.T) func() {
	t.Helper()
	saved := map[string]string{}
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "WRITER_") {
			idx := strings.IndexByte(kv, '=')
			if idx > 0 {
				k := kv[:idx]
				saved[k] = kv[idx+1:]
				_ = os.Unsetenv(k)
			}
		}
	}
	return func() {
		for k, v := range saved {
			_ = os.Setenv(k, v)
		}
	}
}

// newTestFlagSet returns an empty flag set so Load does not attempt to read
// command-line flags from `go test`.
func newTestFlagSet() *flag.FlagSet {
	return flag.NewFlagSet("writer-test", flag.ContinueOnError)
}

func TestLoad_Defaults(t *testing.T) {
	defer resetEnv(t)()
	// PG.DSN is required; set it so we exercise defaults for the rest.
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db?sslmode=disable")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: unexpected error: %v", err)
	}

	if got, want := cfg.Log.Level, "info"; got != want {
		t.Errorf("Log.Level = %q, want %q", got, want)
	}
	if got, want := cfg.Scenario.Name, "smoke"; got != want {
		t.Errorf("Scenario.Name = %q, want %q", got, want)
	}
	if got, want := cfg.Scenario.CommitRate, 10.0; got != want {
		t.Errorf("Scenario.CommitRate = %v, want %v", got, want)
	}
	if got, want := cfg.Scenario.RowsPerTx, 1; got != want {
		t.Errorf("Scenario.RowsPerTx = %d, want %d", got, want)
	}
	if got, want := cfg.Scenario.RampDuration, 5*time.Minute; got != want {
		t.Errorf("Scenario.RampDuration = %v, want %v", got, want)
	}
	if got, want := cfg.Pool.MaxConns, 8; got != want {
		t.Errorf("Pool.MaxConns = %d, want %d", got, want)
	}
	if got, want := cfg.Pool.MinConns, 1; got != want {
		t.Errorf("Pool.MinConns = %d, want %d", got, want)
	}
	if got, want := cfg.HTTP.Addr, ":9100"; got != want {
		t.Errorf("HTTP.Addr = %q, want %q", got, want)
	}
	if got, want := cfg.Arrivals.Distribution, "poisson"; got != want {
		t.Errorf("Arrivals.Distribution = %q, want %q", got, want)
	}
	if got, want := cfg.PG.TxTimeout, 5*time.Second; got != want {
		t.Errorf("PG.TxTimeout = %v, want %v", got, want)
	}
	wantTargets := []string{"orders", "devices", "articles"}
	if len(cfg.PG.TargetTables) != len(wantTargets) {
		t.Fatalf("PG.TargetTables length = %d, want %d (%v)",
			len(cfg.PG.TargetTables), len(wantTargets), cfg.PG.TargetTables)
	}
	for i, w := range wantTargets {
		if cfg.PG.TargetTables[i] != w {
			t.Errorf("PG.TargetTables[%d] = %q, want %q", i, cfg.PG.TargetTables[i], w)
		}
	}
}

func TestLoad_RequiresPGDSN(t *testing.T) {
	defer resetEnv(t)()
	_, err := Load("", newTestFlagSet())
	if err == nil {
		t.Fatalf("Load: expected error for missing PG DSN, got nil")
	}
	if !strings.Contains(err.Error(), "pg.dsn is required") {
		t.Errorf("Load error = %q, want substring %q", err.Error(), "pg.dsn is required")
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db?sslmode=disable")
	t.Setenv("WRITER_SCENARIO_COMMIT_RATE", "42.5")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.Scenario.CommitRate, 42.5; got != want {
		t.Errorf("Scenario.CommitRate = %v, want %v", got, want)
	}
}

func TestLoad_TargetTablesCSV(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db?sslmode=disable")
	t.Setenv("WRITER_PG_TARGET_TABLES", "orders,devices")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.PG.TargetTables, []string{"orders", "devices"}; len(got) != len(want) {
		t.Fatalf("PG.TargetTables = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("PG.TargetTables[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	}
}

// TestLoad_CORSOriginsFromCommaSeparatedEnv asserts that the
// WRITER_HTTP_CORS_ORIGINS env var (a single comma-separated string) is
// coerced to []string via the env transform. Mirrors walera's
// TestLoad_CORSOriginsFromSingleStringEnv pattern. Empty entries (stray
// trailing comma, double comma) are dropped; whitespace is trimmed.
func TestLoad_CORSOriginsFromCommaSeparatedEnv(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db?sslmode=disable")
	t.Setenv("WRITER_HTTP_CORS_ORIGINS", "http://localhost:8081, http://localhost:9000 ,,")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"http://localhost:8081", "http://localhost:9000"}
	if got := cfg.HTTP.CORSOrigins; len(got) != len(want) {
		t.Fatalf("HTTP.CORSOrigins = %v, want %v", got, want)
	}
	for i := range want {
		if cfg.HTTP.CORSOrigins[i] != want[i] {
			t.Errorf("HTTP.CORSOrigins[%d] = %q, want %q", i, cfg.HTTP.CORSOrigins[i], want[i])
		}
	}
}

// TestLoad_CORSOriginsDefault asserts the default config has an empty
// allowlist (CORS disabled — current behavior preserved when the env var
// is absent).
func TestLoad_CORSOriginsDefault(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db?sslmode=disable")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.HTTP.CORSOrigins) != 0 {
		t.Errorf("HTTP.CORSOrigins = %v, want empty by default", cfg.HTTP.CORSOrigins)
	}
}

// TestLoad_DatabaseURLFallback_Resolves verifies that WALERA_DATABASE_URL
// alone (no -pg-dsn flag, no WRITER_PG_DSN) resolves pg.dsn and passes
// validation.
func TestLoad_DatabaseURLFallback_Resolves(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WALERA_DATABASE_URL", "postgres://shared:p@h/db?sslmode=disable")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.PG.DSN, "postgres://shared:p@h/db?sslmode=disable"; got != want {
		t.Errorf("PG.DSN = %q, want %q (WALERA_DATABASE_URL fallback)", got, want)
	}
}

// TestLoad_WriterPGDSNWinsOverDatabaseURL verifies WRITER_PG_DSN takes
// precedence over the WALERA_DATABASE_URL fallback.
func TestLoad_WriterPGDSNWinsOverDatabaseURL(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://writer:p@h/db")
	t.Setenv("WALERA_DATABASE_URL", "postgres://shared:p@h/db")

	cfg, err := Load("", newTestFlagSet())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.PG.DSN, "postgres://writer:p@h/db"; got != want {
		t.Errorf("PG.DSN = %q, want %q (WRITER_PG_DSN wins)", got, want)
	}
}

// TestLoad_PGDSNFlagWinsOverDatabaseURL verifies an explicitly-set -pg-dsn
// flag wins over the WALERA_DATABASE_URL fallback.
func TestLoad_PGDSNFlagWinsOverDatabaseURL(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WALERA_DATABASE_URL", "postgres://shared:p@h/db")

	fs := newTestFlagSet()
	fs.String("pg-dsn", "", "")
	if err := fs.Parse([]string{"-pg-dsn", "postgres://flag:p@h/db"}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	cfg, err := Load("", fs)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.PG.DSN, "postgres://flag:p@h/db"; got != want {
		t.Errorf("PG.DSN = %q, want %q (-pg-dsn flag wins)", got, want)
	}
}

// TestLoad_NoPGDSNSource_StillRequired verifies that with none of the three
// sources set, validation still fails with "pg.dsn is required".
func TestLoad_NoPGDSNSource_StillRequired(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WALERA_DATABASE_URL", "")

	_, err := Load("", newTestFlagSet())
	if err == nil {
		t.Fatal("Load: expected error for missing pg.dsn, got nil")
	}
	if !strings.Contains(err.Error(), "pg.dsn is required") {
		t.Errorf("Load error = %q, want substring %q", err.Error(), "pg.dsn is required")
	}
}

func TestLoad_InvalidArrivalDistribution(t *testing.T) {
	defer resetEnv(t)()
	t.Setenv("WRITER_PG_DSN", "postgres://u:p@h/db?sslmode=disable")
	t.Setenv("WRITER_ARRIVALS_DISTRIBUTION", "foo")
	_, err := Load("", newTestFlagSet())
	if err == nil {
		t.Fatalf("Load: expected error for bad arrivals.distribution, got nil")
	}
	if !strings.Contains(err.Error(), "arrivals.distribution") {
		t.Errorf("Load error = %q, want substring %q", err.Error(), "arrivals.distribution")
	}
}
