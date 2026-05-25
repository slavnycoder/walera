package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeTempYAML creates a temporary YAML config file and returns its path.
// The caller is responsible for cleanup (t.Cleanup is used).
func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writeTempYAML: %v", err)
	}
	return path
}

// setPhase3RequiredEnv sets the Phase-3 mandatory auth backend URL needed for
// Load() to pass validation. Phase-2 tests call this before Load to keep their
// focus on Phase-2 fields without re-asserting Phase-3 defaults.
func setPhase3RequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.example/test")
}

// TestLoad_YAMLFields verifies that Load() correctly reads fields from a YAML file.
func TestLoad_YAMLFields(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writeTempYAML(t, `
log:
  level: debug
database:
  url: "postgres://admin:pass@localhost/db"
wal:
  publication_name: my_publication
  slot_name_prefix: cdc
  slot_headroom_min: 5
  naive_timestamp_assume_utc: false
`)

	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.WAL.PublicationName != "my_publication" {
		t.Errorf("WAL.PublicationName = %q; want %q", cfg.WAL.PublicationName, "my_publication")
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q; want %q", cfg.Log.Level, "debug")
	}
	if cfg.WAL.SlotNamePrefix != "cdc" {
		t.Errorf("WAL.SlotNamePrefix = %q; want %q", cfg.WAL.SlotNamePrefix, "cdc")
	}
	if cfg.WAL.SlotHeadroomMin != 5 {
		t.Errorf("WAL.SlotHeadroomMin = %d; want 5", cfg.WAL.SlotHeadroomMin)
	}
	if cfg.WAL.NaiveTimestampAssumeUTC != false {
		t.Errorf("WAL.NaiveTimestampAssumeUTC = true; want false")
	}
}

// TestLoad_EnvOverride verifies that a WALERA_-prefixed env var overrides the YAML value.
func TestLoad_EnvOverride(t *testing.T) {
	setPhase3RequiredEnv(t)
	// YAML file does NOT set publication_name.
	path := writeTempYAML(t, `
database:
  url: "postgres://admin:x@localhost/db"
`)

	// Set env var override.
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "env_publication")

	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.WAL.PublicationName != "env_publication" {
		t.Errorf("WAL.PublicationName = %q; want %q", cfg.WAL.PublicationName, "env_publication")
	}
}

// TestLoad_MissingMandatoryFields verifies that Load() returns a non-nil error
// when mandatory fields are absent.
func TestLoad_MissingMandatoryFields(t *testing.T) {
	// Empty YAML file — all mandatory fields are missing.
	path := writeTempYAML(t, ``)

	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("Load() expected error for missing mandatory fields, got nil")
	}
}

// TestLoad_DatabaseURLEnvDerivesBothDSNs verifies that WALERA_DATABASE_URL
// alone (no YAML database.url) populates both the admin PostgresDSN (the base
// URL unchanged) and the derived ReplicationDSN (base + replication=database).
func TestLoad_DatabaseURLEnvDerivesBothDSNs(t *testing.T) {
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_DATABASE_URL", "postgres://admin:pass@host:5432/db?sslmode=disable")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "pub")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if string(cfg.WAL.PostgresDSN) != "postgres://admin:pass@host:5432/db?sslmode=disable" {
		t.Errorf("WAL.PostgresDSN = %q; want the base URL unchanged", cfg.WAL.PostgresDSN)
	}
	repl := string(cfg.WAL.ReplicationDSN)
	if !strings.Contains(repl, "replication=database") {
		t.Errorf("WAL.ReplicationDSN = %q; want replication=database added", repl)
	}
	if !strings.Contains(repl, "sslmode=disable") {
		t.Errorf("WAL.ReplicationDSN = %q; want sslmode=disable preserved", repl)
	}
}

// TestLoad_MissingDatabaseURL verifies that omitting both database.url and
// WALERA_DATABASE_URL fails with the "database.url is required" message.
func TestLoad_MissingDatabaseURL(t *testing.T) {
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_DATABASE_URL", "")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "pub")

	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("Load() expected error for missing database.url, got nil")
	}
	if !strings.Contains(err.Error(), "database.url is required") {
		t.Errorf("err = %v; want substring %q", err, "database.url is required")
	}
}

// SlotName is covered by internal/wal.Config.NewSlotName tests
// (internal/wal/coverage_test.go::TestConfig_NewSlotName); the config-side
// duplicate method was removed because it had no production callers.

// TestLoad_Defaults verifies that optional fields receive their documented defaults
// when neither YAML nor env provides a value.
func TestLoad_Defaults(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writeTempYAML(t, `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: pub
`)

	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.WAL.SlotNamePrefix != "walera" {
		t.Errorf("default SlotNamePrefix = %q; want %q", cfg.WAL.SlotNamePrefix, "walera")
	}
	if cfg.WAL.SlotHeadroomMin != 2 {
		t.Errorf("default SlotHeadroomMin = %d; want 2", cfg.WAL.SlotHeadroomMin)
	}
	if !cfg.WAL.NaiveTimestampAssumeUTC {
		t.Error("default NaiveTimestampAssumeUTC should be true")
	}
	if cfg.Log.Level != "info" {
		t.Errorf("default Log.Level = %q; want %q", cfg.Log.Level, "info")
	}
}

// TestLoad_DefaultsForPhase2Fields verifies that the HTTP + Router
// defaults are applied.
func TestLoad_DefaultsForPhase2Fields(t *testing.T) {
	// All three mandatory WAL fields come from env so validation passes
	// without writing a YAML file.
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "pub")
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("default Http.Addr = %q; want %q", cfg.HTTP.Addr, ":8080")
	}
	if len(cfg.HTTP.CORSOrigins) != 0 {
		t.Errorf("default Http.CORSOrigins = %v; want empty", cfg.HTTP.CORSOrigins)
	}
	if cfg.Router.ExactBuffer != 64 {
		t.Errorf("default Router.ExactBuffer = %d; want 64", cfg.Router.ExactBuffer)
	}
	if cfg.Router.WildcardBuffer != 512 {
		t.Errorf("default Router.WildcardBuffer = %d; want 512", cfg.Router.WildcardBuffer)
	}
	if cfg.Router.MaxChangesPerTx != 10000 {
		t.Errorf("default Router.MaxChangesPerTx = %d; want 10000", cfg.Router.MaxChangesPerTx)
	}
	if cfg.Router.HeartbeatInterval != 15*time.Second {
		t.Errorf("default Router.HeartbeatInterval = %s; want 15s", cfg.Router.HeartbeatInterval)
	}
}

// TestApplyDefaults_HttpWriteTimeoutAndMaxHeaderBytes verifies that the
// Phase-13 SEC-01 / F-P1-01 defaults (http.write_timeout=5s,
// http.max_header_bytes=16 KiB) land on the loaded Config when neither YAML
// nor env overrides are present, and that the env-var transform accepts the
// expected overrides.
func TestApplyDefaults_HttpWriteTimeoutAndMaxHeaderBytes(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.HTTP.WriteTimeout != 5*time.Second {
		t.Errorf("default Http.WriteTimeout = %s; want 5s", cfg.HTTP.WriteTimeout)
	}
	if cfg.HTTP.MaxHeaderBytes != 16*1024 {
		t.Errorf("default Http.MaxHeaderBytes = %d; want 16384", cfg.HTTP.MaxHeaderBytes)
	}
}

// TestEnvOverride_HttpWriteTimeoutAndMaxHeaderBytes confirms the koanf env
// transform routes WALERA_HTTP_WRITE_TIMEOUT and WALERA_HTTP_MAX_HEADER_BYTES
// onto cfg.HTTP.WriteTimeout / MaxHeaderBytes (SEC-01 / F-P1-01).
func TestEnvOverride_HttpWriteTimeoutAndMaxHeaderBytes(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_WRITE_TIMEOUT", "7s")
	t.Setenv("WALERA_HTTP_MAX_HEADER_BYTES", "32768")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.WriteTimeout != 7*time.Second {
		t.Errorf("Http.WriteTimeout = %s; want 7s (env override)", cfg.HTTP.WriteTimeout)
	}
	if cfg.HTTP.MaxHeaderBytes != 32768 {
		t.Errorf("Http.MaxHeaderBytes = %d; want 32768 (env override)", cfg.HTTP.MaxHeaderBytes)
	}
}

// TestLoad_EnvOverride_HttpAddr verifies that WALERA_HTTP_ADDR overrides the
// default ":8080".
func TestLoad_EnvOverride_HttpAddr(t *testing.T) {
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "pub")
	t.Setenv("WALERA_HTTP_ADDR", ":9090")
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.HTTP.Addr != ":9090" {
		t.Errorf("Http.Addr = %q; want %q (env override)", cfg.HTTP.Addr, ":9090")
	}
}

// TestLoad_Validate_HttpAddrEmpty verifies the validator rejects an empty
// http.addr.
func TestLoad_Validate_HttpAddrEmpty(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writeTempYAML(t, `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: pub
http:
  addr: ""
`)

	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("Load() expected error for empty http.addr, got nil")
	}
	if !strings.Contains(err.Error(), "http.addr is required") {
		t.Errorf("err = %v; want substring %q", err, "http.addr is required")
	}
}

// TestLoad_Validate_HttpAddrSchema — schema-layer host:port + port-range
// validation on http.addr.
func TestLoad_Validate_HttpAddrSchema(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		ok      bool
		wantSub string
	}{
		{name: "loopback ok", addr: "127.0.0.1:8080", ok: true},
		{name: "wildcard ok", addr: ":8080", ok: true},
		{name: "host:port ok", addr: "localhost:8080", ok: true},
		{name: "no port", addr: "no-port", ok: false, wantSub: "host:port"},
		{name: "port too large", addr: ":99999", ok: false, wantSub: "http.addr"},
		{name: "port zero", addr: ":0", ok: false, wantSub: "http.addr"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setPhase2RequiredEnv(t)
			setPhase3RequiredEnv(t)
			t.Setenv("WALERA_HTTP_ADDR", tc.addr)
			_, err := LoadAppConfig("")
			if tc.ok {
				if err != nil {
					t.Fatalf("LoadAppConfig(%q): %v", tc.addr, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("LoadAppConfig(%q) = nil err; want error", tc.addr)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q; want substring %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// --- Auth + HTTP wiring (formerly tracked separately) ---

// setPhase2RequiredEnv sets the mandatory database URL so Load passes
// validation for tests that focus only on Phase-3 keys.
func setPhase2RequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
}

func TestLoad_MinimalConfig(t *testing.T) {
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.example/test")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("LoadAppConfig minimal env returned error: %v", err)
	}
	if cfg.WAL.PublicationName != "walera_pub" {
		t.Errorf("WAL.PublicationName = %q; want default walera_pub", cfg.WAL.PublicationName)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q; want default :8080", cfg.HTTP.Addr)
	}
}

// TestConfigPhase3Defaults asserts every documented default for the
// auth, limits, and health sub-trees lands on the loaded Config.
func TestConfigPhase3Defaults(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Auth.DefaultTTLSeconds != 60 {
		t.Errorf("Auth.DefaultTTLSeconds: got %d; want 60", cfg.Auth.DefaultTTLSeconds)
	}
	if cfg.Auth.HealthChannel != "_health" {
		t.Errorf("Auth.HealthChannel: got %q; want %q", cfg.Auth.HealthChannel, "_health")
	}
	if cfg.Auth.RequestTimeout != 2*time.Second {
		t.Errorf("Auth.RequestTimeout: got %s; want 2s", cfg.Auth.RequestTimeout)
	}
	if cfg.Auth.Breaker.WindowBuckets != 30 {
		t.Errorf("Auth.Breaker.WindowBuckets: got %d; want 30", cfg.Auth.Breaker.WindowBuckets)
	}
	if cfg.Auth.Breaker.BucketSeconds != 1 {
		t.Errorf("Auth.Breaker.BucketSeconds: got %d; want 1", cfg.Auth.Breaker.BucketSeconds)
	}
	if cfg.Auth.Breaker.FailureRateThreshold != 0.5 {
		t.Errorf("Auth.Breaker.FailureRateThreshold: got %v; want 0.5", cfg.Auth.Breaker.FailureRateThreshold)
	}
	if cfg.Auth.Breaker.DebounceFloor != 20 {
		t.Errorf("Auth.Breaker.DebounceFloor: got %d; want 20", cfg.Auth.Breaker.DebounceFloor)
	}
	if cfg.Auth.Breaker.Cooldown != 30*time.Second {
		t.Errorf("Auth.Breaker.Cooldown: got %s; want 30s", cfg.Auth.Breaker.Cooldown)
	}
	if cfg.Auth.Breaker.StaleRefreshJitter != 5*time.Second {
		t.Errorf("Auth.Breaker.StaleRefreshJitter: got %s; want 5s", cfg.Auth.Breaker.StaleRefreshJitter)
	}

	if cfg.Limits.GlobalConcurrent != 50000 {
		t.Errorf("Limits.GlobalConcurrent: got %d; want 50000", cfg.Limits.GlobalConcurrent)
	}
	if cfg.Limits.PerUserConcurrentMax != 10 {
		t.Errorf("Limits.PerUserConcurrentMax: got %d; want 10", cfg.Limits.PerUserConcurrentMax)
	}
	if cfg.Limits.PerUserRatePerSecond != 5.0 {
		t.Errorf("Limits.PerUserRatePerSecond: got %v; want 5.0", cfg.Limits.PerUserRatePerSecond)
	}
	if cfg.Limits.PerUserBurst != 10 {
		t.Errorf("Limits.PerUserBurst: got %d; want 10", cfg.Limits.PerUserBurst)
	}
	if cfg.Limits.PreAuthRatePerSecond != 5.0 {
		t.Errorf("Limits.PreAuthRatePerSecond: got %v; want 5.0", cfg.Limits.PreAuthRatePerSecond)
	}
	if cfg.Limits.PreAuthBurst != 10 {
		t.Errorf("Limits.PreAuthBurst: got %d; want 10", cfg.Limits.PreAuthBurst)
	}
	if cfg.Limits.SweepInterval != 60*time.Second {
		t.Errorf("Limits.SweepInterval: got %s; want 60s", cfg.Limits.SweepInterval)
	}
	if cfg.Limits.SweepIdleThreshold != 5*time.Minute {
		t.Errorf("Limits.SweepIdleThreshold: got %s; want 5m", cfg.Limits.SweepIdleThreshold)
	}

	if cfg.Health.ReadyzProbeInterval != 5*time.Second {
		t.Errorf("Health.ReadyzProbeInterval: got %s; want 5s", cfg.Health.ReadyzProbeInterval)
	}
}

// TestConfigPhase3EnvOverrides asserts env-var overrides reach the unmarshalled
// Config for backend_url and a representative limits int.
func TestConfigPhase3EnvOverrides(t *testing.T) {
	setPhase2RequiredEnv(t)
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.test")
	t.Setenv("WALERA_LIMITS_GLOBAL_CONCURRENT", "100")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Auth.BackendURL != "https://auth.test" {
		t.Errorf("Auth.BackendURL: got %q; want %q", cfg.Auth.BackendURL, "https://auth.test")
	}
	if cfg.Limits.GlobalConcurrent != 100 {
		t.Errorf("Limits.GlobalConcurrent: got %d; want 100", cfg.Limits.GlobalConcurrent)
	}
}

// TestLoad_H2CEnabledDefaultsTrue verifies that Http.H2CEnabled defaults to
// true when neither YAML nor env overrides it.
func TestLoad_H2CEnabledDefaultsTrue(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if !cfg.HTTP.H2CEnabled {
		t.Errorf("default Http.H2CEnabled = false; want true")
	}
}

// TestLoad_H2CEnabledEnvOverride verifies that WALERA_HTTP_H2C_ENABLED=false
// flips Http.H2CEnabled to false.
func TestLoad_H2CEnabledEnvOverride(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_H2C_ENABLED", "false")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.H2CEnabled {
		t.Errorf("Http.H2CEnabled = true; want false (env override)")
	}
}

// TestLoad_CORSOriginsFromSingleStringEnv locks the contract that
// WALERA_HTTP_CORS_ORIGINS with a single value (no comma)
// unmarshals into a one-element []string{"http://localhost:8081"} (WALERA-02).
func TestLoad_CORSOriginsFromSingleStringEnv(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_CORS_ORIGINS", "http://localhost:8081")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	want := []string{"http://localhost:8081"}
	if len(cfg.HTTP.CORSOrigins) != len(want) || cfg.HTTP.CORSOrigins[0] != want[0] {
		t.Errorf("Http.CORSOrigins = %v; want %v", cfg.HTTP.CORSOrigins, want)
	}
}

// TestLoad_CORSOriginsFromCommaSeparatedEnv verifies that
// WALERA_HTTP_CORS_ORIGINS with comma-separated values unmarshals into a
// multi-element []string (WALERA-02). The envTransform splits on ',' and
// trims whitespace.
func TestLoad_CORSOriginsFromCommaSeparatedEnv(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_CORS_ORIGINS", "http://localhost:8081,http://localhost:8082")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	want := []string{"http://localhost:8081", "http://localhost:8082"}
	if len(cfg.HTTP.CORSOrigins) != len(want) {
		t.Fatalf("Http.CORSOrigins length = %d; want %d (got %v)", len(cfg.HTTP.CORSOrigins), len(want), cfg.HTTP.CORSOrigins)
	}
	for i := range want {
		if cfg.HTTP.CORSOrigins[i] != want[i] {
			t.Errorf("Http.CORSOrigins[%d] = %q; want %q", i, cfg.HTTP.CORSOrigins[i], want[i])
		}
	}
}

// TestLoad_AppliesPhase4Defaults verifies every reconnect/lag/shutdown default value lands on
// the loaded Config when no YAML or env override is present.
func TestLoad_AppliesPhase4Defaults(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"wal.reconnect.reset_after_success_duration", cfg.WAL.Reconnect.ResetAfterSuccessDuration, 60 * time.Second},
		{"wal.lag_sample_interval", cfg.WAL.LagSampleInterval, 5 * time.Second},
		{"metrics.sample_interval", cfg.Metrics.SampleInterval, 30 * time.Second},
		{"shutdown.deadline", cfg.Shutdown.Deadline.Duration(), 10 * time.Second},
		{"shutdown.drain_deadline", cfg.Shutdown.DrainDeadline.Duration(), 8 * time.Second},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("%s: got %v; want %v", tc.name, tc.got, tc.want)
		}
	}
}

// TestValidate_RejectsInvalidPhase4Values exercises the validator with several
// invalid combinations. Each row sets one bad YAML value and asserts that
// Load() returns a non-nil error containing the expected substring.
//
// Policy on drain_deadline > deadline: the validator REJECTS (error, not
// warning) because the broadcaster.Shutdown phase MUST finish before the hard
// cap fires; allowing drain_deadline > deadline would silently violate the
// shutdown ordering contract.
func TestValidate_RejectsInvalidPhase4Values(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantInErr string
	}{
		{
			name: "metrics.sample_interval zero",
			yaml: `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: p
metrics:
  sample_interval: 0s
`,
			wantInErr: "metrics.sample_interval must be > 0",
		},
		{
			name: "drain_deadline > deadline",
			yaml: `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: p
shutdown:
  deadline: 5s
  drain_deadline: 10s
`,
			wantInErr: "shutdown.drain_deadline must be <= shutdown.deadline",
		},
		{
			name: "wal.lag_sample_interval zero",
			yaml: `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: p
  lag_sample_interval: 0s
`,
			wantInErr: "wal.lag_sample_interval must be > 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
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

// --- SEC-03 (PG identifier validation) ---

// writePhase14SEC03YAML produces a minimal valid YAML body except for the
// publication_name and slot_name_prefix fields, which the caller overrides.
func writePhase14SEC03YAML(t *testing.T, pubName, slotPrefix string) string {
	t.Helper()
	body := "database:\n" +
		"  url: \"postgres://a:b@localhost/db\"\n" +
		"wal:\n" +
		"  publication_name: " + pubName + "\n"
	if slotPrefix != "" {
		body += "  slot_name_prefix: " + slotPrefix + "\n"
	}
	return writeTempYAML(t, body)
}

func TestLoad_SEC03_PgIdent_Accepts(t *testing.T) {
	tests := []struct {
		name        string
		publication string
		slotPrefix  string
	}{
		{"simple", "my_pub", ""},
		{"leading_underscore", "_underscore_first", ""},
		{"mixed_case", "Mixed_Case", ""},
		{"63_chars", strings.Repeat("a", 63), ""},
		{"slot_walera_default", "pub_ok", "walera"},
		{"slot_with_digits", "pub_ok", "cdc1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
			path := writePhase14SEC03YAML(t, tc.publication, tc.slotPrefix)
			if _, err := LoadAppConfig(path); err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}
		})
	}
}

func TestLoad_SEC03_PgIdent_RejectsHyphen(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC03YAML(t, "pub-name", "")
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "must match PG identifier regex") {
		t.Errorf("err = %v; want substring %q", err, "must match PG identifier regex")
	}
}

func TestLoad_SEC03_PgIdent_RejectsInjection(t *testing.T) {
	setPhase3RequiredEnv(t)
	// Pass via env so we don't fight YAML quoting on the apostrophe.
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "pub'); DROP")
	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "must match PG identifier regex") {
		t.Errorf("err = %v; want substring %q", err, "must match PG identifier regex")
	}
}

func TestLoad_SEC03_PgIdent_RejectsLongOver63(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC03YAML(t, strings.Repeat("a", 64), "")
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "must match PG identifier regex") {
		t.Errorf("err = %v; want substring %q", err, "must match PG identifier regex")
	}
}

func TestLoad_SEC03_PgIdent_RejectsStartsWithDigit(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC03YAML(t, "1pub", "")
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "must match PG identifier regex") {
		t.Errorf("err = %v; want substring %q", err, "must match PG identifier regex")
	}
}

func TestLoad_SEC03_SlotNamePrefix_RejectsHyphen(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC03YAML(t, "pub_ok", "cdc-streamer")
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "wal.slot_name_prefix") || !strings.Contains(err.Error(), "PG identifier regex") {
		t.Errorf("err = %v; want substring %q + %q", err, "wal.slot_name_prefix", "PG identifier regex")
	}
}

// TestLoad_PublicationName_DefaultsWhenEmpty — quick task 260518-j8x.
// Replaces the prior TestLoad_SEC03_EmptyPublicationName_Required test.
// Before the auto-bootstrap change, an explicitly-empty
// WALERA_WAL_PUBLICATION_NAME produced "wal.publication_name is required".
// After the change the default "walera_pub" applies and Load succeeds:
// koanf treats an empty env override as unset, so the default in
// applyDefaults takes effect.
func TestLoad_PublicationName_DefaultsWhenEmpty(t *testing.T) {
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "")
	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned unexpected error: %v", err)
	}
	if cfg.WAL.PublicationName != "walera_pub" {
		t.Errorf("WAL.PublicationName = %q; want %q", cfg.WAL.PublicationName, "walera_pub")
	}
}

// --- SEC-04 (https scheme enforcement) ---

func TestLoad_SEC04_HttpsAccepted(t *testing.T) {
	setPhase2RequiredEnv(t)
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.example/test")
	if _, err := LoadAppConfig(""); err != nil {
		t.Fatalf("Load() with https returned error: %v", err)
	}
}

func TestLoad_SEC04_HttpsRequired_NoOverride(t *testing.T) {
	setPhase2RequiredEnv(t)
	t.Setenv("WALERA_AUTH_BACKEND_URL", "http://auth.example/test")
	t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("want error for http://, got nil")
	}
	if !strings.Contains(err.Error(), "must use https") {
		t.Errorf("err missing 'must use https': %v", err)
	}
	if !strings.Contains(err.Error(), "WALERA_AUTH_ALLOW_PLAINTEXT=1") {
		t.Errorf("err missing 'WALERA_AUTH_ALLOW_PLAINTEXT=1' literal: %v", err)
	}
}

func TestLoad_SEC04_HttpsRequired_OverrideAccepts(t *testing.T) {
	setPhase2RequiredEnv(t)
	t.Setenv("WALERA_AUTH_BACKEND_URL", "http://auth.example/test")
	t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "1")
	if _, err := LoadAppConfig(""); err != nil {
		t.Fatalf("Load() with override returned error: %v", err)
	}
}

func TestLoad_SEC04_HttpsRequired_OverrideExactlyOne(t *testing.T) {
	cases := []string{"0", "true", "yes", "", " 1", "1 "}
	for _, v := range cases {
		t.Run("override="+v, func(t *testing.T) {
			setPhase2RequiredEnv(t)
			t.Setenv("WALERA_AUTH_BACKEND_URL", "http://auth.example/test")
			t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", v)
			_, err := LoadAppConfig("")
			if err == nil {
				t.Fatalf("want error for override=%q, got nil", v)
			}
			if !strings.Contains(err.Error(), "must use https") {
				t.Errorf("err missing 'must use https': %v", err)
			}
		})
	}
}

func TestLoad_SEC04_MalformedURL_TreatedAsNonHttps(t *testing.T) {
	setPhase2RequiredEnv(t)
	t.Setenv("WALERA_AUTH_BACKEND_URL", "not a url")
	t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("want error for malformed URL, got nil")
	}
	// Schema validation now surfaces one of: missing host, bad scheme, or
	// URL parse failure. Any of those is a valid "URL is rejected" signal.
	msg := err.Error()
	if !strings.Contains(msg, "auth.backend_url") {
		t.Errorf("err missing 'auth.backend_url' reference: %v", err)
	}
}

// TestLoad_SEC04_ControlCharURL_TreatedAsNonHttps —
// control-character regression. A URL containing an ASCII control
// character is one of the only inputs for which url.Parse returns
// a non-nil error. The current implementation handles this via the
// (u == nil) branch — the same "must use https" message is
// surfaced. This test pins that behaviour so a future change
// cannot reintroduce a separate error path that only fires for
// control chars (the original code's dead branch).
func TestLoad_SEC04_ControlCharURL_TreatedAsNonHttps(t *testing.T) {
	setPhase2RequiredEnv(t)
	// \x7f (DEL) is a control character that causes url.Parse to error.
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://example.com/\x7f")
	t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("want error for control-char URL, got nil")
	}
	// Schema validation surfaces a URL-parse error for the control char.
	if !strings.Contains(err.Error(), "auth.backend_url") {
		t.Errorf("err missing 'auth.backend_url' reference: %v", err)
	}
}

// --- SEC-05 (limits.trusted_proxies CIDR validation) ---

func writePhase14SEC05YAML(t *testing.T, proxies []string) string {
	t.Helper()
	body := "database:\n" +
		"  url: \"postgres://a:b@localhost/db\"\n" +
		"wal:\n" +
		"  publication_name: pub_ok\n" +
		"limits:\n" +
		"  trusted_proxies:\n"
	for _, p := range proxies {
		body += "    - \"" + p + "\"\n"
	}
	return writeTempYAML(t, body)
}

func TestLoad_SEC05_TrustedProxies_AcceptsValidCIDRs(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC05YAML(t, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"})
	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	want := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	if len(cfg.Limits.TrustedProxies) != len(want) {
		t.Fatalf("TrustedProxies len = %d (%v); want %d (%v)",
			len(cfg.Limits.TrustedProxies), cfg.Limits.TrustedProxies, len(want), want)
	}
	for i := range want {
		if cfg.Limits.TrustedProxies[i] != want[i] {
			t.Errorf("TrustedProxies[%d] = %q; want %q", i, cfg.Limits.TrustedProxies[i], want[i])
		}
	}
}

func TestLoad_SEC05_TrustedProxies_RejectsMalformed(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC05YAML(t, []string{"10.0.0.0"})
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error for missing prefix, got nil")
	}
	if !strings.Contains(err.Error(), "is not a valid CIDR") {
		t.Errorf("err missing 'is not a valid CIDR': %v", err)
	}
}

func TestLoad_SEC05_TrustedProxies_RejectsMixed(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC05YAML(t, []string{"10.0.0.0/8", "garbage"})
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error for mixed-validity list, got nil")
	}
	if !strings.Contains(err.Error(), "limits.trusted_proxies[1]") {
		t.Errorf("err missing index [1]: %v", err)
	}
}

func TestLoad_SEC05_TrustedProxies_EmptyDefault(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if len(cfg.Limits.TrustedProxies) != 0 {
		t.Errorf("default TrustedProxies = %v; want empty", cfg.Limits.TrustedProxies)
	}
}

// --- SEC-09 (CORS canonicalisation) ---

func writePhase14SEC09YAML(t *testing.T, origins []string) string {
	t.Helper()
	body := "database:\n" +
		"  url: \"postgres://a:b@localhost/db\"\n" +
		"wal:\n" +
		"  publication_name: pub_ok\n" +
		"http:\n" +
		"  cors_origins:\n"
	for _, o := range origins {
		body += "    - \"" + o + "\"\n"
	}
	return writeTempYAML(t, body)
}

func TestLoad_SEC09_CORSOrigins_Canonicalised(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"uppercase_host_trailing_slash", "https://EXAMPLE.com/", "https://example.com"},
		{"explicit_port_preserved", "https://example.com:8080", "https://example.com:8080"},
		{"mixed_case_host", "https://Example.COM", "https://example.com"},
		{"localhost_port", "http://localhost:8081", "http://localhost:8081"},
		{"path_stripped", "https://example.com/path", "https://example.com"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
			path := writePhase14SEC09YAML(t, []string{tc.in})
			cfg, err := LoadAppConfig(path)
			if err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}
			if len(cfg.HTTP.CORSOrigins) != 1 || cfg.HTTP.CORSOrigins[0] != tc.want {
				t.Errorf("CORSOrigins = %v; want [%q]", cfg.HTTP.CORSOrigins, tc.want)
			}
		})
	}
}

func TestLoad_SEC09_CORSOrigins_RejectsMalformed(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC09YAML(t, []string{"not a url"})
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error for malformed URL, got nil")
	}
	if !strings.Contains(err.Error(), "is not a valid URL") {
		t.Errorf("err missing 'is not a valid URL': %v", err)
	}
}

func TestLoad_SEC09_CORSOrigins_RejectsEmptyHost(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC09YAML(t, []string{"http:///path-only"})
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error for empty host, got nil")
	}
}

func TestLoad_SEC09_CORSOrigins_RejectsEmpty(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writePhase14SEC09YAML(t, []string{""})
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("want error for empty origin, got nil")
	}
}

func TestLoad_SEC09_CORSOrigins_Empty_Passes(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if len(cfg.HTTP.CORSOrigins) != 0 {
		t.Errorf("default CORSOrigins = %v; want empty", cfg.HTTP.CORSOrigins)
	}
}

// --- Named regression anchors (REQUIREMENTS.md TEST-09) ---

// Test_SEC03_BadPublicationName_FailsStartup — REQUIREMENTS.md TEST-09.3.
// Canonical-name regression anchor for SEC-03 / F-P1-03. The full charset
// of edge cases (long-over-63, starts-with-digit, control-char, empty,
// slot-name-prefix variants) is already exercised by TestLoad_SEC03_PgIdent_*
// above (config_test.go:653-757); this test locks the named-regression
// invariant in CI logs so a future refactor produces an
// instantly-diagnosable failure mapping 1-to-1 to the audit finding ID.
//
// No t.Parallel(): every subtest uses t.Setenv (mirrors RESEARCH §note 4).
func Test_SEC03_BadPublicationName_FailsStartup(t *testing.T) {
	t.Run("hyphen_rejected", func(t *testing.T) {
		setPhase3RequiredEnv(t)
		path := writePhase14SEC03YAML(t, "pub-name", "")
		_, err := LoadAppConfig(path)
		if err == nil {
			t.Fatal("Load() expected error for 'pub-name' (hyphen), got nil")
		}
		if !strings.Contains(err.Error(), "wal.publication_name") {
			t.Errorf("err = %v; want substring 'wal.publication_name'", err)
		}
		if !strings.Contains(err.Error(), "PG identifier regex") {
			t.Errorf("err = %v; want substring 'PG identifier regex'", err)
		}
	})

	t.Run("quote_injection_rejected", func(t *testing.T) {
		setPhase3RequiredEnv(t)
		// Pass via env so we don't fight YAML quoting on the apostrophe;
		// mirrors TestLoad_SEC03_PgIdent_RejectsInjection at
		// config_test.go:689-702.
		t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
		t.Setenv("WALERA_WAL_PUBLICATION_NAME", "pub'); DROP")
		_, err := LoadAppConfig("")
		if err == nil {
			t.Fatal("Load() expected error for 'pub'); DROP' (quote injection), got nil")
		}
		if !strings.Contains(err.Error(), "wal.publication_name") {
			t.Errorf("err = %v; want substring 'wal.publication_name'", err)
		}
	})

	t.Run("valid_baseline_accepted", func(t *testing.T) {
		setPhase3RequiredEnv(t)
		path := writePhase14SEC03YAML(t, "valid_underscored_name123", "")
		if _, err := LoadAppConfig(path); err != nil {
			t.Fatalf("Load() returned unexpected error for valid name: %v", err)
		}
	})
}

// Test_SEC04_HttpAuthBackend_FailsStartup — REQUIREMENTS.md TEST-09.4.
// Canonical-name regression anchor for SEC-04 / F-P1-04. Edge cases
// (malformed URL, control-char URL, exact-override-value matching) are
// already exercised by TestLoad_SEC04_* above (config_test.go:761-850);
// this test locks the named-regression invariant in CI logs.
//
// No t.Parallel(): every subtest uses t.Setenv (mirrors RESEARCH §note 4).
func Test_SEC04_HttpAuthBackend_FailsStartup(t *testing.T) {
	t.Run("http_rejected_without_override", func(t *testing.T) {
		setPhase2RequiredEnv(t)
		t.Setenv("WALERA_AUTH_BACKEND_URL", "http://auth.local")
		// Explicit empty — clears any inherited override from the parent
		// process env so the test is hermetic. Mirrors
		// TestLoad_SEC04_HttpsRequired_NoOverride at config_test.go:774.
		t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
		_, err := LoadAppConfig("")
		if err == nil {
			t.Fatal("Load() expected error for http:// without override, got nil")
		}
		if !strings.Contains(err.Error(), "auth.backend_url") {
			t.Errorf("err = %v; want substring 'auth.backend_url'", err)
		}
		// The error MUST name the env-var literally so a confused
		// operator can find the override without grepping (defence
		// rationale at config.go:461-463).
		if !strings.Contains(err.Error(), "WALERA_AUTH_ALLOW_PLAINTEXT=1") {
			t.Errorf("err = %v; want substring 'WALERA_AUTH_ALLOW_PLAINTEXT=1'", err)
		}
	})

	t.Run("http_accepted_with_override", func(t *testing.T) {
		setPhase2RequiredEnv(t)
		t.Setenv("WALERA_AUTH_BACKEND_URL", "http://auth.local")
		t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "1")
		if _, err := LoadAppConfig(""); err != nil {
			t.Fatalf("Load() with override returned unexpected error: %v", err)
		}
	})

	t.Run("https_baseline_accepted", func(t *testing.T) {
		setPhase2RequiredEnv(t)
		t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.local")
		// Even with the override unset/empty, https:// is the valid
		// baseline.
		t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
		if _, err := LoadAppConfig(""); err != nil {
			t.Fatalf("Load() with https baseline returned unexpected error: %v", err)
		}
	})
}

// --- Quick task 260518-lh1 (pprof opt-in listener) ---

// TestLoad_PProfAddr_DefaultEmpty — quick task 260518-lh1. With neither YAML
// nor env supplying http.pprof_addr the loaded Config carries "" (disabled).
func TestLoad_PProfAddr_DefaultEmpty(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	// Defensive: clear any inherited override so the test is hermetic.
	t.Setenv("WALERA_PPROF_ALLOW_PUBLIC", "")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.PProfAddr != "" {
		t.Errorf("default Http.PProfAddr = %q; want empty (disabled)", cfg.HTTP.PProfAddr)
	}
}

// TestLoad_PProfAddr_Loopback_Accepts — quick task 260518-lh1. Each of the
// three loopback forms (127.0.0.1, ::1, localhost) is accepted by validate
// without the WALERA_PPROF_ALLOW_PUBLIC escape hatch.
func TestLoad_PProfAddr_Loopback_Accepts(t *testing.T) {
	cases := []string{
		"127.0.0.1:6060",
		"[::1]:6060",
		"localhost:6060",
		"LocalHost:6060", // case-insensitive
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			setPhase2RequiredEnv(t)
			setPhase3RequiredEnv(t)
			t.Setenv("WALERA_PPROF_ALLOW_PUBLIC", "")
			t.Setenv("WALERA_HTTP_PPROF_ADDR", addr)

			cfg, err := LoadAppConfig("")
			if err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}
			if cfg.HTTP.PProfAddr != addr {
				t.Errorf("Http.PProfAddr = %q; want %q", cfg.HTTP.PProfAddr, addr)
			}
		})
	}
}

// TestLoad_PProfAddr_NonLoopback_RejectedWithoutOverride — quick task
// 260518-lh1 / T-LH1-01. A non-loopback bind without
// WALERA_PPROF_ALLOW_PUBLIC=1 is rejected at validate() with an error that
// names the env var literally so a confused operator can grep for it.
func TestLoad_PProfAddr_NonLoopback_RejectedWithoutOverride(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_PPROF_ALLOW_PUBLIC", "")
	t.Setenv("WALERA_HTTP_PPROF_ADDR", "0.0.0.0:6060")

	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("Load() expected error for non-loopback pprof_addr, got nil")
	}
	if !strings.Contains(err.Error(), "http.pprof_addr") {
		t.Errorf("err = %v; want substring 'http.pprof_addr'", err)
	}
	if !strings.Contains(err.Error(), "WALERA_PPROF_ALLOW_PUBLIC=1") {
		t.Errorf("err = %v; want substring 'WALERA_PPROF_ALLOW_PUBLIC=1' (literal env name)", err)
	}
}

// TestLoad_PProfAddr_NonLoopback_AcceptedWithOverride — quick task 260518-lh1.
// Setting WALERA_PPROF_ALLOW_PUBLIC="1" unlocks the non-loopback bind.
func TestLoad_PProfAddr_NonLoopback_AcceptedWithOverride(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_PPROF_ALLOW_PUBLIC", "1")
	t.Setenv("WALERA_HTTP_PPROF_ADDR", "0.0.0.0:6060")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.PProfAddr != "0.0.0.0:6060" {
		t.Errorf("Http.PProfAddr = %q; want %q (with override)", cfg.HTTP.PProfAddr, "0.0.0.0:6060")
	}
}

// TestLoad_PProfAddr_MalformedRejected — quick task 260518-lh1. A value that
// cannot be split by net.SplitHostPort produces a validation error.
func TestLoad_PProfAddr_MalformedRejected(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_PPROF_ALLOW_PUBLIC", "")
	t.Setenv("WALERA_HTTP_PPROF_ADDR", "bad-no-port")

	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("Load() expected error for malformed pprof_addr, got nil")
	}
	if !strings.Contains(err.Error(), "http.pprof_addr") {
		t.Errorf("err = %v; want substring 'http.pprof_addr'", err)
	}
}

// --- Quick task 260518-j8x (auto-bootstrap publication) ---

// TestLoad_BootstrapAndPublicationDefaults verifies the two new defaults land
// on the loaded Config when neither YAML nor env supplies a value:
//
//   - cfg.WAL.PublicationName defaults to "walera_pub"
//   - cfg.WAL.Bootstrap.Mode defaults to "auto"
//
// To exercise the publication_name default we deliberately skip
// setPhase2RequiredEnv (which sets WALERA_WAL_PUBLICATION_NAME=pub) and set
// only the DSN env vars explicitly so the default applies.
func TestLoad_BootstrapAndPublicationDefaults(t *testing.T) {
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.WAL.PublicationName != "walera_pub" {
		t.Errorf("default WAL.PublicationName = %q; want %q", cfg.WAL.PublicationName, "walera_pub")
	}
	if cfg.WAL.Bootstrap.Mode != "auto" {
		t.Errorf("default WAL.Bootstrap.Mode = %q; want %q", cfg.WAL.Bootstrap.Mode, "auto")
	}
}

// TestLoad_BootstrapModeFromYAML — table test verifying each allowed mode
// round-trips from a YAML file to cfg.WAL.Bootstrap.Mode.
func TestLoad_BootstrapModeFromYAML(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{"auto", "auto"},
		{"verify", "verify"},
		{"off", "off"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
			yaml := "database:\n" +
				"  url: \"postgres://a:b@localhost/db\"\n" +
				"wal:\n" +
				"  publication_name: pub_ok\n" +
				"  bootstrap:\n" +
				"    mode: " + tc.mode + "\n"
			path := writeTempYAML(t, yaml)
			cfg, err := LoadAppConfig(path)
			if err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}
			if cfg.WAL.Bootstrap.Mode != tc.mode {
				t.Errorf("WAL.Bootstrap.Mode = %q; want %q", cfg.WAL.Bootstrap.Mode, tc.mode)
			}
		})
	}
}

// TestLoad_BootstrapModeFromEnv — single-value env override (no commas, uses
// the standard koanf transform path) lands on cfg.WAL.Bootstrap.Mode.
func TestLoad_BootstrapModeFromEnv(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_WAL_BOOTSTRAP_MODE", "verify")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.WAL.Bootstrap.Mode != "verify" {
		t.Errorf("WAL.Bootstrap.Mode = %q; want %q (env override)", cfg.WAL.Bootstrap.Mode, "verify")
	}
}

// TestLoad_Validate_BootstrapModeInvalid — unknown modes are rejected at
// Load time with a clear error naming the offending value and the allowed
// set.
func TestLoad_Validate_BootstrapModeInvalid(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writeTempYAML(t, `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: pub_ok
  bootstrap:
    mode: bogus
`)
	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("Load() expected error for bootstrap.mode=bogus, got nil")
	}
	if !strings.Contains(err.Error(), "wal.bootstrap.mode") || !strings.Contains(err.Error(), "bogus") {
		t.Errorf("err = %v; want substring %q", err, `wal.bootstrap.mode "bogus" is invalid`)
	}
}

// --- bootstrap-v2: tables list + create_roles knobs ---

// TestLoad_BootstrapV2_Defaults — the new knobs default to an empty table
// list and create_roles=false so legacy FOR-ALL-TABLES auto behaviour is
// preserved when neither YAML nor env supplies a value.
func TestLoad_BootstrapV2_Defaults(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if len(cfg.WAL.Bootstrap.Tables) != 0 {
		t.Errorf("default WAL.Bootstrap.Tables = %v; want empty", cfg.WAL.Bootstrap.Tables)
	}
	if cfg.WAL.Bootstrap.CreateRoles {
		t.Errorf("default WAL.Bootstrap.CreateRoles = true; want false")
	}
}

// TestLoad_BootstrapV2_TablesFromYAML — explicit schema-qualified entries
// in YAML land on cfg.WAL.Bootstrap.Tables in order.
func TestLoad_BootstrapV2_TablesFromYAML(t *testing.T) {
	setPhase3RequiredEnv(t)
	path := writeTempYAML(t, `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: pub_ok
  bootstrap:
    mode: auto
    tables:
      - public.orders
      - public.devices
    create_roles: true
`)
	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	want := []string{"public.orders", "public.devices"}
	if len(cfg.WAL.Bootstrap.Tables) != len(want) {
		t.Fatalf("WAL.Bootstrap.Tables = %v; want %v", cfg.WAL.Bootstrap.Tables, want)
	}
	for i, w := range want {
		if cfg.WAL.Bootstrap.Tables[i] != w {
			t.Errorf("WAL.Bootstrap.Tables[%d] = %q; want %q", i, cfg.WAL.Bootstrap.Tables[i], w)
		}
	}
	if !cfg.WAL.Bootstrap.CreateRoles {
		t.Errorf("WAL.Bootstrap.CreateRoles = false; want true")
	}
}

// TestLoad_BootstrapV2_TablesFromEnv — comma-separated env override is split
// into a string slice (mirrors the cors_origins WALERA-02 pattern).
func TestLoad_BootstrapV2_TablesFromEnv(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_WAL_BOOTSTRAP_TABLES", "public.orders, public.devices,  app.users")
	t.Setenv("WALERA_WAL_BOOTSTRAP_CREATE_ROLES", "true")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	want := []string{"public.orders", "public.devices", "app.users"}
	if len(cfg.WAL.Bootstrap.Tables) != len(want) {
		t.Fatalf("WAL.Bootstrap.Tables = %v; want %v", cfg.WAL.Bootstrap.Tables, want)
	}
	for i, w := range want {
		if cfg.WAL.Bootstrap.Tables[i] != w {
			t.Errorf("WAL.Bootstrap.Tables[%d] = %q; want %q", i, cfg.WAL.Bootstrap.Tables[i], w)
		}
	}
	if !cfg.WAL.Bootstrap.CreateRoles {
		t.Errorf("WAL.Bootstrap.CreateRoles = false; want true (env)")
	}
}

// TestLoad_BootstrapV2_TableValidation — entries that lack a schema prefix
// (or whose schema/table segment is not a valid PG identifier) are rejected
// at Load with a message naming the offending index and value.
func TestLoad_BootstrapV2_TableValidation(t *testing.T) {
	tests := []struct {
		name    string
		entry   string
		wantSub string
	}{
		{"missing_schema", "orders", `wal.bootstrap.tables[0] (orders)`},
		{"trailing_dot", "public.", `wal.bootstrap.tables[0] (public.)`},
		{"hyphen_in_name", "public.my-table", `wal.bootstrap.tables[0] (public.my-table)`},
		{"three_dots", "a.b.c", `wal.bootstrap.tables[0] (a.b.c)`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
			path := writeTempYAML(t, `
database:
  url: "postgres://a:b@localhost/db"
wal:
  publication_name: pub_ok
  bootstrap:
    mode: auto
    tables:
      - `+tc.entry+`
`)
			_, err := LoadAppConfig(path)
			if err == nil {
				t.Fatalf("Load() expected error for entry %q, got nil", tc.entry)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %v; want substring %q", err, tc.wantSub)
			}
		})
	}
}

// --- SSE writer-pool config knobs ---
//
// Coverage requirements:
//   - Default-value test for each of the 6 knobs.
//   - Env-override happy-path test for each knob (BatchingDisabled tested
//     with both true and false).
//   - Invalid-value test for each of the 5 numeric range-checked knobs,
//     asserting Load errors AND the error message names the koanf key.
//   - Explicit sanity test that max_wait_ms=0 is ACCEPTED (the lower bound
//     is `>= 0`, not `>= 1`; zero is the tightest possible lag ceiling).
//
// Tests use the existing setPhase2RequiredEnv + setPhase3RequiredEnv helpers
// instead of introducing a new mustLoadMinimal helper, mirroring the pattern
// established by the SEC-01 / SEC-04 tests above.

// TestLoad_HttpPoolKnobs_Defaults asserts every pool-tuning default value
// lands on the loaded Config when neither YAML nor env overrides it
// (CONTEXT.md §"Config knobs").
func TestLoad_HttpPoolKnobs_Defaults(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.HTTP.PoolFactor != 2 {
		t.Errorf("default Http.PoolFactor = %d; want 2", cfg.HTTP.PoolFactor)
	}
	if cfg.HTTP.SubQueueSize != 32 {
		t.Errorf("default Http.SubQueueSize = %d; want 32", cfg.HTTP.SubQueueSize)
	}
	if cfg.HTTP.MaxWaitMs != 2 {
		t.Errorf("default Http.MaxWaitMs = %d; want 2", cfg.HTTP.MaxWaitMs)
	}
	if cfg.HTTP.DrainThresholdSubs != 0 {
		t.Errorf("default Http.DrainThresholdSubs = %d; want 0 (sentinel = use formula)", cfg.HTTP.DrainThresholdSubs)
	}
	if cfg.HTTP.MaxBatchBytesPerSub != 65536 {
		t.Errorf("default Http.MaxBatchBytesPerSub = %d; want 65536", cfg.HTTP.MaxBatchBytesPerSub)
	}
	if cfg.HTTP.BatchingDisabled {
		t.Errorf("default Http.BatchingDisabled = true; want false")
	}
}

// TestLoad_HttpPoolKnobs_EnvOverride_pool_factor asserts
// WALERA_HTTP_POOL_FACTOR overrides the default.
func TestLoad_HttpPoolKnobs_EnvOverride_pool_factor(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_POOL_FACTOR", "4")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.PoolFactor != 4 {
		t.Errorf("Http.PoolFactor = %d; want 4 (env override)", cfg.HTTP.PoolFactor)
	}
}

// TestLoad_HttpPoolKnobs_EnvOverride_sub_queue_size asserts
// WALERA_HTTP_SUB_QUEUE_SIZE overrides the default.
func TestLoad_HttpPoolKnobs_EnvOverride_sub_queue_size(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_SUB_QUEUE_SIZE", "128")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.SubQueueSize != 128 {
		t.Errorf("Http.SubQueueSize = %d; want 128 (env override)", cfg.HTTP.SubQueueSize)
	}
}

// TestLoad_HttpPoolKnobs_EnvOverride_max_wait_ms asserts
// WALERA_HTTP_MAX_WAIT_MS overrides the default.
func TestLoad_HttpPoolKnobs_EnvOverride_max_wait_ms(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_MAX_WAIT_MS", "5")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.MaxWaitMs != 5 {
		t.Errorf("Http.MaxWaitMs = %d; want 5 (env override)", cfg.HTTP.MaxWaitMs)
	}
}

// TestLoad_HttpPoolKnobs_max_wait_ms_zero_accepted locks the lower-bound
// invariant: max_wait_ms=0 IS valid (tightest lag ceiling), distinct from
// batching_disabled=true (which disables batching entirely).
func TestLoad_HttpPoolKnobs_max_wait_ms_zero_accepted(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_MAX_WAIT_MS", "0")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error for max_wait_ms=0; expected accepted: %v", err)
	}
	if cfg.HTTP.MaxWaitMs != 0 {
		t.Errorf("Http.MaxWaitMs = %d; want 0", cfg.HTTP.MaxWaitMs)
	}
}

// TestLoad_HttpPoolKnobs_EnvOverride_drain_threshold_subs asserts
// WALERA_HTTP_DRAIN_THRESHOLD_SUBS overrides the default.
func TestLoad_HttpPoolKnobs_EnvOverride_drain_threshold_subs(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_DRAIN_THRESHOLD_SUBS", "16")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.DrainThresholdSubs != 16 {
		t.Errorf("Http.DrainThresholdSubs = %d; want 16 (env override)", cfg.HTTP.DrainThresholdSubs)
	}
}

// TestLoad_HttpPoolKnobs_EnvOverride_max_batch_bytes_per_sub asserts
// WALERA_HTTP_MAX_BATCH_BYTES_PER_SUB overrides the default.
func TestLoad_HttpPoolKnobs_EnvOverride_max_batch_bytes_per_sub(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	t.Setenv("WALERA_HTTP_MAX_BATCH_BYTES_PER_SUB", "131072")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.MaxBatchBytesPerSub != 131072 {
		t.Errorf("Http.MaxBatchBytesPerSub = %d; want 131072 (env override)", cfg.HTTP.MaxBatchBytesPerSub)
	}
}

// TestLoad_HttpPoolKnobs_EnvOverride_batching_disabled covers both true and
// false env values.
func TestLoad_HttpPoolKnobs_EnvOverride_batching_disabled(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		setPhase2RequiredEnv(t)
		setPhase3RequiredEnv(t)
		t.Setenv("WALERA_HTTP_BATCHING_DISABLED", "true")

		cfg, err := LoadAppConfig("")
		if err != nil {
			t.Fatalf("Load() returned error: %v", err)
		}
		if !cfg.HTTP.BatchingDisabled {
			t.Errorf("Http.BatchingDisabled = false; want true (env override)")
		}
	})

	t.Run("false", func(t *testing.T) {
		setPhase2RequiredEnv(t)
		setPhase3RequiredEnv(t)
		t.Setenv("WALERA_HTTP_BATCHING_DISABLED", "false")

		cfg, err := LoadAppConfig("")
		if err != nil {
			t.Fatalf("Load() returned error: %v", err)
		}
		if cfg.HTTP.BatchingDisabled {
			t.Errorf("Http.BatchingDisabled = true; want false (env override)")
		}
	})
}

// TestLoad_HttpPoolKnobs_Invalid asserts each numeric range-checked knob
// rejects an out-of-range value AND the error names the koanf key
// (operators must be able to grep for the offending key).
func TestLoad_HttpPoolKnobs_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		envKey  string
		envVal  string
		wantKey string // koanf key the error message MUST contain
	}{
		{"pool_factor zero rejected", "WALERA_HTTP_POOL_FACTOR", "0", "http.pool_factor"},
		{"pool_factor negative rejected", "WALERA_HTTP_POOL_FACTOR", "-1", "http.pool_factor"},
		{"sub_queue_size zero rejected", "WALERA_HTTP_SUB_QUEUE_SIZE", "0", "http.sub_queue_size"},
		{"sub_queue_size negative rejected", "WALERA_HTTP_SUB_QUEUE_SIZE", "-1", "http.sub_queue_size"},
		{"max_wait_ms negative rejected", "WALERA_HTTP_MAX_WAIT_MS", "-1", "http.max_wait_ms"},
		{"drain_threshold_subs negative rejected", "WALERA_HTTP_DRAIN_THRESHOLD_SUBS", "-1", "http.drain_threshold_subs"},
		{"max_batch_bytes_per_sub zero rejected", "WALERA_HTTP_MAX_BATCH_BYTES_PER_SUB", "0", "http.max_batch_bytes_per_sub"},
		{"max_batch_bytes_per_sub negative rejected", "WALERA_HTTP_MAX_BATCH_BYTES_PER_SUB", "-1", "http.max_batch_bytes_per_sub"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPhase2RequiredEnv(t)
			setPhase3RequiredEnv(t)
			t.Setenv(tc.envKey, tc.envVal)

			_, err := LoadAppConfig("")
			if err == nil {
				t.Fatalf("Load() expected error for %s=%s, got nil", tc.envKey, tc.envVal)
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("err = %v; want substring %q (key name required for grep)", err, tc.wantKey)
			}
			// Sanity: the precedent error format embeds "must be" — assert
			// that the format precedent is preserved.
			if !strings.Contains(err.Error(), "must be") {
				t.Errorf("err = %v; want substring 'must be' (matches existing 'must be >= N (got M)' precedent)", err)
			}
		})
	}
}
