package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf/v2"
)

func TestLoad_RejectsRemovedKeys(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		envKey  string
		wantKey string
	}{
		{"yaml signing", "auth:\n  signing:\n    secret: s3cr3t-value\n", "", "auth.signing.secret"},
		{"yaml breaker", "auth:\n  breaker:\n    cooldown: 30s\n", "", "auth.breaker.cooldown"},
		{"yaml health channel", "auth:\n  health_channel: _health\n", "", "auth.health_channel"},
		{"yaml drain deadline", "shutdown:\n  drain_deadline: 8s\n", "", "shutdown.drain_deadline"},
		{"yaml pool factor", "http:\n  pool_factor: 2\n", "", "http.pool_factor"},
		{"yaml sub queue size", "http:\n  sub_queue_size: 100\n", "", "http.sub_queue_size"},
		{"yaml max wait ms", "http:\n  max_wait_ms: 50\n", "", "http.max_wait_ms"},
		{"yaml drain threshold subs", "http:\n  drain_threshold_subs: 10\n", "", "http.drain_threshold_subs"},
		{"yaml max batch bytes per sub", "http:\n  max_batch_bytes_per_sub: 1024\n", "", "http.max_batch_bytes_per_sub"},
		{"yaml batching", "http:\n  batching_disabled: true\n", "", "http.batching_disabled"},
		{"env signing secret", "", "WALERA_AUTH_SIGNING_SECRET", "auth.signing_secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
			if tc.envKey != "" {
				t.Setenv(tc.envKey, "s3cr3t-value")
			}
			path := writeTempYAML(t, "database:\n  url: \"postgres://a:b@localhost/db\"\n"+tc.yaml)
			_, err := LoadAppConfig(path)
			if err == nil {
				t.Fatal("LoadAppConfig: err = nil; want removed-key rejection")
			}
			if !strings.Contains(err.Error(), "removed config keys") || !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("err = %q; want it to name removed key %q", err.Error(), tc.wantKey)
			}
			if strings.Contains(err.Error(), "s3cr3t-value") {
				t.Errorf("err = %q; must not echo the key's value", err.Error())
			}
		})
	}
}

// A key that merely shares a prefix with a removed key — but isn't it, or a
// dotted/underscored child of it — must not be rejected (M4).
func TestRejectRemovedKeys_PrefixBoundary(t *testing.T) {
	k := koanf.New(".")
	if err := k.Set("auth.signingalg", "hmac-sha256"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := rejectRemovedKeys(k); err != nil {
		t.Errorf("rejectRemovedKeys(auth.signingalg) = %v; want nil", err)
	}
}

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writeTempYAML: %v", err)
	}
	return path
}

func setPhase3RequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.example/test")
	t.Setenv("WALERA_WAL_BOOTSTRAP_TABLES", "public.orders")
}

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

func TestLoad_EnvOverride(t *testing.T) {
	setPhase3RequiredEnv(t)

	path := writeTempYAML(t, `
database:
  url: "postgres://admin:x@localhost/db"
`)

	t.Setenv("WALERA_WAL_PUBLICATION_NAME", "env_publication")

	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.WAL.PublicationName != "env_publication" {
		t.Errorf("WAL.PublicationName = %q; want %q", cfg.WAL.PublicationName, "env_publication")
	}
}

func TestLoad_MissingMandatoryFields(t *testing.T) {

	path := writeTempYAML(t, ``)

	_, err := LoadAppConfig(path)
	if err == nil {
		t.Fatal("Load() expected error for missing mandatory fields, got nil")
	}
}

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

func TestLoad_DefaultsForPhase2Fields(t *testing.T) {

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

func setPhase2RequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_WAL_BOOTSTRAP_TABLES", "public.orders")
}

func TestLoad_MinimalConfig(t *testing.T) {
	t.Setenv("WALERA_DATABASE_URL", "postgres://a:b@localhost/db")
	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://auth.example/test")
	t.Setenv("WALERA_WAL_BOOTSTRAP_TABLES", "public.orders")

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
	if cfg.Auth.RequestTimeout != 2*time.Second {
		t.Errorf("Auth.RequestTimeout: got %s; want 2s", cfg.Auth.RequestTimeout)
	}
	if cfg.Limits.GlobalConcurrent != 50000 {
		t.Errorf("Limits.GlobalConcurrent: got %d; want 50000", cfg.Limits.GlobalConcurrent)
	}
	if cfg.Limits.PerUserConcurrentMax != 10 {
		t.Errorf("Limits.PerUserConcurrentMax: got %d; want 10", cfg.Limits.PerUserConcurrentMax)
	}

	if cfg.Health.ReadyzProbeInterval != 5*time.Second {
		t.Errorf("Health.ReadyzProbeInterval: got %s; want 5s", cfg.Health.ReadyzProbeInterval)
	}
}

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
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("%s: got %v; want %v", tc.name, tc.got, tc.want)
		}
	}
}

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

	msg := err.Error()
	if !strings.Contains(msg, "auth.backend_url") {
		t.Errorf("err missing 'auth.backend_url' reference: %v", err)
	}
}

func TestLoad_SEC04_ControlCharURL_TreatedAsNonHttps(t *testing.T) {
	setPhase2RequiredEnv(t)

	t.Setenv("WALERA_AUTH_BACKEND_URL", "https://example.com/\x7f")
	t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
	_, err := LoadAppConfig("")
	if err == nil {
		t.Fatal("want error for control-char URL, got nil")
	}

	if !strings.Contains(err.Error(), "auth.backend_url") {
		t.Errorf("err missing 'auth.backend_url' reference: %v", err)
	}
}

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

func Test_SEC04_HttpAuthBackend_FailsStartup(t *testing.T) {
	t.Run("http_rejected_without_override", func(t *testing.T) {
		setPhase2RequiredEnv(t)
		t.Setenv("WALERA_AUTH_BACKEND_URL", "http://auth.local")

		t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
		_, err := LoadAppConfig("")
		if err == nil {
			t.Fatal("Load() expected error for http:// without override, got nil")
		}
		if !strings.Contains(err.Error(), "auth.backend_url") {
			t.Errorf("err = %v; want substring 'auth.backend_url'", err)
		}

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

		t.Setenv("WALERA_AUTH_ALLOW_PLAINTEXT", "")
		if _, err := LoadAppConfig(""); err != nil {
			t.Fatalf("Load() with https baseline returned unexpected error: %v", err)
		}
	})
}

func TestLoad_PProfAddr_DefaultEmpty(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)

	t.Setenv("WALERA_PPROF_ALLOW_PUBLIC", "")

	cfg, err := LoadAppConfig("")
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTP.PProfAddr != "" {
		t.Errorf("default Http.PProfAddr = %q; want empty (disabled)", cfg.HTTP.PProfAddr)
	}
}

func TestLoad_PProfAddr_Loopback_Accepts(t *testing.T) {
	cases := []string{
		"127.0.0.1:6060",
		"[::1]:6060",
		"localhost:6060",
		"LocalHost:6060",
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

func TestLoad_BootstrapV2_Defaults(t *testing.T) {
	setPhase2RequiredEnv(t)
	setPhase3RequiredEnv(t)
	// mode=verify doesn't require tables; keeps the "Tables defaults to empty" assertion meaningful.
	_ = os.Unsetenv("WALERA_WAL_BOOTSTRAP_TABLES")
	t.Setenv("WALERA_WAL_BOOTSTRAP_MODE", "verify")

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

func TestLoad_BootstrapV2_TablesFromYAML(t *testing.T) {
	setPhase3RequiredEnv(t)
	// let the YAML tables list take effect instead of the helper's env default.
	_ = os.Unsetenv("WALERA_WAL_BOOTSTRAP_TABLES")
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
			// let the YAML's invalid entry take effect instead of the helper's valid env default.
			_ = os.Unsetenv("WALERA_WAL_BOOTSTRAP_TABLES")
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
