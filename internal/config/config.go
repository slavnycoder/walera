// Package config provides the koanf source-wiring primitive used by the
// cdc-sse binary to read configuration from a YAML file overlaid with
// WALERA_-prefixed environment variables.
//
// Per-package typed Config structs and their validation live in each
// internal/<pkg>/config.go (e.g., wal.Config, auth.Config, router.Config).
// The aggregate AppConfig is assembled in cmd/cdc-sse/config.go via
// LoadAppConfig, which calls LoadKoanf below to obtain the populated koanf
// instance and then dispatches to each <pkg>.LoadConfig.
//
// This package intentionally has NO imports under walera/internal — it is a
// primitives-only leaf so the import graph stays directional (DEPS-03).
//
// Sources (in order, later overrides earlier):
//  1. Caller-supplied defaults (passed as the applyDefaults closure).
//  2. YAML file at the path provided to LoadKoanf.
//  3. Environment variables with the WALERA_ prefix
//     (e.g., WALERA_DATABASE_URL).
//
// Env var mapping: strip WALERA_ prefix, replace the first underscore with
// a dot, lowercase the result. Examples:
//
//	WALERA_DATABASE_URL         → database.url
//	WALERA_WAL_PUBLICATION_NAME → wal.publication_name
//	WALERA_LOG_LEVEL            → log.level
//	WALERA_LOG_DEV_MODE         → log.dev_mode
//
// database.url is the single Postgres DSN input; the admin and (derived)
// replication connection strings both come from it. The former
// WALERA_WAL_POSTGRES_DSN / WALERA_WAL_REPLICATION_DSN inputs are removed.
//
// A small allow-list of documented multi-level keys (for example
// wal.bootstrap.*, wal.reconnect.*, and auth.breaker.*) is remapped explicitly
// so the env shape stays predictable.
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	envprovider "github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// LoadKoanf constructs a koanf instance, applies the caller-supplied
// defaults (via applyDefaults), layers the YAML file at path (when path is
// non-empty and the file exists), then overlays WALERA_-prefixed env vars.
// Returns the populated koanf instance ready for per-package Unmarshal.
//
// LoadKoanf does NOT unmarshal or validate — that work belongs to each
// internal/<pkg>.LoadConfig. The caller (cmd/cdc-sse.LoadAppConfig) wires
// each package's loader against the returned instance.
func LoadKoanf(path string, applyDefaults func(*koanf.Koanf)) (*koanf.Koanf, error) {
	k := koanf.New(".")

	// Layer 0: caller-supplied defaults. Invoked before any source loader
	// runs so the defaults survive when neither YAML nor env supplies a
	// value. nil-safe: callers that want a bare koanf may pass nil.
	if applyDefaults != nil {
		applyDefaults(k)
	}

	// Dev-flag guard: refuse WALERA_EXPERIMENTAL_*, WALERA_DEBUG_FORCE_*,
	// and WALERA_PLAN_* env vars in production builds. The companion
	// dev_guard_dev.go (//go:build dev) makes this a no-op under
	// `-tags dev` so developers may opt in intentionally. Runs BEFORE any
	// source loader so a refused env var never reaches koanf.
	if err := refuseDevEnv(os.Environ()); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	// Layer 1: YAML file (if provided and exists).
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
				return nil, fmt.Errorf("config: load YAML %q: %w", path, err)
			}
		}
	}

	// Layer 2: Environment variables (override YAML).
	if err := k.Load(envprovider.Provider(".", envprovider.Opt{
		Prefix:        "WALERA_",
		TransformFunc: envTransform,
	}), nil); err != nil {
		return nil, fmt.Errorf("config: load env: %w", err)
	}

	return k, nil
}

// envTransform maps WALERA_-prefixed env vars to koanf paths.
//
//  1. Strip WALERA_ prefix.
//  2. Replace the FIRST underscore with a dot (creates the top-level key).
//  3. Lowercase the result.
//
// Slice-valued keys: koanf v2 will coerce a bare string into a one-element
// []string at Unmarshal time, but it does NOT split on commas. For known
// slice-valued keys (currently "http.cors_origins" and
// "wal.bootstrap.tables") this transform splits the env value on ',' and
// returns []string, so operators can write
//
//	WALERA_HTTP_CORS_ORIGINS="http://a.com,http://b.com"
//
// and get two origins. Single-value form (no comma) still produces a
// one-element slice.
//
// Multi-level keys whose flat env name carries a second underscore that
// maps to a nested koanf path are explicitly remapped. Add new entries when
// introducing a documented sub-tree that must be settable from env.
func envTransform(key, value string) (string, any) {
	const prefix = "WALERA_"
	if !strings.HasPrefix(key, prefix) {
		return "", nil // ignore — should not happen given the prefix filter
	}
	k2 := strings.ToLower(strings.Replace(key[len(prefix):], "_", ".", 1))
	if remapped, ok := multiLevelEnvKeys[k2]; ok {
		k2 = remapped
	}
	// Slice-valued keys: split on comma and trim whitespace.
	switch k2 {
	case "http.cors_origins", "wal.bootstrap.tables":
		parts := strings.Split(value, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return k2, out
	}
	// Empty env override → treat as unset so caller-supplied defaults
	// survive. Without this, `WALERA_WAL_PUBLICATION_NAME=""` would
	// overwrite the "walera_pub" default with "". Slice-valued keys are
	// handled above and already produce []string{} for an empty input on
	// purpose; this branch only fires for string/numeric scalars.
	if value == "" {
		return "", nil
	}
	return k2, value
}

var multiLevelEnvKeys = map[string]string{
	"auth.breaker_bucket_seconds":         "auth.breaker.bucket_seconds",
	"auth.breaker_cooldown":               "auth.breaker.cooldown",
	"auth.breaker_debounce_floor":         "auth.breaker.debounce_floor",
	"auth.breaker_failure_rate_threshold": "auth.breaker.failure_rate_threshold",
	"auth.breaker_stale_refresh_jitter":   "auth.breaker.stale_refresh_jitter",
	"auth.breaker_window_buckets":         "auth.breaker.window_buckets",

	"wal.bootstrap_create_roles": "wal.bootstrap.create_roles",
	"wal.bootstrap_mode":         "wal.bootstrap.mode",
	"wal.bootstrap_tables":       "wal.bootstrap.tables",

	"wal.reconnect_reset_after_success_duration": "wal.reconnect.reset_after_success_duration",
}
