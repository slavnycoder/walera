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

func LoadKoanf(path string, applyDefaults func(*koanf.Koanf)) (*koanf.Koanf, error) {
	k := koanf.New(".")

	if applyDefaults != nil {
		applyDefaults(k)
	}

	if err := refuseDevEnv(os.Environ()); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	if path != "" {
		if _, err := os.Stat(path); err == nil {
			if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
				return nil, fmt.Errorf("config: load YAML %q: %w", path, err)
			}
		}
	}

	if err := k.Load(envprovider.Provider(".", envprovider.Opt{
		Prefix:        "WALERA_",
		TransformFunc: envTransform,
	}), nil); err != nil {
		return nil, fmt.Errorf("config: load env: %w", err)
	}

	return k, nil
}

func envTransform(key, value string) (string, any) {
	const prefix = "WALERA_"
	if !strings.HasPrefix(key, prefix) {
		return "", nil
	}
	k2 := strings.ToLower(strings.Replace(key[len(prefix):], "_", ".", 1))
	if remapped, ok := multiLevelEnvKeys[k2]; ok {
		k2 = remapped
	}

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
