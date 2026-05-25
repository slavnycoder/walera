// Package writer provides the testbench load-generator that drives quantitative
// scenario-named traffic against the Walera testbench Postgres.
//
// This file (config.go) defines the typed WriterConfig and its koanf-based
// loader. The shape and env-transform mirror internal/config but deliberately
// duplicate to avoid importing the larger Walera config surface (no shared
// sub-types between cmd/writer + internal/writer and the main service).
//
// Env mapping: WRITER_ prefix is stripped, the first underscore becomes a dot,
// the result is lowercased. Examples:
//
//	WRITER_PG_DSN                 → pg.dsn
//	WRITER_SCENARIO_COMMIT_RATE   → scenario.commit_rate
//
// pg.dsn resolution precedence (high → low): the -pg-dsn flag, then
// WRITER_PG_DSN, then WALERA_DATABASE_URL (a fallback shared with the main
// cdc-sse service so a compose run with one DSN points the writer at the same
// DB). The writer only opens an admin pgxpool connection, so it never derives
// or needs replication=database.
//
//	WRITER_PG_TARGET_TABLES       → pg.target_tables   (CSV-split into []string)
//	WRITER_HTTP_CORS_ORIGINS      → http.cors_origins  (CSV-split into []string)
//	WRITER_ARRIVALS_DISTRIBUTION  → arrivals.distribution
//
// Security: PG.DSN holds the password and is never logged by this package.
// Validate() rejects an empty DSN and an unknown arrivals.distribution value.
package writer

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	envprovider "github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// WriterConfig is the typed root config consumed by cmd/writer/main.go.
type WriterConfig struct {
	Log      WriterLogConfig      `koanf:"log"`
	PG       WriterPGConfig       `koanf:"pg"`
	HTTP     WriterHTTPConfig     `koanf:"http"`
	Scenario WriterScenarioConfig `koanf:"scenario"`
	Pool     WriterPoolConfig     `koanf:"pool"`
	Arrivals WriterArrivalsConfig `koanf:"arrivals"`
}

// WriterLogConfig controls zerolog log level.
type WriterLogConfig struct {
	Level string `koanf:"level"`
}

// WriterPGConfig holds the writer's Postgres admin DSN and the round-robin
// target table list. PG.DSN is required; an empty value fails Load.
type WriterPGConfig struct {
	DSN          string        `koanf:"dsn"`
	TargetTables []string      `koanf:"target_tables"`
	TxTimeout    time.Duration `koanf:"tx_timeout"`
}

// WriterHTTPConfig holds the writer's HTTP control endpoint configuration.
//
// CORSOrigins is the cross-origin allowlist applied to POST /control + the
// synthesised OPTIONS preflight. Empty (the default) disables CORS entirely
// — no Access-Control-Allow-* headers are written. The env binding
// WRITER_HTTP_CORS_ORIGINS accepts a comma-separated string which the env
// transform splits into []string (whitespace-trimmed; empty entries dropped).
type WriterHTTPConfig struct {
	Addr        string   `koanf:"addr"`
	CORSOrigins []string `koanf:"cors_origins"`
}

// WriterScenarioConfig knobs override the registered scenario defaults at
// boot. RampDuration is only consulted by the ramp-up scenario.
type WriterScenarioConfig struct {
	Name         string        `koanf:"name"`
	CommitRate   float64       `koanf:"commit_rate"`
	RowsPerTx    int           `koanf:"rows_per_tx"`
	RampDuration time.Duration `koanf:"ramp_duration"`
}

// WriterPoolConfig bounds the pgxpool. MinConns defaults to 1 so a warm
// connection is kept even at low scenarios (smoke). See INVARIANTS.md.
type WriterPoolConfig struct {
	MaxConns int `koanf:"max_conns"`
	MinConns int `koanf:"min_conns"`
}

// WriterArrivalsConfig selects the inter-arrival distribution for the commit
// loop. "poisson" gives Exp(1/λ) inter-arrival times; "uniform" gives the
// deterministic 1/λ spacing produced by rate.Limiter alone.
type WriterArrivalsConfig struct {
	Distribution string `koanf:"distribution"`
}

// Load reads configuration in this precedence (low → high):
//
//	defaults → YAML (if configPath given) → env (WRITER_*) → flag overrides
//
// flagSet may be nil; when provided, only flags that were explicitly set
// (visited) override the lower layers — this matches CLI semantics where the
// default of an unprovided flag should not stomp an env value.
//
// Returns the validated config or a joined error on missing required fields.
func Load(configPath string, flagSet *flag.FlagSet) (*WriterConfig, error) {
	k := koanf.New(".")

	applyDefaults(k)

	if configPath != "" {
		if _, err := os.Stat(configPath); err == nil {
			if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
				return nil, fmt.Errorf("writer config: load YAML %q: %w", configPath, err)
			}
		}
	}

	envTransform := func(key, value string) (string, any) {
		const prefix = "WRITER_"
		if !strings.HasPrefix(key, prefix) {
			return "", nil
		}
		k2 := strings.ToLower(strings.Replace(key[len(prefix):], "_", ".", 1))
		switch k2 {
		case "pg.target_tables", "http.cors_origins":
			// Comma-separated env string → []string. Whitespace-trimmed;
			// empty entries dropped so a stray trailing comma is harmless.
			parts := strings.Split(value, ",")
			out := make([]string, 0, len(parts))
			for _, p := range parts {
				if trimmed := strings.TrimSpace(p); trimmed != "" {
					out = append(out, trimmed)
				}
			}
			return k2, out
		}
		return k2, value
	}

	if err := k.Load(envprovider.Provider(".", envprovider.Opt{
		Prefix:        "WRITER_",
		TransformFunc: envTransform,
	}), nil); err != nil {
		return nil, fmt.Errorf("writer config: load env: %w", err)
	}

	// WALERA_DATABASE_URL fallback: when no WRITER_PG_DSN (or YAML pg.dsn) has
	// supplied a value, reuse the shared service DSN so an operator running
	// compose with one WALERA_DATABASE_URL gets the writer pointed at the same
	// DB. Applied AFTER the WRITER_ env load (so WRITER_PG_DSN wins) and BEFORE
	// applyFlagOverrides (so the -pg-dsn flag still wins). The writer only ever
	// opens an admin pgxpool connection, so no replication=database handling is
	// needed here.
	if k.String("pg.dsn") == "" {
		if v := os.Getenv("WALERA_DATABASE_URL"); v != "" {
			_ = k.Set("pg.dsn", v)
		}
	}

	// Flag overrides — only for flags the user explicitly set (Visit semantics).
	if flagSet != nil {
		applyFlagOverrides(k, flagSet)
	}

	var cfg WriterConfig
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("writer config: unmarshal: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// applyDefaults seeds koanf with the documented defaults for every key.
func applyDefaults(k *koanf.Koanf) {
	_ = k.Set("log.level", "info")
	_ = k.Set("pg.target_tables", []string{"orders", "devices", "articles"})
	_ = k.Set("pg.tx_timeout", "5s")
	_ = k.Set("http.addr", ":9100")
	_ = k.Set("scenario.name", "smoke")
	_ = k.Set("scenario.commit_rate", 10.0)
	_ = k.Set("scenario.rows_per_tx", 1)
	_ = k.Set("scenario.ramp_duration", "5m")
	_ = k.Set("pool.max_conns", 8)
	_ = k.Set("pool.min_conns", 1)
	_ = k.Set("arrivals.distribution", "poisson")
}

// applyFlagOverrides looks up known flag names on flagSet and, when the flag
// was explicitly set, writes its value into koanf. Unknown flags are
// silently ignored so cmd/writer can add CLI knobs without touching this
// function.
func applyFlagOverrides(k *koanf.Koanf, flagSet *flag.FlagSet) {
	// Map of CLI flag name → koanf key.
	flagToKey := map[string]string{
		"scenario":             "scenario.name",
		"commit-rate":          "scenario.commit_rate",
		"rows-per-tx":          "scenario.rows_per_tx",
		"ramp-duration":        "scenario.ramp_duration",
		"pg-dsn":               "pg.dsn",
		"pool-max-conns":       "pool.max_conns",
		"http-addr":            "http.addr",
		"target-tables":        "pg.target_tables",
		"log-level":            "log.level",
		"arrival-distribution": "arrivals.distribution",
	}
	flagSet.Visit(func(f *flag.Flag) {
		key, ok := flagToKey[f.Name]
		if !ok {
			return
		}
		val := f.Value.String()
		if key == "pg.target_tables" {
			parts := strings.Split(val, ",")
			out := make([]string, 0, len(parts))
			for _, p := range parts {
				if t := strings.TrimSpace(p); t != "" {
					out = append(out, t)
				}
			}
			_ = k.Set(key, out)
			return
		}
		_ = k.Set(key, val)
	})
}

// validate returns a joined error listing every problem with the resolved
// config. Mirrors internal/config.validate's "fail with full list" pattern so
// operators don't iterate one fix at a time.
func validate(cfg *WriterConfig) error {
	var errs []error
	if cfg.PG.DSN == "" {
		errs = append(errs, errors.New("pg.dsn is required"))
	}
	if cfg.PG.TxTimeout <= 0 {
		errs = append(errs, errors.New("pg.tx_timeout must be > 0"))
	}
	switch cfg.Arrivals.Distribution {
	case "poisson", "uniform":
		// ok
	default:
		errs = append(errs, fmt.Errorf("arrivals.distribution %q is invalid (want poisson|uniform)", cfg.Arrivals.Distribution))
	}
	if cfg.Pool.MaxConns <= 0 {
		errs = append(errs, errors.New("pool.max_conns must be > 0"))
	}
	if cfg.Pool.MinConns < 0 {
		errs = append(errs, errors.New("pool.min_conns must be >= 0"))
	}
	if cfg.Scenario.CommitRate <= 0 {
		errs = append(errs, errors.New("scenario.commit_rate must be > 0"))
	}
	if cfg.Scenario.RowsPerTx <= 0 {
		errs = append(errs, errors.New("scenario.rows_per_tx must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("writer config: validation failed: %w", errors.Join(errs...))
	}
	return nil
}
