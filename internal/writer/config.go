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

type WriterConfig struct {
	Log      WriterLogConfig      `koanf:"log"`
	PG       WriterPGConfig       `koanf:"pg"`
	HTTP     WriterHTTPConfig     `koanf:"http"`
	Scenario WriterScenarioConfig `koanf:"scenario"`
	Pool     WriterPoolConfig     `koanf:"pool"`
	Arrivals WriterArrivalsConfig `koanf:"arrivals"`
}

type WriterLogConfig struct {
	Level string `koanf:"level"`
}

type WriterPGConfig struct {
	DSN          string        `koanf:"dsn"`
	TargetTables []string      `koanf:"target_tables"`
	TxTimeout    time.Duration `koanf:"tx_timeout"`
}

type WriterHTTPConfig struct {
	Addr        string   `koanf:"addr"`
	CORSOrigins []string `koanf:"cors_origins"`
}

type WriterScenarioConfig struct {
	Name         string        `koanf:"name"`
	CommitRate   float64       `koanf:"commit_rate"`
	RowsPerTx    int           `koanf:"rows_per_tx"`
	RampDuration time.Duration `koanf:"ramp_duration"`
}

type WriterPoolConfig struct {
	MaxConns int `koanf:"max_conns"`
	MinConns int `koanf:"min_conns"`
}

type WriterArrivalsConfig struct {
	Distribution string `koanf:"distribution"`
}

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

	if k.String("pg.dsn") == "" {
		if v := os.Getenv("WALERA_DATABASE_URL"); v != "" {
			_ = k.Set("pg.dsn", v)
		}
	}

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

func applyFlagOverrides(k *koanf.Koanf, flagSet *flag.FlagSet) {

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
