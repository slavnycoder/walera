package metrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/knadh/koanf/v2"
)

type Config struct {
	SampleInterval time.Duration `koanf:"sample_interval"`
}

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("metrics.sample_interval", "30s")
}

func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("metrics", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("metrics config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("metrics config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.SampleInterval <= 0 {
		errs = append(errs, errors.New("metrics.sample_interval must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
