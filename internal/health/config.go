package health

import (
	"errors"
	"fmt"
	"time"

	"github.com/knadh/koanf/v2"
)

type Config struct {
	ReadyzProbeInterval time.Duration `koanf:"readyz_probe_interval"`
}

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("health.readyz_probe_interval", "5s")
}

func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("health", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("health config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("health config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.ReadyzProbeInterval <= 0 {
		errs = append(errs, errors.New("health.readyz_probe_interval must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
