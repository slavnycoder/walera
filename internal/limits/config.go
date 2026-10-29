package limits

import (
	"errors"
	"fmt"

	"github.com/knadh/koanf/v2"
)

type Config struct {
	GlobalConcurrent int `koanf:"global_concurrent"`

	PerUserConcurrentMax int `koanf:"per_user_concurrent"`
}

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("limits.global_concurrent", 50000)
	_ = k.Set("limits.per_user_concurrent", 10)
}

func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("limits", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("limits config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("limits config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.GlobalConcurrent <= 0 {
		errs = append(errs, errors.New("limits.global_concurrent must be > 0"))
	}
	if c.PerUserConcurrentMax <= 0 {
		errs = append(errs, errors.New("limits.per_user_concurrent must be > 0"))
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
