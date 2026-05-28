package limits

import (
	"errors"
	"fmt"
	"net"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
)

type Config struct {
	GlobalConcurrent int `koanf:"global_concurrent"`

	PerUserConcurrentMax int `koanf:"per_user_concurrent"`

	TrustedProxies []string `koanf:"trusted_proxies"`
}

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("limits.global_concurrent", 50000)
	_ = k.Set("limits.per_user_concurrent", 10)
	_ = k.Set("limits.trusted_proxies", []string{})
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

	for i, s := range c.TrustedProxies {
		if _, _, err := net.ParseCIDR(s); err != nil {
			errs = append(errs, config.FormatError(
				fmt.Sprintf("limits.trusted_proxies[%d]", i),
				s,
				"is not a valid CIDR: "+err.Error(),
				"use a CIDR like 10.0.0.0/8",
			))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
