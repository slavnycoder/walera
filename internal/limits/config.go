package limits

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
)

type Config struct {
	GlobalConcurrent int `koanf:"global_concurrent"`

	PerUserConcurrentMax int `koanf:"per_user_concurrent"`

	PerUserRatePerSecond float64 `koanf:"per_user_rate_per_second"`

	PerUserBurst int `koanf:"per_user_burst"`

	PreAuthRatePerSecond float64 `koanf:"pre_auth_rate_per_second"`

	PreAuthBurst int `koanf:"pre_auth_burst"`

	SweepInterval time.Duration `koanf:"sweep_interval"`

	SweepIdleThreshold time.Duration `koanf:"sweep_idle_threshold"`

	TrustedProxies []string `koanf:"trusted_proxies"`
}

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("limits.global_concurrent", 50000)
	_ = k.Set("limits.per_user_concurrent", 10)
	_ = k.Set("limits.per_user_rate_per_second", 5.0)
	_ = k.Set("limits.per_user_burst", 10)
	_ = k.Set("limits.pre_auth_rate_per_second", 5.0)
	_ = k.Set("limits.pre_auth_burst", 10)
	_ = k.Set("limits.sweep_interval", "60s")
	_ = k.Set("limits.sweep_idle_threshold", "5m")
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
	if c.PerUserRatePerSecond <= 0 {
		errs = append(errs, config.FormatError(
			"limits.per_user_rate_per_second",
			fmt.Sprintf("%v", c.PerUserRatePerSecond),
			"must be > 0",
			"choose a positive rate; zero would disable the limiter",
		))
	}
	if c.PerUserBurst <= 0 {
		errs = append(errs, config.FormatError(
			"limits.per_user_burst",
			fmt.Sprintf("%d", c.PerUserBurst),
			"must be > 0",
			"choose a positive burst capacity",
		))
	}
	if c.PreAuthRatePerSecond <= 0 {
		errs = append(errs, config.FormatError(
			"limits.pre_auth_rate_per_second",
			fmt.Sprintf("%v", c.PreAuthRatePerSecond),
			"must be > 0",
			"choose a positive rate; zero would disable the limiter",
		))
	}
	if c.PreAuthBurst <= 0 {
		errs = append(errs, config.FormatError(
			"limits.pre_auth_burst",
			fmt.Sprintf("%d", c.PreAuthBurst),
			"must be > 0",
			"choose a positive burst capacity",
		))
	}
	if c.SweepInterval <= 0 {
		errs = append(errs, config.FormatError(
			"limits.sweep_interval",
			c.SweepInterval.String(),
			"must be > 0",
			"choose a positive duration (e.g., 60s)",
		))
	}
	if c.SweepIdleThreshold <= 0 {
		errs = append(errs, config.FormatError(
			"limits.sweep_idle_threshold",
			c.SweepIdleThreshold.String(),
			"must be > 0",
			"choose a positive duration (e.g., 5m)",
		))
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

	if float64(c.PerUserBurst) < c.PerUserRatePerSecond {
		errs = append(errs, config.FormatError(
			"limits.per_user_burst vs limits.per_user_rate_per_second",
			fmt.Sprintf("%d vs %v", c.PerUserBurst, c.PerUserRatePerSecond),
			"burst must be >= rate",
			"increase per_user_burst or lower per_user_rate_per_second",
		))
	}
	if float64(c.PreAuthBurst) < c.PreAuthRatePerSecond {
		errs = append(errs, config.FormatError(
			"limits.pre_auth_burst vs limits.pre_auth_rate_per_second",
			fmt.Sprintf("%d vs %v", c.PreAuthBurst, c.PreAuthRatePerSecond),
			"burst must be >= rate",
			"increase pre_auth_burst or lower pre_auth_rate_per_second",
		))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
