// Package limits — config.go defines the typed configuration for admission
// control and the per-package LoadConfig that owns the "limits." subtree.
//
// Two pre-auth gates (global concurrency + per-IP rate) precede the auth
// backend call; two post-auth gates (per-user concurrency + per-user rate)
// follow. The GC sweeper removes idle rate-limiter entries every
// SweepInterval / older than SweepIdleThreshold.
package limits

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
)

// Config holds every limits knob. Mounted at the "limits." key in the root
// koanf tree.
type Config struct {
	// GlobalConcurrent is the cap on total in-flight SSE handshakes
	// (pre-auth gate). Default: 50000.
	GlobalConcurrent int `koanf:"global_concurrent"`

	// PerUserConcurrentMax is the maximum number of simultaneously connected
	// SSE streams per authenticated user (post-auth gate). Default: 10.
	PerUserConcurrentMax int `koanf:"per_user_concurrent"`

	// PerUserRatePerSecond is the steady-state replenishment rate for the
	// per-user token bucket (post-auth gate). Default: 5.0.
	PerUserRatePerSecond float64 `koanf:"per_user_rate_per_second"`

	// PerUserBurst is the per-user token-bucket burst capacity. Default: 10.
	PerUserBurst int `koanf:"per_user_burst"`

	// PreAuthRatePerSecond is the per-IP token replenishment rate applied
	// BEFORE the auth backend call (pre-auth gate). Default: 5.0.
	PreAuthRatePerSecond float64 `koanf:"pre_auth_rate_per_second"`

	// PreAuthBurst is the per-IP token-bucket burst capacity. Default: 10.
	PreAuthBurst int `koanf:"pre_auth_burst"`

	// SweepInterval is the cadence at which RunSweeper scans the rate-limiter
	// maps for idle entries. Default: 60s.
	SweepInterval time.Duration `koanf:"sweep_interval"`

	// SweepIdleThreshold is the duration an entry must remain untouched
	// before the sweeper deletes it. Default: 5m.
	SweepIdleThreshold time.Duration `koanf:"sweep_idle_threshold"`

	// TrustedProxies is the CIDR allowlist consulted by sse.clientIP when
	// deciding whether to honour the request's X-Forwarded-For header.
	// Empty (default) means XFF is ignored and r.RemoteAddr's host is the
	// only client identifier.
	//
	// Each entry must parse via net.ParseCIDR; LoadConfig rejects
	// malformed entries with a named error. The parsed []*net.IPNet is
	// computed once in limits.New() so the request-time IsTrustedProxy
	// lookup stays allocation-free.
	//
	// Canonical k8s example: ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
	TrustedProxies []string `koanf:"trusted_proxies"`
}

// ApplyDefaults registers the limits.* defaults on the supplied koanf instance.
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

// LoadConfig unmarshals the "limits" subtree from k into a Config and runs
// all limits-specific validation.
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

// Validate enforces the limits-package invariants.
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
	// SEC-05 — every trusted_proxies entry must parse as a CIDR.
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
	// Combination layer (D-12 layer 3): burst must be >= rate for both
	// rate-limiter pairs. A burst smaller than the steady-state rate
	// means the bucket can never refill enough to serve a steady-rate
	// stream — operational footgun that silently rejects valid traffic.
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
