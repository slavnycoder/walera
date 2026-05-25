// Package auth — config.go: typed config sub-structs + LoadConfig.
package auth

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
)

// Config holds every auth knob. Mounted at the "auth." key in the root koanf
// tree.
type Config struct {
	// BackendURL is the base URL of the external auth service. Mandatory.
	BackendURL string `koanf:"backend_url"`

	// DefaultTTLSeconds is the default permission-map TTL. Default: 60.
	DefaultTTLSeconds int `koanf:"default_ttl_seconds"`

	// HealthChannel is the channel name used by health.Server's auth probe.
	// Default: "_health".
	HealthChannel string `koanf:"health_channel"`

	// RequestTimeout caps every HTTP call to the auth backend. Default: 2s.
	RequestTimeout time.Duration `koanf:"request_timeout"`

	// Breaker holds the auth circuit-breaker tuning sub-struct.
	Breaker BreakerConfig `koanf:"breaker"`
}

// BreakerConfig tunes the auth circuit breaker.
type BreakerConfig struct {
	// WindowBuckets is the rolling-window bucket count. Default: 30.
	WindowBuckets int `koanf:"window_buckets"`

	// BucketSeconds is the duration of each window bucket in seconds. Default: 1.
	BucketSeconds int `koanf:"bucket_seconds"`

	// FailureRateThreshold is the failure ratio that opens the breaker once
	// DebounceFloor calls have accumulated. Default: 0.5.
	FailureRateThreshold float64 `koanf:"failure_rate_threshold"`

	// DebounceFloor is the minimum sample size before the failure-rate
	// threshold can fire. Default: 20.
	DebounceFloor int `koanf:"debounce_floor"`

	// Cooldown is how long the breaker stays Open before transitioning to
	// HalfOpen. Default: 30s.
	Cooldown time.Duration `koanf:"cooldown"`

	// StaleRefreshJitter bounds random jitter on background refresh
	// scheduling. Default: 5s.
	StaleRefreshJitter time.Duration `koanf:"stale_refresh_jitter"`
}

// ApplyDefaults registers the auth.* defaults on the supplied koanf instance.
func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("auth.default_ttl_seconds", 60)
	_ = k.Set("auth.health_channel", "_health")
	_ = k.Set("auth.request_timeout", "2s")
	_ = k.Set("auth.breaker.window_buckets", 30)
	_ = k.Set("auth.breaker.bucket_seconds", 1)
	_ = k.Set("auth.breaker.failure_rate_threshold", 0.5)
	_ = k.Set("auth.breaker.debounce_floor", 20)
	_ = k.Set("auth.breaker.cooldown", "30s")
	_ = k.Set("auth.breaker.stale_refresh_jitter", "5s")
}

// LoadConfig unmarshals the "auth" subtree from k into a Config and runs all
// auth-specific validation.
func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("auth", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("auth config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	return cfg, nil
}

// Validate enforces the auth-package invariants.
func (c Config) Validate() error {
	var errs []error
	if c.BackendURL == "" {
		errs = append(errs, errors.New("auth.backend_url is required"))
	} else {
		u, err := url.Parse(c.BackendURL)
		switch {
		case err != nil:
			errs = append(errs, config.FormatError(
				"auth.backend_url",
				c.BackendURL,
				"URL parse failed: "+err.Error(),
				"provide an absolute https:// URL",
			))
		case u.Scheme != "http" && u.Scheme != "https":
			errs = append(errs, config.FormatError(
				"auth.backend_url",
				c.BackendURL,
				"scheme must be http or https",
				"use https://<host>/<base-path>",
			))
		case u.Host == "":
			errs = append(errs, config.FormatError(
				"auth.backend_url",
				c.BackendURL,
				"host is empty",
				"include host[:port] in the URL",
			))
		default:
			// SEC-04 — require https unless the dev escape hatch is set.
			if u.Scheme != "https" && os.Getenv("WALERA_AUTH_ALLOW_PLAINTEXT") != "1" {
				errs = append(errs, errors.New("auth.backend_url must use https:// (override with WALERA_AUTH_ALLOW_PLAINTEXT=1 for dev)"))
			}
		}
	}
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("auth.request_timeout must be > 0"))
	}
	// Cross-field rule: cooldown must be >= request_timeout so the half-open
	// probe has at least one full request-timeout window.
	if c.RequestTimeout > 0 && c.Breaker.Cooldown > 0 && c.Breaker.Cooldown < c.RequestTimeout {
		errs = append(errs, config.FormatError(
			"auth.breaker.cooldown vs auth.request_timeout",
			fmt.Sprintf("%s vs %s", c.Breaker.Cooldown, c.RequestTimeout),
			"breaker cooldown must be >= request_timeout",
			"increase auth.breaker.cooldown or lower auth.request_timeout",
		))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}

var _ = time.Second
