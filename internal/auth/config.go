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

type Config struct {
	BackendURL string `koanf:"backend_url"`

	DefaultTTLSeconds int `koanf:"default_ttl_seconds"`

	HealthChannel string `koanf:"health_channel"`

	RequestTimeout time.Duration `koanf:"request_timeout"`

	Breaker BreakerConfig `koanf:"breaker"`

	Signing SigningConfig `koanf:"signing"`
}

type SigningConfig struct {
	Secret string `koanf:"secret"`

	Kid string `koanf:"kid"`
}

type BreakerConfig struct {
	WindowBuckets int `koanf:"window_buckets"`

	BucketSeconds int `koanf:"bucket_seconds"`

	FailureRateThreshold float64 `koanf:"failure_rate_threshold"`

	DebounceFloor int `koanf:"debounce_floor"`

	Cooldown time.Duration `koanf:"cooldown"`

	StaleRefreshJitter time.Duration `koanf:"stale_refresh_jitter"`
}

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
	_ = k.Set("auth.signing.kid", "v1")
}

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

			if u.Scheme != "https" && os.Getenv("WALERA_AUTH_ALLOW_PLAINTEXT") != "1" {
				errs = append(errs, errors.New("auth.backend_url must use https:// (override with WALERA_AUTH_ALLOW_PLAINTEXT=1 for dev)"))
			}
		}
	}
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("auth.request_timeout must be > 0"))
	}

	if c.Signing.Secret == "" {
		errs = append(errs, errors.New("auth.signing.secret is required (set WALERA_AUTH_SIGNING_SECRET, ≥32 bytes)"))
	} else if len(c.Signing.Secret) < MinSigningSecretBytes {
		errs = append(errs, config.FormatError(
			"auth.signing.secret",
			fmt.Sprintf("len=%d", len(c.Signing.Secret)),
			fmt.Sprintf("must be at least %d bytes", MinSigningSecretBytes),
			"generate via: openssl rand -hex 32",
		))
	}
	if c.Signing.Kid == "" {
		errs = append(errs, errors.New("auth.signing.kid is required"))
	}

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
