// Package router — config.go: Config sub-struct + LoadConfig for the "router." koanf subtree.
package router

import (
	"errors"
	"fmt"
	"time"

	"github.com/knadh/koanf/v2"
)

// Config holds router-specific tuning knobs (koanf keys under "router.").
type Config struct {
	// ExactBuffer is per-subscriber buffered-channel capacity for exact
	// subscriptions. Default: 64.
	ExactBuffer int `koanf:"exact_buffer"`

	// WildcardBuffer is per-subscriber buffered-channel capacity for
	// wildcard subscriptions. Default: 512 (larger because one tx may carry
	// many changes against the same table).
	WildcardBuffer int `koanf:"wildcard_buffer"`

	// MaxChangesPerTx caps matched changes per tx for ALL subscriber classes
	// (exact and wildcard). Exceedance → tx_too_large drop. Default: 10000.
	MaxChangesPerTx int `koanf:"max_changes_per_tx"`

	// HeartbeatInterval is the SSE writer keep-alive cadence. Default: 15s.
	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`
}

// ApplyDefaults registers the router.* defaults on the supplied koanf instance.
func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("router.exact_buffer", 64)
	_ = k.Set("router.wildcard_buffer", 512)
	_ = k.Set("router.max_changes_per_tx", 10000)
	_ = k.Set("router.heartbeat_interval", "15s")
}

// LoadConfig unmarshals the "router" subtree from k into a Config and runs
// all router-specific validation.
func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("router", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("router config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("router config: %w", err)
	}
	return cfg, nil
}

// Validate enforces the router-package invariants.
func (c Config) Validate() error {
	var errs []error
	if c.ExactBuffer <= 0 {
		errs = append(errs, errors.New("router.exact_buffer must be > 0"))
	}
	if c.WildcardBuffer <= 0 {
		errs = append(errs, errors.New("router.wildcard_buffer must be > 0"))
	}
	if c.MaxChangesPerTx <= 0 {
		errs = append(errs, errors.New("router.max_changes_per_tx must be > 0"))
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, errors.New("router.heartbeat_interval must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
