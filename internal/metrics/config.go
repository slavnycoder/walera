// Package metrics — config.go defines the metrics-sampler tuning knobs and the
// per-package LoadConfig that owns the "metrics." subtree.
package metrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/knadh/koanf/v2"
)

// Config holds the metrics-sampler tuning.
// SampleInterval is the cadence at which size-gauges (routing_index_size,
// subscriber_queue_depth) are re-sampled. Default 30s.
type Config struct {
	SampleInterval time.Duration `koanf:"sample_interval"`
}

// ApplyDefaults registers the metrics.* defaults on the supplied koanf instance.
func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("metrics.sample_interval", "30s")
}

// LoadConfig unmarshals the "metrics" subtree from k into a Config and runs
// all metrics-specific validation.
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

// Validate enforces the metrics-package invariants.
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
