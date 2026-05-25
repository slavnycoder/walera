// Package health — config.go defines the typed configuration sub-struct for
// the /healthz and /readyz handlers, plus the per-package LoadConfig that
// owns the "health." subtree.
package health

import (
	"errors"
	"fmt"
	"time"

	"github.com/knadh/koanf/v2"
)

// Config holds the health-server tuning knobs. Mounted at the
// "health." key in the root koanf tree.
type Config struct {
	// ReadyzProbeInterval is the cadence at which the background /readyz
	// prober samples the auth backend (via auth.Client.Health) and the WAL
	// reader (via wal.Reader.IsConnected). Default: 5s.
	ReadyzProbeInterval time.Duration `koanf:"readyz_probe_interval"`
}

// ApplyDefaults registers the health.* defaults on the supplied koanf instance.
func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("health.readyz_probe_interval", "5s")
}

// LoadConfig unmarshals the "health" subtree from k into a Config and runs
// all health-specific validation.
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

// Validate enforces the health-package invariants.
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
