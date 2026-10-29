package health

import (
	"errors"
	"fmt"
	"time"
)

type Config struct {
	ReadyzProbeInterval time.Duration
}

// Defaults are Walera's fixed health-probe parameters; they are not configurable.
func Defaults() Config {
	return Config{ReadyzProbeInterval: 5 * time.Second}
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
