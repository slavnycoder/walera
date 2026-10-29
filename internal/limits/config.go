package limits

import (
	"errors"
	"fmt"
)

type Config struct {
	GlobalConcurrent int

	PerUserConcurrentMax int
}

// Defaults are Walera's admission limits; only GlobalConcurrent is
// configurable (WALERA_MAX_CONNECTIONS).
func Defaults() Config {
	return Config{
		GlobalConcurrent:     50000,
		PerUserConcurrentMax: 10,
	}
}

func (c Config) Validate() error {
	var errs []error
	if c.GlobalConcurrent <= 0 {
		errs = append(errs, errors.New("WALERA_MAX_CONNECTIONS must be > 0"))
	}
	if c.PerUserConcurrentMax <= 0 {
		errs = append(errs, errors.New("limits: per-user connection limit must be > 0"))
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
