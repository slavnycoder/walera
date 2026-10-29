package router

import (
	"errors"
	"fmt"
	"time"
)

type Config struct {
	ExactBuffer int

	WildcardBuffer int

	MaxChangesPerTx int

	HeartbeatInterval time.Duration
}

// Defaults are Walera's fixed routing parameters; they are not configurable.
func Defaults() Config {
	return Config{
		ExactBuffer:       64,
		WildcardBuffer:    512,
		MaxChangesPerTx:   10000,
		HeartbeatInterval: 15 * time.Second,
	}
}

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
