package router

import (
	"errors"
	"fmt"
	"time"

	"github.com/knadh/koanf/v2"
)

type Config struct {
	ExactBuffer int `koanf:"exact_buffer"`

	WildcardBuffer int `koanf:"wildcard_buffer"`

	MaxChangesPerTx int `koanf:"max_changes_per_tx"`

	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`
}

func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("router.exact_buffer", 64)
	_ = k.Set("router.wildcard_buffer", 512)
	_ = k.Set("router.max_changes_per_tx", 10000)
	_ = k.Set("router.heartbeat_interval", "15s")
}

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
