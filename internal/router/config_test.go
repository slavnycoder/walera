package router_test

import (
	"strings"
	"testing"
	"time"

	"github.com/walera/walera/internal/router"
)

func TestDefaults(t *testing.T) {
	cfg := router.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Defaults().Validate: %v", err)
	}
	if cfg.ExactBuffer != 64 {
		t.Errorf("ExactBuffer = %d; want 64", cfg.ExactBuffer)
	}
	if cfg.WildcardBuffer != 512 {
		t.Errorf("WildcardBuffer = %d; want 512", cfg.WildcardBuffer)
	}
	if cfg.HeartbeatInterval != 15*time.Second {
		t.Errorf("HeartbeatInterval = %s; want 15s", cfg.HeartbeatInterval)
	}
	if cfg.MaxChangesPerTx != 10000 {
		t.Errorf("MaxChangesPerTx = %d; want 10000", cfg.MaxChangesPerTx)
	}
}

func TestValidate_RejectsNonPositive(t *testing.T) {
	cases := map[string]func(*router.Config){
		"router.exact_buffer must be > 0":       func(c *router.Config) { c.ExactBuffer = 0 },
		"router.wildcard_buffer must be > 0":    func(c *router.Config) { c.WildcardBuffer = 0 },
		"router.max_changes_per_tx must be > 0": func(c *router.Config) { c.MaxChangesPerTx = 0 },
		"router.heartbeat_interval must be > 0": func(c *router.Config) { c.HeartbeatInterval = 0 },
	}
	for want, mutate := range cases {
		t.Run(want, func(t *testing.T) {
			cfg := router.Defaults()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate: err = %v; want %q", err, want)
			}
		})
	}
}
