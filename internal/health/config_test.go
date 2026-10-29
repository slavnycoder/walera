package health_test

import (
	"strings"
	"testing"
	"time"

	"github.com/walera/walera/internal/health"
)

func TestDefaults(t *testing.T) {
	cfg := health.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Defaults().Validate: %v", err)
	}
	if cfg.ReadyzProbeInterval != 5*time.Second {
		t.Errorf("ReadyzProbeInterval = %s; want 5s", cfg.ReadyzProbeInterval)
	}
}

func TestValidate_RejectsNonPositiveInterval(t *testing.T) {
	cfg := health.Defaults()
	cfg.ReadyzProbeInterval = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "health.readyz_probe_interval must be > 0") {
		t.Fatalf("Validate: err = %v; want readyz_probe_interval > 0", err)
	}
}
