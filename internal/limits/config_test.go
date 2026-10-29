package limits_test

import (
	"strings"
	"testing"

	"github.com/walera/walera/internal/limits"
)

func TestDefaults(t *testing.T) {
	cfg := limits.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Defaults().Validate: %v", err)
	}
	if cfg.GlobalConcurrent != 50000 {
		t.Errorf("GlobalConcurrent = %d; want 50000", cfg.GlobalConcurrent)
	}
	if cfg.PerUserConcurrentMax != 10 {
		t.Errorf("PerUserConcurrentMax = %d; want 10", cfg.PerUserConcurrentMax)
	}
}

func TestValidate_RequiresPositiveGlobalConcurrent(t *testing.T) {
	cfg := limits.Defaults()
	cfg.GlobalConcurrent = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "WALERA_MAX_CONNECTIONS must be > 0") {
		t.Fatalf("Validate: err = %v; want WALERA_MAX_CONNECTIONS > 0", err)
	}
}

func TestValidate_RequiresPositivePerUserConcurrent(t *testing.T) {
	cfg := limits.Defaults()
	cfg.PerUserConcurrentMax = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "per-user connection limit must be > 0") {
		t.Fatalf("Validate: err = %v; want per-user limit > 0", err)
	}
}
