package limits_test

import (
	"strings"
	"testing"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/limits"
)

func newK(t *testing.T) *koanf.Koanf {
	t.Helper()
	k := koanf.New(".")
	limits.ApplyDefaults(k)
	return k
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := limits.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.GlobalConcurrent != 50000 {
		t.Errorf("GlobalConcurrent = %d; want 50000", cfg.GlobalConcurrent)
	}
	if cfg.PerUserConcurrentMax != 10 {
		t.Errorf("PerUserConcurrentMax = %d; want 10", cfg.PerUserConcurrentMax)
	}
}

func TestLoadConfig_RequiresPositiveGlobalConcurrent(t *testing.T) {
	k := newK(t)
	_ = k.Set("limits.global_concurrent", 0)
	_, err := limits.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "limits.global_concurrent must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want global_concurrent > 0", err)
	}
}

func TestLoadConfig_RequiresPositivePerUserConcurrent(t *testing.T) {
	k := newK(t)
	_ = k.Set("limits.per_user_concurrent", 0)
	_, err := limits.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "limits.per_user_concurrent must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want per_user_concurrent > 0", err)
	}
}
