package health_test

import (
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/health"
)

func newK(t *testing.T) *koanf.Koanf {
	t.Helper()
	k := koanf.New(".")
	health.ApplyDefaults(k)
	return k
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := health.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ReadyzProbeInterval != 5*time.Second {
		t.Errorf("ReadyzProbeInterval = %s; want 5s", cfg.ReadyzProbeInterval)
	}
}

func TestLoadConfig_RejectsNonPositiveInterval(t *testing.T) {
	k := newK(t)
	_ = k.Set("health.readyz_probe_interval", "0s")
	_, err := health.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "health.readyz_probe_interval must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want readyz_probe_interval > 0", err)
	}
}
