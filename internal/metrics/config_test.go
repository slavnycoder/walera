package metrics_test

import (
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/metrics"
)

func newK(t *testing.T) *koanf.Koanf {
	t.Helper()
	k := koanf.New(".")
	metrics.ApplyDefaults(k)
	return k
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := metrics.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.SampleInterval != 30*time.Second {
		t.Errorf("SampleInterval = %s; want 30s", cfg.SampleInterval)
	}
}

func TestLoadConfig_RejectsNonPositive(t *testing.T) {
	k := newK(t)
	_ = k.Set("metrics.sample_interval", "0s")
	_, err := metrics.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "metrics.sample_interval must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want sample_interval > 0", err)
	}
}
