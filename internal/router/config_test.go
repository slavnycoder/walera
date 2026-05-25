package router_test

import (
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/router"
)

func newK(t *testing.T) *koanf.Koanf {
	t.Helper()
	k := koanf.New(".")
	router.ApplyDefaults(k)
	return k
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := router.LoadConfig(newK(t))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
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

func TestLoadConfig_RejectsNonPositiveMaxChangesPerTx(t *testing.T) {
	k := newK(t)
	_ = k.Set("router.max_changes_per_tx", 0)
	_, err := router.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "router.max_changes_per_tx must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want max_changes_per_tx > 0", err)
	}
}

func TestLoadConfig_RejectsNonPositiveExactBuffer(t *testing.T) {
	k := newK(t)
	_ = k.Set("router.exact_buffer", 0)
	_, err := router.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "router.exact_buffer must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want exact_buffer > 0", err)
	}
}

func TestLoadConfig_RejectsNonPositiveHeartbeat(t *testing.T) {
	k := newK(t)
	_ = k.Set("router.heartbeat_interval", "0s")
	_, err := router.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "router.heartbeat_interval must be > 0") {
		t.Fatalf("LoadConfig: err = %v; want heartbeat_interval > 0", err)
	}
}
