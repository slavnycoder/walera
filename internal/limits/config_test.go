package limits_test

import (
	"strings"
	"testing"
	"time"

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
	if cfg.SweepInterval != time.Minute {
		t.Errorf("SweepInterval = %s; want 1m", cfg.SweepInterval)
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

func TestLoadConfig_TrustedProxiesValidCIDRs(t *testing.T) {
	k := newK(t)
	_ = k.Set("limits.trusted_proxies", []string{"10.0.0.0/8", "192.168.0.0/16"})
	cfg, err := limits.LoadConfig(k)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.TrustedProxies) != 2 {
		t.Errorf("TrustedProxies = %v; want 2 entries", cfg.TrustedProxies)
	}
}

func TestLoadConfig_TrustedProxiesRejectsBadCIDR(t *testing.T) {
	k := newK(t)
	_ = k.Set("limits.trusted_proxies", []string{"not-a-cidr"})
	_, err := limits.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "limits.trusted_proxies[0]") {
		t.Fatalf("LoadConfig: err = %v; want trusted_proxies CIDR error", err)
	}
}

func TestLoadConfig_SchemaRules(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(*koanf.Koanf)
		wantSub string
	}{
		{
			name:    "per_user_rate_per_second zero",
			setup:   func(k *koanf.Koanf) { _ = k.Set("limits.per_user_rate_per_second", 0.0) },
			wantSub: "limits.per_user_rate_per_second",
		},
		{
			name:    "per_user_burst zero",
			setup:   func(k *koanf.Koanf) { _ = k.Set("limits.per_user_burst", 0) },
			wantSub: "limits.per_user_burst",
		},
		{
			name:    "pre_auth_rate_per_second zero",
			setup:   func(k *koanf.Koanf) { _ = k.Set("limits.pre_auth_rate_per_second", 0.0) },
			wantSub: "limits.pre_auth_rate_per_second",
		},
		{
			name:    "pre_auth_burst zero",
			setup:   func(k *koanf.Koanf) { _ = k.Set("limits.pre_auth_burst", 0) },
			wantSub: "limits.pre_auth_burst",
		},
		{
			name:    "sweep_interval zero",
			setup:   func(k *koanf.Koanf) { _ = k.Set("limits.sweep_interval", "0s") },
			wantSub: "limits.sweep_interval",
		},
		{
			name:    "sweep_idle_threshold zero",
			setup:   func(k *koanf.Koanf) { _ = k.Set("limits.sweep_idle_threshold", "0s") },
			wantSub: "limits.sweep_idle_threshold",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := newK(t)
			tc.setup(k)
			_, err := limits.LoadConfig(k)
			if err == nil {
				t.Fatalf("LoadConfig: err = nil; want error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q; want substring %q", err.Error(), tc.wantSub)
			}
		})
	}
}

var _ = time.Second

func TestLoadConfig_CombinationRules(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(*koanf.Koanf)
		wantSub string
	}{
		{
			name: "per_user burst < rate",
			setup: func(k *koanf.Koanf) {
				_ = k.Set("limits.per_user_burst", 1)
				_ = k.Set("limits.per_user_rate_per_second", 10.0)
			},
			wantSub: "limits.per_user_burst vs limits.per_user_rate_per_second",
		},
		{
			name: "pre_auth burst < rate",
			setup: func(k *koanf.Koanf) {
				_ = k.Set("limits.pre_auth_burst", 1)
				_ = k.Set("limits.pre_auth_rate_per_second", 10.0)
			},
			wantSub: "limits.pre_auth_burst vs limits.pre_auth_rate_per_second",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := newK(t)
			tc.setup(k)
			_, err := limits.LoadConfig(k)
			if err == nil {
				t.Fatalf("LoadConfig: err = nil; want error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q; want substring %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestLoadConfig_CombinationRules_BurstEqualToRateOK(t *testing.T) {
	k := newK(t)
	_ = k.Set("limits.per_user_burst", 5)
	_ = k.Set("limits.per_user_rate_per_second", 5.0)
	_ = k.Set("limits.pre_auth_burst", 5)
	_ = k.Set("limits.pre_auth_rate_per_second", 5.0)
	if _, err := limits.LoadConfig(k); err != nil {
		t.Errorf("LoadConfig: %v; want nil at burst==rate", err)
	}
}
