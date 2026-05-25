//go:build !dev

package config

import (
	"strings"
	"testing"
)

func TestRefuseDevEnv(t *testing.T) {
	cases := []struct {
		name        string
		env         []string
		wantErr     bool
		wantSubstrs []string
	}{
		{
			name:        "RefusesExperimental",
			env:         []string{"WALERA_EXPERIMENTAL_X=1"},
			wantErr:     true,
			wantSubstrs: []string{"WALERA_EXPERIMENTAL_X", "-tags dev"},
		},
		{
			name:        "RefusesDebugForce",
			env:         []string{"WALERA_DEBUG_FORCE_Y=1"},
			wantErr:     true,
			wantSubstrs: []string{"WALERA_DEBUG_FORCE_Y", "-tags dev"},
		},
		{
			name:        "RefusesPlan",
			env:         []string{"WALERA_PLAN_Z=1"},
			wantErr:     true,
			wantSubstrs: []string{"WALERA_PLAN_Z", "-tags dev"},
		},
		{
			name: "PassesBenign",
			env: []string{
				"WALERA_WAL_POSTGRES_DSN=postgres://x/y",
				"WALERA_LOG_LEVEL=info",
				"PATH=/usr/bin",

				"WALERA_EXPERIMENTATION=ok",
			},
			wantErr: false,
		},
		{
			name: "StopsAtFirstMatch",
			env: []string{
				"WALERA_EXPERIMENTAL_FIRST=1",
				"WALERA_PLAN_SECOND=1",
			},
			wantErr:     true,
			wantSubstrs: []string{"WALERA_EXPERIMENTAL_FIRST"},
		},
		{
			name: "IgnoresNonWaleraPrefix",
			env: []string{
				"EXPERIMENTAL_X=1",
				"DEBUG_FORCE_Y=1",
				"PLAN_Z=1",
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseDevEnv(tc.env)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("refuseDevEnv(%v) = nil; want error", tc.env)
				}
				for _, s := range tc.wantSubstrs {
					if !strings.Contains(err.Error(), s) {
						t.Errorf("refuseDevEnv error = %q; want substring %q", err.Error(), s)
					}
				}
			} else if err != nil {
				t.Fatalf("refuseDevEnv(%v) = %v; want nil", tc.env, err)
			}
		})
	}
}

func TestLoadKoanf_RefusesDevEnv(t *testing.T) {
	t.Setenv("WALERA_EXPERIMENTAL_FOO", "1")
	_, err := LoadKoanf("", nil)
	if err == nil {
		t.Fatal("LoadKoanf: err = nil; want refusal")
	}
	if !strings.Contains(err.Error(), "config:") {
		t.Errorf("LoadKoanf err = %q; want config: prefix", err.Error())
	}
	if !strings.Contains(err.Error(), "WALERA_EXPERIMENTAL_FOO") {
		t.Errorf("LoadKoanf err = %q; want WALERA_EXPERIMENTAL_FOO in message", err.Error())
	}
}
