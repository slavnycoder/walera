//go:build dev

package config

import "testing"

// TestRefuseDevEnv_AcceptsAllPatterns_DevBuild asserts that the dev-build
// no-op accepts every reserved pattern. Runs only under `go test -tags dev`.
func TestRefuseDevEnv_AcceptsAllPatterns_DevBuild(t *testing.T) {
	cases := []struct {
		name string
		env  []string
	}{
		{"Experimental", []string{"WALERA_EXPERIMENTAL_X=1"}},
		{"DebugForce", []string{"WALERA_DEBUG_FORCE_Y=1"}},
		{"Plan", []string{"WALERA_PLAN_Z=1"}},
		{"All", []string{"WALERA_EXPERIMENTAL_X=1", "WALERA_DEBUG_FORCE_Y=1", "WALERA_PLAN_Z=1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := refuseDevEnv(tc.env); err != nil {
				t.Fatalf("dev-build refuseDevEnv(%v) = %v; want nil", tc.env, err)
			}
		})
	}
}
