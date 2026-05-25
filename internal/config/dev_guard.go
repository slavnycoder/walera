//go:build !dev

// Package config — dev_guard.go is the default-build refusal that prevents
// dev-only env vars from taking effect in a release binary. The companion
// file dev_guard_dev.go (//go:build dev) is in scope only under `-tags dev`
// and provides a no-op implementation so dev builds may set the same
// patterns intentionally.
//
// The three reserved name prefixes (after stripping the WALERA_ namespace)
// are EXPERIMENTAL_, DEBUG_FORCE_, and PLAN_. Any new dev-only escape hatch
// MUST use one of these prefixes so the guard catches it. The runtime
// refusal pairs with the static `make config-check` grep which catches the
// same literals at build time.
package config

import (
	"fmt"
	"regexp"
	"strings"
)

// devEnvPattern matches the part AFTER the WALERA_ prefix that identifies
// a dev-only env var. The three reserved prefixes are EXPERIMENTAL_,
// DEBUG_FORCE_, and PLAN_. Production builds refuse any matching env var
// to prevent dev escape hatches from accidentally enabling in release.
var devEnvPattern = regexp.MustCompile(`^(EXPERIMENTAL|DEBUG_FORCE|PLAN)_`)

// refuseDevEnv scans the provided environment slice (typically os.Environ())
// for WALERA_-prefixed names whose stripped form matches devEnvPattern.
// Returns an actionable error on the FIRST match; nil otherwise.
//
// The companion file dev_guard_dev.go (build tag `dev`) defines a no-op
// version that always returns nil — that file is in scope only under
// `-tags dev`, so production builds always link this strict version.
func refuseDevEnv(env []string) error {
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, "WALERA_") {
			continue
		}
		stripped := strings.TrimPrefix(name, "WALERA_")
		if devEnvPattern.MatchString(stripped) {
			return fmt.Errorf(
				"refusing dev-only env var %q in non-dev build; remove it or rebuild with -tags dev",
				name,
			)
		}
	}
	return nil
}
