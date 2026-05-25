//go:build !dev

package config

import (
	"fmt"
	"regexp"
	"strings"
)

var devEnvPattern = regexp.MustCompile(`^(EXPERIMENTAL|DEBUG_FORCE|PLAN)_`)

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
