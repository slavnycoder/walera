package config

import "fmt"

// FormatError builds a config error. It carries no "config:" prefix; the
// loader adds that once. An empty value is omitted, for callers
// that must not echo what the operator set.
func FormatError(key, value, problem, remediation string) error {
	if value == "" {
		if remediation == "" {
			return fmt.Errorf("%s %s", key, problem)
		}
		return fmt.Errorf("%s %s; %s", key, problem, remediation)
	}
	if remediation == "" {
		return fmt.Errorf("%s (%s) %s", key, value, problem)
	}
	return fmt.Errorf("%s (%s) %s; %s", key, value, problem, remediation)
}
