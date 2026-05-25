// Package config — format.go owns the small shared helpers per-package
// validators use to emit error messages in the project's standard shape
// and to redact passwords from any DSN that bleeds into a user-visible
// message.
//
// The standard shape (D-13) is:
//
//	config: <key> (<value>) <problem>; <remediation>
//
// Empty remediation collapses the trailing "; " so the format reads
// cleanly when no remediation is available.
//
// FormatError and RedactDSN are stdlib-only so internal/config stays a
// leaf — adding either does not introduce a sibling-internal import.
package config

import (
	"fmt"
	"net/url"
	"strings"
)

// FormatError produces a config validation error in the standard format:
//
//	config: <key> (<value>) <problem>; <remediation>
//
// Use RedactDSN on any value that may contain a password before passing
// it to FormatError. An empty remediation collapses the trailing "; ".
func FormatError(key, value, problem, remediation string) error {
	if remediation == "" {
		return fmt.Errorf("config: %s (%s) %s", key, value, problem)
	}
	return fmt.Errorf("config: %s (%s) %s; %s", key, value, problem, remediation)
}

// RedactDSN returns the input DSN with the password component replaced
// by "***". Accepts both URL-style ("postgres://user:pass@host/db") and
// keyword-style ("host=... password=... ...") DSNs. URL-style input that
// fails to parse collapses to "***" rather than leaking the raw string.
//
// DSNs without a password are returned unchanged so the helper is safe
// to call unconditionally on any DSN-bearing value.
func RedactDSN(raw string) string {
	if strings.HasPrefix(raw, "postgres://") || strings.HasPrefix(raw, "postgresql://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "***"
		}
		if u.User != nil {
			if _, hasPass := u.User.Password(); hasPass {
				u.User = url.UserPassword(u.User.Username(), "***")
			}
		}
		return u.String()
	}
	// Keyword form: rewrite any password=... token (case-sensitive prefix
	// match — covers the canonical libpq form `password=secret`).
	out := raw
	for _, prefix := range []string{"password=", "PASSWORD="} {
		searchFrom := 0
		for {
			rel := strings.Index(out[searchFrom:], prefix)
			if rel < 0 {
				break
			}
			i := searchFrom + rel
			start := i + len(prefix)
			end := start
			for end < len(out) && out[end] != ' ' && out[end] != '\t' {
				end++
			}
			out = out[:start] + "***" + out[end:]
			// Advance past the redacted token so the next iteration cannot
			// re-match the same position.
			searchFrom = start + len("***")
		}
	}
	return out
}
