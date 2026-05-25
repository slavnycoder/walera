package config

import (
	"fmt"
	"net/url"
	"strings"
)

func FormatError(key, value, problem, remediation string) error {
	if remediation == "" {
		return fmt.Errorf("config: %s (%s) %s", key, value, problem)
	}
	return fmt.Errorf("config: %s (%s) %s; %s", key, value, problem, remediation)
}

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

			searchFrom = start + len("***")
		}
	}
	return out
}
