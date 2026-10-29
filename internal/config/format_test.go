package config_test

import (
	"testing"

	"github.com/walera/walera/internal/config"
)

func TestFormatError(t *testing.T) {
	cases := []struct {
		name        string
		key         string
		value       string
		problem     string
		remediation string
		want        string
	}{
		{
			name:        "happy",
			key:         "wal.publication_name",
			value:       "bad-name",
			problem:     "must match PG identifier",
			remediation: "use [A-Za-z_][A-Za-z0-9_]*",
			want:        "wal.publication_name (bad-name) must match PG identifier; use [A-Za-z_][A-Za-z0-9_]*",
		},
		{
			name:    "empty remediation collapses trailing punctuation",
			key:     "wal.publication_name",
			value:   "bad-name",
			problem: "must match PG identifier",
			want:    "wal.publication_name (bad-name) must match PG identifier",
		},
		{
			name:        "empty value is omitted",
			key:         "WALERA_PUBLICATION",
			problem:     "must match PG identifier",
			remediation: "use [A-Za-z_][A-Za-z0-9_]*",
			want:        "WALERA_PUBLICATION must match PG identifier; use [A-Za-z_][A-Za-z0-9_]*",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := config.FormatError(tc.key, tc.value, tc.problem, tc.remediation)
			if err == nil {
				t.Fatal("FormatError returned nil error")
			}
			if got := err.Error(); got != tc.want {
				t.Errorf("FormatError = %q; want %q", got, tc.want)
			}
		})
	}
}
