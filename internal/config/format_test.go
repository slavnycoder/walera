package config_test

import (
	"strings"
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
			want:        "config: wal.publication_name (bad-name) must match PG identifier; use [A-Za-z_][A-Za-z0-9_]*",
		},
		{
			name:    "empty remediation collapses trailing punctuation",
			key:     "wal.publication_name",
			value:   "bad-name",
			problem: "must match PG identifier",
			want:    "config: wal.publication_name (bad-name) must match PG identifier",
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

func TestRedactDSN(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{

			name: "url form with password",
			in:   "postgres://alice:secret@db:5432/walera",
			want: "postgres://alice:%2A%2A%2A@db:5432/walera",
		},
		{
			name: "url form postgresql:// prefix",
			in:   "postgresql://alice:secret@db:5432/walera?sslmode=require",
			want: "postgresql://alice:%2A%2A%2A@db:5432/walera?sslmode=require",
		},
		{
			name: "url form without password",
			in:   "postgres://alice@db:5432/walera",
			want: "postgres://alice@db:5432/walera",
		},
		{
			name: "keyword form",
			in:   "host=db password=secret user=alice",
			want: "host=db password=*** user=alice",
		},
		{
			name: "keyword form upper-case PASSWORD",
			in:   "host=db PASSWORD=secret user=alice",
			want: "host=db PASSWORD=*** user=alice",
		},
		{
			name: "no password returns unchanged",
			in:   "host=db user=alice",
			want: "host=db user=alice",
		},
		{
			name: "unparseable postgres URL collapses to ***",
			in:   "postgres://%zz",
			want: "***",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := config.RedactDSN(tc.in)
			if got != tc.want {
				t.Errorf("RedactDSN(%q) = %q; want %q", tc.in, got, tc.want)
			}

			if strings.Contains(got, "secret") {
				t.Errorf("RedactDSN(%q) = %q; leaks 'secret'", tc.in, got)
			}
		})
	}
}
