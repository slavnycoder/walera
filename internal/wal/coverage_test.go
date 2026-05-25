package wal

import (
	"strings"
	"testing"
)

func TestConfig_NewSlotName(t *testing.T) {
	cfg := Config{SlotNamePrefix: "walera"}
	got := cfg.NewSlotName("Host-1.local", 42)
	if got != SlotName("walera_host_1_local_42") {
		t.Errorf("Config.NewSlotName(%q, 42) = %q; want %q", "Host-1.local", string(got), "walera_host_1_local_42")
	}
}

func TestParseNaiveTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantYear int
	}{
		{name: "no fractional seconds", input: "2024-01-02 03:04:05", wantYear: 2024},
		{name: "with fractional seconds", input: "2024-12-31 23:59:59.123456", wantYear: 2024},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, err := parseNaiveTimestamp(tc.input)
			if err != nil {
				t.Fatalf("parseNaiveTimestamp(%q) error: %v", tc.input, err)
			}
			if ts.Year() != tc.wantYear {
				t.Errorf("year = %d; want %d", ts.Year(), tc.wantYear)
			}
		})
	}

	if _, err := parseNaiveTimestamp("not-a-timestamp"); err == nil {
		t.Error("parseNaiveTimestamp(invalid) expected error, got nil")
	}
}

func TestParseTimestampTZ(t *testing.T) {
	tests := []string{
		"2024-01-02 03:04:05+00",
		"2024-01-02 03:04:05+02",
		"2024-01-02 03:04:05-05:30",
		"2024-01-02 03:04:05.123+01",
	}
	for _, in := range tests {
		t.Run(in, func(t *testing.T) {
			if _, err := parseTimestampTZ(in); err != nil {
				t.Errorf("parseTimestampTZ(%q) error: %v", in, err)
			}
		})
	}
	if _, err := parseTimestampTZ("2024-01-02 03:04:05"); err == nil {
		t.Error("parseTimestampTZ(no offset) expected error, got nil")
	}
}

func TestNormalizeOffset(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"2026-05-14 10:23:45.123+00", "2026-05-14 10:23:45.123+00:00"},
		{"2026-05-14 10:23:45.123-05", "2026-05-14 10:23:45.123-05:00"},
		{"2026-05-14 10:23:45-05:30", "2026-05-14 10:23:45-05:30"},
		{"+00", "+00"},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := normalizeOffset(tc.in); got != tc.want {
				t.Errorf("normalizeOffset(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSplitArrayElements(t *testing.T) {
	t.Run("simple unquoted", func(t *testing.T) {
		got, err := splitArrayElements("a,b,c")
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		want := []string{"a", "b", "c"}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("got %v; want %v", got, want)
		}
	})

	t.Run("quoted with escapes", func(t *testing.T) {
		got, err := splitArrayElements(`"a,b","c\"d","e\\f"`)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		if len(got) != 3 || got[0] != "a,b" || got[1] != `c"d` || got[2] != `e\f` {
			t.Errorf("got %v", got)
		}
	})

	t.Run("NULL passthrough", func(t *testing.T) {
		got, err := splitArrayElements("a,NULL,c")
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if len(got) != 3 || got[1] != "NULL" {
			t.Errorf("got %v; expected NULL marker preserved", got)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		got, err := splitArrayElements("")
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("got %v; want empty slice", got)
		}
	})

	t.Run("unterminated quote — best-effort", func(t *testing.T) {

		if _, err := splitArrayElements(`"unterminated`); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
