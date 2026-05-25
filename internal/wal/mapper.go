package wal

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// NaiveTimestampAssumeUTC controls how TIMESTAMP WITHOUT TIME ZONE values are
// interpreted. When true (the default), naive timestamps are treated as UTC and
// formatted as RFC3339Nano with a "Z" suffix. When false, they are formatted
// with the Go time.UTC location but no explicit timezone assumption is made —
// callers should document this behavior for their consumers.
//
// Set once from config at startup (wal.NaiveTimestampAssumeUTC = cfg.WAL.NaiveTimestampAssumeUTC).
// Concurrent reads are safe because the value is only written once before any
// goroutine that calls mapValue is started.
var NaiveTimestampAssumeUTC bool = true

// PostgreSQL type OID constants.
// These are the well-known OIDs from pg_type for standard types.
const (
	OIDInt2        uint32 = 21
	OIDInt4        uint32 = 23
	OIDInt8        uint32 = 20
	OIDFloat4      uint32 = 700
	OIDFloat8      uint32 = 701
	OIDBool        uint32 = 16
	OIDText        uint32 = 25
	OIDVarchar     uint32 = 1043
	OIDBpchar      uint32 = 1042
	OIDUUID        uint32 = 2950
	OIDTimestamp   uint32 = 1114
	OIDTimestampTZ uint32 = 1184
	OIDDate        uint32 = 1082
	OIDTime        uint32 = 1083
	OIDTimeTZ      uint32 = 1266
	OIDInterval    uint32 = 1186
	OIDJSONB       uint32 = 3802
	OIDJSON        uint32 = 114
	OIDBytea       uint32 = 17
	OIDNumeric     uint32 = 1700
	OIDInet        uint32 = 869
	OIDCidr        uint32 = 650

	// Array OIDs.
	OIDInt2Array    uint32 = 1005
	OIDInt4Array    uint32 = 1007
	OIDInt8Array    uint32 = 1016
	OIDTextArray    uint32 = 1009
	OIDVarcharArray uint32 = 1015
	OIDBpcharArray  uint32 = 1014
	OIDFloat4Array  uint32 = 1021
	OIDFloat8Array  uint32 = 1022
	OIDBoolArray    uint32 = 1000
	OIDUUIDArray    uint32 = 2951
	OIDNumericArray uint32 = 1231
)

// arrayElementOID maps an array OID to its element OID, used for recursive element mapping.
var arrayElementOID = map[uint32]uint32{
	OIDInt2Array:    OIDInt2,
	OIDInt4Array:    OIDInt4,
	OIDInt8Array:    OIDInt8,
	OIDTextArray:    OIDText,
	OIDVarcharArray: OIDVarchar,
	OIDBpcharArray:  OIDBpchar,
	OIDFloat4Array:  OIDFloat4,
	OIDFloat8Array:  OIDFloat8,
	OIDBoolArray:    OIDBool,
	OIDUUIDArray:    OIDUUID,
	OIDNumericArray: OIDNumeric,
}

// pgTimestampFormats lists the time formats pgoutput uses for TIMESTAMP WITHOUT TIME ZONE.
// PG outputs naive timestamps in these formats (no timezone designator).
var pgTimestampFormats = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
}

// pgTimestampTZFormats lists the time formats pgoutput uses for TIMESTAMP WITH TIME ZONE.
// PG outputs timestamptz values with a timezone offset.
var pgTimestampTZFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999+07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05+07:00",
	"2006-01-02 15:04:05.999999999 UTC",
	"2006-01-02 15:04:05.999999999 +0000",
	"2006-01-02 15:04:05 +0000",
}

// mapValue converts a pgoutput text-mode column value to a JSON-safe Go value.
//
// oid is the PostgreSQL type OID. raw is the wire bytes in text format (the
// pgoutput text representation). isNull must be true for SQL NULL values (raw
// is ignored in that case).
//
// Returns (nil, nil) for NULL. Returns (string(raw), nil) for unknown OIDs
// (TYPES-03 raw text passthrough). Returns (nil, error) if the value is
// malformed for the expected type.
//
// Security (T-02-01): malformed values return an error rather than panicking.
// Security (T-02-03): bytea payloads are capped at 64 MiB; array nesting is
// limited to 1 level (multidimensional arrays fall back to raw text passthrough).
func mapValue(oid uint32, raw []byte, isNull bool) (any, error) {
	if isNull {
		return nil, nil
	}

	s := string(raw)

	switch oid {
	case OIDInt2, OIDInt4:
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("wal: OID %d: parse int %q: %w", oid, s, err)
		}
		return n, nil

	case OIDInt8:
		// JS-safe: always return as string even if it fits in a JS Number.
		return s, nil

	case OIDFloat4, OIDFloat8:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("wal: OID %d: parse float %q: %w", oid, s, err)
		}
		return f, nil

	case OIDBool:
		switch s {
		case "t", "true":
			return true, nil
		case "f", "false":
			return false, nil
		default:
			return nil, fmt.Errorf("wal: OID 16 (bool): unexpected value %q", s)
		}

	case OIDText, OIDVarchar, OIDBpchar:
		return s, nil

	case OIDUUID:
		return s, nil

	case OIDNumeric:
		// JS-safe: always return as string to preserve precision.
		return s, nil

	case OIDTimestamp:
		t, err := parseNaiveTimestamp(s)
		if err != nil {
			return nil, fmt.Errorf("wal: OID 1114 (timestamp): %w", err)
		}
		return t.UTC().Format(time.RFC3339Nano), nil

	case OIDTimestampTZ:
		t, err := parseTimestampTZ(s)
		if err != nil {
			return nil, fmt.Errorf("wal: OID 1184 (timestamptz): %w", err)
		}
		return t.UTC().Format(time.RFC3339Nano), nil

	case OIDDate:
		// PG date text format is already "YYYY-MM-DD".
		return s, nil

	case OIDTime, OIDTimeTZ:
		// PG time text format is already "HH:MM:SS[.ffffff][+TZ]".
		return s, nil

	case OIDInterval:
		iso, err := parseInterval(s)
		if err != nil {
			return nil, fmt.Errorf("wal: OID 1186 (interval): %w", err)
		}
		return iso, nil

	case OIDJSONB, OIDJSON:
		if !json.Valid(raw) {
			return nil, fmt.Errorf("wal: OID %d (json/jsonb): invalid JSON: %q", oid, s)
		}
		// Return json.RawMessage so JSON marshalling embeds it inline, not as a string.
		return json.RawMessage(raw), nil

	case OIDBytea:
		// pgoutput text-mode bytea uses the hex escape format: \x<hexdigits>.
		// Security (T-02-03): cap decoded length at 64 MiB.
		const maxByteaSize = 64 * 1024 * 1024
		if !strings.HasPrefix(s, `\x`) {
			return nil, fmt.Errorf("wal: OID 17 (bytea): expected \\x prefix, got %q", s)
		}
		hexStr := s[2:] // strip the "\x" prefix
		if len(hexStr)/2 > maxByteaSize {
			return nil, fmt.Errorf("wal: OID 17 (bytea): payload exceeds 64 MiB limit")
		}
		decoded, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("wal: OID 17 (bytea): hex decode: %w", err)
		}
		return base64.StdEncoding.EncodeToString(decoded), nil

	case OIDInet, OIDCidr:
		return s, nil

	// Array OIDs: parse PG text array format "{elem,...}".
	// Supports 1D scalar arrays only. Multi-dim arrays fall back to raw text
	// passthrough per TYPES-03.
	// Security (T-02-03): recursion depth is limited to 1 level.
	case OIDInt2Array, OIDInt4Array, OIDInt8Array, OIDTextArray, OIDVarcharArray,
		OIDBpcharArray, OIDFloat4Array, OIDFloat8Array, OIDBoolArray, OIDUUIDArray,
		OIDNumericArray:
		elemOID := arrayElementOID[oid]
		return parseArray(s, elemOID)

	default:
		// TYPES-03: unknown OID → raw text passthrough.
		return s, nil
	}
}

// parseNaiveTimestamp parses a TIMESTAMP WITHOUT TIME ZONE value from pgoutput
// text format. The value has no timezone indicator (e.g. "2026-05-14 10:23:45.123456").
// If NaiveTimestampAssumeUTC is true, the parsed time is interpreted as UTC.
func parseNaiveTimestamp(s string) (time.Time, error) {
	for _, layout := range pgTimestampFormats {
		if t, err := time.Parse(layout, s); err == nil {
			if NaiveTimestampAssumeUTC {
				// Override: treat the naive timestamp as UTC.
				t = time.Date(t.Year(), t.Month(), t.Day(),
					t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

// parseTimestampTZ parses a TIMESTAMP WITH TIME ZONE value from pgoutput text
// format. The value may have various timezone indicator forms. The returned time
// is in whatever timezone PG sent; callers should call .UTC() to normalize.
func parseTimestampTZ(s string) (time.Time, error) {
	// First, try standard Go formats.
	for _, layout := range pgTimestampTZFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Fallback: try time.ParseInLocation with UTC for bare-UTC forms like
	// "2026-05-14 10:23:45.123456+00" (two-digit offset without colon).
	// Normalize "+00" suffix to "+00:00".
	normalized := normalizeOffset(s)
	for _, layout := range pgTimestampTZFormats {
		if t, err := time.Parse(layout, normalized); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamptz format: %q", s)
}

// normalizeOffset converts PG-style short offsets like "+00", "+02", "-05"
// (without a colon and without minutes) to RFC3339-compatible "+00:00", "+02:00"
// etc. This handles PG's "2026-05-14 10:23:45.123+00" output.
func normalizeOffset(s string) string {
	// Look for a [+-]\d{2}$ or [+-]\d{2}\.\d+$ suffix.
	// We only need to handle +HH and -HH without colon and without minutes.
	n := len(s)
	if n >= 3 {
		// Check for fractional seconds + short offset: "...+02" or "...-05"
		last := s[n-3:]
		if (last[0] == '+' || last[0] == '-') &&
			last[1] >= '0' && last[1] <= '9' &&
			last[2] >= '0' && last[2] <= '9' {
			// Check the char before: if it's a digit or dot, it's part of fractional seconds.
			if n > 3 {
				prev := s[n-4]
				if (prev >= '0' && prev <= '9') || prev == '.' {
					return s[:n-3] + last[:3] + ":00"
				}
			}
		}
	}
	return s
}

// parseInterval converts a PG interval text representation to an ISO-8601
// duration string (e.g. "PT2H30M", "P1Y2M3DT4H5M6S").
//
// PG interval text formats:
//   - postgres_verbose:  "@ 2 hours 30 mins" — not used in pgoutput text mode
//   - postgres:          "2:30:00" or "1 year 2 mons 3 days 04:05:06.789"
//   - iso_8601:          "P1Y2M3DT4H5M6S" — already ISO-8601
//   - sql_standard:      "1-2 3 4:05:06" (years-months days hours:mins:secs)
//
// Supported cases:
//   - Pure time: "HH:MM:SS[.ffffff]" → PT{H}H{M}M{S}S
//   - Mixed:     "[N year[s]] [N mon[s]] [N day[s]] HH:MM:SS[.ffffff]"
//   - ISO-8601 passthrough: already starts with "P"
func parseInterval(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "PT0S", nil
	}
	// If already ISO-8601, pass through.
	if strings.HasPrefix(s, "P") {
		return s, nil
	}

	var years, months, days int
	var hours, mins int
	var secs float64

	// Parse the date part: "[N year[s]] [N mon[s]] [N day[s]]"
	rest := s
	for _, unit := range []struct {
		singular, plural string
		dest             *int
	}{
		{"year", "years", &years},
		{"mon", "mons", &months},
		{"day", "days", &days},
	} {
		for _, suffix := range []string{unit.plural, unit.singular} {
			if idx := strings.Index(rest, suffix); idx > 0 {
				// Find the number before the unit.
				before := strings.TrimSpace(rest[:idx])
				// The number is the last token in before.
				tokens := strings.Fields(before)
				if len(tokens) > 0 {
					if n, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil {
						*unit.dest = n
						// Remove the consumed part from rest.
						after := rest[idx+len(suffix):]
						// Remove the consumed number from before.
						before2 := strings.Join(tokens[:len(tokens)-1], " ")
						rest = strings.TrimSpace(before2 + " " + after)
						break
					}
				}
			}
		}
	}

	// The remaining rest should be "HH:MM:SS[.ffffff]" or empty.
	rest = strings.TrimSpace(rest)
	if rest != "" {
		// Handle negative sign for the time part.
		negative := false
		if strings.HasPrefix(rest, "-") {
			negative = true
			rest = rest[1:]
		}

		parts := strings.SplitN(rest, ":", 3)
		if len(parts) != 3 {
			// Not a time format — return raw passthrough as string.
			return s, nil
		}
		h, err1 := strconv.Atoi(parts[0])
		m, err2 := strconv.Atoi(parts[1])
		sec, err3 := strconv.ParseFloat(parts[2], 64)
		if err1 != nil || err2 != nil || err3 != nil {
			return s, nil
		}
		if negative {
			h, m = -h, -m
			sec = -sec
		}
		hours, mins, secs = h, m, sec
	}

	// Build ISO-8601 duration string. Skip zero components for brevity.
	var sb strings.Builder
	sb.WriteString("P")
	if years != 0 {
		fmt.Fprintf(&sb, "%dY", years)
	}
	if months != 0 {
		fmt.Fprintf(&sb, "%dM", months)
	}
	if days != 0 {
		fmt.Fprintf(&sb, "%dD", days)
	}
	if hours != 0 || mins != 0 || secs != 0 {
		sb.WriteString("T")
		if hours != 0 {
			fmt.Fprintf(&sb, "%dH", hours)
		}
		if mins != 0 {
			fmt.Fprintf(&sb, "%dM", mins)
		}
		if secs != 0 {
			// Format seconds: omit fractional part if it is zero.
			if secs == float64(int(secs)) {
				fmt.Fprintf(&sb, "%dS", int(secs))
			} else {
				fmt.Fprintf(&sb, "%gS", secs)
			}
		}
	}
	result := sb.String()
	if result == "P" {
		return "PT0S", nil
	}
	return result, nil
}

// parseArray parses a PG text-format array string like "{1,2,3}" or
// `{foo,"bar,baz",NULL}` into a []any, mapping each element via mapValue
// with the given element OID.
//
// Rules:
//   - NULL (unquoted, case-insensitive) elements map to nil.
//   - Quoted elements (double-quoted) support \\ and \" escape sequences.
//   - Multi-dimensional arrays (arrays of arrays) fall back to raw text passthrough.
//   - Security: nesting depth > 1 → raw text passthrough (T-02-03).
func parseArray(s string, elemOID uint32) (any, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		// Not a PG array — raw text passthrough.
		return s, nil
	}
	inner := s[1 : len(s)-1]

	// Check for multi-dimensional array: inner starts with '{'.
	// Multi-dim arrays fall back to raw text.
	trimmed := strings.TrimSpace(inner)
	if strings.HasPrefix(trimmed, "{") {
		// Multi-dim array → TYPES-03 raw text passthrough.
		return s, nil
	}

	if trimmed == "" {
		return []any{}, nil
	}

	elems, err := splitArrayElements(inner)
	if err != nil {
		return nil, fmt.Errorf("wal: parse array: %w", err)
	}

	result := make([]any, 0, len(elems))
	for _, elem := range elems {
		if strings.EqualFold(elem, "NULL") {
			result = append(result, nil)
			continue
		}
		v, err := mapValue(elemOID, []byte(elem), false)
		if err != nil {
			return nil, fmt.Errorf("wal: parse array element %q: %w", elem, err)
		}
		result = append(result, v)
	}
	return result, nil
}

// splitArrayElements splits the inner content of a PG array string into
// individual element strings, respecting quoted strings and escape sequences.
// Input: the content between the outer braces (e.g. `foo,"bar,baz",NULL`).
func splitArrayElements(s string) ([]string, error) {
	var elems []string
	i := 0
	for i < len(s) {
		// Skip leading whitespace.
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			break
		}

		if s[i] == '"' {
			// Quoted element: read until closing unescaped quote.
			i++ // skip opening quote
			var sb strings.Builder
			for i < len(s) {
				c := s[i]
				if c == '\\' && i+1 < len(s) {
					i++
					sb.WriteByte(s[i])
				} else if c == '"' {
					i++ // skip closing quote
					break
				} else {
					sb.WriteByte(c)
				}
				i++
			}
			elems = append(elems, sb.String())
			// Consume trailing comma.
			for i < len(s) && s[i] == ' ' {
				i++
			}
			if i < len(s) && s[i] == ',' {
				i++
			}
		} else {
			// Unquoted element: read until comma or end.
			start := i
			for i < len(s) && s[i] != ',' {
				i++
			}
			elem := strings.TrimSpace(s[start:i])
			elems = append(elems, elem)
			if i < len(s) && s[i] == ',' {
				i++
			}
		}
	}
	return elems, nil
}
