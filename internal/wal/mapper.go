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

const defaultNaiveTimestampAssumeUTC = true

type valueMapper struct {
	naiveTimestampAssumeUTC bool
}

func newValueMapper(cfg Config) valueMapper {
	return valueMapper{naiveTimestampAssumeUTC: cfg.NaiveTimestampAssumeUTC}
}

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

var pgTimestampFormats = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
}

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

func mapValue(oid uint32, raw []byte, isNull bool) (any, error) {
	return valueMapper{naiveTimestampAssumeUTC: defaultNaiveTimestampAssumeUTC}.mapValue(oid, raw, isNull)
}

func (m valueMapper) mapValue(oid uint32, raw []byte, isNull bool) (any, error) {
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

		return s, nil

	case OIDTimestamp:
		t, err := m.parseNaiveTimestamp(s)
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

		return s, nil

	case OIDTime, OIDTimeTZ:

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

		return json.RawMessage(raw), nil

	case OIDBytea:

		const maxByteaSize = 64 * 1024 * 1024
		if !strings.HasPrefix(s, `\x`) {
			return nil, fmt.Errorf("wal: OID 17 (bytea): expected \\x prefix, got %q", s)
		}
		hexStr := s[2:]
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

	case OIDInt2Array, OIDInt4Array, OIDInt8Array, OIDTextArray, OIDVarcharArray,
		OIDBpcharArray, OIDFloat4Array, OIDFloat8Array, OIDBoolArray, OIDUUIDArray,
		OIDNumericArray:
		elemOID := arrayElementOID[oid]
		return m.parseArray(s, elemOID)

	default:

		return s, nil
	}
}

func parseNaiveTimestamp(s string) (time.Time, error) {
	return valueMapper{naiveTimestampAssumeUTC: defaultNaiveTimestampAssumeUTC}.parseNaiveTimestamp(s)
}

func (m valueMapper) parseNaiveTimestamp(s string) (time.Time, error) {
	for _, layout := range pgTimestampFormats {
		if t, err := time.Parse(layout, s); err == nil {
			if m.naiveTimestampAssumeUTC {

				t = time.Date(t.Year(), t.Month(), t.Day(),
					t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

func parseTimestampTZ(s string) (time.Time, error) {

	for _, layout := range pgTimestampTZFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}

	normalized := normalizeOffset(s)
	for _, layout := range pgTimestampTZFormats {
		if t, err := time.Parse(layout, normalized); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamptz format: %q", s)
}

func normalizeOffset(s string) string {

	n := len(s)
	if n >= 3 {

		last := s[n-3:]
		if (last[0] == '+' || last[0] == '-') &&
			last[1] >= '0' && last[1] <= '9' &&
			last[2] >= '0' && last[2] <= '9' {

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

func parseInterval(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "PT0S", nil
	}

	if strings.HasPrefix(s, "P") {
		return s, nil
	}

	var years, months, days int
	var hours, mins int
	var secs float64

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

				before := strings.TrimSpace(rest[:idx])

				tokens := strings.Fields(before)
				if len(tokens) > 0 {
					if n, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil {
						*unit.dest = n

						after := rest[idx+len(suffix):]

						before2 := strings.Join(tokens[:len(tokens)-1], " ")
						rest = strings.TrimSpace(before2 + " " + after)
						break
					}
				}
			}
		}
	}

	rest = strings.TrimSpace(rest)
	if rest != "" {

		negative := false
		if strings.HasPrefix(rest, "-") {
			negative = true
			rest = rest[1:]
		}

		parts := strings.SplitN(rest, ":", 3)
		if len(parts) != 3 {

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

func (m valueMapper) parseArray(s string, elemOID uint32) (any, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {

		return s, nil
	}
	inner := s[1 : len(s)-1]

	trimmed := strings.TrimSpace(inner)
	if strings.HasPrefix(trimmed, "{") {

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
		v, err := m.mapValue(elemOID, []byte(elem), false)
		if err != nil {
			return nil, fmt.Errorf("wal: parse array element %q: %w", elem, err)
		}
		result = append(result, v)
	}
	return result, nil
}

func splitArrayElements(s string) ([]string, error) {
	var elems []string
	i := 0
	for i < len(s) {

		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			break
		}

		if s[i] == '"' {

			i++
			var sb strings.Builder
			for i < len(s) {
				c := s[i]
				if c == '\\' && i+1 < len(s) {
					i++
					sb.WriteByte(s[i])
				} else if c == '"' {
					i++
					break
				} else {
					sb.WriteByte(c)
				}
				i++
			}
			elems = append(elems, sb.String())

			for i < len(s) && s[i] == ' ' {
				i++
			}
			if i < len(s) && s[i] == ',' {
				i++
			}
		} else {

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
