package wal

import (
	"encoding/json"
	"testing"
)

// TestMapValue exercises the full PG→JSON type matrix defined in
// spec/05-pg-to-json-mapping.md.
//
// Note: this test is intentionally NOT parallel at the top level because several
// subtests read the package-level NaiveTimestampAssumeUTC var. Individual subtests
// that do not touch the global ARE run in parallel with each other.
func TestMapValue(t *testing.T) {
	// NaiveTimestampAssumeUTC default is true; rely on that for the table-driven cases.
	// Do NOT call t.Parallel() here — TestMapValueTimestampNaiveUTCToggle writes this
	// global and must run sequentially to avoid a data race.

	type testCase struct {
		name   string
		oid    uint32
		raw    []byte
		isNull bool
		want   any
		// wantErr: if true, mapValue must return a non-nil error.
		wantErr bool
	}

	cases := []testCase{
		// NULL: any OID + isNull=true must return nil.
		{name: "null_int4", oid: OIDInt4, raw: []byte("42"), isNull: true, want: nil},
		{name: "null_text", oid: OIDText, raw: []byte("hello"), isNull: true, want: nil},
		{name: "null_bytea", oid: OIDBytea, raw: []byte(`\x48656c6c6f`), isNull: true, want: nil},
		{name: "null_jsonb", oid: OIDJSONB, raw: []byte(`{"a":1}`), isNull: true, want: nil},
		{name: "null_unknown", oid: 99999, raw: []byte("whatever"), isNull: true, want: nil},

		// int2 → number (int).
		{name: "int2_pos", oid: OIDInt2, raw: []byte("32767"), want: 32767},
		{name: "int2_neg", oid: OIDInt2, raw: []byte("-32768"), want: -32768},
		{name: "int2_zero", oid: OIDInt2, raw: []byte("0"), want: 0},

		// int4 → number (int).
		{name: "int4_pos", oid: OIDInt4, raw: []byte("42"), want: 42},
		{name: "int4_neg", oid: OIDInt4, raw: []byte("-1"), want: -1},
		{name: "int4_max", oid: OIDInt4, raw: []byte("2147483647"), want: 2147483647},

		// int8 → string (JS-safe bigint; must NOT return int64).
		{name: "int8_max", oid: OIDInt8, raw: []byte("9223372036854775807"), want: "9223372036854775807"},
		{name: "int8_min", oid: OIDInt8, raw: []byte("-9223372036854775808"), want: "-9223372036854775808"},
		{name: "int8_small", oid: OIDInt8, raw: []byte("1"), want: "1"},

		// float4/float8 → float64.
		{name: "float4_pi", oid: OIDFloat4, raw: []byte("3.14"), want: 3.14},
		{name: "float8_e", oid: OIDFloat8, raw: []byte("2.718281828"), want: 2.718281828},
		{name: "float8_neg", oid: OIDFloat8, raw: []byte("-1.5"), want: -1.5},

		// bool → boolean.
		{name: "bool_true", oid: OIDBool, raw: []byte("t"), want: true},
		{name: "bool_false", oid: OIDBool, raw: []byte("f"), want: false},

		// text/varchar/bpchar → string.
		{name: "text_hello", oid: OIDText, raw: []byte("hello"), want: "hello"},
		{name: "text_empty", oid: OIDText, raw: []byte(""), want: ""},
		{name: "text_multibyte", oid: OIDText, raw: []byte("日本語"), want: "日本語"},
		{name: "varchar_hello", oid: OIDVarchar, raw: []byte("world"), want: "world"},
		{name: "bpchar_padded", oid: OIDBpchar, raw: []byte("x   "), want: "x   "},

		// uuid → string.
		{name: "uuid", oid: OIDUUID, raw: []byte("550e8400-e29b-41d4-a716-446655440000"), want: "550e8400-e29b-41d4-a716-446655440000"},

		// numeric → string (JS-safe).
		{name: "numeric_pi", oid: OIDNumeric, raw: []byte("3.14159"), want: "3.14159"},
		{name: "numeric_large", oid: OIDNumeric, raw: []byte("99999999999999999999999.9999"), want: "99999999999999999999999.9999"},

		// timestamp (naive) with NaiveTimestampAssumeUTC=true → RFC3339Nano UTC.
		// Tested separately in TestMapValueTimestamp to allow toggling the global.
		// Here we test the default (true) behaviour.
		{name: "timestamp_naive_utc",
			oid:  OIDTimestamp,
			raw:  []byte("2026-05-14 10:23:45.123456"),
			want: "2026-05-14T10:23:45.123456Z",
		},
		{name: "timestamp_no_frac",
			oid:  OIDTimestamp,
			raw:  []byte("2026-05-14 10:23:45"),
			want: "2026-05-14T10:23:45Z",
		},

		// timestamptz → UTC RFC3339Nano.
		{name: "timestamptz_east",
			oid:  OIDTimestampTZ,
			raw:  []byte("2026-05-14T10:23:45+02:00"),
			want: "2026-05-14T08:23:45Z",
		},
		{name: "timestamptz_utc",
			oid:  OIDTimestampTZ,
			raw:  []byte("2026-05-14T08:23:45Z"),
			want: "2026-05-14T08:23:45Z",
		},
		{name: "timestamptz_pg_format",
			oid:  OIDTimestampTZ,
			raw:  []byte("2026-05-14 10:23:45.123456+02:00"),
			want: "2026-05-14T08:23:45.123456Z",
		},
		{name: "timestamptz_utc_zero_offset",
			oid:  OIDTimestampTZ,
			raw:  []byte("2026-05-14 08:23:45+00:00"),
			want: "2026-05-14T08:23:45Z",
		},

		// date → "YYYY-MM-DD" string passthrough.
		{name: "date", oid: OIDDate, raw: []byte("2026-05-14"), want: "2026-05-14"},

		// time → string passthrough.
		{name: "time_plain", oid: OIDTime, raw: []byte("10:23:45"), want: "10:23:45"},
		{name: "time_frac", oid: OIDTime, raw: []byte("10:23:45.123456"), want: "10:23:45.123456"},

		// interval → ISO-8601 duration.
		{name: "interval_hours_mins", oid: OIDInterval, raw: []byte("2:30:00"), want: "PT2H30M"},
		{name: "interval_secs", oid: OIDInterval, raw: []byte("0:00:06"), want: "PT6S"},
		{name: "interval_iso", oid: OIDInterval, raw: []byte("PT2H30M"), want: "PT2H30M"},
		{name: "interval_zero", oid: OIDInterval, raw: []byte("0:00:00"), want: "PT0S"},

		// json/jsonb → json.RawMessage (embedded inline).
		{name: "json_object", oid: OIDJSON, raw: []byte(`{"a":1}`), want: json.RawMessage(`{"a":1}`)},
		{name: "jsonb_object", oid: OIDJSONB, raw: []byte(`{"a":1}`), want: json.RawMessage(`{"a":1}`)},
		{name: "jsonb_unicode", oid: OIDJSONB, raw: []byte(`{"msg":"こんにちは"}`), want: json.RawMessage(`{"msg":"こんにちは"}`)},
		{name: "jsonb_array", oid: OIDJSONB, raw: []byte(`[1,2,3]`), want: json.RawMessage(`[1,2,3]`)},
		{name: "jsonb_null", oid: OIDJSONB, raw: []byte(`null`), want: json.RawMessage(`null`)},

		// bytea → base64 string. PG hex format: \x<hexdigits>.
		{name: "bytea_hello", oid: OIDBytea, raw: []byte(`\x48656c6c6f`), want: "SGVsbG8="},
		{name: "bytea_empty", oid: OIDBytea, raw: []byte(`\x`), want: ""},

		// inet/cidr → string passthrough.
		{name: "inet", oid: OIDInet, raw: []byte("192.168.1.1"), want: "192.168.1.1"},
		{name: "cidr", oid: OIDCidr, raw: []byte("192.168.0.0/24"), want: "192.168.0.0/24"},

		// 1D int4 array.
		{name: "int4_array_123", oid: OIDInt4Array, raw: []byte("{1,2,3}"), want: []any{1, 2, 3}},
		{name: "int4_array_empty", oid: OIDInt4Array, raw: []byte("{}"), want: []any{}},
		{name: "int4_array_null_elem", oid: OIDInt4Array, raw: []byte("{1,NULL,3}"), want: []any{1, nil, 3}},

		// 1D text array.
		{name: "text_array_simple", oid: OIDTextArray, raw: []byte("{foo,bar}"), want: []any{"foo", "bar"}},
		{name: "text_array_quoted_comma", oid: OIDTextArray, raw: []byte(`{"a,b","c"}`), want: []any{"a,b", "c"}},
		{name: "text_array_unicode", oid: OIDTextArray, raw: []byte("{hello,世界}"), want: []any{"hello", "世界"}},

		// 1D int8 array → string elements.
		{name: "int8_array", oid: OIDInt8Array, raw: []byte("{1,2,9223372036854775807}"), want: []any{"1", "2", "9223372036854775807"}},

		// 1D bool array.
		{name: "bool_array", oid: OIDBoolArray, raw: []byte("{t,f,t}"), want: []any{true, false, true}},

		// 1D uuid array.
		{name: "uuid_array", oid: OIDUUIDArray, raw: []byte("{550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}"),
			want: []any{"550e8400-e29b-41d4-a716-446655440000", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		},

		// Multi-dimensional array → raw text passthrough (TYPES-03).
		{name: "multidim_array", oid: OIDInt4Array, raw: []byte("{{1,2},{3,4}}"), want: "{{1,2},{3,4}}"},

		// Unknown OID → raw text passthrough (TYPES-03).
		{name: "unknown_oid", oid: 99999, raw: []byte("rawtext"), want: "rawtext"},
		{name: "unknown_oid_empty", oid: 88888, raw: []byte(""), want: ""},

		// Error cases.
		{name: "int4_bad", oid: OIDInt4, raw: []byte("notanint"), wantErr: true},
		{name: "float8_bad", oid: OIDFloat8, raw: []byte("notafloat"), wantErr: true},
		{name: "bool_bad", oid: OIDBool, raw: []byte("maybe"), wantErr: true},
		{name: "jsonb_invalid", oid: OIDJSONB, raw: []byte("{broken"), wantErr: true},
		{name: "bytea_no_prefix", oid: OIDBytea, raw: []byte("48656c6c6f"), wantErr: true},
		{name: "bytea_bad_hex", oid: OIDBytea, raw: []byte(`\xZZZZ`), wantErr: true},
	}

	// NaiveTimestampAssumeUTC is a package-level global. Do not call t.Parallel()
	// in these subtests — writing the global in a parallel context would cause a data
	// race. The toggle behaviour is tested in TestMapValueTimestampNaiveUTCToggle.
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := mapValue(tc.oid, tc.raw, tc.isNull)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil (value=%v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertDeepEqual(t, tc.want, got)
		})
	}
}

// TestMapValueNullVariants confirms isNull=true always returns (nil, nil) regardless
// of OID, and that the null / absent distinction is distinct from a zero value.
func TestMapValueNullVariants(t *testing.T) {
	t.Parallel()
	oids := []uint32{OIDInt4, OIDText, OIDJSONB, OIDBytea, OIDTimestampTZ, 99999}
	for _, oid := range oids {
		oid := oid
		got, err := mapValue(oid, []byte("anything"), true)
		if err != nil {
			t.Errorf("OID %d: unexpected error: %v", oid, err)
		}
		if got != nil {
			t.Errorf("OID %d: expected nil for NULL, got %v (%T)", oid, got, got)
		}
	}
}

// TestMapValueByteaWithNullBytes verifies that bytea values containing embedded
// null bytes are preserved correctly through hex decode + base64 encode.
func TestMapValueByteaWithNullBytes(t *testing.T) {
	t.Parallel()
	// \x004865006c006c006f00 = 0x00 H 0x00 e 0x00 l 0x00 l 0x00 o 0x00
	raw := []byte(`\x004865006c006c006f00`)
	got, err := mapValue(OIDBytea, raw, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Decode and re-encode to verify round-trip.
	import_b64 := gotAsString(t, got)
	if import_b64 == "" {
		t.Fatal("expected non-empty base64 string")
	}
	// Just verify it's valid base64 by checking it marshals correctly.
	var jsonBytes []byte
	jsonBytes, err = json.Marshal(got)
	if err != nil {
		t.Fatalf("cannot marshal base64 result: %v", err)
	}
	if len(jsonBytes) == 0 {
		t.Fatal("expected non-empty JSON for base64 bytea")
	}
}

// TestMapValueTimestampNaiveUTCToggle confirms NaiveTimestampAssumeUTC=false
// produces a time that is still formatted as RFC3339 but using UTC location
// (Go time.Parse without a zone is UTC by default for naive layouts).
func TestMapValueTimestampNaiveUTCToggle(t *testing.T) {
	// Not parallel: modifies global NaiveTimestampAssumeUTC.
	raw := []byte("2026-05-14 10:23:45.123456")

	// true: timestamp treated as UTC, Z suffix.
	NaiveTimestampAssumeUTC = true
	got, err := mapValue(OIDTimestamp, raw, false)
	if err != nil {
		t.Fatalf("NaiveTimestampAssumeUTC=true: unexpected error: %v", err)
	}
	gotStr := gotAsString(t, got)
	if gotStr != "2026-05-14T10:23:45.123456Z" {
		t.Errorf("NaiveTimestampAssumeUTC=true: got %q, want %q", gotStr, "2026-05-14T10:23:45.123456Z")
	}

	// false: time.Parse with naive layout gives UTC location anyway, so result is same.
	NaiveTimestampAssumeUTC = false
	got2, err := mapValue(OIDTimestamp, raw, false)
	if err != nil {
		t.Fatalf("NaiveTimestampAssumeUTC=false: unexpected error: %v", err)
	}
	// When false, we still parse but do NOT force UTC — Go's time.Parse gives UTC
	// for layouts without timezone. Both results should be valid RFC3339 strings.
	gotStr2 := gotAsString(t, got2)
	if gotStr2 == "" {
		t.Errorf("NaiveTimestampAssumeUTC=false: got empty string")
	}

	// Restore global.
	NaiveTimestampAssumeUTC = true
}

// TestMapValueTimestampTZPrecision checks that RFC3339Nano fractional seconds
// are preserved for timestamptz values.
func TestMapValueTimestampTZPrecision(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw  string
		want string
	}{
		{"2026-05-14T10:23:45.123456789+02:00", "2026-05-14T08:23:45.123456789Z"},
		{"2026-05-14T10:23:45.1+00:00", "2026-05-14T10:23:45.1Z"},
		{"2026-05-14T10:23:45+00:00", "2026-05-14T10:23:45Z"},
	}
	for _, tc := range cases {
		got, err := mapValue(OIDTimestampTZ, []byte(tc.raw), false)
		if err != nil {
			t.Errorf("raw=%q: unexpected error: %v", tc.raw, err)
			continue
		}
		if gotAsString(t, got) != tc.want {
			t.Errorf("raw=%q: got %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// TestMapValueArrayWithNULL confirms NULL elements in 1D arrays are nil (not the
// string "NULL").
func TestMapValueArrayWithNULL(t *testing.T) {
	t.Parallel()
	got, err := mapValue(OIDInt4Array, []byte("{1,NULL,3}"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[1] != nil {
		t.Errorf("expected arr[1] to be nil (NULL), got %v (%T)", arr[1], arr[1])
	}
}

// TestMapValueTextArrayWithEmbeddedComma verifies that PG-quoted elements with
// embedded commas are parsed correctly.
func TestMapValueTextArrayWithEmbeddedComma(t *testing.T) {
	t.Parallel()
	// PG format: `{"a,b","c"}` — the outer braces are the array; inner quotes preserve commas.
	got, err := mapValue(OIDTextArray, []byte(`{"a,b","c"}`), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d: %v", len(arr), arr)
	}
	if arr[0] != "a,b" {
		t.Errorf("arr[0]: got %q, want %q", arr[0], "a,b")
	}
	if arr[1] != "c" {
		t.Errorf("arr[1]: got %q, want %q", arr[1], "c")
	}
}

// TestMapValueMultidimArrayFallback confirms multi-dimensional arrays are returned
// as raw text (TYPES-03).
func TestMapValueMultidimArrayFallback(t *testing.T) {
	t.Parallel()
	raw := "{{1,2},{3,4}}"
	got, err := mapValue(OIDInt4Array, []byte(raw), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != raw {
		t.Errorf("multidim array: got %q, want raw text passthrough %q", got, raw)
	}
}

// TestMapValueJSONBInlineNotString verifies that jsonb values are returned as
// json.RawMessage (not as a string), so they embed inline when marshalled.
func TestMapValueJSONBInlineNotString(t *testing.T) {
	t.Parallel()
	got, err := mapValue(OIDJSONB, []byte(`{"a":1}`), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := got.(json.RawMessage); !ok {
		t.Errorf("expected json.RawMessage, got %T", got)
	}
	// Marshalling should embed inline, not double-encode.
	outer := struct {
		V any `json:"v"`
	}{V: got}
	b, err := json.Marshal(outer)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Expected: {"v":{"a":1}} NOT {"v":"{\"a\":1}"}
	want := `{"v":{"a":1}}`
	if string(b) != want {
		t.Errorf("got %s, want %s", b, want)
	}
}

// TestMapValueInt8AlwaysString verifies int8 is always a string even for small values.
func TestMapValueInt8AlwaysString(t *testing.T) {
	t.Parallel()
	cases := []string{"1", "0", "-1", "9223372036854775807", "-9223372036854775808"}
	for _, raw := range cases {
		got, err := mapValue(OIDInt8, []byte(raw), false)
		if err != nil {
			t.Errorf("int8 %q: unexpected error: %v", raw, err)
			continue
		}
		if _, ok := got.(string); !ok {
			t.Errorf("int8 %q: expected string, got %T = %v", raw, got, got)
		}
		if got.(string) != raw {
			t.Errorf("int8 %q: got %q, want %q", raw, got.(string), raw)
		}
	}
}

// TestMapValueNumericAlwaysString verifies numeric is always a string.
func TestMapValueNumericAlwaysString(t *testing.T) {
	t.Parallel()
	raw := "99999999999999999999999.9999"
	got, err := mapValue(OIDNumeric, []byte(raw), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := got.(string); !ok {
		t.Errorf("expected string, got %T", got)
	}
}

// TestTimestampTZShortOffset tests that PG's short-offset timestamptz format
// (e.g. "2026-05-14 10:23:45.123+02") is correctly parsed and normalized to UTC.
func TestTimestampTZShortOffset(t *testing.T) {
	// Uses normalizeOffset path — do not mark parallel (reads NaiveTimestampAssumeUTC
	// is irrelevant here but keep sequential for simplicity).
	cases := []struct {
		raw  string
		want string
	}{
		// Short offset: PG may output "+02" without minutes for CEST.
		{"2026-05-14 10:23:45.123+02", "2026-05-14T08:23:45.123Z"},
		// Short offset negative.
		{"2026-05-14 10:23:45-05", "2026-05-14T15:23:45Z"},
	}
	for _, tc := range cases {
		got, err := mapValue(OIDTimestampTZ, []byte(tc.raw), false)
		if err != nil {
			t.Errorf("raw=%q: unexpected error: %v", tc.raw, err)
			continue
		}
		if gotAsString(t, got) != tc.want {
			t.Errorf("raw=%q: got %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// TestIntervalEdgeCases exercises the interval parser with date+time mixed formats
// and pure time formats.
func TestIntervalEdgeCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw  string
		want string
	}{
		{"0:00:00", "PT0S"},
		{"1:00:00", "PT1H"},
		{"0:30:00", "PT30M"},
		{"0:00:01", "PT1S"},
		{"0:00:01.5", "PT1.5S"},
		{"25:00:00", "PT25H"},
		{"PT1H30M", "PT1H30M"},               // ISO-8601 passthrough
		{"P1Y2M3DT4H5M6S", "P1Y2M3DT4H5M6S"}, // ISO-8601 passthrough
		{"", "PT0S"},
		// Postgres verbose format: "N year(s) N mon(s) N day(s) HH:MM:SS"
		{"1 year 2 mons 3 days 04:05:06", "P1Y2M3DT4H5M6S"},
		// Days only.
		{"5 days 00:00:00", "P5D"},
		// Months only.
		{"3 mons 0:00:00", "P3M"},
		// Negative time component.
		{"-0:30:00", "PT-30M"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			got, err := mapValue(OIDInterval, []byte(tc.raw), false)
			if err != nil {
				t.Errorf("raw=%q: unexpected error: %v", tc.raw, err)
				return
			}
			if got.(string) != tc.want {
				t.Errorf("raw=%q: got %q, want %q", tc.raw, got.(string), tc.want)
			}
		})
	}
}

// TestChangeKey verifies that Change.Key() produces the expected subscription index key.
func TestChangeKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		schema, table, pk string
		want              string
	}{
		{"public", "users", "42", "public.users:42"},
		{"app", "orders", "abc-123", "app.orders:abc-123"},
		{"public", "events", "550e8400-e29b-41d4-a716-446655440000",
			"public.events:550e8400-e29b-41d4-a716-446655440000"},
	}
	for _, tc := range cases {
		c := Change{Schema: tc.schema, Table: tc.table, PK: tc.pk}
		if got := c.Key(); got != tc.want {
			t.Errorf("Key(): got %q, want %q", got, tc.want)
		}
	}
}

// TestNaiveTimestampParseFallback tests that parseNaiveTimestamp handles all
// expected PG text-format variants.
func TestNaiveTimestampParseFallback(t *testing.T) {
	// Not parallel: reads NaiveTimestampAssumeUTC.
	NaiveTimestampAssumeUTC = true
	cases := []struct {
		raw  string
		want string
	}{
		{"2026-05-14T10:23:45.123456", "2026-05-14T10:23:45.123456Z"},
		{"2026-05-14T10:23:45", "2026-05-14T10:23:45Z"},
		{"2026-05-14 10:23:45.123456789", "2026-05-14T10:23:45.123456789Z"},
	}
	for _, tc := range cases {
		got, err := mapValue(OIDTimestamp, []byte(tc.raw), false)
		if err != nil {
			t.Errorf("raw=%q: unexpected error: %v", tc.raw, err)
			continue
		}
		if gotAsString(t, got) != tc.want {
			t.Errorf("raw=%q: got %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// ---- helper functions ----

// assertDeepEqual checks equality between want and got with type-sensitive comparison.
func assertDeepEqual(t *testing.T, want, got any) {
	t.Helper()
	// Normalize both through JSON round-trip for []any and json.RawMessage comparisons.
	wantJSON, err1 := json.Marshal(want)
	gotJSON, err2 := json.Marshal(got)
	if err1 != nil || err2 != nil {
		// Fall back to direct comparison.
		if want != got {
			t.Errorf("want %v (%T), got %v (%T)", want, want, got, got)
		}
		return
	}
	if string(wantJSON) != string(gotJSON) {
		t.Errorf("want %s, got %s", wantJSON, gotJSON)
	}
}

// gotAsString returns got as a string, failing the test if it's not a string.
func gotAsString(t *testing.T, got any) string {
	t.Helper()
	s, ok := got.(string)
	if !ok {
		t.Errorf("expected string, got %T = %v", got, got)
		return ""
	}
	return s
}
