# 5. PostgreSQL → JSON type mapping

Use the **JS-safe** mapping: values that may lose precision in JavaScript `Number` are serialized as strings.

| PG type | JSON | Example |
|---|---|---|
| `int2`, `int4` | number | `42` |
| `int8` (bigint) | **string** | `"9223372036854775807"` |
| `numeric`, `decimal` | **string** | `"3.14159"` |
| `float4`, `float8` | number | `3.14` |
| `bool` | boolean | `true` |
| `text`, `varchar`, `char` | string | `"hello"` |
| `uuid` | string | `"550e8400-e29b-41d4-a716-446655440000"` |
| `timestamp`, `timestamptz` | string (RFC3339, UTC) | `"2026-05-14T10:23:45.123Z"` |
| `date` | string | `"2026-05-14"` |
| `time` | string | `"10:23:45.123"` |
| `interval` | string (ISO-8601 duration) | `"PT2H30M"` |
| `jsonb`, `json` | embedded JSON value | `{"a":1}` |
| `bytea` | string (base64) | `"SGVsbG8="` |
| array (`T[]`) | JSON array | `[1, 2, 3]` |
| `enum`, custom domain | string | `"active"` |
| `inet`, `cidr` | string | `"192.168.0.0/24"` |
| `NULL` | `null` | `null` |
| Unknown OID | string (raw pgoutput text) | as-is |

Notes:
- `timestamp` without timezone is interpreted as UTC by default. Make this configurable: `naive_timestamp_assume_utc: true`.
- `jsonb` is parsed for validity and embedded inline — the client should NOT need to `JSON.parse` it again.
- In an UPDATE's `data` map (unified field — `op` disambiguates INSERT vs UPDATE shape): **absence of a field means "not changed"**; presence with `null` means "now NULL". These are distinct. (For INSERT, `data` is the full new row, so absence simply means the column was not present in the row, e.g. an unlogged TOAST.)

Use pgoutput's text mode (the default). Binary mode is a possible future CPU optimization.
