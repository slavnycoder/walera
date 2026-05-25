// Package main — parser.go decodes a single SSE frame (already split into
// lines by the subscriber's bufio.Scanner) into an (event, data, ok) tuple.
//
// Wire shape it accepts (spec §3.5, produced by internal/sse/encoder.go):
//
//	event: <name>\n
//	id: <commit_lsn>/<tx_id>\n   (informational; consumed but not returned)
//	data: <payload>\n
//	\n                           (blank-line frame terminator, applied by caller)
//
// Lines starting with ':' are SSE comments (heartbeats) and MUST be ignored
// per the SSE spec; ParseFrame returns ok=false when the input contains no
// event: + data: pair, including the heartbeat case.
//
// Quick task 260518-lh1 / T-LH1-02 — pure function: no I/O, no allocations
// beyond the returned strings.
package main

import "strings"

// SSE field prefixes (spec §3.5; produced by internal/sse/encoder.go). The
// leading space after the colon is canonical but tolerated as optional —
// browsers do the same.
const (
	prefixEvent = "event:"
	prefixData  = "data:"
	prefixID    = "id:"
)

// ParseFrame decodes the (already line-split) body of one SSE frame.
//
// Returns (event, data, true) only when both `event:` and `data:` field
// lines were found. Returns ok=false for:
//   - heartbeat frames (single `:` comment line),
//   - frames missing either `event:` or `data:`,
//   - empty input.
//
// Lines starting with ':' (SSE comments) are silently skipped. The `id:`
// line is consumed but its value is not returned — the subscriber does not
// need the last-event-id (Walera does not support Last-Event-ID resume per
// spec §1.4).
//
// The trim removes ONLY the leading space after the colon per spec §3.5
// — payload bytes themselves are returned verbatim so JSON parsers
// downstream see exactly what Walera wrote.
func ParseFrame(lines []string) (event, data string, ok bool) {
	var haveEvent, haveData bool
	for _, line := range lines {
		// SSE comment lines (heartbeats and stream-keep-alive). Per spec
		// these MUST be ignored by clients.
		if len(line) > 0 && line[0] == ':' {
			continue
		}
		switch {
		case strings.HasPrefix(line, prefixEvent):
			if !haveEvent {
				event = strings.TrimPrefix(strings.TrimPrefix(line, prefixEvent), " ")
				haveEvent = true
			}
		case strings.HasPrefix(line, prefixData):
			if !haveData {
				data = strings.TrimPrefix(strings.TrimPrefix(line, prefixData), " ")
				haveData = true
			}
		case strings.HasPrefix(line, prefixID):
			// Informational; intentionally discarded.
		default:
			// Unknown field — ignore (spec §3.5 says clients MUST ignore
			// fields they do not recognise).
		}
	}
	if !haveEvent || !haveData {
		return "", "", false
	}
	return event, data, true
}
