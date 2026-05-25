//go:build integration

// Package integration — sse_client.go is a minimal SSE client used by the
// integration scenarios. It parses the wire format produced by
// internal/sse/encoder.go:
//
//	event: <name>\n
//	id: <opt-id>\n
//	data: <payload>\n
//	\n          ← blank line terminates a frame
//
// Heartbeat frames (":\n\n") are dropped silently.
//
// Concurrency model: Connect spawns ONE reader goroutine using a raw `go`
// statement. safego.Go is production-only (Pattern S1 in PATTERNS.md); test
// code is exempt. The reader exits when:
//  1. closeFn() cancels the context (which causes the http body Read to
//     return an error), OR
//  2. the server closes the connection (EOF on Body.Read), OR
//  3. the response status is non-200 (in which case Connect returns before
//     ever spawning the goroutine).
//
// Channels are buffered to prevent the reader from blocking when a test is
// mid-assertion:
//   - events: cap 16 (typical scenario asserts on the first 1-3 events).
//   - errCh:  cap 1  (at most one error per connection — terminal).
package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// SSEEvent is one parsed SSE frame. Type is the value after "event: ", ID is
// the value after "id: " (empty for error frames), Data is the raw bytes
// after "data: " (typically a JSON object, but kept as []byte so tests can
// dispatch on the event type before deciding how to decode).
type SSEEvent struct {
	Type string
	ID   string
	Data []byte
}

// HTTPError carries the non-200 status and (bounded) response body returned
// by the SSE handshake. Returned via errCh on the INITIAL handshake response
// only; mid-stream errors (auth refresh revoke, slow-consumer drop, scanner
// failure, etc.) remain plain errors / parsed SSEEvent error frames.
//
// Callers extract the typed value via errors.As:
//
//	var httpErr *HTTPError
//	if errors.As(err, &httpErr) { /* httpErr.Status, httpErr.Body */ }
//
// Defined for TEST-07 / ROADMAP §10 SC #3 — replaces the prior stringified
// fmt.Errorf("sse client: status %d: %s", ...) so reject sub-tests can
// assert the status code directly instead of substring-matching.
type HTTPError struct {
	Status int
	Body   []byte
}

// Error renders the typed error in the same shape the previous fmt.Errorf
// path produced, so callers that still string-match on "status %d" continue
// to work during the rollout (the new sub-tests use errors.As instead).
func (e *HTTPError) Error() string {
	return fmt.Sprintf("sse client: status %d: %s", e.Status, string(e.Body))
}

// Client constructs SSE connections against the spawned Walera binary.
type Client struct {
	baseURL string
}

// NewClient returns a Client targeting baseURL (e.g. "http://127.0.0.1:18080").
func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL}
}

// Connect opens a GET request against /sse/v1/<channel> with the supplied
// bearer token. On non-200 it returns an error on errCh and immediately
// returns a no-op closeFn — the http body is drained and closed by the
// helper before returning.
//
// On 200 it spawns a reader goroutine that parses SSE frames and pushes them
// onto events. closeFn cancels the underlying context (causing the body
// Read to error out), waits for the reader goroutine to exit, and returns.
// Calling closeFn twice is safe.
//
// The channel argument is path-style, e.g. "users/42" or "users/all" — the
// caller is responsible for URL-encoding any reserved characters within the
// individual segments (the test scenarios use only [a-z0-9_] identifiers).
func (c *Client) Connect(ctx context.Context, channel, token string) (<-chan SSEEvent, <-chan error, func()) {
	events := make(chan SSEEvent, 16)
	errCh := make(chan error, 1)

	// Build URL: baseURL + /sse/v1/<channel>.
	target := strings.TrimRight(c.baseURL, "/") + "/sse/v1/" + channel
	reqCtx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
	if err != nil {
		errCh <- fmt.Errorf("sse client: new request: %w", err)
		close(events)
		cancel()
		return events, errCh, func() {}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Request-ID", randomHex(16))
	req.Header.Set("Accept", "text/event-stream")

	// Per-call http.Client; no body timeout (the SSE stream must stay open).
	hc := &http.Client{}
	resp, err := hc.Do(req)
	if err != nil {
		errCh <- fmt.Errorf("sse client: do: %w", err)
		close(events)
		cancel()
		return events, errCh, func() {}
	}
	if resp.StatusCode != http.StatusOK {
		// Read the body so the caller can inspect the error payload via the
		// errCh message; bound the read to 64 KiB defensively.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()
		// TEST-07 / ROADMAP §10 SC #3: typed error so callers can use
		// errors.As(err, &httpErr) and assert httpErr.Status / httpErr.Body
		// directly. errCh has cap 1 (non-blocking send) and we MUST queue the
		// error BEFORE closing events so any select on both channels sees the
		// errCh arm ready before the closed-channel arm.
		errCh <- &HTTPError{Status: resp.StatusCode, Body: body}
		close(events)
		cancel()
		return events, errCh, func() {}
	}

	// Verify response shape so a misrouted 200 (e.g., a default ServeMux
	// handler) is surfaced loudly rather than producing 0 events.
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()
		errCh <- fmt.Errorf("sse client: unexpected content-type %q (body=%q)", ct, string(body))
		close(events)
		cancel()
		return events, errCh, func() {}
	}

	var once sync.Once
	done := make(chan struct{})

	go readSSE(resp.Body, events, errCh, done)

	closeFn := func() {
		once.Do(func() {
			cancel()
			// Closing the body unblocks any in-flight Read with an error;
			// also free the underlying conn.
			_ = resp.Body.Close()
			<-done
		})
	}
	return events, errCh, closeFn
}

// readSSE parses SSE frames out of r until r is closed or EOF. Successful
// frames go to events; the first read error (after EOF / cancellation) goes
// to errCh; close(done) signals the parent that the goroutine has exited.
func readSSE(r io.ReadCloser, events chan<- SSEEvent, errCh chan<- error, done chan struct{}) {
	defer close(done)
	defer close(events)

	// bufio.Scanner over the http body — one Scan() per line; we
	// re-assemble multi-line SSE frames in the loop below.
	scanner := bufio.NewScanner(r)
	// 1 MiB max line — well above any expected tx payload (the server cap
	// is 10 MiB) because the scanner sees lines, not whole frames; lines
	// inside a tx payload should never approach 1 MiB.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var (
		curType string
		curID   string
		curData bytes.Buffer
	)
	reset := func() {
		curType = ""
		curID = ""
		curData.Reset()
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		// Blank line terminates a frame.
		if len(line) == 0 {
			// Heartbeat (":") or empty frame: emit only if we have a type.
			if curType != "" {
				ev := SSEEvent{Type: curType, ID: curID}
				if curData.Len() > 0 {
					ev.Data = append([]byte(nil), curData.Bytes()...)
				}
				events <- ev
			}
			reset()
			continue
		}
		// Comment line (heartbeat). encoder.go writes exactly ":\n\n".
		if line[0] == ':' {
			continue
		}
		// "<field>: <value>" — at most ': ' separator. The wire produced by
		// encoder.go always uses ": " but the SSE spec allows just ":".
		colon := bytes.IndexByte(line, ':')
		if colon < 0 {
			// Malformed line — ignore (spec behavior).
			continue
		}
		field := string(line[:colon])
		value := line[colon+1:]
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		switch field {
		case "event":
			curType = string(value)
		case "id":
			curID = string(value)
		case "data":
			// Per SSE spec multiple data: lines concatenate with \n
			// between them. encoder.go never writes more than one.
			if curData.Len() > 0 {
				curData.WriteByte('\n')
			}
			curData.Write(value)
		default:
			// Ignore unknown fields (e.g. retry:).
		}
	}

	if err := scanner.Err(); err != nil && !isClosedErr(err) {
		errCh <- fmt.Errorf("sse client: scanner: %w", err)
	}
}

// isClosedErr reports whether err is one of the expected terminal errors that
// follow a normal shutdown (ctx cancel, body close, EOF).
func isClosedErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// http.Client returns wrapped "use of closed network connection" / "context
	// canceled" inside url.Error — these are also expected.
	var ue *url.Error
	if errors.As(err, &ue) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

// randomHex returns 2n hex digits drawn from crypto/rand. Used to populate
// X-Request-ID on outbound SSE handshakes.
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failure on Linux is unrecoverable; tests would have
		// bigger problems than a bad request id.
		return strings.Repeat("0", 2*n)
	}
	return hex.EncodeToString(buf)
}
