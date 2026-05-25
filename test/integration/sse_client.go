//go:build integration

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

type SSEEvent struct {
	Type string
	ID   string
	Data []byte
}

type HTTPError struct {
	Status int
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("sse client: status %d: %s", e.Status, string(e.Body))
}

type Client struct {
	baseURL string
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL}
}

func (c *Client) Connect(ctx context.Context, channel, token string) (<-chan SSEEvent, <-chan error, func()) {
	events := make(chan SSEEvent, 16)
	errCh := make(chan error, 1)

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

	hc := &http.Client{}
	resp, err := hc.Do(req)
	if err != nil {
		errCh <- fmt.Errorf("sse client: do: %w", err)
		close(events)
		cancel()
		return events, errCh, func() {}
	}
	if resp.StatusCode != http.StatusOK {

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()

		errCh <- &HTTPError{Status: resp.StatusCode, Body: body}
		close(events)
		cancel()
		return events, errCh, func() {}
	}

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

			_ = resp.Body.Close()
			<-done
		})
	}
	return events, errCh, closeFn
}

func readSSE(r io.ReadCloser, events chan<- SSEEvent, errCh chan<- error, done chan struct{}) {
	defer close(done)
	defer close(events)

	scanner := bufio.NewScanner(r)

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

		if len(line) == 0 {

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

		if line[0] == ':' {
			continue
		}

		colon := bytes.IndexByte(line, ':')
		if colon < 0 {

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

			if curData.Len() > 0 {
				curData.WriteByte('\n')
			}
			curData.Write(value)
		default:

		}
	}

	if err := scanner.Err(); err != nil && !isClosedErr(err) {
		errCh <- fmt.Errorf("sse client: scanner: %w", err)
	}
}

func isClosedErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var ue *url.Error
	if errors.As(err, &ue) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {

		return strings.Repeat("0", 2*n)
	}
	return hex.EncodeToString(buf)
}
