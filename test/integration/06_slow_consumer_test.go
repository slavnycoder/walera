//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func Test06SlowConsumer(t *testing.T) {
	t.Skip("flaky on slow CI hardware; tracked as Phase 11 follow-up. " +
		"Slow-consumer behaviour is covered by internal/sse " +
		"TestPoolSlowClientIsolation and TestPoolSlowClientIsolationStress.")
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rawConn := dialRawSSE(t, h.Binary.BaseURL(), "users/all", "test-token")
	defer rawConn.Close() //nolint:errcheck

	br := bufio.NewReaderSize(rawConn, 2*1024*1024)
	if err := readHTTPResponseHeaders(br); err != nil {
		t.Fatalf("read response headers: %v; stderr:\n%s", err, h.Binary.Stderr())
	}

	metricsURL := h.Binary.BaseURL() + "/metrics"
	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_subscribers_active{type="wildcard"}`,
		func(v float64) bool { return v >= 1 },
		3*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("subscriber never registered: %v; stderr:\n%s", err, h.Binary.Stderr())
	}

	baseline, _ := scrapeMetric(ctx, metricsURL, `walera_tx_dropped_total{reason="slow_consumer"}`)

	const bigTxChanges = 500
	const bigTxNameSize = 1024
	const smallTxBurst = 20
	bigName := strings.Repeat("x", bigTxNameSize)
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		conn, err := pgx.Connect(ctx, h.PG.DSN)
		if err != nil {
			t.Logf("burst connect: %v (continuing)", err)
			return
		}
		defer conn.Close(ctx) //nolint:errcheck

		if _, err := conn.Exec(ctx, "SET synchronous_commit = off"); err != nil {
			t.Logf("set synchronous_commit: %v (continuing)", err)
		}

		var sb strings.Builder
		sb.WriteString("INSERT INTO users (id, email, name) VALUES ")
		args := make([]any, 0, bigTxChanges*3)
		for i := 0; i < bigTxChanges; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
			args = append(args, i+1, fmt.Sprintf("big%d@x", i+1), bigName)
		}
		if _, err := conn.Exec(ctx, sb.String(), args...); err != nil {
			t.Logf("big tx: %v (continuing)", err)
			return
		}

		for i := 0; i < smallTxBurst; i++ {
			id := bigTxChanges + i + 1
			if _, err := conn.Exec(ctx,
				"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
				id, fmt.Sprintf("small%d@x", id), "s",
			); err != nil {

				t.Logf("small tx #%d: %v (continuing)", i, err)
				return
			}
		}
	}()

	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_tx_dropped_total{reason="slow_consumer"}`,
		func(v float64) bool { return v > baseline },
		3*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("slow_consumer counter never bumped above baseline=%v: %v; stderr:\n%s",
			baseline, err, h.Binary.Stderr())
	}

	select {
	case <-burstDone:
	case <-ctx.Done():
		t.Fatalf("ctx done waiting for burst goroutine: %v", ctx.Err())
	}

	if err := readUntilErrorFrame(rawConn, br, "slow_consumer", 5*time.Second); err != nil {
		t.Fatalf("drain for slow_consumer error frame: %v; stderr:\n%s", err, h.Binary.Stderr())
	}
}

func dialRawSSE(t *testing.T, baseURL, channel, token string) net.Conn {
	t.Helper()
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("dialRawSSE: parse %q: %v", baseURL, err)
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("tcp", host)
	if err != nil {
		t.Fatalf("dialRawSSE: %v", err)
	}
	req := fmt.Sprintf(
		"GET /sse/v1/%s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"User-Agent: walera-test-slow-consumer/1.0\r\n"+
			"Accept: text/event-stream\r\n"+
			"Authorization: Bearer %s\r\n"+
			"Connection: keep-alive\r\n"+
			"\r\n",
		channel, host, token,
	)
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		t.Fatalf("dialRawSSE: write request: %v", err)
	}
	return conn
}

func readHTTPResponseHeaders(br *bufio.Reader) error {
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 status: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		return fmt.Errorf("unexpected content-type %q", ct)
	}
	return nil
}

func readUntilErrorFrame(conn net.Conn, br *bufio.Reader, wantReason string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	_ = conn.SetReadDeadline(deadline)
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	var curType string
	var curData bytes.Buffer
	for time.Now().Before(deadline) {
		line, err := br.ReadSlice('\n')
		if err != nil {
			if isEOFLike(err) {
				return fmt.Errorf("conn closed before error frame: %w", err)
			}
			return fmt.Errorf("read line: %w", err)
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if curType == "error" && bytes.Contains(curData.Bytes(), []byte(wantReason)) {
				return nil
			}
			curType = ""
			curData.Reset()
			continue
		}
		if line[0] == ':' {
			continue
		}
		if bytes.HasPrefix(line, []byte("event: ")) {
			curType = string(bytes.TrimPrefix(line, []byte("event: ")))
		} else if bytes.HasPrefix(line, []byte("data: ")) {
			if curData.Len() > 0 {
				curData.WriteByte('\n')
			}
			curData.Write(bytes.TrimPrefix(line, []byte("data: ")))
		}
	}
	return fmt.Errorf("timeout waiting for error frame with reason=%q", wantReason)
}

func isEOFLike(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	s := err.Error()
	return s == "EOF" || strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection reset by peer")
}
