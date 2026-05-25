//go:build integration

package integration

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func Test08GracefulShutdown(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)

	h.Auth.SetMap(
		"test-token",
		"test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)

	if err := h.Auth.SetTTL("test-token", 60); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type sub struct {
		idx     int
		events  <-chan SSEEvent
		errCh   <-chan error
		closeFn func()
	}
	subs := make([]sub, 3)
	for i := range subs {
		ev, ec, cf := h.Client.Connect(ctx, channelFor(i+1), "test-token")
		subs[i] = sub{idx: i + 1, events: ev, errCh: ec, closeFn: cf}
		defer cf()
	}

	for i := 1; i <= 3; i++ {
		if err := h.PG.Exec(ctx,
			"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
			i, "u@x", "U",
		); err != nil {
			t.Fatalf("seed insert #%d: %v", i, err)
		}
	}
	for _, s := range subs {
		_ = readTxEvent(ctx, t, h, s.events, s.errCh)
	}

	if err := h.Binary.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}

	const want = `{"reason":"service_restart"}`
	var wg sync.WaitGroup
	failures := make(chan string, len(subs))
	for _, s := range subs {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(6 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case ev, ok := <-s.events:
					if !ok {
						failures <- "sub " + itoa(s.idx) + ": events closed before observing shutdown frame"
						return
					}
					if ev.Type == "shutdown" {
						if string(ev.Data) != want {
							failures <- "sub " + itoa(s.idx) + ": shutdown data = " + string(ev.Data) + "; want " + want
							return
						}

						closeDeadline := time.Now().Add(2 * time.Second)
						for time.Now().Before(closeDeadline) {
							select {
							case _, ok := <-s.events:
								if !ok {
									return
								}

							case <-s.errCh:
								return
							case <-time.After(100 * time.Millisecond):
							}
						}

						return
					}

				case err := <-s.errCh:
					failures <- "sub " + itoa(s.idx) + ": errCh before shutdown frame: " + err.Error()
					return
				case <-time.After(250 * time.Millisecond):
				case <-ctx.Done():
					failures <- "sub " + itoa(s.idx) + ": ctx done before shutdown frame"
					return
				}
			}
			failures <- "sub " + itoa(s.idx) + ": did not observe shutdown frame within 6s"
		}()
	}
	wg.Wait()
	close(failures)
	var failed []string
	for f := range failures {
		failed = append(failed, f)
	}
	if len(failed) > 0 {
		t.Fatalf("shutdown assertions failed:\n  %s\nstderr:\n%s",
			strings.Join(failed, "\n  "), h.Binary.Stderr())
	}

	stderr := h.Binary.Stderr()
	if !strings.Contains(stderr, "shutdown") {
		t.Errorf("binary stderr missing 'shutdown' log entries; stderr:\n%s", stderr)
	}

}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
