// Package auth — client.go: HTTP client for the external auth backend.
// Security: bearer credentials never appear in any zerolog statement; see
// doc.go grep gate. See INVARIANTS.md Security/PII §5 for the canonical rule.
package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/metrics"
)

// outboundRequestIDRe — same charset as internal/sse.requestIDRe. Duplicated
// rather than imported to avoid an internal/auth -> internal/sse import cycle.
var outboundRequestIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// validOutboundRequestID reports whether s is a well-formed X-Request-ID:
// non-empty, <= 128 bytes, matches outboundRequestIDRe.
func validOutboundRequestID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	return outboundRequestIDRe.MatchString(s)
}

// truncateForLog returns s if len(s) <= max, otherwise the first max bytes
// followed by "...". Used for log hygiene on malformed inbound IDs.
func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// maxResponseBytes bounds the response-body read so a hostile or buggy backend
// cannot exhaust client memory.
const maxResponseBytes = 64 * 1024

// BreakerHook decouples Client from *Breaker so tests can drive failure
// classification without instantiating the real FSM.
//
// Contract: RecordResult is called after every Permissions call. Allow is
// consulted by background refresh logic — NOT by Permissions itself.
type BreakerHook interface {
	RecordResult(success bool)
	Allow() bool
}

// nopBreaker is the placeholder BreakerHook used when callers pass nil.
type nopBreaker struct{}

// RecordResult discards the result.
func (nopBreaker) RecordResult(_ bool) {}

// Allow always permits.
func (nopBreaker) Allow() bool { return true }

// Client performs HTTP calls against the configured auth backend. All exported
// methods are safe for concurrent use.
type Client struct {
	base         string
	serviceToken string
	hc           *http.Client
	bk           BreakerHook
	log          zerolog.Logger
	mc           *metrics.Registry

	setBreakerOnce sync.Once
}

// Deps bundles the collaborators auth.New requires.
type Deps struct {
	// Logger zero value is a usable Nop.
	Logger zerolog.Logger
	// Breaker may be nil; auth.New substitutes nopBreaker{}.
	Breaker BreakerHook
	// Metrics is the typed Prometheus registry. Required.
	Metrics *metrics.Registry
}

func validateDeps(d Deps) {
	if d.Metrics == nil {
		panic("auth.New: Deps.Metrics is required")
	}
}

// New constructs a Client. The HTTP transport is preconfigured with
// conservative pool limits suited to a single 4-CPU Walera pod. The five
// walera_auth_requests_total result-label series are pre-touched at zero.
func New(cfg Config, deps Deps) *Client {
	validateDeps(deps)
	bk := deps.Breaker
	if bk == nil {
		bk = nopBreaker{}
	}
	c := &Client{
		base:         cfg.BackendURL,
		serviceToken: cfg.ServiceToken,
		hc:           newHTTPClient(cfg),
		bk:           bk,
		log:          deps.Logger,
		mc:           deps.Metrics,
	}
	for _, r := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		c.mc.AuthRequests(r).Add(0)
	}
	return c
}

// Metrics returns the registry this Client publishes counters into.
func (c *Client) Metrics() *metrics.Registry { return c.mc }

// SetBreaker installs the real BreakerHook on the client. INIT-ONLY: callers
// MUST invoke this before any goroutine reads any Client method. The first
// call installs the hook; subsequent calls are no-ops with a Warn log.
// Nil substitutes nopBreaker{}.
func (c *Client) SetBreaker(bk BreakerHook) {
	if bk == nil {
		bk = nopBreaker{}
	}
	called := false
	c.setBreakerOnce.Do(func() {
		c.bk = bk
		called = true
	})
	if !called {
		c.log.Warn().Msg("auth.Client.SetBreaker: ignored second call (init-only)")
	}
}

// newHTTPClient builds the *http.Client with conservative pool limits and the
// configured request timeout.
func newHTTPClient(cfg Config) *http.Client {
	t := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// ResponseHeaderTimeout: 0 — covered by http.Client.Timeout.
		DisableCompression: false,
	}
	return &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: t,
	}
}

// Permissions performs the per-handshake authorization call. On 200 returns a
// parsed *Whitelist. On 4xx returns the corresponding typed error carrying the
// upstream body bytes (bounded ≤ 64 KiB). On 5xx, network errors, timeouts,
// malformed JSON, or shape-invalid Whitelist responses returns *ErrUnavailable
// and notifies the breaker via RecordResult(false).
//
// Every call increments walera_auth_requests_total{result=…} and observes
// walera_auth_request_duration_seconds.
func (c *Client) Permissions(ctx context.Context, token, channel, requestID string) (*Whitelist, error) {
	start := time.Now()
	defer func() {
		c.mc.AuthRequestDuration().Observe(time.Since(start).Seconds())
	}()

	u := c.base + "/auth/permissions?channel=" + url.QueryEscape(channel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: err}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if !validOutboundRequestID(requestID) {
		var buf [16]byte
		if _, err := rand.Read(buf[:]); err != nil {
			panic("auth: crypto/rand.Read failed: " + err.Error())
		}
		substitute := hex.EncodeToString(buf[:])
		c.log.Warn().
			Int("invalid_len", len(requestID)).
			Str("invalid_id_truncated", truncateForLog(requestID, 16)).
			Str("substitute_id", substitute).
			Msg("auth: outbound X-Request-ID failed validation; substituting fresh ID")
		requestID = substitute
	}
	req.Header.Set("X-Request-ID", requestID)
	req.Header.Set("Accept", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		c.log.Warn().
			Str("auth_request_id", requestID).
			Err(err).
			Msg("auth: HTTP dispatch failed")
		return nil, &ErrUnavailable{Cause: err}
	}
	defer resp.Body.Close() //nolint:errcheck

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if readErr != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		c.log.Warn().
			Str("auth_request_id", requestID).
			Int("status", resp.StatusCode).
			Err(readErr).
			Msg("auth: response body read failed")
		return nil, &ErrUnavailable{Cause: readErr}
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		m, err := ParseWhitelist(body)
		if err != nil {
			c.mc.AuthRequests("unavailable").Inc()
			c.bk.RecordResult(false)
			c.log.Warn().
				Str("auth_request_id", requestID).
				Int("status", resp.StatusCode).
				Err(err).
				Msg("auth: response parse failed")
			return nil, &ErrUnavailable{Cause: fmt.Errorf("decode: %w", err)}
		}
		c.mc.AuthRequests("ok").Inc()
		c.bk.RecordResult(true)
		return m, nil

	case resp.StatusCode == http.StatusUnauthorized:
		c.mc.AuthRequests("unauthorized").Inc()
		c.bk.RecordResult(true)
		return nil, &ErrUnauthorized{Body: body}

	case resp.StatusCode == http.StatusForbidden:
		c.mc.AuthRequests("forbidden").Inc()
		c.bk.RecordResult(true)
		return nil, &ErrForbidden{Body: body}

	case resp.StatusCode == http.StatusNotFound:
		c.mc.AuthRequests("not_found").Inc()
		c.bk.RecordResult(true)
		return nil, &ErrNotFound{Body: body}

	default:
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		c.log.Warn().
			Str("auth_request_id", requestID).
			Int("status", resp.StatusCode).
			Msg("auth: unexpected backend status")
		return nil, &ErrUnavailable{
			Cause: fmt.Errorf("auth backend returned %d", resp.StatusCode),
		}
	}
}

// CheckAuth performs the readyz background probe. Implements
// internal/health.AuthChecker. Any non-network response (401/403/404) counts
// as "reachable" — only *ErrUnavailable is propagated.
func (c *Client) CheckAuth(ctx context.Context) error {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("auth: crypto/rand.Read failed: " + err.Error())
	}
	id := "health-" + hex.EncodeToString(buf[:])
	_, err := c.Permissions(ctx, c.serviceToken, "_health", id)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *ErrUnauthorized, *ErrForbidden, *ErrNotFound:
		return nil
	}
	return err
}
