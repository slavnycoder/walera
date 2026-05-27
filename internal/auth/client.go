package auth

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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

var outboundRequestIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validOutboundRequestID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	return outboundRequestIDRe.MatchString(s)
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

const maxResponseBytes = 64 * 1024

type BreakerHook interface {
	RecordResult(success bool)
	Allow() bool
}

type nopBreaker struct{}

func (nopBreaker) RecordResult(_ bool) {}

func (nopBreaker) Allow() bool { return true }

type Client struct {
	base   string
	hc     *http.Client
	bk     BreakerHook
	log    zerolog.Logger
	mc     *metrics.Registry
	signer *Signer

	setBreakerOnce sync.Once
}

type Deps struct {
	Logger zerolog.Logger

	Breaker BreakerHook

	Metrics *metrics.Registry
}

func validateDeps(d Deps) {
	if d.Metrics == nil {
		panic("auth.New: Deps.Metrics is required")
	}
}

func New(cfg Config, deps Deps) *Client {
	validateDeps(deps)
	bk := deps.Breaker
	if bk == nil {
		bk = nopBreaker{}
	}
	c := &Client{
		base: cfg.BackendURL,
		hc:   newHTTPClient(cfg),
		bk:   bk,
		log:  deps.Logger,
		mc:   deps.Metrics,
	}
	if cfg.Signing.Secret != "" {
		signer, err := NewSigner([]byte(cfg.Signing.Secret), cfg.Signing.Kid)
		if err != nil {
			panic("auth.New: signing config rejected by NewSigner — config.Validate should have caught this: " + err.Error())
		}
		c.signer = signer
	}
	for _, r := range []string{"ok", "unauthorized", "forbidden", "not_found", "unavailable"} {
		c.mc.AuthRequests(r).Add(0)
	}
	return c
}

func (c *Client) Metrics() *metrics.Registry { return c.mc }

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

		DisableCompression: false,
	}
	return &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: t,
	}
}

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
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
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

func (c *Client) OpenSession(ctx context.Context, bearer, channel, requestID string) (*Whitelist, error) {
	start := time.Now()
	defer func() {
		c.mc.AuthRequestDuration().Observe(time.Since(start).Seconds())
	}()

	if bearer == "" {
		c.mc.AuthRequests("unauthorized").Inc()
		c.bk.RecordResult(true)
		return nil, &ErrUnauthorized{Body: []byte(`{"reason":"missing_bearer"}`)}
	}

	body, err := jsonBody(map[string]string{"channel": channel})
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: err}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/auth/sessions", body)
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: err}
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Request-ID", c.sanitizeRequestID(requestID))

	return c.doAndDecode(req, c.sanitizeRequestID(requestID))
}

func (c *Client) RefreshPermissions(ctx context.Context, userID, channel, requestID string) (*Whitelist, error) {
	start := time.Now()
	defer func() {
		c.mc.AuthRequestDuration().Observe(time.Since(start).Seconds())
	}()

	if c.signer == nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: fmt.Errorf("auth: signer not configured")}
	}
	if userID == "" {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: fmt.Errorf("auth: empty user_id")}
	}

	ts := time.Now().Unix()
	nonce, err := newNonce()
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: err}
	}
	sig := c.signer.Sign(userID, channel, ts, nonce)

	payload := refreshRequest{
		UserID:  userID,
		Channel: channel,
		TS:      ts,
		Nonce:   nonce,
	}
	body, err := jsonBody(payload)
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: err}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/auth/permissions", body)
	if err != nil {
		c.mc.AuthRequests("unavailable").Inc()
		c.bk.RecordResult(false)
		return nil, &ErrUnavailable{Cause: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Walera-Sig", sig)
	req.Header.Set("X-Walera-Kid", c.signer.Kid())
	req.Header.Set("X-Request-ID", c.sanitizeRequestID(requestID))

	return c.doAndDecode(req, c.sanitizeRequestID(requestID))
}

type refreshRequest struct {
	UserID  string `json:"user_id"`
	Channel string `json:"channel"`
	TS      int64  `json:"ts"`
	Nonce   string `json:"nonce"`
}

func jsonBody(v any) (io.Reader, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	return buf, nil
}

func newNonce() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func (c *Client) sanitizeRequestID(requestID string) string {
	if validOutboundRequestID(requestID) {
		return requestID
	}
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
	return substitute
}

func (c *Client) doAndDecode(req *http.Request, requestID string) (*Whitelist, error) {
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
		m, perr := ParseWhitelist(body)
		if perr != nil {
			c.mc.AuthRequests("unavailable").Inc()
			c.bk.RecordResult(false)
			c.log.Warn().
				Str("auth_request_id", requestID).
				Int("status", resp.StatusCode).
				Err(perr).
				Msg("auth: response parse failed")
			return nil, &ErrUnavailable{Cause: fmt.Errorf("decode: %w", perr)}
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

func (c *Client) CheckAuth(ctx context.Context) error {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("auth: crypto/rand.Read failed: " + err.Error())
	}
	id := "health-" + hex.EncodeToString(buf[:])
	_, err := c.Permissions(ctx, "", "_health", id)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *ErrUnauthorized, *ErrForbidden, *ErrNotFound:
		return nil
	}
	return err
}
