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
	"net/textproto"
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

// reservedHeaders is the set of canonical header names Walera owns on the
// OpenSession request. They can never be injected or overridden via the
// header allowlist — config.Validate rejects them, and ForwardedFromRequest /
// OpenSession skip them defensively even if validation were bypassed.
var reservedHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("Authorization"):     {},
	textproto.CanonicalMIMEHeaderKey("Host"):              {},
	textproto.CanonicalMIMEHeaderKey("Content-Length"):    {},
	textproto.CanonicalMIMEHeaderKey("Content-Type"):      {},
	textproto.CanonicalMIMEHeaderKey("Accept"):            {},
	textproto.CanonicalMIMEHeaderKey("Connection"):        {},
	textproto.CanonicalMIMEHeaderKey("Transfer-Encoding"): {},
	textproto.CanonicalMIMEHeaderKey("Cookie"):            {},
	textproto.CanonicalMIMEHeaderKey("X-Request-Id"):      {},
	textproto.CanonicalMIMEHeaderKey("X-Walera-Sig"):      {},
	textproto.CanonicalMIMEHeaderKey("X-Walera-Kid"):      {},
}

// ForwardedAuth carries the client-supplied cookies and headers selected by
// the configured allowlists. Values are never logged (PII/secret).
type ForwardedAuth struct {
	Cookies []*http.Cookie
	Headers http.Header
}

// Empty reports whether no cookies and no headers were forwarded.
func (f ForwardedAuth) Empty() bool {
	return len(f.Cookies) == 0 && len(f.Headers) == 0
}

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

	// fwdCookies holds the allowlisted cookie names (exact, case-sensitive
	// per RFC 6265). fwdHeaders holds the allowlisted header names canonicalized
	// via textproto.CanonicalMIMEHeaderKey (case-insensitive lookup).
	fwdCookies map[string]struct{}
	fwdHeaders map[string]struct{}

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
		base:       cfg.BackendURL,
		hc:         newHTTPClient(cfg),
		bk:         bk,
		log:        deps.Logger,
		mc:         deps.Metrics,
		fwdCookies: buildCookieAllowlist(cfg.ForwardedCookies),
		fwdHeaders: buildHeaderAllowlist(cfg.ForwardedHeaders),
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

// buildCookieAllowlist normalizes configured cookie names into a lookup set.
// Cookie names are case-sensitive (RFC 6265), so they are stored verbatim.
func buildCookieAllowlist(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}

// buildHeaderAllowlist normalizes configured header names into a lookup set
// keyed by canonical MIME header key (header names are case-insensitive).
func buildHeaderAllowlist(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[textproto.CanonicalMIMEHeaderKey(n)] = struct{}{}
	}
	return m
}

// ForwardedFromRequest extracts the allowlisted client cookies and headers from
// the inbound SSE handshake. Reserved headers are skipped defensively. Cookie
// and header values are never logged.
func (c *Client) ForwardedFromRequest(r *http.Request) ForwardedAuth {
	var fwd ForwardedAuth

	if len(c.fwdCookies) > 0 {
		for _, ck := range r.Cookies() {
			if _, ok := c.fwdCookies[ck.Name]; ok {
				fwd.Cookies = append(fwd.Cookies, &http.Cookie{Name: ck.Name, Value: ck.Value})
			}
		}
	}

	if len(c.fwdHeaders) > 0 {
		for name := range c.fwdHeaders {
			if _, reserved := reservedHeaders[name]; reserved {
				continue
			}
			vals := r.Header.Values(name)
			if len(vals) == 0 {
				continue
			}
			if fwd.Headers == nil {
				fwd.Headers = make(http.Header, len(c.fwdHeaders))
			}
			for _, v := range vals {
				fwd.Headers.Add(name, v)
			}
		}
	}

	return fwd
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

func (c *Client) OpenSession(ctx context.Context, bearer string, fwd ForwardedAuth, channel, requestID string) (*Whitelist, error) {
	start := time.Now()
	defer func() {
		c.mc.AuthRequestDuration().Observe(time.Since(start).Seconds())
	}()

	// Credential gate: never make an unauthenticated backend call. The open
	// proceeds when a bearer OR any forwarded cookie/header is present.
	if bearer == "" && fwd.Empty() {
		c.mc.AuthRequests("unauthorized").Inc()
		c.bk.RecordResult(true)
		return nil, &ErrUnauthorized{Body: []byte(`{"reason":"missing_credentials"}`)}
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

	// Apply forwarded credentials FIRST, then let Walera's own headers win.
	// Reserved canonical names are skipped here as defense-in-depth even though
	// config.Validate already rejects them from the allowlist.
	for name, vals := range fwd.Headers {
		if _, reserved := reservedHeaders[textproto.CanonicalMIMEHeaderKey(name)]; reserved {
			continue
		}
		for _, v := range vals {
			req.Header.Add(name, v)
		}
	}
	for _, ck := range fwd.Cookies {
		req.AddCookie(ck)
	}

	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
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

// CheckAuth verifies that the auth backend is reachable AND that the
// configured walera_secret is accepted by it. A successful sentinel
// RefreshPermissions(user_id="_health") proves three things at once:
// backend reachability, HMAC secret freshness, and refresh-path health.
// Non-success statuses 401/403/404 from the backend still count as
// "reachable" — only network errors and 5xx are treated as unavailable.
func (c *Client) CheckAuth(ctx context.Context) error {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("auth: crypto/rand.Read failed: " + err.Error())
	}
	id := "health-" + hex.EncodeToString(buf[:])
	_, err := c.RefreshPermissions(ctx, HealthSentinelUserID, HealthSentinelUserID, id)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *ErrUnauthorized, *ErrForbidden, *ErrNotFound:
		return nil
	}
	return err
}
