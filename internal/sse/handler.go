// Package sse — HTTP entrypoint for SSE: Handler, constructor, route
// registration, per-path validators, and the runHandshakeAndWriter
// orchestrator that sequences the handshake gates (auth.go) and the
// writer loop (stream.go). See INVARIANTS.md §10 + doc.go #3, #4.
package sse

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"regexp"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/metrics"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// broadcaster is the minimal router-facing interface. *router.Broadcaster
// satisfies it implicitly; handler_test.go's fakeBroadcaster captures
// Register/Deregister calls without the real router data plane.
// Lifetime: pool.Attach(sub) → broadcaster.Register(sub); Register-then-
// Attach would let the router dispatch a frame before sendFunc is wired,
// producing a spurious slow_consumer drop. Deregister is called by the
// handler's defer exactly once when the handler returns.
type broadcaster interface {
	Register(sub *router.Subscriber)
	Deregister(sub *router.Subscriber)
}

// hijackTCPConn hijacks the underlying TCP conn so the pool can perform
// batched writev(2) drains via (*net.Buffers).WriteTo. Returns
// (*net.TCPConn, nil) on success; (nil, nil) when the transport
// declines hijack (h2c/TLS) → caller falls back to respWriter+rc
// per-frame Write+Flush; (nil, err) on unexpected error → caller aborts
// the handshake. On successful hijack: defensive Discard of pre-read
// body bytes, Flush bufrw.Writer (push status-line + headers that
// WriteHeader(200) buffered), assert conn.(*net.TCPConn) — if assertion
// fails (TLS-wrapped conn that nonetheless permitted hijack), close
// the conn and surface errHijackedConnNotTCP (post-hijack the
// respWriter cannot be used either).
func hijackTCPConn(rc *http.ResponseController) (*net.TCPConn, error) {
	conn, bufrw, err := rc.Hijack()
	if err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			// Fallback path: TLS, h2c, httptest.ResponseRecorder.
			return nil, nil
		}
		return nil, err
	}
	if bufrw != nil {
		if buffered := bufrw.Reader.Buffered(); buffered > 0 {
			_, _ = bufrw.Reader.Discard(buffered)
		}
		// Drain handler-side write buffer accumulated between
		// WriteHeader(200) and Hijack(). bufrw is then discarded.
		if ferr := bufrw.Writer.Flush(); ferr != nil {
			_ = conn.Close()
			return nil, ferr
		}
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		// Degenerate: hijack succeeded but conn is not raw TCP.
		_ = conn.Close()
		return nil, errHijackedConnNotTCP
	}
	return tcpConn, nil
}

// tableNameRe matches a Postgres identifier shape — lowercase letter
// or underscore followed by lowercase letters / digits / underscores.
var tableNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// validTableName reports whether s matches the table-name regex.
func validTableName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	return tableNameRe.MatchString(s)
}

// errorBody is the body shape for writeJSONError: {"error":"<code>"}\n.
type errorBody struct {
	Error string `json:"error"`
}

// Handler is the HTTP entrypoint for SSE. Construct via NewHandler.
type Handler struct {
	bc           broadcaster
	routerCfg    router.Config
	cfg          Config
	enc          *Encoder
	pool         *WriterPool
	authClient   *auth.Client
	limits       *limits.Limits
	authRegistry *auth.Subscribers
	breaker      *auth.Breaker
	authCfg      auth.Config
	logger       zerolog.Logger
	metrics      *metrics.Registry
}

// AuthDeps groups the three auth collaborators required by the SSE handler.
// All three fields are required and nil-checked at construction time.
type AuthDeps struct {
	// Client is the auth backend client.
	Client *auth.Client
	// Subscribers is the per-subscriber auth state registry.
	Subscribers *auth.Subscribers
	// Breaker is the auth circuit-breaker.
	Breaker *auth.Breaker
}

// Deps groups the collaborators required by NewHandler. Six top-level fields:
// Broadcaster, Auth (groups the auth trio), Limits, Pool, Logger, Metrics.
// Required (nil-checked): Broadcaster, Auth.Client, Auth.Subscribers,
// Auth.Breaker, Limits, Pool, Metrics. Optional: Logger.
type Deps struct {
	// Broadcaster is the router-facing broadcaster.
	Broadcaster broadcaster
	// Auth groups the three auth collaborators (Client, Subscribers, Breaker).
	Auth AuthDeps
	// Limits is the admission-control limiter.
	Limits *limits.Limits
	// Pool is the SSE WriterPool.
	Pool *WriterPool
	// Logger zero value is a usable Nop.
	Logger zerolog.Logger
	// Metrics is the Prometheus registry.
	Metrics *metrics.Registry
}

// validateDeps panics on any required nil field. Logger is exempt.
// Failing fast at construction surfaces wiring bugs at startup.
func validateDeps(deps Deps) {
	if deps.Broadcaster == nil {
		panic("sse.NewHandler: Deps.Broadcaster is required")
	}
	if deps.Auth.Client == nil {
		panic("sse.NewHandler: Deps.Auth.Client is required")
	}
	if deps.Auth.Subscribers == nil {
		panic("sse.NewHandler: Deps.Auth.Subscribers is required")
	}
	if deps.Auth.Breaker == nil {
		panic("sse.NewHandler: Deps.Auth.Breaker is required")
	}
	if deps.Limits == nil {
		panic("sse.NewHandler: Deps.Limits is required")
	}
	if deps.Pool == nil {
		panic("sse.NewHandler: Deps.Pool is required")
	}
	if deps.Metrics == nil {
		panic("sse.NewHandler: Deps.Metrics is required")
	}
}

// NewHandler constructs an SSE Handler. The encoder is constructed
// inline from cfg.MaxPayloadBytes (owned by the handler).
func NewHandler(cfg Config, deps Deps) *Handler {
	validateDeps(deps)
	return &Handler{
		bc:           deps.Broadcaster,
		routerCfg:    cfg.Router,
		cfg:          cfg,
		enc:          NewEncoder(cfg.MaxPayloadBytes),
		pool:         deps.Pool,
		authClient:   deps.Auth.Client,
		limits:       deps.Limits,
		authRegistry: deps.Auth.Subscribers,
		breaker:      deps.Auth.Breaker,
		authCfg:      cfg.Auth,
		logger:       deps.Logger,
		metrics:      deps.Metrics,
	}
}

// Routes registers the four SSE route patterns on mux. Call once
// during HTTP server setup; mount AFTER health.Server.Routes. Go 1.22
// ServeMux's "more-specific-pattern-wins" precedence routes
// /sse/v1/users/all to serveWildcard and /sse/v1/users/42 to serveExact.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /sse/v1/{table}/{pk}", h.serveExact)
	mux.HandleFunc("GET /sse/v1/{table}/all", h.serveWildcard)
	mux.HandleFunc("OPTIONS /sse/v1/{table}/{pk}", h.servePreflight)
	mux.HandleFunc("OPTIONS /sse/v1/{table}/all", h.servePreflight)
}

// servePreflight handles OPTIONS preflight requests. Always returns 204.
func (h *Handler) servePreflight(w http.ResponseWriter, r *http.Request) {
	if !handlePreflight(w, r, h.cfg.CORSOrigins) {
		// Unreachable: route only matches OPTIONS. Defensive 405.
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// serveExact handles GET /sse/v1/{table}/{pk}. Validates table-name
// regex, pk length, pk != "all", then parses ?since_lsn= before the
// handshake-and-writer orchestrator (validate-then-execute, doc.go #3).
func (h *Handler) serveExact(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	pk := r.PathValue("pk")

	if !validTableName(table) {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_channel")
		return
	}
	if pk == "" || len(pk) > 256 {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_channel")
		return
	}
	// Literal "all" routed to serveExact means the client URL-encoded
	// segments to bypass the more-specific wildcard pattern — reject.
	if pk == "all" {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_channel")
		return
	}

	startLSN, ok := h.parseSinceLSN(w, r)
	if !ok {
		return
	}

	channelStr := table + ":" + pk
	h.runHandshakeAndWriter(w, r, table, pk, channelStr, router.KindExact, startLSN)
}

// serveWildcard handles GET /sse/v1/{table}/all. The auth backend
// gates which root tables a user may subscribe to.
func (h *Handler) serveWildcard(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")

	if !validTableName(table) {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_channel")
		return
	}

	startLSN, ok := h.parseSinceLSN(w, r)
	if !ok {
		return
	}

	channelStr := table + ":all"
	h.runHandshakeAndWriter(w, r, table, "", channelStr, router.KindWildcard, startLSN)
}

// runHandshakeAndWriter executes the handshake gates and (on full
// success) constructs subscribers, registers them, and enters the
// writer loop. Shared core between serveExact/serveWildcard. Any
// non-success path has already written an HTTP response and released
// any acquired limits via the deferred Release flags.
func (h *Handler) runHandshakeAndWriter(
	w http.ResponseWriter,
	r *http.Request,
	table, pk, channelStr string,
	kind router.Kind,
	startLSN pglogrepl.LSN,
) {
	// Flags govern release; runHandshake sets them true only after the
	// matching gate succeeds so early-gate failure does not over-release.
	var hs handshakeResult
	defer func() {
		if hs.globalAcquired {
			h.limits.ReleaseGlobal()
		}
		if hs.perUserAcquired {
			h.limits.ReleasePerUser(hs.userID)
		}
	}()

	res, ok := h.runHandshake(w, r, table, channelStr)
	hs = res
	if !ok {
		return
	}

	h.runWriter(w, r, table, pk, channelStr, kind, startLSN, hs)
}

// parseSinceLSN reads ?since_lsn= and returns the parsed LSN (or
// wal.CurrentLSN when absent). Invalid input writes a 400 JSON error
// directly and returns ok=false.
func (h *Handler) parseSinceLSN(w http.ResponseWriter, r *http.Request) (pglogrepl.LSN, bool) {
	raw := r.URL.Query().Get("since_lsn")
	if raw == "" {
		return wal.CurrentLSN(), true
	}
	parsed, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_since_lsn")
		return 0, false
	}
	return parsed, true
}

// writeJSONError emits a status application/json error body of shape
// {"error":"<code>"}. Used for path-validation; handshake-gate errors
// use writeJSONReason ({"reason":"<code>"}). Vary: Origin is set so
// caches key on Origin even for error responses.
func (h *Handler) writeJSONError(w http.ResponseWriter, r *http.Request, status int, code string) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: code})
}
