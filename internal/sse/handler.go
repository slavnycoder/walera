package sse

import (
	"encoding/json"
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

type broadcaster interface {
	Register(sub *router.Subscriber)
	Deregister(sub *router.Subscriber)
}

var tableNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func validTableName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	return tableNameRe.MatchString(s)
}

type errorBody struct {
	Error string `json:"error"`
}

type Handler struct {
	bc               broadcaster
	routerCfg        router.Config
	cfg              Config
	enc              *Encoder
	authClient       *auth.Client
	limits           *limits.Limits
	authRegistry     *auth.Subscribers
	authCfg          auth.Config
	logger           zerolog.Logger
	metrics          *metrics.Registry
	corsAllowHeaders string
}

type AuthDeps struct {
	Client *auth.Client

	Subscribers *auth.Subscribers
}

type Deps struct {
	Broadcaster broadcaster

	Auth AuthDeps

	Limits *limits.Limits

	Logger zerolog.Logger

	Metrics *metrics.Registry
}

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
	if deps.Limits == nil {
		panic("sse.NewHandler: Deps.Limits is required")
	}
	if deps.Metrics == nil {
		panic("sse.NewHandler: Deps.Metrics is required")
	}
}

func NewHandler(cfg Config, deps Deps) *Handler {
	validateDeps(deps)
	return &Handler{
		bc:               deps.Broadcaster,
		routerCfg:        cfg.Router,
		cfg:              cfg,
		enc:              NewEncoder(cfg.MaxPayloadBytes),
		authClient:       deps.Auth.Client,
		limits:           deps.Limits,
		authRegistry:     deps.Auth.Subscribers,
		authCfg:          cfg.Auth,
		logger:           deps.Logger,
		metrics:          deps.Metrics,
		corsAllowHeaders: buildAllowHeaders(cfg.Auth.ForwardedHeaders),
	}
}

func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /sse/v1/{table}/{pk}", h.serveExact)
	mux.HandleFunc("GET /sse/v1/{table}/all", h.serveWildcard)
	mux.HandleFunc("OPTIONS /sse/v1/{table}/{pk}", h.servePreflight)
	mux.HandleFunc("OPTIONS /sse/v1/{table}/all", h.servePreflight)
	mux.HandleFunc("POST /sse/v1/refresh", h.serveRefresh)
	mux.HandleFunc("OPTIONS /sse/v1/refresh", h.serveRefreshPreflight)
}

func (h *Handler) servePreflight(w http.ResponseWriter, r *http.Request) {
	if !handlePreflight(w, r, h.cfg.CORSOrigins, "GET, OPTIONS", h.corsAllowHeaders) {

		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

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

func (h *Handler) runHandshakeAndWriter(
	w http.ResponseWriter,
	r *http.Request,
	table, pk, channelStr string,
	kind router.Kind,
	startLSN pglogrepl.LSN,
) {

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

func (h *Handler) writeJSONError(w http.ResponseWriter, r *http.Request, status int, code string) {
	handleCORS(w, r, h.cfg.CORSOrigins)
	writeNoSniff(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: code})
}
