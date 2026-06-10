package api

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/openrelay/openrelay/internal/auth"
	"github.com/openrelay/openrelay/internal/relay"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

// SessionInfo is the API response body for create/join endpoints.
// Token is a short-lived HMAC-SHA256 credential the client must present
// when opening the WebSocket connection (?token=...).
// Empty when HMAC auth is disabled on the server.
type SessionInfo struct {
	JoinCode    string `json:"joinCode"`
	WsEndpoint  string `json:"wsEndpoint,omitempty"`
	UDPEndpoint string `json:"udpEndpoint,omitempty"`
	PeerCount   int    `json:"peerCount"`
	Token       string `json:"token,omitempty"`
}

type SessionListItem struct {
	JoinCode  string    `json:"joinCode"`
	PeerCount int       `json:"peerCount"`
	CreatedAt time.Time `json:"createdAt"`
}

// Handler holds HTTP handler dependencies.
type Handler struct {
	manager       *relay.Manager
	metrics       *relay.Metrics
	publicWSHost  string
	publicUDPAddr string
	wsEnabled     bool
	udpEnabled    bool
	hmacSecret    []byte // nil or empty = auth disabled
	log           zerolog.Logger

	apiLimiters sync.Map // *rate.Limiter (10 req/s, burst 30)
}

func NewHandler(
	mgr *relay.Manager,
	metrics *relay.Metrics,
	publicWSHost, publicUDPAddr string,
	wsEnabled, udpEnabled bool,
	hmacSecret []byte,
	log zerolog.Logger,
) *Handler {
	return &Handler{
		manager:       mgr,
		metrics:       metrics,
		publicWSHost:  publicWSHost,
		publicUDPAddr: publicUDPAddr,
		wsEnabled:     wsEnabled,
		udpEnabled:    udpEnabled,
		hmacSecret:    hmacSecret,
		log:           log,
	}
}

func (h *Handler) RegisterRoutes(r *mux.Router) {
	api := r.PathPrefix("/api/v1").Subrouter()
	api.Use(h.rateLimitMiddleware)
	api.HandleFunc("/sessions/create", h.CreateSession).Methods(http.MethodPut, http.MethodPost)
	api.HandleFunc("/sessions/join", h.JoinSession).Methods(http.MethodPost)
	api.HandleFunc("/sessions", h.ListSessions).Methods(http.MethodGet)
	r.HandleFunc("/health", h.Health).Methods(http.MethodGet)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func (h *Handler) CreateSession(w http.ResponseWriter, r *http.Request) {
	code, err := h.manager.CreateSession()
	if err != nil {
		h.log.Warn().Err(err).Msg("create session failed")
		h.writeError(w, http.StatusServiceUnavailable, err.Error())
		h.recordAPI("create", "503")
		return
	}
	h.writeJSON(w, http.StatusCreated, h.buildInfo(code, 0))
	h.recordAPI("create", "201")
}

func (h *Handler) JoinSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JoinCode string `json:"joinCode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.JoinCode == "" {
		h.writeError(w, http.StatusBadRequest, `body must be {"joinCode":"XXXX"}`)
		return
	}
	sess := h.manager.GetSession(req.JoinCode)
	if sess == nil {
		h.writeError(w, http.StatusNotFound, "session not found")
		h.recordAPI("join", "404")
		return
	}
	h.writeJSON(w, http.StatusOK, h.buildInfo(sess.JoinCode(), sess.PeerCount()))
	h.recordAPI("join", "200")
}

func (h *Handler) ListSessions(w http.ResponseWriter, r *http.Request) {
	sessions := h.manager.AllSessions()
	list := make([]SessionListItem, len(sessions))
	for i, s := range sessions {
		list[i] = SessionListItem{
			JoinCode:  s.JoinCode(),
			PeerCount: s.PeerCount(),
			CreatedAt: s.CreatedAt(),
		}
	}
	h.writeJSON(w, http.StatusOK, list)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	sessions := h.manager.AllSessions()
	clients := 0
	for _, s := range sessions {
		clients += s.PeerCount()
	}
	transports := make([]string, 0, 2)
	if h.wsEnabled {
		transports = append(transports, "websocket")
	}
	if h.udpEnabled {
		transports = append(transports, "udp")
	}
	h.writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "sessions": len(sessions),
		"clients": clients, "transports": transports,
		"auth": len(h.hmacSecret) > 0,
	})
}

// ── Rate limiting ─────────────────────────────────────────────────────────────

func (h *Handler) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := extractIP(r)
		if !h.limiterFor(ip).Allow() {
			h.log.Warn().Str("ip", ip).Msg("rate limited")
			if h.metrics != nil {
				h.metrics.RateLimited.WithLabelValues("api").Inc()
			}
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) limiterFor(ip string) *rate.Limiter {
	if v, ok := h.apiLimiters.Load(ip); ok {
		return v.(*rate.Limiter)
	}
	lim := rate.NewLimiter(10, 30)
	actual, _ := h.apiLimiters.LoadOrStore(ip, lim)
	return actual.(*rate.Limiter)
}

func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i != -1 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// buildInfo constructs a SessionInfo and — when auth is enabled — includes a
// fresh HMAC token the client must present at WebSocket connect time.
func (h *Handler) buildInfo(code string, peerCount int) SessionInfo {
	info := SessionInfo{JoinCode: code, PeerCount: peerCount}
	if h.wsEnabled {
		info.WsEndpoint = "ws://" + h.publicWSHost + "/relay"
	}
	if h.udpEnabled {
		info.UDPEndpoint = h.publicUDPAddr
	}
	if len(h.hmacSecret) > 0 {
		info.Token = auth.GenerateToken(h.hmacSecret, code)
	}
	return info
}

func (h *Handler) recordAPI(ep, status string) {
	if h.metrics != nil {
		h.metrics.APIRequests.WithLabelValues(ep, status).Inc()
	}
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}
