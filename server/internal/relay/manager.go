package relay

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/openrelay/openrelay/internal/auth"
	"github.com/rs/zerolog"
)

// Config holds tunable relay parameters.
type Config struct {
	MaxSessions        int
	MaxPeersPerSession int
	JoinCodeLength     int
	SessionTTL         time.Duration
	HMACSecret         []byte // empty = HMAC auth disabled
}

func DefaultConfig() Config {
	return Config{
		MaxSessions:        200,
		MaxPeersPerSession: 16,
		JoinCodeLength:     6,
		SessionTTL:         5 * time.Minute,
	}
}

// Manager owns all active sessions and the WebSocket upgrader.
type Manager struct {
	cfg      Config
	log      zerolog.Logger
	metrics  *Metrics
	upgrader websocket.Upgrader

	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewManager creates a Manager. Call Start(ctx) after server startup to run
// the background session reaper.
func NewManager(cfg Config, metrics *Metrics, log zerolog.Logger) *Manager {
	return &Manager{
		cfg:      cfg,
		log:      log,
		metrics:  metrics,
		sessions: make(map[string]*Session),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

// Start launches the background reaper goroutine tied to ctx.
func (m *Manager) Start(ctx context.Context) {
	if m.cfg.SessionTTL > 0 {
		go m.reapLoop(ctx)
	}
}

// HMACSecret returns the configured HMAC secret (used by api.Handler).
func (m *Manager) HMACSecret() []byte { return m.cfg.HMACSecret }

// ── Session lifecycle ─────────────────────────────────────────────────────────

func (m *Manager) CreateSession() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cfg.MaxSessions > 0 && len(m.sessions) >= m.cfg.MaxSessions {
		return "", fmt.Errorf("server at capacity (%d sessions)", m.cfg.MaxSessions)
	}
	code, err := m.generateCode()
	if err != nil {
		return "", err
	}
	m.sessions[code] = newSession(code, m, m.metrics, m.log)
	m.log.Info().Str("code", code).Msg("session created")
	if m.metrics != nil {
		m.metrics.ActiveSessions.Inc()
		m.metrics.SessionsCreated.Inc()
	}
	return code, nil
}

func (m *Manager) GetSession(code string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[code]
}

func (m *Manager) AllSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s)
	}
	return out
}

// ServeWebSocket upgrades an HTTP connection to WebSocket and joins the session.
// If HMAC auth is enabled, the ?token= query parameter is validated before
// the upgrade — so invalid clients never touch the WebSocket state machine.
func (m *Manager) ServeWebSocket(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	code := q.Get("code")
	if code == "" {
		http.Error(w, `{"error":"missing ?code="}`, http.StatusBadRequest)
		return
	}

	// ── HMAC token validation (pre-upgrade, returns plain HTTP 401) ──────────
	token := q.Get("token")
	if !auth.ValidateToken(m.cfg.HMACSecret, code, token) {
		m.log.Warn().Str("code", code).Str("ip", r.RemoteAddr).Msg("WS token rejected")
		if m.metrics != nil {
			m.metrics.RateLimited.WithLabelValues("ws_auth").Inc()
		}
		http.Error(w, `{"error":"invalid or expired token"}`, http.StatusUnauthorized)
		return
	}

	sess := m.GetSession(code)
	if sess == nil {
		http.Error(w, `{"error":"session not found"}`, http.StatusNotFound)
		return
	}
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		m.log.Error().Err(err).Str("code", code).Msg("WS upgrade failed")
		return
	}
	if err := sess.JoinWS(conn); err != nil {
		m.log.Warn().Err(err).Str("code", code).Msg("WS join rejected")
	}
}

func (m *Manager) deleteSession(code string) {
	m.mu.Lock()
	delete(m.sessions, code)
	m.mu.Unlock()
	m.log.Info().Str("code", code).Msg("session deleted")
	if m.metrics != nil {
		m.metrics.ActiveSessions.Dec()
	}
}

// ── Code generation ───────────────────────────────────────────────────────────

func (m *Manager) generateCode() (string, error) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	n := m.cfg.JoinCodeLength
	if n <= 0 {
		n = 6
	}
	for i := 0; i < 100; i++ {
		b := make([]byte, n)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		for i := range b {
			b[i] = alphabet[int(b[i])%len(alphabet)]
		}
		code := string(b)
		if _, exists := m.sessions[code]; !exists {
			return code, nil
		}
	}
	return "", fmt.Errorf("could not generate unique join code")
}

// ── Reaper ────────────────────────────────────────────────────────────────────

func (m *Manager) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.SessionTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.reapEmpty()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) reapEmpty() {
	deadline := time.Now().Add(-m.cfg.SessionTTL)

	m.mu.RLock()
	candidates := make([]*Session, 0)
	for _, s := range m.sessions {
		if s.createdAt.Before(deadline) {
			candidates = append(candidates, s)
		}
	}
	m.mu.RUnlock()

	for _, s := range candidates {
		if s.PeerCount() != 0 {
			continue
		}
		m.mu.Lock()
		cur, ok := m.sessions[s.joinCode]
		if ok && cur == s {
			s.mu.RLock()
			empty := len(s.peers) == 0
			s.mu.RUnlock()
			if empty {
				delete(m.sessions, s.joinCode)
				m.log.Info().Str("code", s.joinCode).Msg("reaped empty session")
				if m.metrics != nil {
					m.metrics.ActiveSessions.Dec()
				}
			}
		}
		m.mu.Unlock()
	}
}
