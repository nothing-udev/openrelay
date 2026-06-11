package config

import (
	"os"
	"strconv"
	"time"

	"github.com/openrelay/openrelay/internal/relay"
)

const (
	TransportBoth      = "both"
	TransportWebSocket = "websocket"
	TransportUDP       = "udp"
)

type Server struct {
	Transport string // "both" | "websocket" | "udp"

	WSAddr       string // ":7777"
	PublicWSHost string // "my-server.com:7777"

	UDPAddr       string // ":7779"
	PublicUDPAddr string // "my-server.com:7779"

	APIAddr string // ":7778"

	// TLS for the WebSocket relay (makes ws:// → wss://).
	// Set both to enable; leave empty for plain HTTP (default).
	// Works out of the box with Let's Encrypt certbot paths:
	//   OPENRELAY_TLS_CERT=/etc/letsencrypt/live/my.domain/fullchain.pem
	//   OPENRELAY_TLS_KEY=/etc/letsencrypt/live/my.domain/privkey.pem
	TLSCertFile string // OPENRELAY_TLS_CERT
	TLSKeyFile  string // OPENRELAY_TLS_KEY

	Relay relay.Config
}

// TLSEnabled returns true when both cert and key paths are configured.
func (s Server) TLSEnabled() bool {
	return s.TLSCertFile != "" && s.TLSKeyFile != ""
}

func (s Server) WSEnabled() bool {
	return s.Transport == TransportBoth || s.Transport == TransportWebSocket
}
func (s Server) UDPEnabled() bool { return s.Transport == TransportBoth || s.Transport == TransportUDP }

// Load reads configuration from environment variables.
//
//	OPENRELAY_TRANSPORT        "both" | "websocket" | "udp"     (default: "both")
//	OPENRELAY_ADDR             WebSocket listen addr             (default: ":7777")
//	OPENRELAY_PUBLIC_HOST      Public WS host:port               (default: "localhost:7777")
//	OPENRELAY_UDP_ADDR         UDP listen addr                   (default: ":7779")
//	OPENRELAY_UDP_PUBLIC_ADDR  Public UDP host:port              (default: "localhost:7779")
//	OPENRELAY_API_ADDR         HTTP API listen addr              (default: ":7778")
//	OPENRELAY_MAX_SESSIONS     Max concurrent sessions           (default: 200)
//	OPENRELAY_MAX_PEERS        Max peers per session             (default: 16)
//	OPENRELAY_CODE_LENGTH      Join code character count         (default: 6)
//	OPENRELAY_SESSION_TTL      Empty session lifetime            (default: 5m)
//	OPENRELAY_HMAC_SECRET      HMAC-SHA256 secret for WS tokens  (default: "" = disabled)
func Load() Server {
	return Server{
		Transport:     envStr("OPENRELAY_TRANSPORT", TransportBoth),
		WSAddr:        envStr("OPENRELAY_ADDR", ":7777"),
		PublicWSHost:  envStr("OPENRELAY_PUBLIC_HOST", "localhost:7777"),
		UDPAddr:       envStr("OPENRELAY_UDP_ADDR", ":7779"),
		PublicUDPAddr: envStr("OPENRELAY_UDP_PUBLIC_ADDR", "localhost:7779"),
		APIAddr:       envStr("OPENRELAY_API_ADDR", ":7778"),
		TLSCertFile:   envStr("OPENRELAY_TLS_CERT", ""),
		TLSKeyFile:    envStr("OPENRELAY_TLS_KEY", ""),
		Relay: relay.Config{
			MaxSessions:        envInt("OPENRELAY_MAX_SESSIONS", 200),
			MaxPeersPerSession: envInt("OPENRELAY_MAX_PEERS", 16),
			JoinCodeLength:     envInt("OPENRELAY_CODE_LENGTH", 6),
			SessionTTL:         envDur("OPENRELAY_SESSION_TTL", 5*time.Minute),
			HMACSecret:         []byte(envStr("OPENRELAY_HMAC_SECRET", "")),
		},
	}
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
