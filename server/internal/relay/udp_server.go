package relay

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/openrelay/openrelay/internal/auth"
	"github.com/openrelay/openrelay/internal/protocol"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

const (
	udpMaxPacket      = 1472  // 1500 MTU − 28 B IP/UDP headers
	udpWorkers        = 8     // packet-processing goroutines
	udpWorkQueueDepth = 65536 // buffered channel depth
	udpPeerTimeout    = 30 * time.Second
	udpReaperInterval = 10 * time.Second
)

type udpPacket struct {
	addr *net.UDPAddr
	data []byte
}

// UDPServer listens on a single UDP socket and handles all relay traffic.
//
// Architecture for 5-10k CCU:
//   - 1 reader goroutine; copies datagrams into a buffered work channel.
//   - udpWorkers goroutines drain the work channel in parallel.
//   - sync.Map (lock-free reads) for addr→peer lookup.
//   - sync.Pool on the hot read path eliminates per-packet allocations.
//   - WriteToUDP is OS-level thread-safe — no write mutex needed.
//   - Context-aware reaperLoop exits cleanly on shutdown.
type UDPServer struct {
	conn    *net.UDPConn
	manager *Manager
	metrics *Metrics
	log     zerolog.Logger

	peers   sync.Map // string(addr) → *UDPPeer
	workCh  chan udpPacket
	bufPool sync.Pool

	handshakeLimiters sync.Map // string(IP) → *rate.Limiter  (5 handshakes/s, burst 10)
}

// NewUDPServer creates a UDPServer bound to addr (e.g. ":7779").
func NewUDPServer(addr string, mgr *Manager, metrics *Metrics, log zerolog.Logger) (*UDPServer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(8 * 1024 * 1024)
	_ = conn.SetWriteBuffer(8 * 1024 * 1024)

	s := &UDPServer{
		conn:    conn,
		manager: mgr,
		metrics: metrics,
		log:     log.With().Str("transport", "udp").Logger(),
		workCh:  make(chan udpPacket, udpWorkQueueDepth),
	}
	s.bufPool = sync.Pool{New: func() any {
		b := make([]byte, udpMaxPacket)
		return &b
	}}
	return s, nil
}

// Serve blocks until ctx is cancelled or Close() is called.
// BUG FIX: old version had no stop mechanism for reaperLoop — it ran forever
// after server shutdown. Now ctx cancellation stops it cleanly.
func (s *UDPServer) Serve(ctx context.Context) {
	for i := 0; i < udpWorkers; i++ {
		go s.worker()
	}
	go s.reaperLoop(ctx)
	s.readLoop() // blocks until socket closed
	close(s.workCh)
}

// Close stops the server.
func (s *UDPServer) Close() error { return s.conn.Close() }

// ── Read loop ─────────────────────────────────────────────────────────────────

func (s *UDPServer) readLoop() {
	for {
		pBuf := s.bufPool.Get().(*[]byte)
		n, addr, err := s.conn.ReadFromUDP(*pBuf)
		if err != nil {
			s.bufPool.Put(pBuf)
			if isUDPClosedErr(err) {
				return
			}
			s.log.Error().Err(err).Msg("UDP read error")
			continue
		}
		data := make([]byte, n)
		copy(data, (*pBuf)[:n])
		s.bufPool.Put(pBuf)

		select {
		case s.workCh <- udpPacket{addr: addr, data: data}:
		default:
			s.log.Warn().Msg("UDP work queue full — dropping packet")
			if s.metrics != nil {
				s.metrics.UDPPacketsDropped.Inc()
			}
		}
	}
}

// ── Worker pool ───────────────────────────────────────────────────────────────

func (s *UDPServer) worker() {
	for pkt := range s.workCh {
		s.handlePacket(pkt.addr, pkt.data)
	}
}

func (s *UDPServer) handlePacket(addr *net.UDPAddr, data []byte) {
	if len(data) < protocol.HeaderSize {
		return
	}
	msgType := protocol.MessageType(data[0])

	if msgType == protocol.MessageTypeUDPHandshake {
		s.handleHandshake(addr, data)
		return
	}

	addrStr := addr.String()
	v, ok := s.peers.Load(addrStr)
	if !ok {
		return
	}
	peer := v.(*UDPPeer)
	if peer.closed.Load() {
		s.peers.Delete(addrStr)
		return
	}
	peer.touch()

	switch msgType {
	case protocol.MessageTypeAck:
		msg, err := protocol.Decode(data)
		if err != nil {
			return
		}
		peer.AckReceived(msg.AuthorClientId)

	case protocol.MessageTypeUDPPing:
		peer.Send(protocol.UDPPongMessage())

	case protocol.MessageTypeUDPDisconnect:
		s.removePeer(peer, addrStr)

	default:
		msg, err := protocol.Decode(data)
		if err != nil {
			peer.log.Warn().Err(err).Msg("malformed UDP message")
			return
		}
		if peer.session != nil {
			peer.session.handleMessage(peer, msg)
		}
	}
}

// ── Handshake ─────────────────────────────────────────────────────────────────

// handleHandshake sends HandshakeAck BEFORE triggering notifyConnected.
//
// BUG FIX (UDP ordering race): the original code called session.JoinUDP which
// internally calls notifyConnected → sends ReliableEnvelope(Connected) BEFORE
// returning. Then handleHandshake sent HandshakeAck afterward.
//
// On the wire: Connected datagram could arrive before HandshakeAck because
// the client's handshake loop only watches for HandshakeAck. If Connected
// arrived first, it was silently discarded, leaving the client stuck.
//
// Fix: JoinUDP now only registers the peer (no notification). handleHandshake
// sends HandshakeAck first, then calls session.notifyPeerConnected explicitly.
// The client's handshake loop also buffers non-ack datagrams as a safety net.
func (s *UDPServer) handleHandshake(addr *net.UDPAddr, data []byte) {
	msg, err := protocol.Decode(data)
	if err != nil || len(msg.Data) == 0 {
		return
	}

	// ── Per-IP rate limit (5 handshakes/s, burst 10) ─────────────────────
	// Keyed by IP (not IP:port) so rotating ephemeral ports don't bypass it.
	// Drop silently — sending an error reply would enable UDP amplification.
	if !s.handshakeLimiterFor(addr.IP.String()).Allow() {
		s.log.Warn().Str("ip", addr.IP.String()).Msg("UDP handshake rate limited")
		if s.metrics != nil {
			s.metrics.RateLimited.WithLabelValues("udp").Inc()
		}
		return
	}

	// Parse "joinCode\ntoken" (new) or just "joinCode" (legacy / auth disabled).
	// The '\n' delimiter is safe: join codes are A-Z0-9, tokens are "ts.hex".
	payload := string(msg.Data)
	joinCode := payload
	token := ""
	if i := strings.IndexByte(payload, '\n'); i >= 0 {
		joinCode = payload[:i]
		token = payload[i+1:]
	}
	addrStr := addr.String()

	// ── HMAC token validation ──────────────────────────────────────────────
	// ValidateToken is a no-op (returns true) when HMACSecret is empty,
	// so existing unauthed deployments keep working without any config change.
	if !auth.ValidateToken(s.manager.cfg.HMACSecret, joinCode, token) {
		s.log.Warn().Str("code", joinCode).Str("addr", addrStr).Msg("UDP token rejected")
		if s.metrics != nil {
			s.metrics.RateLimited.WithLabelValues("udp_auth").Inc()
		}
		_, _ = s.conn.WriteToUDP(
			protocol.UDPHandshakeErrorMessage(protocol.UDPErrInvalidToken).Encode(), addr)
		return
	}

	// Idempotent re-handshake: resend ack if the peer is already live.
	if v, ok := s.peers.Load(addrStr); ok {
		existing := v.(*UDPPeer)
		if !existing.closed.Load() {
			existing.Send(protocol.UDPHandshakeAckMessage(existing.id))
			return
		}
		s.peers.Delete(addrStr)
	}

	session := s.manager.GetSession(joinCode)
	if session == nil {
		s.log.Debug().Str("code", joinCode).Str("addr", addrStr).Msg("UDP: session not found")
		_, _ = s.conn.WriteToUDP(
			protocol.UDPHandshakeErrorMessage(protocol.UDPErrSessionNotFound).Encode(), addr)
		return
	}

	peer, err := session.JoinUDP(addr, s.conn) // registers peer only; no notification yet
	if err != nil {
		s.log.Warn().Err(err).Str("code", joinCode).Msg("UDP: join rejected")
		_, _ = s.conn.WriteToUDP(
			protocol.UDPHandshakeErrorMessage(protocol.UDPErrSessionFull).Encode(), addr)
		return
	}
	s.peers.Store(addrStr, peer)

	// 1. HandshakeAck FIRST — client exits handshake loop and enters receive loop.
	peer.Send(protocol.UDPHandshakeAckMessage(peer.id))

	// 2. Connected notification AFTER — client is now ready to receive it.
	session.notifyPeerConnected(peer)

	s.log.Info().Uint64("client", peer.id).Str("code", joinCode).Str("addr", addrStr).
		Msg("UDP peer joined")
}

// ── Peer removal ──────────────────────────────────────────────────────────────

func (s *UDPServer) removePeer(peer *UDPPeer, addrStr string) {
	s.peers.Delete(addrStr)
	if peer.session != nil {
		peer.session.leave(peer)
	} else {
		peer.Close()
	}
}

// ── Context-aware reaper ──────────────────────────────────────────────────────

func (s *UDPServer) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(udpReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reapTimedOut()
		case <-ctx.Done():
			return
		}
	}
}

func (s *UDPServer) reapTimedOut() {
	s.peers.Range(func(k, v any) bool {
		peer := v.(*UDPPeer)
		if peer.isTimedOut(udpPeerTimeout) {
			addrStr := k.(string)
			s.log.Info().Str("addr", addrStr).Uint64("client", peer.id).Msg("UDP peer timed out")
			s.removePeer(peer, addrStr)
		}
		return true
	})
}

// handshakeLimiterFor returns (creating if needed) the per-IP rate limiter for
// UDP handshake packets. Keyed by IP without port so ephemeral port cycling
// cannot bypass the limit.
func (s *UDPServer) handshakeLimiterFor(ip string) *rate.Limiter {
	if v, ok := s.handshakeLimiters.Load(ip); ok {
		return v.(*rate.Limiter)
	}
	lim := rate.NewLimiter(5, 10)
	actual, _ := s.handshakeLimiters.LoadOrStore(ip, lim)
	return actual.(*rate.Limiter)
}

func isUDPClosedErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}
