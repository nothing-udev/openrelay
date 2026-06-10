package relay

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/openrelay/openrelay/internal/protocol"
	"github.com/rs/zerolog"
)

// Session manages a group of peers sharing a join code.
// Transport-agnostic: peers may be WebSocket or UDP in the same session.
type Session struct {
	joinCode  string
	createdAt time.Time

	mu     sync.RWMutex
	peers  map[uint64]Peer
	hostID *uint64 // nil until first peer; written once, never cleared

	// nextID is incremented under mu.Lock() — NOT via atomic — because the
	// increment and the "is this the host?" check must be a single atomic
	// operation. Using an atomic outside the lock would allow two goroutines
	// to interleave: goroutine B could win the lock with id=1 and become host,
	// while goroutine A has id=0 and becomes a client.
	// Unity's ServerClientId == 0, so the host MUST receive id=0.
	nextID  uint64
	log     zerolog.Logger
	manager *Manager
	metrics *Metrics
}

func newSession(code string, mgr *Manager, metrics *Metrics, log zerolog.Logger) *Session {
	return &Session{
		joinCode:  code,
		createdAt: time.Now(),
		peers:     make(map[uint64]Peer),
		manager:   mgr,
		metrics:   metrics,
		log:       log.With().Str("session", code).Logger(),
	}
}

func (s *Session) JoinCode() string     { return s.joinCode }
func (s *Session) CreatedAt() time.Time { return s.createdAt }

func (s *Session) PeerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.peers)
}

// ── WebSocket entry point ─────────────────────────────────────────────────────

// JoinWS registers a WebSocket peer and blocks in the read loop until disconnect.
func (s *Session) JoinWS(conn *websocket.Conn) error {
	s.mu.Lock()
	if s.manager.cfg.MaxPeersPerSession > 0 && len(s.peers) >= s.manager.cfg.MaxPeersPerSession {
		s.mu.Unlock()
		conn.Close()
		return fmt.Errorf("session %q full (%d/%d)", s.joinCode, len(s.peers), s.manager.cfg.MaxPeersPerSession)
	}
	id := s.nextID
	s.nextID++
	peer := newWSPeer(id, conn, s.log)
	s.peers[id] = peer
	isHost := s.hostID == nil
	if isHost {
		s.hostID = ptrU64(id)
	}
	s.mu.Unlock()

	s.log.Info().Uint64("id", id).Bool("host", isHost).Str("transport", "ws").Msg("peer joined")
	if s.metrics != nil {
		s.metrics.ActivePeers.Inc()
	}
	s.notifyConnectedDirect(peer, isHost)
	s.readLoopWS(peer)
	return nil
}

// ── UDP entry point ────────────────────────────────────────────────────────────

// JoinUDP registers a UDP peer (non-blocking) and returns it.
//
// IMPORTANT: JoinUDP does NOT call notifyPeerConnected.
// The caller (udp_server.handleHandshake) must:
//  1. Send HandshakeAck to the client FIRST.
//  2. Call session.notifyPeerConnected(peer) AFTER.
//
// This ordering ensures the client exits its handshake loop (which watches for
// HandshakeAck) before the ReliableEnvelope(Connected) arrives. Without this
// fix, Connected could arrive before HandshakeAck and be discarded because the
// client wasn't yet in the normal receive loop.
func (s *Session) JoinUDP(addr *net.UDPAddr, conn *net.UDPConn) (*UDPPeer, error) {
	s.mu.Lock()
	if s.manager.cfg.MaxPeersPerSession > 0 && len(s.peers) >= s.manager.cfg.MaxPeersPerSession {
		s.mu.Unlock()
		return nil, fmt.Errorf("session %q full (%d/%d)", s.joinCode, len(s.peers), s.manager.cfg.MaxPeersPerSession)
	}
	id := s.nextID
	s.nextID++
	peer := newUDPPeer(id, addr, conn, s, s.metrics, s.log)
	s.peers[id] = peer
	isHost := s.hostID == nil
	if isHost {
		s.hostID = ptrU64(id)
	}
	s.mu.Unlock()

	s.log.Info().Uint64("id", id).Bool("host", isHost).Str("transport", "udp").Msg("peer registered")
	if s.metrics != nil {
		s.metrics.ActivePeers.Inc()
	}
	// Store isHost on the peer so notifyPeerConnected can use it.
	// We use the session's hostID to determine this.
	return peer, nil
}

// notifyPeerConnected sends the Connected notification for a UDP peer that has
// already been registered via JoinUDP. Called by udp_server.handleHandshake
// AFTER HandshakeAck is delivered.
func (s *Session) notifyPeerConnected(peer *UDPPeer) {
	s.mu.RLock()
	isHost := s.hostID != nil && *s.hostID == peer.id
	s.mu.RUnlock()
	s.notifyConnectedDirect(peer, isHost)
}

// ── Internal notification helper ─────────────────────────────────────────────

// notifyConnectedDirect sends Connected to the joining peer and to the host.
// Uses SendReliable so UDP clients are guaranteed to receive it under packet loss.
func (s *Session) notifyConnectedDirect(peer Peer, isHost bool) {
	if isHost {
		peer.SendReliable(protocol.ConnectedMessage(peer.ID()))
		return
	}
	// s.hostID is guaranteed non-nil: it was set before the capacity check
	// that admitted this peer, and it is never cleared.
	s.mu.RLock()
	hostID := *s.hostID
	host, hostOK := s.peers[hostID]
	s.mu.RUnlock()

	peer.SendReliable(protocol.ConnectedMessage(hostID))
	if hostOK {
		host.SendReliable(protocol.ConnectedMessage(peer.ID()))
	}
}

// ── WebSocket read loop ───────────────────────────────────────────────────────

func (s *Session) readLoopWS(peer *WSPeer) {
	defer s.leave(peer)
	peer.conn.SetReadLimit(1 << 20)
	if err := peer.conn.SetReadDeadline(time.Now().Add(wsReadDeadline)); err != nil {
		return
	}
	peer.conn.SetPongHandler(func(string) error {
		return peer.conn.SetReadDeadline(time.Now().Add(wsReadDeadline))
	})
	for {
		_, data, err := peer.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				peer.log.Error().Err(err).Msg("unexpected WS close")
			}
			return
		}
		_ = peer.conn.SetReadDeadline(time.Now().Add(wsReadDeadline))
		msg, err := protocol.Decode(data)
		if err != nil {
			peer.log.Debug().Err(err).Msg("malformed WS message")
			continue
		}
		s.handleMessage(peer, msg)
	}
}

// ── Message routing ───────────────────────────────────────────────────────────

// handleMessage routes an incoming relay message.
// Called from readLoopWS (per-peer goroutine) and UDPServer workers (shared pool).
// Goroutine-safe.
func (s *Session) handleMessage(from Peer, msg *protocol.Message) {
	switch msg.Type {

	case protocol.MessageTypeData:
		s.mu.RLock()
		dest, ok := s.peers[msg.AuthorClientId]
		s.mu.RUnlock()
		if !ok {
			s.log.Warn().Uint64("from", from.ID()).Uint64("target", msg.AuthorClientId).
				Msg("data: target not found")
			return
		}
		dest.Send(protocol.DataMessage(from.ID(), msg.Data))
		if s.metrics != nil {
			s.metrics.BytesRelayed.WithLabelValues(from.Transport()).Add(float64(len(msg.Data)))
			s.metrics.MessagesRelayed.WithLabelValues(from.Transport(), "data").Inc()
		}

	case protocol.MessageTypeDataBroadcast:
		out := protocol.DataMessage(from.ID(), msg.Data)
		s.mu.RLock()
		targets := make([]Peer, 0, len(s.peers))
		for id, p := range s.peers {
			if id != from.ID() {
				targets = append(targets, p)
			}
		}
		s.mu.RUnlock()
		for _, p := range targets {
			p.Send(out)
		}
		if s.metrics != nil && len(targets) > 0 {
			s.metrics.BytesRelayed.WithLabelValues(from.Transport()).
				Add(float64(len(msg.Data)) * float64(len(targets)))
			s.metrics.MessagesRelayed.WithLabelValues(from.Transport(), "broadcast").
				Add(float64(len(targets)))
		}

	case protocol.MessageTypeKickFromRelay:
		s.mu.RLock()
		isHost := s.hostID != nil && *s.hostID == from.ID()
		target, ok := s.peers[msg.AuthorClientId]
		s.mu.RUnlock()
		if !isHost {
			s.log.Warn().Uint64("from", from.ID()).Msg("non-host kick ignored")
			return
		}
		if ok {
			s.log.Info().Uint64("kicked", msg.AuthorClientId).Msg("host kicked peer")
			s.leave(target)
		}

	default:
		s.log.Warn().Uint8("type", uint8(msg.Type)).Uint64("from", from.ID()).Msg("unknown message type")
	}
}

// ── Leave & cleanup ───────────────────────────────────────────────────────────

// leave removes a peer and sends Disconnected to the host.
// Idempotent: concurrent calls for the same peer are safe.
func (s *Session) leave(peer Peer) {
	peer.Close()

	s.mu.Lock()
	if _, existed := s.peers[peer.ID()]; !existed {
		s.mu.Unlock()
		return
	}
	delete(s.peers, peer.ID())
	wasHost := s.hostID != nil && *s.hostID == peer.ID()
	s.mu.Unlock()

	s.log.Info().Uint64("id", peer.ID()).Bool("was_host", wasHost).
		Str("transport", peer.Transport()).Msg("peer left")
	if s.metrics != nil {
		s.metrics.ActivePeers.Dec()
	}

	if wasHost {
		s.log.Info().Msg("host left — destroying session")
		s.destroyAll()
		s.manager.deleteSession(s.joinCode)
		return
	}
	// Notify host of departure. SendReliable ensures UDP hosts don't miss it.
	s.mu.RLock()
	var host Peer
	if s.hostID != nil {
		host = s.peers[*s.hostID]
	}
	s.mu.RUnlock()
	if host != nil {
		host.SendReliable(protocol.DisconnectedMessage(peer.ID()))
	}
}

// destroyAll disconnects every remaining peer after the host leaves.
func (s *Session) destroyAll() {
	s.mu.Lock()
	remaining := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		remaining = append(remaining, p)
	}
	s.peers = make(map[uint64]Peer)
	s.mu.Unlock()

	for _, p := range remaining {
		p.Close()
		if s.metrics != nil {
			s.metrics.ActivePeers.Dec()
		}
	}
}

func ptrU64(v uint64) *uint64 { return &v }
