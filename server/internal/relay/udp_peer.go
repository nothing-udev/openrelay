package relay

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/openrelay/openrelay/internal/protocol"
	"github.com/rs/zerolog"
)

// UDPPeer represents a single UDP "connection" (source addr + session).
//
// Unlike WSPeer there is no write goroutine: WriteToUDP is safe to call from
// multiple goroutines simultaneously (the OS handles serialisation). This
// keeps the hot relay path lock-free on the write side.
type UDPPeer struct {
	id      uint64
	addr    *net.UDPAddr
	addrStr string       // cached addr.String(), used as sync.Map key
	conn    *net.UDPConn // shared server socket — thread-safe for writes
	session *Session

	reliable *ReliableSender // ARQ layer for control messages

	lastSeen atomic.Int64 // UnixNano, updated on every received datagram
	closed   atomic.Bool  // set to true exactly once by Close()

	log zerolog.Logger
}

var _ Peer = (*UDPPeer)(nil)

func newUDPPeer(id uint64, addr *net.UDPAddr, conn *net.UDPConn, session *Session, metrics *Metrics, log zerolog.Logger) *UDPPeer {
	p := &UDPPeer{
		id:      id,
		addr:    addr,
		addrStr: addr.String(),
		conn:    conn,
		session: session,
		log:     log.With().Uint64("client", id).Str("addr", addr.String()).Str("transport", "udp").Logger(),
	}
	p.lastSeen.Store(time.Now().UnixNano())

	// onDead is the ONLY correct way to remove a UDP peer — it calls
	// session.leave() which handles map cleanup and Disconnected notifications.
	// Calling peer.Close() directly (as the old reliable.go did) left the peer
	// in session.peers forever (memory leak + stale routing).
	p.reliable = newReliableSender(p, metrics, p.log, func() {
		if session != nil {
			session.leave(p)
		}
	})
	return p
}

func (p *UDPPeer) ID() uint64        { return p.id }
func (p *UDPPeer) Transport() string { return "udp" }

// Send writes a datagram. Best-effort, no retransmission.
func (p *UDPPeer) Send(msg *protocol.Message) {
	if p.closed.Load() {
		return
	}
	if _, err := p.conn.WriteToUDP(msg.Encode(), p.addr); err != nil {
		p.log.Error().Err(err).Msg("UDP write error")
	}
}

// SendReliable wraps the message in a ReliableEnvelope and retransmits until
// an Ack arrives or the peer is declared dead.
func (p *UDPPeer) SendReliable(msg *protocol.Message) {
	if p.closed.Load() {
		return
	}
	p.reliable.Send(msg)
}

// AckReceived removes a pending reliable message from the retransmit queue.
// Called by the UDP server when an Ack datagram arrives.
func (p *UDPPeer) AckReceived(seq uint64) {
	p.reliable.Ack(seq)
}

// Close marks the peer as closed and stops the reliable sender.
// Idempotent — safe to call multiple times.
func (p *UDPPeer) Close() {
	if p.closed.CompareAndSwap(false, true) {
		p.reliable.Stop()
	}
}

// touch updates lastSeen to now. Called on every received datagram.
func (p *UDPPeer) touch() {
	p.lastSeen.Store(time.Now().UnixNano())
}

// isTimedOut returns true if no datagram has been received within ttl.
func (p *UDPPeer) isTimedOut(ttl time.Duration) bool {
	return p.lastSeen.Load() < time.Now().Add(-ttl).UnixNano()
}

// sendRaw writes pre-encoded bytes directly to the peer's address.
// Used by the reliable sender for retransmission to avoid re-encoding.
func (p *UDPPeer) sendRaw(data []byte) {
	if p.closed.Load() {
		return
	}
	_, _ = p.conn.WriteToUDP(data, p.addr)
}
