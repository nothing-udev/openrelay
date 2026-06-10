package relay

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/openrelay/openrelay/internal/protocol"
	"github.com/rs/zerolog"
)

const (
	reliableTickInterval = 50 * time.Millisecond
	reliableBaseDelay    = 100 * time.Millisecond
	reliableMaxDelay     = 5 * time.Second
	reliableMaxAttempts  = 10
)

// pendingMsg holds a pre-encoded ReliableEnvelope awaiting acknowledgement.
type pendingMsg struct {
	raw      []byte // pre-encoded complete UDP payload — no re-encoding on retransmit
	attempts int
	nextAt   time.Time
}

// ReliableSender implements a simple stop-and-wait ARQ layer on top of UDP.
// It is created per-UDPPeer and wraps control messages (Connected, Disconnected)
// that must be delivered exactly once.
//
// Fixed bugs vs the original implementation:
//  1. Pre-encoded raw bytes stored — no re-encoding on every retransmission.
//  2. rs.mu released BEFORE calling sendRaw — no mutex held during syscall.
//  3. When retries exhausted, onDead() is called (→ session.leave), not
//     peer.Close() — the old approach left the peer in session.peers forever.
//  4. Stop() is idempotent via sync.Once.
type ReliableSender struct {
	peer    *UDPPeer
	onDead  func() // called once when peer is declared unresponsive
	metrics *Metrics
	log     zerolog.Logger

	mu      sync.Mutex
	pending map[uint64]*pendingMsg

	nextSeq  uint64 // access via atomic.AddUint64
	stop     chan struct{}
	stopOnce sync.Once
}

func newReliableSender(
	peer *UDPPeer,
	metrics *Metrics,
	log zerolog.Logger,
	onDead func(),
) *ReliableSender {
	rs := &ReliableSender{
		peer:    peer,
		onDead:  onDead,
		metrics: metrics,
		log:     log,
		pending: make(map[uint64]*pendingMsg),
		stop:    make(chan struct{}),
	}
	go rs.tickLoop()
	return rs
}

// Send wraps msg in a ReliableEnvelope, stores pre-encoded bytes, and sends
// the first copy immediately without waiting for the first tick.
func (rs *ReliableSender) Send(msg *protocol.Message) {
	seq := atomic.AddUint64(&rs.nextSeq, 1) - 1

	envelope := &protocol.Message{
		Type:           protocol.MessageTypeReliableEnvelope,
		AuthorClientId: seq,
		Data:           msg.Encode(), // inner message encoded as Data
	}
	raw := envelope.Encode()

	rs.mu.Lock()
	rs.pending[seq] = &pendingMsg{
		raw:    raw,
		nextAt: time.Now().Add(reliableBaseDelay), // first retransmit after base delay
	}
	rs.mu.Unlock()

	// First transmission immediately (no need to wait for tick).
	rs.peer.sendRaw(raw)
}

// Ack removes a pending entry. Called when the client's Ack datagram arrives.
func (rs *ReliableSender) Ack(seq uint64) {
	rs.mu.Lock()
	delete(rs.pending, seq)
	rs.mu.Unlock()
}

// Stop halts the retransmit goroutine. Idempotent.
func (rs *ReliableSender) Stop() {
	rs.stopOnce.Do(func() { close(rs.stop) })
}

func (rs *ReliableSender) tickLoop() {
	ticker := time.NewTicker(reliableTickInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			rs.tick(now)
		case <-rs.stop:
			return
		}
	}
}

// tick scans pending messages and retransmits or calls onDead.
//
// Key correctness property: the lock is released BEFORE every sendRaw call.
// This ensures we never hold a mutex during WriteToUDP (a blocking syscall).
func (rs *ReliableSender) tick(now time.Time) {
	rs.mu.Lock()

	var toSend [][]byte
	deadSeq, dead := uint64(0), false

	for seq, pm := range rs.pending {
		if now.Before(pm.nextAt) {
			continue
		}
		pm.attempts++
		if pm.attempts > reliableMaxAttempts {
			deadSeq, dead = seq, true
			break
		}
		// Exponential back-off capped at reliableMaxDelay.
		delay := reliableBaseDelay << uint(pm.attempts-1)
		if delay > reliableMaxDelay {
			delay = reliableMaxDelay
		}
		pm.nextAt = now.Add(delay)
		toSend = append(toSend, pm.raw)
	}

	if dead {
		delete(rs.pending, deadSeq)
	}

	rs.mu.Unlock() // MUST be released before any sendRaw / onDead call

	if dead {
		rs.log.Warn().Uint64("seq", deadSeq).Msg("reliable delivery failed — declaring peer dead")
		rs.Stop()
		rs.onDead() // → session.leave(peer)
		return
	}

	for _, raw := range toSend {
		rs.peer.sendRaw(raw)
		if rs.metrics != nil {
			rs.metrics.ReliableRetransmits.Inc()
		}
	}
}
