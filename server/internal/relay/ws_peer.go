package relay

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/openrelay/openrelay/internal/protocol"
	"github.com/rs/zerolog"
)

// Keepalive constants. wsReadDeadline must be greater than wsPingInterval.
const (
	wsPingInterval = 30 * time.Second
	wsWriteTimeout = 10 * time.Second
	wsReadDeadline = 90 * time.Second
)

// WSPeer represents one WebSocket connection inside a relay session.
//
// Keepalive: writePump sends a WebSocket Ping every wsPingInterval.
// ClientWebSocket and browsers reply automatically with Pong.
// session.readLoopWS resets the read deadline on every received Pong.
//
// Send-after-close safety: send channel is never closed; writePump exits via
// the done channel; Send() uses a two-stage select — never panics.
type WSPeer struct {
	id   uint64
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
	once sync.Once
	log  zerolog.Logger
}

var _ Peer = (*WSPeer)(nil)

func newWSPeer(id uint64, conn *websocket.Conn, log zerolog.Logger) *WSPeer {
	p := &WSPeer{
		id:   id,
		conn: conn,
		send: make(chan []byte, 256),
		done: make(chan struct{}),
		log:  log.With().Uint64("client", id).Str("transport", "ws").Logger(),
	}
	go p.writePump()
	return p
}

func (p *WSPeer) ID() uint64        { return p.id }
func (p *WSPeer) Transport() string { return "websocket" }

func (p *WSPeer) Send(msg *protocol.Message) {
	select {
	case <-p.done:
		return
	default:
	}
	select {
	case p.send <- msg.Encode():
	case <-p.done:
	default:
		p.log.Warn().Msg("WS send buffer full, dropping")
	}
}

func (p *WSPeer) SendReliable(msg *protocol.Message) { p.Send(msg) }

func (p *WSPeer) Close() {
	p.once.Do(func() {
		close(p.done)
		p.conn.Close()
	})
}

// writePump serialises writes and sends periodic WebSocket Ping frames.
// gorilla/websocket requires all writes from a single goroutine.
func (p *WSPeer) writePump() {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case data := <-p.send:
			if err := p.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout)); err != nil {
				p.Close()
				return
			}
			if err := p.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				p.log.Error().Err(err).Msg("WS write error")
				p.Close()
				return
			}

		case <-ticker.C:
			if err := p.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout)); err != nil {
				p.Close()
				return
			}
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				p.log.Debug().Err(err).Msg("WS ping failed")
				p.Close()
				return
			}

		case <-p.done:
			return
		}
	}
}
