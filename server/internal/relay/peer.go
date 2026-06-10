package relay

import "github.com/openrelay/openrelay/internal/protocol"

// Peer represents a connected relay participant regardless of transport.
// Sessions operate entirely through this interface.
type Peer interface {
	// ID returns the unique client ID assigned when the peer joined.
	// The first peer in every session (the host) always receives ID 0.
	ID() uint64

	// Send delivers a message with best-effort (fire-and-forget).
	// Used for Data and Broadcast traffic where occasional loss is acceptable.
	// Must be goroutine-safe and must never panic after Close().
	Send(msg *protocol.Message)

	// SendReliable delivers a message with retransmission until acknowledged.
	// Used for control messages (Connected, Disconnected) where loss is not
	// acceptable. For WebSocket this is identical to Send() since TCP provides
	// reliability. For UDP it uses the ARQ layer.
	// Must be goroutine-safe and must never panic after Close().
	SendReliable(msg *protocol.Message)

	// Close disconnects the peer. Must be idempotent.
	Close()

	// Transport returns a label for logs and metrics ("websocket" or "udp").
	Transport() string
}
