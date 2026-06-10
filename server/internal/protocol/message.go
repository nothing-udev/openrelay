package protocol

import (
	"encoding/binary"
	"fmt"
)

// MessageType is the first byte of every relay message on the wire.
type MessageType byte

const (
	// ── Application messages (both WebSocket and UDP) ──────────────────────

	// MessageTypeData relays a payload to one specific peer.
	//   client→server: AuthorClientId = target peer ID.
	//   server→client: AuthorClientId = sender peer ID.
	MessageTypeData MessageType = 0x00

	// MessageTypeKickFromRelay allows the host to disconnect a specific peer.
	//   Direction: host→server. AuthorClientId = target peer ID.
	MessageTypeKickFromRelay MessageType = 0x01

	// MessageTypeDataBroadcast relays a payload to ALL peers except the sender.
	//   Direction: client→server. The server fans it out as Data messages.
	//   Reduces host upstream bandwidth from O(N) sends to O(1).
	MessageTypeDataBroadcast MessageType = 0x02

	// MessageTypeConnected signals that a peer joined the session.
	//   Direction: server→client.
	//   Host receives: AuthorClientId = new client ID.
	//   Client receives: AuthorClientId = host ID (always 0).
	MessageTypeConnected MessageType = 0x10

	// MessageTypeDisconnected signals that a peer left the session.
	//   Direction: server→host. AuthorClientId = departed peer ID.
	MessageTypeDisconnected MessageType = 0x12

	// ── UDP transport: session handshake ───────────────────────────────────

	// MessageTypeUDPHandshake is the first packet a UDP client sends.
	//   Direction: client→server. Data = UTF-8 join code.
	MessageTypeUDPHandshake MessageType = 0xF0

	// MessageTypeUDPHandshakeAck confirms a successful join.
	//   Direction: server→client. AuthorClientId = assigned peer ID.
	MessageTypeUDPHandshakeAck MessageType = 0xF1

	// MessageTypeUDPHandshakeError signals a failed join.
	//   Direction: server→client. AuthorClientId = UDPErrorCode.
	MessageTypeUDPHandshakeError MessageType = 0xFC

	// ── UDP transport: keepalive ───────────────────────────────────────────

	// MessageTypeUDPPing is sent by the client every 5 s.
	MessageTypeUDPPing MessageType = 0xFD

	// MessageTypeUDPPong is the server's reply to a Ping.
	MessageTypeUDPPong MessageType = 0xFE

	// MessageTypeUDPDisconnect is a graceful disconnect notification from client.
	MessageTypeUDPDisconnect MessageType = 0xFF

	// ── UDP transport: reliable delivery (ARQ) ─────────────────────────────
	//
	// UDP is connectionless. Connected/Disconnected must arrive exactly once.
	// They are wrapped in a ReliableEnvelope; the client returns an Ack.
	// The server retransmits until the Ack arrives or the peer is dead.

	// MessageTypeReliableEnvelope wraps a control message for guaranteed delivery.
	//   Direction: server→client.
	//   AuthorClientId = monotonically increasing sequence number.
	//   Data = fully encoded inner message (e.g. Connected).
	MessageTypeReliableEnvelope MessageType = 0xF3

	// MessageTypeAck acknowledges receipt of a ReliableEnvelope.
	//   Direction: client→server.
	//   AuthorClientId = sequence number being acknowledged.
	MessageTypeAck MessageType = 0xFB
)

// UDPErrorCode is carried in MessageTypeUDPHandshakeError.AuthorClientId.
type UDPErrorCode uint64

const (
	UDPErrSessionNotFound UDPErrorCode = 1
	UDPErrSessionFull     UDPErrorCode = 2
)

// ── Wire format ────────────────────────────────────────────────────────────────
//
//   ┌──────────┬──────────────────────────┬─────────────────┐
//   │ Type     │ AuthorClientId (uint64)  │ Data (optional) │
//   │ 1 byte   │ 8 bytes big-endian       │ 0…N bytes       │
//   └──────────┴──────────────────────────┴─────────────────┘
//
// HeaderSize = 9 bytes (minimum valid message length).

// Message is a relay control message.
type Message struct {
	Type           MessageType
	AuthorClientId uint64
	Data           []byte
}

// HeaderSize is the fixed on-wire header.
const HeaderSize = 9

// Encode serialises the message to a freshly allocated byte slice.
func (m *Message) Encode() []byte {
	buf := make([]byte, HeaderSize+len(m.Data))
	buf[0] = byte(m.Type)
	binary.BigEndian.PutUint64(buf[1:9], m.AuthorClientId)
	if len(m.Data) > 0 {
		copy(buf[HeaderSize:], m.Data)
	}
	return buf
}

// Decode parses a Message from buf, copying the payload into a fresh slice.
func Decode(buf []byte) (*Message, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("message too short: %d bytes (need %d)", len(buf), HeaderSize)
	}
	m := &Message{
		Type:           MessageType(buf[0]),
		AuthorClientId: binary.BigEndian.Uint64(buf[1:9]),
	}
	if len(buf) > HeaderSize {
		m.Data = make([]byte, len(buf)-HeaderSize)
		copy(m.Data, buf[HeaderSize:])
	}
	return m, nil
}

// ── Constructor helpers ────────────────────────────────────────────────────────

func DataMessage(senderOrTarget uint64, payload []byte) *Message {
	return &Message{Type: MessageTypeData, AuthorClientId: senderOrTarget, Data: payload}
}

func DataBroadcastMessage(payload []byte) *Message {
	return &Message{Type: MessageTypeDataBroadcast, Data: payload}
}

func ConnectedMessage(peerID uint64) *Message {
	return &Message{Type: MessageTypeConnected, AuthorClientId: peerID}
}

func DisconnectedMessage(peerID uint64) *Message {
	return &Message{Type: MessageTypeDisconnected, AuthorClientId: peerID}
}

func UDPHandshakeAckMessage(clientID uint64) *Message {
	return &Message{Type: MessageTypeUDPHandshakeAck, AuthorClientId: clientID}
}

func UDPHandshakeErrorMessage(code UDPErrorCode) *Message {
	return &Message{Type: MessageTypeUDPHandshakeError, AuthorClientId: uint64(code)}
}

func UDPPongMessage() *Message {
	return &Message{Type: MessageTypeUDPPong}
}

func AckMessage(seq uint64) *Message {
	return &Message{Type: MessageTypeAck, AuthorClientId: seq}
}
