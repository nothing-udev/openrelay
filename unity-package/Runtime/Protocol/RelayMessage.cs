using System;
using System.Text;

namespace OpenRelay.Protocol
{
    public enum RelayMessageType : byte
    {
        // ── Both transports ───────────────────────────────────────────────────
        Data          = 0x00,
        KickFromRelay = 0x01,
        DataBroadcast = 0x02,
        Connected     = 0x10,
        Disconnected  = 0x12,

        // ── UDP only: handshake ───────────────────────────────────────────────
        UDPHandshake      = 0xF0,
        UDPHandshakeAck   = 0xF1,
        UDPHandshakeError = 0xFC,

        // ── UDP only: keepalive ───────────────────────────────────────────────
        UDPPing       = 0xFD,
        UDPPong       = 0xFE,
        UDPDisconnect = 0xFF,

        // ── UDP only: reliable delivery ───────────────────────────────────────
        ReliableEnvelope = 0xF3,
        Ack              = 0xFB,
    }

    public enum UDPHandshakeErrorCode : ulong
    {
        SessionNotFound = 1,
        SessionFull     = 2,
    }

    /// <summary>
    /// A relay control message.
    ///
    /// Wire format (big-endian):
    ///   [Type 1B][AuthorClientId 8B][Data 0…N B]
    ///
    /// Minimum valid message = 9 bytes (HeaderSize).
    /// </summary>
    public readonly struct RelayMessage
    {
        public readonly RelayMessageType  Type;
        public readonly ulong             AuthorClientId;
        public readonly ArraySegment<byte> Data;

        public const int HeaderSize = 9;

        public RelayMessage(RelayMessageType type, ulong authorClientId = 0, ArraySegment<byte> data = default)
        {
            Type           = type;
            AuthorClientId = authorClientId;
            Data           = data;
        }

        // ── Encode / Decode ───────────────────────────────────────────────────

        public byte[] Encode()
        {
            var buf = new byte[HeaderSize + Data.Count];
            buf[0] = (byte)Type;
            WriteBE64(buf, 1, AuthorClientId);
            if (Data.Count > 0)
                Buffer.BlockCopy(Data.Array!, Data.Offset, buf, HeaderSize, Data.Count);
            return buf;
        }

        public static RelayMessage Decode(byte[] buf, int offset, int count)
        {
            if (count < HeaderSize)
                throw new InvalidOperationException($"[OpenRelay] Packet too short: {count}B");
            return new RelayMessage(
                (RelayMessageType)buf[offset],
                ReadBE64(buf, offset + 1),
                count > HeaderSize
                    ? new ArraySegment<byte>(buf, offset + HeaderSize, count - HeaderSize)
                    : default);
        }

        // ── Factory methods for common message types ───────────────────────────

        public static RelayMessage DataMessage(ulong target, ArraySegment<byte> payload)
            => new(RelayMessageType.Data, target, payload);

        /// <summary>
        /// Server-side broadcast: the relay fans this out to all peers.
        /// Use instead of N individual DataMessage() calls on the host — O(1) bandwidth.
        /// </summary>
        public static RelayMessage Broadcast(ArraySegment<byte> payload)
            => new(RelayMessageType.DataBroadcast, 0, payload);

        public static RelayMessage Kick(ulong target)
            => new(RelayMessageType.KickFromRelay, target);

        public static RelayMessage UDPHandshake(string joinCode)
        {
            var bytes = Encoding.UTF8.GetBytes(joinCode);
            return new(RelayMessageType.UDPHandshake, 0, new ArraySegment<byte>(bytes));
        }

        public static RelayMessage UDPPing() => new(RelayMessageType.UDPPing);
        public static RelayMessage Ack(ulong seq) => new(RelayMessageType.Ack, seq);
        public static RelayMessage UDPDisconnect() => new(RelayMessageType.UDPDisconnect);

        private static void WriteBE64(byte[] b, int o, ulong v)
        {
            b[o]   = (byte)(v >> 56); b[o+1] = (byte)(v >> 48);
            b[o+2] = (byte)(v >> 40); b[o+3] = (byte)(v >> 32);
            b[o+4] = (byte)(v >> 24); b[o+5] = (byte)(v >> 16);
            b[o+6] = (byte)(v >>  8); b[o+7] = (byte) v;
        }

        private static ulong ReadBE64(byte[] b, int o)
            => ((ulong)b[o]   << 56) | ((ulong)b[o+1] << 48)
             | ((ulong)b[o+2] << 40) | ((ulong)b[o+3] << 32)
             | ((ulong)b[o+4] << 24) | ((ulong)b[o+5] << 16)
             | ((ulong)b[o+6] <<  8) |  (ulong)b[o+7];
    }
}
