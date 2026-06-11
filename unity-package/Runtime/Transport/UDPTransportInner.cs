using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Unity.Netcode;
using UnityEngine;
using OpenRelay.Protocol;

namespace OpenRelay.Transport
{
    /// <summary>
    /// UDP relay transport.
    ///
    /// Protocol flow:
    ///   1. Resolve server endpoint; create UdpClient.
    ///   2. Send UDPHandshake (join code). Retry up to 5× at 500 ms.
    ///      Buffer any non-ack datagrams received during this phase.
    ///   3. Receive UDPHandshakeAck → assigned peer ID known.
    ///      Immediately dispatch any buffered datagrams (e.g. Connected).
    ///   4. Start receive loop + keepalive loop in parallel.
    ///   5. On Close(): send UDPDisconnect, shut down socket.
    ///
    /// Reliability:
    ///   Connected and Disconnected arrive wrapped in ReliableEnvelope (0xF3).
    ///   We always Ack them; we dispatch the inner message exactly once via a
    ///   HashSet dedup — server retransmits can't produce duplicate NGO events.
    ///
    /// Bug fixes applied here:
    ///   - Pre-handshake buffer: datagrams arriving before HandshakeAck are
    ///     queued and replayed after the handshake completes.
    ///   - Interlocked close flag: prevents TOCTOU race on concurrent Close().
    /// </summary>
    internal sealed class UDPTransportInner : ITransportInner
    {
        // ── tunables ──────────────────────────────────────────────────────────
        private const int HandshakeRetries   = 5;
        private const int HandshakeTimeoutMs = 600;
        private const int PingIntervalMs     = 5_000;
        private const int ReceivePollMs      = 200;  

        // ── state ─────────────────────────────────────────────────────────────
        private readonly string  _endpoint;
        private readonly string  _joinCode;
        private readonly string  _token;           // HMAC auth token ("" = auth disabled)
        private readonly float   _connectTimeoutSec;
        private readonly Action<NetworkEvent, ulong, byte[]> _enqueue;

        private UdpClient  _udp;
        private IPEndPoint _serverEP;

        private int  _closeFlag;        // 0 = open, 1 = closed (Interlocked)
        private bool Closed => _closeFlag != 0;

        private ulong _lastRttMs;
        private bool  _wasKicked;

        // Prevents duplicate NGO events from server retransmissions.
        private readonly HashSet<ulong> _processedSeqs = new();

        // Datagrams received during the handshake phase, before HandshakeAck.
        // Replayed immediately after the handshake succeeds.
        private readonly List<byte[]> _preHandshakeBuf = new();

        public ulong LastRttMs => _lastRttMs;
        /// <summary>
        /// True when the relay server explicitly kicked this peer (not a network drop).
        /// OpenRelayTransport.ConnectAsync checks this to skip the reconnect loop.
        /// </summary>
        public bool WasKicked => _wasKicked;

        public UDPTransportInner(
            string endpoint,
            string joinCode,
            string token,
            float  connectTimeoutSec,
            Action<NetworkEvent, ulong, byte[]> enqueue)
        {
            _endpoint          = endpoint;
            _joinCode          = joinCode;
            _token             = token ?? "";
            _connectTimeoutSec = connectTimeoutSec;
            _enqueue           = enqueue;
        }

        // ── ITransportInner ───────────────────────────────────────────────────

        public void SendRaw(byte[] data)
        {
            if (Closed || _udp == null) return;
            try { _udp.Send(data, data.Length); }
            catch (Exception ex)
                when (!Closed) // suppress errors that occur during/after Close()
            { Debug.LogError($"[OpenRelay][UDP] Send error: {ex.Message}"); }
        }

        /// <summary>
        /// Closes the transport.
        /// execution even when called concurrently (e.g. NGO shutdown + timeout).
        /// </summary>
        public void Close()
        {
            if (Interlocked.CompareExchange(ref _closeFlag, 1, 0) != 0)
                return; 
            try
            {
                _udp?.Send(RelayMessage.UDPDisconnect().Encode(), RelayMessage.HeaderSize);
            }
            catch { }
            _udp?.Close();
            _udp = null;
        }

        public async Task RunAsync(CancellationToken ct)
        {
            _serverEP = await ResolveEndpointAsync(_endpoint, ct);
            _udp = new UdpClient(_serverEP.AddressFamily);
            _udp.Connect(_serverEP);

            Debug.Log($"[OpenRelay][UDP] Handshaking → {_serverEP}  code={_joinCode}");
            await HandshakeAsync(ct);
            Debug.Log("[OpenRelay][UDP] Connected");

            foreach (var raw in _preHandshakeBuf)
                DispatchRaw(raw);
            _preHandshakeBuf.Clear();

            using var loopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var recvTask = Task.Run(() => ReceiveLoopAsync(loopCts.Token), loopCts.Token);
            var pingTask = Task.Run(() => KeepaliveLoopAsync(loopCts.Token), loopCts.Token);

            await Task.WhenAny(recvTask, pingTask);
            loopCts.Cancel();
            await Swallow(recvTask);
            await Swallow(pingTask);
        }

        // ── Handshake ─────────────────────────────────────────────────────────

        /// <summary>
        /// Sends UDPHandshake and waits for UDPHandshakeAck.
        /// Any other datagrams received during this window (e.g. ReliableEnvelope
        /// carrying Connected, which the server may send before HandshakeAck
        /// arrives due to network reordering) are buffered in _preHandshakeBuf
        /// and replayed in RunAsync after handshake success.
        /// </summary>
        private async Task HandshakeAsync(CancellationToken ct)
        {
            var bytes = RelayMessage.UDPHandshake(_joinCode, _token).Encode();
            var ep    = new IPEndPoint(IPAddress.Any, 0);

            for (int attempt = 1; attempt <= HandshakeRetries; attempt++)
            {
                ct.ThrowIfCancellationRequested();
                _udp.Send(bytes, bytes.Length);
                _udp.Client.ReceiveTimeout = HandshakeTimeoutMs;

                // Read packets until we get HandshakeAck, or the timeout fires.
                var deadline = DateTime.UtcNow.AddMilliseconds(HandshakeTimeoutMs);
                while (DateTime.UtcNow < deadline)
                {
                    byte[] resp;
                    try { resp = _udp.Receive(ref ep); }
                    catch (SocketException ex)
                        when (ex.SocketErrorCode == SocketError.TimedOut) { break; }

                    if (resp.Length < RelayMessage.HeaderSize) continue;

                    var type = (RelayMessageType)resp[0];
                    if (type == RelayMessageType.UDPHandshakeAck)
                    {
                        _udp.Client.ReceiveTimeout = 0; 
                        return;                          
                    }
                    if (type == RelayMessageType.UDPHandshakeError)
                    {
                        var msg  = RelayMessage.Decode(resp, 0, resp.Length);
                        var code = (UDPHandshakeErrorCode)msg.AuthorClientId;
                        var reason = code switch
                        {
                            UDPHandshakeErrorCode.SessionNotFound => "session not found",
                            UDPHandshakeErrorCode.SessionFull     => "session is full",
                            UDPHandshakeErrorCode.InvalidToken    => "invalid or expired auth token",
                            _                                     => $"error code {(ulong)code}",
                        };
                        throw new Exception($"[OpenRelay][UDP] Join rejected: {reason}");
                    }
                    _preHandshakeBuf.Add(resp);
                }

                Debug.LogWarning($"[OpenRelay][UDP] Handshake attempt {attempt}/{HandshakeRetries} timed out");
            }
            throw new TimeoutException(
                $"[OpenRelay][UDP] Handshake failed after {HandshakeRetries} attempts.");
        }

        // ── Receive loop ──────────────────────────────────────────────────────

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            _udp.Client.ReceiveTimeout = ReceivePollMs;
            var ep = new IPEndPoint(IPAddress.Any, 0);

            while (!ct.IsCancellationRequested && !Closed)
            {
                byte[] data;
                try
                {
                    data = _udp.Receive(ref ep);
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
                {
                    await Task.Yield(); 
                    continue;
                }
                catch (SocketException) when (Closed) { return; }
                catch (ObjectDisposedException) { return; }

                if (data.Length >= RelayMessage.HeaderSize)
                    DispatchRaw(data);
            }
        }

        private void DispatchRaw(byte[] data)
        {
            try { Dispatch(RelayMessage.Decode(data, 0, data.Length)); }
            catch (Exception ex)
            { Debug.LogWarning($"[OpenRelay][UDP] Decode error: {ex.Message}"); }
        }

        private void Dispatch(RelayMessage msg)
        {
            switch (msg.Type)
            {
                case RelayMessageType.ReliableEnvelope:
                    HandleReliableEnvelope(msg);
                    break;

                case RelayMessageType.Data:
                    if (msg.Data.Count > 0)
                    {
                        var copy = new byte[msg.Data.Count];
                        Buffer.BlockCopy(msg.Data.Array!, msg.Data.Offset, copy, 0, msg.Data.Count);
                        _enqueue(NetworkEvent.Data, msg.AuthorClientId, copy);
                    }
                    break;

                case RelayMessageType.KickFromRelay:
                    Debug.Log("[OpenRelay][UDP] Kicked by host.");
                    _wasKicked = true;
                    // Do NOT enqueue Disconnect here.
                    // ConnectAsync sees WasKicked=true and sends Disconnect without retrying.
                    Close();
                    break;

                case RelayMessageType.UDPPong:
                    break;

                default:
                    Debug.LogWarning($"[OpenRelay][UDP] Unhandled type 0x{(byte)msg.Type:X2}");
                    break;
            }
        }

        /// <summary>
        /// Unwraps a ReliableEnvelope, sends Ack, and dispatches inner message
        /// exactly once. HashSet dedup prevents duplicate NGO events from
        /// server retransmissions (server keeps retransmitting until Ack arrives;
        /// if our Ack is lost, the same sequence arrives again).
        /// </summary>
        private void HandleReliableEnvelope(RelayMessage envelope)
        {
            var seq = envelope.AuthorClientId;

            // Always Ack — even duplicates — so the server stops retransmitting.
            SendRaw(RelayMessage.Ack(seq).Encode());

            // Dispatch inner message exactly once.
            lock (_processedSeqs)
            {
                if (!_processedSeqs.Add(seq)) return;
            }

            if (envelope.Data.Count < RelayMessage.HeaderSize) return;

            var inner = RelayMessage.Decode(
                envelope.Data.Array!,
                envelope.Data.Offset,
                envelope.Data.Count);

            switch (inner.Type)
            {
                case RelayMessageType.Connected:
                    Debug.Log($"[OpenRelay][UDP] Peer {inner.AuthorClientId} connected");
                    _enqueue(NetworkEvent.Connect, inner.AuthorClientId, null);
                    break;

                case RelayMessageType.Disconnected:
                    Debug.Log($"[OpenRelay][UDP] Peer {inner.AuthorClientId} disconnected");
                    _enqueue(NetworkEvent.Disconnect, inner.AuthorClientId, null);
                    break;

                default:
                    Debug.LogWarning(
                        $"[OpenRelay][UDP] Unknown reliable inner type 0x{(byte)inner.Type:X2}");
                    break;
            }
        }

        // ── Keepalive ─────────────────────────────────────────────────────────

        private async Task KeepaliveLoopAsync(CancellationToken ct)
        {
            var ping = RelayMessage.UDPPing().Encode();
            while (!ct.IsCancellationRequested && !Closed)
            {
                await Task.Delay(PingIntervalMs, ct).ConfigureAwait(false);
                if (Closed) break;
                try
                {
                    var t0 = DateTime.UtcNow;
                    _udp.Send(ping, ping.Length);
                    _lastRttMs = (ulong)(DateTime.UtcNow - t0).TotalMilliseconds;
                }
                catch (Exception ex) when (!Closed)
                { Debug.LogWarning($"[OpenRelay][UDP] Ping: {ex.Message}"); }
            }
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private static async Task<IPEndPoint> ResolveEndpointAsync(string ep, CancellationToken ct)
        {
            int colon = ep.LastIndexOf(':');
            if (colon < 0 || !int.TryParse(ep[(colon + 1)..], out int port))
                throw new ArgumentException($"[OpenRelay][UDP] Bad endpoint: '{ep}'");
            var host = ep[..colon].Trim('[', ']');
            var addrs = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
            if (addrs.Length == 0)
                throw new Exception($"[OpenRelay][UDP] DNS failed for '{host}'");
            return new IPEndPoint(addrs[0], port);
        }

        private static async Task Swallow(Task t)
        {
            try { await t.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            { Debug.LogWarning($"[OpenRelay][UDP] Task ended: {ex.Message}"); }
        }
    }
}
