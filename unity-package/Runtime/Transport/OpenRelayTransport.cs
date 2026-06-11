using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using OpenRelay.Api;
using OpenRelay.Protocol;
using Unity.Netcode;
using UnityEngine;

namespace OpenRelay.Transport
{
    public enum RelayTransportMode
    {
        PreferUDP     = 0,
        WebSocketOnly = 1,
        UDPOnly       = 2,
    }

    /// <summary>
    /// Drop-in Unity NGO transport for self-hosted OpenRelay.
    ///
    /// Usage:
    ///   Host:   code = await transport.StartServerWithSession();
    ///           NetworkManager.Singleton.StartHost();
    ///   Client: await transport.StartClientWithCode(code);
    ///           NetworkManager.Singleton.StartClient();
    ///
    /// Broadcast (O(1) upstream):
    ///   transport.SendBroadcast(data);  // server fans out to all peers
    ///
    /// WebGL: set TransportMode = WebSocketOnly.
    /// Auth:  transparent — token is fetched from the API and passed to the
    ///        WS connection automatically. No extra setup needed.
    /// </summary>
    [AddComponentMenu("OpenRelay/OpenRelay Transport")]
    public class OpenRelayTransport : NetworkTransport
    {
        [Header("Server")]
        [Tooltip("HTTP API URL. Example: http://my-server.com:7778")]
        public string ApiBaseUrl = "http://localhost:7778";

        [Tooltip("PreferUDP (default) | WebSocketOnly (WebGL) | UDPOnly")]
        public RelayTransportMode TransportMode = RelayTransportMode.PreferUDP;

        [Header("Timing")]
        public float ConnectTimeoutSeconds = 10f;
        public int   WsReceiveBufferSize   = 65536;

        [Header("Reconnect")]
        [Tooltip("Automatically retry on unexpected disconnects. Hosts never reconnect (their session is destroyed).")]
        public bool  AutoReconnect         = true;
        [Tooltip("Maximum reconnect attempts before raising TransportFailure.")]
        public int   MaxReconnectAttempts  = 5;
        [Tooltip("Base delay between attempts in seconds (doubles each try, capped at 30 s).")]
        public float ReconnectDelaySeconds = 2f;

        [Header("Session (populated at runtime)")]
        public string JoinCode    = "";
        public string WsEndpoint  = "";
        public string UdpEndpoint = "";

        // ── NetworkTransport ──────────────────────────────────────────────────

        public override ulong ServerClientId => 0;
        public override bool  IsSupported    => true;

        public override void Initialize(NetworkManager networkManager = null)
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts               = new CancellationTokenSource();
            _queue             = new ConcurrentQueue<RelayEvent>();
            _reconnectAttempts = 0;
        }

        public override bool StartServer() { _isHost = true;  BeginConnect(); return true; }
        public override bool StartClient() { _isHost = false; BeginConnect(); return true; }

        public override void Send(ulong clientId, ArraySegment<byte> data, NetworkDelivery _)
            => _inner?.SendRaw(RelayMessage.DataMessage(clientId, data).Encode());

        /// <summary>
        /// Server-side fanout: one call delivers to ALL peers.
        /// Use instead of N individual Send() calls — saves O(N) upstream bandwidth.
        /// </summary>
        public void SendBroadcast(ArraySegment<byte> data)
            => _inner?.SendRaw(RelayMessage.Broadcast(data).Encode());

        public override NetworkEvent PollEvent(
            out ulong clientId, out ArraySegment<byte> payload, out float receiveTime)
        {
            clientId    = 0;
            payload     = default;
            receiveTime = Time.realtimeSinceStartup;

            if (_queue != null && _queue.TryDequeue(out var ev))
            {
                clientId    = ev.ClientId;
                receiveTime = Time.realtimeSinceStartup;
                if (ev.Data != null) payload = new ArraySegment<byte>(ev.Data);
                return ev.Event;
            }
            return NetworkEvent.Nothing;
        }

        public override void DisconnectRemoteClient(ulong clientId)
            => _inner?.SendRaw(RelayMessage.Kick(clientId).Encode());

        public override void DisconnectLocalClient() { _cts?.Cancel(); _inner?.Close(); }

        public override ulong GetCurrentRtt(ulong _) => _inner?.LastRttMs ?? 0;

        public override void Shutdown()
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;

            _inner?.Close();
            _inner = null;
        }

        // ── Unity lifecycle ───────────────────────────────────────────────────

        /// <summary>
        /// Pumps NativeWebSocket's message queue on the main thread.
        /// No-op for UDP transport and on WebGL (where callbacks arrive via JS).
        /// </summary>
        private void Update()
        {
            (_inner as WSTransportInner)?.DispatchMessageQueue();
        }

        // ── Public helpers ────────────────────────────────────────────────────

        public async Task<string> StartServerWithSession()
        {
            var info = await OpenRelayApiClient.CreateSessionAsync(ApiBaseUrl);
            ApplySessionInfo(info);
            Debug.Log($"[OpenRelay] Session: {JoinCode} transport: {_activeMode}");
            return JoinCode;
        }

        public async Task StartClientWithCode(string joinCode)
        {
            var info = await OpenRelayApiClient.JoinSessionAsync(ApiBaseUrl, joinCode);
            ApplySessionInfo(info);
            Debug.Log($"[OpenRelay] Joined: {JoinCode} transport: {_activeMode}");
        }

        // ── Private ───────────────────────────────────────────────────────────

        private ConcurrentQueue<RelayEvent> _queue;
        private CancellationTokenSource     _cts;
        private ITransportInner             _inner;
        private RelayTransportMode          _activeMode;
        private string                      _token = "";
        private bool                        _isHost;
        private int                         _reconnectAttempts;

        private void ApplySessionInfo(SessionInfo info)
        {
            JoinCode    = info.joinCode   ?? "";
            WsEndpoint  = info.wsEndpoint  ?? "";
            UdpEndpoint = info.udpEndpoint ?? "";
            _token      = info.token       ?? "";

            bool hasWS  = !string.IsNullOrEmpty(WsEndpoint);
            bool hasUDP = !string.IsNullOrEmpty(UdpEndpoint);

            _activeMode = TransportMode switch
            {
                RelayTransportMode.WebSocketOnly => RelayTransportMode.WebSocketOnly,
                RelayTransportMode.UDPOnly       => RelayTransportMode.UDPOnly,
                _                                => hasUDP
                                                        ? RelayTransportMode.UDPOnly
                                                        : RelayTransportMode.WebSocketOnly,
            };

            if (_activeMode == RelayTransportMode.UDPOnly && !hasUDP)
                throw new InvalidOperationException(
                    "[OpenRelay] UDP requested but server has no UDP endpoint.");
            if (_activeMode == RelayTransportMode.WebSocketOnly && !hasWS)
                throw new InvalidOperationException(
                    "[OpenRelay] WebSocket requested but server has no WS endpoint.");
        }

        private void BeginConnect()
        {
            _ = Task.Run(() => ConnectAsync(_cts?.Token ?? CancellationToken.None));
        }

        /// <summary>Creates the appropriate inner transport based on the current session info.</summary>
        private ITransportInner CreateInner() =>
            _activeMode == RelayTransportMode.UDPOnly
                ? (ITransportInner)new UDPTransportInner(
                    UdpEndpoint, JoinCode, _token, ConnectTimeoutSeconds, Enqueue)
                : new WSTransportInner(
                    WsEndpoint, JoinCode, _token,
                    ConnectTimeoutSeconds, WsReceiveBufferSize, Enqueue);

        private async Task ConnectAsync(CancellationToken ct)
        {
            // Bootstrap: if ApplySessionInfo was never called (manual endpoint setup),
            // build session info from the inspector fields.
            if (_activeMode == default)
                ApplySessionInfo(new SessionInfo
                {
                    joinCode    = JoinCode,
                    wsEndpoint  = WsEndpoint,
                    udpEndpoint = UdpEndpoint,
                    token       = _token,
                });

            _reconnectAttempts = 0;

            while (!ct.IsCancellationRequested)
            {
                bool isRetry = _reconnectAttempts > 0;

                try
                {
                    // On retry: fetch a fresh token — the original expires in 5 min.
                    // Hosts don't retry (server-side session is gone on host disconnect).
                    if (isRetry)
                    {
                        Debug.Log($"[OpenRelay] Reconnect attempt {_reconnectAttempts}/{MaxReconnectAttempts}…");
                        var fresh = await OpenRelayApiClient.JoinSessionAsync(ApiBaseUrl, JoinCode);
                        ApplySessionInfo(fresh);
                    }

                    _inner?.Close();
                    _inner = CreateInner();

                    // Track connect time to reset counter after a stable session.
                    var connectedAt = DateTime.UtcNow;
                    await _inner.RunAsync(ct);

                    // If we were stably connected (≥ 10 s), treat the next drop
                    // as a fresh reconnect sequence rather than continuing the count.
                    if ((DateTime.UtcNow - connectedAt).TotalSeconds >= 10)
                        _reconnectAttempts = 0;
                }
                catch (OperationCanceledException) { return; }
                catch (Exception ex)
                {
                    Debug.LogError($"[OpenRelay] {(isRetry ? "Reconnect" : "Connect")} error: {ex.Message}");
                }

                if (ct.IsCancellationRequested) return;

                // ── Was this an explicit kick? ───────────────────────────────
                // Kicks are intentional — don't retry, just report disconnect.
                if (_inner?.WasKicked == true)
                {
                    Debug.Log("[OpenRelay] Kicked by host — not reconnecting.");
                    Enqueue(NetworkEvent.Disconnect, 0, null);
                    return;
                }

                // ── Hosts never reconnect ────────────────────────────────────
                // Their session is destroyed the moment they disconnect;
                // reconnecting would create a ghost peer, not restore the room.
                if (_isHost)
                {
                    Enqueue(NetworkEvent.Disconnect, 0, null);
                    return;
                }

                // ── Auto-reconnect disabled ──────────────────────────────────
                if (!AutoReconnect)
                {
                    Enqueue(NetworkEvent.Disconnect, 0, null);
                    return;
                }

                // ── Retry budget exhausted ───────────────────────────────────
                _reconnectAttempts++;
                if (_reconnectAttempts > MaxReconnectAttempts)
                {
                    Debug.LogError($"[OpenRelay] Gave up after {MaxReconnectAttempts} reconnect attempts.");
                    Enqueue(NetworkEvent.TransportFailure, 0, null);
                    return;
                }

                // Exponential back-off: 2 s → 4 s → 8 s → 16 s → 30 s cap.
                float delaySec = Mathf.Min(
                    ReconnectDelaySeconds * Mathf.Pow(2f, _reconnectAttempts - 1f), 30f);
                Debug.LogWarning(
                    $"[OpenRelay] Disconnected. Retry {_reconnectAttempts}/{MaxReconnectAttempts} in {delaySec:F0}s…");

                try { await Task.Delay(TimeSpan.FromSeconds(delaySec), ct); }
                catch (OperationCanceledException) { return; }
            }
        }

        internal void Enqueue(NetworkEvent ev, ulong clientId, byte[] data)
            => _queue?.Enqueue(new RelayEvent(ev, clientId, data));

        internal readonly struct RelayEvent
        {
            public readonly NetworkEvent Event;
            public readonly ulong        ClientId;
            public readonly byte[]       Data;
            public RelayEvent(NetworkEvent e, ulong c, byte[] d)
            { Event = e; ClientId = c; Data = d; }
        }
    }

    internal interface ITransportInner
    {
        ulong LastRttMs { get; }
        /// <summary>True when the server explicitly kicked this peer (not a network drop).</summary>
        bool  WasKicked { get; }
        void  SendRaw(byte[] data);
        void  Close();
        Task  RunAsync(CancellationToken ct);
    }
}
