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
            _cts   = new CancellationTokenSource();
            _queue = new ConcurrentQueue<RelayEvent>();
        }

        public override bool StartServer() { BeginConnect(); return true; }
        public override bool StartClient() { BeginConnect(); return true; }

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

        private async Task ConnectAsync(CancellationToken ct)
        {
            try
            {
                if (_activeMode == default)
                    ApplySessionInfo(new SessionInfo
                    {
                        joinCode    = JoinCode,
                        wsEndpoint  = WsEndpoint,
                        udpEndpoint = UdpEndpoint,
                        token       = _token,
                    });

                _inner = _activeMode == RelayTransportMode.UDPOnly
                    ? (ITransportInner)new UDPTransportInner(
                        UdpEndpoint, JoinCode, ConnectTimeoutSeconds, Enqueue)
                    : new WSTransportInner(
                        WsEndpoint, JoinCode, _token,
                        ConnectTimeoutSeconds, WsReceiveBufferSize, Enqueue);

                await _inner.RunAsync(ct);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Debug.LogError($"[OpenRelay] Error: {ex.Message}");
                Enqueue(NetworkEvent.TransportFailure, 0, null);
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
        void  SendRaw(byte[] data);
        void  Close();
        Task  RunAsync(CancellationToken ct);
    }
}
