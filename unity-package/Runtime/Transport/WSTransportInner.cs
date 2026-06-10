using System;
using System.Threading;
using System.Threading.Tasks;
using NativeWebSocket;
using OpenRelay.Protocol;
using Unity.Netcode;
using UnityEngine;

namespace OpenRelay.Transport
{
    /// <summary>
    /// WebSocket relay transport backed by NativeWebSocket.
    /// Works on all Unity targets: Standalone, Mobile, and WebGL.
    ///
    /// Keepalive is handled server-side: ws_peer.go writePump sends WebSocket
    /// Ping frames every 30 s; NativeWebSocket replies with Pong automatically.
    ///
    /// Thread safety: NativeWebSocket serialises writes internally.
    /// Messages are delivered on the main thread via DispatchMessageQueue(),
    /// which OpenRelayTransport.Update() calls every frame.
    ///
    /// Auth: when the server has HMAC enabled, the token returned by the API
    /// is appended as ?token=... to the WS URL. The server validates it before
    /// upgrading the connection — invalid tokens get a plain HTTP 401.
    /// </summary>
    internal sealed class WSTransportInner : ITransportInner
    {
        private readonly string  _endpoint;
        private readonly string  _joinCode;
        private readonly string  _token;       // HMAC auth token ("" = auth disabled)
        private readonly float   _timeoutSec;
        private readonly Action<NetworkEvent, ulong, byte[]> _enqueue;

        private WebSocket _ws;
        private int       _closeFlag;           // Interlocked: 0=open, 1=closed
        private ulong     _lastRttMs;

        // Signals RunAsync to return once the socket is closed (from any cause).
        private TaskCompletionSource<bool> _closeTcs;

        public ulong LastRttMs => _lastRttMs;

        public WSTransportInner(
            string endpoint, string joinCode, string token,
            float timeoutSec, int bufSize,
            Action<NetworkEvent, ulong, byte[]> enqueue)
        {
            _endpoint   = endpoint;
            _joinCode   = joinCode;
            _token      = token ?? "";
            _timeoutSec = timeoutSec;
            _enqueue    = enqueue;
        }

        // ── ITransportInner ───────────────────────────────────────────────────

        public void SendRaw(byte[] data)
        {
            if (_closeFlag != 0 || _ws?.State != WebSocketState.Open) return;
            _ = _ws.Send(data);
        }

        public void Close()
        {
            if (Interlocked.CompareExchange(ref _closeFlag, 1, 0) != 0) return;
            _ = Task.Run(CloseInternalAsync);
        }

        public async Task RunAsync(CancellationToken ct)
        {
            _closeTcs = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            var url = BuildUrl();
            _ws = new WebSocket(url);

            _ws.OnOpen    += OnOpen;
            _ws.OnError   += OnError;
            _ws.OnClose   += OnClose;
            _ws.OnMessage += OnMessage;

            Debug.Log($"[OpenRelay][WS] Connecting → {url}");

            // Cancel on external CancellationToken.
            using var ctReg = ct.Register(() =>
            {
                if (Interlocked.CompareExchange(ref _closeFlag, 1, 0) == 0)
                    _ = Task.Run(CloseInternalAsync);
            });

            // Connection-timeout watchdog.
            using var connCts  = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSec));
            using var timeoutReg = connCts.Token.Register(() =>
            {
                if (_ws?.State != WebSocketState.Open &&
                    Interlocked.CompareExchange(ref _closeFlag, 1, 0) == 0)
                {
                    Debug.LogError("[OpenRelay][WS] Connection timed out");
                    _closeTcs.TrySetResult(true);
                }
            });

            try
            {
                await _ws.Connect();
            }
            catch (Exception ex)
            {
                Debug.LogError($"[OpenRelay][WS] Connect failed: {ex.Message}");
                _closeTcs.TrySetResult(true);
            }

            // Block until socket closes (server close, error, or Close() call).
            await _closeTcs.Task;
        }

        /// <summary>
        /// Dispatches queued WS messages on the main Unity thread.
        /// Must be called every frame from a MonoBehaviour.Update().
        /// No-op on WebGL (callbacks are already on the main thread via JS).
        /// </summary>
        public void DispatchMessageQueue()
        {
#if !UNITY_WEBGL || UNITY_EDITOR
            _ws?.DispatchMessageQueue();
#endif
        }

        // ── NativeWebSocket callbacks ─────────────────────────────────────────

        private void OnOpen()
        {
            Debug.Log("[OpenRelay][WS] Connected");
        }

        private void OnError(string err)
        {
            Debug.LogError($"[OpenRelay][WS] Error: {err}");
        }

        private void OnClose(WebSocketCloseCode code)
        {
            Debug.Log($"[OpenRelay][WS] Closed ({code})");
            // Notify NGO only when the close was unexpected (not from our Close()).
            if (_closeFlag == 0)
                _enqueue(NetworkEvent.Disconnect, 0, null);
            _closeTcs?.TrySetResult(true);
        }

        private void OnMessage(byte[] data)
        {
            if (data.Length < RelayMessage.HeaderSize) return;
            try   { Dispatch(RelayMessage.Decode(data, 0, data.Length)); }
            catch (Exception ex)
            { Debug.LogWarning($"[OpenRelay][WS] Decode: {ex.Message}"); }
        }

        // ── Helpers ──────────────────────────────────────────────────

        private string BuildUrl()
        {
            var url = $"{_endpoint}?code={Uri.EscapeDataString(_joinCode)}";
            if (!string.IsNullOrEmpty(_token))
                url += $"&token={Uri.EscapeDataString(_token)}";
            return url;
        }

        private async Task CloseInternalAsync()
        {
            try
            {
                if (_ws?.State == WebSocketState.Open)
                    await _ws.Close();
            }
            catch { }
            finally { _closeTcs?.TrySetResult(true); }
        }

        private void Dispatch(RelayMessage msg)
        {
            switch (msg.Type)
            {
                case RelayMessageType.Connected:
                    Debug.Log($"[OpenRelay][WS] Peer {msg.AuthorClientId} connected");
                    _enqueue(NetworkEvent.Connect, msg.AuthorClientId, null);
                    break;

                case RelayMessageType.Disconnected:
                    Debug.Log($"[OpenRelay][WS] Peer {msg.AuthorClientId} disconnected");
                    _enqueue(NetworkEvent.Disconnect, msg.AuthorClientId, null);
                    break;

                case RelayMessageType.Data:
                    if (msg.Data.Count > 0)
                    {
                        var copy = new byte[msg.Data.Count];
                        Buffer.BlockCopy(msg.Data.Array!, msg.Data.Offset, copy, 0, msg.Data.Count);
                        _enqueue(NetworkEvent.Data, msg.AuthorClientId, copy);
                    }
                    break;

                default:
                    Debug.LogWarning($"[OpenRelay][WS] Unknown type 0x{(byte)msg.Type:X2}");
                    break;
            }
        }
    }
}
