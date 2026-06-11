<div align="center">

# Open Relay by nothing

**Self-hosted Unity Relay. WebSocket + UDP. Zero per-CCU cost.**

[![Go 1.21+](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![UDP](https://img.shields.io/badge/UDP-low%20latency-orange?style=flat-square)](#transports)
[![Prometheus](https://img.shields.io/badge/metrics-Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io)
[![License: PolyForm Small Business](https://img.shields.io/badge/License-PolyForm%20Small%20Business-0ea5e9?style=flat-square)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?style=flat-square&logo=docker&logoColor=white)](docker-compose.yml)
[![Unity 2022.3+](https://img.shields.io/badge/Unity-2022.3+-000?style=flat-square&logo=unity)](https://unity.com)
[![Netcode for GameObjects](https://img.shields.io/badge/Netcode-NGO-black?style=flat-square&logo=unity)](https://docs.unity3d.com/Packages/com.unity.netcode.gameobjects@latest)
[![NativeWebSocket](https://img.shields.io/badge/NativeWebSocket-1.1.5+-blue?style=flat-square)](https://github.com/endel/NativeWebSocket)

Drop-in replacement for [Unity Gaming Services Relay](https://unity.com/products/relay).  
Deploy on your own VPS, pay nothing per concurrent user, own your infrastructure.

</div>

---

## Why OpenRelay?

| | Unity Relay | **OpenRelay** |
|---|---|---|
| Hosting | Unity Cloud | **Your VPS** |
| Cost | Per-CCU | Fixed server cost |
| Transports | Proprietary | **WebSocket + UDP** |
| Broadcast | N sends → O(N) | **1 send → O(1)** |
| Auth | Managed | **HMAC-SHA256, both transports** |
| Auto-reconnect | ✅ | **✅ (exponential backoff)** |
| Metrics | Cloud dashboard | **Prometheus + Grafana** |
| Data ownership | Unity's servers | **Yours** |

---

## Features

- **Dual transport** — WebSocket (TCP, all platforms) and UDP (low latency, games)
- **Auto-select** — client picks UDP when available, falls back to WebSocket automatically
- **Broadcast** — host sends once, server fans out to all peers; O(1) upstream bandwidth
- **Reliable control messages** — Connected/Disconnected delivered via ARQ even over UDP
- **HMAC-SHA256 auth** — tokens issued at session create/join, validated on **both** WebSocket and UDP handshake; disabled by default (leave `OPENRELAY_HMAC_SECRET` empty)
- **Rate limiting** — per-IP on HTTP API (10 req/s), WS connects (5/s) and UDP handshakes (5/s)
- **Auto-reconnect** — Unity client retries with exponential backoff; fetches a fresh token each attempt; hosts never reconnect (their session is destroyed on disconnect)
- **Built-in TLS** — set two env vars and get `wss://` without nginx; works with Let's Encrypt certbot paths out of the box
- **Prometheus + Grafana** — included in docker-compose, pre-configured scraping
- **Session lifecycle** — host-leave destroys session; empty sessions reaped automatically
- **High throughput** — worker pool + `sync.Map` + buffer pool for 5–10k CCU

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          OpenRelay Server                               │
│                                                                         │
│  :7778  HTTP API + /metrics                                             │
│ ┌──────────────────────────┐                                            │
│ │  PUT  /sessions/create   │  ← rate-limited per IP (10 req/s)          │
│ │  POST /sessions/join     │                                            │
│ │  GET  /health            │                                            │
│ │  GET  /metrics           │  ← Prometheus scrape                       │
│ └───────────┬──────────────┘                                            │
│             │  issues HMAC tokens when OPENRELAY_HMAC_SECRET is set     │
│  :7777 WebSocket (TCP / TLS)        :7779 UDP                           │
│ ┌───────────┴──────────────────────────────────────┐                    │
│ │           Manager  (sync.RWMutex)                │                    │
│ │       map[joinCode] → *Session                   │                    │
│ │  per-IP rate limiter (5 conn/s)                  │                    │
│ │  per-IP rate limiter (5 handshake/s)             │                    │
│ └──────────────────────┬───────────────────────────┘                    │
│                        │                                                │
│             ┌──────────▼───────────┐                                    │
│             │       Session        │  WS peers + UDP peers              │
│             │   map[id] → Peer     │  coexist in same session           │
│             └──────────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Connection lifecycle

```
Host                    OpenRelay Server                    Client
  │                             │                              │
  │── PUT /sessions/create ────►│                              │
  │◄── { joinCode, token } ─────│                              │
  │                             │                              │
  │── WS /relay?code=X          │                              │
  │       &token=... ──────────►│  ← token validated here      │
  │◄── ReliableEnv(Connected(0))│                              │
  │                             │                              │
  │                             │◄── POST /sessions/join ──────│
  │                             │─── { joinCode, token } ─────►│
  │                             │◄── UDP "JOINCODE\nTOKEN" ────│
  │                             │    ← token validated here    │
  │                             │─── UDPHandshakeAck(id=1) ───►│
  │                             │─ ReliableEnv(Connected(0)) ─►│
  │◄── ReliableEnv(Connected(1))│                              │
  │                             │                              │
  │── DataBroadcast(payload) ──►│                              │
  │                             │────── Data(from=0) ─────────►│  fanout
```

### Reconnect sequence (client)

```
Client                          Server
  │                               │
  │  [network drop]               │
  │                               │
  │  wait 2 s                     │   attempt 1 — base delay
  │── POST /sessions/join ───────►│   fresh token (old may be expired)
  │◄── { token } ─────────────────│
  │── UDP / WS connect ──────────►│
  │◄── Connected ─────────────────│   ✓ reconnected, counter resets
  │                               │
  │  [another drop after 10 s+]   │   stable session → counter resets
  │   wait 2 s, 4 s, 8 s…         │   exponential back-off, cap 30 s
  │  × 5 attempts → TransportFailure
```

---

### Why UDP instead of raw custom protocol?

| Concern | Solution |
|---|---|
| Control messages lost | ARQ: Connected/Disconnected wrapped in ReliableEnvelope (0xF3), retransmit until Ack |
| Stale peers | Server-side reaper: no Ping in 30 s → peer removed |
| NAT traversal | Relay server is always the intermediary — direct P2P not needed |
| WebGL | UDP unavailable in browsers → auto-fall back to WebSocket |

---

## Security

### HMAC-SHA256 tokens

When `OPENRELAY_HMAC_SECRET` is set, the API returns a short-lived token (5-minute TTL) with every session create/join response. Clients must present this token when connecting:

- **WebSocket** — `?token=<token>` query param, validated before the HTTP upgrade
- **UDP** — wire payload `"JOINCODE\nTOKEN"`, validated before the peer is registered

Both paths reject connections with an invalid or expired token and increment `openrelay_rate_limited_total{source="ws_auth"}` / `{source="udp_auth"}`.

When the secret is empty (default), `ValidateToken` is a no-op — all connections pass.

### Rate limiting

| Path | Limit | Notes |
|---|---|---|
| HTTP API (`/api/v1/*`) | 10 req/s, burst 30 per IP | Returns HTTP 429 |
| WS connect (`:7777`) | 5 conn/s, burst 10 per IP | Returns HTTP 429 |
| UDP handshake (`:7779`) | 5/s, burst 10 per IP | **Silent drop** — no reply prevents UDP amplification |

All rate-limited events are counted in `openrelay_rate_limited_total{source}`.

### What auth protects against

| Threat | Protected? |
|---|---|
| Filling `MaxSessions` via spam `create` | ✅ API rate limit + HMAC |
| Joining a session by guessing join code | ✅ HMAC token required |
| Unauthorized UDP entry bypassing WS auth | ✅ Token validated on UDP handshake |
| Payload eavesdropping | ⚠️ Enable TLS for WS; UDP is unencrypted |
| Host kicking valid clients | ❌ By design — host is trusted |

---

## Quickstart

### 1 — Deploy the server

**Using the pre-built image (recommended):**

```bash
# Create a working directory on your VPS
mkdir openrelay && cd openrelay

# Download docker-compose and config files
curl -O https://raw.githubusercontent.com/nothing-udev/openrelay/main/docker-compose.yml
curl -O https://raw.githubusercontent.com/nothing-udev/openrelay/main/prometheus.yml
```

Create `.env` next to `docker-compose.yml`:
```bash
OPENRELAY_IMAGE=ghcr.io/nothing-udev/openrelay:latest
OPENRELAY_PUBLIC_HOST=YOUR_SERVER_IP:7777
OPENRELAY_UDP_PUBLIC_ADDR=YOUR_SERVER_IP:7779
# OPENRELAY_HMAC_SECRET=change-this-to-a-long-random-string
```

```bash
docker compose pull
docker compose up -d

curl http://YOUR_SERVER_IP:7778/health
# → {"status":"ok","sessions":0,"clients":0,"transports":["websocket","udp"]}
```

**To update to a newer image:**
```bash
docker compose pull && docker compose up -d
```

**Building from source** (if you fork or modify the server):
```bash
git clone https://github.com/nothing-udev/openrelay.git
cd openrelay
```

Edit `docker-compose.yml`:
```yaml
environment:
  - OPENRELAY_PUBLIC_HOST=YOUR_SERVER_IP:7777
  - OPENRELAY_UDP_PUBLIC_ADDR=YOUR_SERVER_IP:7779
  # Optional: enable HMAC auth
  # - OPENRELAY_HMAC_SECRET=change-this-to-a-long-random-string
```

```bash
# leave OPENRELAY_IMAGE unset — docker-compose will build locally
docker compose up -d --build

curl http://YOUR_SERVER_IP:7778/health
# → {"status":"ok","sessions":0,"clients":0,"transports":["websocket","udp"]}
```

**Open these ports on your VPS:**

| Port | Protocol | Purpose |
|------|----------|---------|
| 7777 | TCP | WebSocket relay |
| 7778 | TCP | REST API + Prometheus metrics |
| 7779 | **UDP** | UDP relay |

### 2 — Enable TLS (optional, required for WebGL)

If your game targets WebGL or any browser environment, you need `wss://`. Point the server at your cert files — no nginx required:

```yaml
environment:
  - OPENRELAY_TLS_CERT=/certs/fullchain.pem
  - OPENRELAY_TLS_KEY=/certs/privkey.pem
  - OPENRELAY_PUBLIC_HOST=my-server.com:7777   # no port if using 443
volumes:
  - /etc/letsencrypt/live/my-server.com:/certs:ro
```

The server will automatically serve `wss://` and report the correct scheme to clients. Renew certs with certbot as usual; restart the container to pick up the new cert.

> [!NOTE]
> UDP traffic is not encrypted regardless of TLS setting. For most game data (positions, inputs) this is acceptable. If you need encryption for sensitive payloads, use WebSocket-only mode with TLS.

### 3 — Install the Unity package

`Packages/manifest.json`:
```json
{
  "dependencies": {
    "com.openrelay.unity": "https://github.com/nothing-udev/openrelay.git?path=unity-package",
    "com.endel.nativewebsocket": "https://github.com/endel/NativeWebSocket.git#upm-2"
  }
}
```

OpenRelay automatically installs its required dependencies via UPM:

- **Unity Netcode for GameObjects**
- **NativeWebSocket**

No manual installation is required.

> [!IMPORTANT]
> After installing OpenRelay, it is recommended to update **Netcode for GameObjects** to the latest version available through **Window → Package Manager**.

### NativeWebSocket compatibility

| Unity version | Recommended NativeWebSocket version |
|---|---|
| Unity 2022.3 LTS | `1.1.5` |
| Unity 6+ | `1.1.6+` |

OpenRelay's `package.json` currently references:

```json
{
  "dependencies": {
    "com.unity.netcode.gameobjects": "2.6.0"
  }
}
```

### 4 — Configure NetworkManager

1. Add **`OpenRelayTransport`** component to your NetworkManager GameObject
2. Assign it as **Network Transport**
3. Set **`Api Base Url`** → `http://YOUR_SERVER_IP:7778`
4. Set **`Transport Mode`**:
   - `PreferUDP` *(default)* — UDP when available, WS fallback
   - `WebSocketOnly` — **required for WebGL**
   - `UDPOnly` — UDP only, error if unavailable

### 5 — Host / Client code

**Host:**
```csharp
var relay = NetworkManager.Singleton.NetworkConfig.NetworkTransport
    as OpenRelayTransport;

string code = await relay.StartServerWithSession();
lobbyUI.ShowCode(code);
NetworkManager.Singleton.StartHost();
```

**Client:**
```csharp
var relay = NetworkManager.Singleton.NetworkConfig.NetworkTransport
    as OpenRelayTransport;

await relay.StartClientWithCode(codeFromLobby);
NetworkManager.Singleton.StartClient();
```

**Efficient broadcast (O(1) upstream bandwidth):**
```csharp
relay.SendBroadcast(new ArraySegment<byte>(stateBytes));
```

---

## Protocol

### Wire format

Every message — WebSocket frames and UDP datagrams — uses the same 9-byte header:

```
Byte  0        1       2       3       4       5       6       7       8      9…
     ┌────────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬──────┬────┐
     │  Type  │              AuthorClientId  (uint64, big-endian)            │Data│
     └────────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴──────┴────┘
```

### Message types

| Code | Name | Direction | Notes |
|------|------|-----------|-------|
| `0x00` | `Data` | Both | client→server: target ID; server→client: sender ID |
| `0x01` | `KickFromRelay` | Host→Server | AuthorClientId = peer to kick |
| `0x02` | `DataBroadcast` | Client→Server | Server fans out as `Data` to all other peers |
| `0x10` | `Connected` | Server→Client | Host gets new client's ID; client gets host ID (0) |
| `0x12` | `Disconnected` | Server→Host | AuthorClientId = departed peer's ID |
| `0xF0` | `UDPHandshake` | Client→Server | Data = `"JOINCODE\ntoken"` (token empty when auth off) |
| `0xF1` | `UDPHandshakeAck` | Server→Client | AuthorClientId = assigned peer ID |
| `0xFC` | `UDPHandshakeError` | Server→Client | AuthorClientId = error code (see below) |
| `0xFD` | `UDPPing` | Client→Server | Keepalive every 5 s |
| `0xFE` | `UDPPong` | Server→Client | Reply to Ping |
| `0xFF` | `UDPDisconnect` | Client→Server | Graceful disconnect |
| `0xF3` | `ReliableEnvelope` | Server→Client | ARQ wrapper; AuthorClientId = seq; Data = inner |
| `0xFB` | `Ack` | Client→Server | AuthorClientId = seq being acknowledged |

### UDP handshake wire format

```
Data field = UTF-8("JOINCODE\ntoken")
```

The server splits on `\n`. When auth is disabled (`OPENRELAY_HMAC_SECRET` is empty), the token portion is ignored, so legacy clients that send only `"JOINCODE"` continue to work.

### UDPHandshakeError codes

| Code | Name | Meaning |
|------|------|---------|
| `1` | `SessionNotFound` | No session with that join code |
| `2` | `SessionFull` | Session has reached `MaxPeers` |
| `3` | `InvalidToken` | HMAC token missing, invalid, or expired |

### REST API

```
PUT  /api/v1/sessions/create
     → 201 { "joinCode":"A3F9KZ", "wsEndpoint":"ws://…/relay",
             "udpEndpoint":"…:7779", "token":"…" }
             (token only present when OPENRELAY_HMAC_SECRET is set)

POST /api/v1/sessions/join   { "joinCode":"A3F9KZ" }
     → 200 { "joinCode":"A3F9KZ", "wsEndpoint":"…", "udpEndpoint":"…",
             "peerCount":1, "token":"…" }

GET  /api/v1/sessions
     → 200 [ { "joinCode", "peerCount", "createdAt" } ]

GET  /health
     → 200 { "status":"ok", "sessions":3, "clients":11, "transports":["websocket","udp"] }

GET  /metrics
     → Prometheus text format
```

All `/api/v1/*` endpoints are rate-limited to 10 req/s per IP (burst 30).

---

## Configuration

### Server — environment variables

| Variable | Default | Description |
|---|---|---|
| `OPENRELAY_TRANSPORT` | `both` | `both` · `websocket` · `udp` |
| `OPENRELAY_ADDR` | `:7777` | WebSocket listen address |
| `OPENRELAY_PUBLIC_HOST` | `localhost:7777` | Public WS `host:port` for clients |
| `OPENRELAY_UDP_ADDR` | `:7779` | UDP listen address |
| `OPENRELAY_UDP_PUBLIC_ADDR` | `localhost:7779` | Public UDP `host:port` for clients |
| `OPENRELAY_API_ADDR` | `:7778` | HTTP API listen address |
| `OPENRELAY_MAX_SESSIONS` | `200` | Max concurrent sessions (0 = unlimited) |
| `OPENRELAY_MAX_PEERS` | `16` | Max peers per session (0 = unlimited) |
| `OPENRELAY_CODE_LENGTH` | `6` | Join code character count |
| `OPENRELAY_SESSION_TTL` | `5m` | Empty session lifetime before reap |
| `OPENRELAY_HMAC_SECRET` | *(empty)* | Secret for HMAC-SHA256 tokens. Empty = auth disabled |
| `OPENRELAY_TLS_CERT` | *(empty)* | Path to TLS certificate (PEM). Set with `TLS_KEY` to enable TLS |
| `OPENRELAY_TLS_KEY` | *(empty)* | Path to TLS private key (PEM). |

### Unity — Inspector fields

| Field | Default | Description |
|---|---|---|
| `ApiBaseUrl` | `http://localhost:7778` | Base URL of the REST API |
| `TransportMode` | `PreferUDP` | `PreferUDP` · `WebSocketOnly` · `UDPOnly` |
| `ConnectTimeoutSeconds` | `10` | Handshake / connect timeout |
| `WsReceiveBufferSize` | `65536` | WebSocket receive buffer in bytes |
| `AutoReconnect` | `true` | Retry on unexpected disconnects (clients only) |
| `MaxReconnectAttempts` | `5` | Give up after N failed retries → `TransportFailure` |
| `ReconnectDelaySeconds` | `2` | Base delay; doubles each attempt, capped at 30 s |

---

## Metrics

Prometheus metrics at `GET :7778/metrics`:

| Metric | Type | Description |
|---|---|---|
| `openrelay_active_peers` | Gauge | Connected peers across all sessions |
| `openrelay_active_sessions` | Gauge | Active sessions |
| `openrelay_sessions_created_total` | Counter | Total sessions created |
| `openrelay_bytes_relayed_total{transport}` | Counter | Payload bytes forwarded |
| `openrelay_messages_relayed_total{transport,type}` | Counter | Messages forwarded |
| `openrelay_udp_packets_dropped_total` | Counter | UDP datagrams dropped (queue full) |
| `openrelay_reliable_retransmits_total` | Counter | ARQ retransmissions |
| `openrelay_api_requests_total{endpoint,status}` | Counter | HTTP API requests |
| `openrelay_rate_limited_total{source}` | Counter | Rate-limited / rejected events |

`source` label values: `api` · `ws` · `ws_auth` · `udp` · `udp_auth`

Grafana at `:3000` (default password: `changeme`) is pre-configured to scrape OpenRelay.

---

## Platform support

| Platform | WebSocket | UDP |
|---|---|---|
| Windows / macOS / Linux | ✅ | ✅ |
| iOS / Android | ✅ | ✅ |
| WebGL | ✅ | ❌ → `WebSocketOnly` |
| Console (PS5, Xbox, Switch) | ✅ | ✅ (platform-dependent) |

---

## CI/CD

`.github/workflows/docker-publish.yml` runs on every push:

| Event | Result |
|---|---|
| Push to `main` | `ghcr.io/…/openrelay:latest` + `:sha-abc1234` |
| Push tag `v1.2.3` | `:1.2.3` + `:1.2` + `:1` + `:latest` |
| Pull Request | Build only — no push, catches Dockerfile breakage early |

Requires **Settings → Actions → General → Workflow permissions → Read and write** in the repository (one-time setup). No secrets needed — uses the built-in `GITHUB_TOKEN`.

---

## Repository structure

```
openrelay/
├── .github/
│   └── workflows/
│       └── docker-publish.yml        Build + push to ghcr.io on push/tag
├── server/                           Go server
│   ├── cmd/openrelay/main.go         Startup: WS + UDP + API + TLS + graceful shutdown
│   ├── internal/
│   │   ├── protocol/message.go       Wire format (shared with Unity)
│   │   ├── auth/token.go             HMAC-SHA256 token issue + validation
│   │   ├── relay/
│   │   │   ├── peer.go               Peer interface (Send, SendReliable, Close)
│   │   │   ├── ws_peer.go            WebSocket peer (done-channel, write pump)
│   │   │   ├── udp_peer.go           UDP peer (atomic close, ARQ integration)
│   │   │   ├── reliable.go           ARQ layer for UDP control messages
│   │   │   ├── udp_server.go         UDP listener (worker pool, rate limiter, reaper)
│   │   │   ├── session.go            Join/leave/route/broadcast/kick
│   │   │   ├── manager.go            Session registry, WS rate limiter, reaper
│   │   │   └── metrics.go            Prometheus metric definitions
│   │   ├── api/handler.go            REST + rate limiting + /metrics + wss:// scheme
│   │   └── config/config.go          Env-var configuration loader (incl. TLS)
│   ├── Dockerfile
│   └── go.mod
│
├── unity-package/                    Unity Package (install via UPM)
│   ├── Runtime/
│   │   ├── Protocol/RelayMessage.cs  Wire types + UDPHandshake(joinCode, token)
│   │   ├── Api/OpenRelayApiClient.cs HTTP session client
│   │   └── Transport/
│   │       ├── OpenRelayTransport.cs Transport + reconnect loop + CreateInner()
│   │       ├── WSTransportInner.cs   WebSocket: connect, receive, WasKicked=false
│   │       └── UDPTransportInner.cs  UDP: handshake w/ token, ARQ, WasKicked flag
│   └── package.json
│
├── docker-compose.yml               OpenRelay + Prometheus + Grafana
├── nginx/nginx.conf                 Optional nginx config (not required with built-in TLS)
├── prometheus.yml                   Prometheus scrape config
└── README.md
```

---

## Local development

```bash
cd openrelay/server

# Run both transports locally (no auth, no TLS)
OPENRELAY_TRANSPORT=both \
# With auth enabled
OPENRELAY_HMAC_SECRET=dev-secret \
OPENRELAY_PUBLIC_HOST=localhost:7777 \
OPENRELAY_UDP_PUBLIC_ADDR=localhost:7779 \
  go run ./cmd/openrelay


# Verify
curl http://localhost:7778/health
curl -XPUT http://localhost:7778/api/v1/sessions/create
```

---
