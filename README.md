<div align="center">

# Open Relay by nothing

**Self-hosted Unity Relay. WebSocket + UDP. Zero per-CCU cost.**

[![Go 1.21+](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![UDP](https://img.shields.io/badge/UDP-low%20latency-orange?style=flat-square)](#transports)
[![Prometheus](https://img.shields.io/badge/metrics-Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io)
[![License: PSBL](https://img.shields.io/badge/License-MIT-22c55e?style=flat-square)](LICENSE)
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
| Metrics | Cloud dashboard | **Prometheus + Grafana** |
| Data ownership | Unity's servers | **Yours** |

---

## Features

- **Dual transport** — WebSocket (TCP, all platforms) and UDP (low latency, games)
- **Auto-select** — client picks UDP when available, falls back to WebSocket automatically
- **Broadcast** — host sends once, server fans out to all peers; O(1) upstream bandwidth
- **Reliable control messages** — Connected/Disconnected delivered via ARQ even over UDP
- **Rate limiting** — per-IP API protection (10 req/s, burst 30)
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
│ │  PUT  /sessions/create   │  ← rate-limited per IP                     │
│ │  POST /sessions/join     │                                            │
│ │  GET  /health            │                                            │
│ │  GET  /metrics           │  ← Prometheus scrape                       │
│ └───────────┬──────────────┘                                            │
│             │                                                           │
│  :7777 WebSocket (TCP)              :7779 UDP                           │
│ ┌───────────┴──────────────────────────────────────┐                    │
│ │              Manager  (sync.RWMutex)             │                    │
│ │         map[joinCode] → *Session                 │                    │
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
  │◄── { joinCode:"A3F9KZ" } ───│                              │
  │                             │                              │
  │── WS /relay?code=A3F9KZ ───►│   (or UDP Handshake)         │
  │◄── ReliableEnv(Connected(0))│                              │
  │──── Ack ───────────────────►│                              │
  │                             │◄── POST /sessions/join ──────│
  │                             │◄── UDP Handshake(A3F9KZ) ────│
  │                             │─── UDPHandshakeAck(id=1) ───►│
  │                             │─ ReliableEnv(Connected(0)) ─►│
  │◄── ReliableEnv(Connected(1))│◄─────────── Ack ─────────────│
  │──── Ack ───────────────────►│                              │
  │                             │                              │
  │── DataBroadcast(payload) ──►│                              │
  │                             │────── Data(from=0) ─────────►│  fanout
  │── Data(to=1, payload) ─────►│                              │
  │                             │────── Data(from=0) ─────────►│
  │◄── Data(from=1) ────────────│◄────── Data(to=0) ────────── │
```

### Why UDP instead of raw custom protocol?

| Concern | Solution |
|---|---|
| Control messages lost | ARQ: Connected/Disconnected wrapped in ReliableEnvelope (0xF3), retransmit until Ack |
| Stale peers | Server-side reaper: no Ping in 30 s → peer removed |
| NAT traversal | Relay server is always the intermediary — direct P2P not needed |
| WebGL | UDP unavailable in browsers → auto-fall back to WebSocket |

---

## Quickstart

### 1 — Deploy the server

```bash
git clone https://github.com/nothing-udev/openrelay.git
cd openrelay
```

Edit `docker-compose.yml`:
```yaml
environment:
  - OPENRELAY_PUBLIC_HOST=YOUR_SERVER_IP:7777
  - OPENRELAY_UDP_PUBLIC_ADDR=YOUR_SERVER_IP:7779
```

```bash
docker compose up -d

curl http://YOUR_SERVER_IP:7778/health
# → {"status":"ok","sessions":0,"clients":0,"transports":["websocket","udp"]}
```

**Open these ports on your VPS:**

| Port | Protocol | Purpose |
|------|----------|---------|
| 7777 | TCP | WebSocket relay |
| 7778 | TCP | REST API + Prometheus metrics |
| 7779 | **UDP** | UDP relay |

### 2 — Install the Unity package

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

> [!NOTE]
> Unity 2022.3 LTS should use **NativeWebSocket 1.1.5**.
>
> Unity 6 users should update the package to **1.1.6 or newer** through the Package Manager or by editing `Packages/manifest.json`.

### 3 — Configure NetworkManager

1. Add **`OpenRelayTransport`** component to your NetworkManager GameObject
2. Assign it as **Network Transport**
3. Set **`Api Base Url`** → `http://YOUR_SERVER_IP:7778`
4. Set **`Transport Mode`**:
   - `PreferUDP` *(default)* — UDP when available, WS fallback
   - `WebSocketOnly` — **required for WebGL**
   - `UDPOnly` — UDP only, error if unavailable

### 4 — Host / Client code

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
| `0xF0` | `UDPHandshake` | Client→Server | Data = UTF-8 join code |
| `0xF1` | `UDPHandshakeAck` | Server→Client | AuthorClientId = assigned peer ID |
| `0xFC` | `UDPHandshakeError` | Server→Client | AuthorClientId = 1 (not found) or 2 (full) |
| `0xFD` | `UDPPing` | Client→Server | Keepalive every 5 s |
| `0xFE` | `UDPPong` | Server→Client | Reply to Ping |
| `0xFF` | `UDPDisconnect` | Client→Server | Graceful disconnect |
| `0xF3` | `ReliableEnvelope` | Server→Client | ARQ wrapper; AuthorClientId = seq; Data = inner |
| `0xFB` | `Ack` | Client→Server | AuthorClientId = seq being acknowledged |

### REST API

```
PUT  /api/v1/sessions/create
     → 201 { "joinCode":"A3F9KZ", "wsEndpoint":"ws://…/relay", "udpEndpoint":"…:7779" }

POST /api/v1/sessions/join   { "joinCode":"A3F9KZ" }
     → 200 { "joinCode":"A3F9KZ", "wsEndpoint":"…", "udpEndpoint":"…", "peerCount":1 }

GET  /api/v1/sessions
     → 200 [ { "joinCode", "peerCount", "createdAt" } ]

GET  /health
     → 200 { "status":"ok", "sessions":3, "clients":11, "transports":["websocket","udp"] }

GET  /metrics
     → Prometheus text format (scrape from Prometheus/Grafana)
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
| `OPENRELAY_SESSION_TTL` | `5m` | Empty session lifetime before automatic reap |

### Unity — Inspector fields

| Field | Default | Description |
|---|---|---|
| `ApiBaseUrl` | `http://localhost:7778` | Base URL of the REST API |
| `TransportMode` | `PreferUDP` | `PreferUDP` · `WebSocketOnly` · `UDPOnly` |
| `ConnectTimeoutSeconds` | `10` | Handshake / connect timeout |
| `WsReceiveBufferSize` | `65536` | WebSocket receive buffer in bytes |

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
| `openrelay_rate_limited_total{source}` | Counter | Rate-limited requests |

Grafana at `:3000` (default password: `changeme`) is pre-configured to scrape OpenRelay.


## Platform support

| Platform | WebSocket | UDP |
|---|---|---|
| Windows / macOS / Linux | ✅ | ✅ |
| iOS / Android | ✅ | ✅ |
| WebGL | ✅ | ❌ → `WebSocketOnly` |
| Console (PS5, Xbox, Switch) | ✅ | ✅ (platform-dependent) |

---

## Repository structure

```
openrelay/
├── server/                           Go server
│   ├── cmd/openrelay/main.go         Startup: WS + UDP + API + graceful shutdown
│   ├── internal/
│   │   ├── protocol/message.go       Wire format shared with Unity clients
│   │   ├── relay/
│   │   │   ├── peer.go               Peer interface (Send, SendReliable, Close)
│   │   │   ├── ws_peer.go            WebSocket peer (done-channel, write pump)
│   │   │   ├── udp_peer.go           UDP peer (atomic close, ARQ integration)
│   │   │   ├── reliable.go           ARQ layer for UDP control messages
│   │   │   ├── udp_server.go         UDP listener (worker pool, sync.Map, reaper)
│   │   │   ├── session.go            Join/leave/route/broadcast/kick
│   │   │   ├── manager.go            Session registry, context-aware reaper
│   │   │   └── metrics.go            Prometheus metric definitions
│   │   ├── api/handler.go            REST + rate limiting + /metrics
│   │   └── config/config.go          Env-var configuration loader
│   ├── Dockerfile
│   └── go.mod
│
├── unity-package/                    Unity Package (install via UPM)
│   ├── Runtime/
│   │   ├── Protocol/RelayMessage.cs  Wire types — exact match with Go server
│   │   ├── Api/OpenRelayApiClient.cs HTTP session client
│   │   └── Transport/
│   │       ├── OpenRelayTransport.cs NetworkTransport + transport selection
│   │       ├── WSTransportInner.cs   WebSocket: connect, receive loop
│   │       └── UDPTransportInner.cs  UDP: handshake, ARQ ack, keepalive
│   └── package.json
│
├── docker-compose.yml               OpenRelay + Prometheus + Grafana
├── prometheus.yml                   Prometheus scrape config
└── README.md
```

---

## Local development

```bash
cd openrelay/server

# Run both transports locally
OPENRELAY_TRANSPORT=both \
OPENRELAY_PUBLIC_HOST=localhost:7777 \
OPENRELAY_UDP_PUBLIC_ADDR=localhost:7779 \
  go run ./cmd/openrelay

# Verify
curl http://localhost:7778/health
curl -XPUT http://localhost:7778/api/v1/sessions/create
```

---
