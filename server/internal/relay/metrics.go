package relay

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics groups all Prometheus counters and gauges for the relay server.
type Metrics struct {
	ActivePeers     prometheus.Gauge
	ActiveSessions  prometheus.Gauge
	SessionsCreated prometheus.Counter

	BytesRelayed    *prometheus.CounterVec
	MessagesRelayed *prometheus.CounterVec

	UDPPacketsDropped   prometheus.Counter
	ReliableRetransmits prometheus.Counter

	APIRequests *prometheus.CounterVec
	RateLimited *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		ActivePeers: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "openrelay", Name: "active_peers",
			Help: "Currently connected peers across all sessions.",
		}),
		ActiveSessions: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "openrelay", Name: "active_sessions",
			Help: "Currently active relay sessions.",
		}),
		SessionsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "sessions_created_total",
			Help: "Total sessions created since startup.",
		}),
		BytesRelayed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "bytes_relayed_total",
			Help: "Payload bytes forwarded, labelled by transport.",
		}, []string{"transport"}),
		MessagesRelayed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "messages_relayed_total",
			Help: "Messages forwarded (broadcast fan-outs counted individually).",
		}, []string{"transport", "type"}),
		UDPPacketsDropped: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "udp_packets_dropped_total",
			Help: "UDP datagrams dropped because the work queue was full.",
		}),
		ReliableRetransmits: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "reliable_retransmits_total",
			Help: "UDP ARQ retransmissions of reliable control messages.",
		}),
		APIRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "api_requests_total",
			Help: "HTTP API requests by endpoint and status code.",
		}, []string{"endpoint", "status"}),
		RateLimited: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "openrelay", Name: "rate_limited_total",
			Help: "Requests rejected by the rate limiter.",
		}, []string{"source"}),
	}
}
