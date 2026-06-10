package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/openrelay/openrelay/internal/api"
	"github.com/openrelay/openrelay/internal/config"
	"github.com/openrelay/openrelay/internal/relay"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

func main() {
	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		With().Timestamp().Str("svc", "openrelay").Logger()

	cfg := config.Load()
	if !cfg.WSEnabled() && !cfg.UDPEnabled() {
		log.Fatal().Msg("OPENRELAY_TRANSPORT must be 'both', 'websocket', or 'udp'")
	}

	authStatus := "disabled"
	if len(cfg.Relay.HMACSecret) > 0 {
		authStatus = "enabled"
	}
	log.Info().
		Str("transport", cfg.Transport).
		Str("api", cfg.APIAddr).
		Str("hmac_auth", authStatus).
		Msg("OpenRelay starting")

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	metrics := relay.NewMetrics()
	mgr := relay.NewManager(cfg.Relay, metrics, log)
	mgr.Start(rootCtx)

	// ── HTTP API + Prometheus ──────────────────────────────────────────────
	apiRouter := mux.NewRouter()
	apiRouter.Handle("/metrics", promhttp.Handler())
	api.NewHandler(
		mgr, metrics,
		cfg.PublicWSHost, cfg.PublicUDPAddr,
		cfg.WSEnabled(), cfg.UDPEnabled(),
		cfg.Relay.HMACSecret,
		log,
	).RegisterRoutes(apiRouter)

	apiSrv := &http.Server{
		Addr:         cfg.APIAddr,
		Handler:      apiRouter,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Info().Str("addr", cfg.APIAddr).Msg("API + /metrics listening")
		if err := apiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("API server failed")
		}
	}()

	// ── WebSocket relay ────────────────────────────────────────────────────
	var wsSrv *http.Server
	if cfg.WSEnabled() {
		r := mux.NewRouter()
		r.HandleFunc("/relay", mgr.ServeWebSocket)
		wsSrv = &http.Server{Addr: cfg.WSAddr, Handler: r}
		go func() {
			log.Info().Str("addr", cfg.WSAddr).Msg("WebSocket relay listening")
			if err := wsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal().Err(err).Msg("WebSocket server failed")
			}
		}()
	}

	// ── UDP relay ─────────────────────────────────────────────────────────
	var udpSrv *relay.UDPServer
	if cfg.UDPEnabled() {
		var err error
		udpSrv, err = relay.NewUDPServer(cfg.UDPAddr, mgr, metrics, log)
		if err != nil {
			log.Fatal().Err(err).Str("addr", cfg.UDPAddr).Msg("UDP bind failed")
		}
		go func() {
			log.Info().Str("addr", cfg.UDPAddr).Msg("UDP relay listening")
			udpSrv.Serve(rootCtx)
		}()
	}

	// ── Shutdown ─────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Info().Str("signal", sig.String()).Msg("shutting down…")

	rootCancel()
	if udpSrv != nil {
		_ = udpSrv.Close()
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	_ = apiSrv.Shutdown(shutCtx)
	if wsSrv != nil {
		_ = wsSrv.Shutdown(shutCtx)
	}

	log.Info().Msg("stopped")
}
