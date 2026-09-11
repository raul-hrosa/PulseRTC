package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/raulhrosa/pulsertc/internal/api"
	"github.com/raulhrosa/pulsertc/internal/auth"
	"github.com/raulhrosa/pulsertc/internal/signaling"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := loadServerConfig()
	if err != nil {
		logger.Error("invalid server configuration", "err", err)
		os.Exit(1)
	}

	srv, err := signaling.NewServer(logger)
	if err != nil {
		log.Fatalf("failed to build signaling server: %v", err)
	}

	mux := http.NewServeMux()
	// PUBLIC: health must stay reachable for Docker / orchestration.
	mux.HandleFunc("/health", srv.HandleHealth)
	// The WS handler authenticates internally, before the upgrade.
	mux.HandleFunc("/ws", srv.HandleWS)
	// AUTHENTICATED / INTERNAL: diagnostics and profiling can reveal internal
	// state, so they require a bearer token unless PULSERTC_METRICS_PUBLIC=true
	// or auth is disabled.
	mux.HandleFunc("/metrics", srv.Protected(srv.HandleMetrics))
	mux.HandleFunc("/metrics.json", srv.Protected(srv.HandleMetricsJSON))
	mux.HandleFunc("/sfu/stats", srv.Protected(srv.HandleSFUStats))
	mux.HandleFunc("/quality", srv.Protected(srv.HandleQualityStats))
	mux.HandleFunc("/quality/stats", srv.Protected(srv.HandleQualityStats))
	mux.Handle("/debug/pprof/", srv.Protected(http.DefaultServeMux.ServeHTTP))
	// /ready (public readiness) + /internal/* (node-to-node only,
	// cluster-credential authenticated). No-ops when the cluster is disabled.
	srv.Cluster().RegisterRoutes(mux)

	// Public integration API under /v1 (control plane only — never in
	// the media path). A separate layer over the existing core; the WebSocket
	// protocol, the web client and /internal are untouched.
	var apiHooks *api.WebhookDispatcher
	apiCfg := api.FromEnv()
	if apiCfg.Enabled || apiCfg.WebhookURL != "" {
		apiHooks = api.NewWebhookDispatcher(apiCfg.WebhookURL, apiCfg.WebhookSecret, logger)
		apiSvc := api.NewService(apiCfg, signaling.NewAPICore(srv),
			api.NewTokenIssuer(auth.FromEnv(), apiCfg), apiHooks, logger)
		api.RegisterRoutes(mux, apiSvc, apiCfg, logger)
		if apiHooks != nil {
			srv.SetParticipantEventHook(func(evType, roomID, participantID string) {
				apiHooks.Emit(api.Event{Type: evType, RoomID: roomID,
					Data: map[string]any{"identity": participantID}})
			})
		}
		go func() {
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for range t.C {
				apiSvc.Sweep()
			}
		}()
	}

	// DEV ONLY: serve the built browser SDK at /sdk/ so web/example-sdk/ can
	// import it with an absolute path. http.FileServer blocks ".." traversal,
	// so the example page cannot reach ../../sdk/dist itself. Production images
	// have no sdk/dist directory, so this route is an inert no-op there.
	if info, err := os.Stat(cfg.SDKDistDir); err == nil && info.IsDir() {
		mux.Handle("/sdk/", http.StripPrefix("/sdk/", http.FileServer(http.Dir(cfg.SDKDistDir))))
		logger.Info("serving built browser SDK", "path", "/sdk/", "dir", cfg.SDKDistDir)
	}

	mux.Handle("/", http.FileServer(http.Dir(cfg.WebDir)))

	// Slow background sweep of idle per-IP rate-limit buckets.
	go func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for range t.C {
			srv.SweepRateLimiters()
		}
	}()

	// Close logical sessions stuck in RECOVERING past the timeout.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			srv.SweepSessions()
		}
	}()

	httpServer := &http.Server{
		Addr:         cfg.Addr + ":" + strconv.Itoa(cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("PulseRTC signaling server listening on %s:%d", cfg.Addr, cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Mark the node READY (and start its heartbeat when clustered).
	srv.Cluster().Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	// Drain — stop being assigned new rooms, let existing
	// sessions finish, then unregister. Rooms are NOT migrated.
	log.Println("shutting down...")
	srv.Cluster().BeginShutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	if apiHooks != nil {
		apiHooks.Close()
	}
	srv.Cluster().Shutdown()
}
