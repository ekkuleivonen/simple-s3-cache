package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/gateway"
	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"
	"github.com/ekkuleivonen/simple-s3-cache/internal/peer"
	"github.com/ekkuleivonen/simple-s3-cache/internal/server"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "simple-s3-cache.yaml", "path to YAML gateway configuration file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadGateway(*configPath)
	if err != nil {
		logger.Error("load config", slog.String("error", err.Error()))
		return 1
	}

	router, err := newOwnerRouter(cfg.Peer)
	if err != nil {
		logger.Error("create owner router", slog.String("error", err.Error()))
		return 1
	}
	recorder := metrics.NewRecorder(0)
	handler := gateway.New(gateway.Options{
		Router:         router,
		Logger:         logger,
		Metrics:        recorder,
		ForwardTimeout: cfg.Peer.ForwardTimeout,
	})

	srv := server.New(cfg, logger, handler, recorder.Handler())
	errCh := make(chan error, 1)

	go func() {
		logger.Info("starting gateway", slog.String("listen", cfg.Listen))
		errCh <- srv.ListenAndServe()
	}()

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopCh)

	select {
	case sig := <-stopCh:
		logger.Info("shutting down", slog.String("signal", sig.String()))
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return 0
		}
		logger.Error("server failed", slog.String("error", err.Error()))
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown failed", slog.String("error", err.Error()))
		return 1
	}

	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failed", slog.String("error", err.Error()))
		return 1
	}

	logger.Info("gateway stopped")
	return 0
}

func newOwnerRouter(cfg config.PeerConfig) (*peer.Router, error) {
	peers := make([]peer.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, peer.Peer{ID: p.ID, URL: p.URL})
	}
	return peer.NewOwnerRouter(peers)
}
