package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rorical/IPFSniffer/internal/config"
	"github.com/Rorical/IPFSniffer/internal/logging"
	"github.com/Rorical/IPFSniffer/internal/opensearch"
	"github.com/Rorical/IPFSniffer/internal/search"
	"github.com/Rorical/IPFSniffer/internal/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	logger := logging.New(logging.Config{Level: slog.LevelInfo})
	slog.SetDefault(logger)
	ctx = logging.WithLogger(ctx, logger)

	shutdownOTel, err := logging.InitOTel(ctx, logging.OTelConfig{Insecure: true, ServiceName: "ipfsniffer-server"})
	if err != nil {
		slog.Error("otel init", "err", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	osc, err := opensearch.New(opensearch.Config{URL: cfg.OpenSearch.URL, Insecure: true})
	if err != nil {
		slog.Error("opensearch client", "err", err)
		os.Exit(1)
	}

	searchClient := &search.Client{OS: osc, Index: "ipfsniffer-docs"}

	api := &server.API{Search: searchClient}
	mux := api.Handler()

	addr := getenv("IPFSNIFFER_HTTP_ADDR", "127.0.0.1:8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("server listening", "addr", addr, "env", cfg.Service.Env)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	slog.Info("server shutdown")
}

func getenv(key, def string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	if v == "" {
		return def
	}
	return v
}
