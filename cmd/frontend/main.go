// Command frontend is the HTTP edge of an edgerun deployment: it forwards
// requests to an orchestrator and streams the responses back.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Olian04/edgerun/internal/frontend/app"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var cfg app.Config
	flag.StringVar(&cfg.Listen, "listen", env("EDGERUN_FRONTEND_LISTEN", ":8000"), "public HTTP address")
	flag.StringVar(&cfg.HealthListen, "health-listen", env("EDGERUN_FRONTEND_HEALTH_LISTEN", ":8081"), "address for /healthz (empty disables)")
	flag.StringVar(&cfg.OrchestratorURL, "orchestrator-url", env("EDGERUN_ORCHESTRATOR_URL", ""), "orchestrator base URL")
	flag.StringVar(&cfg.IngressToken, "ingress-token", env("EDGERUN_INGRESS_TOKEN", ""), "token presented to the orchestrator")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 30*time.Second, "whole-invocation budget per request")
	flag.BoolVar(&cfg.TrustForwarded, "trust-forwarded", false, "believe client X-Forwarded-* headers (only behind a proxy you control)")
	flag.Parse()
	cfg.Log = slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	srv, err := app.Start(ctx, cfg)
	if err != nil {
		cfg.Log.Error("frontend failed", "error", err)
		os.Exit(1)
	}
	<-ctx.Done()
	cfg.Log.Info("shutting down")
	if err := srv.Stop(context.Background()); err != nil {
		cfg.Log.Error("shutdown", "error", err)
		os.Exit(1)
	}
}
