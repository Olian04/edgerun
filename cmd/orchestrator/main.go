// Command orchestrator routes requests to functions, schedules them onto
// workers, and owns all persistent state in PostgreSQL.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Olian04/edgerun/internal/orchestrator/app"
)

// version is set at build time: -ldflags "-X main.version=..."
var version = "dev"

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var cfg app.Config
	var key string
	flag.StringVar(&cfg.Listen, "listen", env("EDGERUN_ORCHESTRATOR_LISTEN", ":8080"), "address to serve on")
	flag.StringVar(&cfg.DatabaseURL, "database-url", env("EDGERUN_DATABASE_URL", ""), "PostgreSQL connection string")
	flag.StringVar(&cfg.IngressToken, "ingress-token", env("EDGERUN_INGRESS_TOKEN", ""), "token frontends present")
	flag.StringVar(&cfg.WorkerToken, "worker-token", env("EDGERUN_WORKER_TOKEN", ""), "token workers present")
	flag.StringVar(&cfg.AdminToken, "admin-token", env("EDGERUN_ADMIN_TOKEN", ""), "token the admin UI and CLIs present")
	flag.StringVar(&cfg.ExecutionToken, "execution-token", env("EDGERUN_EXECUTION_TOKEN", ""), "token presented to workers")
	flag.StringVar(&cfg.NetworkToken, "network-token", env("EDGERUN_NETWORK_TOKEN", ""), "token network services present when they register (empty: none may)")
	flag.StringVar(&key, "secret-key", env("EDGERUN_SECRET_KEY", ""), "base64 of 32 random bytes; seals secret environment values")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", 5*time.Second, "heartbeat interval dictated to workers")
	flag.DurationVar(&cfg.InvocationRetention, "invocation-retention", 14*24*time.Hour, "how long invocation records are kept")
	flag.Parse()

	cfg.Version = version
	cfg.Log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if key != "" {
		b, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			fatal(cfg.Log, fmt.Errorf("secret key is not valid base64: %w", err))
		}
		cfg.SecretKey = b
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	srv, err := app.Start(ctx, cfg)
	if err != nil {
		fatal(cfg.Log, err)
	}
	<-ctx.Done()
	cfg.Log.Info("shutting down")
	if err := srv.Stop(context.Background()); err != nil {
		cfg.Log.Error("shutdown", "error", err)
		os.Exit(1)
	}
}

func fatal(log *slog.Logger, err error) {
	log.Error("orchestrator failed", "error", err)
	os.Exit(1)
}
