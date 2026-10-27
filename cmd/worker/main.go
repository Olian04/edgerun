// Command worker executes WebAssembly functions for an edgerun orchestrator.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Olian04/edgerun/internal/worker/app"
)

// version is set at build time: -ldflags "-X main.version=..."
var version = "dev"

type labels map[string]string

func (l labels) String() string { return fmt.Sprint(map[string]string(l)) }
func (l labels) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q: want key=value", s)
	}
	l[k] = v
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envUint(key string, def uint) uint {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint(n)
		}
	}
	return def
}

func main() {
	lbl := labels{}
	var cfg app.Config
	flag.StringVar(&cfg.Listen, "listen", env("EDGERUN_WORKER_LISTEN", ":9000"), "address to serve ExecutionService on")
	flag.StringVar(&cfg.AdvertiseURL, "advertise-url", env("EDGERUN_WORKER_ADVERTISE_URL", ""), "URL the orchestrator uses to reach this worker")
	flag.StringVar(&cfg.OrchestratorURL, "orchestrator-url", env("EDGERUN_ORCHESTRATOR_URL", ""), "orchestrator base URL")
	flag.StringVar(&cfg.WorkerToken, "worker-token", env("EDGERUN_WORKER_TOKEN", ""), "token presented to the orchestrator")
	flag.StringVar(&cfg.ExecutionToken, "execution-token", env("EDGERUN_EXECUTION_TOKEN", ""), "token required of the orchestrator")
	flag.StringVar(&cfg.EgressURL, "egress-url", env("EDGERUN_EGRESS_URL", ""), "network service for guests' outbound requests (empty: guests have no network)")
	flag.StringVar(&cfg.EgressToken, "egress-token", env("EDGERUN_EGRESS_TOKEN", ""), "token presented to the network service")
	maxConc := flag.Uint("max-concurrency", envUint("EDGERUN_WORKER_MAX_CONCURRENCY", 8), "invocations run at once")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", 30*time.Second, "how long to wait for in-flight work on shutdown")
	flag.StringVar(&cfg.Runtime.CompilationCacheDir, "compilation-cache-dir", env("EDGERUN_WORKER_COMPILATION_CACHE_DIR", ""), "persist compiled code here across restarts")
	flag.Var(lbl, "label", "scheduling label key=value (repeatable)")
	flag.Parse()

	cfg.MaxConcurrency = uint32(*maxConc)
	cfg.Labels = lbl
	cfg.Version = version
	cfg.Log = slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg); err != nil {
		cfg.Log.Error("worker failed", "error", err)
		os.Exit(1)
	}
}
