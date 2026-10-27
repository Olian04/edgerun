// Command egress is the network service of an edgerun deployment: the one
// place guests' outbound HTTP requests are allowed or refused, and made.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Olian04/edgerun/internal/egress/app"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// networks is a repeatable flag of CIDR ranges.
type networks []netip.Prefix

func (n *networks) String() string {
	var s []string
	for _, p := range *n {
		s = append(s, p.String())
	}
	return strings.Join(s, ",")
}

func (n *networks) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		p, err := netip.ParsePrefix(strings.TrimSpace(part))
		if err != nil {
			return fmt.Errorf("%q is not a CIDR range: %w", part, err)
		}
		*n = append(*n, p)
	}
	return nil
}

func main() {
	var cfg app.Config
	var allow networks
	flag.StringVar(&cfg.Listen, "listen", env("EDGERUN_EGRESS_LISTEN", ":8090"), "address to serve on")
	flag.StringVar(&cfg.Token, "token", env("EDGERUN_EGRESS_TOKEN", ""), "bearer token workers present")
	flag.Var(&allow, "allow-network", "CIDR range to connect to although it is refused by default, e.g. 10.20.0.0/16 (repeatable)")
	flag.Uint64Var(&cfg.MaxRequestBody, "max-request-bytes", 0, "largest request body accepted (default 1 MiB)")
	flag.Uint64Var(&cfg.MaxResponseBody, "max-response-bytes", 0, "largest response body returned (default 4 MiB)")
	if v := os.Getenv("EDGERUN_EGRESS_ALLOW_NETWORKS"); v != "" {
		if err := allow.Set(v); err != nil {
			fmt.Fprintln(os.Stderr, "EDGERUN_EGRESS_ALLOW_NETWORKS:", err)
			os.Exit(2)
		}
	}
	flag.Parse()
	cfg.AllowNetworks = allow
	cfg.Log = slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	srv, err := app.Start(ctx, cfg)
	if err != nil {
		cfg.Log.Error("egress failed", "error", err)
		os.Exit(1)
	}
	<-ctx.Done()
	cfg.Log.Info("shutting down")
	if err := srv.Stop(context.Background()); err != nil {
		cfg.Log.Error("shutdown", "error", err)
		os.Exit(1)
	}
}
