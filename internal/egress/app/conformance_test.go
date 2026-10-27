package app_test

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"testing"

	egressconf "github.com/Olian04/edgerun/conformance/egress"
	"github.com/Olian04/edgerun/internal/egress/app"
)

// TestConformance runs the network service conformance suite against the
// in-repo service, assembled by the same app.Start that cmd/egress uses.
func TestConformance(t *testing.T) {
	egressconf.Run(t, egressconf.Target{
		Launch: func(t testing.TB, env egressconf.Env) string {
			cfg := app.Config{Listen: "127.0.0.1:0", Token: env.Token, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if env.AllowLoopback {
				cfg.AllowNetworks = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}
			}
			srv, err := app.Start(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Stop(context.Background()) })
			return "http://" + srv.Addr()
		},
	})
}
