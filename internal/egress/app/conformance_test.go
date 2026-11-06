package app_test

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"testing"

	egressconf "github.com/Olian04/edgerun/conformance/egress"
	"github.com/Olian04/edgerun/internal/egress/app"
)

// TestConformance runs the network service conformance suite against the
// in-repo service, assembled by the same app.Start that cmd/egress uses.
func TestConformance(t *testing.T) {
	egressconf.Run(t, egressconf.Target{
		Launch: func(t testing.TB, env egressconf.Env) egressconf.Instance {
			cfg := app.Config{
				Listen: "127.0.0.1:0", EgressToken: env.EgressToken,
				OrchestratorURL: env.OrchestratorURL, NetworkToken: env.NetworkToken, Covers: env.Covers,
				Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			for _, c := range env.AllowNetworks {
				p, err := netip.ParsePrefix(c)
				if err != nil {
					t.Fatal(err)
				}
				cfg.AllowNetworks = append(cfg.AllowNetworks, p)
			}
			srv, err := app.Start(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			stop := func() { once.Do(func() { srv.Stop(context.Background()) }) }
			t.Cleanup(stop)
			return egressconf.Instance{URL: "http://" + srv.Addr(), Stop: stop}
		},
	})
}
