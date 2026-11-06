package app_test

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"testing"

	egressconf "github.com/Olian04/edgerun/conformance/egress"
	membershipconf "github.com/Olian04/edgerun/conformance/membership"
	"github.com/Olian04/edgerun/internal/egress/app"
)

// target launches in-repo network services, assembled by the same app.Start that
// cmd/egress uses.
func target() egressconf.Target {
	return egressconf.Target{
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
	}
}

// TestConformance runs the conformance suite of the service a network service
// hosts (Egress) and of its duties as a client of Membership against the in-repo
// service: the network-service profile of edgerun-conformance.
func TestConformance(t *testing.T) {
	t.Run("Egress", func(t *testing.T) { egressconf.Run(t, target()) })
	t.Run("MembershipClient", func(t *testing.T) { membershipconf.RunNetworkServiceClient(t, target()) })
}
