package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	artifactsconf "github.com/Olian04/edgerun/conformance/artifacts"
	catalogconf "github.com/Olian04/edgerun/conformance/catalog"
	executionconf "github.com/Olian04/edgerun/conformance/execution"
	ingressconf "github.com/Olian04/edgerun/conformance/ingress"
	membershipconf "github.com/Olian04/edgerun/conformance/membership"
	plansconf "github.com/Olian04/edgerun/conformance/plans"
	recordsconf "github.com/Olian04/edgerun/conformance/records"
	routingconf "github.com/Olian04/edgerun/conformance/routing"
	"github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
)

func TestMain(m *testing.M) { testpg.Main(m) }

// target launches the in-repo orchestrator, assembled by the same app.Start that
// cmd/orchestrator uses, on an empty database of its own. Every suite gets a
// fresh one: they are written to find an orchestrator that is new and empty.
func target() ingressconf.Target {
	return ingressconf.Target{
		Launch: func(t testing.TB, env ingressconf.Env) string {
			srv, err := app.Start(context.Background(), app.Config{
				Listen: "127.0.0.1:0", DatabaseURL: testpg.DSN(t),
				IngressToken: env.IngressToken, WorkerToken: env.WorkerToken, AdminToken: env.AdminToken, NetworkToken: env.NetworkToken,
				ExecutionToken: env.ExecutionToken, SecretKey: orchtest.Key, HeartbeatInterval: env.HeartbeatInterval,
				Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Stop(context.Background()) })
			return "http://" + srv.Addr()
		},
	}
}

// TestConformance runs the conformance suite of every platform service the
// orchestrator hosts (the orchestrator profile of edgerun-conformance), and of
// the orchestrator as the client of Execution, against the in-repo orchestrator.
func TestConformance(t *testing.T) {
	t.Run("Ingress", func(t *testing.T) { ingressconf.Run(t, target()) })
	t.Run("Membership", func(t *testing.T) { membershipconf.Run(t, target()) })
	t.Run("Artifacts", func(t *testing.T) { artifactsconf.Run(t, target()) })
	t.Run("Catalog", func(t *testing.T) { catalogconf.Run(t, target()) })
	t.Run("Routing", func(t *testing.T) { routingconf.Run(t, target()) })
	t.Run("Plans", func(t *testing.T) { plansconf.Run(t, target()) })
	t.Run("Records", func(t *testing.T) { recordsconf.Run(t, target()) })
	t.Run("ExecutionClient", func(t *testing.T) { executionconf.RunClient(t, target()) })
}
