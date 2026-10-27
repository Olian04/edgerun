package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	orchestratorconf "github.com/Olian04/edgerun/conformance/orchestrator"
	"github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
)

func TestMain(m *testing.M) { testpg.Main(m) }

// TestConformance runs the orchestrator conformance suite against the in-repo
// orchestrator, assembled by the same app.Start that cmd/orchestrator uses.
func TestConformance(t *testing.T) {
	orchestratorconf.Run(t, orchestratorconf.Target{
		Launch: func(t testing.TB, env orchestratorconf.Env) string {
			srv, err := app.Start(context.Background(), app.Config{
				Listen: "127.0.0.1:0", DatabaseURL: testpg.DSN(t),
				IngressToken: env.IngressToken, WorkerToken: env.WorkerToken, AdminToken: env.AdminToken,
				ExecutionToken: env.ExecutionToken, SecretKey: orchtest.Key, HeartbeatInterval: env.HeartbeatInterval,
				Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Stop(context.Background()) })
			return "http://" + srv.Addr()
		},
	})
}
