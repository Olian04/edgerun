package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	frontendconf "github.com/Olian04/edgerun/conformance/frontend"
	"github.com/Olian04/edgerun/internal/frontend/app"
)

// TestConformance runs the frontend conformance suite against the in-repo
// frontend, assembled by the same app.Start that cmd/frontend uses.
func TestConformance(t *testing.T) {
	frontendconf.Run(t, frontendconf.Target{
		Launch: func(t testing.TB, env frontendconf.Env) string {
			srv, err := app.Start(context.Background(), app.Config{
				Listen: "127.0.0.1:0", OrchestratorURL: env.OrchestratorURL, IngressToken: env.IngressToken,
				RequestTimeout: env.RequestTimeout, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { srv.Stop(context.Background()) })
			return "http://" + srv.Addr()
		},
	})
}
