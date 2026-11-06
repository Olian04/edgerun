package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	edgeconf "github.com/Olian04/edgerun/conformance/edge"
	"github.com/Olian04/edgerun/internal/frontend/app"
)

// TestConformance runs the Edge conformance suite (the frontend profile of
// edgerun-conformance) against the in-repo frontend, assembled by the same
// app.Start that cmd/frontend uses.
func TestConformance(t *testing.T) {
	edgeconf.Run(t, edgeconf.Target{
		Launch: func(t testing.TB, env edgeconf.Env) string {
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
