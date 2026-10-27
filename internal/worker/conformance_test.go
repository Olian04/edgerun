package worker_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	workerconf "github.com/Olian04/edgerun/conformance/worker"
	"github.com/Olian04/edgerun/internal/worker/app"
)

// TestConformance runs the worker conformance suite against the in-repo worker,
// assembled by the same app.Run that cmd/worker uses.
func TestConformance(t *testing.T) {
	workerconf.Run(t, workerconf.Target{
		Launch: func(t testing.TB, env workerconf.Env) func() {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- app.Run(ctx, app.Config{
					Listen:          "127.0.0.1:0",
					OrchestratorURL: env.OrchestratorURL,
					WorkerToken:     env.WorkerToken,
					ExecutionToken:  env.ExecutionToken,
					MaxConcurrency:  env.MaxConcurrency,
					Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
				})
			}()
			var once sync.Once
			stop := func() {
				once.Do(func() {
					cancel()
					if err := <-done; err != nil {
						t.Errorf("worker exited with error: %v", err)
					}
				})
			}
			t.Cleanup(stop)
			return stop
		},
	})
}
