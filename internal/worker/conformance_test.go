package worker_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

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
					EgressURL:       env.EgressURL,
					EgressToken:     env.EgressToken,
					MaxConcurrency:  env.MaxConcurrency,
					Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
				})
			}()
			var once sync.Once
			stop := func() {
				once.Do(func() {
					cancel()
					// A worker that cannot be stopped must fail the suite,
					// not hang it (e.g. a guest that ignores its deadline).
					select {
					case err := <-done:
						if err != nil {
							t.Errorf("worker exited with error: %v", err)
						}
					case <-time.After(90 * time.Second):
						t.Errorf("worker did not exit within 90s of being asked to stop")
					}
				})
			}
			t.Cleanup(stop)
			return stop
		},
	})
}
