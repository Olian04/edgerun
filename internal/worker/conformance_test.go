package worker_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	artifactsconf "github.com/Olian04/edgerun/conformance/artifacts"
	egressconf "github.com/Olian04/edgerun/conformance/egress"
	executionconf "github.com/Olian04/edgerun/conformance/execution"
	membershipconf "github.com/Olian04/edgerun/conformance/membership"
	"github.com/Olian04/edgerun/internal/worker/app"
)

// TestConformance runs the conformance suite of the service a worker hosts
// (Execution) and of its duties as a client (Membership, Artifacts, Egress)
// against the in-repo worker, assembled by the same app.Run that cmd/worker uses:
// the worker profile of edgerun-conformance. Each suite launches a worker of its
// own.
func TestConformance(t *testing.T) {
	t.Run("Execution", func(t *testing.T) { executionconf.Run(t, target(2)) })
	t.Run("ArtifactsClient", func(t *testing.T) { artifactsconf.RunClient(t, target(2)) })
	t.Run("EgressClient", func(t *testing.T) { egressconf.RunClient(t, target(2)) })
	t.Run("MembershipClient", func(t *testing.T) { membershipconf.RunWorkerClient(t, target(2)) })
}

// With one slot every invocation is the whole capacity: the edge where
// "all slots busy" is easiest to see too early.
func TestConformanceWithOneSlot(t *testing.T) {
	t.Run("Execution", func(t *testing.T) { executionconf.Run(t, target(1)) })
	t.Run("MembershipClient", func(t *testing.T) { membershipconf.RunWorkerClient(t, target(1)) })
}

// target launches the in-repo worker with that many slots.
func target(slots uint32) executionconf.Target {
	return executionconf.Target{Env: executionconf.Env{MaxConcurrency: slots},
		Launch: func(t testing.TB, env executionconf.Env) func() {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- app.Run(ctx, app.Config{
					Listen:          "127.0.0.1:0",
					OrchestratorURL: env.OrchestratorURL,
					WorkerToken:     env.WorkerToken,
					ExecutionToken:  env.ExecutionToken,
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
	}
}
