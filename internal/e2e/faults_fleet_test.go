package e2e

// Fault injection between a real worker and a real orchestrator (Fleet and
// Artifact): the worker dials the orchestrator through the proxy.
//
//	(b) the worker's in-flight count returns to zero and no slot is leaked;
//	(c) a broken artifact stream is a typed failure by the invocation's budget,
//	    never bytes that were not checked, and does not poison the worker;
//	(d) a black hole ends by the deadline;
//	(e) after the faults stop, the worker is back without a restart: it
//	    registers and heartbeats again.

import (
	"context"
	"testing"
	"time"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

// ftStartWorker runs a real worker and stops it when the test ends.
func ftStartWorker(t testing.TB, cfg workerapp.Config) {
	t.Helper()
	cfg.WorkerToken, cfg.ExecutionToken = workerToken, executionToken
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 3 * time.Second
	}
	cfg.Log = quiet
	wctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- workerapp.Run(wctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Error("the worker did not exit")
		}
	})
}

// inFlight is the in-flight count the orchestrator shows for the workers (their last heartbeat's).
func (s *stack) ftInFlight() (n uint32, workers int) {
	r, err := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
	if err != nil {
		return 1 << 30, 0
	}
	for _, w := range r.Workers {
		n += w.InFlight
		workers++
	}
	return n, workers
}

func (s *stack) ftWaitIdle(t testing.TB, what string, within time.Duration) {
	t.Helper()
	eventually(t.(*testing.T), what+": the worker's in-flight count to return to zero", within, func() bool {
		n, w := s.ftInFlight()
		return w > 0 && n == 0
	})
}

type ftFleetRig struct {
	s   *stack
	px  *ftProxy // worker -> orchestrator
	fid string
}

// ftNewFleetRig starts an orchestrator and one real worker whose orchestrator is the proxy.
func ftNewFleetRig(t *testing.T, startWorker bool) *ftFleetRig {
	t.Helper()
	s := newStack(t)
	r := &ftFleetRig{s: s, px: ftListen(t, s.addr)}
	r.fid = s.createFunction("fleet")
	s.uploadVersion(r.fid, stTinyWasm("fleet"), nil, true)
	s.route("fleet.test", "/", r.fid)
	if startWorker {
		r.startWorker(t)
		s.waitServing("fleet.test", "/echo")
	}
	return r
}

func (r *ftFleetRig) startWorker(t *testing.T) {
	ftStartWorker(t, workerapp.Config{Listen: stFreePort(t), OrchestratorURL: r.px.URL(), MaxConcurrency: 2})
}

// The worker comes back by itself after its link to the orchestrator is cut, and
// after the link was a black hole.
func TestFaults_WorkerRejoinsAfterLinkFaults(t *testing.T) {
	r := ftNewFleetRig(t, true)
	s := r.s

	t.Run("connections reset for a while", func(t *testing.T) {
		stop := time.Now().Add(2 * time.Second)
		for time.Now().Before(stop) {
			r.px.KillAll()
			r.px.Refuse(true)
			time.Sleep(50 * time.Millisecond)
		}
		r.px.Refuse(false)
		eventually(t, "the worker to be alive and serving again", 30*time.Second, func() bool {
			return s.aliveWorkers() == 1 && s.call(req{host: "fleet.test", path: "/echo"}).err == nil
		})
	})

	t.Run("the link refuses new connections long enough for the worker to be forgotten", func(t *testing.T) {
		r.px.Refuse(true)
		r.px.KillAll()
		eventually(t, "the orchestrator to stop counting the worker as alive", 15*time.Second, func() bool { return s.aliveWorkers() == 0 })
		r.px.Refuse(false)
		eventually(t, "the worker to be alive and serving again", 30*time.Second, func() bool {
			return s.aliveWorkers() == 1 && s.call(req{host: "fleet.test", path: "/echo"}).err == nil
		})
	})
}

// D-WK-HEARTBEAT-NO-TIMEOUT: the worker's Register, Heartbeat and Deregister calls
// carry no deadline (fleetclient passes the long-lived context). A link that
// goes silent while up (a partition, a NAT mapping that expired, a peer that hung:
// the connection stays open, nothing arrives) leaves the heartbeat call waiting
// for ever on that connection: the worker never heartbeats again, the
// orchestrator drops it after three intervals, and it stays dropped after the
// link is healthy again, until someone restarts the worker. protocol.md: "A worker
// that has not heartbeated for three intervals is not alive": nothing in the
// worker makes sure it keeps heartbeating.
func TestFaults_KnownDefect_HeartbeatOnBlackHoledLink(t *testing.T) {
	stSkipKnown(t, "D-WK-HEARTBEAT-NO-TIMEOUT", "a heartbeat on a silent connection waits for ever: the worker is lost until restarted")
	r := ftNewFleetRig(t, true)
	s := r.s
	r.px.Freeze(true)
	eventually(t, "the orchestrator to drop the silent worker", 15*time.Second, func() bool { return s.aliveWorkers() == 0 })
	r.px.Freeze(false)
	eventually(t, "the worker to heartbeat again with no restart, the link being healthy", 12*time.Second, func() bool {
		return s.aliveWorkers() == 1
	})
	if res := s.call(req{host: "fleet.test", path: "/echo"}); res.err != nil {
		t.Errorf("after the link came back: %v", res.err)
	}
}

// A worker started while the orchestrator is unreachable keeps trying and joins when it can.
func TestFaults_WorkerStartedBeforeItsOrchestrator(t *testing.T) {
	r := ftNewFleetRig(t, false)
	r.px.Refuse(true)
	r.startWorker(t)
	time.Sleep(1500 * time.Millisecond) // the worker is trying: nothing to poll for
	if n := r.s.aliveWorkers(); n != 0 {
		t.Fatalf("%d workers alive with the link refusing", n)
	}
	r.px.Refuse(false)
	eventually(t, "the worker to register", 30*time.Second, func() bool { return r.s.aliveWorkers() == 1 })
	r.s.waitServing("fleet.test", "/echo")
}
