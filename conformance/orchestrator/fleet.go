package orchestratorconf

import (
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// listWorkers returns what the operator sees.
func (h *harness) listWorkers(t testing.TB) []*controlv1.WorkerStatus {
	t.Helper()
	ctx, cancel := ctxT(settle)
	defer cancel()
	r, err := h.insp.ListWorkers(ctx, connect.NewRequest(&controlv1.ListWorkersRequest{}))
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	return r.Msg.GetWorkers()
}

// workerStatus returns the listed worker with this endpoint, or nil.
func (h *harness) workerStatus(t testing.TB, endpoint string) *controlv1.WorkerStatus {
	t.Helper()
	for _, w := range h.listWorkers(t) {
		if w.GetEndpoint() == endpoint {
			return w
		}
	}
	return nil
}

func (h *harness) aliveCount(t testing.TB) int {
	n := 0
	for _, w := range h.listWorkers(t) {
		if w.GetAlive() {
			n++
		}
	}
	return n
}

func info(abis ...string) *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "conformance-raw-worker", Version: "1"},
		Abis:           abis, MaxConcurrency: 3, Labels: map[string]string{"zone": "z1"},
	}
}

func (h *harness) fleetRegistration(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	fleet := h.fleet(h.env.WorkerToken)
	register := func(endpoint string, in *workerv1.WorkerInfo) (*workerv1.RegisterResponse, error) {
		r, err := fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: endpoint, Info: in}))
		if err != nil {
			return nil, err
		}
		return r.Msg, nil
	}
	heartbeat := func(id string, state workerv1.WorkerState, inFlight uint32) (*workerv1.HeartbeatResponse, error) {
		r, err := fleet.Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{WorkerId: id, State: state, InFlight: inFlight}))
		if err != nil {
			return nil, err
		}
		return r.Msg, nil
	}
	deregister := func(id string) error {
		_, err := fleet.Deregister(ctx, connect.NewRequest(&workerv1.DeregisterRequest{WorkerId: id}))
		return err
	}
	ready := workerv1.WorkerState_WORKER_STATE_READY

	t.Run("invalid registrations", func(t *testing.T) {
		ep := "http://127.0.0.1:1/" + h.run
		zero := info("a")
		zero.MaxConcurrency = 0
		for name, c := range map[string]struct {
			endpoint string
			info     *workerv1.WorkerInfo
		}{
			"no endpoint":    {"", info("a")},
			"not a URL":      {"not a url", info("a")},
			"not http":       {"ftp://example.test/x", info("a")},
			"no host":        {"http:///x", info("a")},
			"no info":        {ep, nil},
			"no concurrency": {ep, zero},
			"no ABI":         {ep, info()},
		} {
			_, err := register(c.endpoint, c.info)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("%s: %v, want invalid_argument", name, err)
			}
		}
		if h.workerStatus(t, ep) != nil {
			t.Error("a refused registration must leave no worker behind")
		}
	})

	t.Run("registering, listing and leaving", func(t *testing.T) {
		ep := "http://127.0.0.1:1/reg-" + h.run
		r, err := register(ep, info("abi-x", "abi-y"))
		if err != nil {
			t.Fatal(err)
		}
		if r.GetWorkerId() == "" {
			t.Fatal("no worker id")
		}
		if got := r.GetHeartbeatInterval().AsDuration(); got != h.env.HeartbeatInterval {
			t.Errorf("heartbeat interval = %v, want the configured %v", got, h.env.HeartbeatInterval)
		}
		w := h.workerStatus(t, ep)
		if w == nil {
			t.Fatal("a registered worker is not listed")
		}
		if w.GetId() != r.GetWorkerId() || !w.GetAlive() || w.GetState() != ready {
			t.Errorf("listed as %+v", w)
		}
		if got := w.GetInfo(); got.GetImplementation().GetName() != "conformance-raw-worker" ||
			len(got.GetAbis()) != 2 || got.GetMaxConcurrency() != 3 || got.GetLabels()["zone"] != "z1" {
			t.Errorf("the worker's info was not kept: %v", got)
		}

		// Heartbeats carry state and load, which the operator sees.
		if hb, err := heartbeat(r.GetWorkerId(), ready, 2); err != nil || hb.GetReregister() {
			t.Fatalf("Heartbeat = %v, %v", hb, err)
		}
		if w := h.workerStatus(t, ep); w.GetInFlight() != 2 || !w.GetAlive() {
			t.Errorf("after a heartbeat with in_flight 2: %+v", w)
		}
		if _, err := heartbeat(r.GetWorkerId(), workerv1.WorkerState_WORKER_STATE_DRAINING, 0); err != nil {
			t.Fatal(err)
		}
		if w := h.workerStatus(t, ep); w.GetState() != workerv1.WorkerState_WORKER_STATE_DRAINING {
			t.Errorf("a draining worker must be listed as draining, not removed: %+v", w)
		}
		_, err = heartbeat(r.GetWorkerId(), workerv1.WorkerState_WORKER_STATE_UNSPECIFIED, 0)
		check.WantCode(t, err, connect.CodeInvalidArgument)

		if err := deregister(r.GetWorkerId()); err != nil {
			t.Fatal(err)
		}
		if h.workerStatus(t, ep) != nil {
			t.Error("a deregistered worker is still listed")
		}
		if hb, err := heartbeat(r.GetWorkerId(), ready, 0); err != nil || !hb.GetReregister() {
			t.Errorf("a heartbeat from a worker that left must ask it to register again: %v, %v", hb, err)
		}
		// Leaving is idempotent.
		for _, id := range []string{r.GetWorkerId(), unknownID, "not-an-id"} {
			if err := deregister(id); err != nil {
				t.Errorf("Deregister(%q) = %v, want success", id, err)
			}
		}
	})

	t.Run("unknown workers are told to register again", func(t *testing.T) {
		for _, id := range []string{unknownID, "not-an-id", ""} {
			hb, err := heartbeat(id, ready, 0)
			if err != nil || !hb.GetReregister() {
				t.Errorf("Heartbeat(%q) = %v, %v, want reregister", id, hb, err)
			}
		}
	})

	t.Run("an endpoint is one worker", func(t *testing.T) {
		ep := "http://127.0.0.1:1/again-" + h.run
		first, err := register(ep, info("abi-x"))
		if err != nil {
			t.Fatal(err)
		}
		second, err := register(ep, info("abi-x"))
		if err != nil {
			t.Fatal(err)
		}
		if first.GetWorkerId() == second.GetWorkerId() {
			t.Error("registering again must issue a new id")
		}
		n := 0
		for _, w := range h.listWorkers(t) {
			if w.GetEndpoint() == ep {
				n++
				if w.GetId() != second.GetWorkerId() {
					t.Error("the older registration must be replaced")
				}
			}
		}
		if n != 1 {
			t.Errorf("%d workers share one endpoint, want 1", n)
		}
		if hb, _ := heartbeat(first.GetWorkerId(), ready, 0); !hb.GetReregister() {
			t.Error("the replaced registration's id must be told to register again")
		}
		deregister(second.GetWorkerId())
	})
}

// fleetLiveness: a worker that stops heartbeating, or says it is draining,
// receives no new work, and one that comes back receives it again.
func (h *harness) fleetLiveness(t *testing.T) {
	abi := h.abi()
	w := h.worker(t, []string{abi}, respond("alive"))
	f := h.deploy(t, abi)
	h.served(t, f.host)
	if st := h.workerStatus(t, w.srv.URL); st == nil || !st.GetAlive() {
		t.Fatalf("a heartbeating worker is not alive: %+v", st)
	}

	t.Run("silence", func(t *testing.T) {
		w.pause(true)
		check.Eventually(t, "a silent worker to be reported not alive", settle, func() bool {
			st := h.workerStatus(t, w.srv.URL)
			return st != nil && !st.GetAlive()
		})
		check.Eventually(t, "a silent worker to stop receiving work", settle, func() bool {
			return h.invoke(t, call{host: f.host}).err != nil
		})
		r := h.invoke(t, call{host: f.host})
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)

		w.pause(false)
		check.Eventually(t, "the worker to be alive again", settle, func() bool {
			st := h.workerStatus(t, w.srv.URL)
			return st != nil && st.GetAlive()
		})
		h.served(t, f.host)
	})

	t.Run("draining", func(t *testing.T) {
		w.setState(workerv1.WorkerState_WORKER_STATE_DRAINING)
		check.Eventually(t, "a draining worker to stop receiving work", settle, func() bool {
			return h.invoke(t, call{host: f.host}).err != nil
		})
		check.WantReason(t, h.invoke(t, call{host: f.host}).err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
		if st := h.workerStatus(t, w.srv.URL); st == nil || st.GetState() != workerv1.WorkerState_WORKER_STATE_DRAINING {
			t.Errorf("a draining worker stays listed, as draining: %+v", st)
		}
		w.setState(workerv1.WorkerState_WORKER_STATE_READY)
		h.served(t, f.host)
	})

	t.Run("leaving", func(t *testing.T) {
		w.stop()
		if h.workerStatus(t, w.srv.URL) != nil {
			t.Error("a deregistered worker is still listed")
		}
		check.Eventually(t, "a departed worker to stop receiving work", settle, func() bool {
			return h.invoke(t, call{host: f.host}).err != nil
		})
	})
}

// noWorkers: with nobody alive to run it, a function answers NO_CAPACITY.
func (h *harness) noWorkers(t *testing.T) {
	check.Eventually(t, "every earlier worker to be gone", settle, func() bool { return h.aliveCount(t) == 0 })
	f := h.deploy(t, h.abi())
	var r result
	check.Eventually(t, "the orchestrator to see the empty fleet", settle, func() bool {
		r = h.invoke(t, call{host: f.host})
		return r.err != nil
	})
	check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
	_ = time.Second
}
