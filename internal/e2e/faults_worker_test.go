package e2e

// Fault injection around a real worker running real WebAssembly:
//
//   - orchestrator -> worker (Execute): the connection is cut while a guest runs.
//     The worker stops the guest and frees the slot (b), however the connection
//     ends, and keeps all its slots;
//   - worker -> network service (Fetch): the connection is cut at every byte of the
//     exchange. The guest is told the fetch failed (-2), never handed a partial
//     response as a success; a black hole ends by the invocation's budget; the
//     worker's slot is freed; the service is used again when its link is whole.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

// ---- Execute ---------------------------------------------------------------------------

type ftWorkerRig struct {
	s  *stack
	px *ftProxy // orchestrator -> worker
}

func ftNewWorkerRig(t *testing.T, slots uint32) *ftWorkerRig {
	t.Helper()
	s := newStack(t)
	addr := stFreePort(t)
	px := ftListen(t, addr)
	ftStartWorker(t, workerapp.Config{Listen: addr, AdvertiseURL: px.URL(), OrchestratorURL: "http://" + s.addr, MaxConcurrency: slots})
	fid := s.createFunction("worker-faults")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("wf.test", "/", fid)
	s.waitServing("wf.test", "/echo")
	return &ftWorkerRig{s: s, px: px}
}

func (r *ftWorkerRig) call(path, query string, budget time.Duration) result {
	return r.s.call(req{host: "wf.test", path: path, query: query, method: "POST", body: "payload", budget: budget})
}

func TestFaults_WorkerFreesItsSlotWhenTheConnectionBreaks(t *testing.T) {
	r := ftNewWorkerRig(t, 2)
	s := r.s
	base := stSnapshot()

	for _, c := range []struct{ name, path, query string }{
		{"a guest that sleeps", "/sleep", "ms=6000"},
		{"a guest that spins", "/spin", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			done := make(chan result, 1)
			go func() { done <- r.call(c.path, c.query, 30*time.Second) }()
			eventually(t, "the guest to be running", 10*time.Second, func() bool {
				n, _ := s.ftInFlight()
				return n == 1
			})
			start := time.Now()
			r.px.KillAll() // the orchestrator's connection to the worker is reset
			var freed time.Duration
			eventually(t, "the worker to free the slot of the invocation nobody waits for", 5*time.Second, func() bool {
				n, _ := s.ftInFlight()
				if n == 0 {
					freed = time.Since(start)
				}
				return n == 0
			})
			t.Logf("slot freed %v after the connection broke", freed.Round(10*time.Millisecond))
			select {
			case res := <-done:
				if res.err == nil {
					t.Error("the invocation whose worker connection was reset succeeded")
				} else if reason(res.err) != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
					t.Errorf("error %v, want INTERNAL", res.err)
				}
			case <-time.After(5 * time.Second):
				t.Error("the orchestrator did not answer the cut invocation")
			}
		})
	}

	t.Run("no slot was leaked: both slots work at once", func(t *testing.T) {
		out := make(chan result, 2)
		for i := 0; i < 2; i++ {
			go func() { out <- r.call("/sleep", "ms=300", 10*time.Second) }()
		}
		for i := 0; i < 2; i++ {
			if res := <-out; res.err != nil {
				t.Errorf("a concurrent invocation after the cut ones: %v", res.err)
			}
		}
	})

	t.Run("the answer is dropped (the worker runs it, nobody hears): the deadline ends it, the guest is stopped", func(t *testing.T) {
		r.px.Arm(ftFault{Match: ftExecMatch, S2C: ftDir{ftBlackHole, 0}})
		start := time.Now()
		res := r.call("/sleep", "ms=6000", 1200*time.Millisecond)
		r.px.Disarm()
		if reason(res.err) != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED || res.elapsed > 1200*time.Millisecond+1500*time.Millisecond {
			t.Errorf("%v after %v, want DEADLINE_EXCEEDED by the 1.2 s budget", res.err, res.elapsed)
		}
		eventually(t, "the guest to be stopped at the deadline, not run to its end", 3*time.Second, func() bool {
			n, _ := s.ftInFlight()
			return n == 0
		})
		if d := time.Since(start); d > 4500*time.Millisecond {
			t.Errorf("the slot was free after %v", d)
		}
	})

	t.Run("every byte of an Execute", func(t *testing.T) {
		px := r.px
		px.Capture()
		px.Arm(ftFault{Match: ftExecMatch})
		r.call("/echo", "", 10*time.Second)
		c2s, s2c := px.Captured()
		reqBytes, respBytes := len(c2s), len(s2c)
		t.Logf("an Execute with a real guest: %d bytes out, %d back", reqBytes, respBytes)
		cuts := 0
		for _, dir := range []string{"c2s", "s2c"} {
			total := reqBytes
			if dir == "s2c" {
				total = respBytes
			}
			for _, n := range ftSteps(total, 0, 9) {
				ft := ftFault{Match: ftExecMatch}
				if dir == "c2s" {
					ft.C2S = ftDir{ftReset, n}
				} else {
					ft.S2C = ftDir{ftReset, n}
				}
				px.Arm(ft)
				res := r.call("/echo", "", 10*time.Second)
				px.Disarm()
				cuts++
				tail := 12
				if res.err == nil && int(n) < total-tail {
					t.Errorf("%s reset@%d of %d: the invocation succeeded", dir, n, total)
				}
				if res.err != nil && reason(res.err) != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
					t.Errorf("%s reset@%d: %v, want a typed INTERNAL", dir, n, res.err)
				}
			}
		}
		t.Logf("%d cuts", cuts)
		s.ftWaitIdle(t, "after the cuts", 10*time.Second)
		if res := r.call("/echo", "", 10*time.Second); res.err != nil || string(res.body) != "payload" {
			t.Errorf("after the cuts: %v %q", res.err, res.body)
		}
	})

	if cur, ok := stSettle(base, 14, 10, 10*time.Second); !ok {
		t.Errorf("after the faults: %v, before %v", cur, base)
	}
}

// ---- Fetch -------------------------------------------------------------------------------

const ftOriginBody = "ORIGIN-BODY-0123456789-0123456789-0123456789-0123456789-0123456789-0123456789-0123456789-END"

type ftEgressRig struct {
	s      *stack
	px     *ftProxy // worker -> network service
	hits   atomic.Int64
	origin string
}

func ftNewEgressRig(t *testing.T) *ftEgressRig {
	t.Helper()
	r := &ftEgressRig{}
	s := newStack(t)
	r.s = s
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.hits.Add(1)
		w.Header().Set("X-Origin", "yes")
		fmt.Fprint(w, ftOriginBody)
	}))
	t.Cleanup(origin.Close)
	r.origin = strings.TrimPrefix(origin.URL, "http://")

	svcAddr := stFreePort(t)
	r.px = ftListen(t, svcAddr)
	eg, err := egressapp.Start(ctx, egressapp.Config{
		Listen: svcAddr, AdvertiseURL: r.px.URL(), EgressToken: egressToken, OrchestratorURL: "http://" + s.addr, NetworkToken: networkToken,
		Covers: []string{"127.0.0.0/8"}, AllowNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eg.Stop(context.Background()) })
	ftStartWorker(t, workerapp.Config{Listen: stFreePort(t), OrchestratorURL: "http://" + s.addr, EgressToken: egressToken, MaxConcurrency: 4})

	fid := s.createFunction("talker")
	if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: stBuildNet(t), Abi: "wagi-2", Activate: true}); err != nil {
		t.Fatal(err)
	}
	s.route("net.test", "/", fid)
	if _, err := s.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{r.origin}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a fetch through the network service to work", 120*time.Second, func() bool {
		o := r.fetch(10 * time.Second)
		return o.err == nil && o.code >= 0
	})
	return r
}

type ftFetchObs struct {
	err     error
	code    int
	status  string
	body    string
	elapsed time.Duration
}

func (r *ftEgressRig) fetch(budget time.Duration) ftFetchObs {
	res := r.s.call(req{host: "net.test", path: "/fetch", method: "POST", body: "GET http://" + r.origin + "/page\n\n", budget: budget})
	o := ftFetchObs{err: res.err, elapsed: res.elapsed}
	if res.err != nil {
		return o
	}
	text := string(res.body)
	if _, err := fmt.Sscanf(text, "code=%d\n", &o.code); err != nil {
		o.err = fmt.Errorf("the guest said %q", text)
		return o
	}
	rest := text[strings.Index(text, "\n")+1:]
	if parts := strings.SplitN(rest, "\n\n", 2); len(parts) == 2 {
		o.status, o.body = parts[0], parts[1]
	}
	return o
}

func TestFaults_EgressConnectionBreaks(t *testing.T) {
	r := ftNewEgressRig(t)
	s, px := r.s, r.px

	px.Capture()
	px.Arm(ftFault{Match: "EgressService/Fetch"})
	if o := r.fetch(10 * time.Second); o.err != nil || o.code < 0 {
		t.Fatalf("a fetch: %+v", o)
	}
	c2s, s2c := px.Captured()
	reqBytes, respBytes := len(c2s), len(s2c)
	t.Logf("a Fetch is %d bytes out and %d bytes back", reqBytes, respBytes)

	base := stSnapshot()
	var f stFindings
	cuts := 0
	check := func(name string, ft ftFault, whole bool, budget time.Duration, wantDeadline bool) {
		hits := r.hits.Load()
		fired := px.Fired.Load()
		px.Arm(ft)
		o := r.fetch(budget)
		px.Disarm()
		cuts++
		if px.Fired.Load() == fired && !whole {
			f.add("FT-NOT-FIRED", name, "the fault did not go off")
			return
		}
		if d := r.hits.Load() - hits; d > 1 {
			f.add("D-EG-AT-MOST-ONCE", name, fmt.Sprintf("the origin was reached %d times by one fetch", d))
		}
		if o.elapsed > budget+1500*time.Millisecond {
			f.add("FT-HANG", name, fmt.Sprintf("took %v with a %v budget", o.elapsed, budget))
		}
		switch {
		case o.err != nil:
			// the guest was blocked in the fetch when the budget ended
			if !(wantDeadline && reason(o.err) == commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED) {
				f.add("FT-EXEC", name, fmt.Sprintf("the invocation failed: %v", o.err))
			}
		case o.code >= 0:
			if !whole {
				f.add("D-WK-PARTIAL-AS-SUCCESS", name, fmt.Sprintf("the guest was told the fetch succeeded (code %d, body %q) after the connection was cut", o.code, o.body))
			} else if o.body != ftOriginBody || !strings.HasPrefix(o.status, "Status: 200") {
				f.add("FT-CORRUPT", name, fmt.Sprintf("status %q body %q", o.status, o.body))
			}
		case o.code != -2:
			f.add("FT-CODE", name, fmt.Sprintf("the guest got code %d, want -2 (failed)", o.code))
		}
	}
	for _, n := range ftSteps(reqBytes, 0, 11) {
		check(fmt.Sprintf("c2s reset@%d", n), ftFault{Match: "EgressService/Fetch", C2S: ftDir{ftReset, n}}, int(n) >= reqBytes-12, 5*time.Second, false)
	}
	for _, n := range ftSteps(respBytes, 80, 4) {
		check(fmt.Sprintf("s2c reset@%d", n), ftFault{Match: "EgressService/Fetch", S2C: ftDir{ftReset, n}}, int(n) >= respBytes-8, 5*time.Second, false)
	}
	for _, n := range ftSteps(respBytes, 0, 29) {
		check(fmt.Sprintf("s2c close@%d", n), ftFault{Match: "EgressService/Fetch", S2C: ftDir{ftClose, n}}, int(n) >= respBytes-8, 5*time.Second, false)
		check(fmt.Sprintf("s2c half-close@%d", n), ftFault{Match: "EgressService/Fetch", S2C: ftDir{ftHalfClose, n}}, int(n) >= respBytes-8, 5*time.Second, false)
	}
	// black holes: the guest waits in its fetch, and the budget ends the invocation
	for _, n := range []int64{0, 120} {
		check(fmt.Sprintf("s2c black-hole@%d", n), ftFault{Match: "EgressService/Fetch", S2C: ftDir{ftBlackHole, n}}, false, 1500*time.Millisecond, true)
	}
	check("c2s black-hole@50", ftFault{Match: "EgressService/Fetch", C2S: ftDir{ftBlackHole, 50}}, false, 1500*time.Millisecond, true)
	t.Logf("%d faults", cuts)

	// the slots are free, the service is used again, nothing is left open
	s.ftWaitIdle(t, "after the faults", 10*time.Second)
	if o := r.fetch(10 * time.Second); o.err != nil || o.code < 0 || o.body != ftOriginBody {
		f.add("FT-RECOVERY", "(end)", fmt.Sprintf("a fetch after the faults: %+v", o))
	}
	if cur, ok := stSettle(base, 14, 10, 10*time.Second); !ok {
		f.add("FT-LEAK-RES", "(end)", fmt.Sprintf("after %d faults: %v, before %v", cuts, cur, base))
	}

	// latency and a slow drip are only time
	px.SetLatency(30 * time.Millisecond)
	if o := r.fetch(10 * time.Second); o.err != nil || o.code < 0 || o.body != ftOriginBody {
		f.add("FT-SLOW", "latency", fmt.Sprintf("%+v", o))
	}
	px.SetLatency(0)
	px.SetDrip(9, time.Millisecond)
	if o := r.fetch(20 * time.Second); o.err != nil || o.code < 0 || o.body != ftOriginBody {
		f.add("FT-SLOW", "drip", fmt.Sprintf("%+v", o))
	}
	px.SetDrip(0, 0)

	// the service's port closed: the guest is told at once, and it is used again with no restart
	px.Pause()
	if o := r.fetch(5 * time.Second); o.err != nil || o.code != -2 || o.elapsed > 3*time.Second {
		f.add("FT-DOWN", "service refused", fmt.Sprintf("a fetch through a refused service: %+v", o))
	}
	px.Resume()
	eventually(t, "the network service to be used again", 20*time.Second, func() bool {
		o := r.fetch(5 * time.Second)
		return o.err == nil && o.code >= 0
	})
	f.report(t, ftWorkerKnown)
}

var ftWorkerKnown = map[string]string{}
