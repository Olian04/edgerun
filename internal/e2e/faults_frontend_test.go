package e2e

// Fault injection between the frontend and the orchestrator (Invoke). The
// frontend is real, the orchestrator is real (on its PostgreSQL), the worker is
// a counting script; the proxy breaks the Invoke connection at every byte.
//
//	- the client never sees a complete success for a cut invocation, and is
//	  never left hanging;
//	- at most once: the frontend does not retry, so the function runs at most
//	  once per client request;
//	- a request cut before the orchestrator had all of it is not run and not
//	  recorded; one the orchestrator had is recorded exactly once;
//	- a black-holed orchestrator ends in a 504 by the frontend's own budget, and the
//	  frontend serves again when it answers, with no restart;
//	- a client (here: the frontend's connection) that goes away cancels the
//	  invocation at the worker.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	frontendapp "github.com/Olian04/edgerun/internal/frontend/app"
)

const ftInvokeMatch = "IngressService/Invoke"

type ftFrontRig struct {
	s      *stack
	w      *stFakeWorker
	px     *ftProxy
	fe     *frontendapp.Server
	url    string
	client *http.Client
	fid    string
}

func ftNewFrontRig(t *testing.T, budget time.Duration) *ftFrontRig {
	t.Helper()
	s := newStack(t)
	r := &ftFrontRig{s: s}
	r.w = stNewFakeWorker(t)
	stJoinFleet(t, "http://"+s.addr, r.w.srv.URL, stWorkerInfo(64, "wagi-1"), nil)
	r.fid = s.createFunction("fronted")
	s.uploadVersion(r.fid, []byte("\x00asm-not-run-by-a-scripted-worker"), nil, true)
	s.route("front.test", "/", r.fid)
	r.px = ftListen(t, s.addr)
	fe, err := frontendapp.Start(context.Background(), frontendapp.Config{
		Listen: "127.0.0.1:0", OrchestratorURL: r.px.URL(), IngressToken: ingressToken,
		RequestTimeout: budget, LimitRefresh: time.Hour, ShutdownTimeout: 2 * time.Second, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fe.Stop(context.Background()) })
	r.fe, r.url = fe, "http://"+fe.Addr()
	r.client = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 20 * time.Second}
	eventually(t, "the stack to serve through the frontend", 30*time.Second, func() bool {
		o := r.get("probe")
		return !o.aborted() && o.status == 200
	})
	return r
}

func (r *ftFrontRig) get(id string) stHTTPObs {
	start := time.Now()
	req, _ := http.NewRequest("GET", r.url+"/f", nil)
	req.Host = "front.test"
	req.Header.Set("X-Request-Id", id)
	req.Header.Set("X-Seq", id)
	resp, err := r.client.Do(req)
	if err != nil {
		return stHTTPObs{transportErr: err, elapsed: time.Since(start)}
	}
	defer resp.Body.Close()
	b := new(strings.Builder)
	buf := make([]byte, 4096)
	var berr error
	for {
		n, e := resp.Body.Read(buf)
		b.Write(buf[:n])
		if e != nil {
			if e.Error() != "EOF" {
				berr = e
			}
			break
		}
	}
	return stHTTPObs{status: resp.StatusCode, header: resp.Header, body: []byte(b.String()), bodyErr: berr, elapsed: time.Since(start)}
}

func TestFaults_FrontendOrchestratorConnectionBreaks(t *testing.T) {
	budget := 3 * time.Second
	r := ftNewFrontRig(t, budget)
	px := r.px

	px.Capture()
	px.Arm(ftFault{Match: ftInvokeMatch})
	r.get("calibrate")
	capC2S, capS2C := px.Captured()
	reqBytes, respBytes := len(capC2S), len(capS2C)
	t.Logf("an Invoke is %d bytes out and %d bytes back", reqBytes, respBytes)

	type fault struct {
		name string
		f    ftFault
		c2s  bool
		n    int
	}
	var faults []fault
	for _, n := range ftSteps(reqBytes, 96, 3) {
		faults = append(faults, fault{fmt.Sprintf("c2s reset@%d", n), ftFault{Match: ftInvokeMatch, C2S: ftDir{ftReset, n}}, int(n) < reqBytes-12, int(n)})
	}
	for _, n := range ftSteps(reqBytes, 0, 19) {
		faults = append(faults, fault{fmt.Sprintf("c2s close@%d", n), ftFault{Match: ftInvokeMatch, C2S: ftDir{ftClose, n}}, int(n) < reqBytes-12, int(n)})
	}
	for _, n := range ftSteps(respBytes, 96, 3) {
		faults = append(faults, fault{fmt.Sprintf("s2c reset@%d", n), ftFault{Match: ftInvokeMatch, S2C: ftDir{ftReset, n}}, false, int(n)})
	}
	for _, n := range ftSteps(respBytes, 0, 13) {
		faults = append(faults, fault{fmt.Sprintf("s2c close@%d", n), ftFault{Match: ftInvokeMatch, S2C: ftDir{ftClose, n}}, false, int(n)})
	}

	base := stSnapshot()
	var f stFindings
	obs := map[string]stHTTPObs{}
	names := map[string]string{}
	ranIDs := 0
	for i, ft := range faults {
		id := fmt.Sprintf("g-%d", i)
		names[id] = ft.name
		claimed := px.Claimed.Load()
		px.Arm(ft.f)
		o := r.get(id)
		px.Disarm()
		obs[id] = o
		if px.Claimed.Load() == claimed {
			f.add("FT-NOT-CLAIMED", ft.name, "the fault was not placed")
			continue
		}
		ran := r.w.count(id)
		ranIDs += ran
		if ran > 1 {
			f.add("D-FE-AT-MOST-ONCE", ft.name, fmt.Sprintf("the function ran %d times for one client request", ran))
		}
		if o.elapsed > budget+1500*time.Millisecond {
			f.add("FT-HANG", ft.name, fmt.Sprintf("the client waited %v", o.elapsed))
		}
		complete := !o.aborted() && o.status >= 200 && o.status < 300
		cut := ft.c2s || (strings.HasPrefix(ft.name, "s2c") && ft.n < respBytes-8)
		if cut && complete {
			f.add("D-FE-SUCCESS-AFTER-CUT", ft.name, fmt.Sprintf("the client got a complete %d (%q) though the connection to the orchestrator was cut at byte %d", o.status, o.body, ft.n))
		}
		if ft.c2s && ran != 0 {
			f.add("FT-ODD", ft.name, "a request whose last byte never arrived was run")
		}
		if !o.aborted() && o.status >= 500 && o.status != 500 && o.status != 502 && o.status != 503 && o.status != 504 {
			f.add("FT-STATUS", ft.name, fmt.Sprintf("status %d", o.status))
		}
	}

	// records: a request the orchestrator had in full is recorded once, one it did not have is not
	var recs []*controlv1.Invocation
	eventually(t, "the runs to be recorded", 30*time.Second, func() bool {
		recs = recs[:0]
		tok := ""
		for {
			rr, err := r.s.ctl.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: r.fid, PageSize: 500, PageToken: tok})
			if err != nil {
				return false
			}
			for _, x := range rr.Invocations {
				if strings.HasPrefix(x.RequestId, "g-") {
					recs = append(recs, x)
				}
			}
			if tok = rr.NextPageToken; tok == "" {
				break
			}
		}
		return len(recs) >= ranIDs
	})
	byID := map[string][]*controlv1.Invocation{}
	for _, x := range recs {
		byID[x.RequestId] = append(byID[x.RequestId], x)
	}
	noReason := 0
	for id := range obs {
		ran := r.w.count(id)
		if len(byID[id]) != ran {
			f.add("D-FE-RECORDS", names[id], fmt.Sprintf("the function ran %d times and %d records were written", ran, len(byID[id])))
			continue
		}
		if ran == 1 && obs[id].aborted() || ran == 1 && obs[id].status >= 500 {
			if byID[id][0].ErrorReason == 0 {
				noReason++
			}
		}
	}
	if noReason > 0 {
		t.Logf("AMBIGUITY %d invocations that ran and were cut off between the orchestrator and the client are recorded with error_reason UNSPECIFIED (there is no reason for 'the client went away')", noReason)
	}

	// recovery and cleanliness
	if o := r.get("after"); o.aborted() || o.status != 200 {
		f.add("FT-RECOVERY", "(end)", fmt.Sprintf("the next request: %d %v %v", o.status, o.transportErr, o.bodyErr))
	}
	if cur, ok := stSettle(base, 12, 8, 10*time.Second); !ok {
		f.add("FT-LEAK-RES", "(end)", fmt.Sprintf("after %d cut connections: %v, before %v", len(faults), cur, base))
	}
	f.report(t, ftFrontKnown)
}

var ftFrontKnown = map[string]string{}

// A black-holed orchestrator ends in the frontend's own 504, never a hang; the frontend
// serves again when it answers.
func TestFaults_FrontendBlackHoledOrchestrator(t *testing.T) {
	budget := 700 * time.Millisecond
	r := ftNewFrontRig(t, budget)
	base := stSnapshot()
	r.px.Freeze(true)
	for i := 0; i < 3; i++ {
		o := r.get(fmt.Sprintf("hole-%d", i))
		if o.aborted() || o.status != 504 {
			t.Errorf("black-holed orchestrator: status %d, %v %v; want 504", o.status, o.transportErr, o.bodyErr)
		}
		if o.elapsed > budget+1500*time.Millisecond {
			t.Errorf("the client waited %v with a %v budget", o.elapsed, budget)
		}
	}
	r.px.Freeze(false)
	eventually(t, "the frontend to serve again", 15*time.Second, func() bool {
		o := r.get(fmt.Sprintf("thaw-%d", time.Now().UnixNano()))
		return !o.aborted() && o.status == 200
	})
	if cur, ok := stSettle(base, 10, 6, 10*time.Second); !ok {
		t.Errorf("after the black hole: %v, before %v", cur, base)
	}
	// a slow orchestrator link: latency is only time
	r.px.SetLatency(50 * time.Millisecond)
	if o := r.get("slow"); o.aborted() || o.status != 200 {
		t.Errorf("with 50 ms on every chunk: %d %v", o.status, o.transportErr)
	}
	r.px.SetLatency(0)
	r.px.SetDrip(5, time.Millisecond)
	if o := r.get("drip"); o.aborted() || o.status != 200 {
		t.Errorf("with the link dripping 5 bytes at a time: %d %v", o.status, o.transportErr)
	}
	r.px.SetDrip(0, 0)
}

// The client goes away (here: the frontend's connection to the orchestrator is reset
// while the worker is running the function): the worker is told, the invocation is
// recorded once.
func ftGoneClient(t *testing.T) *controlv1.Invocation {
	r := ftNewFrontRig(t, 20*time.Second)
	started := make(chan struct{}, 1)
	cancelled := make(chan time.Duration, 1)
	hook := func(req *workerv1.ExecuteRequest, hr *http.Request) {
		if stHeaderOf(req, "x-seq") != "held" {
			return
		}
		started <- struct{}{}
		t0 := time.Now()
		select {
		case <-hr.Context().Done():
			cancelled <- time.Since(t0)
		case <-time.After(10 * time.Second):
			cancelled <- -1
		}
	}
	r.w.hook.Store(&hook)
	done := make(chan stHTTPObs, 1)
	go func() { done <- r.get("held") }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never got the request")
	}
	r.px.KillAll() // the connection the frontend has open to the orchestrator
	select {
	case d := <-cancelled:
		if d < 0 {
			t.Fatal("the worker was not told that nobody is waiting for the invocation (10 s later it still runs)")
		}
		t.Logf("the worker's request was cancelled %v after the connection broke", d)
	case <-time.After(15 * time.Second):
		t.Fatal("no verdict")
	}
	o := <-done
	if !o.aborted() && o.status < 500 {
		t.Errorf("the client of the cut invocation saw %d", o.status)
	}
	recs := ftRecords(t, r.s, r.fid, 1, "probe")
	if n := len(recs["held"]); n != 1 {
		t.Errorf("%d records for the cancelled invocation, want 1", n)
		return nil
	}
	t.Logf("record of the invocation whose client went away: status %d reason %v", recs["held"][0].Status, recs["held"][0].ErrorReason)
	return recs["held"][0]
}

func TestFaults_GoneClientCancelsTheWorker(t *testing.T) { ftGoneClient(t) }

// D-OR-CANCEL-RECORD: an invocation whose client went away is recorded with no
// status and no error_reason, which is what a record that "failed" never looks like
// (protocol.md: "the error_reason if it failed"). ErrorReason has no value for
// "cancelled", and the record of a function that ran and was abandoned cannot be told
// from one that was never answered at all. Every cut Invoke between a frontend and an
// orchestrator leaves such a record.
func TestFaults_CancelledInvocationRecord(t *testing.T) {
	rec := ftGoneClient(t)
	if rec != nil && rec.ErrorReason != commonv1.ErrorReason_ERROR_REASON_CANCELLED {
		t.Fatalf("a cancelled invocation is recorded with status %d and reason %v, want CANCELLED", rec.Status, rec.ErrorReason)
	}
}
