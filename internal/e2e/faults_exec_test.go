package e2e

// Fault injection between the orchestrator and its workers (Execute), with the
// TCP fault proxy of faults_proxy_test.go in the path.
//
// Two scripted workers (they count what they are asked to run) sit behind a
// proxy each, so that a connection can be broken at any byte of the request or
// of the response. Invariants asserted:
//
//	(a) AT MOST ONCE: whatever breaks, the function is run by at most one worker,
//	    at most once. (The retry rule: only NO_CAPACITY and a worker that could not
//	    be reached at all - a dial that fails - are retried.)
//	(c) exactly one invocation record per routed invocation, which says what the
//	    client saw (status, a reason when it failed);
//	(d) a black-holed worker does not hold the invocation past its deadline;
//	(e) after the faults stop, everything works again, no restart.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

const ftExecMatch = "ExecutionService/Execute"

type ftExecRig struct {
	s    *stack
	w    [2]*stFakeWorker
	px   [2]*ftProxy
	ing  *stIngress
	fid  string
	pref int // the worker the orchestrator prefers for the function's module
}

func ftNewExecRig(t *testing.T) *ftExecRig {
	t.Helper()
	s := newStack(t)
	r := &ftExecRig{s: s}
	for i := range r.w {
		r.w[i] = stNewFakeWorker(t)
		r.px[i] = ftListen(t, r.w[i].srv.Addr)
		stJoinFleet(t, "http://"+s.addr, r.px[i].URL(), stWorkerInfo(64, "wagi-1"), nil)
	}
	r.fid = s.createFunction("faults")
	s.uploadVersion(r.fid, []byte("\x00asm-not-run-by-a-scripted-worker"), nil, true)
	s.route("faults.test", "/", r.fid)
	r.ing = stNewIngress("http://" + s.addr)
	eventually(t, "both scripted workers to be schedulable", 30*time.Second, func() bool {
		return r.invoke("probe", 5*time.Second).err == nil && s.aliveWorkers() == 2
	})
	r.pref = 0
	if r.w[1].count("probe") > 0 && r.w[0].count("probe") == 0 {
		r.pref = 1
	}
	return r
}

func (r *ftExecRig) invoke(id string, budget time.Duration) stInvokeObs {
	return r.ing.invoke("faults.test", "/f", id, id, budget)
}

func (r *ftExecRig) ran(id string) (pref, other int) {
	return r.w[r.pref].count(id), r.w[1-r.pref].count(id)
}

// ftSteps is every offset below dense, then every step-th, and the last one.
func ftSteps(total, dense, step int) []int64 {
	if stRaceBuild { // coarser under -race
		dense, step = dense/2, step*3
	}
	var out []int64
	for n := 0; n < total; n++ {
		if n < dense || n%step == 0 || n == total-1 {
			out = append(out, int64(n))
		}
	}
	return out
}

// ftRecords lists the records of a function without those whose request id has the prefix skip.
func ftRecords(t *testing.T, s *stack, fid string, want int, skip string) map[string][]*controlv1.Invocation {
	t.Helper()
	by := map[string][]*controlv1.Invocation{}
	eventually(t, "the invocations to be recorded", 30*time.Second, func() bool {
		by = map[string][]*controlv1.Invocation{}
		n, tok := 0, ""
		for {
			r, err := s.ctl.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: fid, PageSize: 500, PageToken: tok})
			if err != nil {
				return false
			}
			for _, rec := range r.Invocations {
				if skip != "" && strings.HasPrefix(rec.RequestId, skip) {
					continue
				}
				by[rec.RequestId] = append(by[rec.RequestId], rec)
				n++
			}
			if tok = r.NextPageToken; tok == "" {
				break
			}
		}
		return n >= want
	})
	return by
}

// ftCheckRecord is invariant (c) for one invocation: one record that says what the client saw.
func ftCheckRecord(recs []*controlv1.Invocation, o stInvokeObs) string {
	if len(recs) != 1 {
		return fmt.Sprintf("%d invocation records, want exactly 1", len(recs))
	}
	rec := recs[0]
	if o.err == nil {
		if rec.ErrorReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED || rec.Status != 200 {
			return fmt.Sprintf("the client saw a success, the record says status %d reason %v", rec.Status, rec.ErrorReason)
		}
		return ""
	}
	if got := edgerun.Reason(o.err); rec.ErrorReason != got {
		return fmt.Sprintf("the client saw %v, the record says reason %v (status %d)", o.err, rec.ErrorReason, rec.Status)
	}
	return ""
}

// AT MOST ONCE, and the rest of invariants, with the connection to the preferred
// worker broken at every byte of the request and of the response.
func TestFaults_ExecuteConnectionBreaks(t *testing.T) {
	r := ftNewExecRig(t)
	px := r.px[r.pref]

	// the size of a request and of a response on the wire
	c0, s0 := px.C2S.Load(), px.S2C.Load()
	r.invoke("calibrate", 5*time.Second)
	reqBytes, respBytes := int(px.C2S.Load()-c0), int(px.S2C.Load()-s0)
	t.Logf("an Execute is %d bytes out and %d bytes back", reqBytes, respBytes)
	if reqBytes == 0 || respBytes == 0 {
		t.Fatalf("the proxy saw nothing of the call (%d, %d)", reqBytes, respBytes)
	}

	type fault struct {
		name string
		f    ftFault
		c2s  bool // the request is cut (the worker cannot have run it)
		n    int
	}
	var faults []fault
	for _, n := range ftSteps(reqBytes, 64, 3) {
		faults = append(faults, fault{fmt.Sprintf("c2s reset@%d", n), ftFault{Match: ftExecMatch, C2S: ftDir{ftReset, n}}, int(n) < reqBytes-5, int(n)})
	}
	for _, n := range ftSteps(reqBytes, 0, 17) {
		faults = append(faults, fault{fmt.Sprintf("c2s close@%d", n), ftFault{Match: ftExecMatch, C2S: ftDir{ftClose, n}}, int(n) < reqBytes-5, int(n)})
		faults = append(faults, fault{fmt.Sprintf("c2s half-close@%d", n), ftFault{Match: ftExecMatch, C2S: ftDir{ftHalfClose, n}}, int(n) < reqBytes-5, int(n)})
	}
	for _, n := range ftSteps(respBytes, 96, 3) {
		faults = append(faults, fault{fmt.Sprintf("s2c reset@%d", n), ftFault{Match: ftExecMatch, S2C: ftDir{ftReset, n}}, false, int(n)})
	}
	for _, n := range ftSteps(respBytes, 0, 13) {
		faults = append(faults, fault{fmt.Sprintf("s2c close@%d", n), ftFault{Match: ftExecMatch, S2C: ftDir{ftClose, n}}, false, int(n)})
		faults = append(faults, fault{fmt.Sprintf("s2c half-close@%d", n), ftFault{Match: ftExecMatch, S2C: ftDir{ftHalfClose, n}}, false, int(n)})
	}

	http0 := stSnapshot()
	var f stFindings
	type result struct {
		name string
		obs  stInvokeObs
	}
	results := map[string]result{}
	var retried, failedUnrun []string
	start := time.Now()
	for i, ft := range faults {
		var id string
		var o stInvokeObs
		placed := false
		// (a worker whose heartbeat was late is not the preferred one for a moment: the request then
		// goes to the other, the fault is not placed, and the attempt is made again)
		for attempt := 0; attempt < 5 && !placed; attempt++ {
			id = fmt.Sprintf("f-%d-%d", i, attempt)
			px.Arm(ft.f)
			claimed := px.Claimed.Load()
			o = r.invoke(id, 4*time.Second)
			px.Disarm()
			results[id] = result{ft.name, o}
			placed = px.Claimed.Load() != claimed
			if !placed {
				if pr, ot := r.ran(id); pr+ot > 1 {
					f.add("D-OR-AT-MOST-ONCE", ft.name, fmt.Sprintf("the function was run %d times", pr+ot))
				}
			}
		}
		if !placed {
			f.add("FT-NOT-CLAIMED", ft.name, "the fault was not placed: the request did not pass through the proxy")
			continue
		}
		pr, ot := r.ran(id)
		switch {
		case pr+ot > 1:
			f.add("D-OR-AT-MOST-ONCE", ft.name, fmt.Sprintf("the function was run %d times (preferred worker %d, other %d)", pr+ot, pr, ot))
		case ft.c2s && pr > 0:
			f.add("FT-ODD", ft.name, "the worker ran a request whose last byte never arrived")
		}
		if o.elapsed > 4*time.Second+1500*time.Millisecond {
			f.add("FT-HANG", ft.name, fmt.Sprintf("took %v", o.elapsed))
		}
		if o.err == nil {
			// a success is legitimate only if the whole answer got through (the last bytes of a
			// response are the chunked encoding's terminator)
			if ft.c2s || (strings.HasPrefix(ft.name, "s2c") && ft.n < respBytes-8) {
				f.add("D-OR-SUCCESS-AFTER-CUT", ft.name, fmt.Sprintf("the client saw a success although the connection was cut at byte %d of %d (ran %d/%d times)", ft.n, respBytes, pr, ot))
			}
			if string(o.body) != string(stChunk(1)) && !(ot == 1 && pr == 0) {
				f.add("FT-CORRUPT", ft.name, fmt.Sprintf("body %q", o.body))
			}
		} else {
			info := stInfoOf(o.err)
			if info == nil || info.GetReason() != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
				f.add("FT-REASON", ft.name, fmt.Sprintf("a broken connection to a worker is %v, want a typed INTERNAL", o.err))
			} else if info.GetInvocationId() == "" {
				f.add("OR-NO-INVOCATION-ID", ft.name, "no invocation id on the error")
			}
		}
		if ot > 0 && o.err == nil {
			retried = append(retried, ft.name)
		}
		if pr+ot == 0 && o.err != nil && ft.c2s {
			failedUnrun = append(failedUnrun, ft.name)
		}
		// the sweep never wastes the other worker's slot: both stay schedulable
	}
	t.Logf("%d faults in %v", len(faults), time.Since(start).Round(time.Millisecond))
	t.Logf("retried on the other worker and succeeded: %d %v", len(retried), retried)
	t.Logf("AMBIGUITY failed although the request cannot have run and the other worker was idle: %d of %d cut requests (the retry rule only retries a dial that fails)", len(failedUnrun), countTrue(faults, func(x fault) bool { return x.c2s }))

	// (c) one record per invocation, saying what the client saw
	by := ftRecords(t, r.s, r.fid, len(results)+2, "")
	for id, res := range results {
		if msg := ftCheckRecord(by[id], res.obs); msg != "" {
			f.add("D-OR-RECORD", res.name, msg)
		}
	}

	// (e) recovery with no restart, (b) nothing left behind
	if o := r.invoke("after", 5*time.Second); o.err != nil {
		f.add("FT-RECOVERY", "(end)", fmt.Sprintf("the next, normal invocation: %v", o.err))
	}
	if n := r.s.aliveWorkers(); n != 2 {
		f.add("FT-RECOVERY", "(end)", fmt.Sprintf("%d workers alive, want 2", n))
	}
	if cur, ok := stSettle(http0, 12, 8, 10*time.Second); !ok {
		f.add("FT-LEAK-RES", "(end)", fmt.Sprintf("after %d broken connections: %v, before %v", len(faults), cur, http0))
	}
	f.report(t, ftExecKnown)
}

func countTrue[T any](xs []T, p func(T) bool) int {
	n := 0
	for _, x := range xs {
		if p(x) {
			n++
		}
	}
	return n
}

var ftExecKnown = map[string]string{}

// A worker that cannot be reached at all (the dial is refused) is the one case the
// retry rule allows: the other worker runs it, exactly once, and the dead one is
// skipped for a while.
func TestFaults_UnreachableWorkerIsRetried(t *testing.T) {
	r := ftNewExecRig(t)
	px := r.px[r.pref]
	px.Pause() // the dial is refused: the request cannot have run
	o := r.invoke("unreachable", 5*time.Second)
	if o.err != nil || string(o.body) != string(stChunk(1)) {
		t.Fatalf("with the preferred worker's port closed the invocation fails: %v", o.err)
	}
	if pr, ot := r.ran("unreachable"); pr != 0 || ot != 1 {
		t.Errorf("ran %d times on the unreachable worker and %d on the other, want 0 and 1", pr, ot)
	}
	// skipped for a few seconds: the next one does not even try it
	for i := 0; i < 3; i++ {
		if o := r.invoke(fmt.Sprintf("skipped-%d", i), 5*time.Second); o.err != nil {
			t.Fatalf("invocation %d while the worker is down: %v", i, o.err)
		}
	}
	px.Resume()
	// back in rotation without a restart: the orchestrator sends to it again once its skip time is over
	eventually(t, "the worker to be used again", 30*time.Second, func() bool {
		id := fmt.Sprintf("back-%d", time.Now().UnixNano())
		o := r.invoke(id, 5*time.Second)
		pr, _ := r.ran(id)
		return o.err == nil && pr == 1
	})
}

// A black-holed worker (it takes the connection and never answers, with heartbeats
// going through) does not hold the invocation past its deadline, and is used
// again as soon as it answers.
func TestFaults_BlackHoledWorker(t *testing.T) {
	r := ftNewExecRig(t)
	px := r.px[r.pref]
	budget := 800 * time.Millisecond
	for _, c := range []struct {
		name string
		f    ftFault
	}{
		{"nothing of the request is forwarded", ftFault{Match: ftExecMatch, C2S: ftDir{ftBlackHole, 0}}},
		{"half of the request is forwarded", ftFault{Match: ftExecMatch, C2S: ftDir{ftBlackHole, 100}}},
		{"the worker runs it and its answer is dropped", ftFault{Match: ftExecMatch, S2C: ftDir{ftBlackHole, 0}}},
		{"the answer stops after its first bytes", ftFault{Match: ftExecMatch, S2C: ftDir{ftBlackHole, 150}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			id := "hole-" + strings.ReplaceAll(c.name, " ", "-")
			px.Arm(c.f)
			o := r.invoke(id, budget)
			px.Disarm()
			if o.elapsed > budget+1500*time.Millisecond {
				t.Errorf("the invocation took %v with a %v budget", o.elapsed, budget)
			}
			if got := edgerun.Reason(o.err); got != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
				t.Errorf("error %v (reason %v), want DEADLINE_EXCEEDED", o.err, got)
			}
			if pr, ot := r.ran(id); pr+ot > 1 {
				t.Errorf("ran %d times", pr+ot)
			}
			// it answers again
			if o := r.invoke(id+"-after", 5*time.Second); o.err != nil {
				t.Errorf("after the black hole: %v", o.err)
			}
		})
	}

	t.Run("a worker whose execution path is down while its heartbeats go on", func(t *testing.T) {
		px.Freeze(true)
		var fails int
		for i := 0; i < 3; i++ {
			o := r.invoke(fmt.Sprintf("frozen-%d", i), budget)
			if o.err != nil {
				fails++
			}
			if o.elapsed > budget+1500*time.Millisecond {
				t.Errorf("invocation %d took %v", i, o.elapsed)
			}
		}
		// SPEC AMBIGUITY (logged): a worker that takes the connection and never answers keeps
		// getting the function: the orchestrator only skips a worker it could not dial, the
		// heartbeat path is separate, and a deadline is never retried. The healthy worker idles.
		t.Logf("AMBIGUITY with the preferred worker black-holed and the other one healthy, %d of 3 invocations failed", fails)
		px.Freeze(false)
		eventually(t, "the worker to answer again", 15*time.Second, func() bool {
			return r.invoke(fmt.Sprintf("thawed-%d", time.Now().UnixNano()), budget).err == nil
		})
	})
}

// Latency and a slow drip on the execution path change nothing but time.
func TestFaults_SlowExecutionPath(t *testing.T) {
	r := ftNewExecRig(t)
	px := r.px[r.pref]
	px.SetLatency(40 * time.Millisecond)
	if o := r.invoke("latency", 5*time.Second); o.err != nil || string(o.body) != string(stChunk(1)) {
		t.Errorf("with 40 ms on every chunk: %v %q", o.err, o.body)
	}
	px.SetLatency(0)
	px.SetDrip(7, time.Millisecond)
	if o := r.invoke("drip", 10*time.Second); o.err != nil || string(o.body) != string(stChunk(1)) {
		t.Errorf("with the connection dripping 7 bytes at a time: %v %q", o.err, o.body)
	}
	px.SetDrip(0, 0)
	// latency beyond the budget is the deadline
	px.SetLatency(600 * time.Millisecond)
	o := r.invoke("too-slow", 500*time.Millisecond)
	px.SetLatency(0)
	if got := edgerun.Reason(o.err); got != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
		t.Errorf("a path slower than the budget: %v, want DEADLINE_EXCEEDED", o.err)
	}
	if pr, ot := r.ran("too-slow"); pr+ot > 1 {
		t.Errorf("ran %d times", pr+ot)
	}
}
