package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	feapp "github.com/Olian04/edgerun/internal/frontend/app"
)

func moInFlight(t testing.TB, o *moOrch) int {
	r, err := o.control().ListWorkers(context.Background(), &controlv1.ListWorkersRequest{})
	if err != nil {
		return -1
	}
	n := 0
	for _, w := range r.Workers {
		if w.Alive { // a worker that died keeps its last report in its row until it is pruned
			n += int(w.InFlight)
		}
	}
	return n
}

func moStartFrontend(t *testing.T, o *moOrch, timeout time.Duration) string {
	t.Helper()
	fe, err := feapp.Start(context.Background(), feapp.Config{
		Listen: "127.0.0.1:0", OrchestratorURL: o.url(), IngressToken: ingressToken,
		RequestTimeout: timeout, LimitRefresh: time.Second, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fe.Stop(context.Background()) })
	return "http://" + fe.Addr()
}

type moHTTPReply struct {
	status int
	body   string
	err    error
	header http.Header
}

func moHTTP(base, host, path, nonce string, sleepMs int, timeout time.Duration) moHTTPReply {
	cctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, "POST", base+path+"?n="+nonce+"&sleep="+fmt.Sprint(sleepMs), strings.NewReader(nonce))
	req.Host = host
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Request-Id", nonce)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return moHTTPReply{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return moHTTPReply{status: resp.StatusCode, body: string(b), err: err, header: resp.Header}
}

// TestMultiOrchKillOneMidStream: an orchestrator that dies with invocations in
// flight. Those invocations fail (through the frontend: with a 5xx, never with a
// success or an empty 200); the other orchestrators lose nothing, before, during
// or after; the dead one's database connections go away and the worker's slots
// are freed (the guests are stopped); and when it comes back at the same address
// its frontend serves again with no restart of anything else.
func TestMultiOrchKillOneMidStream(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, heartbeat: 200 * time.Millisecond}) // in-flight is read from heartbeats
	// The worker dials C, so killing B takes nothing but B from anyone.
	c.startWorker(t, 32, moOnly(c.C()))
	moWaitAlive(t, c.C(), 1, 60*time.Second)
	ctl := c.A().control()
	fid := moCreateFn(t, ctl, "slow")
	moDeploy(t, ctl, fid, "wagi-1", c.wasm)
	moSetEnv(t, ctl, fid, "FN", "slow")
	moRoute(t, ctl, "slow.test", "/", fid)
	for _, o := range c.orchs {
		o := o
		moEventually(t, "slow to be served by "+o.name, 90*time.Second, func() bool {
			r := moInvoke(o.ingress(), "slow.test", "/", "warm-"+o.name, 0, 30*time.Second)
			return r.err == nil && r.status == 200
		})
	}
	feB := moStartFrontend(t, c.B(), 20*time.Second)
	feA := moStartFrontend(t, c.A(), 20*time.Second)
	moEventually(t, "the frontends", 20*time.Second, func() bool {
		return moHTTP(feB, "slow.test", "/", "fe-b", 0, 20*time.Second).status == 200 && moHTTP(feA, "slow.test", "/", "fe-a", 0, 20*time.Second).status == 200
	})
	// The database connections of each orchestrator are visible by name.
	if n := c.conns("mo-orch-b"); n == 0 {
		t.Fatal("orchestrator B holds no database connection: the leak check below would prove nothing")
	}
	// Run once with load so goroutines of connections exist, then take a baseline.
	time.Sleep(200 * time.Millisecond)
	baseG := runtime.NumGoroutine()

	// Continuous load on A and C, which must not lose a request at any point.
	stop := make(chan struct{})
	var bg sync.WaitGroup
	var steadyOK, steadyBad atomic.Int64
	for _, o := range []*moOrch{c.A(), c.C()} {
		for g := range 2 {
			bg.Add(1)
			go func() {
				defer bg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					nonce := fmt.Sprintf("steady-%s-%d-%d", o.name, g, i)
					r := moInvoke(o.ingress(), "slow.test", "/", nonce, 50, 20*time.Second)
					if r.err != nil || r.status != 200 || r.mismatch != "" || r.f("fn") != "slow" {
						steadyBad.Add(1)
						t.Errorf("%s on a surviving orchestrator: %v status %d %s", nonce, r.err, r.status, r.mismatch)
						return
					}
					steadyOK.Add(1)
				}
			}()
		}
	}

	// In flight on B: 6 through the ingress protocol, 6 through the HTTP frontend;
	// and 4 on A, which must complete.
	const slowMs = 1500
	type result struct {
		who   string
		r     moReply
		h     moHTTPReply
		viaFE bool
	}
	var rmu sync.Mutex
	var results []result
	var wg sync.WaitGroup
	launch := func(who string, f func(i int) result, n int) {
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := f(i)
				r.who = who
				rmu.Lock()
				results = append(results, r)
				rmu.Unlock()
			}()
		}
	}
	launch("B-ingress", func(i int) result {
		return result{r: moInvoke(c.B().ingress(), "slow.test", "/", fmt.Sprint("kb-", i), slowMs, 30*time.Second)}
	}, 6)
	launch("B-frontend", func(i int) result {
		return result{viaFE: true, h: moHTTP(feB, "slow.test", "/", fmt.Sprint("kfb-", i), slowMs, 30*time.Second)}
	}, 6)
	launch("A-ingress", func(i int) result {
		return result{r: moInvoke(c.A().ingress(), "slow.test", "/", fmt.Sprint("ka-", i), slowMs, 30*time.Second)}
	}, 4)
	// 12 on B are in flight in the worker when its slots show them.
	moEventually(t, "12+ invocations in flight at the worker", 20*time.Second, func() bool { return moInFlight(t, c.A()) >= 12 })
	tKill := time.Now()
	c.B().kill()
	t.Logf("B killed with %d invocations in flight at the worker", moInFlight(t, c.A()))
	var timeline []string
	for time.Since(tKill) < 2*time.Second {
		n := moInFlight(t, c.A())
		timeline = append(timeline, fmt.Sprintf("%dms:%d", time.Since(tKill).Milliseconds(), n))
		// B's 12 invocations have 1.5 s left to sleep; A's 4 and the steady load's up to 4 stay.
		// The guests of the dead orchestrator's calls must be stopped when it goes (the
		// connection is gone), not left to run to their end.
		if time.Since(tKill) > 1000*time.Millisecond && n > 8 {
			t.Errorf("%v after B died, %d invocations still occupy the worker: the guests of B's dropped calls are not being stopped", time.Since(tKill).Round(time.Millisecond), n)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("slots in flight at the worker after the kill: %v", timeline)
	wg.Wait()
	t.Logf("all in-flight requests ended %v after the kill", time.Since(tKill).Round(time.Millisecond))

	codes := map[string]int{}
	for _, r := range results {
		switch r.who {
		case "A-ingress":
			if r.r.err != nil || r.r.status != 200 || r.r.mismatch != "" {
				t.Errorf("a request on the surviving orchestrator A failed: %v status %d %s", r.r.err, r.r.status, r.r.mismatch)
			}
		case "B-ingress":
			// The server died: no typed error can arrive. What matters is that it is an
			// error, never a response, and which one.
			if r.r.err == nil {
				// it may have completed before the kill
				if r.r.mismatch != "" {
					t.Errorf("B-ingress answered with the wrong content: %s", r.r.mismatch)
				}
				codes["B-ingress:completed"]++
				continue
			}
			codes["B-ingress:"+moCode(r.r.err)+"/"+reason(r.r.err).String()]++
		case "B-frontend":
			switch {
			case r.h.err != nil:
				codes["B-frontend:transport-error"]++
			case r.h.status == 200:
				if !strings.Contains(r.h.body, "fn=slow") {
					t.Errorf("the frontend of the dead orchestrator answered 200 with a body that is not the function's: %q", r.h.body)
				}
				codes["B-frontend:200 (completed before the kill)"]++
			case r.h.status >= 500 && r.h.status <= 504:
				codes[fmt.Sprintf("B-frontend:%d", r.h.status)]++
			default:
				t.Errorf("the frontend of the dead orchestrator answered %d %q", r.h.status, r.h.body)
			}
		}
	}
	t.Logf("what the invocations in flight on B ended as: %v", codes)

	// Nothing leaks.
	moEventually(t, "B's database connections to be closed", 15*time.Second, func() bool { return c.conns("mo-orch-b") == 0 })
	d := moEventually(t, "the worker's slots to be free (the guests stopped)", 20*time.Second, func() bool { return moInFlight(t, c.A()) <= 4 })
	t.Logf("worker slots held for B's invocations were freed %v after the kill", (time.Since(tKill)).Round(time.Millisecond))
	_ = d

	// B is gone: its address refuses, the others are unaffected.
	if r := moInvoke(c.B().ingress(), "slow.test", "/", "dead", 0, 5*time.Second); r.err == nil {
		t.Error("the killed orchestrator answered")
	} else if connect.CodeOf(r.err) != connect.CodeUnavailable {
		t.Logf("an invocation sent to the dead orchestrator: %v (code %v)", r.err, connect.CodeOf(r.err))
	}
	if h := moHTTP(feB, "slow.test", "/", "fe-dead", 0, 10*time.Second); h.status != 503 {
		t.Errorf("the frontend of a dead orchestrator answers %d (err %v), want 503 (docs/protocol.md: unavailable is 503 with Retry-After)", h.status, h.err)
	} else if h.header.Get("Retry-After") == "" {
		t.Error("the 503 of an unreachable orchestrator lacks Retry-After")
	}

	// B comes back at its address: the frontend that pointed at it serves again.
	if err := c.B().restart(); err != nil {
		t.Fatal(err)
	}
	moEventually(t, "the frontend of the restarted orchestrator to serve", 60*time.Second, func() bool {
		h := moHTTP(feB, "slow.test", "/", "fe-back", 0, 20*time.Second)
		return h.status == 200 && strings.Contains(h.body, "fn=slow")
	})
	close(stop)
	bg.Wait()
	t.Logf("%d requests on A and C during all of it, %d failed", steadyOK.Load(), steadyBad.Load())
	moEventually(t, "goroutines to settle", 15*time.Second, func() bool { return runtime.NumGoroutine() <= baseG+60 })
	t.Logf("goroutines in the test process (all five components live in it): %d before the kill, %d after B died and came back", baseG, runtime.NumGoroutine())
	_ = commonv1.ErrorReason_ERROR_REASON_INTERNAL
}
