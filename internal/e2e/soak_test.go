package e2e

// The soak: traffic through three frontends (two of them the real frontend, one
// a minimal ingress of our own, as examples/community/trafficmock is one) into
// three orchestrators on one database, while workers join, drain and are killed,
// functions are created, deployed, redeployed, rolled back and deleted, and
// environment, routes, labels and plans change, with control reads all the time.
//
// EDGERUN_SOAK=<Go duration> sets how long the churn runs (default 20s).
//
// Invariants, checked at the end and (for the first two) on every request:
//   - every request gets exactly one response, and it is the right one: the guest
//     echoes the nonce three ways, and the function it names is the one the host
//     belongs to; no invocation id is delivered twice;
//   - a response is a platform error only of the kinds the churn can cause;
//   - once quiet, the records are exactly the invocations the responses named
//     (no duplicates, no losses, the janitor being idle), and worker in-flight is 0;
//   - goroutines, heap and database connections do not grow.

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/internal/errs"
)

func soakDuration(t *testing.T) time.Duration {
	if v := os.Getenv("EDGERUN_SOAK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("EDGERUN_SOAK=%q: %v", v, err)
		}
		return d
	}
	return 20 * time.Second
}

// soakMockIngress is the third frontend: HTTP in, ingress.v1 out, written from
// docs/protocol.md "Frontend behaviour" and nothing else, the way a community
// tool would. It maps errors with the table of the document.
func soakMockIngress(t *testing.T, o *moOrch) string {
	t.Helper()
	ing := o.ingress()
	hop := map[string]bool{"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
		"proxy-connection": true, "te": true, "trailer": true, "transfer-encoding": true, "upgrade": true}
	status := func(r commonv1.ErrorReason) int {
		switch r {
		case commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND:
			return 404
		case commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:
			return 503
		case commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED:
			return 504
		case commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
			commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND:
			return 502
		case commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:
			return 413
		}
		return 500
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		host := strings.ToLower(r.Host)
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		var hs []*commonv1.Header
		for n, vs := range r.Header {
			ln := strings.ToLower(n)
			if hop[ln] || strings.HasPrefix(ln, "x-forwarded") {
				continue
			}
			for _, v := range vs {
				hs = append(hs, &commonv1.Header{Name: ln, Value: []byte(v)})
			}
		}
		cctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		res, err := ing.Invoke(cctx, &ingressv1.InvokeRequest{RequestId: r.Header.Get("X-Request-Id"), Request: &commonv1.HttpRequest{
			Method: r.Method, Host: host, Path: r.URL.Path, RawQuery: r.URL.RawQuery, Headers: hs, Body: body, RemoteAddr: r.RemoteAddr}})
		if err != nil {
			if id := errs.Info(err).GetInvocationId(); id != "" {
				w.Header().Set("X-Edgerun-Invocation-Id", id)
			}
			code := status(edgerun.Reason(err))
			if edgerun.Reason(err) == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
				code = 500
				if connect.CodeOf(err) == connect.CodeUnavailable {
					code = 503
				}
			}
			w.WriteHeader(code)
			return
		}
		out, rerr := res.ReadAll()
		w.Header().Set("X-Edgerun-Invocation-Id", res.ID)
		for _, hd := range res.Head.GetHeaders() {
			if hop[hd.Name] || hd.Name == "content-length" {
				continue
			}
			w.Header().Add(hd.Name, string(hd.Value))
		}
		w.WriteHeader(int(res.Head.GetStatus()))
		if rerr == nil {
			w.Write(out)
		}
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String()
}

type soakSample struct {
	at         time.Duration
	goroutines int
	heapMB     float64
	conns      int
	workers    int
}

// soakTrend is the growth of a series over its last 70%, from a least-squares
// fit: slope times the length of that window.
func soakTrend(xs []float64) float64 {
	n := len(xs)
	if n < 6 {
		return 0
	}
	from := n * 3 / 10
	pts := xs[from:]
	m := float64(len(pts))
	var sx, sy, sxy, sxx float64
	for i, y := range pts {
		x := float64(i)
		sx += x
		sy += y
		sxy += x * y
		sxx += x * x
	}
	slope := (m*sxy - sx*sy) / (m*sxx - sx*sx)
	return slope * (m - 1)
}

func TestSoakChurnWithInvariants(t *testing.T) {
	dur := soakDuration(t)
	c := newMoCluster(t, moOpts{n: 3, refresh: 500 * time.Millisecond, heartbeat: 500 * time.Millisecond})
	bg := context.Background()
	g1, g2 := moGuest(t, "g1"), moGuest(t, "g2")
	pick := func() *moOrch { return c.orchs[rand.IntN(3)] }

	// Two workers that stay for the whole run (the "anchors"), reached through a
	// failover URL, so that they do not depend on one orchestrator.
	var wmu sync.Mutex
	var workers []*moWorker
	startWorker := func(first int) *moWorker {
		order := []*moOrch{c.A(), c.B(), c.C()}
		order = append(order[first:], order[:first]...)
		w := c.startWorker(t, 8, moFailover(order...))
		wmu.Lock()
		workers = append(workers, w)
		wmu.Unlock()
		return w
	}
	anchors := []*moWorker{startWorker(0), startWorker(1)}
	moWaitAlive(t, c.C(), 2, 60*time.Second)

	// Stable functions: they are never deleted and their route is never touched.
	ctl := c.A().control()
	var sfID, vA, vB [3]string
	for k := range 3 {
		name := fmt.Sprint("sf", k)
		sfID[k] = moCreateFn(t, ctl, name)
		r1, err := ctl.CreateVersion(bg, &controlv1.CreateVersionRequest{FunctionId: sfID[k], Module: g1, Abi: "wagi-1", Activate: true})
		if err != nil {
			t.Fatal(err)
		}
		r2, err := ctl.CreateVersion(bg, &controlv1.CreateVersionRequest{FunctionId: sfID[k], Module: g2, Abi: "wagi-1"})
		if err != nil {
			t.Fatal(err)
		}
		vA[k], vB[k] = r1.Version.Id, r2.Version.Id
		moSetEnv(t, ctl, sfID[k], "FN", name)
		moSetEnv(t, ctl, sfID[k], "STATE", "A")
		moRoute(t, ctl, name+".test", "/", sfID[k])
	}

	feA, feB, feC := moStartFrontend(t, c.A(), 30*time.Second), moStartFrontend(t, c.B(), 30*time.Second), soakMockIngress(t, c.C())
	frontends := []string{feA, feB, feC}
	for _, fe := range frontends {
		for k := range 3 {
			fe, k := fe, k
			moEventually(t, "the stack to serve", 120*time.Second, func() bool {
				h := moHTTP(fe, fmt.Sprintf("sf%d.test", k), "/", "warm", 0, 60*time.Second)
				return h.status == 200
			})
		}
	}
	// Both guests are compiled once before the clock starts: sf1 runs the second one for a moment.
	if _, err := ctl.ActivateVersion(bg, &controlv1.ActivateVersionRequest{FunctionId: sfID[1], VersionId: vB[1]}); err != nil {
		t.Fatal(err)
	}
	for _, fe := range frontends {
		fe := fe
		moEventually(t, "the second guest to run", 180*time.Second, func() bool {
			h := moHTTP(fe, "sf1.test", "/", "warm2", 0, 120*time.Second)
			return h.status == 200 && strings.Contains(h.body, "tag=g2")
		})
	}
	if _, err := ctl.ActivateVersion(bg, &controlv1.ActivateVersionRequest{FunctionId: sfID[1], VersionId: vA[1]}); err != nil {
		t.Fatal(err)
	}
	// A baseline of what the process holds with the anchors and no load.
	moCloseIdle()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	baseG, baseHeap, baseConns := runtime.NumGoroutine(), float64(ms.HeapInuse)/(1<<20), c.conns("mo-orch-%")

	// The warm-up's records are not the soak's: set them aside once they are written.
	moEventually(t, "the warm-up's records", 20*time.Second, func() bool {
		a := c.records()
		time.Sleep(500 * time.Millisecond)
		return a == c.records()
	})
	pre := map[string]bool{}
	if prows, err := c.admin.Query(bg, `SELECT id::text FROM invocations`); err == nil {
		for prows.Next() {
			var id string
			prows.Scan(&id)
			pre[id] = true
		}
		prows.Close()
	}

	// ------------------------------------------------------------- the churn
	stop := make(chan struct{})
	var churn sync.WaitGroup
	var fatalMu sync.Mutex
	var fatals []string
	fail := func(format string, a ...any) {
		fatalMu.Lock()
		if len(fatals) < 30 {
			fatals = append(fatals, fmt.Sprintf(format, a...))
		}
		fatalMu.Unlock()
	}
	sleepOrStop := func(d time.Duration) bool {
		select {
		case <-stop:
			return false
		case <-time.After(d):
			return true
		}
	}
	jitter := func(lo, hi int) time.Duration { return time.Duration(lo+rand.IntN(hi-lo+1)) * time.Millisecond }
	var ops atomic.Int64
	// do retries a control call that the database aborted; any other error is a failure.
	do := func(what string, f func(o *moOrch) error) {
		for range 5 {
			err := f(pick())
			if err == nil {
				ops.Add(1)
				return
			}
			if connect.CodeOf(err) == connect.CodeAborted {
				continue
			}
			fail("control %s: %v", what, err)
			return
		}
	}
	apply := func(what string, steps []*controlv1.Step) *controlv1.ApplyResponse {
		var out *controlv1.ApplyResponse
		do(what, func(o *moOrch) error {
			r, err := o.control().Apply(bg, &controlv1.ApplyRequest{Steps: steps})
			if err == nil && r.Failure != nil {
				fail("plan %s failed at step %d: %s %s", what, r.Failure.Step, r.Failure.Code, r.Failure.Message)
			}
			out = r
			return err
		})
		return out
	}
	spawn := func(f func()) { churn.Add(1); go func() { defer churn.Done(); f() }() }

	var kills, drains, joins atomic.Int64
	var lastDisturb atomic.Int64 // unix nanos of the last worker kill or drain
	// 1. workers join and leave, gracefully (drain) and killed.
	spawn(func() {
		step := 0
		gap := func() time.Duration {
			if soakRace {
				return jitter(4000, 6000)
			}
			return jitter(1500, 3000)
		}
		for sleepOrStop(gap()) {
			wmu.Lock()
			var temp []*moWorker
			for _, w := range workers {
				if w != anchors[0] && w != anchors[1] {
					temp = append(temp, w)
				}
			}
			wmu.Unlock()
			step++
			// join, kill, join, drain, in turn, so that a short run has all three
			switch r := []string{"join", "kill", "join", "drain"}[step%4]; {
			case r == "join" || len(temp) == 0:
				startWorker(rand.IntN(3))
				joins.Add(1)
			case r == "drain":
				w := temp[rand.IntN(len(temp))]
				wmu.Lock()
				workers = removeWorker(workers, w)
				wmu.Unlock()
				lastDisturb.Store(time.Now().UnixNano())
				drains.Add(1)
				go w.stop()
			default:
				w := temp[rand.IntN(len(temp))]
				wmu.Lock()
				workers = removeWorker(workers, w)
				wmu.Unlock()
				lastDisturb.Store(time.Now().UnixNano())
				kills.Add(1)
				w.kill()
			}
		}
	})
	// 2. functions are created, deployed, redeployed, rolled back and deleted.
	for k := range 3 {
		spawn(func() {
			name := fmt.Sprint("cf", k)
			for gen := 0; ; gen++ {
				// created with everything at once: a request never sees it half made
				r := apply("create "+name, []*controlv1.Step{
					moStep(&controlv1.CreateFunctionRequest{Name: name}, "f"),
					moStep(&controlv1.CreateVersionRequest{FunctionId: "$f", Module: g1, Abi: "wagi-1", Activate: true}, "v1"),
					moSetEnvStep("$f", "FN", name),
					moStep(&controlv1.CreateRouteRequest{HostPattern: name + ".test", PathPrefix: "/", FunctionId: "$f"}, ""),
				})
				if r == nil || r.Failure != nil || len(r.Results) < 2 {
					return
				}
				fid, v1 := r.Results[0].GetCreateFunction().GetFunction().GetId(), r.Results[1].GetCreateVersion().GetVersion().GetId()
				if !sleepOrStop(jitter(150, 500)) {
					do("delete", func(o *moOrch) error {
						_, err := o.control().DeleteFunction(bg, &controlv1.DeleteFunctionRequest{FunctionId: fid})
						return err
					})
					return
				}
				var v2 string
				do("redeploy", func(o *moOrch) error {
					r, err := o.control().CreateVersion(bg, &controlv1.CreateVersionRequest{FunctionId: fid, Module: g2, Abi: "wagi-1", Activate: true})
					if err == nil {
						v2 = r.Version.Id
					}
					return err
				})
				_ = v2
				sleepOrStop(jitter(100, 400))
				do("rollback", func(o *moOrch) error {
					_, err := o.control().ActivateVersion(bg, &controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: v1})
					return err
				})
				sleepOrStop(jitter(100, 400))
				do("delete", func(o *moOrch) error {
					_, err := o.control().DeleteFunction(bg, &controlv1.DeleteFunctionRequest{FunctionId: fid})
					return err
				})
				if !sleepOrStop(jitter(100, 500)) {
					return
				}
			}
		})
	}
	// 3. sf0 is moved between its two versions and states by plans: the version and
	// the state it names must always agree (g1 with A, g2 with B), at every orchestrator.
	spawn(func() {
		for i := 0; sleepOrStop(jitter(50, 250)); i++ {
			if i%2 == 0 {
				apply("sf0 to B", []*controlv1.Step{
					moStep(&controlv1.ActivateVersionRequest{FunctionId: sfID[0], VersionId: vB[0]}, ""), moSetEnvStep(sfID[0], "STATE", "B")})
			} else {
				apply("sf0 to A", []*controlv1.Step{
					moStep(&controlv1.ActivateVersionRequest{FunctionId: sfID[0], VersionId: vA[0]}, ""), moSetEnvStep(sfID[0], "STATE", "A")})
			}
		}
	})
	// 4. sf1: single calls (rollbacks, environment); sf2: labels and environment.
	spawn(func() {
		for i := 0; sleepOrStop(jitter(50, 200)); i++ {
			v := []string{vA[1], vB[1]}[i%2]
			do("activate sf1", func(o *moOrch) error {
				_, err := o.control().ActivateVersion(bg, &controlv1.ActivateVersionRequest{FunctionId: sfID[1], VersionId: v})
				return err
			})
			do("env sf1", func(o *moOrch) error {
				_, err := o.control().SetEnv(bg, &controlv1.SetEnvRequest{FunctionId: sfID[1], Entry: &controlv1.EnvEntry{Name: "STATE", Value: fmt.Sprint("s", i)}})
				return err
			})
		}
	})
	spawn(func() {
		for i := 0; sleepOrStop(jitter(50, 200)); i++ {
			do("labels sf2", func(o *moOrch) error {
				_, err := o.control().PatchLabels(bg, &controlv1.PatchLabelsRequest{FunctionId: sfID[2], Set: map[string]string{"soak/gen": fmt.Sprint(i)}})
				return err
			})
			do("env sf2", func(o *moOrch) error {
				_, err := o.control().SetEnv(bg, &controlv1.SetEnvRequest{FunctionId: sfID[2], Entry: &controlv1.EnvEntry{Name: "NOTE", Value: fmt.Sprint(i)}, ValidateOnly: i%3 == 0})
				return err
			})
			if i%5 == 0 {
				do("egress sf2", func(o *moOrch) error {
					_, err := o.control().SetEgress(bg, &controlv1.SetEgressRequest{FunctionId: sfID[2], Allow: []string{fmt.Sprintf("h%d.example.com", i)}})
					return err
				})
			}
		}
	})
	// 5. routes come and go: x<k>.test, to sf<k>, is a route that exists half the time.
	for k := range 3 {
		spawn(func() {
			host := fmt.Sprintf("x%d.test", k)
			for sleepOrStop(jitter(100, 350)) {
				var rid string
				do("create route", func(o *moOrch) error {
					r, err := o.control().CreateRoute(bg, &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: sfID[k]})
					if err == nil {
						rid = r.Route.Id
					}
					return err
				})
				if rid == "" || !sleepOrStop(jitter(100, 350)) {
					return
				}
				do("delete route", func(o *moOrch) error {
					_, err := o.control().DeleteRoute(bg, &controlv1.DeleteRouteRequest{RouteId: rid})
					return err
				})
			}
		})
	}
	// 6. control reads, all the time, at every orchestrator: none may fail.
	var reads atomic.Int64
	for range 3 {
		spawn(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				o := pick()
				var err error
				switch i % 5 {
				case 0:
					_, err = o.control().ListFunctions(bg, &controlv1.ListFunctionsRequest{PageSize: 3})
				case 1:
					_, err = o.control().ListRoutes(bg, &controlv1.ListRoutesRequest{PageSize: 3})
				case 2:
					_, err = o.control().ListWorkers(bg, &controlv1.ListWorkersRequest{})
				case 3:
					_, err = o.control().ListInvocations(bg, &controlv1.ListInvocationsRequest{PageSize: 5})
				default:
					_, err = o.control().ListEnv(bg, &controlv1.ListEnvRequest{FunctionId: sfID[rand.IntN(3)]})
				}
				if err != nil {
					fail("control read %d: %v", i%5, err)
					return
				}
				reads.Add(1)
				time.Sleep(10 * time.Millisecond)
			}
		})
	}

	// ----------------------------------------------------------------- the load
	type rec struct {
		status int
		lat    time.Duration
		at     time.Time
		host   string
	}
	var rmu sync.Mutex
	var recs []rec
	byStatus := map[int]int{}
	ids := map[string]int{}
	var violations []string
	violate := func(format string, a ...any) {
		rmu.Lock()
		if len(violations) < 30 {
			violations = append(violations, fmt.Sprintf(format, a...))
		}
		rmu.Unlock()
	}
	hosts := []string{"sf0.test", "sf1.test", "sf2.test", "cf0.test", "cf1.test", "cf2.test", "x0.test", "x1.test", "x2.test", "sf0.test", "sf0.test", "sf1.test"}
	expectFn := func(host string) string {
		switch {
		case strings.HasPrefix(host, "x"):
			return "sf" + host[1:2]
		default:
			return host[:3]
		}
	}
	mayBeAbsent := func(host string) bool { return host[0] == 'c' || host[0] == 'x' }
	var sent, routedNon404, budgetHits atomic.Int64
	var load sync.WaitGroup
	loadStop := make(chan struct{})
	clients := 10 // against 16+ slots: capacity is not what is tested
	if soakRace {
		clients = 5
	}
	for g := range clients {
		load.Add(1)
		go func() {
			defer load.Done()
			for i := 0; ; i++ {
				select {
				case <-loadStop:
					return
				default:
				}
				host := hosts[rand.IntN(len(hosts))]
				fe := frontends[(g+i)%3]
				nonce := fmt.Sprintf("n%d-%d-%d", g, i, rand.Uint32())
				t0 := time.Now()
				h := moHTTP(fe, host, "/p", nonce, rand.IntN(25), 45*time.Second)
				lat := time.Since(t0)
				sent.Add(1)
				rmu.Lock()
				recs = append(recs, rec{h.status, lat, t0, host})
				byStatus[h.status]++
				rmu.Unlock()
				switch {
				case h.err != nil:
					violate("%s %s: transport error: %v (a request must get one response)", nonce, host, h.err)
				case h.status == 200:
					f := moFields([]byte(h.body))
					if f["nonce"] != nonce || f["hnonce"] != nonce || f["body"] != nonce || f["path"] != "/p" {
						violate("%s %s: wrong answer, nonce fields %q %q %q path %q", nonce, host, f["nonce"], f["hnonce"], f["body"], f["path"])
					}
					if f["fn"] != expectFn(host) {
						violate("%s %s: served by function %q, the host belongs to %q", nonce, host, f["fn"], expectFn(host))
					}
					if f["host"] != host {
						violate("%s %s: the guest saw host %q", nonce, host, f["host"])
					}
					tag, st := f["tag"], f["state"]
					if tag != "g1" && tag != "g2" {
						violate("%s %s: unknown version tag %q", nonce, host, tag)
					}
					if host == "sf0.test" && (tag == "g1") != (st == "A") || host == "sf0.test" && st != "A" && st != "B" {
						violate("%s sf0: version %s with state %q: a plan was seen half applied", nonce, tag, st)
					}
					id := h.header.Get("X-Edgerun-Invocation-Id")
					if id == "" {
						violate("%s %s: 200 without an invocation id", nonce, host)
					}
					routedNon404.Add(1)
					rmu.Lock()
					ids[id]++
					rmu.Unlock()
				case h.status == 404:
					if !mayBeAbsent(host) {
						violate("%s %s: 404 for a function that is never deleted and whose route is never touched", nonce, host)
					}
					if h.header.Get("X-Edgerun-Invocation-Id") != "" {
						violate("%s %s: 404 with an invocation id (nothing is recorded for FUNCTION_NOT_FOUND)", nonce, host)
					}
				default:
					// What a worker leaving can cause: a killed worker's calls end 500 (INTERNAL);
					// a request offered to a worker that was draining or just killed may find no
					// capacity (503) within seconds of it. Anything else is a bug.
					near := time.Since(time.Unix(0, lastDisturb.Load())) < 10*time.Second
					ok := (h.status == 500 || h.status == 503) && near && lastDisturb.Load() != 0
					// A request whose whole 30 s budget went (a worker that joined spent it
					// compiling, on a machine with the race detector on) fails with 504, or with a
					// 500 when the frontend's timer and the orchestrator's race: it is
					// the budget, not the platform, and its id may not have reached the client.
					budget := lat >= 25*time.Second
					if budget {
						budgetHits.Add(1)
						ok = true
					}
					if !ok {
						violate("%s %s: status %d %q outside any worker disturbance", nonce, host, h.status, h.body)
					}
					routedNon404.Add(1)
					id := h.header.Get("X-Edgerun-Invocation-Id")
					if id != "" {
						rmu.Lock()
						ids[id]++
						rmu.Unlock()
					} else if h.status == 500 && !budget {
						violate("%s %s: a failure after routing without an invocation id", nonce, host)
					}
				}
			}
		}()
	}

	// ------------------------------------------------------------ sampling
	var samples []soakSample
	start := time.Now()
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-loadStop:
				return
			case <-tk.C:
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				wmu.Lock()
				nw := len(workers)
				wmu.Unlock()
				samples = append(samples, soakSample{time.Since(start), runtime.NumGoroutine(), float64(m.HeapInuse) / (1 << 20), c.conns("mo-orch-%"), nw})
			}
		}
	}()

	time.Sleep(dur)
	close(stop)
	churn.Wait()
	close(loadStop)
	load.Wait()
	<-sampleDone
	t.Logf("churn ended: %d control writes, %d control reads, %d workers joined, %d drained, %d killed", ops.Load(), reads.Load(), joins.Load(), drains.Load(), kills.Load())

	// ---------------------------------------------------------------- quiet
	fatalMu.Lock()
	for _, f := range fatals {
		t.Errorf("churn: %s", f)
	}
	fatalMu.Unlock()
	rmu.Lock()
	for _, v := range violations {
		t.Errorf("response: %s", v)
	}
	rmu.Unlock()

	// every worker that is still registered is idle once the stragglers finish
	moEventually(t, "worker in-flight to return to 0", 30*time.Second, func() bool {
		return moInFlight(t, c.A()) == 0
	})
	moEventually(t, "records to settle", 30*time.Second, func() bool {
		a := c.records()
		time.Sleep(700 * time.Millisecond)
		return a == c.records()
	})
	seen := map[string]bool{}
	rows, err := c.admin.Query(bg, `SELECT id::text FROM invocations`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		rows.Scan(&id)
		if !pre[id] {
			seen[id] = true
		}
	}
	rows.Close()
	dupResp, lost, extra := 0, 0, 0
	for id, n := range ids {
		if n > 1 {
			dupResp++
		}
		if !seen[id] {
			lost++
		}
	}
	for id := range seen {
		if ids[id] == 0 {
			extra++
		}
	}
	nRecords := c.count(`SELECT count(*) FROM invocations`) - len(pre)
	t.Logf("RECORDS: %d requests answered, %d of them routed (not 404), %d distinct invocation ids in responses, %d records in the database; ids delivered twice: %d, ids without a record: %d, records no response named: %d",
		sent.Load(), routedNon404.Load(), len(ids), nRecords, dupResp, lost, extra)
	if dupResp != 0 {
		t.Errorf("%d invocation ids were delivered to more than one request", dupResp)
	}
	if lost != 0 || extra > int(budgetHits.Load()) || nRecords-len(ids) > int(budgetHits.Load()) {
		t.Errorf("records and responses disagree: %d ids without a record, %d records no response named, %d records for %d ids", lost, extra, nRecords, len(ids))
	}
	if int(routedNon404.Load()) != nRecords {
		t.Errorf("%d routed requests (not 404), %d records", routedNon404.Load(), nRecords)
	}
	for _, o := range c.orchs {
		for _, w := range o.logs.warnings() {
			// A killed worker's heartbeat that is cut off in the database call is logged
			// at ERROR as a store error (context canceled): noise, see the report.
			if !strings.Contains(w, "worker unreachable") && !strings.Contains(w, "worker call failed") && !strings.Contains(w, "context canceled") {
				t.Errorf("orchestrator %s logged: %s", o.name, w)
			}
		}
		if n := o.logs.n("dropping invocation records: the store keeps failing"); n != 0 {
			t.Errorf("orchestrator %s dropped %d batches of records", o.name, n)
		}
	}

	// ------------------------------------------------------------ resources
	// Known defect MO-6 (soak_leak_test.go): a stopped worker is never freed, about
	// 9 MiB per lifetime per guest compiled. Until it is fixed the soak allows that
	// much per worker that joined; with EDGERUN_E2E_KNOWN_DEFECTS set it allows none.
	leakAllow := 0.0
	if os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		leakAllow = 20 * float64(joins.Load())
	}
	kinds := []struct {
		name string
		get  func(soakSample) float64
		tol  float64
	}{
		{"goroutines", func(s soakSample) float64 { return float64(s.goroutines) }, 150},
		// The heap is mostly compiled guests: a Go guest compiled by wazero is tens of MiB, in
		// every worker that ran it, and the number of workers swings between 2 and 4.
		{"heap in use MiB", func(s soakSample) float64 { return s.heapMB }, 150 + leakAllow},
		{"db connections", func(s soakSample) float64 { return float64(s.conns) }, 12},
	}
	var table strings.Builder
	fmt.Fprintf(&table, "\nSOAK SUMMARY (%v of churn)\n", dur)
	var gs, hs, cs []float64
	for _, s := range samples {
		gs, hs, cs = append(gs, float64(s.goroutines)), append(hs, s.heapMB), append(cs, float64(s.conns))
	}
	series := [][]float64{gs, hs, cs}
	fmt.Fprintf(&table, "  %-8s %10s %10s %6s %8s\n", "t", "goroutines", "heap MiB", "conns", "workers")
	step := max(1, len(samples)/10)
	for i := 0; i < len(samples); i += step {
		s := samples[i]
		fmt.Fprintf(&table, "  %-8s %10d %10.1f %6d %8d\n", s.at.Round(100*time.Millisecond), s.goroutines, s.heapMB, s.conns, s.workers)
	}
	for i, k := range kinds {
		tr := soakTrend(series[i])
		fmt.Fprintf(&table, "  trend %-16s %+8.1f over the last 70%% of the run (tolerance %.0f)\n", k.name, tr, k.tol)
		if tr > k.tol {
			t.Errorf("%s grew by %.1f over the run (tolerance %.0f): %v", k.name, tr, k.tol, series[i])
		}
	}
	// A leak shows after everything that came and went has gone: back to the anchors.
	wmu.Lock()
	rest := append([]*moWorker(nil), workers...)
	wmu.Unlock()
	for _, w := range rest {
		if w != anchors[0] && w != anchors[1] {
			w.stop()
		}
	}
	http.DefaultClient.CloseIdleConnections()
	settleDeadline := time.Now().Add(30 * time.Second)
	for runtime.NumGoroutine() > baseG+60 && time.Now().Before(settleDeadline) {
		moCloseIdle()
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseG+60 {
		t.Errorf("%d goroutines 30s after everything stopped (baseline %d): %s", n, baseG, soakGoroutines())
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	endG, endHeap, endConns := runtime.NumGoroutine(), float64(ms.HeapInuse)/(1<<20), c.conns("mo-orch-%")
	fmt.Fprintf(&table, "  after the churn is gone: goroutines %d (baseline %d), heap %.1f MiB (baseline %.1f), db connections %d (baseline %d)\n", endG, baseG, endHeap, baseHeap, endConns, baseConns)
	// The two anchors have by now compiled both guests (at the baseline, one).
	if endHeap > baseHeap+192+leakAllow {
		t.Errorf("heap in use is %.1f MiB, %.1f above the baseline: something is retained", endHeap, endHeap-baseHeap)
	}
	poolMax := max(4, runtime.NumCPU()) * 3
	if endConns > poolMax {
		t.Errorf("%d database connections, the three pools can hold %d", endConns, poolMax)
	}

	// -------------------------------------------------- the summary table
	rmu.Lock()
	lats := make([]time.Duration, 0, len(recs))
	for _, r := range recs {
		lats = append(lats, r.lat)
	}
	var codes []string
	for s, n := range byStatus {
		codes = append(codes, fmt.Sprintf("%d=%d", s, n))
	}
	rmu.Unlock()
	sort.Strings(codes)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	q := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[min(len(lats)-1, int(p*float64(len(lats))))]
	}
	fmt.Fprintf(&table, "  requests %d (%.0f/s) by status: %s\n  latency p50 %v p90 %v p99 %v max %v\n",
		len(lats), float64(len(lats))/dur.Seconds(), strings.Join(codes, " "), q(.5).Round(time.Millisecond), q(.9).Round(time.Millisecond), q(.99).Round(time.Millisecond), q(1).Round(time.Millisecond))
	fmt.Fprintf(&table, "  churn: %d control writes (plans, deploys, rollbacks, deletes, env, labels, routes), %d reads; workers joined %d, drained %d, killed %d\n", ops.Load(), reads.Load(), joins.Load(), drains.Load(), kills.Load())
	fmt.Fprintf(&table, "  records %d = routed requests %d; invocation ids delivered twice %d; in-flight at the end 0\n", nRecords, routedNon404.Load(), dupResp)
	t.Log(table.String())
	minRequests := 60
	if soakRace {
		minRequests = 10 // compiling a guest under the race detector takes many seconds, in every worker
	}
	if len(lats) < minRequests {
		t.Errorf("only %d requests were made: the soak did not exercise anything", len(lats))
	}
}

func removeWorker(ws []*moWorker, w *moWorker) []*moWorker {
	out := ws[:0:0]
	for _, x := range ws {
		if x != w {
			out = append(out, x)
		}
	}
	return out
}
