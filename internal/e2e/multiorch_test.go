package e2e

// Several orchestrators on one database (docs/protocol.md "Read your writes":
// "Other orchestrator instances may take a few seconds to catch up"; README:
// "The orchestrator is stateless: run as many as you like against one database").
//
// What is promised, from the documents and the code:
//   - a change made through one orchestrator is visible to Invoke on that one at
//     once, and on the others "within a few seconds" (protocol.md). The code's
//     refresh interval is 1s (snapshot.Cache.Run, app.Config.RefreshEvery), a
//     number that appears in no document and that no flag of cmd/orchestrator can
//     change, so "a few seconds" is the only bound a reader has. The tests assert
//     the code's own number plus slack (moBound) and print what they measured.
//   - a plan is visible all at once, or not at all.
//   - a name or a route is taken by exactly one writer (already_exists for the
//     others), and two changes that deadlock answer aborted.
//
// Run with -v to see the measurements.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// moRefresh is the orchestrators' refresh interval in these tests: the code's
// default, because the point is what ships. moBound is what a change may take to
// reach another orchestrator: one refresh, plus what a loaded machine (and -race)
// adds to a refresh's own round trips.
const (
	moRefresh = time.Second
	moBound   = moRefresh + 1500*time.Millisecond
)

func moRecFn(t *testing.T, c *moCluster, name, host string) string {
	t.Helper()
	ctl := c.A().control()
	fid := moCreateFn(t, ctl, name)
	moDeploy(t, ctl, fid, "rec-1", []byte("module-"+name))
	if host != "" {
		moRoute(t, ctl, host, "/", fid)
	}
	return fid
}

// moView invokes the recording worker's function through o and returns what the
// orchestrator handed the worker (or the error).
func moProbe(o *moOrch, host, nonce string) (moView, error) {
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := o.ingress().Invoke(cctx, moReq(host, "/", nonce))
	if err != nil {
		return moView{}, err
	}
	body, err := res.ReadAll()
	if err != nil {
		return moView{}, err
	}
	return moParseView(string(body)), nil
}

func TestMultiOrchConcurrentStartMigratesOnce(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3})
	want := len(moMigrationFiles(t))
	if got := c.count(`SELECT count(*) FROM schema_migrations`); got != want {
		t.Fatalf("schema_migrations has %d rows, want %d", got, want)
	}
	// Three more, all at once, on the migrated database: none may migrate again,
	// and none may fail on the lock the others hold.
	extra := make([]*moOrch, 3)
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range extra {
		extra[i] = &moOrch{c: c, name: fmt.Sprint("x", i), cfg: c.A().cfg, logs: &moLogs{}}
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = extra[i].start("127.0.0.1:0") }()
	}
	wg.Wait()
	for i, e := range extra {
		if errs[i] != nil {
			t.Fatalf("late orchestrator %d: %v", i, errs[i])
		}
		defer e.stop()
	}
	applied := 0
	for _, o := range append(append([]*moOrch{}, c.orchs...), extra...) {
		applied += o.logs.n("applied migrations")
	}
	if applied != 1 {
		t.Errorf("%d orchestrators logged applying migrations, want exactly 1", applied)
	}
	if got := c.count(`SELECT count(*) FROM schema_migrations`); got != want {
		t.Errorf("schema_migrations has %d rows after the second start, want %d", got, want)
	}
	for _, o := range c.orchs {
		if got := moHealthz(o); got != 200 {
			t.Errorf("orchestrator %s /healthz = %d", o.name, got)
		}
	}
}

// A worker registered at A runs what arrives at B and C, within the bound, with
// the right answer to the right request.
func TestMultiOrchWorkerRegisteredAtOneServesAll(t *testing.T) {
	// A second between heartbeats: compiling the guest keeps a loaded (or -race) machine
	// busy for longer than the 600ms a 200ms heartbeat is alive for.
	c := newMoCluster(t, moOpts{n: 3, heartbeat: time.Second})
	w := c.startWorker(t, 8, moOnly(c.A()))
	moWaitAlive(t, c.A(), 1, 60*time.Second)

	ctl := c.A().control()
	fid := moCreateFn(t, ctl, "echo")
	moDeploy(t, ctl, fid, "wagi-1", c.wasm)
	moSetEnv(t, ctl, fid, "FN", "echo")
	moRoute(t, ctl, "echo.test", "/", fid)
	t0 := time.Now()

	// First success at each orchestrator: the route, the version, the environment
	// and the worker all have to arrive. The first run also compiles the module.
	first := map[string]time.Duration{}
	for _, o := range c.orchs {
		o := o
		first[o.name] = moEventually(t, "echo to be served by "+o.name, 90*time.Second, func() bool {
			r := moInvoke(o.ingress(), "echo.test", "/", "warm-"+o.name, 0, 30*time.Second)
			return r.err == nil && r.status == 200
		})
	}
	t.Logf("first successful invocation, from the moment the deploy returned: %v", first)
	_ = t0

	var wg sync.WaitGroup
	var bad atomic.Int64
	var sent atomic.Int64
	const per = 60
	for _, o := range c.orchs {
		for g := range 2 { // 6 in flight against the worker's 8 slots: capacity is not what is tested
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range per / 2 {
					nonce := fmt.Sprintf("%s-%d-%d", o.name, g, i)
					r := moInvoke(o.ingress(), "echo.test", "/x", nonce, 0, 30*time.Second)
					sent.Add(1)
					switch {
					case r.err != nil || r.status != 200:
						bad.Add(1)
						t.Errorf("%s: %v status %d", nonce, r.err, r.status)
					case r.mismatch != "":
						bad.Add(1)
						t.Errorf("%s: %s", nonce, r.mismatch)
					case r.f("fn") != "echo" || r.f("path") != "/x":
						bad.Add(1)
						t.Errorf("%s: wrong state in the answer %v", nonce, r.fields)
					}
				}
			}()
		}
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d of %d invocations failed", bad.Load(), sent.Load())
	}

	// Every orchestrator wrote records, and all of them are in the one database:
	// what B served is listed through A.
	moEventually(t, "all records", 20*time.Second, func() bool { return c.records() >= int(sent.Load()) })
	listed := 0
	for _, err := range ctl.AllInvocations(context.Background(), fid) {
		if err != nil {
			t.Fatal(err)
		}
		listed++
		if listed > 10_000 {
			break
		}
	}
	if listed < int(sent.Load()) {
		t.Errorf("ListInvocations at A shows %d records, want at least %d", listed, sent.Load())
	}

	// A worker that leaves (graceful: it deregisters at A) stops receiving work
	// from B and C within the bound; until then a request is told NO_CAPACITY,
	// never anything else.
	w.stop()
	if w.exitErr != nil {
		t.Errorf("worker exit: %v", w.exitErr)
	}
	for _, o := range c.orchs {
		o := o
		lag := moEventually(t, "no capacity at "+o.name, 20*time.Second, func() bool {
			r := moInvoke(o.ingress(), "echo.test", "/", "gone-"+o.name, 0, 10*time.Second)
			if r.err == nil && r.status == 200 {
				return false
			}
			if reason(r.err) == commonv1.ErrorReason_ERROR_REASON_INTERNAL {
				// Finding MO-7: the worker deregisters, then closes its listener and its idle
				// connections; a request that this orchestrator sends on a pooled connection in
				// that instant is reset after it was written, which the retry rule forbids
				// retrying, and the client gets INTERNAL for a graceful shutdown.
				t.Logf("%s: a request racing the worker's shutdown ended %v", o.name, r.err)
				return false
			}
			if reason(r.err) != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
				t.Fatalf("%s after the worker left: %v (status %d), want NO_CAPACITY", o.name, r.err, r.status)
			}
			return true
		})
		t.Logf("worker deregistered at A, no longer used by %s after %v", o.name, lag.Round(time.Millisecond))
		if lag > moBound {
			t.Errorf("%s kept using a worker that had deregistered for %v (> %v)", o.name, lag, moBound)
		}
	}
}

func TestMultiOrchVisibilityLag(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, refresh: moRefresh})
	moStartRecWorker(t, c.A().url())
	netSvc := "http://127.0.0.1:1"
	moStartFakeNetwork(t, c.A().url(), netSvc)
	ctl := c.A().control()

	// Registered at A, a worker and a network service become usable at B and C.
	base := moRecFn(t, c, "base", "base.test")
	tReg := time.Now()
	for _, o := range c.orchs[1:] {
		o := o
		moEventually(t, "the worker registered at A to serve at "+o.name, 30*time.Second, func() bool {
			_, err := moProbe(o, "base.test", "w")
			return err == nil
		})
		t.Logf("worker (registered at A before the function was created) usable at %s %v after the deploy", o.name, time.Since(tReg).Round(time.Millisecond))
	}
	egFn := moRecFn(t, c, "eg", "eg.test")
	// A grant needs a live network service that carries the entry: that the
	// service registered at A is known to B and C is part of what this waits for.
	if _, err := ctl.SetEgress(context.Background(), &controlv1.SetEgressRequest{FunctionId: egFn, Allow: []string{"warm.example.com"}}); err != nil {
		t.Fatal(err)
	}
	for _, o := range c.orchs {
		o := o
		d := moEventually(t, "the network service registered at A to be granted at "+o.name, 30*time.Second, func() bool {
			v, err := moProbe(o, "eg.test", "n")
			return err == nil && len(v.Egress) == 1 && strings.HasSuffix(v.Egress[0], ">"+netSvc)
		})
		t.Logf("network service registered at A grantable at %s after %v", o.name, d.Round(time.Millisecond))
	}

	envFn := moRecFn(t, c, "envfn", "env.test")
	routeFn := moRecFn(t, c, "routefn", "")

	type obs struct{ kind, who string }
	var mu sync.Mutex
	lags := map[obs]moDist{}
	record := func(kind, who string, d time.Duration) {
		mu.Lock()
		lags[obs{kind, who}] = append(lags[obs{kind, who}], d)
		mu.Unlock()
	}

	// observe polls o until seen() holds and returns how long after t0 that was.
	observe := func(o *moOrch, t0 time.Time, seen func(*moOrch) bool) time.Duration {
		for time.Since(t0) < 15*time.Second {
			if seen(o) {
				return time.Since(t0)
			}
			time.Sleep(4 * time.Millisecond)
		}
		return time.Since(t0)
	}

	const trials = 20
	// The functions that will be deleted exist, and are served by every orchestrator, beforehand.
	delFns := make([]string, trials)
	for k := range trials {
		name := fmt.Sprint("del", k)
		delFns[k] = moRecFn(t, c, name, name+".test")
	}
	for _, o := range c.orchs {
		for k := range trials {
			name := fmt.Sprint("del", k)
			moEventually(t, name+" to be served at "+o.name, 20*time.Second, func() bool { _, err := moProbe(o, name+".test", "p"); return err == nil })
		}
	}
	for k := range trials {
		// The writer's own refresh is not what is measured, and the readers' ticks
		// are free-running: a random pause stops the trials from marching in step
		// with them.
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)

		kind := map[string]func() (func(*moOrch) bool, time.Time){
			"deploy": func() (func(*moOrch) bool, time.Time) {
				module := []byte(fmt.Sprintf("module-base-%d", k))
				moDeploy(t, ctl, base, "rec-1", module)
				want := store.Digest(module)
				return func(o *moOrch) bool { v, err := moProbe(o, "base.test", "d"); return err == nil && v.Digest == want }, time.Now()
			},
			"env": func() (func(*moOrch) bool, time.Time) {
				val := fmt.Sprint("v", k)
				moSetEnv(t, ctl, envFn, "X", val)
				return func(o *moOrch) bool { v, err := moProbe(o, "env.test", "e"); return err == nil && v.Env["X"] == val }, time.Now()
			},
			"route": func() (func(*moOrch) bool, time.Time) {
				host := fmt.Sprintf("rt%d.test", k)
				moRoute(t, ctl, host, "/", routeFn)
				return func(o *moOrch) bool { _, err := moProbe(o, host, "r"); return err == nil }, time.Now()
			},
			"egress": func() (func(*moOrch) bool, time.Time) {
				entry := fmt.Sprintf("h%d.example.com", k)
				if _, err := ctl.SetEgress(context.Background(), &controlv1.SetEgressRequest{FunctionId: egFn, Allow: []string{entry}}); err != nil {
					t.Error(err)
				}
				want := entry + ">" + netSvc
				return func(o *moOrch) bool {
					v, err := moProbe(o, "eg.test", "g")
					return err == nil && len(v.Egress) == 1 && v.Egress[0] == want
				}, time.Now()
			},
			"delete": func() (func(*moOrch) bool, time.Time) {
				name := fmt.Sprint("del", k)
				fid := delFns[k]
				time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond) // the readers' refreshes are free-running: do not march in step with them
				if _, err := ctl.DeleteFunction(context.Background(), &controlv1.DeleteFunctionRequest{FunctionId: fid}); err != nil {
					t.Error(err)
				}
				return func(o *moOrch) bool {
					_, err := moProbe(o, name+".test", "q")
					if err != nil && reason(err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND {
						t.Errorf("%s at %s while being deleted: %v, want success or FUNCTION_NOT_FOUND", name, o.name, err)
					}
					return reason(err) == commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND
				}, time.Now()
			},
		}
		var wg sync.WaitGroup
		for name, do := range kind {
			wg.Add(1)
			go func() {
				defer wg.Done()
				seen, t0 := do()
				// Read your writes: the orchestrator the change was made at shows it at once.
				if !seen(c.A()) {
					t.Errorf("trial %d %s: not visible at A, the orchestrator it was written through, when the write returned", k, name)
				}
				var inner sync.WaitGroup
				for _, o := range c.orchs[1:] {
					inner.Add(1)
					go func() {
						defer inner.Done()
						d := observe(o, t0, seen)
						record(name, o.name, d)
						if d > moBound {
							t.Errorf("trial %d: a %s made at A took %v to reach %s (bound %v)", k, name, d.Round(time.Millisecond), o.name, moBound)
						}
					}()
				}
				inner.Wait()
			}()
		}
		wg.Wait()
	}

	t.Logf("VISIBILITY LAG, a change made at A until Invoke at the other orchestrator reflects it (refresh interval %v, %d trials, bound asserted %v):", moRefresh, trials, moBound)
	var all moDist
	for _, name := range []string{"deploy", "env", "route", "egress", "delete"} {
		for _, who := range []string{"b", "c"} {
			d := lags[obs{name, who}]
			all = append(all, d...)
			t.Logf("  %-7s at %s: %v", name, strings.ToUpper(who), d)
		}
	}
	t.Logf("  ALL: %v", all)
}

func moMigrationFiles(t *testing.T) []string {
	t.Helper()
	m, err := moLoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func moHealthz(o *moOrch) int {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return moGet(cctx, o.url()+"/healthz")
}

var _ = pgxpool.New
