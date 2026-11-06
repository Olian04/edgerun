package e2e

// The orchestrators and their one database: connections that are killed,
// a database that goes away and comes back, and what the other components dial.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
)

// moTraffic runs invocations of one recording-worker function at every
// orchestrator, and counts what came of them. A routed invocation (one that was
// not FUNCTION_NOT_FOUND) is one that must have a record.
type moTraffic struct {
	stop    chan struct{}
	wg      sync.WaitGroup
	sent    atomic.Int64
	ok      atomic.Int64
	routed  atomic.Int64
	mu      sync.Mutex
	codes   map[string]int
	failAt  []time.Time // when each failure ended
	lat     moDist
	wrong   atomic.Int64
	started time.Time
}

func moStartTraffic(t *testing.T, orchs []*moOrch, host string, perOrch int) *moTraffic {
	tr := &moTraffic{stop: make(chan struct{}), codes: map[string]int{}, started: time.Now()}
	for _, o := range orchs {
		for g := range perOrch {
			tr.wg.Add(1)
			go func() {
				defer tr.wg.Done()
				for i := 0; ; i++ {
					select {
					case <-tr.stop:
						return
					default:
					}
					nonce := fmt.Sprintf("tr-%s-%d-%d", o.name, g, i)
					t0 := time.Now()
					v, err := moProbe(o, host, nonce)
					d := time.Since(t0)
					tr.sent.Add(1)
					key := "ok"
					switch {
					case err != nil:
						key = moCode(err) + "/" + strings.TrimPrefix(reason(err).String(), "ERROR_REASON_")
						if reason(err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND && connect.CodeOf(err) != connect.CodeUnavailable {
							tr.routed.Add(1)
						}
					case v.Nonce != nonce:
						tr.wrong.Add(1)
						key = "WRONG-NONCE"
						t.Errorf("an answer for %s carries nonce %q", nonce, v.Nonce)
					default:
						tr.ok.Add(1)
						tr.routed.Add(1)
					}
					tr.mu.Lock()
					tr.codes[key]++
					tr.lat = append(tr.lat, d)
					if err != nil {
						tr.failAt = append(tr.failAt, time.Now())
					}
					tr.mu.Unlock()
					time.Sleep(2 * time.Millisecond)
				}
			}()
		}
	}
	return tr
}

func (tr *moTraffic) end() { close(tr.stop); tr.wg.Wait() }

func (tr *moTraffic) summary() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var out []string
	for k, v := range tr.codes {
		out = append(out, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// moControlProbe is what an operator doing everyday reads and writes at one
// orchestrator sees: it returns the codes, with the time of each.
type moControlLog struct {
	mu   sync.Mutex
	errs []moCtlErr
	n    atomic.Int64
}

type moCtlErr struct {
	at   time.Time
	code string
	msg  string
	took time.Duration
}

func moControlLoop(o *moOrch, fid string, stop <-chan struct{}, wg *sync.WaitGroup, log *moControlLog) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			t0 := time.Now()
			var err error
			if i%2 == 0 {
				_, err = o.control().SetEnv(cctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "W" + o.name, Value: fmt.Sprint(i)}})
			} else {
				_, err = o.control().ListFunctions(cctx, &controlv1.ListFunctionsRequest{PageSize: 5})
			}
			cancel()
			log.n.Add(1)
			if err != nil {
				log.mu.Lock()
				log.errs = append(log.errs, moCtlErr{time.Now(), moCode(err), err.Error(), time.Since(t0)})
				log.mu.Unlock()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
}

func (l *moControlLog) codes() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := map[string]int{}
	for _, e := range l.errs {
		m[e.code+": "+e.msg]++
	}
	return m
}

func (l *moControlLog) lastError() (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.errs) == 0 {
		return time.Time{}, false
	}
	return l.errs[len(l.errs)-1].at, true
}

type moDBResult struct {
	routed   int
	records  int
	lost     int // log lines "dropping invocation records"
	logCount map[string]int
}

// moTerminateScenario kills every orchestrator database connection, six times
// under traffic and control writes, and returns what became of the records.
func moTerminateScenario(t *testing.T) moDBResult {
	c := newMoCluster(t, moOpts{n: 3, refresh: 200 * time.Millisecond})
	moStartRecWorker(t, c.A().url())
	fid := moRecFn(t, c, "dbt", "dbt.test")
	for _, o := range c.orchs {
		o := o
		moEventually(t, "dbt to be served at "+o.name, 20*time.Second, func() bool { _, err := moProbe(o, "dbt.test", "w"); return err == nil })
	}
	// The warm-up's own routed requests are recorded; count them.
	warm := c.records()
	moEventually(t, "the warm-up's records", 5*time.Second, func() bool { return c.records() >= 3 })
	warm = c.records()

	tr := moStartTraffic(t, c.orchs, "dbt.test", 2)
	stop := make(chan struct{})
	var cwg sync.WaitGroup
	logs := make([]*moControlLog, 3)
	for i, o := range c.orchs {
		logs[i] = &moControlLog{}
		moControlLoop(o, fid, stop, &cwg, logs[i])
	}
	time.Sleep(time.Second)
	var killed int
	var lastKill time.Time
	for range 6 {
		killed += c.terminate("mo-orch-%")
		lastKill = time.Now()
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("%d database sessions of the orchestrators terminated in 6 rounds", killed)
	if killed < 6 {
		t.Fatalf("only %d sessions were there to terminate", killed)
	}
	time.Sleep(3 * time.Second)
	close(stop)
	cwg.Wait()
	tr.end()

	moEventually(t, "records to settle", 10*time.Second, func() bool {
		a := c.records()
		time.Sleep(600 * time.Millisecond)
		return a == c.records()
	})
	settled := c.records()
	t.Logf("invocations during it: %d sent, outcomes: %s", tr.sent.Load(), tr.summary())
	if tr.ok.Load() != tr.sent.Load() {
		t.Errorf("%d of %d invocations failed while database connections were being killed: the invoke path serves from memory and should not notice (%s)", tr.sent.Load()-tr.ok.Load(), tr.sent.Load(), tr.summary())
	}
	for i, l := range logs {
		t.Logf("control calls at %s: %d made, errors: %v", strings.ToUpper(c.orchs[i].name), l.n.Load(), l.codes())
		if at, bad := l.lastError(); bad && at.After(lastKill.Add(3*time.Second)) {
			t.Errorf("orchestrator %s was still failing control calls %v after the last kill", c.orchs[i].name, at.Sub(lastKill))
		}
		for _, e := range l.errs {
			// What an operator is told while a connection dies is not specified:
			// the table in docs/protocol.md has no code for it. Record it.
			if e.code != "internal" && e.code != "unavailable" && e.code != "aborted" {
				t.Errorf("control call at %s failed with %s: %s", c.orchs[i].name, e.code, e.msg)
			}
		}
	}

	// Recovered, with no restart: everything works, and a change at A reaches B and C.
	for _, o := range c.orchs {
		if got := moHealthz(o); got != 200 {
			t.Errorf("orchestrator %s /healthz = %d after recovery", o.name, got)
		}
	}
	moSetEnv(t, c.A().control(), fid, "AFTER", "recovery")
	for _, o := range c.orchs[1:] {
		o := o
		d := moEventually(t, "a change made at A after the kills to reach "+o.name, 10*time.Second, func() bool {
			v, err := moProbe(o, "dbt.test", "r")
			return err == nil && v.Env["AFTER"] == "recovery"
		})
		if d > moBound {
			t.Errorf("after recovery a change took %v to reach %s", d, o.name)
		}
	}
	for _, o := range c.orchs {
		if n := c.conns("mo-orch-" + o.name); n > 40 {
			t.Errorf("orchestrator %s holds %d database connections after recovery", o.name, n)
		}
	}
	res := moDBResult{routed: int(tr.routed.Load()) + warm, records: settled, logCount: map[string]int{}}
	for _, o := range c.orchs {
		res.lost += o.logs.n("dropping invocation records: the store keeps failing")
		for _, msg := range []string{"snapshot refresh failed; serving the previous view", "store error"} {
			res.logCount[msg] += o.logs.n(msg)
		}
	}
	t.Logf("records: %d routed invocations, %d records in the database; log: %v, batches dropped: %d", res.routed, res.records, res.logCount, res.lost)
	return res
}

func TestMultiOrchDatabaseConnectionsTerminated(t *testing.T) {
	res := moTerminateScenario(t)
	// Killed connections lose no record (the second attempt of a batch finds a live one),
	// and duplicate none (the insert is keyed by the invocation id).
	if res.records != res.routed {
		t.Errorf("%d routed invocations, %d records (batches dropped according to the log: %d)", res.routed, res.records, res.lost)
	}
}

// (A database that is away for longer does lose records: see
// TestMultiOrchRecordsSurviveDatabaseOutage, known defect MO-1.)

// moOutage runs the database away and back, through a proxy that can refuse or
// stall the orchestrators' connections.
func moOutageCluster(t *testing.T) (*moCluster, *moProxy) {
	var once sync.Once
	var proxy *moProxy
	names := "abc"
	c := newMoCluster(t, moOpts{n: 3, refresh: 200 * time.Millisecond, dsnFor: func(i int, base string) string {
		once.Do(func() { proxy = newMoProxy(t, func() []moBackend { return []moBackend{moPG(base)} }) })
		return moDSNApp(moDSNVia(base, proxy.Addr()), "mo-orch-"+string(names[i]))
	}})
	return c, proxy
}

func TestMultiOrchDatabaseOutage(t *testing.T) {
	c, proxy := moOutageCluster(t)
	moStartRecWorker(t, c.A().url())
	fid := moRecFn(t, c, "outage", "outage.test")
	for _, o := range c.orchs {
		o := o
		moEventually(t, "outage.test to be served at "+o.name, 20*time.Second, func() bool { _, err := moProbe(o, "outage.test", "w"); return err == nil })
	}
	tr := moStartTraffic(t, c.orchs, "outage.test", 2)
	time.Sleep(500 * time.Millisecond)

	// 1. The database refuses connections for four seconds.
	proxy.Down()
	tDown := time.Now()
	var health [3]int
	moEventually(t, "/healthz to report the database down on every orchestrator", 15*time.Second, func() bool {
		ok := true
		for i, o := range c.orchs {
			health[i] = moHealthz(o)
			ok = ok && health[i] == 503
		}
		return ok
	})
	t.Logf("/healthz reported the database unavailable on all three %v after it went away", time.Since(tDown).Round(time.Millisecond))
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t0 := time.Now()
	_, rerr := c.A().control().ListFunctions(cctx, &controlv1.ListFunctionsRequest{})
	_, werr := c.A().control().SetEnv(cctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "DOWN", Value: "1"}})
	cancel()
	t.Logf("control read with the database down: %v; control write: %v (both took %v)", moCode(rerr), moCode(werr), time.Since(t0).Round(time.Millisecond))
	if rerr == nil || werr == nil {
		t.Errorf("a control call succeeded with the database down (read %v, write %v)", rerr, werr)
	}
	time.Sleep(4*time.Second - time.Since(tDown))
	invDuringDown := tr.sent.Load()
	proxy.Up()
	tUp := time.Now()
	moEventually(t, "control calls to work again at every orchestrator", 20*time.Second, func() bool {
		for _, o := range c.orchs {
			if _, err := o.control().ListFunctions(context.Background(), &controlv1.ListFunctionsRequest{PageSize: 1}); err != nil {
				return false
			}
		}
		return true
	})
	t.Logf("control reads worked again at all three %v after the database returned", time.Since(tUp).Round(time.Millisecond))

	// The worker's heartbeats could not be written while the database was away, so it looks
	// dead until the next one lands (1s here): the brownout. Let it pass before the next
	// phase, or the stall below would only prolong it.
	moEventually(t, "the worker to be usable again at every orchestrator", 20*time.Second, func() bool {
		for _, o := range c.orchs {
			if _, err := moProbe(o, "outage.test", "after-up"); err != nil {
				return false
			}
		}
		return true
	})

	// 2. The database stalls: connections open, nothing answers.
	proxy.Blackhole(true)
	tHole := time.Now()
	time.Sleep(300 * time.Millisecond)
	cctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_, herr := c.B().control().ListFunctions(cctx, &controlv1.ListFunctionsRequest{})
	cancel()
	t.Logf("control read with the database stalled, 2s client deadline: %s after %v", moCode(herr), time.Since(tHole).Round(time.Millisecond))
	if herr == nil {
		t.Error("a control call succeeded while the database stalled")
	}
	time.Sleep(3 * time.Second)
	proxy.Blackhole(false)
	proxy.Cut()
	tBack := time.Now()
	moEventually(t, "control calls to work after the stall", 30*time.Second, func() bool {
		for _, o := range c.orchs {
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := o.control().ListFunctions(cctx, &controlv1.ListFunctionsRequest{PageSize: 1})
			cancel()
			if err != nil {
				return false
			}
		}
		return true
	})
	t.Logf("control reads worked again at all three %v after the stall ended", time.Since(tBack).Round(time.Millisecond))
	time.Sleep(time.Second)
	tr.end()

	t.Logf("invocations over the whole run: %d sent, outcomes: %s", tr.sent.Load(), tr.summary())
	t.Logf("(%d were sent during the first four seconds of the outage)", invDuringDown)
	// What may fail: the moment after the database returns (or the stall ends), when
	// every worker's last heartbeat is older than the liveness window, so the first
	// refresh finds no live worker (liveness is the database's clock, and the
	// database could not be told). Nothing may fail while it is away.
	for code, n := range tr.codes {
		if code != "ok" && code != "resource_exhausted/NO_CAPACITY" {
			t.Errorf("%d invocations ended %s: with the database gone the invoke path may only be told NO_CAPACITY, and only after it returns", n, code)
		}
	}
	tr.mu.Lock()
	var brown []time.Duration
	for _, f := range tr.failAt {
		switch {
		case f.After(tDown) && f.Before(tUp.Add(-100*time.Millisecond)), f.After(tHole) && f.Before(tBack.Add(-100*time.Millisecond)):
			t.Errorf("an invocation failed while the database was away: the invoke path must keep serving from its last view")
		case f.After(tBack):
			brown = append(brown, f.Sub(tBack))
		case f.After(tUp):
			brown = append(brown, f.Sub(tUp))
		}
	}
	slow := tr.lat.max()
	tr.mu.Unlock()
	if len(brown) > 0 {
		sort.Slice(brown, func(i, j int) bool { return brown[i] < brown[j] })
		t.Logf("BROWNOUT after the database returned: %d failed invocations, %v to %v after it was back (a worker looks dead until its next heartbeat lands and the next refresh reads it; heartbeat 1s here, 5s by default)",
			len(brown), brown[0].Round(time.Millisecond), brown[len(brown)-1].Round(time.Millisecond))
		if brown[len(brown)-1] > 3*time.Second {
			t.Errorf("invocations were still failing %v after the database returned", brown[len(brown)-1])
		}
	}
	t.Logf("slowest invocation of the run: %v", slow.Round(time.Millisecond))
	if slow > 5*time.Second {
		t.Errorf("an invocation took %v: the invoke path must not wait for the database", slow)
	}
	// A change made after all of it reaches everyone.
	moSetEnv(t, c.A().control(), fid, "AFTER", "outage")
	for _, o := range c.orchs[1:] {
		o := o
		moEventually(t, "a change made at A after the outage to reach "+o.name, 10*time.Second, func() bool {
			v, err := moProbe(o, "outage.test", "r")
			return err == nil && v.Env["AFTER"] == "outage"
		})
	}
	for _, o := range c.orchs {
		if got := moHealthz(o); got != 200 {
			t.Errorf("orchestrator %s /healthz = %d after recovery", o.name, got)
		}
	}
	routed := int(tr.routed.Load())
	moEventually(t, "records to settle", 10*time.Second, func() bool {
		a := c.records()
		time.Sleep(600 * time.Millisecond)
		return a == c.records()
	})
	t.Logf("RECORDS: %d routed invocations (warm-ups excluded), %d records in the database: %d lost (known defect MO-1)", routed, c.records()-3, routed-(c.records()-3))
}

// KNOWN DEFECT (same as MO-1): records of the invocations served while the
// database is away are lost.
func TestMultiOrchRecordsSurviveDatabaseOutage(t *testing.T) {
	if os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		t.Skip("KNOWN DEFECT MO-1: invocation records are dropped after two failed attempts 100ms apart, so a database outage loses every record written during it")
	}
	c, proxy := moOutageCluster(t)
	moStartRecWorker(t, c.A().url())
	moRecFn(t, c, "outage", "outage.test")
	for _, o := range c.orchs {
		o := o
		moEventually(t, "served", 20*time.Second, func() bool { _, err := moProbe(o, "outage.test", "w"); return err == nil })
	}
	moEventually(t, "warm-up records", 5*time.Second, func() bool { return c.records() >= 3 })
	warm := c.records()
	tr := moStartTraffic(t, c.orchs, "outage.test", 2)
	time.Sleep(500 * time.Millisecond)
	proxy.Down()
	time.Sleep(3 * time.Second)
	proxy.Up()
	time.Sleep(2 * time.Second)
	tr.end()
	moEventually(t, "records to settle", 20*time.Second, func() bool {
		a := c.records()
		time.Sleep(600 * time.Millisecond)
		return a == c.records()
	})
	if got, want := c.records(), int(tr.routed.Load())+warm; got != want {
		t.Errorf("%d routed invocations, %d records: %d were lost to a three second database outage", want, got, want-got)
	}
}

// What a worker's orchestrator URL means. The worker is given one URL and
// registers, heartbeats and fetches artifacts through it. A worker only counts
// as alive to every orchestrator because ITS orchestrator wrote its heartbeat
// to the shared database.
func TestMultiOrchWorkerOrchestratorURL(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, heartbeat: 500 * time.Millisecond})
	fixed := c.startWorker(t, 4, moOnly(c.A()))                      // dials A's own address
	balanced := c.startWorker(t, 4, moFailover(c.A(), c.B(), c.C())) // dials an endpoint that fails over
	_ = fixed
	moWaitAlive(t, c.C(), 2, 60*time.Second)
	ctl := c.B().control()
	fid := moCreateFn(t, ctl, "warm")
	moDeploy(t, ctl, fid, "wagi-1", c.wasm)
	moSetEnv(t, ctl, fid, "FN", "warm")
	moRoute(t, ctl, "warm.test", "/", fid)
	for _, o := range c.orchs {
		o := o
		moEventually(t, "warm to be served at "+o.name, 90*time.Second, func() bool {
			r := moInvoke(o.ingress(), "warm.test", "/", "w-"+o.name, 0, 30*time.Second)
			return r.err == nil && r.status == 200
		})
	}

	aliveSet := func(o *moOrch) (map[string]bool, error) {
		r, err := o.control().ListWorkers(context.Background(), &controlv1.ListWorkersRequest{})
		if err != nil {
			return nil, err
		}
		m := map[string]bool{}
		for _, w := range r.Workers {
			m[w.Endpoint] = w.Alive && w.State == workerv1.WorkerState_WORKER_STATE_READY
		}
		return m, nil
	}
	fixedEP, balancedEP := "http://"+fixed.execP.Addr(), "http://"+balanced.execP.Addr()

	// Twelve functions whose modules no worker has seen, each with a route. The
	// last route made (the sentinel) is a function that is warm: once C routes it,
	// C has the others' routes too (one snapshot).
	ctl2 := c.B().control()
	for i := range 8 {
		f := moCreateFn(t, ctl2, fmt.Sprint("cn", i))
		moDeploy(t, ctl2, f, "wagi-1", moDistinct(c.wasm, fmt.Sprint("cold-", i)))
		moSetEnv(t, ctl2, f, "FN", fmt.Sprint("cn", i))
		moRoute(t, ctl2, fmt.Sprintf("cn%d.test", i), "/", f)
	}
	sentinel := moCreateFn(t, ctl2, "sentinel")
	moDeploy(t, ctl2, sentinel, "wagi-1", c.wasm)
	moSetEnv(t, ctl2, sentinel, "FN", "sentinel")
	moRoute(t, ctl2, "sentinel.test", "/", sentinel)
	moEventually(t, "the sentinel route at C", 20*time.Second, func() bool {
		r := moInvoke(c.C().ingress(), "sentinel.test", "/", "s", 0, 30*time.Second)
		return r.err == nil && r.status == 200
	})

	// A dies. Sample the fleet as C sees it.
	c.A().kill()
	tKill := time.Now()
	// Meanwhile requests for modules that neither worker has yet, at C: the worker that
	// dials A can no longer fetch one, but C goes on offering it work until A's silence
	// has aged its heartbeat out.
	var fixedDown, balancedGap time.Duration
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		lastBalancedUp := tKill
		for time.Since(tKill) < 5*time.Second {
			m, err := aliveSet(c.C())
			if err == nil {
				if !m[fixedEP] && fixedDown == 0 {
					fixedDown = time.Since(tKill)
				}
				if m[balancedEP] {
					balancedGap = max(balancedGap, time.Since(lastBalancedUp))
					lastBalancedUp = time.Now()
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	var coldOK, coldFail int
	coldErrs := map[string]int{}
	var coldLast time.Duration
	for i := range 8 {
		r := moInvoke(c.C().ingress(), fmt.Sprintf("cn%d.test", i), "/", fmt.Sprint("cold", i), 0, 30*time.Second)
		if r.err == nil && r.status == 200 && r.f("fn") == fmt.Sprint("cn", i) {
			coldOK++
		} else {
			coldFail++
			coldLast = time.Since(tKill)
			coldErrs[fmt.Sprintf("%s/%s", moCode(r.err), strings.TrimPrefix(reason(r.err).String(), "ERROR_REASON_"))]++
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Logf("8 first-time invocations at C while A was down, one every 150ms: %d ok, %d failed (last failure %v after the kill): %v", coldOK, coldFail, coldLast.Round(time.Millisecond), coldErrs)
	if coldFail > 0 && os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") != "" {
		t.Errorf("KNOWN DEFECT MO-5: %d invocations of a module the worker had not yet fetched failed with %v because the worker's orchestrator was down, although another worker could have run them and the call cannot have run", coldFail, coldErrs)
	}
	sampler.Wait()
	m, _ := aliveSet(c.C())
	t.Logf("A killed. As C sees the fleet 5s later: worker dialing A's address alive=%v (dropped out after %v); worker dialing the failover endpoint alive=%v (longest stretch it was not alive: %v)",
		m[fixedEP], fixedDown.Round(time.Millisecond), m[balancedEP], balancedGap.Round(time.Millisecond))
	if !m[balancedEP] {
		t.Error("the worker behind the failover endpoint is no longer alive although B and C are up")
	}
	// A characterisation, not a wish: the docs must say it. The worker is healthy
	// and reachable by B and C, yet it is out of the fleet for everyone because
	// its only way to heartbeat was A.
	if m[fixedEP] {
		t.Error("the worker that dials A's address is still alive 5s after A died: this test's picture of how liveness works is wrong")
	}

	// A module neither worker has: the survivor must fetch it, through its URL.
	ctl = c.C().control()
	cold := moCreateFn(t, ctl, "cold")
	moDeploy(t, ctl, cold, "wagi-1", moDistinct(c.wasm, "cold-module"))
	moSetEnv(t, ctl, cold, "FN", "cold")
	moRoute(t, ctl, "cold.test", "/", cold)
	moEventually(t, "a never-seen module to run on the worker behind the failover endpoint", 90*time.Second, func() bool {
		r := moInvoke(c.B().ingress(), "cold.test", "/", "cold-b", 0, 30*time.Second)
		return r.err == nil && r.status == 200 && r.f("fn") == "cold"
	})

	// A comes back, at the same address, as a new process on the same database.
	if err := c.A().restart(); err != nil {
		t.Fatal(err)
	}
	d := moEventually(t, "the worker that dials A to be alive again", 20*time.Second, func() bool {
		m, err := aliveSet(c.C())
		return err == nil && m[fixedEP]
	})
	t.Logf("A restarted at its address: the worker that dials it was alive again after %v, without being restarted", d.Round(time.Millisecond))
	moEventually(t, "both workers alive and serving at A", 20*time.Second, func() bool {
		r := moInvoke(c.A().ingress(), "cold.test", "/", "cold-a", 0, 30*time.Second)
		return r.err == nil && r.status == 200
	})
}

// A secret is sealed with the orchestrator's key. Orchestrators on one database
// must share it: with the same key a secret written through one is read through
// every other, and its revision is the same everywhere.
func TestMultiOrchSecretsAndRevisionsAgreeAcrossOrchestrators(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, refresh: moRefresh})
	moStartRecWorker(t, c.A().url())
	fid := moRecFn(t, c, "sec", "sec.test")
	bg := context.Background()
	r, err := c.A().control().SetEnv(bg, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "TOKEN", Value: "s3cret", Secret: true}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range c.orchs {
		o := o
		l, err := o.control().ListEnv(bg, &controlv1.ListEnvRequest{FunctionId: fid})
		if err != nil || len(l.Entries) != 1 || l.Entries[0].Revision != r.Entry.Revision || l.Entries[0].Value != "" {
			t.Errorf("ListEnv at %s: %v %v (revision written at A: %s)", o.name, l, err, r.Entry.Revision)
		}
		v, err := o.control().SetEnv(bg, &controlv1.SetEnvRequest{FunctionId: fid, ValidateOnly: true, Entry: &controlv1.EnvEntry{Name: "TOKEN", Value: "s3cret", Secret: true}})
		if err != nil || v.Entry.Revision != r.Entry.Revision {
			t.Errorf("validate-only SetEnv at %s: revision %v err %v, want %s", o.name, v.GetEntry().GetRevision(), err, r.Entry.Revision)
		}
		moEventually(t, "the secret to be handed to the worker via "+o.name, 10*time.Second, func() bool {
			v, err := moProbe(o, "sec.test", "s")
			return err == nil && v.Env["TOKEN"] == "s3cret"
		})
	}
}

// KNOWN DEFECT MO-2: an orchestrator started with another secret key than the
// one the data in the database was sealed with starts without a word. It serves,
// and every function that has a secret fails with INTERNAL ("unreadable
// environment") on that instance only, ListEnv of such a function fails for the
// whole function (even its plain variables), and revisions it computes differ
// from every other instance's. The README says only that the key "seals secret
// variables" and that the orchestrator is stateless: nothing says every instance
// must be given the same key, and nothing checks it.
func TestMultiOrchWrongSecretKeyIsRefusedAtStart(t *testing.T) {
	if os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		t.Skip("KNOWN DEFECT MO-2: an orchestrator with a different --secret-key than the data's starts silently and then fails every function that has a secret, on that instance only")
	}
	c := newMoCluster(t, moOpts{n: 2, refresh: moRefresh})
	moStartRecWorker(t, c.A().url())
	fid := moRecFn(t, c, "sec", "sec.test")
	if _, err := c.A().control().SetEnv(context.Background(), &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "TOKEN", Value: "s3cret", Secret: true}}); err != nil {
		t.Fatal(err)
	}
	cfg := c.A().cfg
	cfg.SecretKey = bytes.Repeat([]byte{7}, len(cfg.SecretKey))
	var _ orchapp.Config = cfg
	rogue := &moOrch{c: c, name: "rogue", cfg: cfg, logs: &moLogs{}}
	err := rogue.start("127.0.0.1:0")
	if err == nil {
		defer rogue.stop()
		t.Fatal("an orchestrator whose secret key cannot open the database's secrets started: it will answer INTERNAL for every function with a secret and nobody was told")
	}
}
