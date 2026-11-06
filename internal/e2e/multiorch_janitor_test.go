package e2e

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// TestMultiOrchJanitorsAndRefreshRunTogether: every orchestrator runs the
// retention janitor and the cache refresh. With the janitors running every 40ms
// and the refresh every 50ms on all three, over more old records than one delete
// batch holds, while requests are served and the control API is written through
// all three: old records are gone, records within the retention and the ones just
// written are all there, silent workers and services are pruned only past their
// grace, and nothing else (functions, versions, artifacts, routes, environment,
// live workers) is touched.
func TestMultiOrchJanitorsAndRefreshRunTogether(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, refresh: 50 * time.Millisecond, retention: time.Hour, janitor: 40 * time.Millisecond})
	bg := context.Background()
	moStartRecWorker(t, c.A().url())
	fid := moRecFn(t, c, "live", "live.test")
	moSetEnv(t, c.A().control(), fid, "KEEP", "me")
	liveRecords := 0 // the warm-up's own requests are recorded too (all that were routed)
	for _, o := range c.orchs {
		moEventually(t, "the function to be served at "+o.name, 20*time.Second, func() bool {
			_, err := moProbe(o, "live.test", "w")
			if err == nil || reason(err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND {
				liveRecords++ // routed, so recorded, whatever became of it
			}
			return err == nil
		})
	}
	before := struct{ fns, vers, arts, routes, envs int }{
		c.count(`SELECT count(*) FROM functions`), c.count(`SELECT count(*) FROM function_versions`),
		c.count(`SELECT count(*) FROM artifacts`), c.count(`SELECT count(*) FROM routes`), c.count(`SELECT count(*) FROM env_vars`)}

	// The past: 12,000 records older than the retention (more than two janitor
	// batches of 5,000), 3,000 inside it; workers and services that fell silent
	// long ago (past the janitor's one hour) and ones that fell silent a while ago.
	mustExec := func(q string) {
		t.Helper()
		if _, err := c.admin.Exec(bg, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec(`INSERT INTO invocations (id, request_id, method, path, start_time)
		SELECT gen_random_uuid(), 'old-' || g, 'GET', '/', now() - interval '2 hours' - g * interval '1 millisecond' FROM generate_series(1, 12000) g`)
	mustExec(`INSERT INTO invocations (id, request_id, method, path, start_time)
		SELECT gen_random_uuid(), 'mid-' || g, 'GET', '/', now() - interval '30 minutes' - g * interval '1 millisecond' FROM generate_series(1, 3000) g`)
	mustExec(`INSERT INTO workers (endpoint, info, abis, max_concurrency, state, last_heartbeat)
		SELECT 'http://gone-' || g || '.invalid', '{}', '{wagi-1}', 1, 1, now() - interval '3 hours' FROM generate_series(1, 5) g`)
	mustExec(`INSERT INTO workers (endpoint, info, abis, max_concurrency, state, last_heartbeat)
		SELECT 'http://recent-' || g || '.invalid', '{}', '{wagi-1}', 1, 1, now() - interval '20 minutes' FROM generate_series(1, 5) g`)
	mustExec(`INSERT INTO network_services (endpoint, info, covers, state, last_heartbeat)
		SELECT 'http://gone-svc-' || g || '.invalid', '{}', '{*}', 1, now() - interval '3 hours' FROM generate_series(1, 3) g`)
	mustExec(`INSERT INTO network_services (endpoint, info, covers, state, last_heartbeat)
		SELECT 'http://recent-svc-' || g || '.invalid', '{}', '{*}', 1, now() - interval '20 minutes' FROM generate_series(1, 3) g`)

	// While the janitors work: traffic through all three, and control writes
	// (which refresh the writer's cache) through all three.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var sent, failed, writes, writeErrs atomic.Int64
	for _, o := range c.orchs {
		for g := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					nonce := fmt.Sprintf("j-%s-%d-%d", o.name, g, i)
					v, err := moProbe(o, "live.test", nonce)
					sent.Add(1)
					if err != nil || v.Nonce != nonce || v.Env["KEEP"] != "me" {
						failed.Add(1)
						t.Errorf("%s: %v %+v", nonce, err, v)
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, err := o.control().SetEnv(bg, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "N" + o.name, Value: fmt.Sprint(i)}})
				writes.Add(1)
				if err != nil {
					writeErrs.Add(1)
					t.Errorf("SetEnv at %s: %v", o.name, err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	d := moEventually(t, "the old records, silent workers and services to be removed", 30*time.Second, func() bool {
		return c.count(`SELECT count(*) FROM invocations WHERE request_id LIKE 'old-%'`) == 0 &&
			c.count(`SELECT count(*) FROM workers WHERE endpoint LIKE 'http://gone-%'`) == 0 &&
			c.count(`SELECT count(*) FROM network_services WHERE endpoint LIKE 'http://gone-svc-%'`) == 0
	})
	t.Logf("12,000 old records, 5 silent workers and 3 silent services removed after %v (janitor every 40ms on 3 orchestrators)", d.Round(time.Millisecond))
	// Several more passes on every instance, with traffic still running.
	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := c.count(`SELECT count(*) FROM invocations WHERE request_id LIKE 'mid-%'`); n != 3000 {
		t.Errorf("%d of 3,000 records inside the retention remain", n)
	}
	if n := c.count(`SELECT count(*) FROM workers WHERE endpoint LIKE 'http://recent-%'`); n != 5 {
		t.Errorf("%d of 5 workers that fell silent 20 minutes ago remain (the grace is an hour)", n)
	}
	if n := c.count(`SELECT count(*) FROM network_services WHERE endpoint LIKE 'http://recent-svc-%'`); n != 3 {
		t.Errorf("%d of 3 services that fell silent 20 minutes ago remain", n)
	}
	moEventually(t, "every record of a served request", 20*time.Second, func() bool {
		return c.count(`SELECT count(*) FROM invocations WHERE function_id = $1`, fid) == int(sent.Load())+liveRecords
	})
	if n := c.count(`SELECT count(*) FROM invocations WHERE function_id = $1`, fid); n != int(sent.Load())+liveRecords {
		t.Errorf("%d records for %d requests: the janitors deleted records that were not old", n, int(sent.Load())+liveRecords)
	}
	after := struct{ fns, vers, arts, routes, envs int }{
		c.count(`SELECT count(*) FROM functions`), c.count(`SELECT count(*) FROM function_versions`),
		c.count(`SELECT count(*) FROM artifacts`), c.count(`SELECT count(*) FROM routes`), c.count(`SELECT count(*) FROM env_vars`)}
	// the writers added up to three environment variables (NA, NB, NC)
	if after.fns != before.fns || after.vers != before.vers || after.arts != before.arts || after.routes != before.routes || after.envs != before.envs+3 {
		t.Errorf("the janitor or the refresh changed what they must not: before %+v after %+v (+3 variables expected)", before, after)
	}
	if n := c.count(`SELECT count(*) FROM workers WHERE endpoint LIKE 'http://127.0.0.1:%'`); n != 1 {
		t.Errorf("the live worker is gone (%d)", n)
	}
	for _, o := range c.orchs {
		for _, w := range o.logs.warnings() {
			t.Errorf("orchestrator %s logged: %s", o.name, w)
		}
		if n := o.logs.n("janitor pass failed"); n != 0 {
			t.Errorf("orchestrator %s: %d janitor passes failed", o.name, n)
		}
	}
	t.Logf("%d requests served and %d control writes made through three orchestrators while they ran", sent.Load(), writes.Load())
	_ = strings.TrimSpace
}
