package e2e

// Plans and races across orchestrators: nobody sees a plan half applied, a name
// or a route has exactly one owner, and changes that deadlock answer aborted
// (docs/protocol.md, "Plans: validating and applying" and "Control semantics").

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

func moStep(req any, bind string) *controlv1.Step {
	s := &controlv1.Step{Bind: bind}
	switch r := req.(type) {
	case *controlv1.CreateFunctionRequest:
		s.Request = &controlv1.Step_CreateFunction{CreateFunction: r}
	case *controlv1.DeleteFunctionRequest:
		s.Request = &controlv1.Step_DeleteFunction{DeleteFunction: r}
	case *controlv1.CreateVersionRequest:
		s.Request = &controlv1.Step_CreateVersion{CreateVersion: r}
	case *controlv1.ActivateVersionRequest:
		s.Request = &controlv1.Step_ActivateVersion{ActivateVersion: r}
	case *controlv1.SetEnvRequest:
		s.Request = &controlv1.Step_SetEnv{SetEnv: r}
	case *controlv1.DeleteEnvRequest:
		s.Request = &controlv1.Step_DeleteEnv{DeleteEnv: r}
	case *controlv1.SetEgressRequest:
		s.Request = &controlv1.Step_SetEgress{SetEgress: r}
	case *controlv1.CreateRouteRequest:
		s.Request = &controlv1.Step_CreateRoute{CreateRoute: r}
	case *controlv1.DeleteRouteRequest:
		s.Request = &controlv1.Step_DeleteRoute{DeleteRoute: r}
	default:
		panic(fmt.Sprintf("moStep: %T", req))
	}
	return s
}

func moSetEnvStep(fid, name, value string) *controlv1.Step {
	return moStep(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: name, Value: value}}, "")
}

// TestMultiOrchNoHalfAppliedPlan hammers B and C (and A) with requests while A
// applies plans that move several things together: a function's version, its
// environment and its network grants; a route's target (a deletion and a
// creation); and a function that is deleted and created again with its version,
// environment and route. Every answer must be wholly the old state or wholly the
// new one. A plan that fails at its last step, and one that is only validated,
// must never be seen at all.
func TestMultiOrchNoHalfAppliedPlan(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, refresh: moRefresh})
	moStartRecWorker(t, c.A().url())
	netSvc := "http://127.0.0.1:1"
	moStartFakeNetwork(t, c.A().url(), netSvc)
	ctl := c.A().control()
	bg := context.Background()

	// swp: four versions, only 0 and 1 are ever applied for real.
	swp := moCreateFn(t, ctl, "swp")
	var vers [4]string
	for s := range vers {
		r, err := ctl.CreateVersion(bg, &controlv1.CreateVersionRequest{FunctionId: swp, Module: []byte(fmt.Sprint("swp-", s)), Abi: "rec-1"})
		if err != nil {
			t.Fatal(err)
		}
		vers[s] = r.Version.Id
	}
	moRoute(t, ctl, "swp.test", "/", swp)
	// flip: two functions, one route that is moved between them.
	var fx [2]string
	for i := range fx {
		fx[i] = moCreateFn(t, ctl, fmt.Sprint("fx", i))
		moDeploy(t, ctl, fx[i], "rec-1", []byte(fmt.Sprint("fx-", i)))
		moSetEnv(t, ctl, fx[i], "FN", fmt.Sprint("fx", i))
	}
	flipRoute := moRoute(t, ctl, "flip.test", "/", fx[0])
	// zed: deleted and created again, with everything, by each plan.
	zedID := moCreateFn(t, ctl, "zed")
	moDeploy(t, ctl, zedID, "rec-1", []byte("zed-0"))
	moSetEnv(t, ctl, zedID, "GEN", "0")
	moRoute(t, ctl, "zed.test", "/", zedID)

	swpSteps := func(s int) []*controlv1.Step {
		return []*controlv1.Step{
			moStep(&controlv1.ActivateVersionRequest{FunctionId: swp, VersionId: vers[s]}, ""),
			moSetEnvStep(swp, "STATE", fmt.Sprint(s)),
			moSetEnvStep(swp, "TAG", fmt.Sprint(s)),
			moStep(&controlv1.SetEgressRequest{FunctionId: swp, Allow: []string{fmt.Sprintf("s%d.example.com", s)}}, ""),
		}
	}
	moStep0 := swpSteps(0)
	if r, err := ctl.Apply(bg, &controlv1.ApplyRequest{Steps: moStep0}); err != nil || r.Failure != nil {
		t.Fatalf("initial plan: %v %v", err, r.GetFailure())
	}

	var gen int
	plan := func(s int, final ...*controlv1.Step) []*controlv1.Step {
		steps := swpSteps(s)
		steps = append(steps,
			moStep(&controlv1.DeleteRouteRequest{RouteId: flipRoute}, ""),
			moStep(&controlv1.CreateRouteRequest{HostPattern: "flip.test", PathPrefix: "/", FunctionId: fx[s%2]}, "flip"))
		gen++
		steps = append(steps,
			moStep(&controlv1.DeleteFunctionRequest{FunctionId: zedID}, ""),
			moStep(&controlv1.CreateFunctionRequest{Name: "zed"}, "z"),
			moStep(&controlv1.CreateVersionRequest{FunctionId: "$z", Module: []byte(fmt.Sprint("zed-", gen)), Abi: "rec-1", Activate: true}, ""),
			moSetEnvStep("$z", "GEN", fmt.Sprint(gen)),
			moStep(&controlv1.CreateRouteRequest{HostPattern: "zed.test", PathPrefix: "/", FunctionId: "$z"}, ""))
		return append(steps, final...)
	}

	// The routes and the worker reach B and C within a refresh; the test begins when they have.
	for _, o := range c.orchs {
		for _, host := range []string{"swp.test", "flip.test", "zed.test"} {
			moEventually(t, host+" to be served at "+o.name, 20*time.Second, func() bool { _, err := moProbe(o, host, "w"); return err == nil })
		}
	}

	// What an answer may be.
	okSwp := func(v moView) string {
		for s := range 2 {
			if v.Digest == store.Digest([]byte(fmt.Sprint("swp-", s))) {
				if v.Env["STATE"] == fmt.Sprint(s) && v.Env["TAG"] == fmt.Sprint(s) && len(v.Env) == 2 &&
					len(v.Egress) == 1 && v.Egress[0] == fmt.Sprintf("s%d.example.com>%s", s, netSvc) {
					return ""
				}
				return "a mix of states: " + v.String()
			}
		}
		return "unknown version: " + v.String()
	}
	okFlip := func(v moView) string {
		for i := range 2 {
			if v.Digest == store.Digest([]byte(fmt.Sprint("fx-", i))) {
				if v.Env["FN"] == fmt.Sprint("fx", i) && len(v.Env) == 1 && len(v.Egress) == 0 {
					return ""
				}
				return "a mix of functions: " + v.String()
			}
		}
		return "unknown version: " + v.String()
	}
	okZed := func(v moView) string {
		if len(v.Env) != 1 {
			return "no environment (or too much): " + v.String()
		}
		g := v.Env["GEN"]
		if g == "" || v.Digest != store.Digest([]byte("zed-"+g)) {
			return "a mix of generations: " + v.String()
		}
		return ""
	}

	// The writer: plans that commit, plans that fail at the very last step, and
	// validations, from A, for the whole run.
	stop := make(chan struct{})
	var wwg sync.WaitGroup
	var applied, refused, validated atomic.Int64
	wwg.Add(1)
	go func() {
		defer wwg.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%4 == 2 { // state 3, then a step that cannot be valid: nothing of it may ever be seen
				r, err := ctl.Apply(bg, &controlv1.ApplyRequest{Steps: plan(3, moSetEnvStep(swp, "NOT VALID", "x"))})
				if err != nil || r.Failure == nil || r.Failure.Code != "invalid_argument" || len(r.Results) != 0 {
					t.Errorf("a plan with an invalid last step: err=%v failure=%v results=%d", err, r.GetFailure(), len(r.GetResults()))
					return
				}
				refused.Add(1)
				gen-- // nothing was committed: the generation is not used
				continue
			}
			if i%4 == 3 { // validated, never applied
				r, err := ctl.Validate(bg, &controlv1.ValidateRequest{Steps: plan(3)})
				if err != nil || r.Failure != nil {
					t.Errorf("validate: %v %v", err, r.GetFailure())
					return
				}
				gen--
				validated.Add(1)
				continue
			}
			s := i % 2
			steps := plan(s)
			r, err := ctl.Apply(bg, &controlv1.ApplyRequest{Steps: steps})
			if err != nil || r.Failure != nil {
				t.Errorf("apply %d: %v %v", i, err, r.GetFailure())
				return
			}
			for j, st := range steps { // follow what the plan created
				switch st.Bind {
				case "flip":
					flipRoute = r.Results[j].GetCreateRoute().GetRoute().GetId()
				case "z":
					zedID = r.Results[j].GetCreateFunction().GetFunction().GetId()
				}
			}
			applied.Add(1)
		}
	}()

	type key struct{ who, dim, what string }
	var mu sync.Mutex
	seen := map[key]int{}
	var total atomic.Int64
	var owg sync.WaitGroup
	deadline := time.Now().Add(8 * time.Second)
	for _, o := range c.orchs {
		for g := range 3 {
			owg.Add(1)
			go func() {
				defer owg.Done()
				for i := 0; time.Now().Before(deadline); i++ {
					for _, d := range []struct {
						host string
						ok   func(moView) string
					}{{"swp.test", okSwp}, {"flip.test", okFlip}, {"zed.test", okZed}} {
						v, err := moProbe(o, d.host, fmt.Sprintf("%s%d-%d", o.name, g, i))
						total.Add(1)
						if err != nil {
							t.Errorf("%s at %s during a plan: %v (every answer must be the old state or the new one, never a gap)", d.host, o.name, err)
							return
						}
						if why := d.ok(v); why != "" {
							t.Errorf("%s at %s: %s", d.host, o.name, why)
							return
						}
						what := v.Env["STATE"] + v.Env["FN"]
						if d.host == "zed.test" {
							what = "gen"
						}
						mu.Lock()
						seen[key{o.name, d.host, what}]++
						mu.Unlock()
					}
				}
			}()
		}
	}
	owg.Wait()
	close(stop)
	wwg.Wait()

	t.Logf("plans applied at A: %d, refused at their last step: %d, validated only: %d; %d answers checked at A, B and C", applied.Load(), refused.Load(), validated.Load(), total.Load())
	var keys []string
	for k, n := range seen {
		keys = append(keys, fmt.Sprintf("%s/%s/%s=%d", k.who, k.dim, k.what, n))
	}
	sort.Strings(keys)
	t.Logf("states seen: %s", strings.Join(keys, " "))
	if applied.Load() < 8 {
		t.Errorf("only %d plans were applied: the test did not test much", applied.Load())
	}
	for _, o := range c.orchs {
		for _, want := range []string{"swp.test/0", "swp.test/1", "flip.test/fx0", "flip.test/fx1"} {
			dim, what, _ := strings.Cut(want, "/")
			if seen[key{o.name, dim, what}] == 0 {
				t.Errorf("%s never saw %s: the test never saw both sides of the change there", o.name, want)
			}
		}
	}
	for _, o := range c.orchs {
		for _, bad := range []string{"3"} {
			if seen[key{o.name, "swp.test", bad}] != 0 {
				t.Errorf("%s served state %s, which only a refused and a validated plan ever set", o.name, bad)
			}
		}
	}
}

func moFailCodes(errs []error) map[string]int {
	m := map[string]int{}
	for _, e := range errs {
		m[moCode(e)]++
	}
	return m
}

// TestMultiOrchRacingWriters: writers at three orchestrators race for one
// function name, one route and one version number. Exactly one wins each race;
// the others are told already_exists, and nothing is half made.
func TestMultiOrchRacingWriters(t *testing.T) {
	c := newMoCluster(t, moOpts{n: 3, refresh: moRefresh, deadlock: 50 * time.Millisecond})
	bg := context.Background()

	race := func(n int, f func(o *moOrch) error) (okN int, codes map[string]int) {
		errs := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range n {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; errs[i] = f(c.orchs[i%len(c.orchs)]) }()
		}
		close(start)
		wg.Wait()
		for _, e := range errs {
			if e == nil {
				okN++
			}
		}
		return okN, moFailCodes(errs)
	}

	t.Run("one function name", func(t *testing.T) {
		for round := range 40 {
			name := fmt.Sprint("race-fn-", round)
			ok, codes := race(6, func(o *moOrch) error {
				_, err := o.control().CreateFunction(bg, &controlv1.CreateFunctionRequest{Name: name})
				return err
			})
			if ok != 1 || codes["already_exists"] != 5 {
				t.Fatalf("round %d: %d writers won the name %s (want 1), outcomes %v (want 5 already_exists)", round, ok, name, codes)
			}
			if n := c.count(`SELECT count(*) FROM functions WHERE name = $1`, name); n != 1 {
				t.Fatalf("round %d: %d functions named %s", round, n, name)
			}
		}
	})

	t.Run("one route", func(t *testing.T) {
		const rounds = 30
		fns := make([][]string, rounds) // per round, a function per writer
		for r := range rounds {
			for i := range 6 {
				fns[r] = append(fns[r], moCreateFn(t, c.A().control(), fmt.Sprintf("race-rt-%d-%d", r, i)))
			}
		}
		for r := range rounds {
			var idx atomic.Int64
			var winner atomic.Int64
			winner.Store(-1)
			ok, codes := race(6, func(o *moOrch) error {
				i := int(idx.Add(1)) - 1
				_, err := o.control().CreateRoute(bg, &controlv1.CreateRouteRequest{HostPattern: "race.test", PathPrefix: fmt.Sprintf("/r%d", r), FunctionId: fns[r][i]})
				if err == nil {
					winner.Store(int64(i))
				}
				return err
			})
			if ok != 1 || codes["already_exists"] != 5 {
				t.Fatalf("round %d: %d writers won the route (want 1), outcomes %v", r, ok, codes)
			}
			var got string
			if err := c.admin.QueryRow(bg, `SELECT function_id::text FROM routes WHERE host_pattern='race.test' AND path_prefix=$1`, fmt.Sprintf("/r%d", r)).Scan(&got); err != nil || got != fns[r][winner.Load()] {
				t.Fatalf("round %d: the route points at %s (err %v), the winner's function is %s", r, got, err, fns[r][winner.Load()])
			}
		}
	})

	t.Run("one version number", func(t *testing.T) {
		fid := moCreateFn(t, c.A().control(), "race-versions")
		const writers, each = 9, 12
		var wg sync.WaitGroup
		var mu sync.Mutex
		var codes []error
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range each {
					_, err := c.orchs[i%3].control().CreateVersion(bg, &controlv1.CreateVersionRequest{FunctionId: fid, Abi: "rec-1", Module: []byte(fmt.Sprint("m", i, "-", j))})
					mu.Lock()
					codes = append(codes, err)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if m := moFailCodes(codes); m["ok"] != writers*each {
			t.Errorf("%d concurrent CreateVersion at three orchestrators: outcomes %v, want all ok", writers*each, m)
		}
		rows, err := c.admin.Query(bg, `SELECT number FROM function_versions WHERE function_id=$1 ORDER BY number`, fid)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		want := int64(1)
		for rows.Next() {
			var n int64
			rows.Scan(&n)
			if n != want {
				t.Fatalf("version numbers are not 1, 2, 3...: found %d where %d was due", n, want)
			}
			want++
		}
		if want != writers*each+1 {
			t.Errorf("%d versions numbered, want %d", want-1, writers*each)
		}
	})

	t.Run("plans for one name from two orchestrators", func(t *testing.T) {
		// Each plan first makes a route of its own and then takes the contested
		// name: the loser fails at step 1, and its route must not exist.
		for round := range 25 {
			name := fmt.Sprint("race-plan-", round)
			var res [2]*controlv1.ApplyResponse
			var errs [2]error
			var wg sync.WaitGroup
			start := make(chan struct{})
			owner := moCreateFn(t, c.A().control(), fmt.Sprint(name, "-owner"))
			for i := range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					res[i], errs[i] = c.orchs[i].control().Apply(bg, &controlv1.ApplyRequest{Steps: []*controlv1.Step{
						moStep(&controlv1.CreateRouteRequest{HostPattern: fmt.Sprintf("plan%d-%d.test", round, i), PathPrefix: "/", FunctionId: owner}, ""),
						moStep(&controlv1.CreateFunctionRequest{Name: name}, "f"),
						moStep(&controlv1.CreateVersionRequest{FunctionId: "$f", Module: []byte("m"), Abi: "rec-1", Activate: true}, ""),
					}})
				}()
			}
			close(start)
			wg.Wait()
			wins, loser := 0, -1
			for i := range 2 {
				switch {
				case errs[i] != nil:
					if connect.CodeOf(errs[i]) != connect.CodeAborted {
						t.Fatalf("round %d plan %d: %v", round, i, errs[i])
					}
					loser = i
				case res[i].Failure != nil:
					if res[i].Failure.Code != "already_exists" || res[i].Failure.Step != 1 || len(res[i].Results) != 0 {
						t.Fatalf("round %d plan %d: failure %v results %d, want already_exists at step 1 and no results", round, i, res[i].Failure, len(res[i].Results))
					}
					loser = i
				default:
					wins++
				}
			}
			if wins != 1 {
				t.Fatalf("round %d: %d plans took the name %s, want exactly 1 (errors %v)", round, wins, name, errs)
			}
			if n := c.count(`SELECT count(*) FROM routes WHERE host_pattern = $1`, fmt.Sprintf("plan%d-%d.test", round, loser)); n != 0 {
				t.Fatalf("round %d: the plan that lost the name left its route behind (half applied)", round)
			}
			if n := c.count(`SELECT count(*) FROM routes WHERE host_pattern = $1`, fmt.Sprintf("plan%d-%d.test", round, 1-loser)); n != 1 {
				t.Fatalf("round %d: the winner's route is missing", round)
			}
			if n := c.count(`SELECT count(*) FROM function_versions v JOIN functions f ON f.id = v.function_id WHERE f.name = $1`, name); n != 1 {
				t.Fatalf("round %d: %d versions of %s", round, n, name)
			}
		}
	})

	t.Run("plans that touch the same functions in opposite orders", func(t *testing.T) {
		f1 := moCreateFn(t, c.A().control(), "opp-1")
		f2 := moCreateFn(t, c.A().control(), "opp-2")
		var codes sync.Map
		note := func(s string) {
			v, _ := codes.LoadOrStore(s, new(atomic.Int64))
			v.(*atomic.Int64).Add(1)
		}
		for round := range 40 {
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range 4 { // two plans from A and two from B, in opposite orders
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					val := fmt.Sprintf("p%d-%d", round, i)
					first, second := f1, f2
					if i%2 == 1 {
						first, second = f2, f1
					}
					r, err := c.orchs[i/2].control().Apply(bg, &controlv1.ApplyRequest{Steps: []*controlv1.Step{
						moSetEnvStep(first, "STATE", val), moSetEnvStep(second, "STATE", val)}})
					switch {
					case err != nil:
						note(moCode(err))
					case r.Failure != nil:
						note("failure:" + r.Failure.Code)
						t.Errorf("a plan of two SetEnv failed: %v", r.Failure)
					default:
						note("ok")
					}
				}()
			}
			close(start)
			wg.Wait()
			// Whatever won, each plan wrote the same value to both: they must agree.
			var a, b string
			c.admin.QueryRow(bg, `SELECT convert_from(value,'UTF8') FROM env_vars WHERE function_id=$1 AND name='STATE'`, f1).Scan(&a)
			c.admin.QueryRow(bg, `SELECT convert_from(value,'UTF8') FROM env_vars WHERE function_id=$1 AND name='STATE'`, f2).Scan(&b)
			if a != b {
				t.Fatalf("round %d: opp-1 has STATE=%q and opp-2 has STATE=%q: two plans interleaved (half applied)", round, a, b)
			}
		}
		var out []string
		codes.Range(func(k, v any) bool { out = append(out, fmt.Sprintf("%v=%d", k, v.(*atomic.Int64).Load())); return true })
		sort.Strings(out)
		t.Logf("160 plans, two orchestrators, opposite orders: %s", strings.Join(out, " "))
		codes.Range(func(k, v any) bool {
			if k != "ok" && k != "aborted" {
				t.Errorf("outcome %v (%d times): only ok and aborted are acceptable", k, v.(*atomic.Int64).Load())
			}
			return true
		})
	})
}
