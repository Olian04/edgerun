package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// Tests of what the service gets from the control.v1 features it was migrated to
// (PlanService.Validate, the label patches, the list filters): each states what
// the old workaround could not do, and what the feature does.

// envYAML is a function block with n plain variables named prefix_00...
func envYAML(name, module, prefix string, n int, extra string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  - name: %s\n    module: %s\n%s    env:\n", name, module, extra)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "      %s_%02d: v%d\n", prefix, i, i)
	}
	return b.String()
}

func validateSteps(t *testing.T, l *rpcLog, n int) []*controlv1.Step {
	t.Helper()
	r, ok := l.Request("Validate", n).(*controlv1.ValidateRequest)
	if !ok {
		t.Fatalf("no Validate call number %d", n)
	}
	return r.Steps
}

// New variables that only fit once stale ones are deleted. A validate-only call per
// variable cannot see that: each one fits beside the 60 that are stored, and the
// 5th does not fit beside the 4 before it. The plan is validated as the sequence the
// apply runs (deletions, then writes), which is the only way to be right in both
// directions.
func TestSixtyFourVariableSwap(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	yaml := func(prefix string, n int, module string) string {
		return "functions:\n" + envYAML("big", module, prefix, n, "") + "routes:\n  - {host: big.test, path: /, function: big}\n"
	}
	h.R.Commit("old", files("m1.wasm", env.guest["v1"], "m2.wasm", env.guest["v2"], "edgerun.yaml", yaml("OLD", 60, "m1.wasm")))
	mustRun(t, h)

	// The protocol, directly: five new variables beside 60 stored ones. One at a time
	// every one is accepted; as a sequence the fifth is refused, and with deletions
	// first they all are.
	fn := h.S.functions(t)["big"]
	set := func(i int) *controlv1.Step {
		return &controlv1.Step{Request: &controlv1.Step_SetEnv{SetEnv: &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: fmt.Sprintf("NEW_%02d", i), Value: "x"}}}}
	}
	for i := 0; i < 5; i++ {
		_, err := h.S.Ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: fmt.Sprintf("NEW_%02d", i), Value: "x"}, ValidateOnly: true})
		if err != nil {
			t.Fatalf("a lone validate-only SetEnv beside 60 variables is fine: %v", err)
		}
	}
	res, err := h.S.Ctl.Validate(ctx, &controlv1.ValidateRequest{Steps: []*controlv1.Step{set(0), set(1), set(2), set(3), set(4)}})
	if err != nil || res.Failure == nil || res.Failure.Step != 4 || res.Failure.Code != "invalid_argument" || len(res.Results) != 4 {
		t.Fatalf("the 5th new variable must be refused as a sequence: %v %v", err, res)
	}
	t.Logf("the limit, as a sequence: step %d, %s: %s", res.Failure.Step, res.Failure.Code, res.Failure.Message)
	if n := len(h.S.listEnv(t, fn.Id)); n != 60 {
		t.Fatalf("a validation changed the function: %d variables", n)
	}

	// The service: all 60 replaced.
	h.R.Commit("new", files("edgerun.yaml", yaml("NEW", 60, "m1.wasm")))
	l := h.LogRPC()
	_, err, out := h.Run(true)
	if err != nil || strings.Contains(out, "not validated ahead") || strings.Count(out, "- big: env OLD_") != 60 {
		t.Fatalf("%v\n%s", err, out)
	}
	if l.Count("Validate") != 1 || l.ValidateOnlyCalls() != 0 {
		t.Fatalf("%d Validate, %d single validate-only calls", l.Count("Validate"), l.ValidateOnlyCalls())
	}
	steps := validateSteps(t, l, 0)
	lastDel, firstSet := -1, len(steps)
	for i, st := range steps {
		if st.GetDeleteEnv() != nil {
			lastDel = i
		}
		if st.GetSetEnv() != nil && i < firstSet {
			firstSet = i
		}
	}
	if len(steps) != 120 || lastDel >= firstSet {
		t.Fatalf("want 60 deletions then 60 writes, got %d steps (last delete %d, first write %d)", len(steps), lastDel, firstSet)
	}
	mustRun(t, h)
	mustBeInSync(t, h)
	got := h.S.mustInvoke(t, "big.test", "/env")
	if strings.Contains(got, "OLD_") || strings.Count(got, "NEW_") != 60 {
		t.Fatalf("the guest sees: %s", got)
	}

	// The most there can be: all 64 replaced is 64 + 64 steps, which is exactly what
	// one Validate takes; with a new version as well it is 129. The first 128 are
	// validated and the report says that the last one is not.
	h.R.Commit("64", files("edgerun.yaml", yaml("P", 64, "m1.wasm")))
	mustRun(t, h)
	h.R.Commit("64 swapped", files("edgerun.yaml", yaml("Q", 64, "m1.wasm")))
	_, err, out = h.Run(true)
	if err != nil || strings.Contains(out, "not validated ahead") {
		t.Fatalf("128 steps are one Validate:\n%v\n%s", err, out)
	}
	h.R.Commit("64 swapped, new module", files("edgerun.yaml", yaml("R", 64, "m2.wasm")))
	_, err, out = h.Run(true)
	if err != nil || strings.Count(out, "not validated ahead") != 1 || !strings.Contains(out, "+ big: upload new version") {
		t.Fatalf("129 steps: the last one must be reported as not validated:\n%v\n%s", err, out)
	}
	mustRun(t, h)
	mustBeInSync(t, h)
	if got := h.S.mustInvoke(t, "big.test", "/"); !strings.Contains(got, "version=v2") {
		t.Fatal(got)
	}
}

// The service never replaces a label set: it patches its own keys. What other
// tools put on a function or a route survives a deploy, a rotation of a secret
// (which rewrites the record in the labels) and a write by another tool that lands
// between the service's read and its write.
func TestPatchPreservesForeignLabels(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ctl := h.S.Ctl
	h.Env["K"] = "one"
	l := h.LogRPC()
	yaml := func(module, extra string) string {
		return "functions:\n  - {name: echo, module: " + module + ", secret_env: {API_KEY: K}" + extra + "}\nroutes:\n  - {host: echo.test, path: /, function: echo}\n"
	}
	h.R.Commit("1", files("m.wasm", env.guest["v1"], "m2.wasm", env.guest["v2"], "edgerun.yaml", yaml("m.wasm", "")))
	mustRun(t, h)

	// Another tool marks the function and the route.
	fn := h.S.functions(t)["echo"]
	if _, err := ctl.PatchLabels(ctx, &controlv1.PatchLabelsRequest{FunctionId: fn.Id, Set: map[string]string{"team": "payments", "other.example.com/note": "keep"}}); err != nil {
		t.Fatal(err)
	}
	rt := h.S.routes(t)["echo.test/"]
	if _, err := ctl.PatchRouteLabels(ctx, &controlv1.PatchRouteLabelsRequest{RouteId: rt.Id, Set: map[string]string{"team": "payments"}}); err != nil {
		t.Fatal(err)
	}
	foreign := func(when string) {
		t.Helper()
		f := h.S.functions(t)["echo"]
		if f.Labels["team"] != "payments" || f.Labels["other.example.com/note"] != "keep" || f.Labels[labelOwner] != "test" || len(decodeSecrets(f.Labels)) != 1 {
			t.Fatalf("%s: function labels %v", when, f.Labels)
		}
		if r := h.S.routes(t)["echo.test/"]; r.Labels["team"] != "payments" || r.Labels[labelOwner] != "test" {
			t.Fatalf("%s: route labels %v", when, r.Labels)
		}
	}
	foreign("another tool's labels")

	// A deploy: a new module, an env var, an egress list.
	h.R.Commit("2", files("edgerun.yaml", yaml("m2.wasm", ", env: {GREETING: hi}, egress: [a.example.com]")))
	mustRun(t, h)
	foreign("a deploy")

	// A secret is rotated: the record in the labels is rewritten.
	before := decodeSecrets(h.S.functions(t)["echo"].Labels)
	h.Env["K"] = "two"
	_, out := mustRun(t, h)
	if !strings.Contains(out, "labels: record the revisions") {
		t.Fatalf("%s", out)
	}
	if after := decodeSecrets(h.S.functions(t)["echo"].Labels); fmt.Sprint(after) == fmt.Sprint(before) {
		t.Fatalf("the record did not change: %v", after)
	}
	foreign("a rotation")

	// Another tool writes between the service's read and its write. With a SetLabels
	// built from the read this label would be lost; the patch names only our keys.
	var once sync.Once
	l.Fail = func(proc string, vo bool) error {
		if proc == "PatchLabels" && !vo {
			once.Do(func() {
				if _, err := ctl.PatchLabels(ctx, &controlv1.PatchLabelsRequest{FunctionId: fn.Id, Set: map[string]string{"late.example.com/tag": "survives"}}); err != nil {
					t.Error(err)
				}
			})
		}
		return nil
	}
	h.Env["K"] = "three"
	mustRun(t, h)
	l.Fail = nil
	foreign("a write by another tool in the middle of a pass")
	if got := h.S.functions(t)["echo"].Labels["late.example.com/tag"]; got != "survives" {
		t.Fatalf("a label another tool wrote during the pass was lost: %v", h.S.functions(t)["echo"].Labels)
	}
	if n := l.Count("SetLabels") + l.Count("SetRouteLabels"); n != 0 {
		t.Fatalf("the service replaced a label set %d time(s)", n)
	}
	mustBeInSync(t, h)
}

// Adoption adds the owner label to what is there: one PatchLabels, the labels of
// whoever made the function stay, and the function is not read back first.
func TestAdoptionPatchesOnlyItsKey(t *testing.T) {
	ctx := context.Background()
	h2 := newHarness(t)
	l2 := h2.LogRPC()
	mine, _ := h2.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello", Labels: map[string]string{"team": "payments"}})
	h2.R.Commit("a", files("m.wasm", env.hello, "edgerun.yaml", "functions:\n  - {name: hello, module: m.wasm}\n"))
	h2.Mutate = func(c *Config) { c.Adopt = true }
	mustRun(t, h2)
	if got := h2.S.functions(t)["hello"]; got.Id != mine.Function.Id || got.Labels["team"] != "payments" || got.Labels[labelOwner] != "test" {
		t.Fatalf("adopted: %v", got.Labels)
	}
	if l2.Count("PatchLabels") != 1 || l2.Count("SetLabels") != 0 || l2.Count("GetFunction") != 0 {
		t.Fatalf("adoption should be one PatchLabels, got %v", l2.Writes())
	}
}

// watch asks for each host every few milliseconds until stopped, and says how many
// requests there were and how many were not served (a request that finds no route
// is FUNCTION_NOT_FOUND).
func watch(t *testing.T, s *Stack, hosts ...string) (stop func() (polls, failures int64)) {
	var n, bad atomic.Int64
	quit := make(chan struct{})
	var wg sync.WaitGroup
	for _, host := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-quit:
					return
				default:
				}
				st, _, err := s.Invoke(t, host, "/")
				n.Add(1)
				if err != nil || st != 200 {
					bad.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
	return func() (int64, int64) { close(quit); wg.Wait(); return n.Load(), bad.Load() }
}

// A route somebody made by hand, at a key the file declares, onto the function the
// file declares it for: the service labels it (PatchRouteLabels) instead of deleting
// and creating it. The route is never unrouted, keeps its id and the labels of
// whoever made it. Watched from outside by a client that never stops asking, with
// every write of the pass slowed to 100 ms so that a gap would be wide; and the
// same watch on a route that has to be retargeted shows that the watch can see one.
func TestAdoptingARouteNeverUnroutesIt(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ctl := h.S.Ctl
	h.R.Commit("1", files("m.wasm", env.hello, "edgerun.yaml", "functions:\n  - {name: a, module: m.wasm}\n  - {name: b, module: m.wasm}\nroutes:\n  - {host: a.test, path: /, function: a}\n"))
	mustRun(t, h)
	fns := h.S.functions(t)
	a, b := fns["a"], fns["b"]

	// Hand-made: a route onto an owned function, a route onto an owned function that the
	// file wants elsewhere, and, for the --adopt case, a whole function with its route.
	byHand, err := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "hand.test", PathPrefix: "/", FunctionId: a.Id, Labels: map[string]string{"team": "edge"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "moved.test", PathPrefix: "/", FunctionId: a.Id}); err != nil {
		t.Fatal(err)
	}
	legacy, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "legacy"})
	ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: legacy.Function.Id, Module: env.hello, Abi: "wagi-1", Activate: true})
	lr, _ := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "legacy.test", PathPrefix: "/", FunctionId: legacy.Function.Id, Labels: map[string]string{"team": "old"}})

	h.R.Commit("2", files("edgerun.yaml", `
functions:
  - {name: a, module: m.wasm}
  - {name: b, module: m.wasm}
  - {name: legacy, module: m.wasm}
routes:
  - {host: a.test, path: /, function: a}
  - {host: hand.test, path: /, function: a}
  - {host: moved.test, path: /, function: b}
  - {host: legacy.test, path: /, function: legacy}
`))
	h.Mutate = func(c *Config) { c.Adopt = true }
	// Warm the worker up (the first call of a module compiles it, which would be all
	// the watch saw of the pass).
	if _, err := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "warm-b.test", PathPrefix: "/", FunctionId: b.Id}); err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"hand.test", "legacy.test", "warm-b.test"} {
		h.S.mustInvoke(t, host, "/")
	}
	l := h.LogRPC()
	l.Fail = func(proc string, vo bool) error {
		if !vo && (proc == "DeleteRoute" || proc == "CreateRoute" || proc == "PatchRouteLabels") {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}

	_, err, out := h.Run(true)
	if err != nil || !strings.Contains(out, "labelling it as ours, not recreating it") || strings.Count(out, "replacing it") != 1 {
		t.Fatalf("%v\n%s", err, out)
	}
	stopSame := watch(t, h.S, "hand.test", "legacy.test")
	stopMoved := watch(t, h.S, "moved.test")
	_, out = mustRun(t, h)
	polls, failures := stopSame()
	mPolls, mFailures := stopMoved()
	t.Logf("labelled in place: %d requests, %d unrouted; retargeted: %d requests, %d unrouted", polls, failures, mPolls, mFailures)
	if polls < 20 || failures != 0 {
		t.Fatalf("%d of %d requests failed while the routes were being adopted\n%s", failures, polls, out)
	}
	// The control: a route that has to be retargeted is unrouted between its delete and
	// its create (there is no UpdateRoute), and the watch sees it.
	if mFailures == 0 {
		t.Fatalf("the watch saw no gap in a delete + create of %d requests: it cannot prove the absence of one", mPolls)
	}

	if l.Count("PatchRouteLabels") != 2 || l.Count("SetRouteLabels") != 0 || l.Count("DeleteRoute") != 1 || l.Count("CreateRoute") != 1 {
		t.Fatalf("two routes labelled in place, one replaced: %v", l.Writes())
	}
	rs := h.S.routes(t)
	for key, orig := range map[string]*controlv1.CreateRouteResponse{"hand.test/": byHand, "legacy.test/": lr} {
		rt := rs[key]
		if rt.Id != orig.Route.Id || rt.Labels[labelOwner] != "test" || rt.Labels["team"] != orig.Route.Labels["team"] {
			t.Fatalf("%s: %v (was %v)", key, rt, orig.Route)
		}
	}
	if rt := rs["moved.test/"]; rt.FunctionId != b.Id || rt.Labels[labelOwner] != "test" {
		t.Fatalf("moved.test: %v", rt)
	}
	l.Fail = nil
	mustBeInSync(t, h)
}

// Nothing a pass reads is "everything": every list the service sends names a
// filter (owner label, a name, a function, or the has / lacks key filters), even
// with 130 functions and a lot of routes next to it that are not its own, and when
// it adopts, prunes and finds conflicts.
func TestNoPassScansEverything(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ctl := h.S.Ctl
	var foreign []*controlv1.Function
	for i := 0; i < 130; i++ {
		labels := map[string]string{}
		if i%2 == 0 {
			labels[labelOwner] = "someone-else"
		}
		f, err := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: fmt.Sprintf("bulk-%03d", i), Labels: labels})
		if err != nil {
			t.Fatal(err)
		}
		foreign = append(foreign, f.Function)
		ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: fmt.Sprintf("bulk%d.test", i), PathPrefix: "/", FunctionId: f.Function.Id, Labels: labels})
	}
	legacy, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "legacy"})
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "legacy.test", PathPrefix: "/", FunctionId: legacy.Function.Id})
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "taken.test", PathPrefix: "/", FunctionId: foreign[1].Id})
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "taken2.test", PathPrefix: "/", FunctionId: foreign[0].Id, Labels: ownerLabels("someone-else")})

	h.R.Commit("c", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: hello, module: m.wasm}
  - {name: legacy, module: m.wasm}
  - {name: bulk-001, module: m.wasm}
  - {name: bulk-000, module: m.wasm}
  - {name: solo, module: m.wasm}
routes:
  - {host: hello.test, path: /, function: hello}
  - {host: legacy.test, path: /, function: legacy}
  - {host: taken.test, path: /, function: hello}
  - {host: taken2.test, path: /, function: solo}
`))
	h.Mutate = func(c *Config) { c.Adopt, c.Prune = true, true }
	l := h.LogRPC()
	_, err, out := h.Run(true)
	if err == nil {
		t.Fatalf("bulk-000 and taken2.test are conflicts:\n%s", out)
	}
	for _, want := range []string{`bulk-000: exists and belongs to another owner`, "already_exists"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
	h.Run(false)
	h.Run(false)
	scans := 0
	for _, c := range l.calls {
		switch q := c.Req.(type) {
		case *controlv1.ListFunctionsRequest:
			if q.Name == "" && len(q.MatchLabels) == 0 && len(q.HasLabelKeys) == 0 && len(q.LacksLabelKeys) == 0 {
				scans++
			}
		case *controlv1.ListRoutesRequest:
			if q.FunctionId == "" && len(q.MatchLabels) == 0 && len(q.HasLabelKeys) == 0 && len(q.LacksLabelKeys) == 0 {
				scans++
			}
		}
	}
	if scans != 0 {
		t.Fatalf("%d unfiltered list calls among %d", scans, l.Total())
	}
	if l.Count("ListEnv") > 12 {
		t.Fatalf("%d ListEnv calls: only the functions of the file need theirs", l.Count("ListEnv"))
	}
	t.Logf("3 passes next to 130 foreign functions and 133 foreign routes: %d calls, none unfiltered", l.Total())
	// The unowned routes of adopted functions were taken (one retargeted, one labelled,
	// one deleted as not in the file), another owner's route was left alone.
	rs := h.S.routes(t)
	fs := h.S.functions(t)
	if rs["legacy.test/"].Labels[labelOwner] != "test" || rs["taken.test/"].Labels[labelOwner] != "test" || rs["taken.test/"].FunctionId != fs["hello"].Id ||
		rs["taken2.test/"].Labels[labelOwner] != "someone-else" || rs["bulk1.test/"] != nil || fs["solo"] != nil {
		t.Fatalf("legacy %v, taken %v, taken2 %v, bulk1 %v, solo %v", rs["legacy.test/"], rs["taken.test/"], rs["taken2.test/"], rs["bulk1.test/"], fs["solo"])
	}
}

// The failure of a validated plan comes back as a step index, a code and a message;
// the service maps the index to the item of the plan and reports that. The function
// that owns the step is dropped, and the rest is validated again without it, so that
// one bad function does not hide the next one's problem or hold the others back.
func TestValidateFailureIsMappedToItsStep(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("c", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: a, module: m.wasm, env: {X: '1'}}
  - {name: b, module: m.wasm, egress: ['x.example.com:0']}
  - {name: c, module: m.wasm, env: {PATH_INFO: nope}}
  - {name: d, module: m.wasm}
routes:
  - {host: a.test, path: /, function: a}
  - {host: b.test, path: /, function: b}
  - {host: d.test, path: /, function: d}
`))
	l := h.LogRPC()
	res, err, out := h.Run(true)
	if err == nil || len(res.Plan.Problems) != 2 {
		t.Fatalf("two functions are refused, two are fine: %v\n%s", err, out)
	}
	if l.Count("Validate") != 3 {
		t.Fatalf("one Validate per refusal, and one for what is left: %d", l.Count("Validate"))
	}
	// The index in the report is the position of that step in the request that failed:
	// c's reserved variable in the first (it comes first in the order of the apply),
	// b's egress list in the second.
	find := func(n int, match func(*controlv1.Step) bool) (int, int) {
		steps := validateSteps(t, l, n)
		for i, st := range steps {
			if match(st) {
				return i, len(steps)
			}
		}
		t.Fatalf("Validate %d has no such step", n)
		return 0, 0
	}
	i, n := find(0, func(st *controlv1.Step) bool { return st.GetSetEnv().GetEntry().GetName() == "PATH_INFO" })
	if want := fmt.Sprintf("(step %d of %d)", i, n); !strings.Contains(out, "reserved") || !strings.Contains(out, want) {
		t.Fatalf("want c's refusal, %s:\n%s", want, out)
	}
	i, n = find(1, func(st *controlv1.Step) bool { return st.GetSetEgress() != nil })
	if want := fmt.Sprintf("(step %d of %d)", i, n); !strings.Contains(out, "egress [] -> [x.example.com:0]: invalid_argument") || !strings.Contains(out, want) {
		t.Fatalf("want b's refusal, %s:\n%s", want, out)
	}
	for _, st := range validateSteps(t, l, 2) {
		if st.GetSetEgress() != nil || st.GetSetEnv().GetEntry().GetName() == "PATH_INFO" {
			t.Fatalf("a refused function is still in the last plan: %v", st)
		}
	}
	// And the apply does what the dry run said: a and d, not b and c.
	_, err, _ = h.Run(false)
	if err == nil {
		t.Fatal("the apply must report the refusals")
	}
	fs := h.S.functions(t)
	if fs["a"] == nil || fs["d"] == nil || fs["b"] != nil || fs["c"] != nil {
		t.Fatalf("functions: %v", fs)
	}
	h.S.mustInvoke(t, "a.test", "/")
	h.S.mustInvoke(t, "d.test", "/")
}

// A plan is more than one Validate: at most 128 steps each, and a request is bounded
// as an upload is (one module's limit for all of it), so 198 steps of 6 functions
// and 36 MiB of modules are both cut at function boundaries.
func TestValidateIsCutIntoRequestsTheServerTakes(t *testing.T) {
	h := newHarness(t)
	var yaml strings.Builder
	yaml.WriteString("functions:\n")
	for i := 0; i < 6; i++ {
		yaml.WriteString(envYAML(fmt.Sprintf("f%d", i), "m.wasm", "V", 30, ""))
	}
	yaml.WriteString("routes:\n")
	for i := 0; i < 6; i++ {
		fmt.Fprintf(&yaml, "  - {host: f%d.test, path: /, function: f%d}\n", i, i)
	}
	h.R.Commit("many", files("m.wasm", env.hello, "edgerun.yaml", yaml.String()))
	l := h.LogRPC()
	_, err, out := h.Run(true)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if l.Count("Validate") != 2 {
		t.Fatalf("198 steps are two requests, got %d", l.Count("Validate"))
	}
	for i := 0; i < 2; i++ {
		if n := len(validateSteps(t, l, i)); n > 128 {
			t.Fatalf("Validate %d has %d steps", i, n)
		}
	}
	mustRun(t, h)
	h.S.mustInvoke(t, "f0.test", "/")
	h.S.mustInvoke(t, "f5.test", "/")
	mustBeInSync(t, h)
}

// Three modules of 12 MiB in one request are more than the server accepts for an
// upload (one module's limit, 32 MiB, plus 1 MiB): asked directly, the call is
// refused as a whole although every step of it is valid. The service cuts the plan.
func TestValidateOfBigModulesIsCut(t *testing.T) {
	big := func(i int) []byte {
		b := make([]byte, 12<<20)
		copy(b, "\x00asm\x01\x00\x00\x00")
		b[len(b)-1] = byte(i)
		return b
	}
	h2 := newHarness(t)
	fn, _ := h2.S.Ctl.CreateFunction(context.Background(), &controlv1.CreateFunctionRequest{Name: "x"})
	var steps []*controlv1.Step
	for i := 0; i < 3; i++ {
		steps = append(steps, &controlv1.Step{Request: &controlv1.Step_CreateVersion{CreateVersion: &controlv1.CreateVersionRequest{FunctionId: fn.Function.Id, Module: big(i), Abi: "wagi-1"}}})
	}
	res, err := h2.S.Ctl.Validate(context.Background(), &controlv1.ValidateRequest{Steps: steps})
	t.Logf("three valid 12 MiB modules in one Validate: err=%v failure=%v", err, res.GetFailure())
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("expected the server to refuse the request as a whole (resource_exhausted), got %v / %v", err, res.GetFailure())
	}
	// The service cuts the plan, so the same file is validated and applied.
	var y strings.Builder
	y.WriteString("functions:\n")
	files3 := map[string][]byte{}
	for i := 0; i < 3; i++ {
		fmt.Fprintf(&y, "  - {name: g%d, module: g%d.wasm}\n", i, i)
		files3[fmt.Sprintf("g%d.wasm", i)] = big(i)
	}
	files3["edgerun.yaml"] = []byte(y.String())
	h2.R.Commit("big", files3)
	l2 := h2.LogRPC()
	_, err, out := h2.Run(true)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if l2.Count("Validate") < 2 || l2.Count("Describe") != 1 {
		t.Fatalf("36 MiB of modules need at least two requests: %d Validate, %d Describe", l2.Count("Validate"), l2.Count("Describe"))
	}
	mustRun(t, h2)
	if got := len(h2.S.functions(t)); got != 4 { // x, which the test made, and g0..g2
		t.Fatalf("%d functions", got)
	}
}

// listEnv lists the variables of a function.
func (s *Stack) listEnv(t testing.TB, fnID string) []*controlv1.EnvEntry {
	t.Helper()
	res, err := s.Ctl.ListEnv(context.Background(), &controlv1.ListEnvRequest{FunctionId: fnID})
	if err != nil {
		t.Fatal(err)
	}
	return res.Entries
}

// What a patch of your own keys does not give: a precondition. Two instances with
// different owners adopt the same unowned function; the second PatchLabels does not
// know that the first one put its owner label there a moment ago, and replaces it.
// Both passes succeed, and the loser only finds out at its next pass, when the
// function no longer carries its label. (With SetLabels the outcome was the same,
// and worse for the other labels; there is still no "set only if absent".)
func TestTwoAdoptersLastWriteWins(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "shared", Labels: map[string]string{"team": "x"}})
	spec := "functions:\n  - {name: shared, module: m.wasm}\n"
	h.R.Commit("a", files("m.wasm", env.hello, "edgerun.yaml", spec))
	h2 := &Harness{t: t, S: h.S, R: newRepo(t, t.TempDir()), Env: h.Env, Mutate: func(c *Config) { c.Owner = "other"; c.Adopt = true; c.WorkDir = t.TempDir() + "/c" }}
	h2.R.Commit("b", files("m.wasm", env.hello, "edgerun.yaml", spec))
	h.Mutate = func(c *Config) { c.Adopt = true }

	l := h.LogRPC()
	var once sync.Once
	l.Fail = func(proc string, vo bool) error {
		if proc == "PatchLabels" && !vo {
			once.Do(func() { mustRun(t, h2) }) // the other instance adopts first
		}
		return nil
	}
	mustRun(t, h)
	l.Fail = nil
	got := h.S.functions(t)["shared"]
	if got.Labels[labelOwner] != "test" || got.Labels["team"] != "x" {
		t.Fatalf("the last writer should hold it, with the team label intact: %v", got.Labels)
	}
	// The loser's next pass says so; it does not fight over it.
	_, err, out := h2.Run(false)
	if err == nil || !strings.Contains(err.Error(), `belongs to another owner ("test")`) {
		t.Fatalf("%v\n%s", err, out)
	}
}
