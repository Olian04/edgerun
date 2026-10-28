package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// Everything the service creates carries the owner label (functions, versions,
// routes), versions also the commit they were first uploaded from, and the label
// filters find them.
func TestEverythingItCreatesIsLabelled(t *testing.T) {
	h := newHarness(t)
	yaml := func(g string) string {
		return "functions:\n  - {name: hello, module: m.wasm}\n  - {name: echo, module: g.wasm}\nroutes:\n  - {host: hello.test, path: /, function: hello}\n  - {host: echo.test, path: /, function: echo}\n"
	}
	c1 := h.R.Commit("one", files("m.wasm", env.hello, "g.wasm", env.guest["v1"], "edgerun.yaml", yaml("")))
	mustRun(t, h)
	c2 := h.R.Commit("two", files("g.wasm", env.guest["v2"]))
	mustRun(t, h)
	mustBeInSync(t, h)

	ctx := context.Background()
	ctl := h.S.Ctl
	owner := ownerLabels("test")
	var fns []string
	for fn, err := range ctl.AllFunctionsMatching(ctx, owner) {
		if err != nil {
			t.Fatal(err)
		}
		fns = append(fns, fn.Name)
	}
	if strings.Join(fns, ",") != "echo,hello" {
		t.Fatalf("owner filter returned %v", fns)
	}
	for _, other := range []string{"nobody", ""} {
		for range ctl.AllFunctionsMatching(ctx, map[string]string{labelOwner: other}) {
			t.Fatalf("owner %q matched a function", other)
		}
	}
	rs := 0
	for rt, err := range ctl.AllRoutesMatching(ctx, owner) {
		if err != nil {
			t.Fatal(err)
		}
		rs++
		if rt.Labels[labelOwner] != "test" {
			t.Fatalf("%v", rt.Labels)
		}
	}
	if rs != 2 {
		t.Fatalf("%d labelled routes", rs)
	}
	echo := h.S.functions(t)["echo"]
	for _, c := range []struct{ commit string }{{c1}, {c2}} {
		var got []*controlv1.FunctionVersion
		for v, err := range ctl.AllVersionsMatching(ctx, echo.Id, map[string]string{labelCommit: c.commit, labelOwner: "test"}) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
		}
		if len(got) != 1 {
			t.Fatalf("commit %s: %d versions", c.commit[:10], len(got))
		}
	}
	// hello did not change in the second commit: still the version of the first.
	hello := h.S.functions(t)["hello"]
	if vs := h.S.versions(t, hello.Id); len(vs) != 1 || vs[0].Labels[labelCommit] != c1 {
		t.Fatalf("hello's version: %v", vs)
	}
}

// Creating a function is one RPC and it carries the label, so there is no
// moment at which a function of this service exists without being recognisably
// its own. The service is killed (every later write fails) after k writes, for
// every k of a deploy that touches all kinds of things; at each point nothing is
// unlabelled, and the next run finishes the job without --adopt or any
// "claim an empty function" heuristic.
func TestNoWindowBetweenCreateAndClaim(t *testing.T) {
	h := newHarness(t)
	h.Env["SEC"] = "s3"
	l := h.LogRPC()
	crashAfter := -1
	writes := 0
	l.Fail = func(proc string, vo bool) error {
		if vo || strings.HasPrefix(proc, "List") || proc == "Describe" || strings.HasPrefix(proc, "Get") {
			return nil
		}
		if crashAfter >= 0 && writes >= crashAfter {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("the process died"))
		}
		writes++
		return nil
	}
	h.R.Commit("m", files("m.wasm", env.guest["v1"]))
	total := 0
	for k := 0; k <= 12; k++ {
		name := fmt.Sprintf("f%d", k)
		h.R.Commit(name, files("edgerun.yaml", fmt.Sprintf(`
functions:
  - name: %[1]s
    module: m.wasm
    env: {GREETING: hi}
    secret_env: {API_KEY: SEC}
    egress: [a.example.com]
routes:
  - {host: %[1]s.test, path: /, function: %[1]s}
`, name)))
		l.Reset()
		writes, crashAfter = 0, k
		_, err, _ := h.Run(false)
		if err == nil {
			total = writes // a pass that was not cut short: how many writes there are
		}
		// The state the "crash" left behind: whatever exists is ours, labelled.
		for n, fn := range h.S.functions(t) {
			if fn.Labels[labelOwner] != "test" {
				t.Fatalf("k=%d: function %s exists without the owner label", k, n)
			}
		}
		for key, rt := range h.S.routes(t) {
			if rt.Labels[labelOwner] != "test" {
				t.Fatalf("k=%d: route %s exists without the owner label", k, key)
			}
		}
		for _, c := range l.calls {
			if r, ok := c.Req.(*controlv1.CreateFunctionRequest); ok && !c.Validate && r.Labels[labelOwner] != "test" {
				t.Fatalf("k=%d: CreateFunction without the label", k)
			}
		}
		// Recovery: a normal run, no --adopt, finishes it.
		crashAfter = -1
		_, out := mustRun(t, h)
		if strings.Contains(out, "ADOPT") || strings.Contains(out, "claim") || strings.Contains(out, "not created by this service") {
			t.Fatalf("k=%d: recovery needed a claim:\n%s", k, out)
		}
		h.S.mustInvoke(t, name+".test", "/")
		mustBeInSync(t, h)
		if err == nil {
			break // k was beyond the number of writes: every position has been covered
		}
	}
	if total == 0 {
		t.Fatal("never saw a pass without a crash")
	}
	t.Logf("a deploy of this shape is %d writes; killed after each of them", total)
}

// An empty function that nobody labelled is not "an unfinished one of ours": it
// is somebody's, and needs --adopt. (Covered together with the other conflicts
// in TestConflictAndAdopt; this is the focused statement.)
func TestEmptyUnlabelledFunctionIsNotTakenSilently(t *testing.T) {
	h := newHarness(t)
	h.S.Ctl.CreateFunction(context.Background(), &controlv1.CreateFunctionRequest{Name: "hello"})
	h.R.Commit("c", files("m.wasm", env.hello, "edgerun.yaml", "functions:\n  - {name: hello, module: m.wasm}\n"))
	res, err, out := h.Run(true)
	if err == nil || len(res.Plan.Problems) != 1 || !strings.Contains(out, "pass --adopt") {
		t.Fatalf("%v\n%s", err, out)
	}
	if h.S.functions(t)["hello"].Labels[labelOwner] != "" {
		t.Fatal("claimed")
	}
}
