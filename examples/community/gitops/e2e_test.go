package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

func files(kv ...any) map[string][]byte {
	m := map[string][]byte{}
	for i := 0; i < len(kv); i += 2 {
		switch v := kv[i+1].(type) {
		case string:
			m[kv[i].(string)] = []byte(v)
		case []byte:
			m[kv[i].(string)] = v
		case nil:
			m[kv[i].(string)] = nil
		}
	}
	return m
}

func mustRun(t *testing.T, h *Harness) (*Result, string) {
	t.Helper()
	res, err, out := h.Run(false)
	if err != nil {
		t.Fatalf("reconcile failed: %v\n%s", err, out)
	}
	return res, out
}

func mustBeInSync(t *testing.T, h *Harness) {
	t.Helper()
	res, err, out := h.Run(true)
	if err != nil || !res.Clean {
		t.Fatalf("expected in sync, err=%v\n%s", err, out)
	}
	res, out2 := mustRun(t, h)
	if !res.Clean || res.Applied != 0 {
		t.Fatalf("second run changed something:\n%s", out2)
	}
}

// The whole story: deploy from git, see it serve, change the file, see the change
// take effect, roll back by reverting the commit.
func TestEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.Env["ECHO_KEY"] = "s3cret-one"

	// Commit 1: the hello example, one env var, one route.
	h.R.Commit("hello", files(
		"build/hello.wasm", env.hello,
		"edgerun.yaml", `
functions:
  - name: hello
    module: build/hello.wasm
    env: {GREETING: Howdy}
routes:
  - {host: Hello.Test, path: /, function: hello}
`))

	// A dry run prints the plan and changes nothing.
	res, err, out := h.Run(true)
	if err != nil || res.Clean {
		t.Fatalf("dry run: err=%v clean=%v\n%s", err, res.Clean, out)
	}
	for _, want := range []string{"+ hello: create function", "upload new version", "env GREETING", "route hello.test/"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry run output lacks %q:\n%s", want, out)
		}
	}
	if fs := h.S.functions(t); len(fs) != 0 {
		t.Fatalf("dry run created %d function(s)", len(fs))
	}

	mustRun(t, h)
	if got := h.S.mustInvoke(t, "hello.test", "/"); !strings.HasPrefix(got, "Howdy, world!") {
		t.Fatalf("hello answered %q", got)
	}
	mustBeInSync(t, h) // idempotent

	// Commit 2: new greeting, a second route, a second function with a secret.
	h.R.Commit("grow", files(
		"build/echo.wasm", env.guest["v1"],
		"edgerun.yaml", `
functions:
  - name: hello
    module: build/hello.wasm
    env: {GREETING: Hola}
  - name: echo
    module: build/echo.wasm
    env: {GREETING: hi}
    secret_env: {API_KEY: ECHO_KEY}
    limits: {memory_mib: 64, max_response_kib: 64}
routes:
  - {host: hello.test, path: /, function: hello}
  - {host: echo.test, path: /, function: echo}
`))
	_, out = mustRun(t, h)
	if strings.Contains(out, "s3cret-one") {
		t.Fatalf("a secret value was printed:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "hello.test", "/"); !strings.HasPrefix(got, "Hola, world!") {
		t.Fatalf("hello answered %q", got)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/x"); got != "version=v1 greeting=hi secret=s3cret-one path=/x" {
		t.Fatalf("echo answered %q", got)
	}
	mustBeInSync(t, h) // secrets included: no re-write, no diff

	// Commit 3: a new module for echo: a new version, activated.
	h.R.Commit("echo v2", files("build/echo.wasm", env.guest["v2"]))
	_, out = mustRun(t, h)
	if !strings.Contains(out, "upload new version") {
		t.Fatalf("expected an upload:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "version=v2") {
		t.Fatalf("echo answered %q", got)
	}
	echo := h.S.functions(t)["echo"]
	if vs := h.S.versions(t, echo.Id); len(vs) != 2 {
		t.Fatalf("want 2 versions, have %d", len(vs))
	}
	mustBeInSync(t, h)

	// Commit 4: revert. The old module's version still exists, so it is
	// activated again: no upload, no third version.
	h.R.Revert()
	_, out = mustRun(t, h)
	if strings.Contains(out, "upload") || !strings.Contains(out, "activate existing v1") {
		t.Fatalf("revert should re-activate v1:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "version=v1") {
		t.Fatalf("echo answered %q", got)
	}
	if vs := h.S.versions(t, echo.Id); len(vs) != 2 {
		t.Fatalf("revert created a version: %d", len(vs))
	}
	mustBeInSync(t, h)

	// Commit 5: the secret rotates in the process environment, the file is untouched.
	h.Env["ECHO_KEY"] = "s3cret-two"
	_, out = mustRun(t, h)
	if !strings.Contains(out, "secret API_KEY changed") || strings.Contains(out, "s3cret") {
		t.Fatalf("rotation:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=s3cret-two") {
		t.Fatalf("echo answered %q", got)
	}
	mustBeInSync(t, h)

	// Commit 6: drop echo's route and its env var, add an egress list.
	h.R.Commit("trim", files("edgerun.yaml", `
functions:
  - name: hello
    module: build/hello.wasm
    env: {GREETING: Hola}
  - name: echo
    module: build/echo.wasm
    secret_env: {API_KEY: ECHO_KEY}
    limits: {memory_mib: 64, max_response_kib: 64}
    egress: ["B.example.com", "*.cdn.example.com", "b.example.com"]
routes:
  - {host: hello.test, path: /, function: hello}
`))
	mustRun(t, h)
	_, _, err = h.S.Invoke(t, "echo.test", "/")
	if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND) {
		t.Fatalf("route should be gone, got %v", err)
	}
	if got := h.S.functions(t)["echo"].EgressAllow; len(got) != 2 || got[0] != "*.cdn.example.com" || got[1] != "b.example.com" {
		t.Fatalf("egress: %v", got)
	}
	ev, _ := h.S.Ctl.ListEnv(context.Background(), &controlv1.ListEnvRequest{FunctionId: echo.Id})
	for _, e := range ev.Entries {
		if e.Name == "GREETING" {
			t.Fatalf("GREETING should have been deleted")
		}
	}
	mustBeInSync(t, h)
}

// Someone changes the orchestrator by hand. The dry run reports it; a pass
// puts it back.
func TestDrift(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("v", files(
		"m/hello.wasm", env.hello, "m/g1.wasm", env.guest["v1"], "m/g2.wasm", env.guest["v2"],
		"edgerun.yaml", `
functions:
  - name: hello
    module: m/hello.wasm
    env: {GREETING: Howdy}
    egress: [a.example.com]
routes:
  - {host: hello.test, path: /, function: hello}
`))
	mustRun(t, h)
	mustBeInSync(t, h)

	ctx := context.Background()
	ctl := h.S.Ctl
	fn := h.S.functions(t)["hello"]
	// 1 edit an env var, 2 add a stray one, 3 add a stray route of ours (labelled) and
	// one that is nobody's (unlabelled), 4 delete the real route, 5 point the function
	// at a different version, 6 change egress.
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "GREETING", Value: "Hacked"}})
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "EXTRA", Value: "1"}})
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "stray.test", PathPrefix: "/", FunctionId: fn.Id, Labels: ownerLabels("test")})
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "handmade.test", PathPrefix: "/", FunctionId: fn.Id})
	ctl.DeleteRoute(ctx, &controlv1.DeleteRouteRequest{RouteId: h.S.routes(t)["hello.test/"].Id})
	v2, err := ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fn.Id, Module: env.guest["v2"], Abi: "wagi-1", Activate: true})
	if err != nil {
		t.Fatal(err)
	}
	ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fn.Id, Allow: []string{"evil.example.com"}})

	res, err, out := h.Run(true)
	if err != nil || res.Clean {
		t.Fatalf("drift not detected: %v\n%s", err, out)
	}
	for _, want := range []string{"env GREETING", "env EXTRA", "route stray.test/", "route hello.test/", "activate existing v1", "egress"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry run lacks %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "DRIFT") {
		t.Errorf("a pass at the same commit should call it drift:\n%s", out)
	}
	if got := h.S.functions(t)["hello"].ActiveVersionId; got != v2.Version.Id {
		t.Fatalf("dry run changed the active version")
	}

	_, out = mustRun(t, h)
	if !strings.Contains(out, "DRIFT") {
		t.Errorf("no drift label:\n%s", out)
	}
	mustBeInSync(t, h)
	if got := h.S.mustInvoke(t, "hello.test", "/"); !strings.HasPrefix(got, "Howdy, world!") {
		t.Fatalf("after repair hello answered %q", got)
	}
	if _, ok := h.S.routes(t)["stray.test/"]; ok {
		t.Fatal("stray route survived")
	}
	// A route nobody labelled is not ours, even if it points at one of our
	// functions: ownership is the label, not the function it leads to.
	if _, ok := h.S.routes(t)["handmade.test/"]; !ok {
		t.Fatal("a route this service does not own was deleted")
	}
}

func TestPrune(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	// A function nobody here created, with a route.
	legacy, err := h.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	h.S.Ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: legacy.Function.Id, Module: env.hello, Abi: "wagi-1", Activate: true})
	h.S.Ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "legacy.test", PathPrefix: "/", FunctionId: legacy.Function.Id})

	// A second GitOps instance with another owner, in another repo.
	h2 := &Harness{t: t, S: h.S, R: newRepo(t, t.TempDir()), Env: h.Env, Mutate: func(c *Config) { c.Owner = "other"; c.WorkDir = t.TempDir() + "/c" }}
	h2.R.Commit("o", files("m.wasm", env.hello, "edgerun.yaml", "functions:\n  - {name: theirs, module: m.wasm}\nroutes:\n  - {host: theirs.test, path: /, function: theirs}\n"))
	mustRun(t, h2)

	both := `
functions:
  - {name: a, module: m.wasm}
  - {name: b, module: m.wasm}
routes:
  - {host: a.test, path: /, function: a}
  - {host: b.test, path: /, function: b}
`
	h.R.Commit("two", files("m.wasm", env.hello, "edgerun.yaml", both))
	mustRun(t, h)

	only := "functions:\n  - {name: a, module: m.wasm}\nroutes:\n  - {host: a.test, path: /, function: a}\n"
	h.R.Commit("one", files("edgerun.yaml", only))

	// Without --prune the removed function stays (its route is not in the file, but
	// the function is no longer managed this run, so the route stays too).
	mustRun(t, h)
	if _, ok := h.S.functions(t)["b"]; !ok {
		t.Fatal("b was deleted without --prune")
	}
	// With --prune it goes, with its route; legacy and theirs are left alone.
	h.Mutate = func(c *Config) { c.Prune = true }
	_, out := mustRun(t, h)
	if !strings.Contains(out, "- b: delete function") {
		t.Fatalf("no prune:\n%s", out)
	}
	fs := h.S.functions(t)
	if _, ok := fs["b"]; ok {
		t.Fatal("b survived --prune")
	}
	for _, keep := range []string{"a", "legacy", "theirs"} {
		if _, ok := fs[keep]; !ok {
			t.Fatalf("--prune deleted %s", keep)
		}
	}
	rs := h.S.routes(t)
	if _, ok := rs["b.test/"]; ok {
		t.Fatal("b's route survived")
	}
	for _, keep := range []string{"a.test/", "legacy.test/", "theirs.test/"} {
		if _, ok := rs[keep]; !ok {
			t.Fatalf("lost route %s", keep)
		}
	}
	mustBeInSync(t, h)

	// An empty file with --prune is refused.
	h.R.Commit("oops", files("edgerun.yaml", "functions: []\n"))
	_, err, _ = h.Run(false)
	if err == nil || !strings.Contains(err.Error(), "refusing to prune") {
		t.Fatalf("want a refusal, got %v", err)
	}
	if _, ok := h.S.functions(t)["a"]; !ok {
		t.Fatal("a deleted")
	}
}

// Something that exists and carries no owner label is never taken silently, not
// even when it is empty (what a run that died after CreateFunction used to leave
// behind: that case cannot occur any more, see TestNoWindowBetweenCreateAndClaim).
// --adopt labels it, and keeps the labels other people put on it.
func TestConflictAndAdopt(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ctl := h.S.Ctl
	// "hello" exists, was made by hand, has content and somebody else's label.
	fn, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello", Labels: map[string]string{"team": "payments"}})
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Function.Id, Entry: &controlv1.EnvEntry{Name: "MINE", Value: "1"}})
	// "empty" exists and is completely empty. The old recovery heuristic would have
	// called it "mine".
	ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "empty"})
	// "theirs" belongs to another owner.
	ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "theirs", Labels: ownerLabels("someone-else")})
	// A route of somebody else's function.
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "taken.test", PathPrefix: "/", FunctionId: fn.Function.Id})

	h.R.Commit("c", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: hello, module: m.wasm}
  - {name: empty, module: m.wasm}
  - {name: theirs, module: m.wasm}
  - {name: fresh, module: m.wasm}
routes:
  - {host: taken.test, path: /, function: fresh}
  - {host: fresh.test, path: /, function: fresh}
  - {host: empty.test, path: /, function: empty}
`))
	_, err, out := h.Run(false)
	if err == nil {
		t.Fatalf("conflicts must fail the run:\n%s", out)
	}
	for _, want := range []string{
		"hello: exists but was not created by this service",
		"empty: exists but was not created by this service",
		`theirs: exists and belongs to another owner ("someone-else")`,
		`taken.test/ is taken by a route to function "hello"`,
	} {
		if !strings.Contains(err.Error()+out, want) {
			t.Errorf("missing %q in:\n%v\n%s", want, err, out)
		}
	}
	// The conflicting functions are untouched; the independent one was applied.
	fs := h.S.functions(t)
	for _, n := range []string{"hello", "empty", "theirs"} {
		if fs[n].ActiveVersionId != "" {
			t.Fatalf("%s was deployed over", n)
		}
	}
	if fs["empty"].Labels[labelOwner] != "" || fs["hello"].Labels[labelOwner] != "" {
		t.Fatal("an unowned function was claimed without --adopt")
	}
	ev, _ := ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fn.Function.Id})
	if len(ev.Entries) != 1 || ev.Entries[0].Name != "MINE" {
		t.Fatalf("hello's env was changed: %v", ev.Entries)
	}
	if _, ok := h.S.routes(t)["empty.test/"]; ok {
		t.Fatal("a route was created for a function that is in conflict")
	}
	h.S.mustInvoke(t, "fresh.test", "/")

	// With --adopt, hello and empty are labelled (hello keeps its team label), hello's
	// foreign env var is removed, and its hand-made route, at a key the file
	// declares for another function of ours, is replaced. "theirs" stays theirs.
	h.Mutate = func(c *Config) { c.Adopt = true }
	_, err, out = h.Run(false)
	if err == nil || !strings.Contains(err.Error(), "theirs") {
		t.Fatalf("theirs must still be a conflict: %v\n%s", err, out)
	}
	if !strings.Contains(out, "ADOPT") || !strings.Contains(out, "replacing it with a labelled one") {
		t.Fatalf("adopt:\n%s", out)
	}
	fs = h.S.functions(t)
	if fs["hello"].Labels[labelOwner] != "test" || fs["hello"].Labels["team"] != "payments" {
		t.Fatalf("adopted labels: %v", fs["hello"].Labels)
	}
	if fs["empty"].Labels[labelOwner] != "test" || fs["theirs"].Labels[labelOwner] != "someone-else" {
		t.Fatalf("owners: %v %v", fs["empty"].Labels, fs["theirs"].Labels)
	}
	ev, _ = ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fn.Function.Id})
	if len(ev.Entries) != 0 {
		t.Fatalf("MINE should have been removed by adoption: %v", ev.Entries)
	}
	rs := h.S.routes(t)
	if rs["taken.test/"].FunctionId != fs["fresh"].Id || rs["taken.test/"].Labels[labelOwner] != "test" {
		t.Fatalf("taken.test not retargeted and labelled: %v", rs["taken.test/"])
	}
	h.S.mustInvoke(t, "empty.test", "/")

	// Once "theirs" is out of the file everything is in sync, with --adopt no longer needed.
	h.R.Commit("drop theirs", files("edgerun.yaml", `
functions:
  - {name: hello, module: m.wasm}
  - {name: empty, module: m.wasm}
  - {name: fresh, module: m.wasm}
routes:
  - {host: taken.test, path: /, function: fresh}
  - {host: fresh.test, path: /, function: fresh}
  - {host: empty.test, path: /, function: empty}
`))
	h.Mutate = nil
	mustBeInSync(t, h)
}

func mustRun2(t *testing.T, h *Harness) (*Result, error, string) {
	t.Helper()
	res, err, out := h.Run(false)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return res, err, out
}

// One function cannot be deployed; the others are, and the failure is reported.
func TestPartialFailure(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("p", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: good, module: m.wasm}
  - {name: nomodule, module: missing.wasm}
routes:
  - {host: good.test, path: /, function: good}
  - {host: nomodule.test, path: /, function: nomodule}
`))
	_, err, out := h.Run(false)
	if err == nil || !strings.Contains(err.Error(), "nomodule") {
		t.Fatalf("want an error naming nomodule: %v\n%s", err, out)
	}
	h.S.mustInvoke(t, "good.test", "/")
	if _, ok := h.S.functions(t)["nomodule"]; ok {
		t.Fatal("a function without a module should not be created at all")
	}
	if _, ok := h.S.routes(t)["nomodule.test/"]; ok {
		t.Fatal("route created for a function that does not exist")
	}

	// A step the orchestrator rejects. It used to pass the hand-written validator
	// ("a.example.com:99999"), the dry run said fine, and the apply created the
	// function, uploaded and activated the version, and only then failed at
	// SetEgress. Now every step is validated with validate_only before anything is
	// applied, so the dry run fails where the apply would, and nothing of "half" is
	// created.
	h.R.Commit("p2", files("edgerun.yaml", `
functions:
  - {name: good, module: m.wasm}
  - name: half
    module: m.wasm
    egress: ["a.example.com:99999"]
routes:
  - {host: half.test, path: /, function: half}
`))
	before := len(h.S.functions(t))
	res, err, out := h.Run(true)
	if err == nil || res.Clean || len(res.Plan.Problems) != 1 || !strings.Contains(out, "a.example.com:99999") || !strings.Contains(out, "port") {
		t.Fatalf("the dry run must report the rejection: %v\n%s", err, out)
	}
	if got := len(h.S.functions(t)); got != before {
		t.Fatalf("the dry run changed the orchestrator: %d -> %d functions", before, got)
	}
	_, err, out = h.Run(false)
	if err == nil || !strings.Contains(err.Error(), "99999") {
		t.Fatalf("the apply must fail the same way: %v\n%s", err, out)
	}
	if _, ok := h.S.functions(t)["half"]; ok {
		t.Fatalf("half was created although a step of it is invalid:\n%s", out)
	}
	if _, ok := h.S.routes(t)["half.test/"]; ok {
		t.Fatal("route created for a function that was rejected")
	}
	h.R.Commit("p3", files("edgerun.yaml", `
functions:
  - {name: good, module: m.wasm}
  - {name: half, module: m.wasm, egress: [ok.example.com]}
routes:
  - {host: half.test, path: /, function: half}
`))
	mustRun(t, h)
	h.S.mustInvoke(t, "half.test", "/")
	mustBeInSync(t, h)
}

// A step that fails at apply time although validation passed (the orchestrator
// changed in between, or went away): the later steps of that function are
// skipped, the others are applied, and the next pass finishes the job.
func TestApplyFailureSkipsTheRestOfTheFunction(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("a", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: good, module: m.wasm}
  - {name: flaky, module: m.wasm, egress: [a.example.com]}
routes:
  - {host: good.test, path: /, function: good}
  - {host: flaky.test, path: /, function: flaky}
`))
	l := h.LogRPC()
	l.Fail = func(proc string, vo bool) error {
		if proc == "SetEgress" && !vo {
			return connect.NewError(connect.CodeUnavailable, errors.New("injected"))
		}
		return nil
	}
	_, err, out := h.Run(false)
	if err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("want the injected failure: %v\n%s", err, out)
	}
	h.S.mustInvoke(t, "good.test", "/")
	if _, ok := h.S.routes(t)["flaky.test/"]; ok {
		t.Fatal("the route of a function whose egress step failed was created")
	}
	if !strings.Contains(out, "skipped flaky: route flaky.test/") {
		t.Fatalf("the skip is not reported:\n%s", out)
	}
	l.Fail = nil
	mustRun(t, h)
	h.S.mustInvoke(t, "flaky.test", "/")
	mustBeInSync(t, h)
}

// The poll loop picks up a pushed commit by itself.
func TestPollLoop(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("1", files("m.wasm", env.hello, "edgerun.yaml", "functions:\n  - {name: hello, module: m.wasm, env: {GREETING: One}}\nroutes:\n  - {host: hello.test, path: /, function: hello}\n"))
	c := h.config()
	c.Interval = 300 * 1e6
	var buf syncBuffer
	g := New(c, &buf)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Loop(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "first deploy", func() bool { s, _, err := h.S.Invoke(t, "hello.test", "/"); return err == nil && s == 200 })
	h.R.Commit("2", files("edgerun.yaml", "functions:\n  - {name: hello, module: m.wasm, env: {GREETING: Two}}\nroutes:\n  - {host: hello.test, path: /, function: hello}\n"))
	waitFor(t, "second commit applied", func() bool {
		_, b, err := h.S.Invoke(t, "hello.test", "/")
		return err == nil && strings.HasPrefix(b, "Two, world!")
	})
	// And a manual change is repaired and labelled drift.
	fn := h.S.functions(t)["hello"]
	h.S.Ctl.SetEnv(context.Background(), &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "GREETING", Value: "Hacked"}})
	waitFor(t, "drift repaired", func() bool {
		_, b, err := h.S.Invoke(t, "hello.test", "/")
		return err == nil && strings.HasPrefix(b, "Two, world!")
	})
	if !strings.Contains(buf.String(), "DRIFT") {
		t.Errorf("loop output does not mention drift:\n%s", buf.String())
	}
}

// Quick guard on error classification: the control seam has no ErrorReason.
func TestControlErrorsCarryNoReason(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "dup"})
	_, err := h.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "dup"})
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("want already_exists, got %v", err)
	}
	if r := edgerun.Reason(err); r != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		t.Fatalf("control errors are documented to carry no reason, got %v", r)
	}
}

// Secrets are write-only, but every env entry has a revision. The service
// remembers the revision of each secret it wrote (a label of the function), asks
// the orchestrator which revision the desired value would have (a validate-only
// SetEnv), and so sees a secret changed behind its back, names it, and puts it
// back; and writes nothing when nothing differs. (This used to be the evidence
// that such a change was invisible.)
func TestSecretDriftIsVisible(t *testing.T) {
	h := newHarness(t)
	h.Env["K"] = "good"
	h.Env["K2"] = "other"
	h.R.Commit("s", files("m.wasm", env.guest["v1"], "edgerun.yaml", `
functions:
  - {name: echo, module: m.wasm, secret_env: {API_KEY: K, SECOND: K2}}
routes:
  - {host: echo.test, path: /, function: echo}
`))
	l := h.LogRPC()
	mustRun(t, h)
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=good") {
		t.Fatal(got)
	}
	fn := h.S.functions(t)["echo"]
	ctx := context.Background()
	ctl := h.S.Ctl

	// The record: one label per ten secrets, nothing readable in it.
	rec := decodeSecrets(fn.Labels)
	if len(rec) != 2 || fn.Labels[labelOwner] != "test" {
		t.Fatalf("labels: %v", fn.Labels)
	}
	for k, v := range fn.Labels {
		if strings.Contains(k+v, "good") || strings.Contains(k+v, "other") {
			t.Fatalf("label %s=%s leaks a value", k, v)
		}
	}

	// An unchanged secret causes no write: a second run changes nothing, and the
	// calls it makes are reads and validate-only calls.
	l.Reset()
	res, _ := mustRun(t, h)
	if !res.Clean || res.Applied != 0 {
		t.Fatal("second run changed something")
	}
	if w := l.Writes(); len(w) != 0 {
		t.Fatalf("second run wrote: %v", w)
	}
	if l.Count("SetEnv") != 2 {
		t.Fatalf("expected one validate-only SetEnv per secret to learn the desired revisions, got %d", l.Count("SetEnv"))
	}

	// Tampering: API_KEY is overwritten out of band.
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "API_KEY", Value: "tampered", Secret: true}})
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=tampered") {
		t.Fatal(got)
	}
	l.Reset()
	res, err, out := h.Run(true)
	if err != nil || res.Clean {
		t.Fatalf("the tampering is visible now, yet nothing was reported: %v\n%s", err, out)
	}
	if !strings.Contains(out, "secret API_KEY: DRIFT") || strings.Contains(out, "SECOND") || strings.Contains(out, "tampered") {
		t.Fatalf("drift must name API_KEY (and only it) and not show values:\n%s", out)
	}
	if w := l.Writes(); len(w) != 0 {
		t.Fatalf("the dry run wrote: %v", w)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=tampered") {
		t.Fatal("the dry run corrected the secret")
	}
	// The pass puts it back.
	l.Reset()
	_, out = mustRun(t, h)
	if !strings.Contains(out, "secret API_KEY: DRIFT") {
		t.Fatalf("%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=good") {
		t.Fatalf("not corrected: %s", got)
	}
	if n := l.Count("SetEnv"); n < 1 || strings.Join(l.Writes(), ",") != "SetEnv,SetLabels" {
		t.Fatalf("only the drifted secret and the record should have been written: %v", l.Writes())
	}
	mustBeInSync(t, h)

	// Writing the same value again is a no-op for the orchestrator (same revision),
	// so a hand-made "change" to the value the file wants is not drift.
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "API_KEY", Value: "good", Secret: true}})
	mustBeInSync(t, h)

	// A rotation in the process environment is a change, not drift.
	h.Env["K"] = "good-2"
	_, out = mustRun(t, h)
	if !strings.Contains(out, "secret API_KEY changed in the process environment") || strings.Contains(out, "secret API_KEY: DRIFT") {
		t.Fatalf("rotation:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=good-2") {
		t.Fatal(got)
	}
	mustBeInSync(t, h)

	// Somebody applies the rotation by hand before the service does: the value is
	// already what the file wants, so nothing is written but the record.
	h.Env["K"] = "good-3"
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "API_KEY", Value: "good-3", Secret: true}})
	l.Reset()
	_, out = mustRun(t, h)
	if strings.Join(l.Writes(), ",") != "SetLabels" || strings.Contains(out, "secret API_KEY") {
		t.Fatalf("only the record should change: %v\n%s", l.Writes(), out)
	}
	mustBeInSync(t, h)

	// A change of kind (plain instead of secret) is visible as before.
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "API_KEY", Value: "tampered", Secret: false}})
	_, out = mustRun(t, h)
	if !strings.Contains(out, "was plain, now secret") {
		t.Fatalf("kind change not seen:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=good-3") {
		t.Fatal(got)
	}
	mustBeInSync(t, h)

	// A secret that is no longer in the file is deleted and its record goes with it.
	h.R.Commit("one secret", files("edgerun.yaml", `
functions:
  - {name: echo, module: m.wasm, secret_env: {API_KEY: K}}
routes:
  - {host: echo.test, path: /, function: echo}
`))
	mustRun(t, h)
	if got := decodeSecrets(h.S.functions(t)["echo"].Labels); len(got) != 1 {
		t.Fatalf("stale record: %v", got)
	}
	mustBeInSync(t, h)

	// A secret whose process variable is unset is a plan-time problem, not a silent skip.
	delete(h.Env, "K")
	res, err, out = h.Run(true)
	if err == nil || len(res.Plan.Problems) == 0 || !strings.Contains(out, "environment variable K is not set") {
		t.Fatalf("expected a problem about K: %v\n%s", err, out)
	}
}

// The guest sees the function's real environment and nothing else, and all 64
// slots are the user's. (The marker variable used to be visible to the guest and
// to take one slot.) The largest function also shows that the record of secret
// revisions fits the label limits with a dozen secrets.
func TestGuestSeesOnlyItsEnvAndAll64SlotsAreTheUsers(t *testing.T) {
	h := newHarness(t)
	var yaml strings.Builder
	yaml.WriteString("functions:\n  - name: big\n    module: m.wasm\n    env:\n")
	for i := 0; i < 52; i++ {
		fmt.Fprintf(&yaml, "      PLAIN_%02d: v%d\n", i, i)
	}
	yaml.WriteString("    secret_env:\n")
	for i := 0; i < 12; i++ {
		fmt.Fprintf(&yaml, "      SECRET_%02d: S%d\n", i, i)
		h.Env[fmt.Sprintf("S%d", i)] = fmt.Sprintf("value-%d", i)
	}
	yaml.WriteString("routes:\n  - {host: big.test, path: /, function: big}\n")
	h.R.Commit("big", files("m.wasm", env.guest["v1"], "edgerun.yaml", yaml.String()))
	mustRun(t, h)
	mustBeInSync(t, h)

	got := h.S.mustInvoke(t, "big.test", "/env")
	if strings.Contains(got, "GITOPS") || strings.Contains(strings.ToLower(got), "owner") {
		t.Fatalf("the guest sees a gitops variable: %s", got)
	}
	if n := strings.Count(got, "PLAIN_") + strings.Count(got, "SECRET_"); n != 64 {
		t.Fatalf("the guest should see exactly the 64 declared variables, saw %d: %s", n, got)
	}
	fn := h.S.functions(t)["big"]
	ev, err := h.S.Ctl.ListEnv(context.Background(), &controlv1.ListEnvRequest{FunctionId: fn.Id})
	if err != nil || len(ev.Entries) != 64 {
		t.Fatalf("%d variables stored (%v)", len(ev.Entries), err)
	}
	if len(fn.Labels) != 3 { // owner + 12 secrets in 2 labels
		t.Fatalf("labels: %v", fn.Labels)
	}
	// Any one of the twelve is noticed, by name.
	h.S.Ctl.SetEnv(context.Background(), &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "SECRET_07", Value: "x", Secret: true}})
	_, _, out := h.Run(true)
	if !strings.Contains(out, "secret SECRET_07: DRIFT") || strings.Count(out, "it was changed in the orchestrator") != 1 {
		t.Fatalf("%s", out)
	}
}

// More than one page of functions. Ownership is a label, so finding what to
// prune is one filtered list, not one ListEnv per function (130 functions used to
// cost 130 ListEnv calls and 335 ms).
func TestManyFunctions(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const n = 130
	for i := 0; i < n; i++ {
		name := "bulk-" + strings.Repeat("0", 3-len(itoa(i))) + itoa(i)
		labels := map[string]string{}
		if i%2 == 0 {
			labels[labelOwner] = "someone-else"
		}
		if _, err := h.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name, Labels: labels}); err != nil {
			t.Fatal(err)
		}
	}
	// How big is a page? ListFunctions with page_size 0.
	first, err := h.S.Ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("default page: %d functions, next token set: %v", len(first.Functions), first.NextPageToken != "")
	if got := len(h.S.functions(t)); got != n {
		t.Fatalf("AllFunctions saw %d of %d", got, n)
	}
	h.R.Commit("m", files("m.wasm", env.hello, "edgerun.yaml", "functions:\n  - {name: hello, module: m.wasm}\n"))
	h.Mutate = func(c *Config) { c.Prune = true }
	l := h.LogRPC()
	start := time.Now()
	res, err, out := h.Run(true)
	if err != nil {
		t.Fatal(err, out)
	}
	t.Logf("dry run with --prune over %d foreign functions took %s, %d calls", n, time.Since(start), l.Total())
	if len(res.Plan.Items) == 0 || strings.Contains(out, "bulk-") {
		t.Fatalf("foreign functions must never be pruned:\n%s", out)
	}
	if c := l.Count("ListEnv"); c != 0 {
		t.Fatalf("%d ListEnv calls: ownership should not need to read any function's env", c)
	}
	// Once hello exists and is ours, steady state reads: the owner filter, the
	// function's own env and versions, one full scan (hello was not in the filter
	// the first time); never an env per function.
	mustRun(t, h)
	l.Reset()
	mustBeInSync(t, h)
	t.Logf("steady state next to %d foreign functions: %d calls", n, l.Total())
	if c := l.Count("ListEnv"); c > 4 {
		t.Fatalf("%d ListEnv calls for a one-function file", c)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }
