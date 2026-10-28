package main

import (
	"context"
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
	// 1 edit an env var, 2 add a stray one, 3 add a stray route, 4 delete the real route,
	// 5 point the function at a different version, 6 change egress.
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "GREETING", Value: "Hacked"}})
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "EXTRA", Value: "1"}})
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "stray.test", PathPrefix: "/", FunctionId: fn.Id})
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

func TestConflictAdoptAndRecovery(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ctl := h.S.Ctl
	// "hello" exists, was made by hand, has content.
	fn, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello"})
	ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Function.Id, Entry: &controlv1.EnvEntry{Name: "MINE", Value: "1"}})
	// "crashed" exists and is completely empty: what a run that died after
	// CreateFunction leaves behind.
	ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "crashed"})
	// A route of somebody else's function.
	ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "taken.test", PathPrefix: "/", FunctionId: fn.Function.Id})

	h.R.Commit("c", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: hello, module: m.wasm}
  - {name: crashed, module: m.wasm}
  - {name: fresh, module: m.wasm}
routes:
  - {host: taken.test, path: /, function: fresh}
  - {host: fresh.test, path: /, function: fresh}
  - {host: crashed.test, path: /, function: crashed}
`))
	_, err, out := h.Run(false)
	if err == nil {
		t.Fatalf("conflicts must fail the run:\n%s", out)
	}
	for _, want := range []string{"hello: exists but was not created by this service", "taken.test/ is taken by function \"hello\""} {
		if !strings.Contains(err.Error()+out, want) {
			t.Errorf("missing %q in:\n%v\n%s", want, err, out)
		}
	}
	// The conflicting function is untouched; the rest was applied.
	fs := h.S.functions(t)
	if fs["hello"].ActiveVersionId != "" {
		t.Fatal("hello was deployed over")
	}
	ev, _ := ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fn.Function.Id})
	if len(ev.Entries) != 1 || ev.Entries[0].Name != "MINE" {
		t.Fatalf("hello's env was changed: %v", ev.Entries)
	}
	if !strings.Contains(out, "claim existing empty function") {
		t.Errorf("crashed was not recovered:\n%s", out)
	}
	if h.S.mustInvoke(t, "crashed.test", "/") == "" || h.S.mustInvoke(t, "fresh.test", "/") == "" {
		t.Fatal("independent functions should have been deployed")
	}

	// With --adopt, hello is taken over: its foreign env var is removed.
	h.Mutate = func(c *Config) { c.Adopt = true }
	_, err, out = h.Run(false)
	if err == nil || !strings.Contains(out, "ADOPT") {
		t.Fatalf("adopt: err=%v\n%s", err, out)
	}
	// The route is still someone else's... until hello is ours, which it now is:
	// the next pass retargets it.
	if _, _, out = mustRun2(t, h); !strings.Contains(out, "retargeting") {
		t.Fatalf("no retarget:\n%s", out)
	}
	mustBeInSync(t, h)
	ev, _ = ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fn.Function.Id})
	for _, e := range ev.Entries {
		if e.Name == "MINE" {
			t.Fatal("MINE should have been removed by adoption")
		}
	}
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

	// A step that fails on the server, after earlier steps succeeded: the function
	// stays half configured, the later steps of that function are skipped, and the
	// next pass (with the file fixed) finishes the job.
	h.R.Commit("p2", files("edgerun.yaml", `
functions:
  - {name: good, module: m.wasm}
  - name: half
    module: m.wasm
    egress: ["a.example.com:99999"]
routes:
  - {host: half.test, path: /, function: half}
`))
	// The dry run cannot know: the protocol has no validate-only call and this
	// program's copy of the egress rules does not check port ranges.
	if res, err, out := h.Run(true); err != nil || res.Clean || len(res.Plan.Problems) != 0 {
		t.Fatalf("dry run should look fine: %v\n%s", err, out)
	}
	_, err, out = h.Run(false)
	t.Logf("server-side rejection:\n%s\n%v", out, err)
	if err == nil {
		t.Skip("the server accepted the entry; nothing to demonstrate")
	}
	if _, ok := h.S.routes(t)["half.test/"]; ok {
		t.Fatal("route created although an earlier step of its function failed")
	}
	if h.S.functions(t)["half"].ActiveVersionId == "" {
		t.Fatal("expected the version to have been deployed before the egress step failed")
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

// Secrets are write-only, so an out-of-band change of a secret is invisible to
// this tool: the evidence for FINDINGS.md. A change of *kind* (plain/secret) is
// visible, because ListEnv returns the flag.
func TestSecretDriftIsInvisible(t *testing.T) {
	h := newHarness(t)
	h.Env["K"] = "good"
	h.R.Commit("s", files("m.wasm", env.guest["v1"], "edgerun.yaml", `
functions:
  - {name: echo, module: m.wasm, secret_env: {API_KEY: K}}
routes:
  - {host: echo.test, path: /, function: echo}
`))
	mustRun(t, h)
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=good") {
		t.Fatal(got)
	}
	fn := h.S.functions(t)["echo"]
	ctx := context.Background()
	h.S.Ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "API_KEY", Value: "tampered", Secret: true}})
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=tampered") {
		t.Fatal(got)
	}
	res, err, out := h.Run(true)
	if err != nil || !res.Clean {
		t.Fatalf("the tampering is undetectable in-band, yet a difference was reported: %v\n%s", err, out)
	}
	t.Log("confirmed: a secret changed out of band is reported as in sync")

	// Same name, but now a plain variable: visible.
	h.S.Ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fn.Id, Entry: &controlv1.EnvEntry{Name: "API_KEY", Value: "tampered", Secret: false}})
	_, out = mustRun(t, h)
	if !strings.Contains(out, "was plain, now secret") {
		t.Fatalf("kind change not seen:\n%s", out)
	}
	if got := h.S.mustInvoke(t, "echo.test", "/"); !strings.Contains(got, "secret=good") {
		t.Fatal(got)
	}

	// Running without the fingerprint key is a plan-time error, not a silent downgrade.
	delete(h.Env, "GITOPS_FINGERPRINT_KEY")
	h.G = nil
	res, _, out = h.Run(true)
	if len(res.Plan.Problems) == 0 || !strings.Contains(out, "GITOPS_FINGERPRINT_KEY") {
		t.Fatalf("expected a problem about the key:\n%s", out)
	}
}

// More than one page of functions: the iterators page, and ownership (which is
// an environment variable) costs one ListEnv per function when pruning.
func TestManyFunctions(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const n = 130
	for i := 0; i < n; i++ {
		name := "bulk-" + strings.Repeat("0", 3-len(itoa(i))) + itoa(i)
		if _, err := h.S.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name}); err != nil {
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
	start := time.Now()
	res, err, out := h.Run(true)
	if err != nil {
		t.Fatal(err, out)
	}
	t.Logf("dry run with --prune over %d foreign functions took %s (one ListEnv each)", n, time.Since(start))
	if len(res.Plan.Items) == 0 || strings.Contains(out, "bulk-") {
		t.Fatalf("foreign functions must never be pruned:\n%s", out)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }
