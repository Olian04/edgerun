package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// A dry run and an apply agree: whatever the orchestrator would refuse is
// reported by the dry run (it calls the real RPCs with validate_only), nothing
// the dry run validates is applied, and an apply that the orchestrator would
// refuse leaves nothing of the function behind.
func TestDryRunAgreesWithApply(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("m", files("m.wasm", env.hello))
	type tc struct {
		name, fn string // function block (the name is filled in), appended after the name
		route    string // path of a route to it, "" for none
		reject   string // what the orchestrator's message must contain; "" = accepted
	}
	cases := []tc{
		{"valid", "env: {A: b}, egress: [x.example.com]", "/", ""},
		{"egress port out of range", "egress: ['a.example.com:99999']", "/", "port"},
		{"egress bare star", "egress: ['*']", "/", "egress"},
		{"egress with a scheme", "egress: ['https://a.example.com']", "", "allow entry"},
		{"reserved env", "env: {PATH_INFO: x}", "/", "reserved"},
		{"HTTP_ env", "env: {HTTP_X: x}", "", "reserved"},
		{"bad env name", "env: {1A: x}", "", "environment variable name"},
		{"bad route prefix", "env: {A: b}", "/a//b", "path prefix"},
		{"bad route fragment", "", "/a#b", "path prefix"},
		{"bad route host", "", "/", "host pattern"},
	}
	for i, c := range cases {
		name := fmt.Sprintf("f%d", i)
		route := ""
		if c.route != "" {
			host := name + ".test"
			if strings.Contains(c.name, "route host") {
				host = "'bad host.test'"
			}
			route = fmt.Sprintf("routes:\n  - {host: %s, path: '%s', function: %s}\n", host, c.route, name)
		}
		extra := ""
		if c.fn != "" {
			extra = ", " + c.fn
		}
		h.R.Commit(c.name, files("edgerun.yaml", fmt.Sprintf("functions:\n  - {name: %s, module: m.wasm%s}\n%s", name, extra, route)))
		fnsBefore, routesBefore := len(h.S.functions(t)), len(h.S.routes(t))

		dryRes, dryErr, dryOut := h.Run(true)
		if len(h.S.functions(t)) != fnsBefore || len(h.S.routes(t)) != routesBefore {
			t.Fatalf("%s: the dry run changed the orchestrator", c.name)
		}
		_, applyErr, applyOut := h.Run(false)

		if (dryErr != nil) != (applyErr != nil) {
			t.Fatalf("%s: dry run and apply disagree: dry=%v apply=%v\n%s\n%s", c.name, dryErr, applyErr, dryOut, applyOut)
		}
		if c.reject == "" {
			if dryErr != nil || dryRes.Clean {
				t.Fatalf("%s: should have been accepted: %v\n%s", c.name, dryErr, dryOut)
			}
			h.S.mustInvoke(t, name+".test", "/")
			mustBeInSync(t, h)
			continue
		}
		if !strings.Contains(dryOut, "rejected by the orchestrator (validate_only)") || !strings.Contains(dryOut, c.reject) {
			t.Fatalf("%s: the dry run does not report the rejection (%q):\n%s", c.name, c.reject, dryOut)
		}
		if _, ok := h.S.functions(t)[name]; ok {
			t.Fatalf("%s: a function that the orchestrator would refuse was left half deployed:\n%s", c.name, applyOut)
		}
		if _, ok := h.S.routes(t)[name+".test"+c.route]; ok {
			t.Fatalf("%s: route created", c.name)
		}
	}

	// A function name the orchestrator refuses is caught at the create step.
	h.R.Commit("badname", files("edgerun.yaml", "functions:\n  - {name: Bad_Name, module: m.wasm}\n"))
	_, dryErr, dryOut := h.Run(true)
	_, applyErr, _ := h.Run(false)
	if dryErr == nil || applyErr == nil || !strings.Contains(dryOut, "function name") {
		t.Fatalf("%v %v\n%s", dryErr, applyErr, dryOut)
	}

	// An invalid owner (the label value) is rejected by every create, and says so.
	h.R.Commit("ok", files("edgerun.yaml", "functions:\n  - {name: owned, module: m.wasm}\n"))
	h.Mutate = func(c *Config) { c.Owner = strings.Repeat("x", 300) }
	_, dryErr, dryOut = h.Run(true)
	if dryErr == nil || !strings.Contains(dryOut, "label") {
		t.Fatalf("%v\n%s", dryErr, dryOut)
	}
	h.Mutate = nil
}

// A change to a function that exists: the step the orchestrator rejects keeps the
// function's other steps from being applied (no new version, no env change), and
// the other functions are not held back.
func TestRejectedStepAppliesNothingOfThatFunction(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("1", files("m.wasm", env.guest["v1"], "m2.wasm", env.guest["v2"], "edgerun.yaml", `
functions:
  - {name: a, module: m.wasm, env: {GREETING: one}}
  - {name: b, module: m.wasm, env: {GREETING: one}}
routes:
  - {host: a.test, path: /, function: a}
  - {host: b.test, path: /, function: b}
`))
	mustRun(t, h)
	h.R.Commit("2", files("edgerun.yaml", `
functions:
  - {name: a, module: m2.wasm, env: {GREETING: two}, egress: ['x.example.com:0']}
  - {name: b, module: m2.wasm, env: {GREETING: two}}
routes:
  - {host: a.test, path: /, function: a}
  - {host: b.test, path: /, function: b}
`))
	a := h.S.functions(t)["a"]
	nv := len(h.S.versions(t, a.Id))
	_, err, out := h.Run(true)
	if err == nil || !strings.Contains(out, "x.example.com:0") {
		t.Fatalf("%v\n%s", err, out)
	}
	_, err, out = h.Run(false)
	if err == nil {
		t.Fatal("the apply must report the rejection")
	}
	if !strings.Contains(out, "skipped a:") {
		t.Fatalf("%s", out)
	}
	if got := h.S.mustInvoke(t, "a.test", "/"); !strings.Contains(got, "version=v1 greeting=one") {
		t.Fatalf("a was changed although a step of it is rejected: %s", got)
	}
	if got := len(h.S.versions(t, a.Id)); got != nv {
		t.Fatalf("a got a new version: %d -> %d", nv, got)
	}
	if got := h.S.mustInvoke(t, "b.test", "/"); !strings.Contains(got, "version=v2 greeting=two") {
		t.Fatalf("b was held back: %s", got)
	}
}

// A retarget is a delete and a create of the same (host, path); the create cannot
// be validated while the route is still there. The dry run says so rather than
// pretending, and the apply works.
func TestRetargetIsNotValidatedAhead(t *testing.T) {
	h := newHarness(t)
	two := func(target string) map[string][]byte {
		return files("edgerun.yaml", "functions:\n  - {name: a, module: m.wasm}\n  - {name: b, module: m.wasm, env: {GREETING: bee}}\nroutes:\n  - {host: r.test, path: /, function: "+target+"}\n")
	}
	h.R.Commit("1", files("m.wasm", env.guest["v1"]))
	h.R.Commit("a", two("a"))
	mustRun(t, h)
	h.R.Commit("b", two("b"))
	_, err, out := h.Run(true)
	if err != nil || !strings.Contains(out, "(not validated ahead: the route it replaces is deleted first)") {
		t.Fatalf("%v\n%s", err, out)
	}
	mustRun(t, h)
	if got := h.S.mustInvoke(t, "r.test", "/"); !strings.Contains(got, "greeting=bee") {
		t.Fatal(got)
	}
	mustBeInSync(t, h)
}

// validate_only answers with the error the call itself gives: same code, same
// message (protocol.md says so; the dry run's reports are only as good as that).
func TestValidateOnlyGivesTheCallsOwnError(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ctl := h.S.Ctl
	fn, err := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	id := fn.Function.Id
	for name, call := range map[string]func(vo bool) error{
		"SetEgress": func(vo bool) error {
			_, err := ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: id, Allow: []string{"a.example.com:99999"}, ValidateOnly: vo})
			return err
		},
		"SetEnv": func(vo bool) error {
			_, err := ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: id, Entry: &controlv1.EnvEntry{Name: "PATH_INFO", Value: "x"}, ValidateOnly: vo})
			return err
		},
		"CreateRoute": func(vo bool) error {
			_, err := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "h.test", PathPrefix: "/a//b", FunctionId: id, ValidateOnly: vo})
			return err
		},
		"SetLabels": func(vo bool) error {
			_, err := ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: id, Labels: map[string]string{"Upper": "x"}, ValidateOnly: vo})
			return err
		},
		"CreateFunction (taken)": func(vo bool) error {
			_, err := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "x", ValidateOnly: vo})
			return err
		},
	} {
		dry, real := call(true), call(false)
		if dry == nil || real == nil || dry.Error() != real.Error() || connect.CodeOf(dry) != connect.CodeOf(real) {
			t.Errorf("%s: validate-only says %v, the call says %v", name, dry, real)
		}
	}
}
