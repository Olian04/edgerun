package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestParseSpecNormalises(t *testing.T) {
	s, err := ParseSpec([]byte(`
functions:
  - name: a
    module: m.wasm
    env: {PORT: 8080, DEBUG: true, NAME: x}
    egress: ["B.example.com", "a.example.com", "b.example.com"]
routes:
  - {host: A.Example.COM, path: /api/, function: a}
  - {host: "*", path: /, function: a}
`))
	if err != nil {
		t.Fatal(err)
	}
	f := s.Functions[0]
	if f.ABI != "wagi-1" {
		t.Errorf("abi default: %q", f.ABI)
	}
	if got := strings.Join(f.Egress, ","); got != "a.example.com,b.example.com" {
		t.Errorf("egress: %s", got)
	}
	if s.Routes[0].Host != "a.example.com" || s.Routes[0].Path != "/api" || s.Routes[1].Path != "/" {
		t.Errorf("routes: %+v", s.Routes)
	}
	// YAML scalars that are not strings still arrive as the text they were written as.
	if f.Env["PORT"] != "8080" || f.Env["DEBUG"] != "true" {
		t.Errorf("env: %v", f.Env)
	}
}

// What ParseSpec rejects is what only this program can know: the file's own
// consistency and the repository boundary. Everything the orchestrator has an
// opinion on is left to it (validate_only), see TestDryRunAgreesWithApply.
func TestParseSpecRejects(t *testing.T) {
	base := "functions:\n  - {name: a, module: m.wasm}\n"
	var many strings.Builder
	many.WriteString("functions:\n  - name: a\n    module: m.wasm\n    env:\n")
	for i := 0; i < 65; i++ {
		fmt.Fprintf(&many, "      V%d: x\n", i)
	}
	for name, c := range map[string]struct{ yaml, want string }{
		"unknown key":     {"functions:\n  - name: a\n    module: m.wasm\n    egres: [x]\n", "field egres not found"},
		"duplicate":       {base + "  - {name: a, module: m.wasm}\n", "declared twice"},
		"no name":         {"functions:\n  - {module: m.wasm}\n", "no name"},
		"no module":       {"functions:\n  - {name: a}\n", "module is required"},
		"module escapes":  {"functions:\n  - {name: a, module: ../x.wasm}\n", "inside the repository"},
		"absolute module": {"functions:\n  - {name: a, module: /x.wasm}\n", "relative"},
		"env and secret":  {"functions:\n  - {name: a, module: m.wasm, env: {A: x}, secret_env: {A: Y}}\n", "both"},
		"65 variables":    {many.String(), "at most 64"},
		"route no fn":     {base + "routes:\n  - {host: h, path: /, function: zz}\n", "not declared"},
		"route dup":       {base + "routes:\n  - {host: H, path: /x/, function: a}\n  - {host: h, path: /x, function: a}\n", "declared twice"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSpec([]byte(c.yaml))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want %q, got %v", c.want, err)
			}
		})
	}
}

// The rules that used to be copied from protocol.md are not copied any more: a
// file the orchestrator would refuse parses, and the refusal comes from it.
func TestParseSpecLeavesServerRulesToTheServer(t *testing.T) {
	for name, y := range map[string]string{
		"bad name":     "functions:\n  - {name: A_b, module: m.wasm}\n",
		"reserved env": "functions:\n  - {name: a, module: m.wasm, env: {PATH_INFO: x}}\n",
		"bad env name": "functions:\n  - {name: a, module: m.wasm, env: {1A: x}}\n",
		"star egress":  "functions:\n  - {name: a, module: m.wasm, egress: ['*']}\n",
		"bad route":    "functions:\n  - {name: a, module: m.wasm}\nroutes:\n  - {host: h, path: a//b, function: a}\n",
	} {
		if _, err := ParseSpec([]byte(y)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The record of secret revisions fits the label rules of control.v1 for the
// largest function there can be (64 secrets), and round-trips.
func TestSecretRecordFitsLabels(t *testing.T) {
	revs := map[string]string{}
	for i := 0; i < maxEnvVars; i++ {
		revs[fmt.Sprintf("SECRET_%d", i)] = fmt.Sprintf("%032x", i)
	}
	labels := functionLabels(map[string]string{"team": "x", labelSecret + "9": "stale"}, "o", revs)
	if labels["team"] != "x" || labels[labelOwner] != "o" {
		t.Fatalf("foreign label or owner lost: %v", labels)
	}
	if _, ok := labels[labelSecret+"9"]; ok {
		t.Fatal("a stale record label survived")
	}
	if len(labels) > 32 {
		t.Fatalf("%d labels, the limit is 32", len(labels))
	}
	keyRe := regexp.MustCompile(`^[a-z0-9]([a-z0-9._/-]{0,61}[a-z0-9])?$`)
	for k, v := range labels {
		if !keyRe.MatchString(k) || len(v) > 253 {
			t.Errorf("label %q (%d chars) breaks the rules", k, len(v))
		}
	}
	rec := decodeSecrets(labels)
	if len(rec) != maxEnvVars {
		t.Fatalf("decoded %d of %d", len(rec), maxEnvVars)
	}
	for n, r := range revs {
		if rec[nameTag(n)] != valueTag(n, r) {
			t.Fatalf("%s did not round-trip", n)
		}
		if valueTag(n, r) == valueTag(n, r+"x") {
			t.Fatal("the tag ignores the revision")
		}
	}
	if len(encodeSecrets(nil)) != 0 {
		t.Fatal("no secrets must record nothing")
	}
}
