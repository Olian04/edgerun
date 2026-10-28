package main

import (
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

func TestParseSpecRejects(t *testing.T) {
	base := "functions:\n  - {name: a, module: m.wasm}\n"
	for name, c := range map[string]struct{ yaml, want string }{
		"unknown key":      {"functions:\n  - name: a\n    module: m.wasm\n    egres: [x]\n", "field egres not found"},
		"bad name":         {"functions:\n  - {name: A_b, module: m.wasm}\n", "name must be"},
		"leading dash":     {"functions:\n  - {name: -a, module: m.wasm}\n", "name must be"},
		"duplicate":        {base + "  - {name: a, module: m.wasm}\n", "declared twice"},
		"no module":        {"functions:\n  - {name: a}\n", "module is required"},
		"module escapes":   {"functions:\n  - {name: a, module: ../x.wasm}\n", "inside the repository"},
		"absolute module":  {"functions:\n  - {name: a, module: /x.wasm}\n", "relative"},
		"reserved env":     {"functions:\n  - {name: a, module: m.wasm, env: {PATH_INFO: x}}\n", "reserved"},
		"HTTP_ env":        {"functions:\n  - {name: a, module: m.wasm, env: {HTTP_X: x}}\n", "reserved"},
		"marker env":       {"functions:\n  - {name: a, module: m.wasm, env: {GITOPS_STATE: x}}\n", "reserved by gitops"},
		"bad env name":     {"functions:\n  - {name: a, module: m.wasm, env: {1A: x}}\n", "valid variable name"},
		"env and secret":   {"functions:\n  - {name: a, module: m.wasm, env: {A: x}, secret_env: {A: Y}}\n", "both"},
		"star egress":      {"functions:\n  - {name: a, module: m.wasm, egress: ['*']}\n", "egress"},
		"route no fn":      {base + "routes:\n  - {host: h, path: /, function: zz}\n", "not declared"},
		"route bad prefix": {base + "routes:\n  - {host: h, path: /a//b, function: a}\n", "path must start"},
		"route no slash":   {base + "routes:\n  - {host: h, path: a, function: a}\n", "path must start"},
		"route dup":        {base + "routes:\n  - {host: H, path: /x/, function: a}\n  - {host: h, path: /x, function: a}\n", "declared twice"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSpec([]byte(c.yaml))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want %q, got %v", c.want, err)
			}
		})
	}
}

func TestMarkerRoundTrip(t *testing.T) {
	m := marker{Owner: "o", Secrets: map[string]string{"B": "2", "A": "1"}}
	got, ok := parseMarker(m.encode())
	if !ok || got.Owner != "o" || got.Secrets["A"] != "1" {
		t.Fatalf("%v %v", got, ok)
	}
	if m.encode() != `{"owner":"o","secrets":{"A":"1","B":"2"}}` {
		t.Fatalf("not canonical: %s", m.encode())
	}
	if _, ok := parseMarker("hello"); ok {
		t.Fatal("garbage parsed")
	}
	if fingerprint([]byte("k1"), "o", "f", "n", "v") == fingerprint([]byte("k2"), "o", "f", "n", "v") {
		t.Fatal("fingerprint ignores the key")
	}
}
