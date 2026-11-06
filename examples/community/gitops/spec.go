package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Spec is the desired state, as written in the YAML file of the repository.
//
//	functions:
//	  - name: hello
//	    module: build/hello.wasm        # path inside the repo
//	    abi: wagi-1
//	    limits: {memory_mib: 64, max_response_kib: 256}
//	    env: {GREETING: Howdy}
//	    secret_env: {API_KEY: HELLO_API_KEY}   # name -> variable of the GitOps process
//	    egress: [api.example.com]
//	routes:
//	  - {host: hello.example.com, path: /, function: hello}
type Spec struct {
	Functions []FunctionSpec `yaml:"functions"`
	Routes    []RouteSpec    `yaml:"routes"`
}

type FunctionSpec struct {
	Name      string            `yaml:"name"`
	Module    string            `yaml:"module"`
	ABI       string            `yaml:"abi"`
	Limits    *LimitsSpec       `yaml:"limits"`
	Env       map[string]string `yaml:"env"`
	SecretEnv map[string]string `yaml:"secret_env"`
	Egress    []string          `yaml:"egress"`
}

type LimitsSpec struct {
	MemoryMiB      uint64 `yaml:"memory_mib"`
	MaxResponseKiB uint64 `yaml:"max_response_kib"`
}

type RouteSpec struct {
	Host     string `yaml:"host"`
	Path     string `yaml:"path"`
	Function string `yaml:"function"`
}

// maxEnvVars is the one server rule this program still carries: a function
// holds at most 64 variables. It no longer decides anything: whether a plan fits
// under it, stale variables deleted first, is answered by PlanService.Validate,
// which runs the deletions before the writes. What is left is a copy kept for three
// reasons the protocol gives no way around (Describe reports no limits): a file
// that declares more than 64 is refused before any network call, the planner
// puts the deletions first when the new variables would not fit beside the
// stale ones (an ordering choice, which Validate then checks), and the record of
// secret revisions is sized for 64 secrets. Everything else the server checks
// (names, reserved variables, sizes, egress syntax, label syntax...) is left to
// the server, through Validate.
const (
	maxEnvVars = 64
	defaultABI = "wagi-1"
)

// ParseSpec decodes and validates the YAML. Unknown keys are errors: a typo in
// a GitOps file must not silently mean "no egress list".
func ParseSpec(data []byte) (*Spec, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var s Spec
	if err := dec.Decode(&s); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	s.normalize()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *Spec) normalize() {
	for i := range s.Functions {
		f := &s.Functions[i]
		if f.ABI == "" {
			f.ABI = defaultABI
		}
		f.Egress = canonEgress(f.Egress)
	}
	for i := range s.Routes {
		r := &s.Routes[i]
		r.Host = normHost(r.Host)
		r.Path = normPrefix(r.Path)
	}
}

// normHost and normPrefix mirror what the orchestrator does on write, so that
// "the route in the YAML" and "the route the server returns" compare equal.
// Without them every run would delete and recreate the route. This is not
// validation: it is the price of comparing a file with stored state.
func normHost(h string) string { return strings.ToLower(h) }

func normPrefix(p string) string {
	if len(p) > 1 {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}

// canonEgress mirrors "stored lower-cased, deduplicated and sorted".
func canonEgress(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range in {
		e = strings.ToLower(strings.TrimSpace(e))
		if e == "" || seen[e] {
			continue
		}
		seen[e] = true
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// Validate checks what only this program can know: the file's own consistency
// (a name declared twice, a route to a function the file does not declare, two
// routes that are one after normalisation, a variable that is both plain and
// secret) and that a module path stays inside the repository. Whether the
// orchestrator will accept each value is asked of the orchestrator.
func (s *Spec) Validate() error {
	var errs []error
	add := func(f string, a ...any) { errs = append(errs, fmt.Errorf(f, a...)) }
	names := map[string]bool{}
	for _, f := range s.Functions {
		if f.Name == "" {
			add("a function has no name")
			continue
		}
		if names[f.Name] {
			add("function %q: declared twice", f.Name)
		}
		names[f.Name] = true
		if f.Module == "" {
			add("function %q: module is required", f.Name)
		} else if err := checkRepoPath(f.Module); err != nil {
			add("function %q: module: %v", f.Name, err)
		}
		for k := range f.Env {
			if _, dup := f.SecretEnv[k]; dup {
				add("function %q: %q is both env and secret_env", f.Name, k)
			}
		}
		if n := len(f.Env) + len(f.SecretEnv); n > maxEnvVars {
			add("function %q: %d variables, at most %d", f.Name, n, maxEnvVars)
		}
	}
	keys := map[string]bool{}
	for _, r := range s.Routes {
		id := fmt.Sprintf("route %s%s", r.Host, r.Path)
		if !names[r.Function] {
			add("%s: function %q is not declared in this file", id, r.Function)
		}
		k := routeKey(r.Host, r.Path)
		if keys[k] {
			add("%s: declared twice (after normalisation)", id)
		}
		keys[k] = true
	}
	return errors.Join(errs...)
}

func routeKey(host, prefix string) string { return host + "\x00" + prefix }

// checkRepoPath keeps a module path inside the repository.
func checkRepoPath(p string) error {
	if path.IsAbs(p) {
		return errors.New("must be relative to the repository root")
	}
	c := path.Clean(p)
	if c == ".." || strings.HasPrefix(c, "../") || c == "." {
		return errors.New("must stay inside the repository")
	}
	return nil
}
