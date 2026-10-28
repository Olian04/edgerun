package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
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

// The rules below are copied from docs/protocol.md ("Control semantics"). The
// protocol has no validate-only call, so a dry run can only know a request is
// acceptable if the client re-implements the server's rules. Every one of them
// is a place where this program can disagree with the orchestrator.
var (
	nameRe    = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	reserved  = map[string]bool{
		"GATEWAY_INTERFACE": true, "SERVER_PROTOCOL": true, "REQUEST_METHOD": true, "SERVER_NAME": true,
		"PATH_INFO": true, "QUERY_STRING": true, "REMOTE_ADDR": true, "CONTENT_LENGTH": true, "CONTENT_TYPE": true,
	}
	abiRe = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
)

const (
	maxEnvVars   = 64
	maxEnvValue  = 32 << 10
	maxEgress    = 64
	defaultABI   = "wagi-1"
	markerEnvVar = "GITOPS_STATE" // ours: ownership and secret fingerprints, see marker.go
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
// Without them every run would delete and recreate the route.
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

func (s *Spec) Validate() error {
	var errs []error
	add := func(f string, a ...any) { errs = append(errs, fmt.Errorf(f, a...)) }
	names := map[string]bool{}
	for _, f := range s.Functions {
		if !nameRe.MatchString(f.Name) {
			add("function %q: name must be 1-63 lower-case letters, digits or '-', not starting or ending with '-'", f.Name)
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
		if !abiRe.MatchString(f.ABI) {
			add("function %q: abi %q is not a short lower-case name", f.Name, f.ABI)
		}
		n := 1 // our own marker variable
		check := func(name, val string) {
			n++
			switch {
			case !envNameRe.MatchString(name):
				add("function %q: env %q: not a valid variable name", f.Name, name)
			case reserved[name] || strings.HasPrefix(name, "HTTP_"):
				add("function %q: env %q: reserved by the guest ABI", f.Name, name)
			case name == markerEnvVar:
				add("function %q: env %q: reserved by gitops", f.Name, name)
			case len(val) > maxEnvValue:
				add("function %q: env %q: value over %d bytes", f.Name, name, maxEnvValue)
			case strings.ContainsRune(val, 0):
				add("function %q: env %q: value contains NUL", f.Name, name)
			}
		}
		for k, v := range f.Env {
			check(k, v)
			if _, dup := f.SecretEnv[k]; dup {
				add("function %q: %q is both env and secret_env", f.Name, k)
			}
		}
		for k, ref := range f.SecretEnv {
			check(k, "")
			if !envNameRe.MatchString(ref) {
				add("function %q: secret_env %q: %q is not a variable name", f.Name, k, ref)
			}
		}
		if n > maxEnvVars {
			add("function %q: %d variables (including gitops' own %s), at most %d", f.Name, n, markerEnvVar, maxEnvVars)
		}
		if len(f.Egress) > maxEgress {
			add("function %q: %d egress entries, at most %d", f.Name, len(f.Egress), maxEgress)
		}
		for _, e := range f.Egress {
			if e == "*" || strings.ContainsAny(e, " \t/") || strings.Contains(e, "://") {
				add("function %q: egress %q: want host, host:port, *.suffix or an IP literal", f.Name, e)
			}
		}
	}
	keys := map[string]bool{}
	for _, r := range s.Routes {
		id := fmt.Sprintf("route %s%s", r.Host, r.Path)
		if r.Host == "" {
			add("%s: host is required", id)
		}
		if !strings.HasPrefix(r.Path, "/") || strings.Contains(r.Path, "//") || strings.ContainsAny(r.Path, "?# \t\n") {
			add("%s: path must start with '/' and may not contain '//', '?', '#' or whitespace", id)
		}
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
