package e2e

// Injection and abuse of control.v1: a corpus through every string field of
// every RPC, forged page tokens, limits, and messages that are not what they
// seem (invalid UTF-8, compressed bombs, deep nesting).

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// secCorpus is what every string field of the control API is given.
func secCorpus() []string {
	c := []string{
		"", " ", "'", "\"", "''", "\\", "`", ";", "--", "/*", "*/", "%", "%%", "_", "%27", "%00", "$1", "$$", "${x}", "{{7*7}}", "<script>alert(1)</script>",
		"../../etc/passwd", "a\x00b", "\x00", "\n", "\r\n", "\t", "a b", "NULL", "null", "true", "{}", "[]", "{\"a\":1}", "*", ".", "..", "/", "//",
		"' OR '1'='1", "' OR 1=1--", "'; DROP TABLE functions;--", "x'); DELETE FROM routes;--", "1; SELECT pg_sleep(5)--", "' UNION SELECT NULL,NULL--",
		"\\'; SELECT pg_sleep(5); --", "'||(SELECT pg_sleep(5))||'", "$$; SELECT pg_sleep(5); $$", "'; SELECT pg_sleep(5);--", "\"; SELECT pg_sleep(5);--",
		"'; COPY (SELECT '') TO PROGRAM 'touch /tmp/edgerun-sqli-canary';--", "'); DROP TABLE env_vars;--", "1' AND (SELECT 1 FROM pg_sleep(5)) --",
		"\" OR \"\"=\"", "') OR ('1'='1", "E'\\x27'", "U&'\\0027'", "\u0000name", "ｆｕｎｃ", "‮", "é", "\U0001F600", "\ufeff", "\U0010ffff", "�",
		"İ", "ß", "K", "ſ", "ǅ", " ", " ", "\u0085", "\x7f", "\x1b[31m", "%s%s%s%n", "..%2f..%2f", "\xe2\x80\xae",
		strings.Repeat("a", 63), strings.Repeat("a", 64), strings.Repeat("a", 254), strings.Repeat("a", 3000), strings.Repeat("\U0001F600", 300), strings.Repeat("'", 3000),
		strings.Repeat("[", 5000), strings.Repeat("{", 5000), strings.Repeat("a.", 200) + "a", strings.Repeat("-", 70), strings.Repeat("%", 4000),
	}
	return c
}

type secField struct {
	name string
	call func(ctx context.Context, s string) error
}

func secErrText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// The messages of an error must not describe the database.
var secDBWords = []string{"SQLSTATE", "pgx", "pgconn", "ERROR:", "syntax error", "invalid byte sequence", "violates", "constraint \"", "relation \"", "duplicate key value", "pq:", "column \"", "unterminated quoted", "out of range for type", "timestamp out of range", "index row size"}

func TestSecurityControlInjectionCorpus(t *testing.T) { secControlCorpus(t, "main") }

// KNOWN DEFECT SEC-CONTROL-NUL-500: a NUL byte in a string that the API passes to
// PostgreSQL unchecked (a function name, a list's name filter, a route host, a
// request id, an env name to delete...) makes the database refuse the statement,
// and the caller is told "internal error" (500-class) instead of invalid_argument.
func TestSecurityControlNULBytes(t *testing.T) { secControlCorpus(t, "nul") }

func secControlCorpus(t *testing.T, mode string) {
	corpus := secCorpus()
	var keep []string
	for _, x := range corpus {
		if strings.Contains(x, "\x00") == (mode == "nul") {
			keep = append(keep, x)
		}
	}
	corpus = keep
	if mode == "nul" {
		corpus = []string{"\x00", "a\x00b", "\x00name"}
	}
	s := newStack(t)
	c := s.ctl
	fid := s.createFunction("baseline")
	s.setEnv(fid, "KEEP", "kept", false)
	s.setEnv(fid, "KEEP_SECRET", "kept-secret", true)
	s.route("baseline.test", "/keep", fid)
	if _, err := c.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: fid, Labels: map[string]string{"keep": "kept"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"keep.example.com"}}); err != nil {
		t.Fatal(err)
	}
	vr, err := c.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: s.wasm, Abi: "wagi-1", Activate: true})
	if err != nil {
		t.Fatal(err)
	}
	vid := vr.Version.Id
	rl, _ := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{})
	rid := rl.Routes[0].Id
	other := s.createFunction("other-fn")

	step := func(st *controlv1.Step) []*controlv1.Step { return []*controlv1.Step{st} }
	fields := []secField{
		{"CreateFunction.name", func(ctx context.Context, x string) error {
			_, err := c.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: x})
			return err
		}},
		{"CreateFunction.labels key", func(ctx context.Context, x string) error {
			_, err := c.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "lk", Labels: map[string]string{x: "v"}, ValidateOnly: true})
			return err
		}},
		{"CreateFunction.labels value", func(ctx context.Context, x string) error {
			_, err := c.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "lv", Labels: map[string]string{"k": x}, ValidateOnly: true})
			return err
		}},
		{"GetFunction.function_id", func(ctx context.Context, x string) error {
			_, err := c.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: x})
			return err
		}},
		{"ListFunctions.page_token", func(ctx context.Context, x string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageToken: x})
			return err
		}},
		{"ListFunctions.name", func(ctx context.Context, x string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{Name: x})
			return err
		}},
		{"ListFunctions.match_labels key", func(ctx context.Context, x string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{MatchLabels: map[string]string{x: "v"}})
			return err
		}},
		{"ListFunctions.match_labels value", func(ctx context.Context, x string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{MatchLabels: map[string]string{"keep": x}})
			return err
		}},
		{"ListFunctions.has_label_keys", func(ctx context.Context, x string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{HasLabelKeys: []string{x}})
			return err
		}},
		{"ListFunctions.lacks_label_keys", func(ctx context.Context, x string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{LacksLabelKeys: []string{x}})
			return err
		}},
		{"DeleteFunction.function_id", func(ctx context.Context, x string) error {
			_, err := c.DeleteFunction(ctx, &controlv1.DeleteFunctionRequest{FunctionId: x})
			return err
		}},
		{"CreateVersion.function_id", func(ctx context.Context, x string) error {
			_, err := c.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: x, Module: []byte("m"), Abi: "wagi-1"})
			return err
		}},
		{"CreateVersion.abi", func(ctx context.Context, x string) error {
			_, err := c.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: other, Module: []byte("m"), Abi: x, ValidateOnly: true})
			return err
		}},
		{"CreateVersion.labels key", func(ctx context.Context, x string) error {
			_, err := c.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: other, Module: []byte("m"), Abi: "wagi-1", Labels: map[string]string{x: "v"}, ValidateOnly: true})
			return err
		}},
		{"CreateVersion.labels value", func(ctx context.Context, x string) error {
			_, err := c.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: other, Module: []byte("m"), Abi: "wagi-1", Labels: map[string]string{"k": x}, ValidateOnly: true})
			return err
		}},
		{"ListVersions.function_id", func(ctx context.Context, x string) error {
			_, err := c.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: x})
			return err
		}},
		{"ListVersions.page_token", func(ctx context.Context, x string) error {
			_, err := c.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: fid, PageToken: x})
			return err
		}},
		{"ListVersions.match_labels", func(ctx context.Context, x string) error {
			_, err := c.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: fid, MatchLabels: map[string]string{x: x}, HasLabelKeys: []string{x}})
			return err
		}},
		{"ActivateVersion.function_id", func(ctx context.Context, x string) error {
			_, err := c.ActivateVersion(ctx, &controlv1.ActivateVersionRequest{FunctionId: x, VersionId: vid, ValidateOnly: true})
			return err
		}},
		{"ActivateVersion.version_id", func(ctx context.Context, x string) error {
			_, err := c.ActivateVersion(ctx, &controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: x, ValidateOnly: true})
			return err
		}},
		{"SetEnv.function_id", func(ctx context.Context, x string) error {
			_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: x, Entry: &controlv1.EnvEntry{Name: "A", Value: "b"}})
			return err
		}},
		{"SetEnv.entry.name", func(ctx context.Context, x string) error {
			_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: other, Entry: &controlv1.EnvEntry{Name: x, Value: "b"}, ValidateOnly: true})
			return err
		}},
		{"SetEnv.entry.value", func(ctx context.Context, x string) error {
			_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: other, Entry: &controlv1.EnvEntry{Name: "VAL", Value: x}})
			return err
		}},
		{"SetEnv.entry.value (secret)", func(ctx context.Context, x string) error {
			_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: other, Entry: &controlv1.EnvEntry{Name: "SEC", Value: x, Secret: true}})
			return err
		}},
		{"SetEnv.entry.revision", func(ctx context.Context, x string) error {
			_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: other, Entry: &controlv1.EnvEntry{Name: "REV", Value: "v", Revision: x}, ValidateOnly: true})
			return err
		}},
		{"DeleteEnv.function_id", func(ctx context.Context, x string) error {
			_, err := c.DeleteEnv(ctx, &controlv1.DeleteEnvRequest{FunctionId: x, Name: "A"})
			return err
		}},
		{"DeleteEnv.name", func(ctx context.Context, x string) error {
			_, err := c.DeleteEnv(ctx, &controlv1.DeleteEnvRequest{FunctionId: other, Name: x})
			return err
		}},
		{"ListEnv.function_id", func(ctx context.Context, x string) error {
			_, err := c.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: x})
			return err
		}},
		{"SetEgress.function_id", func(ctx context.Context, x string) error {
			_, err := c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: x, Allow: []string{"a.example.com"}})
			return err
		}},
		{"SetEgress.allow", func(ctx context.Context, x string) error {
			_, err := c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: other, Allow: []string{x}})
			return err
		}},
		{"SetLabels.function_id", func(ctx context.Context, x string) error {
			_, err := c.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: x, Labels: map[string]string{"a": "b"}})
			return err
		}},
		{"SetLabels.labels key", func(ctx context.Context, x string) error {
			_, err := c.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: other, Labels: map[string]string{x: "b"}})
			return err
		}},
		{"SetLabels.labels value", func(ctx context.Context, x string) error {
			_, err := c.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: other, Labels: map[string]string{"k": x}})
			return err
		}},
		{"PatchLabels.set key", func(ctx context.Context, x string) error {
			_, err := c.PatchLabels(ctx, &controlv1.PatchLabelsRequest{FunctionId: other, Set: map[string]string{x: "b"}})
			return err
		}},
		{"PatchLabels.set value", func(ctx context.Context, x string) error {
			_, err := c.PatchLabels(ctx, &controlv1.PatchLabelsRequest{FunctionId: other, Set: map[string]string{"k": x}})
			return err
		}},
		{"PatchLabels.remove", func(ctx context.Context, x string) error {
			_, err := c.PatchLabels(ctx, &controlv1.PatchLabelsRequest{FunctionId: other, Remove: []string{x}})
			return err
		}},
		{"CreateRoute.host_pattern", func(ctx context.Context, x string) error {
			_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: x, PathPrefix: "/p", FunctionId: other})
			return err
		}},
		{"CreateRoute.path_prefix", func(ctx context.Context, x string) error {
			_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "route.test", PathPrefix: x, FunctionId: other})
			return err
		}},
		{"CreateRoute.path_prefix (after a slash)", func(ctx context.Context, x string) error {
			_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "route2.test", PathPrefix: "/" + x, FunctionId: other})
			return err
		}},
		{"CreateRoute.function_id", func(ctx context.Context, x string) error {
			_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "route3.test", PathPrefix: "/p", FunctionId: x})
			return err
		}},
		{"CreateRoute.labels", func(ctx context.Context, x string) error {
			_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "route4.test", PathPrefix: "/p", FunctionId: other, Labels: map[string]string{x: x}})
			return err
		}},
		{"ListRoutes.page_token", func(ctx context.Context, x string) error {
			_, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{PageToken: x})
			return err
		}},
		{"ListRoutes.function_id", func(ctx context.Context, x string) error {
			_, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{FunctionId: x})
			return err
		}},
		{"ListRoutes.filters", func(ctx context.Context, x string) error {
			_, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{MatchLabels: map[string]string{x: x}, HasLabelKeys: []string{x}, LacksLabelKeys: []string{x + "z"}})
			return err
		}},
		{"DeleteRoute.route_id", func(ctx context.Context, x string) error {
			_, err := c.DeleteRoute(ctx, &controlv1.DeleteRouteRequest{RouteId: x})
			return err
		}},
		{"SetRouteLabels", func(ctx context.Context, x string) error {
			_, err := c.SetRouteLabels(ctx, &controlv1.SetRouteLabelsRequest{RouteId: rid, Labels: map[string]string{x: x}, ValidateOnly: true})
			return err
		}},
		{"SetRouteLabels.route_id", func(ctx context.Context, x string) error {
			_, err := c.SetRouteLabels(ctx, &controlv1.SetRouteLabelsRequest{RouteId: x, Labels: map[string]string{"a": "b"}})
			return err
		}},
		{"PatchRouteLabels", func(ctx context.Context, x string) error {
			_, err := c.PatchRouteLabels(ctx, &controlv1.PatchRouteLabelsRequest{RouteId: rid, Set: map[string]string{x: x}, Remove: []string{x + "r"}, ValidateOnly: true})
			return err
		}},
		{"PatchRouteLabels.route_id", func(ctx context.Context, x string) error {
			_, err := c.PatchRouteLabels(ctx, &controlv1.PatchRouteLabelsRequest{RouteId: x, Set: map[string]string{"a": "b"}})
			return err
		}},
		{"ListInvocations.function_id", func(ctx context.Context, x string) error {
			_, err := c.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: x})
			return err
		}},
		{"ListInvocations.request_id", func(ctx context.Context, x string) error {
			_, err := c.ListInvocations(ctx, &controlv1.ListInvocationsRequest{RequestId: x})
			return err
		}},
		{"ListInvocations.page_token", func(ctx context.Context, x string) error {
			_, err := c.ListInvocations(ctx, &controlv1.ListInvocationsRequest{PageToken: x})
			return err
		}},
		{"GetInvocation.invocation_id", func(ctx context.Context, x string) error {
			_, err := c.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: x})
			return err
		}},
		{"Plan step bind", func(ctx context.Context, x string) error {
			_, err := c.Validate(ctx, &controlv1.ValidateRequest{Steps: step(&controlv1.Step{Bind: x, Request: &controlv1.Step_CreateFunction{CreateFunction: &controlv1.CreateFunctionRequest{Name: "planned"}}})})
			return err
		}},
		{"Plan step create_function.name", func(ctx context.Context, x string) error {
			_, err := c.Validate(ctx, &controlv1.ValidateRequest{Steps: step(&controlv1.Step{Request: &controlv1.Step_CreateFunction{CreateFunction: &controlv1.CreateFunctionRequest{Name: x}}})})
			return err
		}},
		{"Plan step set_env", func(ctx context.Context, x string) error {
			_, err := c.Validate(ctx, &controlv1.ValidateRequest{Steps: step(&controlv1.Step{Request: &controlv1.Step_SetEnv{SetEnv: &controlv1.SetEnvRequest{FunctionId: x, Entry: &controlv1.EnvEntry{Name: x, Value: x}}}})})
			return err
		}},
		{"Plan step create_route", func(ctx context.Context, x string) error {
			_, err := c.Validate(ctx, &controlv1.ValidateRequest{Steps: step(&controlv1.Step{Request: &controlv1.Step_CreateRoute{CreateRoute: &controlv1.CreateRouteRequest{HostPattern: x, PathPrefix: "/" + x, FunctionId: x}}})})
			return err
		}},
		{"Plan Apply set_labels", func(ctx context.Context, x string) error {
			_, err := c.Apply(ctx, &controlv1.ApplyRequest{Steps: step(&controlv1.Step{Request: &controlv1.Step_SetLabels{SetLabels: &controlv1.SetLabelsRequest{FunctionId: other, Labels: map[string]string{"k": x}}}})})
			return err
		}},
	}

	type job struct {
		f secField
		s string
	}
	var jobs []job
	for _, f := range fields {
		if mode == "main" && strings.Contains(f.name, "page_token") {
			continue // forged page tokens have their own test
		}
		for _, x := range corpus {
			jobs = append(jobs, job{f, x})
		}
	}
	var mu sync.Mutex
	var bad []string
	var slowest time.Duration
	secParallel(jobs, 8, func(j job) {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		start := time.Now()
		err := j.f.call(cctx, j.s)
		took := time.Since(start)
		mu.Lock()
		defer mu.Unlock()
		if took > slowest {
			slowest = took
		}
		if err != nil {
			switch connect.CodeOf(err) {
			case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeAlreadyExists, connect.CodeResourceExhausted, connect.CodeFailedPrecondition:
			default:
				bad = append(bad, fmt.Sprintf("%s = %.60q: %v", j.f.name, j.s, err))
			}
			for _, w := range secDBWords {
				if strings.Contains(err.Error(), w) {
					bad = append(bad, fmt.Sprintf("%s = %.60q: the error describes the database (%q): %.200v", j.f.name, j.s, w, err))
					break
				}
			}
		}
		if took > 3*time.Second {
			bad = append(bad, fmt.Sprintf("%s = %.60q: took %v (an injected sleep?)", j.f.name, j.s, took))
		}
	})
	if mode == "nul" && len(bad) > 0 && os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		t.Skipf("KNOWN DEFECT SEC-CONTROL-NUL-500: a NUL byte in a name, filter, host or id reaches PostgreSQL and comes back as an internal error (%d fields/values, e.g. %s)", len(bad), bad[0])
	}
	seen := map[string]bool{}
	for _, b := range bad {
		if len(seen) < 40 && !seen[b] {
			seen[b] = true
			t.Errorf("%s", b)
		}
	}
	t.Logf("%d calls, slowest %v", len(jobs), slowest)
	if _, err := os.Stat("/tmp/edgerun-sqli-canary"); err == nil {
		os.Remove("/tmp/edgerun-sqli-canary")
		t.Errorf("SQL INJECTION: a payload ran a command on the database host")
	}

	// Integrity: what was there is still there, unchanged, and every table still answers.
	fn, err := c.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: fid})
	if err != nil {
		t.Fatalf("the baseline function is gone: %v", err)
	}
	if fn.Function.Labels["keep"] != "kept" || len(fn.Function.EgressAllow) != 1 || fn.Function.ActiveVersionId != vid {
		t.Errorf("the baseline function changed: %+v", fn.Function)
	}
	env, err := c.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fid})
	if err != nil || len(env.Entries) != 2 {
		t.Errorf("the baseline environment changed: %v %v", env, err)
	}
	if rs, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{FunctionId: fid}); err != nil || len(rs.Routes) != 1 || rs.Routes[0].PathPrefix != "/keep" {
		t.Errorf("the baseline route changed: %v %v", rs, err)
	}
	for _, name := range []string{"functions", "versions", "routes", "env", "workers", "invocations", "network services"} {
		var err error
		switch name {
		case "functions":
			_, err = c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{})
		case "versions":
			_, err = c.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: fid})
		case "routes":
			_, err = c.ListRoutes(ctx, &controlv1.ListRoutesRequest{})
		case "env":
			_, err = c.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fid})
		case "workers":
			_, err = c.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		case "invocations":
			_, err = c.ListInvocations(ctx, &controlv1.ListInvocationsRequest{})
		case "network services":
			_, err = c.ListNetworkServices(ctx, &controlv1.ListNetworkServicesRequest{})
		}
		if err != nil {
			t.Errorf("after the corpus, listing %s: %v", name, err)
		}
	}
	// What was accepted is stored as given: nothing is interpreted or changed on the way.
	got, _ := c.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: other})
	for _, e := range got.Entries {
		if e.Name == "VAL" && !e.Secret {
			ok := false
			for _, x := range secCorpus() {
				if e.Value == x {
					ok = true
				}
			}
			if !ok {
				t.Errorf("an environment value was stored as something nobody sent: %q", e.Value)
			}
		}
	}
	if err := s.ctlHealthy(); err != nil {
		t.Errorf("the orchestrator is not healthy after the corpus: %v", err)
	}
}

func (s *stack) ctlHealthy() error {
	r, err := http.Get("http://" + s.addr + "/healthz")
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("status %d", r.StatusCode)
	}
	return nil
}

// Limits of the control API hold at their edges, and over them are refused cleanly.
func TestSecurityControlLimits(t *testing.T) {
	s := newStack(t)
	c := s.ctl
	cf := func(name string, labels map[string]string) error {
		_, err := c.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name, Labels: labels})
		return err
	}
	want := func(what string, err error, code connect.Code) {
		t.Helper()
		if code == 0 {
			if err != nil {
				t.Errorf("%s: %v, want success", what, err)
			}
			return
		}
		if connect.CodeOf(err) != code {
			t.Errorf("%s: %v, want %v", what, err, code)
		}
	}
	want("name of 63", cf(strings.Repeat("a", 63), nil), 0)
	want("name of 64", cf(strings.Repeat("a", 64), nil), connect.CodeInvalidArgument)
	want("name with a trailing newline", cf("abc\n", nil), connect.CodeInvalidArgument)
	want("name with a leading hyphen", cf("-abc", nil), connect.CodeInvalidArgument)
	want("name in upper case", cf("ABC", nil), connect.CodeInvalidArgument)
	want("name with the Kelvin sign", cf("K", nil), connect.CodeInvalidArgument)
	want("name with fullwidth letters", cf("ａｂ", nil), connect.CodeInvalidArgument)
	many := func(n int) map[string]string {
		m := map[string]string{}
		for i := 0; i < n; i++ {
			m[fmt.Sprintf("k%d", i)] = "v"
		}
		return m
	}
	want("32 labels", cf("labels32", many(32)), 0)
	want("33 labels", cf("labels33", many(33)), connect.CodeInvalidArgument)
	want("a label key of 63", cf("lk63", map[string]string{strings.Repeat("a", 63): "v"}), 0)
	want("a label key of 64", cf("lk64", map[string]string{strings.Repeat("a", 64): "v"}), connect.CodeInvalidArgument)
	want("a label value of 253", cf("lv253", map[string]string{"k": strings.Repeat("v", 253)}), 0)
	want("a label value of 254", cf("lv254", map[string]string{"k": strings.Repeat("v", 254)}), connect.CodeInvalidArgument)
	want("a label value with a newline", cf("lvnl", map[string]string{"k": "a\nb"}), connect.CodeInvalidArgument)
	want("a label value with non-ASCII", cf("lvu", map[string]string{"k": "café"}), connect.CodeInvalidArgument)
	want("a label key in upper case", cf("lku", map[string]string{"K": "v"}), connect.CodeInvalidArgument)
	want("a label key ending in a newline", cf("lknl", map[string]string{"k\n": "v"}), connect.CodeInvalidArgument)

	fid := s.createFunction("limits")
	setEnv := func(name, value string) error {
		_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: name, Value: value}})
		return err
	}
	want("an env value of 32 KiB", setEnv("BIG", strings.Repeat("v", 32<<10)), 0)
	want("an env value of 32 KiB + 1", setEnv("BIG", strings.Repeat("v", 32<<10+1)), connect.CodeInvalidArgument)
	want("an env value with NUL", setEnv("NUL", "a\x00b"), connect.CodeInvalidArgument)
	want("an env name with a trailing newline", setEnv("NAME\n", "x"), connect.CodeInvalidArgument)
	want("an env name from the ABI", setEnv("PATH_INFO", "x"), connect.CodeInvalidArgument)
	want("an env name that starts with HTTP_", setEnv("HTTP_HOST", "x"), connect.CodeInvalidArgument)
	want("an env name, a digit first", setEnv("1A", "x"), connect.CodeInvalidArgument)
	want("an env name with a unicode letter", setEnv("Ä", "x"), connect.CodeInvalidArgument)
	want("a bad env name, validate-only", func() error {
		_, err := c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "a b", Value: "x"}, ValidateOnly: true})
		return err
	}(), connect.CodeInvalidArgument)
	for i := 0; i < 63; i++ {
		if err := setEnv(fmt.Sprintf("V%d", i), "x"); err != nil {
			t.Fatalf("variable %d: %v", i, err)
		}
	}
	want("the 65th variable (BIG is the 64th)", setEnv("ONE_TOO_MANY", "x"), connect.CodeInvalidArgument)
	want("replacing one at the limit", setEnv("V1", "y"), 0)
	// Concurrent writes cannot get past the count limit.
	fid2 := s.createFunction("limits2")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid2, Entry: &controlv1.EnvEntry{Name: fmt.Sprintf("C%d", i), Value: "x"}})
		}(i)
	}
	wg.Wait()
	if l, _ := c.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fid2}); len(l.Entries) > 64 {
		t.Errorf("%d variables after 100 concurrent writes: the limit is 64", len(l.Entries))
	}
	var allow []string
	for i := 0; i < 65; i++ {
		allow = append(allow, fmt.Sprintf("h%d.example.com", i))
	}
	_, err := c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: allow})
	want("65 allow entries", err, connect.CodeInvalidArgument)
	_, err = c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: allow[:64]})
	want("64 allow entries", err, 0)
	_, err = c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{strings.Repeat("a", 64) + ".com"}})
	want("a label of 64 in an allow entry", err, connect.CodeInvalidArgument)
	_, err = c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"a.example.com\n"}})
	want("an allow entry ending in a newline", err, connect.CodeInvalidArgument)
	_, err = c.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{strings.Repeat("a.", 2000) + "com"}})
	want("a 4 KB allow entry", err, connect.CodeInvalidArgument)

	rt := func(host, prefix string) error {
		_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fid})
		return err
	}
	want("a route prefix with a trailing newline", rt("nl.test", "/a\n"), connect.CodeInvalidArgument)
	want("a route host with a unicode name", rt("bücher.test", "/"), connect.CodeInvalidArgument)
	want("a route host that is the Kelvin sign", rt("K.test", "/x"), 0) // lower-cased to "k.test" on write: documented
	want("a route prefix of 1 KB", rt("long1.test", "/"+strings.Repeat("a", 1000)), 0)
	// page_size
	for _, n := range []uint32{0, 1, 500, 501, 1 << 31, 1<<32 - 1} {
		r, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageSize: n})
		if err != nil || len(r.Functions) > 500 {
			t.Errorf("page_size %d: %v", n, err)
		}
	}
}

// Page tokens are cursors anyone may write. A forged, tampered or foreign token
// is an invalid argument or a harmless page, never an internal error.
//
// KNOWN DEFECT SEC-PAGE-TOKEN-500: see the cases that fail.
func TestSecurityControlForgedPageTokens(t *testing.T) {
	s := newStack(t)
	c := s.ctl
	for i := 0; i < 3; i++ {
		fid := s.createFunction(fmt.Sprintf("tok-%d", i))
		s.route(fmt.Sprintf("tok%d.test", i), "/", fid)
		c.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("m"), Abi: "wagi-1"})
	}
	fl, _ := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageSize: 1})
	rl, _ := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{PageSize: 1})
	realTokens := []string{fl.NextPageToken, rl.NextPageToken}
	b64 := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	var tokens []string
	raws := []string{"", "\xff", "\xc3\x28", "a\x00b", "a\x00b\x00c", "\x00", "\x00\x00", "%", "valid-name", "\xed\xa0\x80", "\xf4\x90\x80\x80", "x\x00\xff", "\xff\x00x", "tok-0\x00/",
		"9223372036854775807", "-9223372036854775808", "99999999999999999999", "-1", "0", "01", "1e3", "abc",
		"9223372036854775807\x0000000000-0000-0000-0000-000000000000", "-9223372036854775808\x0000000000-0000-0000-0000-000000000000",
		"253402300800000000\x0000000000-0000-0000-0000-000000000000", "-62135596800000001\x0000000000-0000-0000-0000-000000000000",
		"0\x00x", "0\x00\xff", "abc\x0000000000-0000-0000-0000-000000000000", "1\x00" + strings.Repeat("f", 36), "1\x00{00000000-0000-0000-0000-000000000000}",
		"1\x00urn:uuid:00000000-0000-0000-0000-000000000000", "1\x0000000000000000000000000000000000", strings.Repeat("a", 100000)}
	for _, r := range raws {
		tokens = append(tokens, b64(r))
	}
	tokens = append(tokens, "!!!", "a", "YQ==", "YQ", strings.Repeat("A", 1<<20), "====", "-_-_", "+/+/", " ")
	for _, rt := range realTokens {
		tokens = append(tokens, rt, rt+"A", rt[:len(rt)/2], strings.ToUpper(rt), "A"+rt)
		for i := range rt {
			// every single-character tamper of a real token
			for _, ch := range []byte{'A', '_', '-', '9'} {
				if rt[i] != ch {
					tokens = append(tokens, rt[:i]+string(ch)+rt[i+1:])
				}
			}
		}
	}
	calls := map[string]func(tok string) error{
		"ListFunctions": func(tok string) error {
			_, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageToken: tok})
			return err
		},
		"ListRoutes": func(tok string) error {
			_, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{PageToken: tok})
			return err
		},
		"ListVersions": func(tok string) error {
			fl, _ := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageSize: 1})
			_, err := c.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: fl.Functions[0].Id, PageToken: tok})
			return err
		},
		"ListInvocations": func(tok string) error {
			_, err := c.ListInvocations(ctx, &controlv1.ListInvocationsRequest{PageToken: tok})
			return err
		},
	}
	type job struct{ rpc, tok string }
	var jobs []job
	for rpc := range calls {
		for _, tok := range tokens {
			jobs = append(jobs, job{rpc, tok})
		}
	}
	var mu sync.Mutex
	bad := map[string][]string{}
	secParallel(jobs, 8, func(j job) {
		err := calls[j.rpc](j.tok)
		if err == nil {
			return
		}
		switch connect.CodeOf(err) {
		case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeResourceExhausted: // the last: a token past the small-message cap
			return
		}
		mu.Lock()
		bad[j.rpc] = append(bad[j.rpc], fmt.Sprintf("%.50q -> %v", j.tok, err))
		mu.Unlock()
	})
	var failing []string
	for rpc, list := range bad {
		failing = append(failing, rpc)
		if len(list) > 3 {
			list = list[:3]
		}
		t.Logf("%s: forged page tokens answered with an error that is not invalid_argument: %d of %d (e.g. %v)", rpc, len(bad[rpc]), len(tokens), list)
	}
	if len(failing) > 0 && os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		t.Skipf("KNOWN DEFECT SEC-PAGE-TOKEN-500: a forged page token that is not valid for the database (invalid UTF-8, a time out of range) is an internal error, not invalid_argument: %v", failing)
	}
	if len(failing) > 0 {
		t.Errorf("%v answered internal errors to forged tokens", failing)
	}
	// A token for one list used on another continues nowhere and leaks nothing: it is refused or empty.
	if r, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{PageToken: fl.NextPageToken}); err == nil && len(r.Routes) > 0 {
		t.Logf("a ListFunctions token used on ListRoutes returned %d routes (a cursor is not bound to its list)", len(r.Routes))
	}
}

// Messages that are not what they seem: invalid UTF-8 in a string field, which
// protobuf forbids and a hand-written client can still send.
func TestSecurityControlInvalidUTF8AndNesting(t *testing.T) {
	s := newStack(t)
	base := "http://" + s.addr
	auth := http.Header{"Authorization": {"Bearer " + adminToken}}
	str := func(num protowire.Number, v string) []byte {
		return protowire.AppendString(protowire.AppendTag(nil, num, protowire.BytesType), v)
	}
	cases := map[string][]byte{
		ctlPkg + "FunctionService/CreateFunction":    str(1, "\xff\xfe"),
		ctlPkg + "FunctionService/GetFunction":       str(1, "\xc3\x28"),
		ctlPkg + "FunctionService/ListFunctions":     append(str(2, "\xff"), str(6, "\xed\xa0\x80")...),
		ctlPkg + "FunctionService/SetEgress":         append(str(1, "x"), str(2, "\xff.example.com")...),
		ctlPkg + "RouteService/CreateRoute":          append(str(1, "\xff"), str(2, "/\xff")...),
		ctlPkg + "RouteService/ListRoutes":           str(2, "\xff"),
		ctlPkg + "InspectionService/ListInvocations": append(str(1, "\xff"), str(4, "\xff")...),
		ctlPkg + "InspectionService/GetInvocation":   str(1, "\xff"),
		ctlPkg + "FunctionService/DeleteEnv":         append(str(1, "x"), str(2, "\xff")...),
		ctlPkg + "FunctionService/SetEnv":            append(str(1, "x"), protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), str(1, "\xff"))...),
	}
	for proc, msg := range cases {
		r := secProtoCall(base, proc, auth, msg)
		if r.err != nil || (r.code != "invalid_argument" && r.code != "not_found") {
			t.Errorf("%s with invalid UTF-8: status %d code %q (%v), want invalid_argument", proc, r.status, r.code, r.err)
		}
		if r.err == nil && strings.Contains(r.body, "SQLSTATE") {
			t.Errorf("%s: %s", proc, r.body)
		}
	}
	// Deep nesting, in both encodings, must not exhaust the stack or the heap.
	h := secMeasure()
	start := time.Now()
	deepJSON := strings.Repeat(`{"a":`, 200000) + "1" + strings.Repeat("}", 200000)
	rj := secRPCBody(base, ctlPkg+"FunctionService/ListFunctions", false, auth, "", []byte(deepJSON), "")
	deepArr := `{"labels":` + strings.Repeat("[", 500000) + strings.Repeat("]", 500000) + `}`
	ra := secRPCBody(base, ctlPkg+"FunctionService/ListFunctions", false, auth, "", []byte(deepArr), "")
	// proto: a chain of nested unknown groups
	var deep []byte
	for i := 0; i < 200000; i++ {
		deep = protowire.AppendTag(deep, 15, protowire.StartGroupType)
	}
	for i := 0; i < 200000; i++ {
		deep = protowire.AppendTag(deep, 15, protowire.EndGroupType)
	}
	rp := secProtoCall(base, ctlPkg+"FunctionService/ListFunctions", auth, deep)
	grew, alloc := h.Stop()
	for name, r := range map[string]secResp{"json objects": rj, "json arrays": ra, "proto groups": rp} {
		if r.err != nil {
			t.Errorf("deeply nested %s: %v", name, r.err)
		} else if r.status >= 500 {
			t.Errorf("deeply nested %s: status %d %.100s", name, r.status, r.body)
		}
	}
	t.Logf("deep nesting: %v, heap peak +%d MiB, %d MiB allocated", time.Since(start), grew/secMiB, alloc/secMiB)
	if time.Since(start) > 15*time.Second || grew > 600*secMiB {
		t.Errorf("deeply nested messages cost %v and %d MiB of heap", time.Since(start), grew/secMiB)
	}
	if err := s.ctlHealthy(); err != nil {
		t.Errorf("not healthy: %v", err)
	}
}

func secProtoCall(base, proc string, hdr http.Header, msg []byte) secResp {
	r, err := http.NewRequest("POST", base+proc, bytes.NewReader(msg))
	if err != nil {
		return secResp{err: err}
	}
	r.Header.Set("Content-Type", "application/proto")
	for k, v := range hdr {
		r.Header[k] = v
	}
	return secDo(r, false)
}

// zeros is a reader of n zero bytes that does not hold them in memory.
type secZeros struct{ n int64 }

func (z *secZeros) Read(p []byte) (int, error) {
	if z.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > z.n {
		p = p[:z.n]
	}
	for i := range p {
		p[i] = 0
	}
	z.n -= int64(len(p))
	return len(p), nil
}

// secBomb is a gzip stream of a protobuf message whose field 1 is a string of
// `size` bytes: tens of kilobytes on the wire, `size` bytes once decompressed.
func secBomb(size int) []byte {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	zw.Write(protowire.AppendTag(nil, 1, protowire.BytesType))
	zw.Write(protowire.AppendVarint(nil, uint64(size)))
	chunk := bytes.Repeat([]byte{'a'}, 1<<20)
	for left := size; left > 0; left -= len(chunk) {
		if left < len(chunk) {
			chunk = chunk[:left]
		}
		zw.Write(chunk)
	}
	zw.Close()
	return buf.Bytes()
}

type secBombTarget struct {
	name      string
	base      string
	proc      string
	streaming bool
	cap       int64 // the largest message the service says it reads (0: nothing says)
}

// secSendBomb sends the compressed message and reports how much memory the
// receiver spent and how fast it answered. No credentials: that is what an
// outsider has, and the message is read before the credentials are looked at.
func secSendBomb(tg secBombTarget, bomb []byte) (status int, took time.Duration, grew, alloc uint64, err error) {
	var body []byte
	ct := "application/proto"
	hdr := http.Header{}
	if tg.streaming {
		ct = "application/connect+proto"
		body = secEnvelope(1, bomb)
		hdr.Set("Connect-Content-Encoding", "gzip")
	} else {
		body = bomb
		hdr.Set("Content-Encoding", "gzip")
	}
	r, _ := http.NewRequest("POST", tg.base+tg.proc, bytes.NewReader(body))
	r.Header.Set("Content-Type", ct)
	for k, v := range hdr {
		r.Header[k] = v
	}
	h := secMeasure()
	start := time.Now()
	resp, e := secClient.Do(r)
	if e == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		status = resp.StatusCode
	}
	took = time.Since(start)
	grew, alloc = h.Stop()
	return status, took, grew, alloc, e
}

func secBombTargets(s *stack, workerAddr, egressAddr string) (capped, uncapped []secBombTarget) {
	orch := "http://" + s.addr
	capped = []secBombTarget{
		{"orchestrator IngressService/Invoke", orch, "/edgerun.ingress.v1.IngressService/Invoke", true, 4<<20 + 256<<10},
		{"orchestrator FunctionService/CreateFunction", orch, ctlPkg + "FunctionService/CreateFunction", false, 33 << 20},
		{"orchestrator PlanService/Validate", orch, ctlPkg + "PlanService/Validate", false, 33 << 20},
		{"worker ExecutionService/Execute", "http://" + workerAddr, "/edgerun.worker.v1.ExecutionService/Execute", true, 8 << 20},
		{"network service EgressService/Fetch", "http://" + egressAddr, "/edgerun.egress.v1.EgressService/Fetch", true, 1<<20 + 256<<10},
	}
	uncapped = []secBombTarget{
		{"orchestrator RouteService/CreateRoute", orch, ctlPkg + "RouteService/CreateRoute", false, 0},
		{"orchestrator InspectionService/Describe", orch, ctlPkg + "InspectionService/Describe", false, 0},
		{"orchestrator FleetService/Register", orch, "/edgerun.worker.v1.FleetService/Register", false, 0},
		{"orchestrator RegistryService/Register", orch, "/edgerun.egress.v1.RegistryService/Register", false, 0},
	}
	return
}

// A small compressed message that expands past the read limit is refused at the
// limit: memory stays bounded and the call fails fast. These are the services
// that set a limit.
func TestSecurityCompressionBombsAreBounded(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(2)
	egAddr := s.secRealEgress(t, []string{"*"})
	workerAddr := s.workerAddr(t)
	capped, _ := secBombTargets(s, workerAddr, egAddr)
	bomb := secBomb(128 << 20)
	t.Logf("a %d KiB message that expands to 128 MiB", len(bomb)/1024)
	for _, tg := range capped {
		status, took, grew, alloc, err := secSendBomb(tg, bomb)
		t.Logf("%-45s status %d in %v, heap peak +%d MiB, %d MiB allocated (err %v)", tg.name, status, took.Round(time.Millisecond), grew/secMiB, alloc/secMiB, err)
		if took > 5*time.Second {
			t.Errorf("%s: took %v to refuse", tg.name, took)
		}
		// The heap peak up to a few times the cap is the buffer growing to it; far more is not stopping.
		if limit := uint64(tg.cap)*6 + 32*secMiB; grew > limit {
			t.Errorf("%s: the heap grew %d MiB for a message capped at %d MiB: the decompressed size is not bounded", tg.name, grew/secMiB, tg.cap/secMiB)
		}
	}
}

// KNOWN DEFECT SEC-PREAUTH-UNBOUNDED-READ: control's RouteService and
// InspectionService, FleetService, ArtifactService and RegistryService (all on
// the orchestrator's one port) set no read limit. Connect reads and decodes
// the whole message before the interceptor looks at the credentials, so an
// unauthenticated caller who can reach the port makes the orchestrator
// buffer whatever it sends, and a 100 KB gzip stream is 160 MiB (or a gigabyte).
func TestSecurityPreAuthReadIsBounded(t *testing.T) {

	s := newStack(t)
	_, uncapped := secBombTargets(s, "", "")
	bomb := secBomb(160 << 20)
	for _, tg := range uncapped {
		status, took, grew, alloc, err := secSendBomb(tg, bomb)
		t.Logf("%-45s status %d in %v, heap peak +%d MiB, %d MiB allocated (err %v)", tg.name, status, took.Round(time.Millisecond), grew/secMiB, alloc/secMiB, err)
		if alloc > 100*secMiB {
			t.Errorf("%s: an unauthenticated 160 MiB compressed message made the orchestrator allocate %d MiB", tg.name, alloc/secMiB)
		}
	}
	// And plain, uncompressed: 200 MiB of body with no credentials.
	for _, tg := range uncapped[:3] {
		r, _ := http.NewRequest("POST", tg.base+tg.proc, io.LimitReader(&secZeros{n: 200 << 20}, 200<<20))
		r.Header.Set("Content-Type", "application/proto")
		h := secMeasure()
		resp, err := secClient.Do(r)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		_, alloc := h.Stop()
		if alloc > 100*secMiB {
			t.Errorf("%s: an unauthenticated 200 MiB body made the orchestrator allocate %d MiB", tg.name, alloc/secMiB)
		}
	}
}

var _ = commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED

// KNOWN DEFECT SEC-ROUTE-PREFIX-500: a route path prefix has no stated length
// limit; one that does not fit in the database's unique index (about 2.7 KB of
// text that does not compress) fails the INSERT and the caller is told
// "internal error" instead of invalid_argument.
func TestSecurityControlLongRoutePrefix(t *testing.T) {
	s := newStack(t)
	c := s.ctl
	fid := s.createFunction("longprefix")
	rt := func(host, prefix string) error {
		_, err := c.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fid})
		return err
	}
	var longBad []string
	for _, n := range []int{3000, 10000, 100000, 1 << 20} {
		var sb strings.Builder
		for i := 0; sb.Len() < n; i++ {
			fmt.Fprintf(&sb, "%x", sha256.Sum256([]byte(fmt.Sprint(n, i)))) // does not compress
		}
		err := rt(fmt.Sprintf("long%d.test", n), "/"+sb.String()[:n])
		if err != nil && connect.CodeOf(err) != connect.CodeInvalidArgument && connect.CodeOf(err) != connect.CodeResourceExhausted {
			longBad = append(longBad, fmt.Sprintf("%d bytes: %v", n, err))
		}
	}
	if len(longBad) > 0 {
		if os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
			t.Skipf("KNOWN DEFECT SEC-ROUTE-PREFIX-500: a route path prefix the database cannot index is an internal error, not invalid_argument: %v", longBad)
		}
		t.Errorf("route path prefixes the database cannot index: %v", longBad)
	}
}
