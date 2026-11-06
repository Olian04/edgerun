package control_test

import (
	"fmt"
	"testing"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

func lbl(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func TestLabelsAreSetListedFilteredAndReplaced(t *testing.T) {
	r := newRig(t, nil)
	mk := func(name string, l map[string]string) string {
		f, err := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name, Labels: l})
		if err != nil {
			t.Fatal(err)
		}
		return f.Function.Id
	}
	a := mk("a", lbl("owner", "gitops", "env", "prod"))
	mk("b", lbl("owner", "gitops", "env", "dev"))
	mk("c", lbl("owner", "human"))
	mk("d", nil)

	names := func(match map[string]string) (out []string) {
		for f, err := range r.ctl.AllFunctionsMatching(ctx, match) {
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, f.Name)
		}
		return
	}
	for match, want := range map[string]string{
		"":                      "a b c d",
		"owner=gitops":          "a b",
		"owner=gitops,env=prod": "a",
		"env=prod,owner=human":  "",
		"owner=Gitops":          "", // exact
		"env=":                  "",
	} {
		m := map[string]string{}
		if match != "" {
			for _, kv := range splitKV(match) {
				m[kv[0]] = kv[1]
			}
		}
		if got := fmt.Sprint(names(m)); got != "["+want+"]" {
			t.Errorf("match %q: %s, want [%s]", match, got, want)
		}
	}

	// Replaced, not merged; the empty map clears.
	set, err := r.ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: a, Labels: lbl("owner", "someone-else")})
	if err != nil || fmt.Sprint(set.Function.Labels) != "map[owner:someone-else]" {
		t.Fatalf("%v %v", set, err)
	}
	got, _ := r.ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: a})
	if fmt.Sprint(got.Function.Labels) != "map[owner:someone-else]" {
		t.Errorf("labels after SetLabels: %v", got.Function.Labels)
	}
	cleared, _ := r.ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: a})
	if len(cleared.Function.Labels) != 0 {
		t.Errorf("an empty map clears: %v", cleared.Function.Labels)
	}
}

func splitKV(s string) (out [][2]string) {
	for _, p := range splitComma(s) {
		for i := 0; i < len(p); i++ {
			if p[i] == '=' {
				out = append(out, [2]string{p[:i], p[i+1:]})
			}
		}
	}
	return
}

func splitComma(s string) (out []string) {
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return
}

func TestLabelsOnVersionsAndRoutesAndTheirFilters(t *testing.T) {
	r := newRig(t, nil)
	f, _ := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "f"})
	fid := f.Function.Id
	v1, err := r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("one"), Abi: "wagi-1", Labels: lbl("commit", "abc")})
	if err != nil {
		t.Fatal(err)
	}
	r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("two"), Abi: "wagi-1", Labels: lbl("commit", "def")})
	if fmt.Sprint(v1.Version.Labels) != "map[commit:abc]" {
		t.Errorf("version labels: %v", v1.Version.Labels)
	}
	n := 0
	for v, err := range r.ctl.AllVersionsMatching(ctx, fid, lbl("commit", "abc")) {
		if err != nil || v.Id != v1.Version.Id {
			t.Errorf("%v %v", v, err)
		}
		n++
	}
	if n != 1 {
		t.Errorf("%d versions matched", n)
	}

	r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "a.test", PathPrefix: "/", FunctionId: fid, Labels: lbl("owner", "gitops")})
	r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "b.test", PathPrefix: "/", FunctionId: fid})
	var hosts []string
	for rt, err := range r.ctl.AllRoutesMatching(ctx, lbl("owner", "gitops")) {
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, rt.HostPattern)
	}
	if fmt.Sprint(hosts) != "[a.test]" {
		t.Errorf("routes matching: %v", hosts)
	}
}

func TestLabelsAreValidated(t *testing.T) {
	r := newRig(t, nil)
	long := make([]byte, 254)
	for i := range long {
		long[i] = 'x'
	}
	many := map[string]string{}
	for i := 0; i < 33; i++ {
		many[fmt.Sprintf("k%d", i)] = "v"
	}
	for name, l := range map[string]map[string]string{
		"an upper-case key":     lbl("Owner", "x"),
		"an empty key":          lbl("", "x"),
		"a key starting with -": lbl("-a", "x"),
		"a key ending with /":   lbl("a/", "x"),
		"a key with a space":    lbl("a b", "x"),
		"a 64-character key":    lbl(string(make([]byte, 0))+fmt.Sprintf("%064d", 0), "x"),
		"a value over 253":      lbl("a", string(long)),
		"a newline in a value":  lbl("a", "x\ny"),
		"a non-ASCII value":     lbl("a", "café"),
		"more than 32 labels":   many,
	} {
		_, err := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "x" + fmt.Sprint(len(name)), Labels: l})
		if code(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v, want invalid_argument", name, err)
		}
	}
	// What is allowed: an empty value, a prefixed key, exactly 32 labels, 253 characters.
	ok := map[string]string{"managed-by.example.com/tool": "", "a": string(long[:253])}
	for i := 0; i < 30; i++ {
		ok[fmt.Sprintf("k%d", i)] = "v"
	}
	if _, err := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "ok", Labels: ok}); err != nil {
		t.Errorf("valid labels: %v", err)
	}
	// A filter is not a label set: it is not validated, it just matches nothing.
	if rs, err := r.ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{MatchLabels: lbl("NOT A KEY", "x")}); err != nil || len(rs.Functions) != 0 {
		t.Errorf("%v %v", rs, err)
	}
}

func TestEnvRevisionsLetAWriteOnlySecretBeCompared(t *testing.T) {
	r := newRig(t, nil)
	mk := func(name string) string {
		f, _ := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name})
		return f.Function.Id
	}
	f1, f2 := mk("one"), mk("two")
	set := func(fid, name, value string, secret bool) *controlv1.EnvEntry {
		res, err := r.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}})
		if err != nil {
			t.Fatal(err)
		}
		return res.Entry
	}
	listed := func(fid, name string) *controlv1.EnvEntry {
		res, err := r.ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fid})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range res.Entries {
			if e.Name == name {
				return e
			}
		}
		t.Fatalf("%s not listed", name)
		return nil
	}

	s1 := set(f1, "API_KEY", "hunter2", true)
	if s1.Value != "" || !s1.Secret || len(s1.Revision) == 0 {
		t.Fatalf("a secret comes back without its value, with a revision: %+v", s1)
	}
	if got := listed(f1, "API_KEY"); got.Revision != s1.Revision || got.Value != "" {
		t.Errorf("the revision a write returned is the one a read shows: %q vs %q", s1.Revision, got.Revision)
	}
	if again := set(f1, "API_KEY", "hunter2", true); again.Revision != s1.Revision {
		t.Error("setting the value that is already there changes nothing, revision included")
	}
	for what, other := range map[string]*controlv1.EnvEntry{
		"another value":                    set(f1, "API_KEY", "hunter3", true),
		"the same value, not secret":       set(f1, "API_KEY", "hunter2", false),
		"the same value, another function": set(f2, "API_KEY", "hunter2", true),
		"the same value, another name":     set(f1, "OTHER_KEY", "hunter2", true),
	} {
		if other.Revision == s1.Revision {
			t.Errorf("%s must have another revision", what)
		}
	}
	// A plain entry has one too, and shows its value.
	p := set(f1, "PLAIN", "visible", false)
	if p.Value != "visible" || p.Revision == "" || listed(f1, "PLAIN").Revision != p.Revision {
		t.Errorf("%+v", p)
	}
	// The revision is never the value, and it is the orchestrator's to compute: a client's own entry cannot set it.
	res, err := r.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: f2, Entry: &controlv1.EnvEntry{Name: "X", Value: "v", Revision: "forged"}})
	if err != nil || res.Entry.Revision == "forged" {
		t.Errorf("revision is output only: %+v %v", res, err)
	}
}

// Validate-only: every check, nothing kept.
func TestValidateOnlyChecksEverythingAndChangesNothing(t *testing.T) {
	r := newRig(t, nil)
	f, _ := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "real", Labels: lbl("a", "b")})
	fid := f.Function.Id
	v, _ := r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("m1"), Abi: "wagi-1"})
	other, _ := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "other"})
	ov, _ := r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: other.Function.Id, Module: []byte("m2"), Abi: "wagi-1"})
	rt, _ := r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "h.test", PathPrefix: "/", FunctionId: fid})
	r.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "E", Value: "1"}})

	type op struct {
		name string
		call func(validateOnly bool) (any, error)
	}
	ops := []op{
		{"create function", func(vo bool) (any, error) {
			return r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "fresh", Labels: lbl("x", "y"), ValidateOnly: vo})
		}},
		{"create a function that exists", func(vo bool) (any, error) {
			return r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "real", ValidateOnly: vo})
		}},
		{"create a function with a bad name", func(vo bool) (any, error) {
			return r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "Bad Name", ValidateOnly: vo})
		}},
		{"create a function with bad labels", func(vo bool) (any, error) {
			return r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "lab", Labels: lbl("BAD", "x"), ValidateOnly: vo})
		}},
		{"create a version", func(vo bool) (any, error) {
			return r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("m3"), Abi: "wagi-1", Activate: true, ValidateOnly: vo})
		}},
		{"create a version of nothing", func(vo bool) (any, error) {
			return r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: "00000000-0000-0000-0000-000000000000", Module: []byte("m3"), Abi: "wagi-1", ValidateOnly: vo})
		}},
		{"create a version with a bad abi", func(vo bool) (any, error) {
			return r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("m3"), Abi: "NOPE", ValidateOnly: vo})
		}},
		{"create a version over the size limit", func(vo bool) (any, error) {
			return r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: make([]byte, 2<<10), Abi: "wagi-1", ValidateOnly: vo})
		}},
		{"activate", func(vo bool) (any, error) {
			return r.ctl.ActivateVersion(ctx, &controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: v.Version.Id, ValidateOnly: vo})
		}},
		{"activate another function's version", func(vo bool) (any, error) {
			return r.ctl.ActivateVersion(ctx, &controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: ov.Version.Id, ValidateOnly: vo})
		}},
		{"set env", func(vo bool) (any, error) {
			return r.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "NEW", Value: "v", Secret: true}, ValidateOnly: vo})
		}},
		{"set a reserved env name", func(vo bool) (any, error) {
			return r.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "PATH_INFO", Value: "v"}, ValidateOnly: vo})
		}},
		{"delete env", func(vo bool) (any, error) {
			return r.ctl.DeleteEnv(ctx, &controlv1.DeleteEnvRequest{FunctionId: fid, Name: "E", ValidateOnly: vo})
		}},
		{"delete env that is not there", func(vo bool) (any, error) {
			return r.ctl.DeleteEnv(ctx, &controlv1.DeleteEnvRequest{FunctionId: fid, Name: "NOPE", ValidateOnly: vo})
		}},
		{"set egress", func(vo bool) (any, error) {
			return r.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"API.example.com"}, ValidateOnly: vo})
		}},
		{"set a bad egress entry", func(vo bool) (any, error) {
			return r.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"a.example.com:99999"}, ValidateOnly: vo})
		}},
		{"set labels", func(vo bool) (any, error) {
			return r.ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: fid, Labels: lbl("n", "m"), ValidateOnly: vo})
		}},
		{"create a route", func(vo bool) (any, error) {
			return r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "new.test", PathPrefix: "/p", FunctionId: fid, ValidateOnly: vo})
		}},
		{"create a route that exists", func(vo bool) (any, error) {
			return r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "h.test", PathPrefix: "/", FunctionId: fid, ValidateOnly: vo})
		}},
		{"create a route to nothing", func(vo bool) (any, error) {
			return r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "z.test", PathPrefix: "/", FunctionId: "00000000-0000-0000-0000-000000000000", ValidateOnly: vo})
		}},
		{"delete a route", func(vo bool) (any, error) {
			return r.ctl.DeleteRoute(ctx, &controlv1.DeleteRouteRequest{RouteId: rt.Route.Id, ValidateOnly: vo})
		}},
		{"delete a route that is not there", func(vo bool) (any, error) {
			return r.ctl.DeleteRoute(ctx, &controlv1.DeleteRouteRequest{RouteId: "00000000-0000-0000-0000-000000000000", ValidateOnly: vo})
		}},
		{"delete a function", func(vo bool) (any, error) {
			return r.ctl.DeleteFunction(ctx, &controlv1.DeleteFunctionRequest{FunctionId: fid, ValidateOnly: vo})
		}},
		{"delete a function that is not there", func(vo bool) (any, error) {
			return r.ctl.DeleteFunction(ctx, &controlv1.DeleteFunctionRequest{FunctionId: "00000000-0000-0000-0000-000000000000", ValidateOnly: vo})
		}},
	}

	snapshot := func() string {
		var s string
		fs, _ := r.ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{})
		for _, f := range fs.Functions {
			s += fmt.Sprintf("F %s %v %v %v|", f.Name, f.ActiveVersionId, f.EgressAllow, f.Labels)
			vs, _ := r.ctl.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: f.Id})
			for _, v := range vs.Versions {
				s += fmt.Sprintf("V %d %s|", v.Number, v.ArtifactDigest)
			}
			es, _ := r.ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: f.Id})
			for _, e := range es.Entries {
				s += fmt.Sprintf("E %s %s %s|", e.Name, e.Value, e.Revision)
			}
		}
		rs, _ := r.ctl.ListRoutes(ctx, &controlv1.ListRoutesRequest{})
		for _, rt := range rs.Routes {
			s += fmt.Sprintf("R %s%s|", rt.HostPattern, rt.PathPrefix)
		}
		return s
	}

	for _, o := range ops {
		t.Run(o.name, func(t *testing.T) {
			before, pokes := snapshot(), r.pokes.Load()
			dry, dryErr := o.call(true)
			if after := snapshot(); after != before {
				t.Fatalf("validate-only changed something:\n before %s\n after  %s", before, after)
			}
			if r.pokes.Load() != pokes {
				t.Error("validate-only must not refresh what the invoke path caches: nothing changed")
			}
			// The real call answers the same error, or succeeds where validation succeeded.
			// (Run last, in a copy of the order that keeps earlier ones from interfering: each
			// real call is undone by the next ones only where they do not touch the same thing,
			// so the real call is only made for the failures, which change nothing either way.)
			if dryErr != nil {
				_, realErr := o.call(false)
				if code(realErr) != code(dryErr) {
					t.Errorf("validate-only answered %v, the call answers %v", dryErr, realErr)
				}
				if realErr != nil && dryErr.Error() != realErr.Error() {
					t.Errorf("not the same error:\n dry  %v\n real %v", dryErr, realErr)
				}
				return
			}
			if dry == nil {
				t.Error("validate-only success returns the response the call would")
			}
		})
	}
}

func TestValidateOnlyCreatesCarryNoIds(t *testing.T) {
	r := newRig(t, nil)
	fn, err := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "dry", ValidateOnly: true})
	if err != nil || fn.Function.Id != "" || fn.Function.Name != "dry" {
		t.Errorf("%+v %v", fn, err)
	}
	if _, err := r.ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: fn.Function.Id}); code(err) != connect.CodeNotFound {
		t.Errorf("nothing was created: %v", err)
	}
	real, _ := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "real"})
	v, err := r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: real.Function.Id, Module: []byte("abc"), Abi: "wagi-1",
		Limits: &commonv1.Limits{MemoryLimitBytes: 1 << 24}, ValidateOnly: true})
	if err != nil || v.Version.Id != "" || v.Version.ArtifactDigest == "" || v.Version.Number != 1 || v.Version.Limits.MemoryLimitBytes != 1<<24 {
		t.Errorf("a dry-run version says what it would be: %+v %v", v, err)
	}
	if vs, _ := r.ctl.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: real.Function.Id}); len(vs.Versions) != 0 {
		t.Error("the version was created")
	}
	// And a real one after it is number 1: the dry run consumed nothing.
	if rv, err := r.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: real.Function.Id, Module: []byte("abc"), Abi: "wagi-1"}); err != nil || rv.Version.Number != 1 {
		t.Errorf("%+v %v", rv, err)
	}
	rt, err := r.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "x.test", PathPrefix: "/", FunctionId: real.Function.Id, ValidateOnly: true})
	if err != nil || rt.Route.Id != "" || rt.Route.HostPattern != "x.test" {
		t.Errorf("%+v %v", rt, err)
	}
}
