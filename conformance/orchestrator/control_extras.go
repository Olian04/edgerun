package orchestratorconf

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// This file is the suite for what control.v1 says about labels, revisions and
// validate-only (docs/protocol.md, "Labels", "Env revisions" and "Validate-only").

func lbls(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func (h *harness) listFunctionsMatching(t testing.TB, match map[string]string) []string {
	t.Helper()
	var names []string
	token := ""
	for {
		ctx, cancel := ctxT(settle)
		r, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageSize: 2, PageToken: token, MatchLabels: match}))
		cancel()
		if err != nil {
			t.Fatalf("ListFunctions: %v", err)
		}
		for _, f := range r.Msg.GetFunctions() {
			names = append(names, f.GetName())
		}
		if token = r.Msg.GetNextPageToken(); token == "" {
			break
		}
	}
	sort.Strings(names)
	return names
}

// controlLabels: metadata a tool sets on what it manages, and finds it by.
func (h *harness) controlLabels(t *testing.T) {
	ctx, cancel := ctxT(60 * 1e9)
	defer cancel()
	with := func(kv ...string) map[string]string {
		m := lbls(kv...)
		m["conformance-run"] = h.run
		return m
	}
	create := func(name string, labels map[string]string) *controlv1.Function {
		t.Helper()
		r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name, Labels: labels}))
		if err != nil {
			t.Fatalf("CreateFunction(%s): %v", name, err)
		}
		return r.Msg.GetFunction()
	}
	a, b, c := h.name("lab-a"), h.name("lab-b"), h.name("lab-c")
	fa := create(a, with("owner", "gitops", "env", "prod"))
	create(b, with("owner", "gitops", "env", "dev"))
	create(c, with("owner", "human"))
	other := h.name("lab-d")
	create(other, nil) // carries none of them

	t.Run("they come back on the resource", func(t *testing.T) {
		g, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fa.GetId()}))
		if err != nil || fmt.Sprint(g.Msg.GetFunction().GetLabels()) != fmt.Sprint(with("owner", "gitops", "env", "prod")) {
			t.Errorf("labels: %v, %v", g.Msg.GetFunction().GetLabels(), err)
		}
	})

	t.Run("a list is filtered by every label named, exactly", func(t *testing.T) {
		for _, tc := range []struct {
			match map[string]string
			want  []string
		}{
			{with("owner", "gitops"), []string{a, b}},
			{with("owner", "gitops", "env", "prod"), []string{a}},
			{with("owner", "human", "env", "prod"), nil},
			{with("owner", "Gitops"), nil}, // values are compared exactly
			{with("env", ""), nil},         // a label with another value does not match an empty one
			{lbls("conformance-run", h.run+"-nope"), nil},
		} {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if got := h.listFunctionsMatching(t, tc.match); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("match %v: %v, want %v", tc.match, got, want)
			}
		}
		// The empty filter matches everything this run made, labelled or not (paged two at a time).
		all := h.listFunctionsMatching(t, nil)
		found := 0
		for _, n := range all {
			if n == a || n == b || n == c || n == other {
				found++
			}
		}
		if found != 4 {
			t.Errorf("an empty filter must list every function: found %d of 4 of this run's", found)
		}
	})

	t.Run("SetLabels replaces them", func(t *testing.T) {
		r, err := h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fa.GetId(), Labels: with("owner", "someone-else")}))
		if err != nil || fmt.Sprint(r.Msg.GetFunction().GetLabels()) != fmt.Sprint(with("owner", "someone-else")) {
			t.Fatalf("%v %v", r.Msg, err)
		}
		if got := h.listFunctionsMatching(t, with("owner", "gitops")); strings.Join(got, ",") != b {
			t.Errorf("after the change, owner=gitops matches %v", got)
		}
		r, err = h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fa.GetId()}))
		if err != nil || len(r.Msg.GetFunction().GetLabels()) != 0 {
			t.Errorf("an empty set clears them: %v %v", r.Msg.GetFunction().GetLabels(), err)
		}
		_, err = h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: "00000000-0000-0000-0000-000000000000"}))
		checkCode(t, err, connect.CodeNotFound)
	})

	t.Run("versions and routes carry them from creation", func(t *testing.T) {
		f := h.createFunction(t, h.name("lab-v"))
		v, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
			FunctionId: f.GetId(), Module: h.module("lab-1"), Abi: h.abi(), Labels: with("commit", "abc")}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
			FunctionId: f.GetId(), Module: h.module("lab-2"), Abi: h.abi(), Labels: with("commit", "def")})); err != nil {
			t.Fatal(err)
		}
		lv, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.GetId(), MatchLabels: with("commit", "abc")}))
		if err != nil || len(lv.Msg.GetVersions()) != 1 || lv.Msg.GetVersions()[0].GetId() != v.Msg.GetVersion().GetId() ||
			lv.Msg.GetVersions()[0].GetLabels()["commit"] != "abc" {
			t.Errorf("versions matching: %v %v", lv.Msg, err)
		}
		host := h.host()
		if _, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: f.GetId(), Labels: with("owner", "gitops")})); err != nil {
			t.Fatal(err)
		}
		lr, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{MatchLabels: with("owner", "gitops")}))
		if err != nil || len(lr.Msg.GetRoutes()) != 1 || lr.Msg.GetRoutes()[0].GetHostPattern() != host || lr.Msg.GetRoutes()[0].GetLabels()["owner"] != "gitops" {
			t.Errorf("routes matching: %v %v", lr.Msg, err)
		}
	})

	t.Run("their form is checked", func(t *testing.T) {
		long := strings.Repeat("x", 254)
		many := map[string]string{}
		for i := 0; i < 33; i++ {
			many[fmt.Sprintf("k%d", i)] = "v"
		}
		bad := map[string]map[string]string{
			"an upper-case key":     lbls("Owner", "x"),
			"an empty key":          lbls("", "x"),
			"a key starting with -": lbls("-a", "x"),
			"a key ending with /":   lbls("a/", "x"),
			"a key with a space":    lbls("a b", "x"),
			"a 64-character key":    lbls(strings.Repeat("k", 64), "x"),
			"a value over 253":      lbls("a", long),
			"a newline in a value":  lbls("a", "x\ny"),
			"a non-ASCII value":     lbls("a", "café"),
			"more than 32 labels":   many,
		}
		for name, l := range bad {
			_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("lab-bad"), Labels: l}))
			checkCode(t, err, connect.CodeInvalidArgument, name)
			f := h.createFunction(t, h.name("lab-bad2"))
			_, err = h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: f.GetId(), Labels: l}))
			checkCode(t, err, connect.CodeInvalidArgument, name+" (SetLabels)")
		}
		edge := map[string]string{"managed-by.example.com/tool": "", "a": strings.Repeat("x", 253), strings.Repeat("k", 63): "v"}
		for i := 0; i < 29; i++ {
			edge[fmt.Sprintf("k%d", i)] = "v"
		}
		if _, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("lab-edge"), Labels: edge})); err != nil {
			t.Errorf("32 labels, a 63-character key, a prefixed key, an empty value and 253 characters are all allowed: %v", err)
		}
	})
}

func checkCode(t *testing.T, err error, want connect.Code, what ...string) {
	t.Helper()
	if err == nil || connect.CodeOf(err) != want {
		t.Errorf("%s: %v, want %v", strings.Join(what, " "), err, want)
	}
}

// controlEnvRevision: a write-only secret can still be compared.
func (h *harness) controlEnvRevision(t *testing.T) {
	ctx, cancel := ctxT(60 * 1e9)
	defer cancel()
	f1, f2 := h.createFunction(t, h.name("rev-a")), h.createFunction(t, h.name("rev-b"))
	set := func(fid, name, value string, secret bool) *controlv1.EnvEntry {
		t.Helper()
		r, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}}))
		if err != nil {
			t.Fatalf("SetEnv(%s): %v", name, err)
		}
		return r.Msg.GetEntry()
	}
	listed := func(fid, name string) *controlv1.EnvEntry {
		t.Helper()
		r, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid}))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range r.Msg.GetEntries() {
			if e.GetName() == name {
				return e
			}
		}
		t.Fatalf("%s is not listed", name)
		return nil
	}

	s := set(f1.GetId(), "API_KEY", "hunter2-"+h.run, true)
	if s.GetValue() != "" || !s.GetSecret() || s.GetRevision() == "" {
		t.Fatalf("a secret comes back with no value and with a revision: %+v", s)
	}
	if strings.Contains(s.GetRevision(), "hunter2") {
		t.Error("a revision must not contain the value")
	}
	if got := listed(f1.GetId(), "API_KEY"); got.GetRevision() != s.GetRevision() || got.GetValue() != "" {
		t.Errorf("the revision a write returns is the one a read shows: %q, %q (value %q)", s.GetRevision(), got.GetRevision(), got.GetValue())
	}
	if again := set(f1.GetId(), "API_KEY", "hunter2-"+h.run, true); again.GetRevision() != s.GetRevision() {
		t.Error("setting the value that is already there changes nothing, its revision included")
	}
	for what, other := range map[string]*controlv1.EnvEntry{
		"another value":                      set(f1.GetId(), "API_KEY", "hunter3-"+h.run, true),
		"the same value, not secret":         set(f1.GetId(), "API_KEY", "hunter2-"+h.run, false),
		"the same value in another function": set(f2.GetId(), "API_KEY", "hunter2-"+h.run, true),
		"the same value under another name":  set(f1.GetId(), "OTHER_KEY", "hunter2-"+h.run, true),
	} {
		if other.GetRevision() == s.GetRevision() {
			t.Errorf("%s must have another revision", what)
		}
	}
	p := set(f1.GetId(), "PLAIN", "visible", false)
	if p.GetValue() != "visible" || p.GetRevision() == "" || listed(f1.GetId(), "PLAIN").GetRevision() != p.GetRevision() {
		t.Errorf("a plain entry has a revision too: %+v", p)
	}
	if r := set(f2.GetId(), "FORGED", "v", false); r.GetRevision() == "" {
		t.Error("no revision")
	}
	forged, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: f2.GetId(), Entry: &controlv1.EnvEntry{Name: "FORGED2", Value: "v", Revision: "forged"}}))
	if err != nil || forged.Msg.GetEntry().GetRevision() == "forged" {
		t.Errorf("the revision is output only: %v %v", forged.Msg, err)
	}
}

// validateOnly: every check, nothing kept.
func (h *harness) validateOnly(t *testing.T) {
	ctx, cancel := ctxT(120 * 1e9)
	defer cancel()
	f := h.createFunction(t, h.name("vo"))
	fid := f.GetId()
	module := h.module("vo")
	abi := h.abi()
	v := h.createVersion(t, fid, abi, module, nil, false)
	other := h.createFunction(t, h.name("vo-other"))
	ov := h.createVersion(t, other.GetId(), abi, h.module("vo-other"), nil, false)
	host := h.host()
	rt := h.createRoute(t, host, "/", fid)
	h.setEnv(t, fid, "E", "1", false)
	missing := "00000000-0000-0000-0000-000000000000"

	// What one function and the routes of this run look like, for before and after.
	snapshot := func() string {
		var sb strings.Builder
		g, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
		if err != nil {
			fmt.Fprintf(&sb, "function: %v|", connect.CodeOf(err))
		} else {
			fmt.Fprintf(&sb, "F %s active=%q egress=%v labels=%v|", g.Msg.GetFunction().GetName(), g.Msg.GetFunction().GetActiveVersionId(), g.Msg.GetFunction().GetEgressAllow(), g.Msg.GetFunction().GetLabels())
		}
		if vs, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fid})); err == nil {
			for _, x := range vs.Msg.GetVersions() {
				fmt.Fprintf(&sb, "V%d %s|", x.GetNumber(), x.GetArtifactDigest())
			}
		}
		if es, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid})); err == nil {
			for _, e := range es.Msg.GetEntries() {
				fmt.Fprintf(&sb, "E %s=%s %s|", e.GetName(), e.GetValue(), e.GetRevision())
			}
		}
		if rs, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageSize: 500})); err == nil {
			var routes []string
			for _, r := range rs.Msg.GetRoutes() {
				if strings.HasSuffix(r.GetHostPattern(), h.run+".conformance.test") {
					routes = append(routes, r.GetHostPattern()+r.GetPathPrefix())
				}
			}
			sort.Strings(routes)
			fmt.Fprintf(&sb, "R %v|", routes)
		}
		if _, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: other.GetId()})); err != nil {
			sb.WriteString("other gone|")
		}
		return sb.String()
	}

	freshName := h.name("vo-fresh")
	type op struct {
		name string
		call func(vo bool) (any, error)
	}
	ops := []op{
		{"create a function", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: freshName, Labels: lbls("a", "b"), ValidateOnly: vo}))
		}},
		{"create a function that exists", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: f.GetName(), ValidateOnly: vo}))
		}},
		{"create a function with a bad name", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "Bad Name", ValidateOnly: vo}))
		}},
		{"create a function with bad labels", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("vo-l"), Labels: lbls("BAD", "x"), ValidateOnly: vo}))
		}},
		{"create a version", func(vo bool) (any, error) {
			return h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-new"), Abi: abi, Activate: true, ValidateOnly: vo}))
		}},
		{"create a version of nothing", func(vo bool) (any, error) {
			return h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: missing, Module: h.module("vo-x"), Abi: abi, ValidateOnly: vo}))
		}},
		{"create a version with a bad abi", func(vo bool) (any, error) {
			return h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-y"), Abi: "NOT AN ABI", ValidateOnly: vo}))
		}},
		{"activate a version", func(vo bool) (any, error) {
			return h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: v.GetId(), ValidateOnly: vo}))
		}},
		{"activate another function's version", func(vo bool) (any, error) {
			return h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: ov.GetId(), ValidateOnly: vo}))
		}},
		{"set an env entry", func(vo bool) (any, error) {
			return h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "NEW", Value: "v", Secret: true}, ValidateOnly: vo}))
		}},
		{"set a reserved env name", func(vo bool) (any, error) {
			return h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "PATH_INFO", Value: "v"}, ValidateOnly: vo}))
		}},
		{"delete an env entry", func(vo bool) (any, error) {
			return h.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fid, Name: "E", ValidateOnly: vo}))
		}},
		{"delete an env entry that is not there", func(vo bool) (any, error) {
			return h.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fid, Name: "NOPE", ValidateOnly: vo}))
		}},
		{"set egress", func(vo bool) (any, error) {
			return h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"API.example.com"}, ValidateOnly: vo}))
		}},
		{"set a bad egress entry", func(vo bool) (any, error) {
			return h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"a.example.com:99999"}, ValidateOnly: vo}))
		}},
		{"set labels", func(vo bool) (any, error) {
			return h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fid, Labels: lbls("n", "m"), ValidateOnly: vo}))
		}},
		{"set bad labels", func(vo bool) (any, error) {
			return h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fid, Labels: lbls("N", "m"), ValidateOnly: vo}))
		}},
		{"create a route", func(vo bool) (any, error) {
			return h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/p", FunctionId: fid, ValidateOnly: vo}))
		}},
		{"create a route that exists", func(vo bool) (any, error) {
			return h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: fid, ValidateOnly: vo}))
		}},
		{"create a route to nothing", func(vo bool) (any, error) {
			return h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/", FunctionId: missing, ValidateOnly: vo}))
		}},
		{"delete a route", func(vo bool) (any, error) {
			return h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: rt.GetId(), ValidateOnly: vo}))
		}},
		{"delete a route that is not there", func(vo bool) (any, error) {
			return h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: missing, ValidateOnly: vo}))
		}},
		{"delete a function", func(vo bool) (any, error) {
			return h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: other.GetId(), ValidateOnly: vo}))
		}},
		{"delete a function that is not there", func(vo bool) (any, error) {
			return h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: missing, ValidateOnly: vo}))
		}},
	}
	for _, o := range ops {
		t.Run(o.name, func(t *testing.T) {
			before := snapshot()
			_, dryErr := o.call(true)
			if after := snapshot(); after != before {
				t.Fatalf("validate-only changed something:\n before %s\n after  %s", before, after)
			}
			if dryErr != nil {
				// The error is the one the call itself gives (the real call fails too, so it changes nothing).
				_, realErr := o.call(false)
				if connect.CodeOf(realErr) != connect.CodeOf(dryErr) {
					t.Errorf("validate-only answered %v, the call answers %v", dryErr, realErr)
				}
			}
		})
	}

	t.Run("a validate-only create names no id, and reserves nothing", func(t *testing.T) {
		name := h.name("vo-twice")
		r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name, ValidateOnly: true}))
		if err != nil || r.Msg.GetFunction().GetId() != "" || r.Msg.GetFunction().GetName() != name {
			t.Errorf("%v %v", r.Msg, err)
		}
		if _, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name})); err != nil {
			t.Errorf("the real create after a validate-only one: %v", err)
		}
		rv, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-ver"), Abi: abi, ValidateOnly: true}))
		if err != nil || rv.Msg.GetVersion().GetId() != "" || rv.Msg.GetVersion().GetArtifactDigest() == "" {
			t.Errorf("a validate-only version says what it would be, with no id: %v %v", rv.Msg, err)
		}
		real, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-ver2"), Abi: abi}))
		if err != nil || real.Msg.GetVersion().GetNumber() != 2 {
			t.Errorf("the number a dry run would have taken is not used up: %v %v", real.Msg.GetVersion(), err)
		}
	})
}
