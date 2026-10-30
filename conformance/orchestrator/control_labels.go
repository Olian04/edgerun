package orchestratorconf

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// This file is the suite for editing labels in place (PatchLabels,
// SetRouteLabels, PatchRouteLabels) and for the filters of the list calls
// (docs/protocol.md, "Labels" and "Control semantics").

// controlLabelEditing: a tool changes its own labels without disturbing another's.
func (h *harness) controlLabelEditing(t *testing.T) {
	ctx, cancel := ctxT(120 * 1e9)
	defer cancel()
	missing := "00000000-0000-0000-0000-000000000000"

	t.Run("PatchLabels changes some and leaves the rest", func(t *testing.T) {
		f, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("patch"), Labels: lbls("a", "1", "b", "2", "c", "3")}))
		if err != nil {
			t.Fatal(err)
		}
		fid := f.Msg.GetFunction().GetId()
		patch := func(set map[string]string, remove ...string) (*controlv1.Function, error) {
			r, err := h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: fid, Set: set, Remove: remove}))
			if err != nil {
				return nil, err
			}
			return r.Msg.GetFunction(), nil
		}
		got, err := patch(lbls("b", "20", "d", "4"), "c")
		if err != nil || fmt.Sprint(got.GetLabels()) != fmt.Sprint(lbls("a", "1", "b", "20", "d", "4")) {
			t.Fatalf("set b and d, remove c: %v %v", got.GetLabels(), err)
		}
		g, _ := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
		if fmt.Sprint(g.Msg.GetFunction().GetLabels()) != fmt.Sprint(got.GetLabels()) {
			t.Errorf("the patch is stored: %v", g.Msg.GetFunction().GetLabels())
		}
		if got, err := patch(nil, "never-there"); err != nil || len(got.GetLabels()) != 3 {
			t.Errorf("removing a key that is not there is not an error and changes nothing: %v %v", got.GetLabels(), err)
		}
		if got, err := patch(nil); err != nil || len(got.GetLabels()) != 3 {
			t.Errorf("an empty patch changes nothing: %v %v", got.GetLabels(), err)
		}
		if got, err := patch(nil, "a", "b", "d"); err != nil || len(got.GetLabels()) != 0 {
			t.Errorf("removing every key clears them: %v %v", got.GetLabels(), err)
		}

		// Refused, and nothing changed.
		patch(lbls("keep", "me"))
		_, err = patch(lbls("x", "1"), "x")
		checkCode(t, err, connect.CodeInvalidArgument, "a key both set and removed")
		_, err = patch(lbls("BAD", "1"))
		checkCode(t, err, connect.CodeInvalidArgument, "a bad key to set")
		_, err = patch(nil, "BAD")
		checkCode(t, err, connect.CodeInvalidArgument, "a bad key to remove")
		_, err = patch(lbls("k", strings.Repeat("x", 254)))
		checkCode(t, err, connect.CodeInvalidArgument, "a bad value")
		many := map[string]string{}
		for i := 0; i < 32; i++ {
			many[fmt.Sprintf("k%d", i)] = "v"
		}
		_, err = patch(many) // with "keep" that makes 33
		checkCode(t, err, connect.CodeInvalidArgument, "a result over 32 labels")
		g, _ = h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
		if fmt.Sprint(g.Msg.GetFunction().GetLabels()) != fmt.Sprint(lbls("keep", "me")) {
			t.Errorf("a refused patch changed something: %v", g.Msg.GetFunction().GetLabels())
		}
		_, err = h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: missing, Set: lbls("a", "b")}))
		checkCode(t, err, connect.CodeNotFound, "a function that does not exist")
		_, err = h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: "not-an-id"}))
		checkCode(t, err, connect.CodeNotFound, "a malformed id")
	})

	t.Run("patches by different tools do not lose each other's", func(t *testing.T) {
		f := h.createFunction(t, h.name("patch-race"))
		const writers = 16
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: f.GetId(), Set: lbls(fmt.Sprintf("tool-%d", i), "x")}))
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		g, _ := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: f.GetId()}))
		if n := len(g.Msg.GetFunction().GetLabels()); n != writers {
			t.Errorf("%d of %d concurrent patches survived: %v", n, writers, g.Msg.GetFunction().GetLabels())
		}
	})

	t.Run("a route can be relabelled and nothing else about it changes", func(t *testing.T) {
		f := h.createFunction(t, h.name("rl"))
		host := h.host()
		r, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/api", FunctionId: f.GetId(), Labels: lbls("a", "1", "b", "2")}))
		if err != nil {
			t.Fatal(err)
		}
		rid := r.Msg.GetRoute().GetId()
		same := func(got *controlv1.Route, what string) {
			t.Helper()
			o := r.Msg.GetRoute()
			if got.GetId() != rid || got.GetHostPattern() != o.GetHostPattern() || got.GetPathPrefix() != o.GetPathPrefix() ||
				got.GetFunctionId() != o.GetFunctionId() || !got.GetCreateTime().AsTime().Equal(o.GetCreateTime().AsTime()) {
				t.Errorf("%s changed more than the labels: %v, was %v", what, got, o)
			}
		}
		s, err := h.rt.SetRouteLabels(ctx, connect.NewRequest(&controlv1.SetRouteLabelsRequest{RouteId: rid, Labels: lbls("owner", "gitops")}))
		if err != nil || fmt.Sprint(s.Msg.GetRoute().GetLabels()) != fmt.Sprint(lbls("owner", "gitops")) {
			t.Fatalf("SetRouteLabels replaces: %v %v", s.Msg.GetRoute().GetLabels(), err)
		}
		same(s.Msg.GetRoute(), "SetRouteLabels")
		p, err := h.rt.PatchRouteLabels(ctx, connect.NewRequest(&controlv1.PatchRouteLabelsRequest{RouteId: rid, Set: lbls("env", "prod"), Remove: []string{"owner"}}))
		if err != nil || fmt.Sprint(p.Msg.GetRoute().GetLabels()) != fmt.Sprint(lbls("env", "prod")) {
			t.Fatalf("PatchRouteLabels: %v %v", p.Msg.GetRoute().GetLabels(), err)
		}
		same(p.Msg.GetRoute(), "PatchRouteLabels")
		lr, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{MatchLabels: lbls("env", "prod"), FunctionId: f.GetId()}))
		if err != nil || len(lr.Msg.GetRoutes()) != 1 || lr.Msg.GetRoutes()[0].GetId() != rid {
			t.Errorf("a list finds it by its new labels: %v %v", lr.Msg, err)
		}
		c, err := h.rt.SetRouteLabels(ctx, connect.NewRequest(&controlv1.SetRouteLabelsRequest{RouteId: rid}))
		if err != nil || len(c.Msg.GetRoute().GetLabels()) != 0 {
			t.Errorf("an empty set clears them: %v %v", c.Msg.GetRoute().GetLabels(), err)
		}
		_, err = h.rt.SetRouteLabels(ctx, connect.NewRequest(&controlv1.SetRouteLabelsRequest{RouteId: rid, Labels: lbls("BAD", "x")}))
		checkCode(t, err, connect.CodeInvalidArgument, "SetRouteLabels with a bad key")
		_, err = h.rt.PatchRouteLabels(ctx, connect.NewRequest(&controlv1.PatchRouteLabelsRequest{RouteId: rid, Set: lbls("x", "1"), Remove: []string{"x"}}))
		checkCode(t, err, connect.CodeInvalidArgument, "PatchRouteLabels setting and removing one key")
		_, err = h.rt.SetRouteLabels(ctx, connect.NewRequest(&controlv1.SetRouteLabelsRequest{RouteId: missing}))
		checkCode(t, err, connect.CodeNotFound, "SetRouteLabels of nothing")
		_, err = h.rt.PatchRouteLabels(ctx, connect.NewRequest(&controlv1.PatchRouteLabelsRequest{RouteId: "nope"}))
		checkCode(t, err, connect.CodeNotFound, "PatchRouteLabels of a malformed id")
	})
}

// controlFilters: finding what is wanted without reading everything.
func (h *harness) controlFilters(t *testing.T) {
	ctx, cancel := ctxT(120 * 1e9)
	defer cancel()
	with := func(kv ...string) map[string]string {
		m := lbls(kv...)
		m["conformance-filters"] = h.run
		return m
	}
	mk := func(name string, labels map[string]string) *controlv1.Function {
		t.Helper()
		r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name, Labels: labels}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetFunction()
	}
	list := func(req *controlv1.ListFunctionsRequest) ([]string, error) {
		var names []string
		for {
			req.PageSize = 2
			r, err := h.fn.ListFunctions(ctx, connect.NewRequest(req))
			if err != nil {
				return nil, err
			}
			for _, f := range r.Msg.GetFunctions() {
				names = append(names, f.GetName())
			}
			if req.PageToken = r.Msg.GetNextPageToken(); req.PageToken == "" {
				sort.Strings(names)
				return names, nil
			}
		}
	}
	a, b, c, d := h.name("flt-a"), h.name("flt-b"), h.name("flt-c"), h.name("flt-d")
	mk(a, with("owner", "gitops", "env", "prod"))
	mk(b, with("owner", "gitops"))
	mk(c, with("env", ""))
	mk(d, with())
	names := func(ns ...string) string { sort.Strings(ns); return strings.Join(ns, ",") }

	t.Run("a key is required whatever its value", func(t *testing.T) {
		for _, tc := range []struct {
			req  *controlv1.ListFunctionsRequest
			want string
		}{
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"owner"}}, names(a, b)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"owner", "env"}}, names(a)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"env"}}, names(a, c)}, // an empty value is a value
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"nobody-has-this"}}, ""},
		} {
			got, err := list(tc.req)
			if err != nil || strings.Join(got, ",") != tc.want {
				t.Errorf("has %v: %v %v, want %s", tc.req.HasLabelKeys, got, err, tc.want)
			}
		}
	})

	t.Run("a key can be excluded: what nobody owns", func(t *testing.T) {
		for _, tc := range []struct {
			req  *controlv1.ListFunctionsRequest
			want string
		}{
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), LacksLabelKeys: []string{"owner"}}, names(c, d)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), LacksLabelKeys: []string{"owner", "env"}}, names(d)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), LacksLabelKeys: []string{"nobody-has-this"}}, names(a, b, c, d)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run, "owner", "gitops"), LacksLabelKeys: []string{"env"}}, names(b)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"env"}, LacksLabelKeys: []string{"owner"}}, names(c)},
		} {
			got, err := list(tc.req)
			if err != nil || strings.Join(got, ",") != tc.want {
				t.Errorf("match %v has %v lacks %v: %v %v, want %s", tc.req.MatchLabels, tc.req.HasLabelKeys, tc.req.LacksLabelKeys, got, err, tc.want)
			}
		}
	})

	t.Run("a filter that can only match nothing is refused", func(t *testing.T) {
		for what, req := range map[string]*controlv1.ListFunctionsRequest{
			"a key both required and excluded": {HasLabelKeys: []string{"x"}, LacksLabelKeys: []string{"x"}},
			"a key matched and excluded":       {MatchLabels: lbls("x", "1"), LacksLabelKeys: []string{"x"}},
			"a malformed key to require":       {HasLabelKeys: []string{"NOT A KEY"}},
			"a malformed key to exclude":       {LacksLabelKeys: []string{"Owner"}},
			"an empty key":                     {HasLabelKeys: []string{""}},
		} {
			_, err := list(req)
			checkCode(t, err, connect.CodeInvalidArgument, what)
		}
	})

	t.Run("a function is found by its name", func(t *testing.T) {
		for _, tc := range []struct {
			req  *controlv1.ListFunctionsRequest
			want string
		}{
			{&controlv1.ListFunctionsRequest{Name: a}, a},
			{&controlv1.ListFunctionsRequest{Name: strings.ToUpper(a)}, ""}, // names are lower case: this one names nothing
			{&controlv1.ListFunctionsRequest{Name: a[:len(a)-1]}, ""},       // exactly, not a prefix
			{&controlv1.ListFunctionsRequest{Name: h.name("flt-nobody")}, ""},
			{&controlv1.ListFunctionsRequest{Name: a, MatchLabels: lbls("owner", "gitops")}, a},
			{&controlv1.ListFunctionsRequest{Name: a, MatchLabels: lbls("owner", "human")}, ""}, // all the filters must hold
			{&controlv1.ListFunctionsRequest{Name: c, HasLabelKeys: []string{"owner"}}, ""},
		} {
			got, err := list(tc.req)
			if err != nil || strings.Join(got, ",") != tc.want {
				t.Errorf("%+v: %v %v, want %q", tc.req, got, err, tc.want)
			}
		}
	})

	t.Run("routes can be listed by function, and by label keys", func(t *testing.T) {
		f1, f2 := h.createFunction(t, h.name("flt-r1")), h.createFunction(t, h.name("flt-r2"))
		route := func(fid string, labels map[string]string) *controlv1.Route {
			t.Helper()
			r, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/", FunctionId: fid, Labels: labels}))
			if err != nil {
				t.Fatal(err)
			}
			return r.Msg.GetRoute()
		}
		r1, r2 := route(f1.GetId(), with("owner", "gitops")), route(f1.GetId(), with())
		r3 := route(f2.GetId(), with("owner", "gitops"))
		ids := func(req *controlv1.ListRoutesRequest) (string, error) {
			var out []string
			for {
				req.PageSize = 1
				r, err := h.rt.ListRoutes(ctx, connect.NewRequest(req))
				if err != nil {
					return "", err
				}
				for _, x := range r.Msg.GetRoutes() {
					out = append(out, x.GetId())
				}
				if req.PageToken = r.Msg.GetNextPageToken(); req.PageToken == "" {
					sort.Strings(out)
					return strings.Join(out, ","), nil
				}
			}
		}
		for _, tc := range []struct {
			req  *controlv1.ListRoutesRequest
			want []string
		}{
			{&controlv1.ListRoutesRequest{FunctionId: f1.GetId()}, []string{r1.GetId(), r2.GetId()}},
			{&controlv1.ListRoutesRequest{FunctionId: f2.GetId()}, []string{r3.GetId()}},
			{&controlv1.ListRoutesRequest{FunctionId: f1.GetId(), HasLabelKeys: []string{"owner"}}, []string{r1.GetId()}},
			{&controlv1.ListRoutesRequest{FunctionId: f1.GetId(), LacksLabelKeys: []string{"owner"}}, []string{r2.GetId()}},
			{&controlv1.ListRoutesRequest{FunctionId: f1.GetId(), MatchLabels: lbls("owner", "gitops"), LacksLabelKeys: []string{"conformance-filters"}}, nil},
			{&controlv1.ListRoutesRequest{FunctionId: "00000000-0000-0000-0000-000000000000"}, nil},
			{&controlv1.ListRoutesRequest{FunctionId: "not-an-id"}, nil},
		} {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if got, err := ids(tc.req); err != nil || got != strings.Join(want, ",") {
				t.Errorf("%+v: %v %v, want %v", tc.req, got, err, want)
			}
		}
		_, err := ids(&controlv1.ListRoutesRequest{HasLabelKeys: []string{"x"}, LacksLabelKeys: []string{"x"}})
		checkCode(t, err, connect.CodeInvalidArgument, "a contradictory filter on routes")
	})

	t.Run("versions can be listed by label keys", func(t *testing.T) {
		f := h.createFunction(t, h.name("flt-v"))
		mkv := func(tag string, labels map[string]string) string {
			r, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: f.GetId(), Module: h.module(tag), Abi: h.abi(), Labels: labels}))
			if err != nil {
				t.Fatal(err)
			}
			return r.Msg.GetVersion().GetId()
		}
		v1, v2 := mkv("fv1", lbls("commit", "abc")), mkv("fv2", nil)
		list := func(req *controlv1.ListVersionsRequest) []string {
			req.FunctionId = f.GetId()
			r, err := h.fn.ListVersions(ctx, connect.NewRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			var out []string
			for _, v := range r.Msg.GetVersions() {
				out = append(out, v.GetId())
			}
			return out
		}
		if got := list(&controlv1.ListVersionsRequest{HasLabelKeys: []string{"commit"}}); fmt.Sprint(got) != fmt.Sprint([]string{v1}) {
			t.Errorf("versions with a commit: %v", got)
		}
		if got := list(&controlv1.ListVersionsRequest{LacksLabelKeys: []string{"commit"}}); fmt.Sprint(got) != fmt.Sprint([]string{v2}) {
			t.Errorf("versions without a commit: %v", got)
		}
		_, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.GetId(), HasLabelKeys: []string{"Bad Key"}}))
		checkCode(t, err, connect.CodeInvalidArgument, "a malformed key on versions")
	})
}
