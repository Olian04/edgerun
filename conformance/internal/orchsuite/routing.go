package orchsuite

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
)

// Routing judges the orchestrator as the host of control.v1 RouteService: which
// host and path reach which function. It is judged through the operator's calls
// and through Invoke, whose answer is the proof that a route routes.
func Routing(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Authentication", h.routingAuthentication)
	t.Run("Routes", h.controlRoutes)
	t.Run("Pagination", h.routingPagination)
	t.Run("LabelsAtCreation", h.routeLabels)
	t.Run("LabelEditing", h.routeLabelEditing)
	t.Run("Filters", h.routeFilters)
	t.Run("Precedence", h.routingPrecedence)
}

func (h *harness) routingAuthentication(t *testing.T) {
	check.Authentication(t, h.scopes(), "admin", []string{h.env.ExecutionToken}, func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := controlv1connect.NewRouteServiceClient(http.DefaultClient, h.url, check.Bearer(token)).ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{}))
		return err
	})
}

func (h *harness) routingPagination(t *testing.T) {
	ctx, cancel := ctxT(2 * settle)
	defer cancel()

	t.Run("routes by host and prefix", func(t *testing.T) {
		fn := h.createFunction(t, h.name("pg-r"))
		base := "pg" + h.run + ".conformance.test"
		var want []string
		for _, host := range []string{"d", "b", "a", "c"} {
			for _, p := range []string{"/z", "/a"} {
				h.createRoute(t, host+"."+base, p, fn.GetId())
				want = append(want, host+"."+base+p)
			}
		}
		sort.Strings(want)
		var got []string
		for _, r := range h.allRoutes(t, 3) {
			if strings.HasSuffix(r.GetHostPattern(), base) {
				got = append(got, r.GetHostPattern()+r.GetPathPrefix())
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("routes = %v, want each once, ordered by (host, prefix): %v", got, want)
		}
	})

	t.Run("malformed tokens", func(t *testing.T) {
		_, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
	})
}

// routeLabels: routes carry labels from creation, and are found by them.
func (h *harness) routeLabels(t *testing.T) {
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	with := func(kv ...string) map[string]string {
		m := lbls(kv...)
		m["conformance-run"] = h.run
		return m
	}
	t.Run("routes carry them from creation", func(t *testing.T) {
		f := h.createFunction(t, h.name("lab-r"))
		host := h.host()
		if _, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: f.GetId(), Labels: with("owner", "gitops")})); err != nil {
			t.Fatal(err)
		}
		lr, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{MatchLabels: with("owner", "gitops")}))
		if err != nil || len(lr.Msg.GetRoutes()) != 1 || lr.Msg.GetRoutes()[0].GetHostPattern() != host || lr.Msg.GetRoutes()[0].GetLabels()["owner"] != "gitops" {
			t.Errorf("routes matching: %v %v", lr.Msg, err)
		}
	})
}

// routeLabelEditing: a route is relabelled in place, and nothing else about it changes.
func (h *harness) routeLabelEditing(t *testing.T) {
	ctx, cancel := ctxT(120 * time.Second)
	defer cancel()
	missing := "00000000-0000-0000-0000-000000000000"

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

// routeFilters: routes are listed by function and by label keys.
func (h *harness) routeFilters(t *testing.T) {
	ctx, cancel := ctxT(120 * time.Second)
	defer cancel()
	with := func(kv ...string) map[string]string {
		m := lbls(kv...)
		m["conformance-filters"] = h.run
		return m
	}

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
}

func (h *harness) controlRoutes(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	fn := h.createFunction(t, h.name("cr"))
	create := func(host, prefix, fnID string) (*controlv1.Route, error) {
		r, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fnID}))
		if err != nil {
			return nil, err
		}
		return r.Msg.GetRoute(), nil
	}
	base := h.host()

	t.Run("normalisation", func(t *testing.T) {
		for _, c := range []struct{ host, prefix, wantHost, wantPrefix string }{
			{strings.ToUpper(base), "/api/", base, "/api"},
			{"a." + base, "/", "a." + base, "/"},
			{"*." + base, "/x/y/", "*." + base, "/x/y"},
		} {
			r, err := create(c.host, c.prefix, fn.GetId())
			if err != nil {
				t.Fatalf("CreateRoute(%s%s): %v", c.host, c.prefix, err)
			}
			if r.GetHostPattern() != c.wantHost || r.GetPathPrefix() != c.wantPrefix || r.GetFunctionId() != fn.GetId() || r.GetId() == "" {
				t.Errorf("CreateRoute(%s%s) = %v, want %s%s", c.host, c.prefix, r, c.wantHost, c.wantPrefix)
			}
		}
	})

	t.Run("a route is unique after normalisation", func(t *testing.T) {
		_, err := create(base, "/api", fn.GetId())
		check.WantCode(t, err, connect.CodeAlreadyExists)
		_, err = create(strings.ToUpper(base), "/api/", fn.GetId())
		check.WantCode(t, err, connect.CodeAlreadyExists)
	})

	t.Run("validation", func(t *testing.T) {
		for _, prefix := range []string{"", "api", "/a//b", "/a?x=1", "/a#f", "/a b"} {
			_, err := create(h.host(), prefix, fn.GetId())
			check.WantCode(t, err, connect.CodeInvalidArgument)
		}
		for _, host := range []string{"", "has space", "*foo.test", "a.*.test"} {
			_, err := create(host, "/", fn.GetId())
			check.WantCode(t, err, connect.CodeInvalidArgument)
		}
		_, err := create(h.host(), "/", unknownID)
		check.WantCode(t, err, connect.CodeNotFound)
		_, err = create(h.host(), "/", "not-a-uuid")
		check.WantCode(t, err, connect.CodeNotFound)
	})

	t.Run("list and delete", func(t *testing.T) {
		r, err := create(h.host(), "/listed", fn.GetId())
		if err != nil {
			t.Fatal(err)
		}
		if !h.hasRoute(t, r.GetId()) {
			t.Fatal("a created route is not listed")
		}
		if _, err := h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: r.GetId()})); err != nil {
			t.Fatal(err)
		}
		if h.hasRoute(t, r.GetId()) {
			t.Error("a deleted route is still listed")
		}
		for _, id := range []string{r.GetId(), unknownID, "not-a-uuid"} {
			_, err := h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: id}))
			check.WantCode(t, err, connect.CodeNotFound)
		}
	})
}

// hasRoute pages through every route looking for id.
func (h *harness) hasRoute(t testing.TB, id string) bool {
	t.Helper()
	for _, r := range h.allRoutes(t, 0) {
		if r.GetId() == id {
			return true
		}
	}
	return false
}

func (h *harness) allRoutes(t testing.TB, pageSize uint32) []*controlv1.Route {
	t.Helper()
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	var out []*controlv1.Route
	tok := ""
	for i := 0; i < 10000; i++ {
		r, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageSize: pageSize, PageToken: tok}))
		if err != nil {
			t.Fatalf("ListRoutes: %v", err)
		}
		out = append(out, r.Msg.GetRoutes()...)
		if tok = r.Msg.GetNextPageToken(); tok == "" {
			return out
		}
	}
	t.Fatal("ListRoutes never ended")
	return nil
}

// routed deploys a function that answers with its own name and routes
// (host, prefix) to it.
func (h *harness) routed(t testing.TB, abi, host, prefix, who string) *controlv1.Function {
	t.Helper()
	fn := h.createFunction(t, h.name("rt"))
	h.createVersion(t, fn.GetId(), abi, h.module(who), nil, true)
	h.setEnv(t, fn.GetId(), "WHO", who, false)
	h.createRoute(t, host, prefix, fn.GetId())
	return fn
}

func (h *harness) routingPrecedence(t *testing.T) {
	abi := h.abi()
	h.worker(t, []string{abi}, echoEnv("WHO"))
	b := fmt.Sprintf("p%s.prec.test", h.run)

	h.routed(t, abi, "a."+b, "/", "exact")
	h.routed(t, abi, "*."+b, "/api", "wild-api")
	h.routed(t, abi, "*."+b, "/api/v1", "wild-api-v1")
	h.routed(t, abi, "*."+b, "/", "wild-root")
	h.routed(t, abi, "*.deep."+b, "/", "deep")
	h.routed(t, abi, "c."+b, "/only", "c-only")

	// Wait for the orchestrator to have seen the worker: the control changes
	// above are visible at once, the new worker is not promised to be.
	h.served(t, "a."+b)

	for _, c := range []struct{ host, path, want, why string }{
		{"a." + b, "/api/v1/x", "exact", "an exact host beats a wildcard even where the wildcard's prefix is longer"},
		{"b." + b, "/api/v1/x", "wild-api-v1", "the longest matching prefix wins"},
		{"b." + b, "/api/v1", "wild-api-v1", "a prefix matches the path equal to it"},
		{"b." + b, "/api/v2", "wild-api", "a shorter prefix when the longer one does not match"},
		{"b." + b, "/api", "wild-api", "a prefix matches the path equal to it"},
		{"b." + b, "/apix", "wild-root", "prefixes match whole path segments: /api is not a prefix of /apix"},
		{"b." + b, "/api/v1x", "wild-api", "prefixes match whole path segments: /api/v1 is not a prefix of /api/v1x"},
		{"b." + b, "/other", "wild-root", "the root prefix matches everything"},
		{"x.deep." + b, "/api", "deep", "the wildcard with the longer suffix is more specific"},
		{"c." + b, "/only/y", "c-only", "an exact host with a matching prefix"},
		{"c." + b, "/api", "wild-api", "an exact host with no matching prefix falls through to less specific hosts"},
		{"A." + strings.ToUpper(b), "/", "exact", "hosts are compared case-insensitively"},
		{"a." + b + ":8080", "/", "exact", "the port is not part of the host"},
		{"a." + b + ".", "/", "exact", "a trailing dot is not part of the host"},
		{"b." + b, "/x/../api/v1", "wild-root", "paths are matched as given, not cleaned"},
	} {
		r := h.invoke(t, call{host: c.host, path: c.path})
		if r.err != nil || bodyOf(r) != c.want {
			t.Errorf("%s%s: got %q (%v), want %q: %s", c.host, c.path, bodyOf(r), r.err, c.want, c.why)
		}
	}

	t.Run("a wildcard does not match its apex", func(t *testing.T) {
		check.WantReason(t, h.invoke(t, call{host: b, path: "/"}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	})
	t.Run("a host nothing routes", func(t *testing.T) {
		check.WantReason(t, h.invoke(t, call{host: "x." + h.run + ".elsewhere.test"}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	})
	t.Run("a catch-all", func(t *testing.T) {
		fn := h.routed(t, abi, "*", "/", "catch-all")
		defer func() {
			// Leave no catch-all behind for the groups that expect nothing to match.
			ctx, cancel := ctxT(settle)
			defer cancel()
			h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: fn.GetId()}))
		}()
		if r := h.invoke(t, call{host: "anything." + h.run + ".elsewhere.test", path: "/x"}); r.err != nil || bodyOf(r) != "catch-all" {
			t.Errorf("a * route must catch what nothing else matches: %q, %v", bodyOf(r), r.err)
		}
		if r := h.invoke(t, call{host: "a." + b}); bodyOf(r) != "exact" {
			t.Errorf("a * route must not shadow a more specific host: %q", bodyOf(r))
		}
	})
}
