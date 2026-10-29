package orchestratorconf

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// This file is the suite for PlanService.Validate (docs/protocol.md, "Validate a
// plan"): validate-only for a sequence of changes.

func stepCreateFunction(bind, name string, labels map[string]string) *controlv1.Step {
	return &controlv1.Step{Bind: bind, Request: &controlv1.Step_CreateFunction{CreateFunction: &controlv1.CreateFunctionRequest{Name: name, Labels: labels}}}
}

func stepCreateVersion(bind, function string, module []byte, abi string, activate bool) *controlv1.Step {
	return &controlv1.Step{Bind: bind, Request: &controlv1.Step_CreateVersion{CreateVersion: &controlv1.CreateVersionRequest{FunctionId: function, Module: module, Abi: abi, Activate: activate}}}
}

func stepCreateRoute(bind, host, prefix, function string) *controlv1.Step {
	return &controlv1.Step{Bind: bind, Request: &controlv1.Step_CreateRoute{CreateRoute: &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: function}}}
}

func stepDeleteRoute(id string) *controlv1.Step {
	return &controlv1.Step{Request: &controlv1.Step_DeleteRoute{DeleteRoute: &controlv1.DeleteRouteRequest{RouteId: id}}}
}

func stepDeleteFunction(id string) *controlv1.Step {
	return &controlv1.Step{Request: &controlv1.Step_DeleteFunction{DeleteFunction: &controlv1.DeleteFunctionRequest{FunctionId: id}}}
}

func stepSetEnv(function, name, value string) *controlv1.Step {
	return &controlv1.Step{Request: &controlv1.Step_SetEnv{SetEnv: &controlv1.SetEnvRequest{FunctionId: function, Entry: &controlv1.EnvEntry{Name: name, Value: value}}}}
}

func stepDeleteEnv(function, name string) *controlv1.Step {
	return &controlv1.Step{Request: &controlv1.Step_DeleteEnv{DeleteEnv: &controlv1.DeleteEnvRequest{FunctionId: function, Name: name}}}
}

func (h *harness) plans(t *testing.T) {
	ctx, cancel := ctxT(180 * 1e9)
	defer cancel()
	abi := h.abi()
	missing := "00000000-0000-0000-0000-000000000000"

	validate := func(steps ...*controlv1.Step) *controlv1.ValidateResponse {
		t.Helper()
		r, err := h.plan.Validate(ctx, connect.NewRequest(&controlv1.ValidateRequest{Steps: steps}))
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		return r.Msg
	}
	valid := func(what string, steps ...*controlv1.Step) *controlv1.ValidateResponse {
		t.Helper()
		r := validate(steps...)
		if r.GetFailure() != nil || len(r.GetResults()) != len(steps) {
			t.Fatalf("%s: want a valid plan with %d results, got failure %v and %d results", what, len(steps), r.GetFailure(), len(r.GetResults()))
		}
		return r
	}
	fails := func(what string, at int, code connect.Code, steps ...*controlv1.Step) *controlv1.ValidateResponse {
		t.Helper()
		r := validate(steps...)
		f := r.GetFailure()
		if f == nil {
			t.Errorf("%s: the plan was valid, want step %d to fail with %v", what, at, code)
			return r
		}
		if int(f.GetStep()) != at || f.GetCode() != code.String() || len(r.GetResults()) != at {
			t.Errorf("%s: failed at step %d with %q after %d results, want step %d with %q after %d results (%s)",
				what, f.GetStep(), f.GetCode(), len(r.GetResults()), at, code, at, f.GetMessage())
		}
		return r
	}

	t.Run("an empty plan is valid", func(t *testing.T) {
		valid("empty")
	})

	t.Run("a step sees what the steps before it did", func(t *testing.T) {
		name, host := h.name("plan-new"), h.host()
		module := h.module("plan-new")
		r := valid("a whole function",
			stepCreateFunction("fn", name, lbls("owner", "plan")),
			stepCreateVersion("v1", "$fn", module, abi, false),
			&controlv1.Step{Request: &controlv1.Step_ActivateVersion{ActivateVersion: &controlv1.ActivateVersionRequest{FunctionId: "$fn", VersionId: "$v1"}}},
			stepSetEnv("$fn", "GREETING", "hello"),
			&controlv1.Step{Request: &controlv1.Step_SetEgress{SetEgress: &controlv1.SetEgressRequest{FunctionId: "$fn", Allow: []string{"API.example.com"}}}},
			&controlv1.Step{Request: &controlv1.Step_PatchLabels{PatchLabels: &controlv1.PatchLabelsRequest{FunctionId: "$fn", Set: lbls("extra", "1")}}},
			stepCreateRoute("rt", host, "/", "$fn"),
			&controlv1.Step{Request: &controlv1.Step_SetRouteLabels{SetRouteLabels: &controlv1.SetRouteLabelsRequest{RouteId: "$rt", Labels: lbls("owner", "plan")}}},
		)
		// What each step would have answered, as the call itself would, with no id that names nothing.
		if f := r.GetResults()[0].GetCreateFunction().GetFunction(); f.GetName() != name || f.GetId() != "" || f.GetLabels()["owner"] != "plan" {
			t.Errorf("the create: %v", f)
		}
		if v := r.GetResults()[1].GetCreateVersion().GetVersion(); v.GetId() != "" || v.GetFunctionId() != "" || v.GetArtifactDigest() != sha(module) || v.GetNumber() != 1 {
			t.Errorf("the version names no id (its own or its function's) and says what it would be: %v", v)
		}
		if f := r.GetResults()[2].GetActivateVersion().GetFunction(); f.GetId() != "" || f.GetActiveVersionId() != "" || f.GetName() != name {
			t.Errorf("the activation names no phantom ids: %v", f)
		}
		if e := r.GetResults()[3].GetSetEnv().GetEntry(); e.GetName() != "GREETING" || e.GetRevision() == "" {
			t.Errorf("the env entry carries the revision it would have: %v", e)
		}
		if f := r.GetResults()[4].GetSetEgress().GetFunction(); strings.Join(f.GetEgressAllow(), ",") != "api.example.com" {
			t.Errorf("the egress list is canonical: %v", f.GetEgressAllow())
		}
		if f := r.GetResults()[5].GetPatchLabels().GetFunction(); f.GetLabels()["owner"] != "plan" || f.GetLabels()["extra"] != "1" {
			t.Errorf("the patch applied to what the create made: %v", f.GetLabels())
		}
		if rt := r.GetResults()[6].GetCreateRoute().GetRoute(); rt.GetId() != "" || rt.GetFunctionId() != "" || rt.GetHostPattern() != host {
			t.Errorf("the route: %v", rt)
		}
		if rt := r.GetResults()[7].GetSetRouteLabels().GetRoute(); rt.GetLabels()["owner"] != "plan" || rt.GetId() != "" {
			t.Errorf("the route relabelled: %v", rt)
		}

		// Nothing is left behind.
		if got, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{Name: name})); err != nil || len(got.Msg.GetFunctions()) != 0 {
			t.Errorf("the function exists after a validation: %v %v", got.Msg, err)
		}
		if got, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{})); err == nil {
			for _, x := range got.Msg.GetRoutes() {
				if x.GetHostPattern() == host {
					t.Error("the route exists after a validation")
				}
			}
		}
		if _, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name})); err != nil {
			t.Errorf("the name was not reserved by the validation: %v", err)
		}
	})

	t.Run("what no sequence of single validations can show", func(t *testing.T) {
		f := h.createFunction(t, h.name("plan-seq"))
		host := h.host()
		rt := h.createRoute(t, host, "/", f.GetId())

		// A retarget: the route is there, so a create alone conflicts, but not after its deletion.
		_, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: f.GetId(), ValidateOnly: true}))
		checkCode(t, err, connect.CodeAlreadyExists, "the create alone")
		fails("the create alone", 0, connect.CodeAlreadyExists, stepCreateRoute("", host, "/", f.GetId()))
		valid("delete, then create again", stepDeleteRoute(rt.GetId()), stepCreateRoute("", host, "/", f.GetId()))
		fails("create, then delete the same thing twice", 1, connect.CodeNotFound, stepDeleteRoute(rt.GetId()), stepDeleteRoute(rt.GetId()))

		// A name freed by a deletion.
		valid("delete a function, then create its name", stepDeleteFunction(f.GetId()), stepCreateFunction("", f.GetName(), nil))
		fails("delete a function, then use it", 1, connect.CodeNotFound, stepDeleteFunction(f.GetId()), stepSetEnv(f.GetId(), "X", "1"))

		// A limit that depends on what the earlier steps stored: 64 variables.
		g := h.createFunction(t, h.name("plan-env"))
		for i := 0; i < 64; i++ {
			h.setEnv(t, g.GetId(), fmt.Sprintf("V%02d", i), "x", false)
		}
		_, err = h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: g.GetId(), Entry: &controlv1.EnvEntry{Name: "ONE_MORE", Value: "x"}, ValidateOnly: true}))
		checkCode(t, err, connect.CodeInvalidArgument, "a 65th variable alone")
		fails("a 65th variable", 0, connect.CodeInvalidArgument, stepSetEnv(g.GetId(), "ONE_MORE", "x"))
		valid("a variable that fits once another is deleted", stepDeleteEnv(g.GetId(), "V00"), stepSetEnv(g.GetId(), "ONE_MORE", "x"))
		fails("two new variables for one freed", 2, connect.CodeInvalidArgument,
			stepDeleteEnv(g.GetId(), "V00"), stepSetEnv(g.GetId(), "ONE_MORE", "x"), stepSetEnv(g.GetId(), "TWO_MORE", "x"))
	})

	t.Run("the failing step is named, with the error its own call gives", func(t *testing.T) {
		f := h.createFunction(t, h.name("plan-err"))
		host := h.host()
		h.createRoute(t, host, "/taken", f.GetId())
		for _, tc := range []struct {
			name string
			step *controlv1.Step
			code connect.Code
			call func() error // the same change, as a validate-only call
		}{
			{"a name taken", stepCreateFunction("", f.GetName(), nil), connect.CodeAlreadyExists, func() error {
				_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: f.GetName(), ValidateOnly: true}))
				return err
			}},
			{"a bad name", stepCreateFunction("", "Bad Name", nil), connect.CodeInvalidArgument, func() error {
				_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "Bad Name", ValidateOnly: true}))
				return err
			}},
			{"bad labels", stepCreateFunction("", h.name("plan-bl"), lbls("BAD", "x")), connect.CodeInvalidArgument, func() error {
				_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("plan-bl"), Labels: lbls("BAD", "x"), ValidateOnly: true}))
				return err
			}},
			{"a route taken", stepCreateRoute("", host, "/taken", f.GetId()), connect.CodeAlreadyExists, func() error {
				_, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/taken", FunctionId: f.GetId(), ValidateOnly: true}))
				return err
			}},
			{"a route to nothing", stepCreateRoute("", h.host(), "/", missing), connect.CodeNotFound, func() error {
				_, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/", FunctionId: missing, ValidateOnly: true}))
				return err
			}},
			{"a reserved variable", stepSetEnv(f.GetId(), "PATH_INFO", "x"), connect.CodeInvalidArgument, func() error {
				_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: f.GetId(), Entry: &controlv1.EnvEntry{Name: "PATH_INFO", Value: "x"}, ValidateOnly: true}))
				return err
			}},
			{"a bad abi", stepCreateVersion("", f.GetId(), h.module("pe"), "NOT AN ABI", false), connect.CodeInvalidArgument, func() error {
				_, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: f.GetId(), Module: h.module("pe"), Abi: "NOT AN ABI", ValidateOnly: true}))
				return err
			}},
		} {
			// The failing step comes after one that is fine: its index is what says so.
			r := fails(tc.name, 1, tc.code, stepSetEnv(f.GetId(), "FINE", "1"), tc.step)
			err := tc.call()
			if r.GetFailure() != nil && err != nil {
				var ce *connect.Error
				if errors.As(err, &ce) && r.GetFailure().GetMessage() != ce.Message() {
					t.Errorf("%s: the message is %q, the call's own error says %q", tc.name, r.GetFailure().GetMessage(), ce.Message())
				}
				if connect.CodeOf(err).String() != r.GetFailure().GetCode() {
					t.Errorf("%s: code %q, the call's own is %v", tc.name, r.GetFailure().GetCode(), connect.CodeOf(err))
				}
			}
		}
		// The first step is the one that fails: no results.
		fails("the first step", 0, connect.CodeNotFound, stepSetEnv(missing, "A", "1"), stepSetEnv(f.GetId(), "B", "1"))
	})

	t.Run("references", func(t *testing.T) {
		f := h.createFunction(t, h.name("plan-ref"))
		fails("a reference nothing bound", 0, connect.CodeInvalidArgument, stepSetEnv("$nobody", "A", "1"))
		fails("a reference before its bind", 0, connect.CodeInvalidArgument,
			stepSetEnv("$later", "A", "1"), stepCreateFunction("later", h.name("plan-late"), nil))
		fails("a bind used twice", 1, connect.CodeInvalidArgument,
			stepCreateFunction("same", h.name("plan-d1"), nil), stepCreateFunction("same", h.name("plan-d2"), nil))
		for _, bad := range []string{"Upper", "1digit", "has-dash", "a b", strings.Repeat("x", 33), "$dollar"} {
			fails("the bind "+bad, 0, connect.CodeInvalidArgument, stepCreateFunction(bad, h.name("plan-bb"), nil))
		}
		fails("a bind on a step that creates nothing", 0, connect.CodeInvalidArgument,
			&controlv1.Step{Bind: "x", Request: &controlv1.Step_SetEnv{SetEnv: &controlv1.SetEnvRequest{FunctionId: f.GetId(), Entry: &controlv1.EnvEntry{Name: "A", Value: "1"}}}})
		fails("a step with no request", 0, connect.CodeInvalidArgument, &controlv1.Step{})
		fails("a version bound as a function", 1, connect.CodeNotFound,
			stepCreateVersion("ver", f.GetId(), h.module("rv"), abi, false), stepSetEnv("$ver", "A", "1"))
		valid("a real id still works beside a bound one", stepCreateFunction("n", h.name("plan-ok"), nil), stepSetEnv(f.GetId(), "A", "1"), stepSetEnv("$n", "A", "1"))
	})

	t.Run("it changes nothing, whatever it says", func(t *testing.T) {
		f := h.createFunction(t, h.name("plan-keep"))
		host := h.host()
		rt := h.createRoute(t, host, "/", f.GetId())
		h.setEnv(t, f.GetId(), "KEEP", "me", false)
		snapshot := func() string {
			var sb strings.Builder
			if g, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: f.GetId()})); err == nil {
				fmt.Fprintf(&sb, "%v|", g.Msg.GetFunction())
			}
			if es, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: f.GetId()})); err == nil {
				fmt.Fprintf(&sb, "%v|", es.Msg.GetEntries())
			}
			if vs, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.GetId()})); err == nil {
				fmt.Fprintf(&sb, "%d versions|", len(vs.Msg.GetVersions()))
			}
			if rs, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{FunctionId: f.GetId()})); err == nil {
				fmt.Fprintf(&sb, "%v|", rs.Msg.GetRoutes())
			}
			return sb.String()
		}
		before := snapshot()
		validate(stepDeleteRoute(rt.GetId()), stepDeleteEnv(f.GetId(), "KEEP"), stepCreateVersion("", f.GetId(), h.module("keep"), abi, true),
			stepDeleteFunction(f.GetId())) // valid, and everything in it would be undone by the rollback
		validate(stepDeleteEnv(f.GetId(), "KEEP"), stepSetEnv(missing, "A", "1"), stepDeleteFunction(f.GetId())) // failing at step 1
		if after := snapshot(); after != before {
			t.Errorf("a validation changed something:\n before %s\n after  %s", before, after)
		}
		// Still routable and writable as if nothing had happened.
		if _, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: f.GetId(), Entry: &controlv1.EnvEntry{Name: "AFTER", Value: "1"}})); err != nil {
			t.Errorf("a function a validation would have deleted: %v", err)
		}
	})

	t.Run("the size of a plan is bounded", func(t *testing.T) {
		steps := make([]*controlv1.Step, 129)
		for i := range steps {
			steps[i] = stepSetEnv(missing, "A", "1")
		}
		_, err := h.plan.Validate(ctx, connect.NewRequest(&controlv1.ValidateRequest{Steps: steps}))
		checkCode(t, err, connect.CodeInvalidArgument, "129 steps")
		if r, err := h.plan.Validate(ctx, connect.NewRequest(&controlv1.ValidateRequest{Steps: steps[:128]})); err != nil || r.Msg.GetFailure().GetStep() != 0 {
			t.Errorf("128 steps are accepted (and the first fails on its own merits): %v %v", r.Msg, err)
		}
	})
}

// invocationLookup: from a response, or an error, to the record of its invocation.
func (h *harness) invocationLookup(t *testing.T) {
	_, f := h.stack(t, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		if req.GetRequest().GetPath() == "/trap" {
			return check.Error(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, "trapped")
		}
		return respond("ok")(ctx, req, st)
	})
	h.served(t, f.host)

	good := h.mustInvoke(t, call{host: f.host, requestID: "lk-ok-" + h.run})
	bad := h.invoke(t, call{host: f.host, path: "/trap", requestID: "lk-bad-" + h.run})
	check.WantReason(t, bad.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	unrouted := h.invoke(t, call{host: "unrouted-" + h.run + ".conformance.test"})
	check.WantReason(t, unrouted.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)

	errInvocation := func(err error) string {
		var ce *connect.Error
		if !errors.As(err, &ce) {
			return ""
		}
		for _, d := range ce.Details() {
			if v, verr := d.Value(); verr == nil {
				if info, ok := v.(*commonv1.ErrorInfo); ok {
					return info.GetInvocationId()
				}
			}
		}
		return ""
	}
	get := func(id string) (*controlv1.Invocation, error) {
		ctx, cancel := ctxT(settle)
		defer cancel()
		r, err := h.insp.GetInvocation(ctx, connect.NewRequest(&controlv1.GetInvocationRequest{InvocationId: id}))
		if err != nil {
			return nil, err
		}
		return r.Msg.GetInvocation(), nil
	}

	t.Run("an error names the invocation that failed", func(t *testing.T) {
		id := errInvocation(bad.err)
		if id == "" {
			t.Fatal("an Invoke error for a routed invocation carries no invocation_id in its ErrorInfo")
		}
		var rec *controlv1.Invocation
		check.Eventually(t, "the record to be readable", settle, func() bool {
			var err error
			rec, err = get(id)
			return err == nil
		})
		if rec.GetId() != id || rec.GetRequestId() != "lk-bad-"+h.run || rec.GetErrorReason() != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED {
			t.Errorf("the record the id leads to: %v", rec)
		}
	})

	t.Run("an error that has no invocation names none", func(t *testing.T) {
		if id := errInvocation(unrouted.err); id != "" {
			t.Errorf("FUNCTION_NOT_FOUND carries invocation_id %q: no record is written for it", id)
		}
	})

	t.Run("a response names its invocation, and so does its record", func(t *testing.T) {
		var rec *controlv1.Invocation
		check.Eventually(t, "the record to be readable", settle, func() bool {
			var err error
			rec, err = get(good.invocationID)
			return err == nil
		})
		if rec.GetId() != good.invocationID || rec.GetRequestId() != "lk-ok-"+h.run || rec.GetFunctionId() != f.fn.GetId() || rec.GetStatus() != 200 {
			t.Errorf("the record: %v", rec)
		}
	})

	t.Run("nothing else is found", func(t *testing.T) {
		for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-an-id", ""} {
			_, err := get(id)
			checkCode(t, err, connect.CodeNotFound, fmt.Sprintf("GetInvocation(%q)", id))
		}
	})

	t.Run("the list can be asked for one request id", func(t *testing.T) {
		list := func(req *controlv1.ListInvocationsRequest) []*controlv1.Invocation {
			t.Helper()
			var out []*controlv1.Invocation
			for {
				ctx, cancel := ctxT(2 * settle)
				r, err := h.insp.ListInvocations(ctx, connect.NewRequest(req))
				cancel()
				if err != nil {
					t.Fatalf("ListInvocations: %v", err)
				}
				out = append(out, r.Msg.GetInvocations()...)
				if req.PageToken = r.Msg.GetNextPageToken(); req.PageToken == "" {
					return out
				}
			}
		}
		// Three invocations share one request id (a client may send the same X-Request-Id more than once).
		shared := "lk-shared-" + h.run
		for i := 0; i < 3; i++ {
			h.mustInvoke(t, call{host: f.host, requestID: shared})
		}
		check.Eventually(t, "the three records", settle, func() bool {
			return len(list(&controlv1.ListInvocationsRequest{RequestId: shared})) == 3
		})
		paged := list(&controlv1.ListInvocationsRequest{RequestId: shared, PageSize: 1})
		if len(paged) != 3 {
			t.Errorf("paged one at a time: %d records, want 3", len(paged))
		}
		for _, r := range paged {
			if r.GetRequestId() != shared {
				t.Errorf("a record of request %q in the list for %q", r.GetRequestId(), shared)
			}
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: "lk-ok-" + h.run}); len(got) != 1 || got[0].GetId() != good.invocationID {
			t.Errorf("one request id, one record: %v", got)
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: "lk-nobody-" + h.run}); len(got) != 0 {
			t.Errorf("an unknown request id: %v", got)
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: shared, FunctionId: "00000000-0000-0000-0000-000000000000"}); len(got) != 0 {
			t.Errorf("both filters must hold: %v", got)
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: shared, FunctionId: f.fn.GetId()}); len(got) != 3 {
			t.Errorf("both filters, both true: %d records", len(got))
		}
	})
}
