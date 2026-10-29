package control

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/errs"
)

// maxPlanSteps bounds one Validate.
const maxPlanSteps = 128

var bindName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

type plans struct{ s *Service }

// outcome is what running a plan came to: the results of the steps that
// succeeded and, if one failed, which.
type outcome struct {
	results []*controlv1.StepResponse
	failure *controlv1.StepFailure
}

// errPlanFailed ends a plan's transaction at a step that failed on its merits:
// the transaction is rolled back, and the failure is an answer, not an error.
var errPlanFailed = errors.New("a step of the plan failed")

// Validate runs the steps in one dry run, so each sees what the ones before it
// did, and stops at the first that fails.
func (p plans) Validate(ctx context.Context, r *connect.Request[controlv1.ValidateRequest]) (*connect.Response[controlv1.ValidateResponse], error) {
	out, err := p.s.runPlan(ctx, r.Msg.GetSteps(), false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&controlv1.ValidateResponse{Results: out.results, Failure: out.failure}), nil
}

// Apply runs the same steps in one transaction that is committed if they all
// succeed.
func (p plans) Apply(ctx context.Context, r *connect.Request[controlv1.ApplyRequest]) (*connect.Response[controlv1.ApplyResponse], error) {
	out, err := p.s.runPlan(ctx, r.Msg.GetSteps(), true)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&controlv1.ApplyResponse{Results: out.results, Failure: out.failure}), nil
}

// runPlan is Validate and Apply: one function, so that a plan that validates
// cannot be applied differently. With commit the steps' changes are kept if
// every step succeeds, and the cache the invoke path reads is brought up to
// date before returning, as after any single change; without it they never are.
func (s *Service) runPlan(ctx context.Context, steps []*controlv1.Step, commit bool) (*outcome, error) {
	if len(steps) > maxPlanSteps {
		return nil, errs.Invalid("a plan has at most %d steps, got %d", maxPlanSteps, len(steps))
	}
	out := &outcome{}
	body := func(ctx context.Context) error {
		binds := map[string]string{} // name -> the id of what the step created
		phantom := map[string]bool{} // in a validation, those ids: they name nothing once the call is over
		for i, st := range steps {
			resp, err := s.step(ctx, st, binds, phantom)
			if err != nil {
				code := connect.CodeOf(s.fail(err))
				switch code {
				case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeAlreadyExists:
					var ce *connect.Error
					msg := err.Error()
					if errors.As(err, &ce) {
						msg = ce.Message()
					}
					out.failure = &controlv1.StepFailure{Step: uint32(i), Code: code.String(), Message: msg}
					return errPlanFailed
				}
				return err // the platform failed, not the plan
			}
			if !commit {
				scrub(resp.ProtoReflect(), phantom)
			}
			out.results = append(out.results, resp)
		}
		return nil
	}
	var err error
	if commit {
		err = s.cfg.Store.Atomic(ctx, body)
	} else {
		err = s.cfg.Store.DryRun(ctx, body)
	}
	switch {
	case errors.Is(err, errPlanFailed):
		if commit {
			out.results = nil // nothing was done
		}
	case err != nil:
		return nil, s.fail(err)
	case commit && len(steps) > 0:
		s.cfg.Refresh(ctx)
	}
	return out, nil
}

// step runs one step: binds the references in it, runs the operation of its own
// call, and records what it bound.
func (s *Service) step(ctx context.Context, st *controlv1.Step, binds map[string]string, phantom map[string]bool) (*controlv1.StepResponse, error) {
	if st.GetBind() != "" {
		if !bindName.MatchString(st.GetBind()) {
			return nil, errs.Invalid("bind %q: want 1-32 lower-case letters, digits and '_', starting with a letter", st.GetBind())
		}
		if _, dup := binds[st.GetBind()]; dup {
			return nil, errs.Invalid("bind %q is already used by an earlier step", st.GetBind())
		}
	}
	ref := func(id *string) error {
		name, ok := strings.CutPrefix(*id, "$")
		if !ok {
			return nil
		}
		real, ok := binds[name]
		if !ok {
			return errs.Invalid("%q refers to nothing: no earlier step binds %q", *id, name)
		}
		*id = real
		return nil
	}
	// created is the id of what a create step made, for it to be bound.
	var created func(*controlv1.StepResponse) string
	var run func() (*controlv1.StepResponse, error)

	switch q := st.GetRequest().(type) {
	case *controlv1.Step_CreateFunction:
		created = func(r *controlv1.StepResponse) string { return r.GetCreateFunction().GetFunction().GetId() }
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.createFunction(ctx, q.CreateFunction)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_CreateFunction{CreateFunction: resp}}, err
		}
	case *controlv1.Step_DeleteFunction:
		if err := ref(&q.DeleteFunction.FunctionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.deleteFunction(ctx, q.DeleteFunction)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_DeleteFunction{DeleteFunction: resp}}, err
		}
	case *controlv1.Step_CreateVersion:
		if err := ref(&q.CreateVersion.FunctionId); err != nil {
			return nil, err
		}
		created = func(r *controlv1.StepResponse) string { return r.GetCreateVersion().GetVersion().GetId() }
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.createVersion(ctx, q.CreateVersion)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_CreateVersion{CreateVersion: resp}}, err
		}
	case *controlv1.Step_ActivateVersion:
		if err := ref(&q.ActivateVersion.FunctionId); err != nil {
			return nil, err
		}
		if err := ref(&q.ActivateVersion.VersionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.activateVersion(ctx, q.ActivateVersion)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_ActivateVersion{ActivateVersion: resp}}, err
		}
	case *controlv1.Step_SetEnv:
		if err := ref(&q.SetEnv.FunctionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.setEnv(ctx, q.SetEnv)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_SetEnv{SetEnv: resp}}, err
		}
	case *controlv1.Step_DeleteEnv:
		if err := ref(&q.DeleteEnv.FunctionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.deleteEnv(ctx, q.DeleteEnv)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_DeleteEnv{DeleteEnv: resp}}, err
		}
	case *controlv1.Step_SetEgress:
		if err := ref(&q.SetEgress.FunctionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.setEgress(ctx, q.SetEgress)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_SetEgress{SetEgress: resp}}, err
		}
	case *controlv1.Step_SetLabels:
		if err := ref(&q.SetLabels.FunctionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.setLabels(ctx, q.SetLabels)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_SetLabels{SetLabels: resp}}, err
		}
	case *controlv1.Step_PatchLabels:
		if err := ref(&q.PatchLabels.FunctionId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.patchLabels(ctx, q.PatchLabels)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_PatchLabels{PatchLabels: resp}}, err
		}
	case *controlv1.Step_CreateRoute:
		if err := ref(&q.CreateRoute.FunctionId); err != nil {
			return nil, err
		}
		created = func(r *controlv1.StepResponse) string { return r.GetCreateRoute().GetRoute().GetId() }
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.createRoute(ctx, q.CreateRoute)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_CreateRoute{CreateRoute: resp}}, err
		}
	case *controlv1.Step_DeleteRoute:
		if err := ref(&q.DeleteRoute.RouteId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.deleteRoute(ctx, q.DeleteRoute)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_DeleteRoute{DeleteRoute: resp}}, err
		}
	case *controlv1.Step_SetRouteLabels:
		if err := ref(&q.SetRouteLabels.RouteId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.setRouteLabels(ctx, q.SetRouteLabels)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_SetRouteLabels{SetRouteLabels: resp}}, err
		}
	case *controlv1.Step_PatchRouteLabels:
		if err := ref(&q.PatchRouteLabels.RouteId); err != nil {
			return nil, err
		}
		run = func() (*controlv1.StepResponse, error) {
			resp, err := s.patchRouteLabels(ctx, q.PatchRouteLabels)
			return &controlv1.StepResponse{Response: &controlv1.StepResponse_PatchRouteLabels{PatchRouteLabels: resp}}, err
		}
	default:
		return nil, errs.Invalid("a step must carry a request")
	}

	if st.GetBind() != "" && created == nil {
		return nil, errs.Invalid("bind %q: only a step that creates a function, a version or a route can bind", st.GetBind())
	}
	resp, err := run()
	if err != nil {
		return nil, err
	}
	if st.GetBind() != "" {
		binds[st.GetBind()] = created(resp)
	}
	if created != nil {
		phantom[created(resp)] = true
	}
	return resp, nil
}

// scrub blanks, in m, every string that is the id of something created in the
// dry run: after the call there is no such thing, and an id that names nothing
// would only mislead.
func scrub(m protoreflect.Message, phantom map[string]bool) {
	var blank []protoreflect.FieldDescriptor
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap(), fd.IsList():
		case fd.Kind() == protoreflect.StringKind:
			if phantom[v.String()] {
				blank = append(blank, fd)
			}
		case fd.Kind() == protoreflect.MessageKind:
			scrub(v.Message(), phantom)
		}
		return true
	})
	for _, fd := range blank {
		m.Clear(fd)
	}
}
