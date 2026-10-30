// Package control implements the orchestrator's control.v1 services: the API
// the admin UI and CLIs use to manage functions, routes and environment, and
// to inspect workers and invocations.
package control

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/apierr"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/telemetry"
)

// AdminScope is the token scope operators present.
const AdminScope = edgerun.ScopeAdmin

var abiPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,31}$`)

// Config configures the services.
type Config struct {
	Store *store.Store
	// Refresh is called, and waited for, after every write the invoke path
	// caches (routes, deployed versions, environment). A client that deploys
	// and then immediately sends a request to this orchestrator therefore sees
	// its own change; other instances catch up within their refresh interval.
	Refresh func(ctx context.Context)
	// MaxModuleBytes bounds an uploaded module. Default 32 MiB.
	MaxModuleBytes uint64
	// AliveWithin is how recently a worker must have heartbeated to be alive.
	AliveWithin time.Duration
	Version     string
	Log         *slog.Logger
}

// Service implements the control.v1 services.
type Service struct{ cfg Config }

// New returns a service.
func New(cfg Config) *Service {
	if cfg.MaxModuleBytes == 0 {
		cfg.MaxModuleBytes = 32 << 20
	}
	if cfg.AliveWithin == 0 {
		cfg.AliveWithin = 15 * time.Second
	}
	if cfg.Refresh == nil {
		cfg.Refresh = func(context.Context) {}
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	return &Service{cfg: cfg}
}

// Mount registers the services on mux, guarded by the admin scope.
func (s *Service) Mount(mux *http.ServeMux, v authn.Verifier) {
	small := connect.WithHandlerOptions(connect.WithInterceptors(telemetry.ServerInterceptor(), authn.Server(v, AdminScope)), connect.WithCompressMinBytes(authn.CompressMinBytes))
	upload := []connect.HandlerOption{small, connect.WithReadMaxBytes(int(s.cfg.MaxModuleBytes) + (1 << 20))}
	mux.Handle(controlv1connect.NewFunctionServiceHandler(functions{s}, upload...))
	mux.Handle(controlv1connect.NewRouteServiceHandler(routes{s}, small))
	mux.Handle(controlv1connect.NewPlanServiceHandler(plans{s}, upload...))
	mux.Handle(controlv1connect.NewInspectionServiceHandler(inspection{s}, small))
}

func (s *Service) fail(err error) error { return apierr.FromStore(s.cfg.Log, err) }

// mutate runs op, a change to the store. With validateOnly it runs as a dry
// run: against the real state, meeting every check and conflict, with nothing
// kept. The caller must not refresh the cache for a validate-only call.
func mutate[T any](ctx context.Context, s *Service, validateOnly bool, op func(ctx context.Context) (T, error)) (out T, err error) {
	if !validateOnly {
		return op(ctx)
	}
	err = s.cfg.Store.DryRun(ctx, func(ctx context.Context) error {
		out, err = op(ctx)
		return err
	})
	return out, err
}

// --- operations --------------------------------------------------------------
//
// Each change the control services make is one function here: the request's own
// checks and the store call, and nothing that has to happen only when the
// change is kept (refreshing the cache) or only for the caller of one RPC
// (blanking the ids of a validate-only create). The RPCs and PlanService.Validate
// both run them, so a step of a plan is checked by the very code its own call is.

func (s *Service) createFunction(ctx context.Context, m *controlv1.CreateFunctionRequest) (*controlv1.CreateFunctionResponse, error) {
	fn, err := s.cfg.Store.CreateFunction(ctx, m.GetName(), m.GetLabels())
	return &controlv1.CreateFunctionResponse{Function: fn}, err
}

func (s *Service) deleteFunction(ctx context.Context, m *controlv1.DeleteFunctionRequest) (*controlv1.DeleteFunctionResponse, error) {
	return &controlv1.DeleteFunctionResponse{}, s.cfg.Store.DeleteFunction(ctx, m.GetFunctionId())
}

func (s *Service) createVersion(ctx context.Context, m *controlv1.CreateVersionRequest) (*controlv1.CreateVersionResponse, error) {
	if uint64(len(m.GetModule())) > s.cfg.MaxModuleBytes {
		return nil, errs.Invalid("module is %d bytes; the limit is %d", len(m.GetModule()), s.cfg.MaxModuleBytes)
	}
	if !abiPattern.MatchString(m.GetAbi()) {
		return nil, errs.Invalid("abi %q: want a short lower-case name such as \"wagi-1\"", m.GetAbi())
	}
	v, err := s.cfg.Store.CreateVersion(ctx, store.NewVersion{
		FunctionID: m.GetFunctionId(), Module: m.GetModule(), ABI: m.GetAbi(), Limits: m.GetLimits(), Activate: m.GetActivate(),
		Labels: m.GetLabels(),
	})
	return &controlv1.CreateVersionResponse{Version: v}, err
}

func (s *Service) activateVersion(ctx context.Context, m *controlv1.ActivateVersionRequest) (*controlv1.ActivateVersionResponse, error) {
	fn, err := s.cfg.Store.ActivateVersion(ctx, m.GetFunctionId(), m.GetVersionId())
	return &controlv1.ActivateVersionResponse{Function: fn}, err
}

func (s *Service) setEnv(ctx context.Context, m *controlv1.SetEnvRequest) (*controlv1.SetEnvResponse, error) {
	if m.GetEntry() == nil {
		return nil, errs.Invalid("entry is required")
	}
	e, err := s.cfg.Store.SetEnv(ctx, m.GetFunctionId(), m.GetEntry())
	return &controlv1.SetEnvResponse{Entry: e}, err
}

func (s *Service) deleteEnv(ctx context.Context, m *controlv1.DeleteEnvRequest) (*controlv1.DeleteEnvResponse, error) {
	return &controlv1.DeleteEnvResponse{}, s.cfg.Store.DeleteEnv(ctx, m.GetFunctionId(), m.GetName())
}

func (s *Service) setEgress(ctx context.Context, m *controlv1.SetEgressRequest) (*controlv1.SetEgressResponse, error) {
	fn, err := s.cfg.Store.SetEgress(ctx, m.GetFunctionId(), m.GetAllow())
	return &controlv1.SetEgressResponse{Function: fn}, err
}

func (s *Service) setLabels(ctx context.Context, m *controlv1.SetLabelsRequest) (*controlv1.SetLabelsResponse, error) {
	fn, err := s.cfg.Store.SetLabels(ctx, m.GetFunctionId(), m.GetLabels())
	return &controlv1.SetLabelsResponse{Function: fn}, err
}

func (s *Service) patchLabels(ctx context.Context, m *controlv1.PatchLabelsRequest) (*controlv1.PatchLabelsResponse, error) {
	fn, err := s.cfg.Store.PatchLabels(ctx, m.GetFunctionId(), m.GetSet(), m.GetRemove())
	return &controlv1.PatchLabelsResponse{Function: fn}, err
}

func (s *Service) createRoute(ctx context.Context, m *controlv1.CreateRouteRequest) (*controlv1.CreateRouteResponse, error) {
	r, err := s.cfg.Store.CreateRoute(ctx, m.GetHostPattern(), m.GetPathPrefix(), m.GetFunctionId(), m.GetLabels())
	return &controlv1.CreateRouteResponse{Route: r}, err
}

func (s *Service) deleteRoute(ctx context.Context, m *controlv1.DeleteRouteRequest) (*controlv1.DeleteRouteResponse, error) {
	return &controlv1.DeleteRouteResponse{}, s.cfg.Store.DeleteRoute(ctx, m.GetRouteId())
}

func (s *Service) setRouteLabels(ctx context.Context, m *controlv1.SetRouteLabelsRequest) (*controlv1.SetRouteLabelsResponse, error) {
	r, err := s.cfg.Store.SetRouteLabels(ctx, m.GetRouteId(), m.GetLabels())
	return &controlv1.SetRouteLabelsResponse{Route: r}, err
}

func (s *Service) patchRouteLabels(ctx context.Context, m *controlv1.PatchRouteLabelsRequest) (*controlv1.PatchRouteLabelsResponse, error) {
	r, err := s.cfg.Store.PatchRouteLabels(ctx, m.GetRouteId(), m.GetSet(), m.GetRemove())
	return &controlv1.PatchRouteLabelsResponse{Route: r}, err
}

// change runs one of the operations as an RPC: with validateOnly, in a dry run;
// otherwise for real, and afterwards, if refresh, the cache the invoke path
// reads is brought up to date, so that the caller sees its own change.
func change[Req, Resp any](ctx context.Context, s *Service, m Req, validateOnly, refresh bool,
	op func(context.Context, Req) (Resp, error)) (Resp, error) {
	resp, err := mutate(ctx, s, validateOnly, func(ctx context.Context) (Resp, error) { return op(ctx, m) })
	if err != nil {
		var zero Resp
		return zero, s.fail(err)
	}
	if refresh && !validateOnly {
		s.cfg.Refresh(ctx)
	}
	return resp, nil
}

// --- FunctionService ---------------------------------------------------------

type functions struct{ s *Service }

func (f functions) CreateFunction(ctx context.Context, r *connect.Request[controlv1.CreateFunctionRequest]) (*connect.Response[controlv1.CreateFunctionResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), false, f.s.createFunction)
	if err != nil {
		return nil, err
	}
	if r.Msg.GetValidateOnly() {
		resp.Function.Id = ""
	}
	return connect.NewResponse(resp), nil
}

func (f functions) GetFunction(ctx context.Context, r *connect.Request[controlv1.GetFunctionRequest]) (*connect.Response[controlv1.GetFunctionResponse], error) {
	fn, err := f.s.cfg.Store.GetFunction(ctx, r.Msg.GetFunctionId())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.GetFunctionResponse{Function: fn}), nil
}

func (f functions) ListFunctions(ctx context.Context, r *connect.Request[controlv1.ListFunctionsRequest]) (*connect.Response[controlv1.ListFunctionsResponse], error) {
	m := r.Msg
	fns, next, err := f.s.cfg.Store.ListFunctions(ctx, m.GetPageSize(), m.GetPageToken(),
		store.LabelFilter{Match: m.GetMatchLabels(), Has: m.GetHasLabelKeys(), Lacks: m.GetLacksLabelKeys()}, m.GetName())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListFunctionsResponse{Functions: fns, NextPageToken: next}), nil
}

func (f functions) DeleteFunction(ctx context.Context, r *connect.Request[controlv1.DeleteFunctionRequest]) (*connect.Response[controlv1.DeleteFunctionResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), true, f.s.deleteFunction)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) CreateVersion(ctx context.Context, r *connect.Request[controlv1.CreateVersionRequest]) (*connect.Response[controlv1.CreateVersionResponse], error) {
	m := r.Msg
	resp, err := change(ctx, f.s, m, m.GetValidateOnly(), m.GetActivate(), f.s.createVersion)
	if err != nil {
		return nil, err
	}
	if m.GetValidateOnly() {
		resp.Version.Id = ""
	}
	return connect.NewResponse(resp), nil
}

func (f functions) ListVersions(ctx context.Context, r *connect.Request[controlv1.ListVersionsRequest]) (*connect.Response[controlv1.ListVersionsResponse], error) {
	m := r.Msg
	vs, next, err := f.s.cfg.Store.ListVersions(ctx, m.GetFunctionId(), m.GetPageSize(), m.GetPageToken(),
		store.LabelFilter{Match: m.GetMatchLabels(), Has: m.GetHasLabelKeys(), Lacks: m.GetLacksLabelKeys()})
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListVersionsResponse{Versions: vs, NextPageToken: next}), nil
}

func (f functions) ActivateVersion(ctx context.Context, r *connect.Request[controlv1.ActivateVersionRequest]) (*connect.Response[controlv1.ActivateVersionResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), true, f.s.activateVersion)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) SetEnv(ctx context.Context, r *connect.Request[controlv1.SetEnvRequest]) (*connect.Response[controlv1.SetEnvResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), true, f.s.setEnv)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) DeleteEnv(ctx context.Context, r *connect.Request[controlv1.DeleteEnvRequest]) (*connect.Response[controlv1.DeleteEnvResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), true, f.s.deleteEnv)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) SetEgress(ctx context.Context, r *connect.Request[controlv1.SetEgressRequest]) (*connect.Response[controlv1.SetEgressResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), true, f.s.setEgress)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) SetLabels(ctx context.Context, r *connect.Request[controlv1.SetLabelsRequest]) (*connect.Response[controlv1.SetLabelsResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), false, f.s.setLabels)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) PatchLabels(ctx context.Context, r *connect.Request[controlv1.PatchLabelsRequest]) (*connect.Response[controlv1.PatchLabelsResponse], error) {
	resp, err := change(ctx, f.s, r.Msg, r.Msg.GetValidateOnly(), false, f.s.patchLabels)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (f functions) ListEnv(ctx context.Context, r *connect.Request[controlv1.ListEnvRequest]) (*connect.Response[controlv1.ListEnvResponse], error) {
	es, err := f.s.cfg.Store.ListEnv(ctx, r.Msg.GetFunctionId())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListEnvResponse{Entries: es}), nil
}

// --- RouteService --------------------------------------------------------------

type routes struct{ s *Service }

func (r routes) CreateRoute(ctx context.Context, req *connect.Request[controlv1.CreateRouteRequest]) (*connect.Response[controlv1.CreateRouteResponse], error) {
	resp, err := change(ctx, r.s, req.Msg, req.Msg.GetValidateOnly(), true, r.s.createRoute)
	if err != nil {
		return nil, err
	}
	if req.Msg.GetValidateOnly() {
		resp.Route.Id = ""
	}
	return connect.NewResponse(resp), nil
}

func (r routes) ListRoutes(ctx context.Context, req *connect.Request[controlv1.ListRoutesRequest]) (*connect.Response[controlv1.ListRoutesResponse], error) {
	m := req.Msg
	rs, next, err := r.s.cfg.Store.ListRoutes(ctx, m.GetPageSize(), m.GetPageToken(),
		store.LabelFilter{Match: m.GetMatchLabels(), Has: m.GetHasLabelKeys(), Lacks: m.GetLacksLabelKeys()}, m.GetFunctionId())
	if err != nil {
		return nil, r.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListRoutesResponse{Routes: rs, NextPageToken: next}), nil
}

func (r routes) DeleteRoute(ctx context.Context, req *connect.Request[controlv1.DeleteRouteRequest]) (*connect.Response[controlv1.DeleteRouteResponse], error) {
	resp, err := change(ctx, r.s, req.Msg, req.Msg.GetValidateOnly(), true, r.s.deleteRoute)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (r routes) SetRouteLabels(ctx context.Context, req *connect.Request[controlv1.SetRouteLabelsRequest]) (*connect.Response[controlv1.SetRouteLabelsResponse], error) {
	resp, err := change(ctx, r.s, req.Msg, req.Msg.GetValidateOnly(), false, r.s.setRouteLabels)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (r routes) PatchRouteLabels(ctx context.Context, req *connect.Request[controlv1.PatchRouteLabelsRequest]) (*connect.Response[controlv1.PatchRouteLabelsResponse], error) {
	resp, err := change(ctx, r.s, req.Msg, req.Msg.GetValidateOnly(), false, r.s.patchRouteLabels)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// --- InspectionService ---------------------------------------------------------

type inspection struct{ s *Service }

func (i inspection) Describe(context.Context, *connect.Request[controlv1.DescribeRequest]) (*connect.Response[controlv1.DescribeResponse], error) {
	return connect.NewResponse(&controlv1.DescribeResponse{
		Implementation: &commonv1.Implementation{
			Name: "edgerun-orchestrator", Version: i.s.cfg.Version, Protocols: []string{"edgerun.control.v1"},
		},
		MaxModuleBytes: i.s.cfg.MaxModuleBytes,
	}), nil
}

func (i inspection) ListWorkers(ctx context.Context, _ *connect.Request[controlv1.ListWorkersRequest]) (*connect.Response[controlv1.ListWorkersResponse], error) {
	ws, err := i.s.cfg.Store.ListWorkers(ctx, i.s.cfg.AliveWithin)
	if err != nil {
		return nil, i.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListWorkersResponse{Workers: ws}), nil
}

func (i inspection) ListNetworkServices(ctx context.Context, _ *connect.Request[controlv1.ListNetworkServicesRequest]) (*connect.Response[controlv1.ListNetworkServicesResponse], error) {
	ns, err := i.s.cfg.Store.ListNetworkServices(ctx, i.s.cfg.AliveWithin)
	if err != nil {
		return nil, i.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListNetworkServicesResponse{Services: ns}), nil
}

func (i inspection) ListInvocations(ctx context.Context, r *connect.Request[controlv1.ListInvocationsRequest]) (*connect.Response[controlv1.ListInvocationsResponse], error) {
	vs, next, err := i.s.cfg.Store.ListInvocations(ctx, r.Msg.GetFunctionId(), r.Msg.GetRequestId(), r.Msg.GetPageSize(), r.Msg.GetPageToken())
	if err != nil {
		return nil, i.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListInvocationsResponse{Invocations: vs, NextPageToken: next}), nil
}

func (i inspection) GetInvocation(ctx context.Context, r *connect.Request[controlv1.GetInvocationRequest]) (*connect.Response[controlv1.GetInvocationResponse], error) {
	v, err := i.s.cfg.Store.GetInvocation(ctx, r.Msg.GetInvocationId())
	if err != nil {
		return nil, i.s.fail(err)
	}
	return connect.NewResponse(&controlv1.GetInvocationResponse{Invocation: v}), nil
}
