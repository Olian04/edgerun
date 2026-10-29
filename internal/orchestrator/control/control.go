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

// --- FunctionService ---------------------------------------------------------

type functions struct{ s *Service }

func (f functions) CreateFunction(ctx context.Context, r *connect.Request[controlv1.CreateFunctionRequest]) (*connect.Response[controlv1.CreateFunctionResponse], error) {
	m := r.Msg
	fn, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.Function, error) {
		return f.s.cfg.Store.CreateFunction(ctx, m.GetName(), m.GetLabels())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if m.GetValidateOnly() {
		fn.Id = ""
	}
	return connect.NewResponse(&controlv1.CreateFunctionResponse{Function: fn}), nil
}

func (f functions) GetFunction(ctx context.Context, r *connect.Request[controlv1.GetFunctionRequest]) (*connect.Response[controlv1.GetFunctionResponse], error) {
	fn, err := f.s.cfg.Store.GetFunction(ctx, r.Msg.GetFunctionId())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.GetFunctionResponse{Function: fn}), nil
}

func (f functions) ListFunctions(ctx context.Context, r *connect.Request[controlv1.ListFunctionsRequest]) (*connect.Response[controlv1.ListFunctionsResponse], error) {
	fns, next, err := f.s.cfg.Store.ListFunctions(ctx, r.Msg.GetPageSize(), r.Msg.GetPageToken(), r.Msg.GetMatchLabels())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListFunctionsResponse{Functions: fns, NextPageToken: next}), nil
}

func (f functions) DeleteFunction(ctx context.Context, r *connect.Request[controlv1.DeleteFunctionRequest]) (*connect.Response[controlv1.DeleteFunctionResponse], error) {
	m := r.Msg
	_, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (struct{}, error) {
		return struct{}{}, f.s.cfg.Store.DeleteFunction(ctx, m.GetFunctionId())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if !m.GetValidateOnly() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.DeleteFunctionResponse{}), nil
}

func (f functions) CreateVersion(ctx context.Context, r *connect.Request[controlv1.CreateVersionRequest]) (*connect.Response[controlv1.CreateVersionResponse], error) {
	m := r.Msg
	if uint64(len(m.GetModule())) > f.s.cfg.MaxModuleBytes {
		return nil, errs.Invalid("module is %d bytes; the limit is %d", len(m.GetModule()), f.s.cfg.MaxModuleBytes)
	}
	if !abiPattern.MatchString(m.GetAbi()) {
		return nil, errs.Invalid("abi %q: want a short lower-case name such as \"wagi-1\"", m.GetAbi())
	}
	v, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.FunctionVersion, error) {
		return f.s.cfg.Store.CreateVersion(ctx, store.NewVersion{
			FunctionID: m.GetFunctionId(), Module: m.GetModule(), ABI: m.GetAbi(), Limits: m.GetLimits(), Activate: m.GetActivate(),
			Labels: m.GetLabels(),
		})
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if m.GetValidateOnly() {
		v.Id = ""
	} else if m.GetActivate() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.CreateVersionResponse{Version: v}), nil
}

func (f functions) ListVersions(ctx context.Context, r *connect.Request[controlv1.ListVersionsRequest]) (*connect.Response[controlv1.ListVersionsResponse], error) {
	vs, next, err := f.s.cfg.Store.ListVersions(ctx, r.Msg.GetFunctionId(), r.Msg.GetPageSize(), r.Msg.GetPageToken(), r.Msg.GetMatchLabels())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListVersionsResponse{Versions: vs, NextPageToken: next}), nil
}

func (f functions) ActivateVersion(ctx context.Context, r *connect.Request[controlv1.ActivateVersionRequest]) (*connect.Response[controlv1.ActivateVersionResponse], error) {
	m := r.Msg
	fn, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.Function, error) {
		return f.s.cfg.Store.ActivateVersion(ctx, m.GetFunctionId(), m.GetVersionId())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if !m.GetValidateOnly() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.ActivateVersionResponse{Function: fn}), nil
}

func (f functions) SetEnv(ctx context.Context, r *connect.Request[controlv1.SetEnvRequest]) (*connect.Response[controlv1.SetEnvResponse], error) {
	m := r.Msg
	if m.GetEntry() == nil {
		return nil, errs.Invalid("entry is required")
	}
	e, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.EnvEntry, error) {
		return f.s.cfg.Store.SetEnv(ctx, m.GetFunctionId(), m.GetEntry())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if !m.GetValidateOnly() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.SetEnvResponse{Entry: e}), nil
}

func (f functions) DeleteEnv(ctx context.Context, r *connect.Request[controlv1.DeleteEnvRequest]) (*connect.Response[controlv1.DeleteEnvResponse], error) {
	m := r.Msg
	_, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (struct{}, error) {
		return struct{}{}, f.s.cfg.Store.DeleteEnv(ctx, m.GetFunctionId(), m.GetName())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if !m.GetValidateOnly() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.DeleteEnvResponse{}), nil
}

func (f functions) SetEgress(ctx context.Context, r *connect.Request[controlv1.SetEgressRequest]) (*connect.Response[controlv1.SetEgressResponse], error) {
	m := r.Msg
	fn, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.Function, error) {
		return f.s.cfg.Store.SetEgress(ctx, m.GetFunctionId(), m.GetAllow())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if !m.GetValidateOnly() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.SetEgressResponse{Function: fn}), nil
}

func (f functions) SetLabels(ctx context.Context, r *connect.Request[controlv1.SetLabelsRequest]) (*connect.Response[controlv1.SetLabelsResponse], error) {
	m := r.Msg
	fn, err := mutate(ctx, f.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.Function, error) {
		return f.s.cfg.Store.SetLabels(ctx, m.GetFunctionId(), m.GetLabels())
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.SetLabelsResponse{Function: fn}), nil
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
	m := req.Msg
	route, err := mutate(ctx, r.s, m.GetValidateOnly(), func(ctx context.Context) (*controlv1.Route, error) {
		return r.s.cfg.Store.CreateRoute(ctx, m.GetHostPattern(), m.GetPathPrefix(), m.GetFunctionId(), m.GetLabels())
	})
	if err != nil {
		return nil, r.s.fail(err)
	}
	if m.GetValidateOnly() {
		route.Id = ""
	} else {
		r.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.CreateRouteResponse{Route: route}), nil
}

func (r routes) ListRoutes(ctx context.Context, req *connect.Request[controlv1.ListRoutesRequest]) (*connect.Response[controlv1.ListRoutesResponse], error) {
	rs, next, err := r.s.cfg.Store.ListRoutes(ctx, req.Msg.GetPageSize(), req.Msg.GetPageToken(), req.Msg.GetMatchLabels())
	if err != nil {
		return nil, r.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListRoutesResponse{Routes: rs, NextPageToken: next}), nil
}

func (r routes) DeleteRoute(ctx context.Context, req *connect.Request[controlv1.DeleteRouteRequest]) (*connect.Response[controlv1.DeleteRouteResponse], error) {
	m := req.Msg
	_, err := mutate(ctx, r.s, m.GetValidateOnly(), func(ctx context.Context) (struct{}, error) {
		return struct{}{}, r.s.cfg.Store.DeleteRoute(ctx, m.GetRouteId())
	})
	if err != nil {
		return nil, r.s.fail(err)
	}
	if !m.GetValidateOnly() {
		r.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.DeleteRouteResponse{}), nil
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
	vs, next, err := i.s.cfg.Store.ListInvocations(ctx, r.Msg.GetFunctionId(), r.Msg.GetPageSize(), r.Msg.GetPageToken())
	if err != nil {
		return nil, i.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListInvocationsResponse{Invocations: vs, NextPageToken: next}), nil
}
