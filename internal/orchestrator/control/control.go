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

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/apierr"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// AdminScope is the token scope operators present.
const AdminScope = "admin"

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
	small := connect.WithInterceptors(authn.Server(v, AdminScope))
	upload := []connect.HandlerOption{small, connect.WithReadMaxBytes(int(s.cfg.MaxModuleBytes) + (1 << 20))}
	mux.Handle(controlv1connect.NewFunctionServiceHandler(functions{s}, upload...))
	mux.Handle(controlv1connect.NewRouteServiceHandler(routes{s}, small))
	mux.Handle(controlv1connect.NewInspectionServiceHandler(inspection{s}, small))
}

func (s *Service) fail(err error) error { return apierr.FromStore(s.cfg.Log, err) }

// --- FunctionService ---------------------------------------------------------

type functions struct{ s *Service }

func (f functions) CreateFunction(ctx context.Context, r *connect.Request[controlv1.CreateFunctionRequest]) (*connect.Response[controlv1.CreateFunctionResponse], error) {
	fn, err := f.s.cfg.Store.CreateFunction(ctx, r.Msg.GetName())
	if err != nil {
		return nil, f.s.fail(err)
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
	fns, next, err := f.s.cfg.Store.ListFunctions(ctx, r.Msg.GetPageSize(), r.Msg.GetPageToken())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListFunctionsResponse{Functions: fns, NextPageToken: next}), nil
}

func (f functions) DeleteFunction(ctx context.Context, r *connect.Request[controlv1.DeleteFunctionRequest]) (*connect.Response[controlv1.DeleteFunctionResponse], error) {
	if err := f.s.cfg.Store.DeleteFunction(ctx, r.Msg.GetFunctionId()); err != nil {
		return nil, f.s.fail(err)
	}
	f.s.cfg.Refresh(ctx)
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
	v, err := f.s.cfg.Store.CreateVersion(ctx, store.NewVersion{
		FunctionID: m.GetFunctionId(), Module: m.GetModule(), ABI: m.GetAbi(), Limits: m.GetLimits(), Activate: m.GetActivate(),
	})
	if err != nil {
		return nil, f.s.fail(err)
	}
	if m.GetActivate() {
		f.s.cfg.Refresh(ctx)
	}
	return connect.NewResponse(&controlv1.CreateVersionResponse{Version: v}), nil
}

func (f functions) ListVersions(ctx context.Context, r *connect.Request[controlv1.ListVersionsRequest]) (*connect.Response[controlv1.ListVersionsResponse], error) {
	vs, next, err := f.s.cfg.Store.ListVersions(ctx, r.Msg.GetFunctionId(), r.Msg.GetPageSize(), r.Msg.GetPageToken())
	if err != nil {
		return nil, f.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListVersionsResponse{Versions: vs, NextPageToken: next}), nil
}

func (f functions) ActivateVersion(ctx context.Context, r *connect.Request[controlv1.ActivateVersionRequest]) (*connect.Response[controlv1.ActivateVersionResponse], error) {
	fn, err := f.s.cfg.Store.ActivateVersion(ctx, r.Msg.GetFunctionId(), r.Msg.GetVersionId())
	if err != nil {
		return nil, f.s.fail(err)
	}
	f.s.cfg.Refresh(ctx)
	return connect.NewResponse(&controlv1.ActivateVersionResponse{Function: fn}), nil
}

func (f functions) SetEnv(ctx context.Context, r *connect.Request[controlv1.SetEnvRequest]) (*connect.Response[controlv1.SetEnvResponse], error) {
	if r.Msg.GetEntry() == nil {
		return nil, errs.Invalid("entry is required")
	}
	if err := f.s.cfg.Store.SetEnv(ctx, r.Msg.GetFunctionId(), r.Msg.GetEntry()); err != nil {
		return nil, f.s.fail(err)
	}
	f.s.cfg.Refresh(ctx)
	return connect.NewResponse(&controlv1.SetEnvResponse{}), nil
}

func (f functions) DeleteEnv(ctx context.Context, r *connect.Request[controlv1.DeleteEnvRequest]) (*connect.Response[controlv1.DeleteEnvResponse], error) {
	if err := f.s.cfg.Store.DeleteEnv(ctx, r.Msg.GetFunctionId(), r.Msg.GetName()); err != nil {
		return nil, f.s.fail(err)
	}
	f.s.cfg.Refresh(ctx)
	return connect.NewResponse(&controlv1.DeleteEnvResponse{}), nil
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
	route, err := r.s.cfg.Store.CreateRoute(ctx, req.Msg.GetHostPattern(), req.Msg.GetPathPrefix(), req.Msg.GetFunctionId())
	if err != nil {
		return nil, r.s.fail(err)
	}
	r.s.cfg.Refresh(ctx)
	return connect.NewResponse(&controlv1.CreateRouteResponse{Route: route}), nil
}

func (r routes) ListRoutes(ctx context.Context, req *connect.Request[controlv1.ListRoutesRequest]) (*connect.Response[controlv1.ListRoutesResponse], error) {
	rs, next, err := r.s.cfg.Store.ListRoutes(ctx, req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, r.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListRoutesResponse{Routes: rs, NextPageToken: next}), nil
}

func (r routes) DeleteRoute(ctx context.Context, req *connect.Request[controlv1.DeleteRouteRequest]) (*connect.Response[controlv1.DeleteRouteResponse], error) {
	if err := r.s.cfg.Store.DeleteRoute(ctx, req.Msg.GetRouteId()); err != nil {
		return nil, r.s.fail(err)
	}
	r.s.cfg.Refresh(ctx)
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

func (i inspection) ListInvocations(ctx context.Context, r *connect.Request[controlv1.ListInvocationsRequest]) (*connect.Response[controlv1.ListInvocationsResponse], error) {
	vs, next, err := i.s.cfg.Store.ListInvocations(ctx, r.Msg.GetFunctionId(), r.Msg.GetPageSize(), r.Msg.GetPageToken())
	if err != nil {
		return nil, i.s.fail(err)
	}
	return connect.NewResponse(&controlv1.ListInvocationsResponse{Invocations: vs, NextPageToken: next}), nil
}
