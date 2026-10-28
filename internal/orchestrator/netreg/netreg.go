// Package netreg implements the orchestrator's side of network service
// registration (egress.v1 RegistryService). Services dial in and announce what
// they carry; the orchestrator needs no inventory of them.
package netreg

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/apierr"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// Scope is the token scope network services present to the orchestrator.
const Scope = edgerun.ScopeNetwork

// Store is what the service needs. *store.Store implements it.
type Store interface {
	RegisterNetworkService(ctx context.Context, endpoint string, info *egressv1.ServiceInfo) (string, error)
	HeartbeatNetworkService(ctx context.Context, id string, state egressv1.ServiceState) (bool, error)
	DeregisterNetworkService(ctx context.Context, id string) error
}

// Config configures the service.
type Config struct {
	Store Store
	// HeartbeatInterval is dictated to services on registration. Default 5s.
	HeartbeatInterval time.Duration
	// OnChange is called after a service registers or leaves, so the invoke
	// path can refresh its view at once. May be nil.
	OnChange func()
	Log      *slog.Logger
}

// Service implements RegistryService.
type Service struct {
	egressv1connect.UnimplementedRegistryServiceHandler
	cfg Config
}

// New returns a service.
func New(cfg Config) *Service {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.OnChange == nil {
		cfg.OnChange = func() {}
	}
	return &Service{cfg: cfg}
}

// Mount registers the service on mux, guarded by the network scope.
func (s *Service) Mount(mux *http.ServeMux, v authn.Verifier) {
	mux.Handle(egressv1connect.NewRegistryServiceHandler(s, connect.WithInterceptors(authn.Server(v, Scope)), connect.WithCompressMinBytes(authn.CompressMinBytes)))
}

func (s *Service) Register(ctx context.Context, req *connect.Request[egressv1.RegisterRequest]) (*connect.Response[egressv1.RegisterResponse], error) {
	id, err := s.cfg.Store.RegisterNetworkService(ctx, req.Msg.GetEndpoint(), req.Msg.GetInfo())
	if err != nil {
		return nil, apierr.FromStore(s.cfg.Log, err)
	}
	s.cfg.Log.Info("network service registered", "service_id", id, "endpoint", req.Msg.GetEndpoint(), "covers", req.Msg.GetInfo().GetCovers())
	s.cfg.OnChange()
	return connect.NewResponse(&egressv1.RegisterResponse{ServiceId: id, HeartbeatInterval: durationpb.New(s.cfg.HeartbeatInterval)}), nil
}

func (s *Service) Heartbeat(ctx context.Context, req *connect.Request[egressv1.HeartbeatRequest]) (*connect.Response[egressv1.HeartbeatResponse], error) {
	switch req.Msg.GetState() {
	case egressv1.ServiceState_SERVICE_STATE_READY, egressv1.ServiceState_SERVICE_STATE_DRAINING:
	default:
		return nil, errs.Invalid("heartbeat state must be READY or DRAINING")
	}
	known, err := s.cfg.Store.HeartbeatNetworkService(ctx, req.Msg.GetServiceId(), req.Msg.GetState())
	if err != nil {
		return nil, apierr.FromStore(s.cfg.Log, err)
	}
	return connect.NewResponse(&egressv1.HeartbeatResponse{Reregister: !known}), nil
}

func (s *Service) Deregister(ctx context.Context, req *connect.Request[egressv1.DeregisterRequest]) (*connect.Response[egressv1.DeregisterResponse], error) {
	if err := s.cfg.Store.DeregisterNetworkService(ctx, req.Msg.GetServiceId()); err != nil {
		return nil, apierr.FromStore(s.cfg.Log, err)
	}
	s.cfg.Log.Info("network service deregistered", "service_id", req.Msg.GetServiceId())
	s.cfg.OnChange()
	return connect.NewResponse(&egressv1.DeregisterResponse{}), nil
}

var _ Store = (*store.Store)(nil)
