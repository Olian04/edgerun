// Package membership implements the orchestrator's side of membership.v1: the
// workers and network services that dial in, announce themselves, stay alive
// and leave, and the operator's view of who is in. A member is never configured
// into the orchestrator; any instance will take its registration.
package membership

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	"github.com/Olian04/edgerun/gen/edgerun/membership/v1/membershipv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/apierr"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/telemetry"
)

// The token scopes of the three kinds of caller: a worker, a network service,
// and an operator (who lists the members).
const (
	WorkerScope  = edgerun.ScopeWorker
	NetworkScope = edgerun.ScopeNetwork
	AdminScope   = edgerun.ScopeAdmin
)

// Store is what the service needs. *store.Store implements it.
type Store interface {
	RegisterWorker(ctx context.Context, endpoint string, info *workerv1.WorkerInfo) (string, error)
	Heartbeat(ctx context.Context, id string, state membershipv1.MemberState, inFlight uint32) (bool, error)
	DeregisterWorker(ctx context.Context, id string) error
	ListWorkers(ctx context.Context, aliveWithin time.Duration) ([]*membershipv1.Member, error)

	RegisterNetworkService(ctx context.Context, endpoint string, info *egressv1.ServiceInfo) (string, error)
	HeartbeatNetworkService(ctx context.Context, id string, state membershipv1.MemberState) (bool, error)
	DeregisterNetworkService(ctx context.Context, id string) error
	ListNetworkServices(ctx context.Context, aliveWithin time.Duration) ([]*membershipv1.Member, error)
}

// Config configures the service.
type Config struct {
	Store Store
	// HeartbeatInterval is dictated to members on registration. Default 5s.
	HeartbeatInterval time.Duration
	// AliveWithin is how recently a member must have heartbeated to be alive.
	// Default 3 heartbeat intervals.
	AliveWithin time.Duration
	// OnChange is called after a member registers or leaves, so the invoke path
	// can refresh its view at once. May be nil.
	OnChange func()
	Log      *slog.Logger
}

// Service implements MembershipService.
type Service struct {
	membershipv1connect.UnimplementedMembershipServiceHandler
	cfg Config
}

// New returns a service.
func New(cfg Config) *Service {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.AliveWithin == 0 {
		cfg.AliveWithin = 3 * cfg.HeartbeatInterval
	}
	if cfg.OnChange == nil {
		cfg.OnChange = func() {}
	}
	return &Service{cfg: cfg}
}

// Mount registers the service on mux. A worker's token, a network service's
// token and an operator's all get in; each call then takes what that scope may
// do: a worker registers, heartbeats and deregisters workers, a network
// service the same for network services, and only an operator lists.
func (s *Service) Mount(mux *http.ServeMux, v authn.Verifier) {
	mux.Handle(membershipv1connect.NewMembershipServiceHandler(s, connect.WithInterceptors(telemetry.ServerInterceptor(),
		authn.Server(v, WorkerScope, NetworkScope, AdminScope)),
		connect.WithReadMaxBytes(authn.SmallMessageBytes), connect.WithCompressMinBytes(authn.CompressMinBytes)))
}

func denied(what string) error {
	return connect.NewError(connect.CodePermissionDenied, errors.New("token scope not allowed here: "+what))
}

func (s *Service) Register(ctx context.Context, req *connect.Request[membershipv1.RegisterRequest]) (*connect.Response[membershipv1.RegisterResponse], error) {
	scope := authn.ScopeFrom(ctx)
	var id string
	var err error
	switch info := req.Msg.GetInfo().(type) {
	case *membershipv1.RegisterRequest_Worker:
		if scope != WorkerScope {
			return nil, denied("a worker registers with a worker token")
		}
		id, err = s.cfg.Store.RegisterWorker(ctx, req.Msg.GetEndpoint(), info.Worker)
		if err == nil {
			s.cfg.Log.Info("worker registered", "member_id", id, "endpoint", req.Msg.GetEndpoint(),
				"abis", info.Worker.GetAbis(), "implementation", info.Worker.GetImplementation().GetName())
		}
	case *membershipv1.RegisterRequest_NetworkService:
		if scope != NetworkScope {
			return nil, denied("a network service registers with a network token")
		}
		id, err = s.cfg.Store.RegisterNetworkService(ctx, req.Msg.GetEndpoint(), info.NetworkService)
		if err == nil {
			s.cfg.Log.Info("network service registered", "member_id", id, "endpoint", req.Msg.GetEndpoint(), "covers", info.NetworkService.GetCovers())
		}
	default:
		return nil, errs.Invalid("a registration says what the member is: worker or network_service")
	}
	if err != nil {
		return nil, apierr.FromStore(s.cfg.Log, err)
	}
	s.cfg.OnChange()
	return connect.NewResponse(&membershipv1.RegisterResponse{MemberId: id, HeartbeatInterval: durationpb.New(s.cfg.HeartbeatInterval)}), nil
}

func (s *Service) Heartbeat(ctx context.Context, req *connect.Request[membershipv1.HeartbeatRequest]) (*connect.Response[membershipv1.HeartbeatResponse], error) {
	scope := authn.ScopeFrom(ctx)
	if scope != WorkerScope && scope != NetworkScope {
		return nil, denied("only a member heartbeats")
	}
	switch req.Msg.GetState() {
	case membershipv1.MemberState_MEMBER_STATE_READY, membershipv1.MemberState_MEMBER_STATE_DRAINING:
	default:
		return nil, errs.Invalid("heartbeat state must be READY or DRAINING")
	}
	var known bool
	var err error
	if scope == WorkerScope {
		known, err = s.cfg.Store.Heartbeat(ctx, req.Msg.GetMemberId(), req.Msg.GetState(), req.Msg.GetInFlight())
	} else {
		known, err = s.cfg.Store.HeartbeatNetworkService(ctx, req.Msg.GetMemberId(), req.Msg.GetState())
	}
	if err != nil {
		return nil, apierr.FromStore(s.cfg.Log, err)
	}
	return connect.NewResponse(&membershipv1.HeartbeatResponse{Reregister: !known}), nil
}

func (s *Service) Deregister(ctx context.Context, req *connect.Request[membershipv1.DeregisterRequest]) (*connect.Response[membershipv1.DeregisterResponse], error) {
	scope := authn.ScopeFrom(ctx)
	var err error
	switch scope {
	case WorkerScope:
		err = s.cfg.Store.DeregisterWorker(ctx, req.Msg.GetMemberId())
	case NetworkScope:
		err = s.cfg.Store.DeregisterNetworkService(ctx, req.Msg.GetMemberId())
	default:
		return nil, denied("only a member deregisters")
	}
	if err != nil {
		return nil, apierr.FromStore(s.cfg.Log, err)
	}
	s.cfg.Log.Info("member deregistered", "member_id", req.Msg.GetMemberId(), "scope", scope)
	s.cfg.OnChange()
	return connect.NewResponse(&membershipv1.DeregisterResponse{}), nil
}

func (s *Service) ListMembers(ctx context.Context, req *connect.Request[membershipv1.ListMembersRequest]) (*connect.Response[membershipv1.ListMembersResponse], error) {
	if authn.ScopeFrom(ctx) != AdminScope {
		return nil, denied("only an operator lists the members")
	}
	var out []*membershipv1.Member
	kind := req.Msg.GetKind()
	if kind == membershipv1.MemberKind_MEMBER_KIND_UNSPECIFIED || kind == membershipv1.MemberKind_MEMBER_KIND_WORKER {
		ws, err := s.cfg.Store.ListWorkers(ctx, s.cfg.AliveWithin)
		if err != nil {
			return nil, apierr.FromStore(s.cfg.Log, err)
		}
		out = append(out, ws...)
	}
	if kind == membershipv1.MemberKind_MEMBER_KIND_UNSPECIFIED || kind == membershipv1.MemberKind_MEMBER_KIND_NETWORK_SERVICE {
		ns, err := s.cfg.Store.ListNetworkServices(ctx, s.cfg.AliveWithin)
		if err != nil {
			return nil, apierr.FromStore(s.cfg.Log, err)
		}
		out = append(out, ns...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Endpoint < out[j].Endpoint
	})
	return connect.NewResponse(&membershipv1.ListMembersResponse{Members: out}), nil
}

var _ Store = (*store.Store)(nil)
