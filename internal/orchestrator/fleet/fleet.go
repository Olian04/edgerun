// Package fleet implements the orchestrator's worker-facing services of
// worker.v1: FleetService (register, heartbeat, deregister) and
// ArtifactService (module bytes by digest). Workers dial in; the orchestrator
// never needs to be configured with a list of them.
package fleet

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/apierr"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// WorkerScope is the token scope workers present to the orchestrator.
const WorkerScope = edgerun.ScopeWorker

// artifactChunk is the size of one GetArtifact message (docs/protocol.md: ≤ 1 MiB).
const artifactChunk = 1 << 20

// Store is what the services need. *store.Store implements it.
type Store interface {
	RegisterWorker(ctx context.Context, endpoint string, info *workerv1.WorkerInfo) (string, error)
	Heartbeat(ctx context.Context, id string, state workerv1.WorkerState, inFlight uint32) (bool, error)
	DeregisterWorker(ctx context.Context, id string) error
	GetArtifact(ctx context.Context, digest string) ([]byte, error)
}

// Config configures the services.
type Config struct {
	Store Store
	// HeartbeatInterval is dictated to workers on registration. Default 5s.
	HeartbeatInterval time.Duration
	// OnChange is called after a worker registers or leaves, so the invoke path
	// can refresh its view at once. May be nil.
	OnChange func()
	Log      *slog.Logger
}

// Service implements FleetService and ArtifactService.
type Service struct {
	workerv1connect.UnimplementedFleetServiceHandler
	workerv1connect.UnimplementedArtifactServiceHandler
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

// Mount registers both services on mux, guarded by the worker scope.
func (s *Service) Mount(mux *http.ServeMux, v authn.Verifier) {
	opts := connect.WithHandlerOptions(connect.WithInterceptors(authn.Server(v, WorkerScope)), connect.WithCompressMinBytes(authn.CompressMinBytes))
	mux.Handle(workerv1connect.NewFleetServiceHandler(fleetHandler{s}, opts))
	mux.Handle(workerv1connect.NewArtifactServiceHandler(artifactHandler{s}, opts))
}

type fleetHandler struct{ s *Service }

func (h fleetHandler) Register(ctx context.Context, req *connect.Request[workerv1.RegisterRequest]) (*connect.Response[workerv1.RegisterResponse], error) {
	id, err := h.s.cfg.Store.RegisterWorker(ctx, req.Msg.GetEndpoint(), req.Msg.GetInfo())
	if err != nil {
		return nil, apierr.FromStore(h.s.cfg.Log, err)
	}
	h.s.cfg.Log.Info("worker registered", "worker_id", id, "endpoint", req.Msg.GetEndpoint(),
		"abis", req.Msg.GetInfo().GetAbis(), "implementation", req.Msg.GetInfo().GetImplementation().GetName())
	h.s.cfg.OnChange()
	return connect.NewResponse(&workerv1.RegisterResponse{
		WorkerId: id, HeartbeatInterval: durationpb.New(h.s.cfg.HeartbeatInterval),
	}), nil
}

func (h fleetHandler) Heartbeat(ctx context.Context, req *connect.Request[workerv1.HeartbeatRequest]) (*connect.Response[workerv1.HeartbeatResponse], error) {
	switch req.Msg.GetState() {
	case workerv1.WorkerState_WORKER_STATE_READY, workerv1.WorkerState_WORKER_STATE_DRAINING:
	default:
		return nil, errs.Invalid("heartbeat state must be READY or DRAINING")
	}
	known, err := h.s.cfg.Store.Heartbeat(ctx, req.Msg.GetWorkerId(), req.Msg.GetState(), req.Msg.GetInFlight())
	if err != nil {
		return nil, apierr.FromStore(h.s.cfg.Log, err)
	}
	return connect.NewResponse(&workerv1.HeartbeatResponse{Reregister: !known}), nil
}

func (h fleetHandler) Deregister(ctx context.Context, req *connect.Request[workerv1.DeregisterRequest]) (*connect.Response[workerv1.DeregisterResponse], error) {
	if err := h.s.cfg.Store.DeregisterWorker(ctx, req.Msg.GetWorkerId()); err != nil {
		return nil, apierr.FromStore(h.s.cfg.Log, err)
	}
	h.s.cfg.Log.Info("worker deregistered", "worker_id", req.Msg.GetWorkerId())
	h.s.cfg.OnChange()
	return connect.NewResponse(&workerv1.DeregisterResponse{}), nil
}

type artifactHandler struct{ s *Service }

func (h artifactHandler) GetArtifact(ctx context.Context, req *connect.Request[workerv1.GetArtifactRequest], st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
	data, err := h.s.cfg.Store.GetArtifact(ctx, req.Msg.GetDigest())
	if errors.Is(err, store.ErrNotFound) {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, "unknown artifact %s", req.Msg.GetDigest())
	}
	if err != nil {
		return apierr.FromStore(h.s.cfg.Log, err)
	}
	for len(data) > 0 {
		n := min(artifactChunk, len(data))
		if err := st.Send(&workerv1.GetArtifactResponse{Chunk: data[:n]}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

var _ Store = (*store.Store)(nil)
