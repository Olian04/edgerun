// Package artifacts implements the orchestrator's host of the Artifacts
// service (worker.v1 ArtifactService): module bytes by content digest, for the
// workers that run them.
package artifacts

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/apierr"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/telemetry"
)

// WorkerScope is the token scope workers present to the orchestrator.
const WorkerScope = edgerun.ScopeWorker

// artifactChunk is the size of one GetArtifact message (docs/protocol.md: <= 1 MiB).
const artifactChunk = 1 << 20

// Store is what the service needs. *store.Store implements it.
type Store interface {
	GetArtifact(ctx context.Context, digest string) ([]byte, error)
}

// Config configures the service.
type Config struct {
	Store Store
	Log   *slog.Logger
}

// Service implements ArtifactService.
type Service struct {
	workerv1connect.UnimplementedArtifactServiceHandler
	cfg Config
}

// New returns a service.
func New(cfg Config) *Service { return &Service{cfg: cfg} }

// Mount registers the service on mux, guarded by the worker scope.
func (s *Service) Mount(mux *http.ServeMux, v authn.Verifier) {
	mux.Handle(workerv1connect.NewArtifactServiceHandler(artifactHandler{s}, connect.WithInterceptors(telemetry.ServerInterceptor(), authn.Server(v, WorkerScope)),
		connect.WithReadMaxBytes(authn.SmallMessageBytes), connect.WithCompressMinBytes(authn.CompressMinBytes)))
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
