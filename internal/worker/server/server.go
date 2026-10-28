// Package server implements worker.v1.ExecutionService.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/worker/cache"
	"github.com/Olian04/edgerun/internal/worker/runtime"
)

const (
	// ExecuteScope is the token scope the orchestrator presents to workers.
	ExecuteScope = edgerun.ScopeExecute

	chunkSize = 64 << 10
	// maxRequestBytes bounds one Execute message: the protocol's request body
	// bound plus headroom for metadata and environment.
	maxRequestBytes = 8 << 20
)

// Server executes invocations. It never queues: when it has no free slot it
// says so immediately and lets the orchestrator pick another worker.
type Server struct {
	workerv1connect.UnimplementedExecutionServiceHandler

	info      *workerv1.WorkerInfo
	engine    *runtime.Engine
	artifacts *cache.Store
	log       *slog.Logger
	// defaultTimeout bounds invocations whose RPC carries no deadline.
	defaultTimeout time.Duration

	mu       sync.Mutex
	inFlight int
	draining bool
	idle     chan struct{} // closed once draining and nothing is in flight
}

// New creates a server. info.MaxConcurrency must be positive.
func New(info *workerv1.WorkerInfo, engine *runtime.Engine, artifacts *cache.Store, log *slog.Logger) *Server {
	return &Server{
		info: info, engine: engine, artifacts: artifacts, log: log,
		defaultTimeout: 30 * time.Second, idle: make(chan struct{}),
	}
}

// Handler returns the path and handler to mount, guarded by the verifier.
func (s *Server) Handler(v authn.Verifier) (string, http.Handler) {
	return workerv1connect.NewExecutionServiceHandler(s,
		connect.WithInterceptors(authn.Server(v, ExecuteScope)),
		connect.WithReadMaxBytes(maxRequestBytes), connect.WithCompressMinBytes(authn.CompressMinBytes))
}

// Info is what this worker advertises.
func (s *Server) Info() *workerv1.WorkerInfo { return s.info }

// State reports READY or DRAINING for heartbeats.
func (s *Server) State() workerv1.WorkerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return workerv1.WorkerState_WORKER_STATE_DRAINING
	}
	return workerv1.WorkerState_WORKER_STATE_READY
}

// InFlight is the number of invocations currently executing.
func (s *Server) InFlight() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint32(s.inFlight)
}

// Drain stops accepting new invocations. In-flight ones run to completion.
func (s *Server) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return
	}
	s.draining = true
	if s.inFlight == 0 {
		close(s.idle)
	}
}

// Idle is closed once the server is draining and no invocation is running.
func (s *Server) Idle() <-chan struct{} { return s.idle }

func (s *Server) acquire() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "worker is draining")
	}
	if s.inFlight >= int(s.info.GetMaxConcurrency()) {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY,
			"all %d slots busy", s.info.GetMaxConcurrency())
	}
	s.inFlight++
	return nil
}

func (s *Server) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight--
	if s.draining && s.inFlight == 0 {
		close(s.idle)
	}
}

// Describe implements ExecutionService.
func (s *Server) Describe(context.Context, *connect.Request[workerv1.DescribeRequest]) (*connect.Response[workerv1.DescribeResponse], error) {
	return connect.NewResponse(&workerv1.DescribeResponse{Info: s.info}), nil
}

// Execute implements ExecutionService.
func (s *Server) Execute(ctx context.Context, req *connect.Request[workerv1.ExecuteRequest], stream *connect.ServerStream[workerv1.ExecuteResponse]) error {
	m := req.Msg
	if m.GetRequest() == nil {
		return errs.Invalid("request is required")
	}
	if !slices.Contains(s.info.GetAbis(), m.GetAbi()) {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
			"abi %q is not supported (supported: %v)", m.GetAbi(), s.info.GetAbis())
	}
	if !cache.ValidDigest(m.GetArtifactDigest()) {
		return errs.Invalid("malformed artifact digest %q", m.GetArtifactDigest())
	}
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.defaultTimeout)
		defer cancel()
	}

	log := s.log.With("invocation_id", m.GetInvocationId(), "digest", m.GetArtifactDigest())
	res, err := s.run(ctx, m)
	if err != nil {
		log.Info("invocation failed", "reason", errs.Reason(err).String(), "error", err)
		return err
	}
	log.Info("invocation done", "status", res.Head.GetStatus(), "duration", res.Duration, "peak_memory", res.PeakMemoryBytes)

	if err := stream.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: res.Head}}); err != nil {
		return err
	}
	for body := res.Body; len(body) > 0; {
		n := min(chunkSize, len(body))
		if err := stream.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: body[:n]}}); err != nil {
			return err
		}
		body = body[n:]
	}
	return stream.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{
		DurationMicros:  uint64(res.Duration.Microseconds()),
		PeakMemoryBytes: res.PeakMemoryBytes,
		Stderr:          res.Stderr,
		StderrTruncated: res.StderrTruncated,
	}}})
}

// run fetches the module and executes it, returning protocol-typed errors.
func (s *Server) run(ctx context.Context, m *workerv1.ExecuteRequest) (*runtime.Result, error) {
	wasm, err := s.artifacts.Get(ctx, m.GetArtifactDigest())
	if err != nil {
		return nil, artifactError(ctx, err)
	}
	res, err := s.engine.Run(ctx, runtime.Invocation{
		Digest: m.GetArtifactDigest(), Wasm: wasm,
		Request: m.GetRequest(), Env: m.GetEnv(), Limits: m.GetLimits(),
		ABI: m.GetAbi(), Egress: grants(m.GetEgress()), InvocationID: m.GetInvocationId(),
	})
	if err == nil {
		return res, nil
	}
	var re *runtime.Error
	var inv *runtime.InvalidError
	switch {
	case errors.As(err, &re):
		return nil, errs.New(re.Reason, "%s", re.Msg)
	case errors.As(err, &inv):
		return nil, errs.Invalid("%s", inv.Msg)
	case errors.Is(err, context.Canceled):
		return nil, connect.NewError(connect.CodeCanceled, err)
	default:
		return nil, errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "%v", err)
	}
}

// artifactError classifies a failure to obtain module bytes. Only "unknown
// artifact" is meaningful to the orchestrator; anything else, including a
// failed authentication against the orchestrator, is this worker's problem.
func artifactError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, cache.ErrDigestMismatch):
		return errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "artifact failed verification: %v", err)
	case errs.Reason(err) == commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND:
		return errs.New(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, "artifact not found")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return errs.New(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded fetching artifact")
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	default:
		return errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "fetching artifact: %v", err)
	}
}

// grants converts the orchestrator's egress grants for the runtime.
func grants(gs []*workerv1.EgressGrant) []runtime.Grant {
	out := make([]runtime.Grant, len(gs))
	for i, g := range gs {
		out[i] = runtime.Grant{Allow: g.GetAllow(), Service: g.GetService()}
	}
	return out
}
