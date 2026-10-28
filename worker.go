package edgerun

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// Execution is the client of a worker's ExecutionService: what an orchestrator
// does. It needs a token of ScopeExecute. With it, a scheduler of your own can
// run functions on edgerun's workers.
type Execution struct {
	c workerv1connect.ExecutionServiceClient
}

// NewExecution returns a client for the worker at baseURL.
func NewExecution(baseURL, token string, opts ...Option) *Execution {
	h, co := settings(token, opts)
	return &Execution{c: workerv1connect.NewExecutionServiceClient(h, baseURL, co...)}
}

func (e *Execution) Describe(ctx context.Context, req *workerv1.DescribeRequest) (*workerv1.DescribeResponse, error) {
	return call(ctx, e.c.Describe, req)
}

// ExecutionResult is the response to an Execute.
type ExecutionResult struct {
	Response
	summary *workerv1.ExecutionSummary
}

// Summary is what the worker measured. It closes a successful stream, so it is
// nil until the body has been read to its end.
func (r *ExecutionResult) Summary() *workerv1.ExecutionSummary { return r.summary }

// Execute runs a module and returns its response as soon as the head arrives.
// The deadline of ctx is the execution's wall-clock budget; the worker enforces
// it. A worker with no free slot answers NO_CAPACITY at once (see Reason).
func (e *Execution) Execute(ctx context.Context, req *workerv1.ExecuteRequest) (*ExecutionResult, error) {
	st, err := e.c.Execute(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	out := &ExecutionResult{}
	res, _, err := openStream(st,
		func(m *workerv1.ExecuteResponse) *commonv1.HttpResponseHead { return m.GetHead() },
		func(m *workerv1.ExecuteResponse) []byte {
			if s := m.GetSummary(); s != nil {
				out.summary = s
			}
			return m.GetChunk()
		})
	if err != nil {
		return nil, err
	}
	out.Response = *res
	return out, nil
}

// Fleet is the client of an orchestrator's FleetService: how a worker joins the
// fleet, stays in it and leaves. It needs a token of ScopeWorker. With it, a
// worker in any language can offer itself to an orchestrator.
type Fleet struct {
	c workerv1connect.FleetServiceClient
}

// NewFleet returns a client for the orchestrator at baseURL.
func NewFleet(baseURL, token string, opts ...Option) *Fleet {
	h, co := settings(token, opts)
	return &Fleet{c: workerv1connect.NewFleetServiceClient(h, baseURL, co...)}
}

func (f *Fleet) Register(ctx context.Context, req *workerv1.RegisterRequest) (*workerv1.RegisterResponse, error) {
	return call(ctx, f.c.Register, req)
}

func (f *Fleet) Heartbeat(ctx context.Context, req *workerv1.HeartbeatRequest) (*workerv1.HeartbeatResponse, error) {
	return call(ctx, f.c.Heartbeat, req)
}

func (f *Fleet) Deregister(ctx context.Context, req *workerv1.DeregisterRequest) (*workerv1.DeregisterResponse, error) {
	return call(ctx, f.c.Deregister, req)
}

// Artifacts is the client of an orchestrator's ArtifactService: how a worker
// obtains module bytes. It needs a token of ScopeWorker.
type Artifacts struct {
	c workerv1connect.ArtifactServiceClient
}

// NewArtifacts returns a client for the orchestrator at baseURL.
func NewArtifacts(baseURL, token string, opts ...Option) *Artifacts {
	h, co := settings(token, opts)
	return &Artifacts{c: workerv1connect.NewArtifactServiceClient(h, baseURL, co...)}
}

// ErrDigestMismatch is returned by Artifacts.GetArtifact for bytes that are not what
// their digest says. Such bytes must never be run.
var ErrDigestMismatch = errors.New("edgerun: artifact does not match its digest")

// GetArtifact returns the artifact with the given digest ("sha256:<lowercase hex>"),
// having verified it: the protocol obliges a client to, and an artifact is
// worth caching for ever precisely because its digest is its identity. The
// whole artifact is held in memory; the orchestrator bounds it (see
// DescribeResponse.MaxModuleBytes of the control protocol).
func (a *Artifacts) GetArtifact(ctx context.Context, digest string) ([]byte, error) {
	want, ok := strings.CutPrefix(digest, "sha256:")
	if !ok {
		return nil, InvalidArgument("digest %q is not sha256:<hex>", digest)
	}
	st, err := a.c.GetArtifact(ctx, connect.NewRequest(&workerv1.GetArtifactRequest{Digest: digest}))
	if err != nil {
		return nil, err
	}
	defer st.Close()
	var buf bytes.Buffer
	for st.Receive() {
		buf.Write(st.Msg().GetChunk())
	}
	if err := st.Err(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(buf.Bytes())
	if got := hex.EncodeToString(sum[:]); got != want {
		return nil, fmt.Errorf("%w: wanted %s, got sha256:%s", ErrDigestMismatch, digest, got)
	}
	return buf.Bytes(), nil
}
