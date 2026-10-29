// Package invoke implements ingress.v1.IngressService: it routes a request
// from a frontend to a function and proxies the worker's streamed response.
//
// The frontend sends bytes; this package knows functions, versions and
// workers. Retries follow docs/protocol.md: only before any response byte has
// been sent, and only when the invocation cannot have run (NO_CAPACITY, or the
// worker was unreachable).
package invoke

import (
	"context"
	"errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/netroute"
	"github.com/Olian04/edgerun/internal/orchestrator/sched"
	"github.com/Olian04/edgerun/internal/orchestrator/snapshot"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/telemetry"
)

// IngressScope is the token scope a frontend presents.
const IngressScope = edgerun.ScopeIngress

// Recorder receives one record per routed invocation.
type Recorder interface{ Record(*controlv1.Invocation) }

// Config configures a Service. Zero values select the defaults.
type Config struct {
	Cache    *snapshot.Cache
	Recorder Recorder
	// HTTP is the client used to reach workers.
	HTTP *http.Client
	// ExecutionToken is presented to workers.
	ExecutionToken string
	// MaxRequestBodyBytes bounds a request body. Default 6 MiB.
	MaxRequestBodyBytes uint64
	// MaxAttempts bounds worker attempts per invocation. Default 3.
	MaxAttempts int
	// DefaultTimeout is the budget when the RPC carries no deadline. Default 30s.
	DefaultTimeout time.Duration
	// DownFor is how long an unreachable worker is skipped. Default 5s.
	DownFor time.Duration
	Version string
	Log     *slog.Logger
}

// Service implements IngressService.
type Service struct {
	ingressv1connect.UnimplementedIngressServiceHandler
	cfg Config

	clients sync.Map // endpoint -> *edgerun.Execution
	tel     *instruments
	downMu  sync.Mutex
	down    map[string]time.Time // endpoint -> skip until
}

// New returns a service.
func New(cfg Config) *Service {
	if cfg.MaxRequestBodyBytes == 0 {
		cfg.MaxRequestBodyBytes = 6 << 20
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.DefaultTimeout == 0 {
		cfg.DefaultTimeout = 30 * time.Second
	}
	if cfg.DownFor == 0 {
		cfg.DownFor = 5 * time.Second
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{}
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	return &Service{cfg: cfg, down: map[string]time.Time{}, tel: newInstruments(cfg.Cache)}
}

// Handler returns the path and handler to mount, guarded by the verifier.
func (s *Service) Handler(v authn.Verifier) (string, http.Handler) {
	return ingressv1connect.NewIngressServiceHandler(s,
		connect.WithInterceptors(telemetry.ServerInterceptor(), authn.Server(v, IngressScope)),
		connect.WithReadMaxBytes(int(s.cfg.MaxRequestBodyBytes)+(256<<10)), connect.WithCompressMinBytes(authn.CompressMinBytes))
}

// Describe implements IngressService.
func (s *Service) Describe(context.Context, *connect.Request[ingressv1.DescribeRequest]) (*connect.Response[ingressv1.DescribeResponse], error) {
	return connect.NewResponse(&ingressv1.DescribeResponse{
		Implementation: &commonv1.Implementation{
			Name: "edgerun-orchestrator", Version: s.cfg.Version,
			Protocols: []string{"edgerun.ingress.v1", "edgerun.control.v1", "edgerun.worker.v1"},
		},
		MaxRequestBodyBytes: s.cfg.MaxRequestBodyBytes,
	}), nil
}

// Invoke implements IngressService.
func (s *Service) Invoke(ctx context.Context, req *connect.Request[ingressv1.InvokeRequest], stream *connect.ServerStream[ingressv1.InvokeResponse]) error {
	r := req.Msg.GetRequest()
	if r == nil {
		return errs.Invalid("request is required")
	}
	if uint64(len(r.GetBody())) > s.cfg.MaxRequestBodyBytes {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE,
			"request body of %d bytes exceeds the %d byte limit", len(r.GetBody()), s.cfg.MaxRequestBodyBytes)
	}
	route := s.cfg.Cache.Route(r.GetHost(), r.GetPath())
	if route == nil {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, "no route for %s%s", r.GetHost(), r.GetPath())
	}
	if route.Version == nil {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, "function %q has no active version", route.FunctionName)
	}

	// From here on the invocation is recorded, whatever happens.
	started := time.Now()
	rec := &controlv1.Invocation{
		Id: newID(), RequestId: req.Msg.GetRequestId(), FunctionId: route.FunctionID, VersionId: route.Version.ID,
		Method: r.GetMethod(), Path: r.GetPath(), StartTime: timestamppb.New(started),
	}
	span := trace.SpanFromContext(ctx)
	if sc := span.SpanContext(); sc.IsValid() {
		rec.TraceId = sc.TraceID().String()
	}
	span.SetAttributes(
		attribute.String("edgerun.invocation.id", rec.Id), attribute.String("edgerun.request.id", rec.RequestId),
		attribute.String("edgerun.function.name", route.FunctionName), attribute.String("edgerun.function.id", route.FunctionID),
		attribute.String("edgerun.version.id", route.Version.ID), attribute.String("edgerun.abi", route.Version.ABI))
	attempts := 0
	var summary *workerv1.ExecutionSummary
	finish := func(err error) error {
		s.tel.finish(ctx, span, rec, route.FunctionName, attempts, started, err)
		if err != nil {
			info := errs.Info(err)
			rec.ErrorReason = info.GetReason()
			if summary == nil && info != nil {
				rec.Stderr = []byte(info.GetMessage())
			}
		}
		if summary != nil {
			rec.DurationMicros, rec.PeakMemoryBytes, rec.Stderr = summary.GetDurationMicros(), summary.GetPeakMemoryBytes(), summary.GetStderr()
		} else {
			rec.DurationMicros = uint64(time.Since(started).Microseconds())
		}
		s.cfg.Recorder.Record(rec)
		return err
	}
	if route.EnvErr != nil {
		s.cfg.Log.ErrorContext(ctx, "cannot open the environment of a routed function", "function", route.FunctionName, "error", route.EnvErr)
		return finish(errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "function %q has an unreadable environment", route.FunctionName))
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefaultTimeout)
		defer cancel()
	}

	candidates, err := s.candidates(route.Version.ABI)
	if err != nil {
		return finish(err)
	}
	order := sched.Order(route.Version.Digest, candidates)

	exec := &workerv1.ExecuteRequest{
		InvocationId: rec.Id, ArtifactDigest: route.Version.Digest, Abi: route.Version.ABI,
		Limits: route.Version.Limits, Env: route.Env, Request: r,
		Egress: netroute.Resolve(route.Egress, s.cfg.Cache.NetworkServices()),
	}
	headSent := false
	var lastErr error
	for i := 0; i < min(s.cfg.MaxAttempts, len(order)); i++ {
		w := order[i]
		rec.WorkerId = w.ID
		attempts++
		actx, aspan := s.tel.tracer.Start(ctx, "edgerun.attempt", trace.WithAttributes(
			attribute.Int("edgerun.attempt", attempts), attribute.String("edgerun.worker.id", w.ID),
			attribute.String("server.address", w.Endpoint)))
		err := s.attempt(actx, w, exec, stream, rec, &summary, &headSent)
		if err == nil {
			aspan.SetAttributes(attribute.String("edgerun.attempt.outcome", "served"))
			aspan.End()
			return finish(nil)
		}
		if headSent || !s.retryable(ctx, w, err) {
			aspan.SetAttributes(attribute.String("edgerun.attempt.outcome", "failed"), errorType(errs.Reason(err)))
			aspan.SetStatus(codes.Error, errs.Reason(err).String())
			aspan.End()
			return finish(s.translate(ctx, err))
		}
		kind := "unreachable"
		if errs.Reason(err) == commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
			kind = "no_capacity"
		}
		aspan.SetAttributes(attribute.String("edgerun.attempt.outcome", "refused: "+kind))
		aspan.End()
		s.tel.refusals.Add(ctx, 1, metric.WithAttributes(attribute.String("edgerun.refusal.kind", kind)))
		lastErr = err
		s.cfg.Log.DebugContext(ctx, "trying another worker", "invocation_id", rec.Id, "attempt", attempts,
			"worker", w.Endpoint, "reason", errs.Reason(err).String())
	}
	// Every attempt was refused or unreachable.
	if errs.Reason(lastErr) == commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
		return finish(s.translate(ctx, lastErr))
	}
	return finish(errs.New(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "no worker could be reached"))
}

// candidates returns the workers that may run a module built for abi.
func (s *Service) candidates(abi string) ([]store.Worker, error) {
	ready := s.cfg.Cache.Workers()
	var supporting, usable []store.Worker
	for _, w := range ready {
		if !w.Supports(abi) {
			continue
		}
		supporting = append(supporting, w)
		if !s.isDown(w.Endpoint) {
			usable = append(usable, w)
		}
	}
	switch {
	case len(usable) > 0:
		return usable, nil
	case len(supporting) > 0:
		return nil, errs.New(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "every worker for %s is currently unreachable", abi)
	case len(ready) > 0:
		return nil, errs.New(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, "no worker supports %s", abi)
	}
	return nil, errs.New(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "no workers are available")
}

// attempt runs the invocation on one worker, forwarding its response. After
// the first byte has been forwarded (*headSent) the attempt cannot be retried.
func (s *Service) attempt(ctx context.Context, w store.Worker, exec *workerv1.ExecuteRequest,
	out *connect.ServerStream[ingressv1.InvokeResponse], rec *controlv1.Invocation,
	summary **workerv1.ExecutionSummary, headSent *bool) error {

	res, err := s.client(w.Endpoint).Execute(ctx, exec)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	*headSent = true
	rec.Status = res.Head.GetStatus()
	if err := out.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{
		Head: &ingressv1.InvokeHead{Http: res.Head, InvocationId: rec.Id}}}); err != nil {
		return err
	}
	buf := make([]byte, 64<<10)
	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			if serr := out.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: buf[:n]}}); serr != nil {
				return serr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if *summary = res.Summary(); *summary == nil {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "worker ended the response without a summary")
	}
	return nil
}

// retryable reports whether the invocation cannot have run, so it is safe to
// try another worker. An unreachable worker is also remembered as down.
func (s *Service) retryable(ctx context.Context, w store.Worker, err error) bool {
	if errs.Reason(err) == commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
		return true
	}
	if unreachable(err) {
		s.markDown(w.Endpoint)
		s.cfg.Log.WarnContext(ctx, "worker unreachable; trying another", "endpoint", w.Endpoint, "error", err)
		return true
	}
	return false
}

// unreachable reports whether the connection to the worker was never
// established: the request cannot have reached it.
func unreachable(err error) bool {
	var op *net.OpError
	return connect.CodeOf(err) == connect.CodeUnavailable && errors.As(err, &op) && op.Op == "dial"
}

// translate turns a failure into the error the frontend sees. Worker errors
// keep their reason; anything else is an internal error whose detail stays in
// the logs, not on the wire.
// deadlineSlack is how close to its deadline a cancelled invocation must have
// been to count as having run out of budget.
const deadlineSlack = 100 * time.Millisecond

func (s *Service) translate(ctx context.Context, err error) error {
	if info := errs.Info(err); info != nil && info.GetReason() != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return errs.New(info.GetReason(), "%s", info.GetMessage())
	}
	// Our own deadline can expire before the worker's typed error arrives
	// (docs/protocol.md), and the worker's deadline can fire before our timer
	// does: a deadline error from either side means the budget is spent, and
	// that is DEADLINE_EXCEEDED whoever noticed first.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || connect.CodeOf(err) == connect.CodeDeadlineExceeded {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded")
	}
	if errors.Is(err, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled {
		// A caller whose own timer is the same budget as ours gives up at about the
		// same moment we would have: a cancellation within the slack of the
		// deadline is the deadline, and so is recorded, or "errors by reason"
		// would count some of the spent budgets as nothing.
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < deadlineSlack {
			return errs.New(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded")
		}
		// Cancelled is the caller's only if the caller's context has ended. A
		// worker that dies mid-call (a reset connection) is reported by the
		// transport as cancelled too, and passing that on would tell the frontend
		// that nobody is waiting, so it would answer nothing: a success, silently.
		if ctx.Err() != nil {
			return connect.NewError(connect.CodeCanceled, errors.New("request cancelled"))
		}
	}
	s.cfg.Log.ErrorContext(ctx, "worker call failed", "error", err)
	return errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "worker call failed")
}

func (s *Service) client(endpoint string) *edgerun.Execution {
	if c, ok := s.clients.Load(endpoint); ok {
		return c.(*edgerun.Execution)
	}
	c := edgerun.NewExecution(endpoint, s.cfg.ExecutionToken, edgerun.WithHTTPClient(s.cfg.HTTP))
	actual, _ := s.clients.LoadOrStore(endpoint, c)
	return actual.(*edgerun.Execution)
}

func (s *Service) markDown(endpoint string) {
	s.downMu.Lock()
	s.down[endpoint] = time.Now().Add(s.cfg.DownFor)
	s.downMu.Unlock()
}

func (s *Service) isDown(endpoint string) bool {
	s.downMu.Lock()
	defer s.downMu.Unlock()
	until, ok := s.down[endpoint]
	if ok && time.Now().After(until) {
		delete(s.down, endpoint)
		return false
	}
	return ok
}

func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}
