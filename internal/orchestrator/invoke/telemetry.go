package invoke

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/codes"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/snapshot"
)

// The instrumentation scope of everything this package emits.
const scope = "github.com/Olian04/edgerun/internal/orchestrator/invoke"

// instruments are the orchestrator's own metrics (docs/observability.md). The RPC
// metrics (rpc.server.duration and the rest) come from the Connect interceptor.
type instruments struct {
	tracer     trace.Tracer
	duration   metric.Float64Histogram
	attempts   metric.Int64Histogram
	refusals   metric.Int64Histogram
	registered metric.Registration
}

func newInstruments(cache *snapshot.Cache) *instruments {
	m := otel.Meter(scope)
	in := &instruments{tracer: otel.Tracer(scope)}
	in.duration, _ = m.Float64Histogram("edgerun.invocation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("How long a routed invocation took, from the orchestrator receiving it to its response ending."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
	in.attempts, _ = m.Int64Histogram("edgerun.invocation.attempts",
		metric.WithUnit("{attempt}"),
		metric.WithDescription("How many workers an invocation was offered to before it was served or given up."),
		metric.WithExplicitBucketBoundaries(1, 2, 3, 4))
	in.refusals, _ = m.Int64Histogram("edgerun.invocation.refusals",
		metric.WithUnit("{refusal}"),
		metric.WithDescription("How many attempts on a worker came to nothing and led to trying another, per invocation and kind (edgerun.refusal.kind): the worker had no free slot, or could not be reached. Zero is recorded too, so a percentile says how often an invocation had to be moved."),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4))
	ready, _ := m.Int64ObservableGauge("edgerun.workers.ready",
		metric.WithUnit("{worker}"), metric.WithDescription("Workers that are alive and ready for work, as this orchestrator sees them."))
	svcs, _ := m.Int64ObservableGauge("edgerun.network_services.ready",
		metric.WithUnit("{service}"), metric.WithDescription("Network services that are alive and ready, as this orchestrator sees them."))
	in.registered, _ = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(ready, int64(len(cache.Workers())))
		o.ObserveInt64(svcs, int64(len(cache.NetworkServices())))
		return nil
	}, ready, svcs)
	return in
}

// finish records what an invocation came to: the metrics, and the span's outcome.
func (in *instruments) finish(ctx context.Context, span trace.Span, rec *controlv1.Invocation, function string, attempts int, refused map[string]int, started time.Time, err error) {
	reason := errs.Reason(err)
	attrs := []attribute.KeyValue{attribute.String("edgerun.function.name", function), errorType(reason)}
	if rec.Status != 0 {
		attrs = append(attrs, attribute.Int("http.response.status_code", int(rec.Status)))
	}
	in.duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attrs...))
	in.attempts.Record(ctx, int64(attempts), metric.WithAttributes(attribute.String("edgerun.function.name", function)))
	for _, kind := range []string{"no_capacity", "unreachable"} {
		in.refusals.Record(ctx, int64(refused[kind]), metric.WithAttributes(attribute.String("edgerun.refusal.kind", kind)))
	}
	if err != nil {
		span.SetAttributes(errorType(reason))
		if reason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			span.SetStatus(codes.Error, reason.String())
		}
	}
	span.SetAttributes(attribute.Int("edgerun.attempts", attempts))
}

func (in *instruments) close() {
	if in.registered != nil {
		in.registered.Unregister()
	}
}

// outcome is the label an invocation ends with: the platform's reason, or none.
func errorType(reason commonv1.ErrorReason) attribute.KeyValue {
	if reason == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return attribute.String("error.type", "")
	}
	return attribute.String("error.type", reason.String())
}
