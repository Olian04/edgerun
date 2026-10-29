package runtime

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const scope = "github.com/Olian04/edgerun/internal/worker/runtime"

func tracer() trace.Tracer { return otel.Tracer(scope) }

// engineTel is what the engine measures about the guests it runs
// (docs/observability.md).
type engineTel struct {
	duration metric.Float64Histogram
	memory   metric.Int64Histogram
	runs     metric.Int64Counter
}

func newEngineTel() *engineTel {
	m := otel.Meter(scope)
	t := &engineTel{}
	t.duration, _ = m.Float64Histogram("edgerun.guest.duration", metric.WithUnit("s"),
		metric.WithDescription("How long a guest ran, instantiation to exit."),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
	t.memory, _ = m.Int64Histogram("edgerun.guest.memory.peak", metric.WithUnit("By"),
		metric.WithDescription("A guest's linear memory at exit, which never shrinks, so its peak."),
		metric.WithExplicitBucketBoundaries(1<<20, 4<<20, 16<<20, 32<<20, 64<<20, 128<<20, 256<<20, 512<<20))
	t.runs, _ = m.Int64Counter("edgerun.guest.runs", metric.WithUnit("{run}"),
		metric.WithDescription("Guest runs, by how they ended (error.type is the platform's reason, empty when the guest answered)."))
	return t
}

func (t *engineTel) finish(ctx context.Context, span trace.Span, abi string, res *Result, err error) {
	attrs := []attribute.KeyValue{attribute.String("edgerun.abi", abi)}
	reason := ""
	if err != nil {
		var re *Error
		var inv *InvalidError
		switch {
		case asError(err, &re):
			reason = re.Reason.String()
		case asInvalid(err, &inv):
			reason = "INVALID_ARGUMENT"
		default:
			reason = "INTERNAL"
		}
		span.SetStatus(codes.Error, reason)
	}
	attrs = append(attrs, attribute.String("error.type", reason))
	if res != nil {
		d := res.Duration
		t.duration.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
		t.memory.Record(ctx, int64(res.PeakMemoryBytes), metric.WithAttributes(attribute.String("edgerun.abi", abi)))
		span.SetAttributes(attribute.Int("http.response.status_code", int(res.Head.GetStatus())),
			attribute.Int64("edgerun.guest.memory.peak", int64(res.PeakMemoryBytes)),
			attribute.Int64("edgerun.guest.duration_us", d.Microseconds()))
	}
	span.SetAttributes(attribute.String("error.type", reason))
	t.runs.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}

func asInvalid(err error, target **InvalidError) bool {
	e, ok := err.(*InvalidError)
	if ok {
		*target = e
	}
	return ok
}
