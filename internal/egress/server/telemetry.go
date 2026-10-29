package server

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/errs"
)

const scope = "github.com/Olian04/edgerun/internal/egress/server"

func tracer() trace.Tracer { return otel.Tracer(scope) }

// fetchOutcome is what a Fetch learned that its error does not say.
type fetchOutcome struct {
	status int    // the upstream's status, 0 if it never answered
	bytes  uint64 // the response body returned
}

type egressTel struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
	bytes    metric.Int64Counter
}

func newEgressTel() *egressTel {
	m := otel.Meter(scope)
	t := &egressTel{}
	t.requests, _ = m.Int64Counter("edgerun.egress.requests", metric.WithUnit("{request}"),
		metric.WithDescription("Fetches this network service was asked for, by how they came out (error.type is the reason it was refused or failed, empty when it was carried out)."))
	t.duration, _ = m.Float64Histogram("edgerun.egress.duration", metric.WithUnit("s"),
		metric.WithDescription("How long a fetch took, request received to response ended."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
	t.bytes, _ = m.Int64Counter("edgerun.egress.response.size", metric.WithUnit("By"),
		metric.WithDescription("Response body bytes this service returned."))
	return t
}

// finish records one Fetch. It names the destination host in the span, never
// the URL: a path or query may carry what a guest does not mean to publish.
func (t *egressTel) finish(ctx context.Context, r *egressv1.FetchRequest, o *fetchOutcome, err error, took time.Duration) {
	reason := errs.Reason(err)
	attrs := []attribute.KeyValue{attribute.String("http.request.method", r.GetMethod()), attribute.String("error.type", errorName(reason, err))}
	if o.status != 0 {
		attrs = append(attrs, attribute.Int("http.response.status_code", o.status))
	}
	t.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.duration.Record(ctx, took.Seconds(), metric.WithAttributes(attrs...))
	t.bytes.Add(ctx, int64(o.bytes))
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("server.address", hostOf(r.GetUrl())), attribute.String("http.request.method", r.GetMethod()),
		attribute.Int("http.response.status_code", o.status), attribute.Int64("edgerun.egress.response.size", int64(o.bytes)),
		attribute.String("edgerun.invocation.id", r.GetInvocationId()))
	if err != nil {
		span.SetAttributes(attribute.String("error.type", errorName(reason, err)))
		span.SetStatus(codes.Error, errorName(reason, err))
	}
}

func errorName(reason commonv1.ErrorReason, err error) string {
	if err == nil {
		return ""
	}
	if reason.String() != "ERROR_REASON_UNSPECIFIED" {
		return reason.String()
	}
	return "INVALID_ARGUMENT"
}
