package cache

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const scope = "github.com/Olian04/edgerun/internal/worker/cache"

// note records how an artifact was obtained: "hit" (already here), "fetched"
// (this call went and got it), "shared" (it joined a fetch already going),
// or "error".
func (s *Store) note(ctx context.Context, span trace.Span, how string) {
	span.SetAttributes(attribute.String("edgerun.artifact.cache", how))
	s.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("edgerun.artifact.cache", how)))
}

func newRequests() metric.Int64Counter {
	c, _ := otel.Meter(scope).Int64Counter("edgerun.artifact.requests", metric.WithUnit("{request}"),
		metric.WithDescription("Module fetches by how they were answered: from this worker's cache, from the orchestrator, by joining a fetch already going, or an error."))
	return c
}
