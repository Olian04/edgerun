// Package telemetrytest installs in-memory OpenTelemetry providers for a test,
// so that what a component emits can be asserted on.
package telemetrytest

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Olian04/edgerun/internal/telemetry"
)

// Recorder holds what was emitted since Install.
type Recorder struct {
	spans   *tracetest.SpanRecorder
	metrics *sdkmetric.ManualReader
	logs    *logRecorder
	Logs    *sdklog.LoggerProvider
}

// Install makes the global providers in-memory ones until the test ends. Build
// the components under test after calling it: they take their instruments from
// the providers that are installed when they are built.
func Install(t testing.TB) *Recorder {
	t.Helper()
	r := &Recorder{spans: tracetest.NewSpanRecorder(), metrics: sdkmetric.NewManualReader(), logs: &logRecorder{}}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(r.spans))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.metrics))
	r.Logs = sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(r.logs)))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	global.SetLoggerProvider(r.Logs)
	otel.SetTextMapPropagator(telemetry.Propagator())
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetMeterProvider(otel.GetMeterProvider())
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
		_ = r.Logs.Shutdown(context.Background())
	})
	return r
}

// Spans returns the spans that have ended.
func (r *Recorder) Spans() []sdktrace.ReadOnlySpan { return r.spans.Ended() }

// Metrics collects every metric now.
func (r *Recorder) Metrics(t testing.TB) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.metrics.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// LogRecord is one exported log record.
type LogRecord struct {
	Body    string
	TraceID string
	SpanID  string
	Attrs   map[string]string
}

// Records returns the log records exported so far.
func (r *Recorder) Records() []LogRecord { return r.logs.snapshot() }

type logRecorder struct {
	mu   sync.Mutex
	recs []LogRecord
}

func (l *logRecorder) Export(_ context.Context, rs []sdklog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range rs {
		lr := LogRecord{Body: r.Body().AsString(), TraceID: r.TraceID().String(), SpanID: r.SpanID().String(), Attrs: map[string]string{}}
		if !r.TraceID().IsValid() {
			lr.TraceID, lr.SpanID = "", ""
		}
		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			lr.Attrs[string(kv.Key)] = kv.Value.Emit()
			return true
		})
		l.recs = append(l.recs, lr)
	}
	return nil
}
func (l *logRecorder) Shutdown(context.Context) error   { return nil }
func (l *logRecorder) ForceFlush(context.Context) error { return nil }
func (l *logRecorder) snapshot() []LogRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]LogRecord(nil), l.recs...)
}
