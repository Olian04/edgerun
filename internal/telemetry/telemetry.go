// Package telemetry connects edgerun's components to OpenTelemetry: traces,
// metrics and logs, exported the way OpenTelemetry says (the standard OTEL_*
// environment variables, OTLP by default) and nothing edgerun-specific to learn.
//
// Telemetry is off unless it is asked for: with no OTEL_* exporter or endpoint
// variable set, a component exports nothing and pays next to nothing. What is
// always on is trace context propagation (W3C Trace Context, docs/protocol.md,
// "Observability"): a component that is not exporting still passes the trace
// along, so that the components that are can join it into one trace.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/google/uuid"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// Config names the component.
type Config struct {
	// Service is service.name unless OTEL_SERVICE_NAME says otherwise,
	// e.g. "edgerun-orchestrator".
	Service string
	// Version is service.version.
	Version string
}

// Telemetry is the running setup. Shut it down to flush what is buffered.
type Telemetry struct {
	shutdown []func(context.Context) error
	logs     *sdklog.LoggerProvider
	service  string
}

// Propagator is what carries a trace between components: W3C Trace Context. No
// baggage: nothing in edgerun reads it, and a public frontend should not pass
// on what a client chose to send.
func Propagator() propagation.TextMapPropagator { return propagation.TraceContext{} }

// Setup configures the global OpenTelemetry providers from the environment. A
// signal is exported when OTEL_<SIGNAL>_EXPORTER names an exporter other than
// "none", or when it is unset and an OTLP endpoint (OTEL_EXPORTER_OTLP_ENDPOINT
// or the signal's own) is set; OTEL_SDK_DISABLED=true turns everything off.
func Setup(ctx context.Context, cfg Config) (*Telemetry, error) {
	otel.SetTextMapPropagator(Propagator())
	t := &Telemetry{service: cfg.Service}
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return t, nil
	}
	res, err := resourceFor(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if signalOn("TRACES") {
		exp, err := autoexport.NewSpanExporter(ctx)
		if err != nil {
			return nil, err
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		t.shutdown = append(t.shutdown, tp.Shutdown)
	}
	if signalOn("METRICS") {
		rd, err := autoexport.NewMetricReader(ctx)
		if err != nil {
			return nil, err
		}
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rd), sdkmetric.WithResource(res))
		otel.SetMeterProvider(mp)
		t.shutdown = append(t.shutdown, mp.Shutdown)
		if err := runtime.Start(); err != nil {
			return nil, err
		}
	}
	if signalOn("LOGS") {
		exp, err := autoexport.NewLogExporter(ctx)
		if err != nil {
			return nil, err
		}
		lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)), sdklog.WithResource(res))
		global.SetLoggerProvider(lp)
		t.logs = lp
		t.shutdown = append(t.shutdown, lp.Shutdown)
	}
	return t, nil
}

// signalOn says whether a signal ("TRACES", "METRICS", "LOGS") is exported.
func signalOn(signal string) bool {
	if e := strings.TrimSpace(os.Getenv("OTEL_" + signal + "_EXPORTER")); e != "" {
		return !strings.EqualFold(e, "none")
	}
	for _, k := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_" + signal + "_ENDPOINT"} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

func resourceFor(ctx context.Context, cfg Config) (*resource.Resource, error) {
	base := resource.NewSchemaless(
		semconv.ServiceName(cfg.Service),
		semconv.ServiceVersion(cfg.Version),
		semconv.ServiceInstanceID(uuid.NewString()),
	)
	// The environment (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES) wins over the defaults.
	env, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK(), resource.WithHost(), resource.WithProcessPID())
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, err
	}
	return resource.Merge(base, env)
}

// Shutdown flushes and stops the exporters.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var errs []error
	for _, f := range t.shutdown {
		errs = append(errs, f(ctx))
	}
	return errors.Join(errs...)
}

// Handler returns h, and, if logs are exported, h and the OpenTelemetry bridge
// together: a record goes to both. A record logged with a context that carries a
// span is correlated with it (trace_id and span_id), in the export.
func (t *Telemetry) Handler(h slog.Handler) slog.Handler {
	if t.logs == nil {
		return h
	}
	return slog.NewMultiHandler(h, otelslog.NewHandler(t.service, otelslog.WithLoggerProvider(t.logs)))
}

// LoggerProvider is the provider logs are exported through, nil if they are not.
func (t *Telemetry) LoggerProvider() otellog.LoggerProvider {
	if t.logs == nil {
		return nil
	}
	return t.logs
}

// ServerInterceptor is the Connect interceptor every edgerun server mounts: a
// server span per call, continuing the trace of its caller (the seams are
// between components that trust each other), and the RPC server metrics.
func ServerInterceptor() connect.Interceptor {
	i, err := otelconnect.NewInterceptor(otelconnect.WithTrustRemote(), otelconnect.WithPropagator(Propagator()))
	if err != nil {
		panic("telemetry: " + err.Error()) // only for a bad option, which is a bug
	}
	return i
}

// InjectHeaders returns headers with the trace context of ctx as the
// "traceparent" (and "tracestate") entries, replacing any the headers already
// carried. It is how a function is given the platform's span for its invocation,
// so that it can continue the trace in its own telemetry (HTTP_TRACEPARENT in
// the guest's environment). With no trace in ctx the entries are just removed.
func InjectHeaders(ctx context.Context, headers []*commonv1.Header) []*commonv1.Header {
	out := make([]*commonv1.Header, 0, len(headers)+2)
	for _, h := range headers {
		if n := strings.ToLower(h.GetName()); n != "traceparent" && n != "tracestate" && n != "baggage" {
			out = append(out, h)
		}
	}
	carrier := propagation.MapCarrier{}
	Propagator().Inject(ctx, carrier)
	for _, name := range []string{"traceparent", "tracestate"} {
		if v := carrier.Get(name); v != "" {
			out = append(out, &commonv1.Header{Name: name, Value: []byte(v)})
		}
	}
	return out
}

// Init is what a component's main does first: it sets telemetry up from the
// environment and returns the logger to use (JSON to stderr, and, if logs are
// exported, to OpenTelemetry too) and a function that flushes on the way out.
// A telemetry failure is logged and never stops the component: it runs without.
func Init(service, version string) (*slog.Logger, func()) {
	base := slog.NewJSONHandler(os.Stderr, nil)
	ctx := context.Background()
	t, err := Setup(ctx, Config{Service: service, Version: version})
	if err != nil {
		l := slog.New(base)
		l.Error("telemetry is not available; continuing without it", "error", err)
		return l, func() {}
	}
	return slog.New(t.Handler(base)), func() { _ = t.Shutdown(ctx) }
}
