package telemetry_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	"github.com/Olian04/edgerun/internal/telemetry"
	"github.com/Olian04/edgerun/internal/telemetry/telemetrytest"
)

func clearEnv(t *testing.T) {
	for _, k := range []string{"OTEL_SDK_DISABLED", "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER", "OTEL_LOGS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "OTEL_SERVICE_NAME"} {
		t.Setenv(k, "")
	}
}

// With nothing asked for, nothing is exported, and a trace is still carried.
func TestSetupWithoutConfigurationExportsNothingButStillPropagates(t *testing.T) {
	clearEnv(t)
	tel, err := telemetry.Setup(context.Background(), telemetry.Config{Service: "edgerun-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())

	if tel.LoggerProvider() != nil {
		t.Error("logs are exported with no OTEL_* variable set")
	}
	h := slog.NewTextHandler(new(strings.Builder), nil)
	if tel.Handler(h) != slog.Handler(h) {
		t.Error("the log handler was wrapped though logs are not exported")
	}
	if !strings.Contains(strings.Join(otel.GetTextMapPropagator().Fields(), ","), "traceparent") {
		t.Errorf("propagator fields %v: a component that exports nothing must still pass the trace on", otel.GetTextMapPropagator().Fields())
	}
}

func TestSetupDisabledAndSignalSelection(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		logs bool
	}{
		{"a logs endpoint turns logs on", map[string]string{"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://127.0.0.1:1"}, true},
		{"a general endpoint turns logs on", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1"}, true},
		{"an explicit none beats the endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1", "OTEL_LOGS_EXPORTER": "none"}, false},
		{"the SDK can be disabled", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1", "OTEL_SDK_DISABLED": "true"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			tel, err := telemetry.Setup(context.Background(), telemetry.Config{Service: "edgerun-test"})
			if err != nil {
				t.Fatal(err)
			}
			defer tel.Shutdown(context.Background())
			if got := tel.LoggerProvider() != nil; got != c.logs {
				t.Errorf("logs exported = %v, want %v", got, c.logs)
			}
		})
	}
}

// A function is handed the platform's span and nobody else's: whatever
// trace headers a request carried are replaced.
func TestInjectHeadersReplacesWhatTheClientSent(t *testing.T) {
	telemetrytest.Install(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "run")
	defer span.End()

	in := []*commonv1.Header{
		{Name: "Accept", Value: "*/*"},
		{Name: "Traceparent", Value: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		{Name: "tracestate", Value: "vendor=client"},
		{Name: "baggage", Value: "user=somebody"},
	}
	out := telemetry.InjectHeaders(ctx, in)

	got := map[string]string{}
	for _, h := range out {
		got[strings.ToLower(h.GetName())] = h.GetValue()
	}
	if got["accept"] != "*/*" {
		t.Errorf("other headers were lost: %v", got)
	}
	if _, ok := got["baggage"]; ok {
		t.Error("baggage survived")
	}
	want := "00-" + span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
	if got["traceparent"] != want {
		t.Errorf("traceparent = %q, want %q", got["traceparent"], want)
	}
	if _, ok := got["tracestate"]; ok {
		t.Errorf("the client's tracestate survived: %q", got["tracestate"])
	}
	if len(in) != 4 {
		t.Error("the input was modified")
	}
}

func TestInjectHeadersWithNoTraceJustRemovesTheClients(t *testing.T) {
	out := telemetry.InjectHeaders(context.Background(), []*commonv1.Header{{Name: "traceparent", Value: "x"}, {Name: "a", Value: "b"}})
	if len(out) != 1 || out[0].GetName() != "a" {
		t.Errorf("got %v", out)
	}
}

// A record logged with a context that has a span carries its ids in the export.
func TestLogsAreCorrelatedWithTheSpan(t *testing.T) {
	rec := telemetrytest.Install(t)
	log := slog.New(otelslog.NewHandler("test", otelslog.WithLoggerProvider(rec.Logs)))

	ctx, span := otel.Tracer("test").Start(context.Background(), "work")
	log.InfoContext(ctx, "inside")
	span.End()
	log.Info("outside")

	var inside, outside *telemetrytest.LogRecord
	for _, r := range rec.Records() {
		r := r
		switch r.Body {
		case "inside":
			inside = &r
		case "outside":
			outside = &r
		}
	}
	if inside == nil || outside == nil {
		t.Fatalf("records: %+v", rec.Records())
	}
	if inside.TraceID != span.SpanContext().TraceID().String() || inside.SpanID != span.SpanContext().SpanID().String() {
		t.Errorf("record carries %s/%s, want the span's %s/%s", inside.TraceID, inside.SpanID, span.SpanContext().TraceID(), span.SpanContext().SpanID())
	}
	if outside.TraceID != "" && outside.TraceID != (trace.TraceID{}).String() {
		t.Errorf("a record with no span carries trace id %s", outside.TraceID)
	}
}
