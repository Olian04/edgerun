# Observability

edgerun emits traces, metrics and logs the way [OpenTelemetry](https://opentelemetry.io) says to, and nothing
edgerun-specific has to be learned to receive them: point the standard `OTEL_*` environment variables at an
OpenTelemetry Collector, or at any OTLP backend, and every component shows up as a service of its own.

The idea in one sentence: **a request is one trace, from the client's first byte to the origin a function called,
across every component it touched, and the logs and the invocation record lead back to it.**

## Turning it on

Telemetry is **off** until asked for: with no `OTEL_*` exporter or endpoint variable set, a component exports
nothing and pays next to nothing. Each signal is configured on its own, by the standard variables:

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Turns on every signal that is not explicitly `none`, over OTLP (`http/protobuf` unless `OTEL_EXPORTER_OTLP_PROTOCOL` says `grpc`). |
| `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT` | The same, for one signal. |
| `OTEL_{TRACES,METRICS,LOGS}_EXPORTER` | `otlp`, `console`, or `none`. An explicit value wins over the endpoint variables. |
| `OTEL_SDK_DISABLED=true` | Everything off. |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | Resource attributes; they win over the defaults below. |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`, `OTEL_BSP_*`, `OTEL_METRIC_EXPORT_INTERVAL`, … | As the SDK defines them. |

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 edgerun-orchestrator …
```

A resource names each component: `service.name` is `edgerun-orchestrator`, `edgerun-worker`, `edgerun-frontend`,
`edgerun-egress` or `edgerun-admin`; `service.version` is the build's; `service.instance.id` is random per process.
If telemetry cannot be set up (a malformed endpoint, say), the component says so in its log and **runs without it**.

What is **always on**, exporting or not, is trace context propagation: a component that exports nothing still
passes the trace along, so that the ones that do can join one trace. That is part of the protocol
([`protocol.md`](protocol.md), "Observability"), and the conformance suites hold every component to it.

## Traces

Propagation is W3C Trace Context (`traceparent`, `tracestate`) on every call. Baggage is not propagated: nothing in
edgerun reads it, and a public frontend should not pass on what a client chose to send.

```
GET                                   frontend   (SERVER, from the client's traceparent if it sent one)
└─ edgerun.ingress.v1.IngressService/Invoke       (CLIENT, in the frontend)
   └─ …/Invoke                                    (SERVER, in the orchestrator)
      ├─ <database spans>                         otelpgx, SQL trimmed from the span name
      └─ edgerun.attempt                          one per worker offered the call
         └─ edgerun.worker.v1.ExecutionService/Execute   (CLIENT)
            └─ …/Execute                          (SERVER, in the worker)
               ├─ edgerun.artifact.get            module bytes: hit, fetched, shared or error
               ├─ edgerun.module.compile          first run of a module on this worker
               └─ edgerun.guest.run               the guest ran
                  └─ edgerun.guest.fetch          the guest asked for the network
                     └─ edgerun.egress.v1.EgressService/Fetch   (CLIENT, then SERVER in the network service)
                        └─ edgerun.egress.upstream   (CLIENT, to the origin)
```

| Span | Attributes |
|---|---|
| frontend server span | `http.*` (otelhttp), `edgerun.request.id` |
| the orchestrator's `Invoke` server span | `edgerun.invocation.id`, `edgerun.request.id`, `edgerun.function.name`, `edgerun.function.id`, `edgerun.version.id`, `edgerun.abi`, `edgerun.attempts`, `http.response.status_code`, `error.type` |
| `edgerun.attempt` | `edgerun.attempt` (1-based), `edgerun.worker.id`, `server.address`, `edgerun.attempt.outcome` (`served`, `failed`, `refused: no_capacity`, `refused: unreachable`), `error.type` |
| Execute (worker) | `edgerun.invocation.id`, `edgerun.module.digest`, `edgerun.abi` |
| `edgerun.artifact.get` | `edgerun.artifact.digest`, `edgerun.artifact.cache` (`hit`, `fetched`, `shared`, `error`) |
| `edgerun.module.compile` | `edgerun.module.digest`, `edgerun.module.size` |
| `edgerun.guest.run` | `edgerun.abi`, `edgerun.module.digest`, `edgerun.invocation.id`, `edgerun.guest.duration_us`, `edgerun.guest.memory.peak`, `http.response.status_code`, `error.type` |
| `edgerun.guest.fetch` | `http.request.method`, `server.address`, `edgerun.egress.service`, `http.response.status_code` |
| Fetch (network service) | `edgerun.invocation.id`, `http.request.method`, `server.address`, `http.response.status_code`, `edgerun.egress.response.size`, `error.type` |
| `edgerun.egress.upstream` | `http.request.method`, `server.address`, `url.scheme`, `http.response.status_code` |

Connect calls get the standard `rpc.*` attributes and the `rpc.server.*` / `rpc.client.*` metrics from
[otelconnect](https://github.com/connectrpc/otelconnect-go). Between components the server span continues the
caller's trace (the seams join components that trust each other).

**The guest gets the trace too.** A `wagi-1` or `wagi-2` module is given `HTTP_TRACEPARENT` in its environment:
the trace id of the invocation and the id of `edgerun.guest.run` as the parent, so a function with telemetry of its
own can continue the trace. A `traceparent`, `tracestate` or `baggage` the *client* sent is never given to it.

**Nothing is injected upstream.** The network service adds no trace context to the request it makes to the origin:
the origin is outside the platform, and a trace id is a way to correlate a guest with whoever it calls, which the
guest, not the platform, decides. The trace ends at the network service.

**An invocation record leads to its trace.** `Invocation.trace_id` (control.v1) holds the 32-digit trace id when the
call carried one or the frontend started one, so from the admin UI or `ListInvocations` you can go to the trace.

## Metrics

Instrument names, units and attributes follow the OpenTelemetry semantic conventions where one fits. Metrics are
recorded by the component that knows the answer; there is no metric derived from another component's data.

| Metric | Type, unit | Where | Attributes |
|---|---|---|---|
| `edgerun.invocation.duration` | histogram, `s` | orchestrator | `edgerun.function.name`, `error.type`, `http.response.status_code` |
| `edgerun.invocation.attempts` | histogram, `{attempt}` | orchestrator | `edgerun.function.name` |
| `edgerun.invocation.refusals` | histogram, `{refusal}` | orchestrator | `edgerun.refusal.kind` (`no_capacity`, `unreachable`) |
| `edgerun.workers.ready` | gauge, `{worker}` | orchestrator | |
| `edgerun.network_services.ready` | gauge, `{service}` | orchestrator | |
| `edgerun.frontend.aborted` | counter, `{response}` | frontend | |
| `edgerun.worker.in_flight` | gauge, `{invocation}` | worker | |
| `edgerun.worker.slots` | gauge, `{invocation}` | worker | |
| `edgerun.worker.refused` | counter, `{invocation}` | worker | |
| `edgerun.guest.duration` | histogram, `s` | worker | `edgerun.abi`, `error.type` |
| `edgerun.guest.memory.peak` | histogram, `By` | worker | `edgerun.abi` |
| `edgerun.guest.runs` | counter, `{run}` | worker | `edgerun.abi`, `error.type` |
| `edgerun.artifact.requests` | counter, `{request}` | worker | `edgerun.artifact.cache` |
| `edgerun.egress.requests` | counter, `{request}` | network service | `http.request.method`, `error.type`, `http.response.status_code` |
| `edgerun.egress.duration` | histogram, `s` | network service | the same |
| `edgerun.egress.response.size` | counter, `By` | network service | |

Both `attempts` and `refusals` are distributions per invocation, so a p95 or p99 says how often an invocation had to be moved to another worker; `refusals` has a point per invocation and kind, zero included. (Nothing of this is kept in the invocation record: what is measured is telemetry's, in one place.)

`error.type` is the platform's error reason (`DEADLINE_EXCEEDED`, `FUNCTION_TRAPPED`, …, see `protocol.md`), and
empty when the guest answered; a function that returns a 500 is a successful run with `http.response.status_code=500`.
`edgerun.function.name` is a label of bounded cardinality only as long as the operator's function names are: a
deployment with thousands of functions will want to drop it in the Collector.

Every process also exports Go runtime metrics (`go.*`, from the OpenTelemetry runtime instrumentation), and the orchestrator its database connection pool's (otelpgx).

## Logs

Each component logs JSON lines to stderr, as it always has. With logs exported, the same records also go over
OTLP (through the `slog` bridge), and a record logged while handling a request carries that request's `trace_id` and
`span_id`: from a log line to its trace, or the other way round. The frontend's request line carries `request_id` and `invocation_id`, as before.

## What is deliberately not in telemetry

- **Never a URL, a header, a body, an environment value or a secret.** A span or a metric names a destination by host;
  a path and a query may hold what a guest does not mean to publish. Guest stderr stays in the invocation record
  (and is exported in no signal).
- **No client address or user agent as a metric attribute.**

## Writing your own component

Nothing here binds a replacement to OpenTelemetry: the protocol asks only that the trace is passed on
([`protocol.md`](protocol.md), "Observability"). The Go library does it for you: every client in package `edgerun`
carries the trace of the context you call it with (W3C Trace Context via otelconnect), and it exports nothing
unless your program sets up an OpenTelemetry SDK.

## A Collector

[`deploy/otel-collector.yaml`](../deploy/otel-collector.yaml) is a minimal Collector configuration that accepts OTLP
and writes everything to its own log; point it at your backend of choice. It is **untested** against a real
backend, like the Compose file.
