# Findings: a mock traffic ingress built on the edgerun library

A probe, written by an agent acting as a community developer: `edgerun-trafficmock`, a frontend-role program that
synthesises or replays traffic into an orchestrator over `ingress.v1`, to load-test and soak a deployment. Each item
says what became of it in this repository.

What it is: `run` (YAML or JSON scenarios: request templates, constant, ramp, spike and burst, closed-loop and trace
replay load models; HDR histograms of time to head and total time; status as a real client would see it, with platform
errors counted by `ErrorReason`; mid-body failures counted apart; `--expect 'p99<200ms'` assertions with exit codes;
a Prometheus endpoint), `listen` (a minimal frontend, real HTTP in and `ingress.v1` out, that can record a replayable
trace) and `fake` (a pretend orchestrator built on the generated handler, `Authenticate` and `NewError`).

Tests: `go test -race ./...`, 42 top-level tests, no races: constant, ramp, spike, closed-loop and replay scenarios
against a real PostgreSQL, orchestrator and worker (counts checked against an independent count of orchestrator
records; a 503 from a function is a status, not an error; `FUNCTION_NOT_FOUND` is 404 with no record;
`DEADLINE_EXCEEDED` 504; `FUNCTION_TRAPPED` 502; `NO_CAPACITY` 503 in about 10 ms under a burst), a fake orchestrator
for mid-body failures, leaks, connection reuse and backpressure, and the frontend conformance suite against `listen`
(45 passed).

## Throughput (a 4-core VM shared with the stack; for comparison only)

| Layer | Result |
|---|---|
| The library alone (`Invoke` and a read loop, 128 concurrent, a fake in another process), small responses | pooled HTTP/1.1 19.4k req/s; h2c 9.9k; `http.DefaultClient` 14.6k |
| The same, 1 MiB responses | pooled HTTP/1.1 939 req/s, 1,490 without gzip; h2c 375-682 |
| The whole tool against the fake | 10-17k req/s closed loop, about 110 µs of tool CPU per request |
| Real orchestrator and worker, `/status/200` | 310-440 req/s |

On the real stack the worker is the limit (about 6.3 ms of CPU per request, wasm instantiation, against 0.85 ms for the
orchestrator and 0.4 ms for the tool). One instance of the tool drives more than 20 times what this stack can serve.
Cancellation and backpressure are sound: 100 abandoned streams left no goroutines or connections behind, and a consumer
that stops reading holds the producer a few MiB ahead, not the whole response.

## Findings, ranked, with what became of each

3. **Every Connect message was gzipped (awkward). Fixed.** The orchestrator answered gzip on every call, so every head
   (about 50 bytes) and every chunk was compressed: 61% of client allocation per small call came from the gzip reader,
   and switching it off raised throughput 6% on small bodies and 59% at 1 MiB. Every server now compresses only above
   1 KiB (`connect.WithCompressMinBytes`).
4. **The default HTTP client was a silent performance trap (awkward). Fixed.** `http.DefaultClient` keeps two idle
   connections per host: 3,000 requests at a concurrency of 64 used 1,085-1,395 TCP connections, against 69-89 with a
   fitted pool (and 35-40% less throughput). The library now defaults to a client with a pool of 64 idle connections per
   host; `docs/library.md` says how to size your own.
2. **A request ended by the caller's own timer could leave a record with no reason (awkward). Fixed.** When a spinning
   request's budget and the caller's timer are the same value, the orchestrator saw a cancellation a hair before the
   deadline and recorded `error_reason` UNSPECIFIED (in about 3 of 8 repeated runs). A cancellation within 100 ms of
   the deadline is now recorded as `DEADLINE_EXCEEDED` (tested, and the test fails without the change).
5. **The frontend status mapping was not in the library (awkward). Fixed.** Both the in-repo frontend and this probe
   re-derived the same 60 lines from the docs table (the table was complete and correct: conformance passed first
   time). It is now `edgerun.FrontendStatus(err, ctx.Err())`, the table as code with a test row for each line of it,
   and the in-repo frontend uses it. Classification needs `ctx.Err()` as well as `Reason()`, as documented.
12. **Frontend conformance start-up order (docs only). Fixed in the suite.** The suite began testing the moment the
    frontend asked for its limits, so a frontend that called `Describe` before it bound its port failed 39 of 40 cases.
    The suite now also waits until the frontend accepts connections.
6. **"Time to head" is the function's run time (docs only). Fixed.** The worker buffers the response, so head and body
   arrive together (`/sleep/400`: TTFB 407.8 ms against total 408.1 ms). Stated in `protocol.md` and `library.md`.
8. **HTTP/2 cleartext (docs only). Fixed.** The orchestrator serves h2c; the recipe
   (`Transport.Protocols.SetUnencryptedHTTP2(true)`) and the advice (it was slower than pooled HTTP/1.1 at high rates)
   are in `library.md`.
9. **`cmd/orchestrator` had no flag for the largest request body (awkward). Fixed.** `--max-request-bytes`.
11. **The library's frontend example ignored the `io.Copy` error (docs only). Fixed.** It now shows what a mid-body
    failure must do, and mentions `FrontendStatus`.
1. **No cheap way from a response or an error to its invocation record (awkward). OPEN, protocol.** `Invoke` errors
    carry no invocation id though a record is written; `ListInvocations` filters only by function (no get-by-id, no
    `request_id` filter), so the tool scans newest-first, with the admin token (a second scope a frontend does not
    have); records become visible 24-200 ms after the response; which worker and version served a request exists only in
    the record. Related: nothing marks traffic as synthetic (the tool uses a request-id prefix). Suggested: an
    `invocation_id` in the error detail, `GetInvocation` and a `request_id` filter, optionally `worker_id` and `version`
    in the head. Additive protocol changes, not made: for the maintainer to decide.
7. **Thin `Invoke` surface (awkward, minor). OPEN, library.** No per-call headers beyond the context; Connect response
    headers and trailers are hidden (so `Retry-After: 1` is a docs constant); `Response.Header()` rebuilds the header on
    each call; the body reader copies each chunk twice (a `WriterTo` would help). The library's own allocations are about
    4 of 118 per call: the stream reader is not a hot spot, Connect, `net/http` and gzip were.
10. **Test-friendliness (awkward). OPEN, library.** `*edgerun.Ingress` is concrete: there is no seam for an in-process
    fake. In practice the generated handler plus `Authenticate` and `NewError` made a complete fake orchestrator in about
    200 lines served over `httptest`, which worked well. Suggested: a `NewIngressFromClient` or a shipped fake (the probe,
    and the GitOps probe, both found the real-stack harness the biggest cost: a supported throwaway-stack helper would pay
    off). Considered feature creep for now.

## What worked well

1:1 clients, context as deadline and `Reason()` made the status mapping straightforward. Cancellation and backpressure are
sound. The records match what the client saw (id, status, reason for typed errors). `FUNCTION_NOT_FOUND` leaves no
record, exactly as documented. Docs claims checked and found accurate: records appear "shortly after", the 6 MiB
limit and its two refusal paths, the reason-to-status mapping. Go's `net/http` answers `OPTIONS *` itself, so the
frontend rule "never forwarded" comes free.

## The probe's own bugs (for honesty)

A spike window start was skipped when the previous slow interval straddled it; float truncation produced 61 requests at
30 req/s for 2 s; the listener bound its port after `Describe`. All fixed in the tool. HDR percentiles are bucket upper
bounds at three significant digits, so they are up to 0.1% pessimistic against a threshold.
