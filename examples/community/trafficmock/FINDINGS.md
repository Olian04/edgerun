# Findings: a mock traffic ingress built on the edgerun library

A probe, written by an agent acting as a community developer: `edgerun-trafficmock`, a frontend-role program that
synthesises or replays traffic into an orchestrator over `ingress.v1`, to load-test and soak a deployment. Each item
says what became of it in this repository.

What it is: `run` (YAML or JSON scenarios: request templates, constant, ramp, spike and burst, closed-loop and trace
replay load models; HDR histograms of time to head and total time; status as a real client would see it, with platform
errors counted by `ErrorReason`; mid-body failures counted apart; `--expect 'p99<200ms'` assertions with exit codes;
a Prometheus endpoint), `listen` (a minimal frontend, real HTTP in and `ingress.v1` out, that can record a replayable
trace) and `fake` (a pretend orchestrator built on the generated handler, `Authenticate` and `NewError`).

Tests: `go test -race ./...`, no races: constant, ramp, spike, closed-loop and replay scenarios
against a real PostgreSQL, orchestrator and worker (counts checked against an independent count of orchestrator
records; a 503 from a function is a status, not an error; `FUNCTION_NOT_FOUND` is 404 with no record;
`DEADLINE_EXCEEDED` 504; `FUNCTION_TRAPPED` 502; `NO_CAPACITY` 503 in about 10 ms under a burst), a fake orchestrator
for mid-body failures, leaks, connection reuse and backpressure, and the frontend conformance suite against `listen`
(45 passed).

## Three ingresses at once: is every response delivered to the instance that made the request?

Answer from the evidence: yes, in every run. Nothing was ever delivered to the wrong instance, and no response was
ever the wrong content. What the test also found is a real defect that has nothing to do with misdelivery
(finding 13 below).

The tool now proves delivery by content (it used to measure status and timing only). A template can stamp every
body with its own request id and the run id (`body: {stamp: true, size_min: .., size_max: ..}`: `tm1 <request id>
<run id>\n`, then padding of the configured size and kind), and `verify: echo` (or `echo-with-id`: the id, a
newline, then the body) compares the response with the request byte for byte while it streams, with no buffering;
`verify_header: x-echo-request-id` also checks that a header the guest echoes back carries the id that was sent.
Counted per instance as `echo_mismatches`, `echo_body_mismatches`, `echo_header_mismatches` and `echo_checked`
(text, JSON, Prometheus; `--expect 'echo_mismatches==0'`), with a bounded sample (50) of the mismatched request ids,
what was received from the first differing byte, and, if the response carries another request's stamp, whose.

`multi_ingress_test.go` (real processes: PostgreSQL, orchestrator, 3 workers of 6 slots, a guest that echoes the body and
the request id it was given): edgerun's own frontend (`./cmd/frontend`, driven by an HTTP client with stamped bodies of
a few bytes to 1 MiB) and two separate processes of this tool (closed loop; open loop with spikes) all send to ONE
function on ONE host for 25 s. Per request the Go client and the tool check the echo, `X-Request-Id`, the echoed id
header and the invocation id; afterwards every invocation record of the orchestrator (control.v1, admin token) is
matched to the instance by request-id prefix, and each (request id, invocation id) pair an instance saw must be the pair
in the record (with its path, status and error reason). 25 s runs, 8 of each of the two scenarios (3 of them with
`-race`, the tool processes included): 6,000-6,300 requests (5,000-5,300 under `-race`; frontend about 3,200; mock-a 1,800-1,900; mock-b 1,135),
every response verified, **0 mismatches, 0 records of one instance on another's request, 0 duplicate invocation ids,
records = requests sent**, about 5,800 (request id, invocation id) pairs checked per run. The peaks in flight (10, 6 and
13-16) add up to 29-32 against 18 slots; about 6% of the requests ended `NO_CAPACITY` (5-7%), and a counting TCP
forwarder between the orchestrator and each worker saw 11,300-11,700 Execute attempts for about 6,000-6,300
invocations: about 5,300 retries between workers (the scheduler prefers one worker for one module, so most requests are refused once or twice).
The second scenario sleeps a random time up to 300 ms before echoing (responses return out of order) and SIGKILLs the
busiest worker at +40% of the run: 4,200-4,700 requests, 2,400-2,450 verified pairs, 6-11 failures per run, every one
on the killed worker according to its record; the other requests verify. No failure ever carried content.

13. **A killed worker can make the orchestrator say "cancelled" to a caller who cancelled nothing; the frontend then
    answers `200 OK` with an empty body (BUG, orchestrator and frontend; FIXED).** Reproduced in
    `TestWorkerKillUnderLoadRepro` (about two runs in five) and seen in 7 of the 8 25 s kill runs (0-6 requests a run): when a worker dies, a
    request on it ends with `read tcp ...: connection reset by peer`; connect-go reports that as `CodeCanceled`, and
    `invoke.translate()` (`errors.Is(err, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled`) takes
    it at its word, returning `canceled` with no `ErrorInfo` (the record has status 0 and no error reason). The
    frontend's `FrontendStatus` maps cancelled to 0, "no answer needed", writes nothing, and `net/http` sends
    `200 OK` with no body and no `X-Edgerun-Invocation-Id`. The client is told its request succeeded with an empty
    response. Not another request's content, so not misdelivery, but a silent wrong success. The same request that
    dies with `FIN` instead of `RST` is a proper 500. Suggested: in `translate`, treat a cancellation as the caller's
    only when `ctx.Err() != nil`, else INTERNAL (or NO_CAPACITY: the invocation may not have run), and in the
    frontend answer 500 when the request context is not cancelled whatever the error says. This tool no longer
    counts such an error as "canceled" (it is `UNTYPED_CANCELED_BY_PEER`, 500) and its `listen` mode no longer
    writes nothing.
    **Fixed as suggested** (the orchestrator treats a cancellation as the caller's only if the caller's context ended;
    `FrontendStatus` answers 0 only when the request's own context was cancelled; both tested, each test failing
    without its fix; `protocol.md` says so). After the fix `TestWorkerKillUnderLoadRepro` asserts it never happens and
    passed 8 runs in a row (callers whose worker died were told a typed 500 or `INTERNAL`), where it had reproduced
    about two runs in five; the full module, both three-instance scenarios included, passes.
14. **Invocation records do not say how many workers were tried, and retries are invisible (awkward, observability).**
    `NO_CAPACITY` retries between workers happen thousands of times a minute under this load and appear nowhere: no
    log, no counter, `worker_id` of the record is the last attempt. The test counts them with a TCP forwarder in
    front of the workers. A retry counter or `attempts` on the record would make saturation visible. **Partly addressed:** the orchestrator now
    logs each retry at debug level (invocation, attempt, worker, reason). A counter or an `attempts` field on the record is
    a protocol and metrics decision, not made.

(An earlier version of the test put `httputil.ReverseProxy` between the orchestrator and the workers to count
attempts: about one run in four then had an invocation torn mid-body ("incomplete envelope: unexpected EOF"). 22 runs
without the proxy, and 10 with a plain TCP forwarder, had none: the reverse proxy was the cause, not edgerun. Noted
because it looked like a platform failure at first.)

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
