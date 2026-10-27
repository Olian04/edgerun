# edgerun protocols

edgerun is four cooperating components. They know each other **only** through the
protocols below, so any one of them can be replaced by a different implementation (another
language, another sandbox) as long as it keeps its side of the contract.

```
 client ──HTTP──▶ frontend ──ingress.v1──▶ orchestrator ──worker.v1──▶ worker
                                              ▲   ▲  │
                         admin UI / CLI ─control.v1┘  └─ worker.v1 (Fleet, Artifact: worker dials in)
                                                  ▼                     │
                                              Postgres  (orchestrator only)     ▼
                                                                  network service ◀─egress.v1─ worker
```

The `.proto` files in [`proto/edgerun`](../proto/edgerun) define the message shapes and are the
source of truth for them. This document defines everything a schema cannot say. Where the two
disagree, that is a bug in one of them.

**Rule of the system:** each seam is one proto package. A component implements one side of a
seam and nothing else. Only the orchestrator talks to Postgres.

| Seam | Package | Served by | Called by |
|---|---|---|---|
| Ingress | `edgerun.ingress.v1` | orchestrator | frontend |
| Execution | `edgerun.worker.v1` `ExecutionService` | worker | orchestrator |
| Fleet, Artifact | `edgerun.worker.v1` `FleetService`, `ArtifactService` | orchestrator | worker |
| Control | `edgerun.control.v1` | orchestrator | admin UI, CLI |
| Egress | `edgerun.egress.v1` | network service | worker |

Shared vocabulary lives in `edgerun.common.v1`.

## Transport

- [Connect](https://connectrpc.com/docs/protocol) over HTTP/1.1 or HTTP/2, `application/proto`
  or `application/json`. gRPC and gRPC-Web clients also work against Connect handlers.
- Only **unary** and **server-streaming** RPCs are used. There is no client or bidirectional
  streaming, so any plain HTTP stack can implement a peer.
- The RPC deadline (`Connect-Timeout-Ms`) is the wall-clock budget of an invocation. Every hop
  must propagate it and enforce it: a worker must actually stop the guest and free its slot,
  not merely stop waiting for it. A caller whose own deadline expires first must treat that as
  `DEADLINE_EXCEEDED`: Connect clients derive the wire deadline from the call's context, so the
  caller's timer and the downstream's budget are the same value and the downstream's typed
  error is not guaranteed to arrive first.

## Authentication

Every request carries `Authorization: Bearer <token>`. A token has one scope:

| Scope | Allows |
|---|---|
| `ingress` | `IngressService` |
| `worker` | `FleetService`, `ArtifactService` (when called *on* the orchestrator) |
| `admin` | everything in `edgerun.control.v1` |
| `egress` | `EgressService` (when called *on* a network service) |

The orchestrator authenticates workers via the `worker` scope; workers authenticate the
orchestrator on `ExecutionService` with a separate shared secret configured on the worker; a
network service authenticates workers via the `egress` scope.
Missing or invalid credentials fail with `UNAUTHENTICATED`; a valid token of the wrong scope
fails with `PERMISSION_DENIED`. TLS or mTLS is expected between components in production; the
token scheme does not replace it.

## Versioning and compatibility

- The major version is part of the package name (`v1`). A breaking change means a new package
  (`v2`) served alongside `v1`, never an edit.
- CI runs `buf lint` and `buf breaking` (category `FILE`) against `main`.
- Peers negotiate by **declared capability, never by implementation name**: protocol packages
  (`Implementation.protocols`) and, for workers, guest ABIs (`WorkerInfo.abis`).

## Limits fixed by the protocol

| Constant | Value | Where |
|---|---|---|
| `MAX_REQUEST_BODY_BYTES` | 6 MiB (default; orchestrator reports its actual value in `ingress.DescribeResponse`) | request body; see below |
| Heartbeat interval | 5 s (default; orchestrator dictates it in `RegisterResponse`) | fleet |
| Worker liveness | 3 missed heartbeats | fleet |
| Module size | reported in `control.DescribeResponse.max_module_bytes` | uploads |
| Artifact chunk | ≤ 1 MiB | `GetArtifact` |
| Stderr tail | ≤ 64 KiB, truncated from the front | `ExecutionSummary` |

A frontend **must** enforce `max_request_body_bytes` (from `Describe`) itself and answer 413
without calling the orchestrator. The orchestrator also checks: a body over the limit but within
the transport's message cap is refused with `REQUEST_TOO_LARGE`; a body far over the limit is
refused by the transport's message cap before it is decoded, with a bare `resource_exhausted` and
no `ErrorInfo`. A frontend that receives that untyped error from `Invoke` must treat it as a 413
too, never as a 500.

## Invocation semantics

1. **Frontend → orchestrator.** The frontend sends `HttpRequest` (host, path, method, headers,
   body) via `Invoke`. It has no knowledge of functions or workers.
2. **Routing** is the orchestrator's: among the routes matching both host and path, the most
   specific host wins, then the longest path prefix on whole segments (see `Route` in
   `model.proto`); a specific host with no matching prefix falls through to less specific
   hosts. No match → `FUNCTION_NOT_FOUND`. The Host header is compared lower-cased, without
   port or trailing dot. Paths are matched as given, not cleaned.
3. **Scheduling.** The orchestrator picks among workers that are alive, `READY`, and list the
   module's ABI, preferring the same worker for the same module (warm cache).
4. **Retry rule.** The orchestrator retries on `NO_CAPACITY`, and when a worker could not be
   reached at all (the connection was never established, so the request cannot have run), on the
   next candidate, at most 3 attempts. It never retries after the response head was received,
   and never retries `FUNCTION_TRAPPED` or `DEADLINE_EXCEEDED`, or a connection that broke
   after the request was sent, because the function may have had side effects. A worker that
   could not be reached is skipped for a few seconds.
5. **Capacity is never queued.** A worker with no free slot answers `NO_CAPACITY` immediately.
6. **Response stream:** head, then chunks. A worker stream ends with exactly one
   `ExecutionSummary`; ending without one is a failed invocation.
7. **Failure after the head** terminates the stream with an error. The frontend must abort the
   client connection instead of finishing the body normally.

### Frontend mapping of platform errors

A function answering with any HTTP status, including 500, is a **successful** invocation and is
forwarded verbatim. Platform errors (`common.v1.ErrorReason`) map as:

| Reason | HTTP | Notes |
|---|---|---|
| `FUNCTION_NOT_FOUND` | 404 | |
| `NO_CAPACITY` | 503 | `Retry-After: 1` |
| `DEADLINE_EXCEEDED` | 504 | |
| `FUNCTION_TRAPPED` | 502 | |
| `ABI_MISMATCH` | 502 | no worker can run it: operator problem |
| `REQUEST_TOO_LARGE` | 413 | |
| `RESPONSE_TOO_LARGE` | 502 | |
| `ARTIFACT_NOT_FOUND` | 502 | |
| `INTERNAL`, unknown | 500 | |

Error messages are for operators. The frontend never forwards `ErrorInfo.message` to clients.
A `FUNCTION_TRAPPED` message SHOULD end with the tail of the guest's stderr (at most 4 KiB),
because a failed invocation has no `ExecutionSummary` to carry logs.

The two `EGRESS_*` reasons occur on the egress seam only. A function never fails an invocation with
them (the guest is told, see below), so the frontend has no mapping for them.

Every reason has one canonical Connect code. Peers branch on the reason, never on the code:

| Reason | Connect code |
|---|---|
| `FUNCTION_NOT_FOUND`, `ARTIFACT_NOT_FOUND` | `not_found` |
| `NO_CAPACITY`, `RESPONSE_TOO_LARGE` | `resource_exhausted` |
| `DEADLINE_EXCEEDED` | `deadline_exceeded` |
| `FUNCTION_TRAPPED` | `aborted` |
| `ABI_MISMATCH` | `failed_precondition` |
| `REQUEST_TOO_LARGE` | `invalid_argument` |
| `INTERNAL` | `internal` |
| `EGRESS_DENIED` | `permission_denied` |
| `EGRESS_FAILED` | `unavailable` |

A malformed request (bad digest, bad environment variable name) is `invalid_argument` with no
reason: it is a caller bug, and nothing should branch on it.

## Frontend behaviour

A frontend serves HTTP and is an `ingress.v1` client; nothing calls it but the outside world. What
makes one replaceable is that every frontend maps HTTP onto the protocol the same way.

**Request.**

- It reads `max_request_body_bytes` from `Describe` when it starts and again from time to time (6 MiB
  until the first answer), and answers 413 without calling `Invoke` to a request whose body is larger.
  That includes a body of undeclared length, which it must stop reading at the limit.
- A request target that is not a path starting with `/` (`OPTIONS *`) is never forwarded: the frontend
  answers it itself, with a 2xx or a 4xx.
- `HttpRequest` carries the method; `host` as the `Host` header arrived; `path` and `raw_query` as
  received (the query without its `?`), neither cleaned nor re-encoded; the body; and `remote_addr`,
  the peer's `ip:port`.
- Header names are lower-cased and every value kept, in order. Headers that describe one connection
  and not the message are dropped: `connection`, `keep-alive`, `proxy-authenticate`,
  `proxy-authorization`, `proxy-connection`, `te`, `trailer`, `transfer-encoding`, `upgrade`.
- Who the client is comes from the connection, not from what the client claims: client-supplied
  `x-forwarded-for`, `x-forwarded-proto`, `x-forwarded-host`, `forwarded` and `x-real-ip` are
  discarded, and the frontend sets `x-forwarded-for` (the peer's IP), `x-forwarded-proto` and
  `x-forwarded-host` itself. A frontend may offer a mode for running behind a proxy it trusts, in
  which those headers are kept and appended to; that mode is off by default.
- `request_id` is the client's `X-Request-Id` if it is printable ASCII of at most 128 characters,
  otherwise a fresh unique value. It is echoed in the `X-Request-Id` header of every response,
  successful or not.
- `Invoke` is called with the frontend's whole-request budget as its deadline, with the ingress
  token, and is cancelled if the client goes away.

**Response.**

- The head's status must be 200–599; anything else is a platform fault and is answered 502. Headers
  are forwarded as given, repeated ones included, except the connection-level ones above and
  `content-length`: the frontend frames the response itself.
- Chunks are written to the client, and flushed, as they arrive.
- A function's own error statuses, 500 included, are forwarded as they are.
- A platform error before the head is answered with the status in the table above, a generic body
  and `Retry-After: 1` where the table says so. An error with no `ErrorInfo` is classified by what
  can be seen: an expired deadline is 504; a cancelled call needs no answer; `resource_exhausted` is
  413 (the transport's size cap); `unavailable` (the orchestrator cannot be reached) is 503 with
  `Retry-After: 1`; `unauthenticated` and `permission_denied` (the frontend's own credentials) are 502;
  anything else is 500.
- A stream that ends cleanly without a head is 502.
- A stream that fails *after* the head has been sent cannot change its status. The frontend aborts
  the client's connection, so the client sees a failed transfer and not a complete, truncated
  response.

## Control semantics

`control.v1` is how operators change what the orchestrator serves. The proto comments say what each
call is; this is what they do not.

**Errors.** Control calls fail with plain Connect codes, never an `ErrorReason` (that vocabulary is
for invocations):

| Code | When |
|---|---|
| `invalid_argument` | a field is malformed or out of range, or a page token is not one the server issued |
| `not_found` | the function, version, route or environment variable does not exist. An id that is not a well-formed UUID names nothing, so it is `not_found` too, not `invalid_argument` |
| `already_exists` | a function name, or a route's `(host_pattern, path_prefix)`, is taken |

**Names and values.**

- A function name is 1–63 lower-case letters, digits or `-`, not starting or ending with `-`. Names
  are unique.
- An environment variable name is `[A-Za-z_][A-Za-z0-9_]*`. Names the guest ABI sets itself
  (`GATEWAY_INTERFACE`, `SERVER_PROTOCOL`, `REQUEST_METHOD`, `SERVER_NAME`, `PATH_INFO`,
  `QUERY_STRING`, `REMOTE_ADDR`, `CONTENT_LENGTH`, `CONTENT_TYPE`) and anything starting `HTTP_` are
  refused. A function holds at most 64 variables, each value at most 32 KiB and free of NUL.
  `SetEnv` creates or replaces. `DeleteEnv` of a variable that is not set is `not_found`.
- `ListEnv` returns variables ordered by name and returns **secret values empty**: secrets are
  write-only. A secret is still handed to the worker at invocation time, in clear.
- A route's host pattern is lower-cased on write: an exact host, `*.suffix`, or `*`. Its path prefix
  must start with `/`, may not contain `//`, `?`, `#` or whitespace, and loses a trailing `/` (so
  `/api` and `/api/` are one route; `/` stays). Routes are returned in the normalised form.
- `module` must not be empty or exceed `max_module_bytes`, and `abi` is a short lower-case name
  (`wagi-1`). Whether any worker supports the ABI is not checked here: that is a scheduling matter,
  reported at invocation time as `ABI_MISMATCH`.

**Versions** are immutable and numbered 1, 2, 3… per function, never reused. `CreateVersion` stores
the module under its content address `sha256:<64 hex>`; identical bytes share one artifact.
`ListVersions` returns newest first. `ActivateVersion` of a version that belongs to another
function (or none) is `not_found`; activating is also how a rollback is done. A function with no
active version answers invocations with `FUNCTION_NOT_FOUND`. `DeleteFunction` also removes its
versions, routes and environment, but **not** its invocation records.

**Pages.** `page_size` 0 means the server's default; a server may clamp a large value. A page
shorter than requested does not mean it is the last: only an empty `next_page_token` does.
Following the tokens yields every item exactly once while nothing is written. Functions are ordered
by name, routes by `(host_pattern, path_prefix)`, invocations newest first.

**Read your writes.** When a call that changes routing (a route, an activation, an environment
variable, a deleted function) returns, an `Invoke` sent to *the same orchestrator endpoint* behaves
as if the change had been made. Other orchestrator instances may take a few seconds to catch up.

**Invocation records** are written for every invocation that was routed to a function version, including
ones that then failed (`NO_CAPACITY`, `ABI_MISMATCH`, …), but not for `FUNCTION_NOT_FOUND` (no route, or
no active version): the request's `request_id`, the function, version
and worker, `status` (0 if no response head was produced), the `error_reason` if it failed, and the
stderr tail (for a failure that has no summary, the error's message). `duration_micros` and
`peak_memory_bytes` are what the worker reported in its summary. Records appear shortly after the
invocation, not necessarily before the call returns.

## Fleet and artifact semantics

Workers dial in to the orchestrator (`FleetService`, `ArtifactService`, scope `worker`).

- `Register` needs an `http(s)` endpoint and a `WorkerInfo` with at least one ABI and a positive
  `max_concurrency`; anything else is `invalid_argument`. It returns a fresh `worker_id` and the
  heartbeat interval the worker must keep. Registering an endpoint that is already known replaces
  that entry (the worker restarted).
- `Heartbeat` states are `READY` or `DRAINING` (anything else is `invalid_argument`). For an unknown
  `worker_id` it answers `reregister = true`, and the worker must register again.
- A worker that has not heartbeated for three intervals is not alive and receives no work. One that
  reports `DRAINING` receives no new work either, and stays listed.
- `Deregister` is idempotent: an unknown id is not an error.
- `GetArtifact` streams the module in chunks of at most 1 MiB. A digest that is well-formed but
  unknown is `ARTIFACT_NOT_FOUND`; one that is not a digest at all is `ARTIFACT_NOT_FOUND` as well,
  since nothing is stored under it.

## Network access (egress)

A guest has no network. The worker gives it none: there is no socket it could open. What a guest
can do, if the ABI offers it (`wagi-2`, below) and its function has been opened up, is ask the
worker to make an HTTP request for it, and the worker hands that request to a **network service**
over `egress.v1`. The service is the one place that decides whether a request may leave, and the one
that makes it. A deployment can put it where the network is, apart from where code runs, and can
replace it with any implementation of the protocol.

**Closed by default.** A function reaches nothing until an operator lists destinations for it
(`control.v1` `SetEgress`). The list is part of the function, not of a version: changing it needs no
deploy and applies to the next invocation. The orchestrator hands the list to the worker in
`ExecuteRequest.egress_allow`, and the worker passes it on in every `Fetch`. A worker makes no call
at all when the list is empty.

**Allow-list entries** are host patterns, written in lower case (anything else is lower-cased):

| Entry | Matches |
|---|---|
| `api.example.com` | that host, on the scheme's default port (80 for `http`, 443 for `https`) |
| `api.example.com:8443` | that host, on that port only (naming a port replaces the default) |
| `*.example.com` | any name under it, however deep, but not `example.com` itself |
| `192.0.2.7`, `[2001:db8::1]:8080` | that address (IPv6 in brackets); no wildcards for addresses |

Host names are ASCII (internationalised names in punycode). A trailing dot and case are not part of
the name. A bare `*` is not an entry: a list names destinations. A list holds at most 64 entries and
is stored lower-cased, deduplicated and sorted; a malformed entry is `invalid_argument`.

**What a network service does with a `Fetch`**, in this order, stopping at the first failure:

1. Refuses a malformed request with `invalid_argument` (no reason): the method is an HTTP token, the
   URL is absolute with scheme `http` or `https`, has a host, no user information and an ASCII host,
   and every `allow` entry is well formed. A body larger than `max_request_body_bytes` is
   `REQUEST_TOO_LARGE`.
2. Checks the URL against `allow` (scheme, host, port). No match is `EGRESS_DENIED`. This happens
   before any name is resolved, so a refused request reveals nothing about the network.
3. Resolves the host and refuses, with `EGRESS_DENIED`, any destination the *service itself* never
   reaches, whatever the caller allowed: loopback, unspecified, link-local (cloud metadata services
   live there), private (RFC 1918, unique-local), carrier-grade NAT, multicast, broadcast, reserved,
   documentation and benchmarking ranges, and IPv4 addresses embedded in IPv6 forms. This is checked on the
   address actually connected to, for every address the name resolves to, so a name that resolves
   somewhere forbidden (or changes its answer between lookups) gets nowhere. A deployment may opt
   particular networks in; the default opts in none.
4. Makes the request. It sets `Host` from the URL (a caller-supplied `Host` is dropped, or an
   allowed name could be used to reach another virtual host), drops connection-level headers
   (`connection`, `keep-alive`, `proxy-*`, `te`, `trailer`, `transfer-encoding`, `upgrade`), neither
   adds nor undoes content encodings, and does not follow redirects. A certificate that does not
   verify is a failure, not a warning.
5. Streams the response: the head, then chunks of at most 1 MiB, with connection-level headers
   removed. An upstream 4xx or 5xx is a response and is forwarded as it is.

Failures are `EGRESS_DENIED` (steps 2 and 3), `EGRESS_FAILED` (no address, connection refused, TLS
failure, upstream hang-up, including after the head, in which case the stream ends with the error),
`RESPONSE_TOO_LARGE` (the body exceeded `max_response_bytes` or the service's own maximum),
`DEADLINE_EXCEEDED` (the RPC deadline) and nothing else. Cancelling the call abandons the upstream
request.

**What a worker does.** It speaks `egress.v1` to the service it is configured with, presenting the
`egress` token, passing its remaining invocation budget as the deadline. A worker that has no
network service configured does not advertise `wagi-2`, and the orchestrator then never schedules
`wagi-2` modules onto it. The worker enforces, before any call, the limits in the ABI below.

## Guest ABI `wagi-1`

The only ABI in v1. It follows the CGI-style convention of [WAGI](https://github.com/deislabs/wagi),
on **WASI preview 1** (`wasip1`). Anything that compiles to a preview-1 command module
(`_start` entrypoint) works, e.g. `GOOS=wasip1 GOARCH=wasm` Go or Rust `wasm32-wasip1`.

**Request** — one fresh instance per invocation:

- stdin: the request body, then EOF.
- Environment variables:

| Variable | Value |
|---|---|
| `GATEWAY_INTERFACE` | `CGI/1.1` |
| `SERVER_PROTOCOL` | `HTTP/1.1` |
| `REQUEST_METHOD` | method |
| `SERVER_NAME` | the request's `Host` |
| `PATH_INFO` | path |
| `QUERY_STRING` | raw query, no `?` |
| `REMOTE_ADDR` | client address |
| `CONTENT_LENGTH` | body length in bytes |
| `CONTENT_TYPE` | request `content-type`, if any |
| `HTTP_<NAME>` | each other header, upper-cased, `-` → `_`; repeated headers joined with `, ` |

User-defined environment variables are added after these. Names colliding with the table, or
starting with `HTTP_`, are rejected when the variable is set.

**Response** — written to stdout:

```
Status: 201 Created        ← optional; defaults to 200
Content-Type: text/plain   ← any number of headers, `\n` or `\r\n` line ends
                           ← blank line ends the headers
<body bytes…>
```

- The exit code must be 0. Anything else, a trap, or exceeding the memory limit is
  `FUNCTION_TRAPPED`. Output (headers and body together) beyond `Limits.max_response_bytes`
  is `RESPONSE_TOO_LARGE`; the guest is stopped as soon as it crosses the limit.
- Output that is not a valid response (no output, headers not ended by a blank line, a bad
  `Status`) is `ABI_MISMATCH`. So is a module that is not a WASI command (no `_start`) or that
  imports anything other than `wasi_snapshot_preview1`, checked when the module is first loaded.
- The guest sees the **real** wall clock, monotonic clock and a cryptographically secure random
  source. Sleeping is real, counts against the deadline, and ends at the deadline.
- Memory limits are enforced per invocation but may be rounded up to a size the worker supports
  (the reference worker uses 16, 32, 64, 128, 256 and 512 MiB). Workers may clamp limits to their
  own maximum.
- stderr is captured as the invocation log (truncated tail).
- `wagi-1` has **no outbound network** for guests (preview 1 has no sockets); `wagi-2` adds it, under
  an allow list, through a network service. The response is buffered by the worker before it is
  streamed on the wire. Neither limits the protocol: both can change without a new protocol version,
  because the wire already streams and an ABI is named separately.

## Guest ABI `wagi-2`

`wagi-2` is `wagi-1` plus outbound HTTP. Everything above under `wagi-1` holds unchanged; a module
built for `wagi-2` may import one more module, `edgerun_http`, besides `wasi_snapshot_preview1`.
(A `wagi-1` module that imports it is refused, as a module that imports anything else is.)

```
fetch(req_ptr: i32, req_len: i32) -> i32
fetch_read(dst_ptr: i32, dst_len: i32) -> i32
```

`fetch` reads a request from guest memory, sends it, waits for the whole response, and keeps the
response for the guest to read. It returns the response's size in bytes, or a negative error:

| Result | Meaning |
|---|---|
| `n ≥ 0` | success: the response is `n` bytes long |
| `-1` | denied: the destination is not on the function's allow list, or the network service refuses it |
| `-2` | failed: unreachable, TLS failure, upstream hang-up, or any other fault |
| `-3` | too large: the response exceeds the limit |
| `-4` | invalid: the request text is malformed, or too large |
| `-5` | limit: the invocation has already made 16 requests |

`fetch_read` copies up to `dst_len` bytes of the pending response, from where the previous call
stopped, and returns how many it copied (0 at the end). A later `fetch` discards what is left.

The request and the response are text in the same convention as a CGI response:

```
POST https://api.example.com/v1/items?x=1     ← METHOD SP URL
Content-Type: application/json                ← any number of headers
                                              ← a blank line ends the headers
{"name": "x"}                                 ← the body, to the end
```

The blank line may be left out when there is no body.
```
Status: 201                                   ← always present
Content-Type: application/json
                                              ← a blank line
<body bytes…>
```

A request is at most 1 MiB and a response at most 4 MiB (smaller if the function's response limit
is). Requests run one at a time, count against the invocation's deadline, and are abandoned when it
ends. Whether a destination is allowed is the worker's and the network service's to say; the guest
only sees `-1`.

## Conformance

Every role that implements a protocol surface has a conformance suite in [`conformance/`](../conformance).
A suite is black-box: it starts the implementation under test, plays every other role itself using the
generated protocol code and nothing else, and judges only what crosses the wire. An implementation that
passes its role's suite can replace any other that passes.

| Role | Suite | Implements | The suite plays |
|---|---|---|---|
| frontend | `conformance/frontend` | an HTTP edge over `ingress.v1` (as a client) | the orchestrator, and an HTTP client |
| orchestrator | `conformance/orchestrator` | `ingress.v1`, `control.v1`, `FleetService`, `ArtifactService` (as a server); `ExecutionService` (as a client) | frontend, operator, and a fleet of workers |
| worker | `conformance/worker` | `ExecutionService` (server); fleet, artifact and egress (as a client) | the orchestrator and the network service |
| network service | `conformance/egress` | `EgressService` (server) | a worker, and the web servers it reaches |

The admin UI and the CLI only call `control.v1`; they implement no surface of their own, and so have no
suite. What they must be able to do is what the orchestrator suite proves the orchestrator offers.

A suite is a Go package: `Run(t, Target)` takes a `Target` whose `Launch` starts the implementation, in
whatever way suits it (a goroutine, a subprocess, a container), and returns where to reach it. The in-repo
components are run through the same `app` packages their `cmd/` binaries use; see the `conformance_test.go`
beside each. Running a suite against an implementation in another language means writing a `Launch` that
starts it; there is no stand-alone runner yet.

The orchestrator suite needs an orchestrator that is empty and used by nothing else. A few behaviours cannot
be judged from outside and are covered by the reference implementation's own tests instead: for example, that
a network service refuses a name when *any* of the addresses it resolves to is forbidden, which needs control
of DNS.
