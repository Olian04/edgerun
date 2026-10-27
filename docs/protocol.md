# edgerun protocols

edgerun is four cooperating components. They know each other **only** through the
protocols below, so any one of them can be replaced by a different implementation (another
language, another sandbox) as long as it keeps its side of the contract.

```
 client ──HTTP──▶ frontend ──ingress.v1──▶ orchestrator ──worker.v1──▶ worker
                                              ▲   ▲  │
                         admin UI / CLI ─control.v1┘  └─ worker.v1 (Fleet, Artifact: worker dials in)
                                                  ▼
                                              Postgres  (orchestrator only)
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

The orchestrator authenticates workers via the `worker` scope; workers authenticate the
orchestrator on `ExecutionService` with a separate shared secret configured on the worker.
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
| `MAX_REQUEST_BODY_BYTES` | 6 MiB (default; orchestrator reports its actual value in `ingress.DescribeResponse`) | request body |
| Heartbeat interval | 5 s (default; orchestrator dictates it in `RegisterResponse`) | fleet |
| Worker liveness | 3 missed heartbeats | fleet |
| Module size | reported in `control.DescribeResponse.max_module_bytes` | uploads |
| Artifact chunk | ≤ 1 MiB | `GetArtifact` |
| Stderr tail | ≤ 64 KiB, truncated from the front | `ExecutionSummary` |

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

A malformed request (bad digest, bad environment variable name) is `invalid_argument` with no
reason: it is a caller bug, and nothing should branch on it.

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
- v1 has **no outbound network** for guests (preview 1 has no sockets) and the response is
  buffered by the worker before it is streamed on the wire. Neither limits the protocol: both
  can change without a new protocol version, because the wire already streams and an ABI is
  named separately (`wagi-1` stays as is; a richer ABI would be `wagi-2`).

## Conformance

A component implements a role correctly if it passes that role's conformance suite
(`conformance/`, also shipped as `edgerun-conformance --role <role> --url <endpoint>`). The
suites exercise a component only through the wire, so they apply to any implementation.
