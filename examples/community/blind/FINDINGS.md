# Findings: a frontend and an orchestrator written from the documents alone

A probe written as a stranger would: an agent that was **not allowed to read any Go source** (only `docs/`, `proto/`,
`README.md` and generated names) implemented a frontend (`frontend/`, about 400 lines) and an in-memory orchestrator
(`orchestrator/`, about 1,200 lines including the egress grammar) in Node.js, then ran the repository's black-box suites
against them. Both pass every group of their suite (frontend 15, orchestrator 36, about 165 subtests), stable over
repeated runs, against the suites as they stood when it ran and again after the changes below.

Before the first pass, the frontend failed 2 cases and the orchestrator 5. All seven were small: two of them rules the
documents stated and the agent missed, three conventions (below), two spec problems. What the exercise is worth is
the list of points where the documents were silent or ambiguous, which the suites did not catch because an
implementation could satisfy them either way. Each point is marked with what was done.

## Resolved in `docs/protocol.md` (and, where marked, in code and suites)

| Point | Resolution |
|---|---|
| "printable ASCII" for `X-Request-Id`: does it include space? | 0x21-0x7E, no space; label values are 0x20-0x7E (space allowed). Both stated. |
| What the orchestrator does with a worker error that has no `ErrorInfo` | `INTERNAL`, generic message, the worker's text only in the orchestrator's own log. Stated. |
| `PatchLabels` with a malformed key to remove | `invalid_argument` (it could never be there). Stated. |
| Whether `host` and `content-length` are also in `HttpRequest.headers` | `Host` is carried in `host` only; every other header, `content-length` included, as received. Stated, and a frontend case now checks both (it found the blind frontend repeating `Host`, which was fixed here). |
| Duplicate `X-Request-Id`; a function that writes its own `X-Request-Id` / `X-Edgerun-Invocation-Id` | First one considered; the platform's value replaces the function's. Stated. |
| A response head with a bad header (control character, non-token name) | 502 as a whole, nothing of the header reaches the client. Stated; the Go frontend now does it (it used to neutralise the value), and the frontend case checks three variants. |
| Trace headers: what "neither" covers; malformed or repeated `traceparent`; starting a trace | Considered only if well formed, first one wins, `tracestate` may ride on the call, none in `HttpRequest.headers`, a frontend need not start a trace. Stated. |
| When the whole-request budget starts | When the call is made (after the request is read; reading is bounded by the frontend's own timeouts). Stated. |
| Missing `Host` | 400 for HTTP/1.1, may forward `host` empty for HTTP/1.0. Stated. |
| Grammar of route host patterns, ABI names, route prefixes; wildcard depth | Host: `*`, `*.suffix` or letters/digits/`.`/`-` (no port, IPv6, underscore); wildcards match any depth, never the apex; ABI: 1-32 of `[a-z0-9.-]` starting alphanumeric; route prefix refuses control characters too (code changed to match). Stated. |
| Error code for limits that depend on stored state; transport caps of single calls | `invalid_argument`; `max_module_bytes` + 1 MiB and `max_request_body_bytes` + 256 KiB. Stated. |
| IPv6 in a coverage; endpoint comparison | Bare IPv6 in a coverage (brackets only in allow entries); endpoints compared as exact text. Stated. |
| Which hash picks "the same worker / service" | Any stable one; stated as the orchestrator's own choice. |
| Page token of another list; `ListInvocations` of an unknown function; page sizes | `invalid_argument` for a token issued for another list; empty list vs `not_found` spelled out; default 50, maximum 500. Stated. |
| Scheduling answers (no workers, none with the ABI, all unreachable) and whether `in_flight` is used | `NO_CAPACITY` / `ABI_MISMATCH` / `NO_CAPACITY`; `in_flight` is information only. Stated. |

## Left as findings

- **Retry rule needs "connection never established" versus "broke after the request was sent".** With `node:http` the
  agent could only approximate it (`ECONNREFUSED`, `ENOTFOUND`... as never-connected; a reset on a reused keep-alive
  socket looks like a mid-request break). The documents could say "a connect-phase failure, and a stale reused
  connection counts as *may have run*". Not changed yet.
- **The request-id echo cannot be total on every HTTP stack.** Node's parser refuses some requests (control bytes, obs-fold,
  duplicate `Content-Length`, a non-ASCII target) before application code sees them, so a fresh id is echoed for those.
  The documents now allow it.
- **Node quirks, not spec problems:** header values above 0x7F survive only if sent with the first body write
  (`flushHeaders()` re-encodes them as UTF-8); `readMaxBytes` is per adapter, so two caps need two adapters.
- **Subtle:** a caller's own timer and the budget it sent downstream are one deadline, so the caller must map its own
  expiry to `DEADLINE_EXCEEDED`; the downstream error and the local abort race. The documents say it; it is easy to get wrong.
- **Not exercised by any suite:** a frontend's classification of an untyped `resource_exhausted` as 413.

The conclusion the agent drew, which I agree with after reading the list: the documents plus the protos were unusually
sufficient for an independent implementation. The holes were edge conventions, not structure.
