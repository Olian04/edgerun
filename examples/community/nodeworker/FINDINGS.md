# Findings: building an edgerun worker in plain Node.js

A probe, written by an agent acting as a JS-only developer, from `proto/` and `docs/protocol.md` alone (the Go
library is no use to it). Each item says what became of it in this repository.

**Answer: yes, a JS-only developer can build a conforming worker from the protos and docs.** The hard part was
`node:wasi`, not the protocols.

What was built (about 950 lines of ESM in `src/`): `ExecutionService`, registration and heartbeats, artifact fetch
with digest verification and a cache, immediate `NO_CAPACITY`, bearer auth, typed errors, and wagi-1 guests in a
`worker_threads` Worker per invocation with the deadline enforced by terminating the thread. Stubs from `buf` and
`protoc-gen-es` 2.x with Connect-ES 2.3. It advertises wagi-1 only, so the suite's Network group is skipped.

`npm test`: 33 of 33 (real orchestrator and PostgreSQL processes). Conformance executable: all groups pass, Network
skipped; three consecutive runs. Re-verified after the changes below, including the new `MalformedRequests` case. The
current numbers, after "Round 2", are in that section.

## Findings

Platform (Node/WASI), nothing for edgerun to fix:

- **P1, blocker without a workaround.** `node:wasi` blocks natively in `poll_oneoff`, so `terminate()` cannot stop a
  sleeping guest (terminate at 500 ms returned at 5066 ms). Replaced `poll_oneoff` with an `Atomics.wait` version:
  terminate then returns at about 505 ms.
- **P2.** `node:wasi` stdio is file-descriptor only; `fd_read` and `fd_write` are overridden to capture in memory and to
  stop the guest the moment the output limit is crossed.
- **P3.** WebAssembly has no host cap on a module's linear memory. `wasmpatch.js` rewrites the memory section's maximum
  after the sha256 check so `memory.grow` fails inside the guest. Linear memory only; no cgroup behind it.
- **P4.** `node:wasi` is experimental; the module must export `memory`; about 60 ms per invocation, 290 ms for the
  first; a guest thread is not a sandbox boundary; guest stderr is lost on a deadline kill.

Suite, docs and reference worker (all in this repository):

- **S1, suite bug. Fixed.** `Capacity` with `--max-concurrency 1` accepted a stale heartbeat from the warm-up request as
  "all slots busy", and the probe was then admitted. The Go reference worker failed it too (and the old logic failed 6 of 6
  runs with one slot). Now only heartbeats after the sleepers launch count, and the in-repo run covers one slot.
- **D1, docs. Fixed.** The docs said a valid token of the wrong scope is `PERMISSION_DENIED`, but the scope table had no
  execution row and the suite correctly expects `UNAUTHENTICATED` for the worker's own `worker` token. A server only
  knows the tokens it is configured with; the table now has `execute` and says so.
- **D2, reference worker deviates from the docs. Fixed.** A bad environment variable name is `invalid_argument` with no
  reason per the docs; the Go worker returned `INTERNAL`. The worker is fixed, and a `MalformedRequests` case now makes the
  suite able to see it (confirmed by mutation: it fails against the old behaviour). The Node worker, written from the docs,
  passes it.
- **D3 to D5, docs. Fixed.** Rules that were only in the suite or the reference implementation are now in a "Worker
  execution semantics" section: tampered artifact is `INTERNAL`, non-wasm bytes are `ABI_MISMATCH`, a draining worker answers
  `NO_CAPACITY`, the full ordering of checks, guest status range 200-599, `CONTENT_LENGTH` is measured and
  `HTTP_CONTENT_LENGTH` is not emitted, and the reference's default limits and budget are observable but not protocol.
- **T1, tooling. Fixed (docs).** No registry module, package or README for non-Go stubs; `buf.gen.yaml` is Go-only. Added
  `docs/other-languages.md` with the template that worked first time, offline.
- **T2, tooling. Open.** The conformance executable and the orchestrator are not distributed, so a non-Go developer needs the
  Go toolchain. Release binaries would fix it; not done.
- **S2, suite. Open.** The suite cannot tell detection from enforcement for memory: the first version of this worker,
  which checked after the fact, passed `MemoryLimit` without enforcing anything. Also not failable today: "stopped as soon
  as it crosses the limit" (the test output is finite), slot release after a client cancel, `PERMISSION_DENIED`.

## Round 2: "HTTP messages" (header values are bytes)

`common.v1.Header.value` became `bytes`, and `docs/protocol.md` got an "HTTP messages" section. Migrated and re-run:
`npm test` 37 of 37; conformance executable (`--role worker`), every group passes, Network skipped, three consecutive runs,
including the new `HTTPMessages` group and `MalformedRequests`. What it cost a JS-only worker:

- **Byte-exact environment: possible, inside Node, without leaving `node:wasi`.** `new WASI({ env })` takes JS strings and
  encodes them as UTF-8, so the bytes `E9 FF` cannot be expressed: not with a `latin1`-decoded string either (it would
  be encoded again, to `C3 A9 C3 BF`). But `wasi.wasiImport` is a plain object and the import object is built from it,
  so the worker overrides `environ_sizes_get` and `environ_get` next to the `fd_*` and `poll_oneoff` overrides it already
  had (P1, P2) and serves the environment itself, from `Buffer`s (`NAME=` + value bytes + NUL). `node:wasi` gets `env: {}`.
  The environment travels to the guest thread as a list of byte strings; `buildEnv` returns `Map<string, Buffer>` (names
  and the ABI's own values as UTF-8, header values untouched, `, ` joined as bytes). About 20 lines. The cost is the usual one
  of re-implementing a piece of `node:wasi`: the worker now owns the layout rules of `environ_get`. A guest that never calls
  it is unaffected; one that does gets exactly what the Go suite asserts (`HTTP_X_JUNK=\xff\xfe\x80`).
- **Response side: the old parser decoded.** It read a header line as Latin-1 and `trim()`med it, then re-decoded as UTF-8:
  that turns `caf\xe9` into U+FFFD, and `String.prototype.trim` also strips U+00A0 (the `X-Nbsp` case: bytes `C2 A0`
  read as Latin-1 end in U+00A0). The rewrite works on the `Buffer` throughout, trims only 0x20 and 0x09, and answers
  `ABI_MISMATCH` for any control character in a value (C0 other than tab, and DEL). A bare CR in a line is one too (CRLF is
  still a line end).
- **Request validation** (`malformedRequest`): path non-empty, starts with `/`, no control character; query without control
  characters; header name an RFC 9110 token; value without a control character other than tab. `invalid_argument`, no reason.
  I also refuse a NUL in `method`, `host` and `remote_addr`: the spec does not list them, but they become environment
  variables and a NUL would silently truncate them for the guest.
- **A path that is not UTF-8 on the wire is `internal` in Connect-ES, not `invalid_argument`.** protobuf-es validates proto3
  strings strictly (good: the worker never sees a lossy U+FFFD path), but Connect-ES turns *every* decode failure into
  `internal` (`parse binary: ...`), before any handler or interceptor runs. The Go client cannot send such a message (the Go
  proto library refuses to marshal it), so no suite case can see this. `src/parse-errors.js` corrects the end-of-stream
  envelope of the Connect streaming response (gunzipping it if compressed) when the error is a decode failure; a test sends
  the bytes by hand. That is a wrapper on `res.write`, the least invasive place Connect-ES leaves: a JS-only worker that
  wants this rule has to know about it, and it only covers the Connect protocol.
- **The suite had moved on without the worker: `TraceContext` (commit 9eb7dc1) was never run against it.** The old probe did
  not implement "the guest gets the call's `traceparent` with a span id of the worker's own, never the client's `traceparent`,
  `tracestate` or `baggage`". Now it does (`guestTraceparent`, from the Execute call's own `traceparent` header). Not
  caused by the HTTP-messages change; found because the whole group list was re-run.

Spec and suite, found along the way (nothing edited):

- **H1, docs, ambiguity.** "HTTP messages" says a header value contains "no control character other than horizontal tab:
  no NUL, CR or LF". For a *path* the same section enumerates the set (U+0000 to U+001F, U+007F); for a value it does
  not, and a value is bytes, so "control character" has no definition (is 0x7F one? 0x80 to 0x9F are "fine"). The Go
  reference (`internal/httpmsg.ValidHeaderValue`) refuses 0x7F; I followed it, from reading the code, not the docs. No suite case
  distinguishes (`/ctlheader` uses 0x01). Suggested wording: "no byte below 0x20 other than 0x09, and not 0x7F".
- **H2, docs, gap.** "Who judges what" lists what a worker refuses (path, query, header names, values) but not
  `method`, `host`, `remote_addr` or the `env` values, and the "Worker execution semantics" table of checks has no row for a
  malformed HTTP message. A NUL in one of those strings cannot be given to a guest at all (an environment entry ends at the NUL).
- **H3, docs vs suite, small.** The check table orders `abi` before `artifact_digest`; this worker (unchanged from round 1)
  checks digest, env names and HTTP shape first and the ABI after. No case sends both faults, so the table is not failable.
- **H4, suite, not failable.** Nothing tests invalid UTF-8 in `path` or `raw_query` at the worker (the Go client cannot send
  it), nor a lone DEL in a header value, nor a tab inside a response header value, nor the request's `HTTP_*` key for a
  header name with characters outside `[A-Za-z0-9-]` (`x_a` and `x-a` collide). A worker that decoded the path leniently would pass.

## What worked well

The protos are small and the reason-to-code table is excellent. Connect-ES wire compatibility was flawless. The suite injects
`Connect-Timeout-Ms` separately from the client's own timer, which is the exact trap the docs warn about. `--print-config`
and `--stop-cmd` worked as described. The orchestrator is easy to run for an outsider. Run against the suite first, before
reading its source or the reference worker, everything passed.
