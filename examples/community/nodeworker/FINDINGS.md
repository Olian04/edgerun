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
skipped; three consecutive runs. Re-verified after the changes below, including the new `MalformedRequests` case.

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

## What worked well

The protos are small and the reason-to-code table is excellent. Connect-ES wire compatibility was flawless. The suite injects
`Connect-Timeout-Ms` separately from the client's own timer, which is the exact trap the docs warn about. `--print-config`
and `--stop-cmd` worked as described. The orchestrator is easy to run for an outsider. Run against the suite first, before
reading its source or the reference worker, everything passed.
