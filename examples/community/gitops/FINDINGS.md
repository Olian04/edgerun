# Findings: building a GitOps service on the edgerun library

A probe, written as an external consumer (own module, `replace` to the repo, no `internal/`
imports). Reported by the agent that built it; items marked **(checked)** were verified against the code
afterwards. "Root cause" says where the gap lives: the *protocol*, the *library*, or the *docs*.

What was built: `edgerun-gitops` (`--once`, `--dry-run`, poll loop, `--prune`, `--adopt`, `--owner`) reading
a YAML file and the `.wasm` modules from one commit of a git repository, and reconciling functions,
versions, env, secret env (by reference to process environment variables), egress and routes through
`edgerun.NewControl`. 13 tests against a real PostgreSQL, orchestrator and worker (idempotent second run,
revert re-activates the old version, drift, prune, partial failure, crash recovery, the real binary).

## Findings, ranked

1. **No labels or ownership on any resource.** Hand-rolled and fragile. Root cause: protocol.
   Ownership is kept as JSON in a plain env var (`GITOPS_STATE`): visible to the guest, one of the 64 slots.
   Creating a function is two RPCs, so a crash between them leaves an unclaimed empty function, and the
   recovery heuristic ("empty means mine") could take one a human just made. Routes cannot be marked at all
   (a route is "mine" iff its function is). One `ListEnv` per function to check ownership: 130 functions, 335 ms.
   Fix: `labels` on Function, Version and Route, and a list filter.
2. **Write-only secrets cannot be diffed.** Hand-rolled and fragile. Root cause: protocol.
   An HMAC fingerprint per secret is kept in the marker; a secret changed by hand is reported "in sync"
   (`TestSecretDriftIsInvisible`). Fix: a server-keyed digest or `revision` on `EnvEntry`.
3. **No validate-only on mutating RPCs.** Hand-rolled and fragile. Root cause: protocol.
   The server's rules were re-implemented from `protocol.md` (they held, which says good things about the
   doc), but `a.example.com:99999` passes the client-side validator, the dry run says fine, and apply creates the
   function, uploads and activates the version, then fails at `SetEgress` and skips the route. Fix: `validate_only`.
4. **Lookup is by scanning.** Awkward. Root cause: protocol. By name: `AllFunctions` into a map. By digest:
   `AllVersions`. Fix: optional name and digest filters; the library should not hide the scan.
5. **Control errors carry no ErrorReason, and the docs said to branch on it.** (checked) Docs. Root cause: docs.
   control.v1 uses plain Connect codes (`already_exists`, `invalid_argument`...), by design: a malformed request
   has no reason. `docs/library.md` and `doc.go` said "branch on the reason, never on the code", which is only
   true of the protocols' platform errors (invoke, execute, fetch, artifacts). Docs corrected. (Re-exporting
   `connect.CodeOf` was considered and rejected: `connect` is already the idiom and a dependency.)
6. **Multi-step operations are not atomic.** Awkward, inherent. There is no `UpdateRoute` (a retarget is delete then
   create), and env, activate and egress are separate calls; only `CreateVersion{activate}` is atomic. The
   service plans from a fresh read each run, applies in phases and stops a function at its first failure; re-runs
   converge.
7. **A deploy cannot be verified before it takes effect.** Awkward, by design. `CreateVersion` accepts garbage
   bytes and an unknown ABI; invocation reaches only the active version; probing needs the ingress token (a
   second scope); `FUNCTION_NOT_FOUND` conflates "no route" and "no active version".
8. **Version identity is subtler than "digest differs".** Awkward. ABI and limits belong to the version and
   `CreateVersion` is not idempotent (`limits: nil` and `limits: {}` gave two identical versions). Matching on
   (digest, abi, limits) makes a revert re-activate the old version. Fix: document it.
9. **No digest helper.** Awkward. Root cause: library. `sha256:<hex>` is computed by hand though the library
   already does it internally. Fixed: `edgerun.Digest` is exported.
10. **Docs vs server on trailing dots.** (checked) `protocol.md` says a trailing dot and case are not part of
    the name; that is true of how a *request's host* is matched, but the allow-list parser rejects an *entry*
    with a trailing dot (`API.Example.com.`) while accepting and lower-casing uppercase. Docs: say which.
11. **Minor.** The `All*` iterators take no page size or per-page deadline; clients are concrete structs, so
    consumers need a real stack or their own interface to test against; there is no supported throwaway-stack
    tool (the harness, copied from `devstack.sh`, was the most work in the project); `ingress.Invocation`
    and `controlv1.Invocation` share a name.

## What worked well

1:1 clients with no hidden behaviour; a precise "Control semantics" section (canonical forms were
implementable, and every second run changed nothing); read-your-writes held, no retries needed anywhere;
`ActivateVersion` as rollback and `CreateVersion{activate}`; secrets never leak; `Describe.max_module_bytes`;
the `iter.Seq2` iterators; context-as-deadline.

## Not done

Post-deploy health check or auto-rollback, multi-instance safety, GitHub auth beyond whatever git is configured
with, Git LFS (pointers are detected and refused). "Drift" is labelled only inside a running loop.
