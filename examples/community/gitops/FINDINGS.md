# Findings: building a GitOps service on the edgerun library

A probe, written as an external consumer (own module, `replace` to the repo, no `internal/` imports). Reported by
the agent that built and then migrated it; items marked **(checked)** were verified against the code afterwards.
"Root cause" says where a gap lives: the *protocol*, the *library*, or the *docs*. This is the second round:
control.v1 gained labels, env revisions and validate-only, and the service was migrated to all three.

What it is: `edgerun-gitops` (`--once`, `--dry-run`, poll loop, `--prune`, `--adopt`, `--owner`) reads a YAML file
and the `.wasm` modules from one commit of a git repository and reconciles functions, versions, env, secret env (by
reference to process environment variables), egress and routes through `edgerun.NewControl`. Everything it creates is
labelled `gitops.example.com/owner=<owner>` (functions, versions, routes; versions also `.../commit`). `--dry-run`
sends every step of the plan to the orchestrator with `validate_only`. 24 tests against a real PostgreSQL,
orchestrator and worker (also with `-race`).

## Status of the first round's findings

1. **No labels or ownership on any resource. RESOLVED (protocol).** Ownership is the owner label, set by the one RPC
   that creates the function; the owner filter finds what is ours (functions, routes) in one list each; no
   `GITOPS_STATE`, so the guest sees only its own environment and all 64 slots are the user's; the "empty means mine"
   recovery heuristic is gone, and a test kills the service after each of the 7 writes of a deploy and finds
   nothing unlabelled. Prune no longer does a `ListEnv` per function (130 foreign functions: 0 `ListEnv`, was 130).
2. **Write-only secrets cannot be diffed. RESOLVED (protocol), with a trick the docs now state.** A secret is compared
   by revision. Three revisions are in play: the stored one (`ListEnv`), the one the desired value would have, and the
   one recorded when the service last wrote it (in labels). The second is learned from the server: a validate-only
   `SetEnv` returns the entry, revision included. A secret changed behind the tool's back is reported by name as
   DRIFT and put back; a rotation in the process environment is reported as a change; an unchanged secret causes no
   write (asserted by counting the calls). `GITOPS_FINGERPRINT_KEY` is gone.
3. **No validate-only on mutating RPCs. RESOLVED for what validation can see; see "Still awkward" for what it
   cannot.** `--dry-run` calls the real RPCs; `a.example.com:99999` is reported by the dry run and, because the whole
   plan is validated before the first write, nothing of the function is created by the apply either. The
   hand-written copy of the server's rules is deleted except for the 64-variable total (a cumulative rule no single
   validate-only call can see).
4. **Lookup is by scanning. STILL OPEN.** No name filter on functions, no function filter on routes. The owner filter
   turns the steady state into one filtered list per kind; a name that is not ours still costs a full scan.
5. **Control errors carry no ErrorReason; docs said to branch on it. RESOLVED (docs).**
6. **Multi-step operations are not atomic. STILL OPEN, inherent.** Validate-only narrows it (a rejected function is not
   touched at all) but a step that fails at apply time still leaves the function half done until the next pass; there
   is still no `UpdateRoute`.
7. **A deploy cannot be verified before it takes effect. STILL OPEN, by design.**
8. **Version identity is subtler than "digest differs". STILL OPEN (docs).** Matching is on (digest, abi, limits),
   over all versions, labelled or not (so that adopting a function does not re-upload its module).
9. **No digest helper. RESOLVED (library).** `edgerun.Digest` is used.
10. **Docs vs server on trailing dots in egress entries. RESOLVED (docs).** `protocol.md` now says a trailing dot is not
    part of a request's host but is not accepted in an entry.
11. **Minor. STILL OPEN.** The `All*` iterators take no page size or per-page deadline; there is no supported
    throwaway-stack tool (the harness, which builds from a snapshot given by `GITOPS_EDGERUN_SRC`, is still the most
    work); `ingress.Invocation` and `controlv1.Invocation` share a name.

## Still awkward, with the new features

- **A validate-only call cannot see the ones before it.** Each runs in its own rolled-back transaction against the
  stored state, and a validate-only create returns an empty id, so a plan cannot be validated as a whole. The steps
  after the creation of a function that does not exist yet have no function to run against. The service sends the
  well-formed id of nothing and treats `not_found` as "every check that does not need the function passed". That
  works because the server checks the form of a request before it looks the function up. **That order is now
  documented as part of the protocol ("Validate-only") and the orchestrator suite enforces it** (it was not promised
  before this probe). Still open: a retarget (delete then create of the same host and path) cannot be validated, because
  the create is `already_exists` while the route is there; and new variables that only fit once stale ones are deleted
  cannot be validated against the 64-variable limit. Wanted, if it matters: a way to validate a sequence ("as if the
  previous ones had been applied") or a pending id for a validated create. Not done.
- **A route cannot be labelled after it exists.** Routes are immutable, so adopting a hand-made route the file
  declares is delete plus create, with the key unrouted in between. The service does it only for a route with no owner
  label that points at one of its functions, and says so in the plan.
- **`SetLabels` replaces the whole map.** Adoption, the revision record and any other tool's labels are a
  read-modify-write: two writers lose each other's updates (no per-key patch, no compare-and-set). Documented.
- **The revision record is a packing exercise.** Keys cannot be upper case and a function holds 64 variables, so one
  label per secret does not fit: secrets are kept as 24-character hashed entries, ten per label (64 secrets, 7 labels of
  the 32). It works, tested at 64, but is machinery the platform could have saved (e.g. a caller-chosen annotation
  per env entry).
- **The desired revision costs one validate-only `SetEnv` per secret per pass.** Chatty in steady state.
- **A validated `CreateVersion` carries the whole module**, so a dry run followed by an apply uploads it twice. No
  validate-by-digest. Documented.
- **Version labels are provenance of the first upload only.** A revert re-activates an old version, which keeps the
  commit it was uploaded from.
- **Labels can only be matched exactly.** No "has key" or "lacks key", so "functions nobody owns" is a full scan.
- Normalisation (lower-cased hosts, trailing `/`, sorted egress) is still mirrored client-side so that a file compares
  equal to stored state. It is not validation, but it is a copy of server behaviour.

## Problems in the new protocol, library and docs, as a consumer saw them

- `protocol.md` "Validate-only" did not say that calls are independent of each other, nor what is checked first.
  **Fixed (checked):** both are stated, and the order is tested.
- It did not say that a validate-only `SetEnv` is how to learn the revision a value would have, without which a
  revision only answers "did someone change it since I wrote it?". **Fixed (checked).**
- Revisions differ per function and for a recreated function, and all change if the orchestrator's key rotates (it
  would look like drift in every secret). **Fixed (checked):** stated, with the advice that rewriting them all is safe.
- `library.md` "Managing declaratively" listed the features but none of these traps. **Fixed (checked).**
- The `*Matching` iterators treat nil or an empty map as "everything" and a typo in a key silently matches nothing:
  the dangerous direction for a prune. Documented ("never prune on the strength of a filter alone"); this service
  refuses an empty file without `--allow-empty`.
- No bug was found in the new server behaviour: validate-only never changed state in any test, returned the apply's
  exact error for five different calls, and the label filters, revision semantics and limits behaved as documented.

## What worked well

The label filter and the single-RPC create make ownership boring. `validate_only` returning the real error and the real
response is exactly what a dry run needs, and "the response the call would have returned" is what made the revision
usable. 1:1 clients with no hidden behaviour; a precise "Control semantics" section; read-your-writes held;
`ActivateVersion` as rollback; secrets never leak (nothing in a label, a plan or a log contains a value).

## Not done

Post-deploy health check or auto-rollback, multi-instance safety (two instances with the same owner race on
`already_exists`, nothing worse), GitHub auth beyond whatever git is configured with, Git LFS (pointers are detected and
refused). Secret drift is detected in any process, from the labels; "same commit as the last pass" only inside a
running loop.
