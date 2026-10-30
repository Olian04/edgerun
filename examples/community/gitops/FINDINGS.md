# Findings: building a GitOps service on the edgerun library

A probe, written as an external consumer (own module, `replace` to the repo, no `internal/` imports). Reported by
the agent that built and then migrated it; items marked **(checked)** were verified against the code afterwards.
"Root cause" says where a gap lives: the *protocol*, the *library*, or the *docs*. This is the third round: control.v1
had gained labels, env revisions and validate-only (second round), and has since gained `PlanService.Validate`,
`PatchLabels` / `PatchRouteLabels` / `SetRouteLabels`, the `has_label_keys` / `lacks_label_keys` filters and the
`name` / `function_id` filters. The service was migrated to all of them; this round says what they replaced, what
they did not, and what they cost.

What it is: `edgerun-gitops` (`--once`, `--dry-run`, poll loop, `--prune`, `--adopt`, `--owner`) reads a YAML file
and the `.wasm` modules from one commit of a git repository and reconciles functions, versions, env, secret env (by
reference to process environment variables), egress and routes through `edgerun.NewControl`. Everything it creates is
labelled `gitops.example.com/owner=<owner>` (functions, versions, routes; versions also `.../commit`). A plan is
a list of items, each exactly one control request; `--dry-run` sends that list to the orchestrator as one
`PlanService.Validate` and the apply sends the same requests one by one. 32 tests (was 23) against a real PostgreSQL,
orchestrator and worker (also with `-race`).

## What the migration replaced

| Before | Now |
|---|---|
| Per-call `validate_only`; steps after a create validated against the nil UUID, `not_found` read as "passed"; a retarget and new variables that needed stale ones deleted first marked "not validated ahead" | One `Validate` of the plan in apply order; `$name` binds for what the plan creates. The nil-UUID trick, `tol`, `partial` and `Blind` for those two cases are deleted. A retarget and a 64-variable swap are validated (tests) |
| A failed validate-only call named the item that sent it | `ValidateResponse.failure` (step index, code, message) is mapped back to the plan item; its function is dropped from the plan and the rest is validated again |
| One validate-only `SetEnv` per secret per pass to learn the revision a value would have | One `Validate` per function per pass, a `SetEnv` step per secret; the results carry the revisions |
| `SetLabels` with a map assembled from a read (adoption; the secret-revision record) | `PatchLabels` of this service's own keys: `owner` to adopt; the record's slots (`set` what is used, `remove` the unused slots, without reading). `SetLabels` / `SetRouteLabels` are never called (asserted by counting) |
| A hand-made route the file declares: `DeleteRoute` + `CreateRoute` | If it already points where the file says: `PatchRouteLabels`; the route keeps its id and its other labels and is never unrouted (watched by a client during the pass). If it must be retargeted: delete + create, as before |
| A scan of every function to learn whether a name is taken or who owns an id; a scan of every route to find a route at a key or an adopted function's routes | `ListFunctions{name}`; `GetFunction` for an id; `ListRoutes{function_id}` for an adopted function's routes; `ListRoutes{lacks_label_keys: [owner]}` for "routes nobody owns", read only when a desired key is not already ours. A test asserts that no `ListFunctions` / `ListRoutes` of any pass is unfiltered, next to 130 foreign functions and 133 foreign routes, with adopt, prune and conflicts |

Measured: a `--prune` dry run next to 130 foreign functions went from 7 calls to 5; a steady-state pass is unchanged at 8
(it was already filtered by owner). A function with two secrets: 1 `Validate` per pass instead of 2 `SetEnv`.

## Status of the first round's findings

1. **No labels or ownership on any resource. RESOLVED (protocol).** Ownership is the owner label, set by the one RPC
   that creates the function; the owner filter finds what is ours (functions, routes) in one list each; no
   `GITOPS_STATE`, so the guest sees only its own environment and all 64 slots are the user's; the "empty means mine"
   recovery heuristic is gone, and a test kills the service after each of the 7 writes of a deploy and finds
   nothing unlabelled. Prune no longer does a `ListEnv` per function (130 foreign functions: 0 `ListEnv`, was 130).
2. **Write-only secrets cannot be diffed. RESOLVED (protocol), with a trick the docs now state.** A secret is compared
   by revision. Three revisions are in play: the stored one (`ListEnv`), the one the desired value would have, and the
   one recorded when the service last wrote it (in labels). The second is learned from the server: a `SetEnv` step of
   a `Validate` returns the entry, revision included (one `Validate` per function per pass; it was one validate-only
   call per secret). A secret changed behind the tool's back is reported by name as DRIFT and put back; a rotation in
   the process environment is reported as a change; an unchanged secret causes no write (asserted by counting the
   calls). `GITOPS_FINGERPRINT_KEY` is gone.
3. **No validate-only on mutating RPCs. RESOLVED, and now for the whole plan.** `--dry-run` is a `Validate` of the plan
   the apply runs; `a.example.com:99999` is reported by the dry run and nothing of the function is created by the
   apply either. The hand-written copy of the server's rules is deleted except for the number 64, which survives as an
   ordering hint (see `spec.go`). The limits of `Validate` itself are listed below.
4. **Lookup is by scanning. RESOLVED (protocol): `ListFunctionsRequest.name`, `ListRoutesRequest.function_id`,
   `has_label_keys` / `lacks_label_keys`; all adopted in this round (table above).** Original finding: no name filter
   on functions, no function filter on routes; a name that is not ours cost a full scan. What is left of the scanning
   is in "Still awkward".
5. **Control errors carry no ErrorReason; docs said to branch on it. RESOLVED (docs).**
6. **Multi-step operations are not atomic. STILL OPEN, inherent.** `Validate` says whether the sequence would be
   accepted, not that it will be: a rejected function is not touched at all, but a step that fails at apply time still
   leaves the function half done until the next pass. There is still no `UpdateRoute`, so a retarget still unroutes
   its key between the delete and the create (measured: 66 of 98 requests failed during one, with every write slowed
   to 100 ms). The one case that used to need it and does not any more is a route that only lacks our label.
7. **A deploy cannot be verified before it takes effect. STILL OPEN, by design.**
8. **Version identity is subtler than "digest differs". STILL OPEN (docs).** Matching is on (digest, abi, limits),
   over all versions, labelled or not (so that adopting a function does not re-upload its module).
9. **No digest helper. RESOLVED (library).** `edgerun.Digest` is used.
10. **Docs vs server on trailing dots in egress entries. RESOLVED (docs).** `protocol.md` now says a trailing dot is not
    part of a request's host but is not accepted in an entry.
11. **Minor. PARTLY RESOLVED.** The `All*Where` iterators take the page size from the request. Still open: no per-page
    deadline; the harness, which builds from a snapshot given by `GITOPS_EDGERUN_SRC`, is still the most work
    (`scripts/devstack.sh` is one long-lived dev stack, and `internal/orchestrator/orchtest` cannot be imported from
    another module); `ingress.Invocation` and `controlv1.Invocation` share a name.

## Still awkward: the previous round's list, after the migration

- **A validate-only call cannot see the ones before it. RESOLVED by `PlanService.Validate`, with limits of its own.**
  The nil-UUID trick and the "(not validated ahead)" cases are deleted. Tested: a retarget (delete then create of one
  key) validates and applies, while a lone validate-only create at that key is `already_exists`; a swap of 60
  variables validates (60 deletions, then 60 writes), while five new writes beside 60 stored ones are refused as a
  sequence at the fifth although each is accepted alone; a failure comes back as step / code / message and is mapped
  to its item (the index in the report is the position in the request that failed). The limits are under "New
  problems" below (128 steps, one upload's worth of bytes, first failure only).
- **A route cannot be labelled after it exists. RESOLVED by `PatchRouteLabels`, for the case it can cover.** A
  hand-made route that points where the file says is labelled in place: never unrouted (tested with a client asking
  every 2 ms during a pass whose writes are slowed to 100 ms, and with a positive control, a retargeted route in the
  same pass, on which the same client does see the gap). A hand-made route that points at another function of ours
  still has to be deleted and created: a label patch does not retarget. That residue is item 6 above.
- **`SetLabels` replaces the whole map. RESOLVED by `PatchLabels` for this service's keys.** Tested: another tool's
  labels on a function and on a route survive a deploy, a rotation of a secret (the record is rewritten) and a write
  by another tool that lands between the service's read and its write. Not resolved: a patch has no precondition. Two
  instances with different owners that adopt one function both succeed and the last write holds it (tested); the loser
  finds out at its next pass, as a conflict. `SetLabels` had the same outcome, and also lost the other labels.
- **The revision record is a packing exercise. STILL OPEN.** Keys cannot be upper case, a function holds 64 variables
  and 32 labels, so secrets are kept as 24-character hashed entries, ten per label (64 secrets, 7 labels of the 32).
  `PatchLabels` made the writes safe, not the encoding smaller; the patch even carries up to 7 `remove` keys for slots
  that may not exist, because removing an absent key is not an error and reading first is what it avoids. A
  caller-chosen annotation per env entry would delete all of `labels.go`.
- **The desired revision costs one validate-only `SetEnv` per secret per pass. PARTLY RESOLVED.** One `Validate` per
  function per pass, a `SetEnv` step per secret. Still a network call per function per pass in steady state, and a
  validation used as a hash oracle: its result is the only place the server will say what a value's revision is. (It
  could be one `Validate` for the whole pass; the service batches per function because it plans per function.)
- **A validated `CreateVersion` carries the whole module. STILL OPEN, and `Validate` makes it worse.** A plan puts
  every changed module in one request, a dry run followed by an apply uploads them twice, and a plan that has to be
  validated again after a function is dropped uploads the rest again. No validate-by-digest.
- **Version labels are provenance of the first upload only. STILL OPEN.** A revert re-activates an old version, which
  keeps the commit it was uploaded from.
- **Labels can only be matched exactly. RESOLVED for what the service needed, with limits.** "Routes nobody owns" is
  one filtered list (`lacks_label_keys`), "the one function called x" is `name`, "the routes of this function" is
  `function_id`. For functions the `name` filter made `lacks_label_keys` unnecessary (the label is read off the one
  function that comes back). Not covered: "owned by somebody else" (there is no negation of a value, so listing the
  other owners' resources means reading ours as well), and no filter on a route's host and path, so finding what
  holds a key is a list of all the routes nobody owns (67 of the 133 foreign routes in the test) or a failed create.
  A route that another owner holds is therefore reported by `Validate` as `already_exists` on the create step,
  without naming the holder.
- **Normalisation (lower-cased hosts, trailing `/`, sorted egress) is mirrored client-side. STILL OPEN.** The server
  could be asked (the `Validate` result of a `CreateRoute` step is the route as it would be stored, which is the
  revision trick again), but a call per route per pass to learn what `strings.ToLower` says is not an improvement.

## New problems in the protocol, library and docs, as a consumer saw them

Each was observed, not read off the source; the numbers are from the tests.

1. **`Validate` takes every module of the plan in one request, bounded like one upload. (protocol, docs)** Three valid
   12 MiB modules in one `Validate` are refused as a whole: `resource_exhausted: message size 37748919 is larger than
   configured max 34603008` (32 MiB + 1 MiB). It is a failure of the call, not a `failure` of a step: no step index, and
   not one of the three codes a `StepFailure` has. `protocol.md` says "bounded as an upload is" and nothing of what
   that means for N modules or that this is the code. The service asks `Describe` for `max_module_bytes` when a plan
   carries more than 1 MiB of modules and cuts the plan to fit. Accounting per step, or validate-by-digest for a
   module the orchestrator already holds, would make the problem go away.
2. **128 steps and the limit of 64 variables do not fit together. (protocol)** Replacing all 64 variables of a
   function is 64 deletions and 64 writes: exactly one `Validate`. With a new module as well it is 129 and the call is
   refused (`invalid_argument`, as an error of the call). The service validates the first 128 steps of a function and
   marks the rest "not validated ahead" in the dry run (tested), so the guarantee `Validate` was built for is missing
   exactly for the largest functions. The number is in no response: `Describe` reports `max_module_bytes` and
   nothing else, so 128 (steps), 64 (variables), 32 (labels) and 253 (label value) are copied into the client. The 64
   survives in `spec.go` as a hint for ordering the deletions first, no longer as a validation.
3. **`Validate` stops at the first failure, and a failure costs a full round trip. (protocol)** Two bad functions out
   of four were three `Validate` calls, each re-sending every module of the plan: the service drops the function of
   the failed step and validates again, because a plan that fails says nothing about the steps after the failure. A
   response that lists the failure of every step that fails independently of the others (or "validate the rest,
   skipping what depends on the failed step") would make a dry run of N problems one call, not N + 1.
4. **A plan has to be cut by the client, and the cut is sound only by argument. (protocol)** Two `Validate` calls are
   each judged against the stored state and cannot see each other. This service cuts at function boundaries (nothing in
   its plan depends on another function: a retarget deletes and creates one function's route, the limits are per
   function), tested with 198 steps and with 36 MiB. A tool whose plans depend across a cut has no sound way to
   validate more than 128 steps.
5. **A step's answer cannot feed the next step, except by id. (protocol)** The record of a new secret's revision is a
   label value derived from the revision the `SetEnv` returns. In a `Validate` the `PatchLabels` that records it is
   validated with a stand-in of the right form, because only ids can be referred to (`$name`). Harmless here (any hash
   is a valid value); it does mean a validated step is not always the step that will be sent.
6. **No precondition on a patch. (protocol)** There is no "set this label only if absent" or "only if it still has this
   value", so adoption is a race between instances (tested).
7. **Page tokens are not bound to the filters they were issued for. (protocol, docs)** A token from a `match_labels`
   list, used with a `has_label_keys` request, was accepted and answered an empty page without an error (observed with
   `ListFunctions`). `protocol.md` says a token the server did not issue is `invalid_argument`. The library's
   `All*Where` clones one request, so it cannot do this; a hand-written pager can, and an empty list is the dangerous
   answer for a tool about to prune.
8. **Filter validation is not uniform. (protocol, docs)** `has_label_keys` / `lacks_label_keys` refuse a malformed key
   and contradictory filters (`invalid_argument`, with good messages), but a malformed key in `match_labels`, a
   malformed or upper-case `name` and a malformed `function_id` select nothing without an error. Documented as
   deliberate for `function_id` and `match_labels`; for `name` the docs say "or none", which is true and the same trap.
9. **The step wrapper and the failure code are the caller's to write. (library)** `Control.Validate` takes `Step`
   oneofs, so every consumer writes the same eleven-case type switch to wrap a request (`exec.go` has it twice: to wrap
   and to send), and `StepFailure.code` is a string ("invalid_argument"), so `connect.CodeOf` cannot be applied to it;
   this service prints it. A `Step` constructor and a `StepFailure.Err()` returning a `*connect.Error` are the obvious
   helpers.
10. **`library.md` and `protocol.md` say what `Validate` is, not what it costs. (docs)** Nothing states the 128 steps in
    `library.md`, that modules travel in the request (twice, in a dry run followed by an apply), the size bound and the
    code it fails with, that the response stops at the first failure, or that the results of `SetEnv` steps carry the
    revision (which is how this service uses it).
11. `GetInvocation` has nothing to do with a GitOps service: nothing adopted, nothing tested.

No bug was found. About forty `Validate` and patch calls were made directly with the Go client (an exploratory probe,
not kept as a test): a step's failure had the code and the exact message its own call gives, for the ones compared
(egress, label count, label key, reserved variable, missing variable, name taken); a name freed by a deletion could be
created again in the same plan; a route could be created for a function an earlier step created and deleted by a `$`
reference; a route of a function deleted earlier in the plan was `not_found`; duplicate, malformed, unbound and
misplaced binds were `invalid_argument` at their step; a `validate_only` inside a step was ignored; validated versions
did not use up a version number; nothing a validation did was visible afterwards. `PatchLabels` with an empty patch
returns the function, removes absent keys without error, refuses a key both set and removed, applies the limit of 32 to
the result and answers `not_found` for an unknown function.

### Earlier rounds

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
- No bug was found in the second round's server behaviour: validate-only never changed state in any test, returned the
  apply's exact error for five different calls, and the label filters, revision semantics and limits behaved as
  documented.

## What worked well

`Validate` is the right shape: the plan the apply runs, checked by the code that will run it, answered with the apply's
own error. Deleting a nil-UUID trick, a client-side copy of two server rules and a list of things that could not be
validated is a real simplification (`reconcile.go` lost its `Blind`, `partial` and `tol` machinery), and the dry run
and the apply can no longer be different lists. The label patches make ownership boring and sharing safe; the has /
lacks filters and `name` / `function_id` remove every full scan from a pass. 1:1 clients with no hidden behaviour; a
precise "Control semantics" section; read-your-writes held; `ActivateVersion` as rollback; secrets never leak (nothing
in a label, a plan or a log contains a value).

## Not done

Post-deploy health check or auto-rollback, multi-instance safety beyond "the loser notices" (two instances with the
same owner race on `already_exists`, nothing worse; two with different owners race on adoption, see above), GitHub auth
beyond whatever git is configured with, Git LFS (pointers are detected and refused). Secret drift is detected in any
process, from the labels; "same commit as the last pass" only inside a running loop. The desired revisions are fetched
per function, not once per pass.
