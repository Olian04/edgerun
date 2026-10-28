# The edgerun library

`github.com/Olian04/edgerun` (the module root) is a Go client library for the edgerun protocols. It exists
so that other developers can extend edgerun by talking to its instances in the roles the protocols define:
a deployment tool that is an operator, a queue consumer that is a frontend, a worker on another runtime, a
network service inside a private network, a scheduler of your own.

```sh
go get github.com/Olian04/edgerun
```

## One idea

edgerun is roles joined by seams; each seam is a proto package (`proto/edgerun/*/v1`), specified in
[`protocol.md`](protocol.md). The library gives you **the other side of each seam, and nothing else**:

| You are | You talk to | Client | Protocol | Token scope |
|---|---|---|---|---|
| an operator | an orchestrator | `NewControl` | `control.v1` | `ScopeAdmin` |
| a frontend | an orchestrator | `NewIngress` | `ingress.v1` | `ScopeIngress` |
| a worker | an orchestrator | `NewFleet`, `NewArtifacts` | `worker.v1` | `ScopeWorker` |
| a scheduler | a worker | `NewExecution` | `worker.v1` | `ScopeExecute` |
| a network service | an orchestrator | `NewRegistry` | `egress.v1` | `ScopeNetwork` |
| a worker | a network service | `NewEgress` | `egress.v1` | `ScopeEgress` |

Every client has one method per RPC, named as the RPC, taking and returning the generated messages. Nothing
is renamed, hidden or composed: what the protocol document says about an RPC is true of the method. A test
(`coverage_test.go`) fails if an RPC of any service in `gen/` is not reachable through a client, so the
library cannot fall behind the protocols.

The messages are the packages under `gen/edgerun/*/v1`; import them next to the library.

## What it adds

Only what every caller would otherwise write, identically, for itself:

- **The token.** `NewX(baseURL, token, opts...)` presents it on every call.
- **No envelopes.** `ctl.GetFunction(ctx, &req)` returns the response message, not a `connect.Response`.
- **Iterators for paged lists.** `AllFunctions`, `AllVersions`, `AllRoutes`, `AllInvocations` are
  `iter.Seq2[T, error]`; they fetch pages as you consume them. `AllFunctionsMatching`, `AllVersionsMatching` and `AllRoutesMatching` take a label
  filter (`match_labels`).
- **Streams as readers.** `Invoke`, `Execute` and `Fetch` return as soon as the response head arrives
  (`Status()`, `Header()`); the body is an `io.ReadCloser`. A failure after the head is returned by `Read`,
  never as `io.EOF`: a body that ends cleanly is complete. `Execution.Summary()` is available once the body has
  been read to its end.
- **Verified artifacts.** `Artifacts.GetArtifact` checks the bytes against their digest and returns
  `ErrDigestMismatch` rather than bytes you must not run.
- **Reasons.** Platform failures (invoke, execute, fetch, artifacts) carry a `common.v1.ErrorReason`. `Reason(err)`
  and `IsReason(err, r)` read it; branch on that, never on the code or the message. `NewError` builds one, for the
  other side. The control protocol has no reasons: its errors are plain Connect codes (`not_found`,
  `already_exists`, `invalid_argument`), and you branch on those with `connect.CodeOf`.
- **Digests.** `Digest(b)` is the `sha256:<hex>` the protocols identify an artifact by.

## Managing declaratively

Three things of `control.v1` exist for tools that manage edgerun from a description of what it should look like
(a GitOps service, a Terraform provider), and the library has nothing to add to them: they are fields and one RPC.

- **Labels** (`labels` on a function, version and route, `SetLabels`, `match_labels` on the lists): mark what
  you own and find it again. Key them under a namespace of your own.
- **Revisions** (`EnvEntry.revision`): a write-only secret cannot be read back, but its revision can be compared.
  Remember the revision your last write returned; a different one later means someone else changed the value.
- **`validate_only`** on every change: all the checks against the real state, nothing kept. A dry run built on it
  cannot disagree with the apply.

Things a tool learns the hard way (the rules are in `protocol.md`, "Control semantics"):

- A validate-only call is judged against the stored state only, never against the validate-only calls before it. To
  validate the later steps of a plan that starts by creating a function, send them with the id of nothing and treat
  `not_found` as "passed every check that does not need the function": the order of checks (form, then existence,
  then conflicts) is guaranteed for exactly this.
- The revision a candidate secret would have is what a validate-only `SetEnv` returns. Compare it with the one
  `ListEnv` shows to ask "is the stored secret what I want?". Revisions differ per function, and all change if the
  orchestrator's key rotates.
- `SetLabels` replaces the whole map, so changing one label is a read, an edit and a write: two writers can lose
  each other's updates. A filter that is nil or empty means *everything*, and a typo in a key matches nothing: never
  prune on the strength of a filter alone.
- A validate-only `CreateVersion` carries the whole module, so a dry run followed by an apply uploads it twice.

## Many calls at once

The library's default HTTP client keeps a connection pool sized for many concurrent calls (64 idle connections per
host), not `http.DefaultClient`'s two. Measured by a traffic generator at a concurrency of 64: 3,000 requests used about
1,100 TCP connections with the default client and 70-90 with a pool that fits. Servers compress a message only above
1 KiB (Connect's default is every message, which made compression the biggest allocator on small calls). If you pass
your own client with `WithHTTPClient`, size its pool the same way. HTTP/2 without TLS needs
`Transport.Protocols.SetUnencryptedHTTP2(true)`; it uses one connection, and was slower than pooled HTTP/1.1 at high
rates in the same measurement, so prefer pooling for load generation.

Time to head: `Invoke` returns when the response head arrives. Workers buffer a response before they send it (see
`wagi-1`), so for the functions edgerun runs today the head and the body arrive together, and the time to head is the
function's run time, not the time to a first byte.

## What it does not do

No retries, caches, connection management beyond Connect's, heartbeat loops or workflows (there is no
`Deploy`; deploying is `CreateVersion` and, if you like, `ActivateVersion`). Those are policy, and the right
policy depends on the program. `WithHTTPClient` and `WithClientOptions` are how you impose yours: timeouts,
proxies, TLS, another wire protocol.

Two rules of the protocols the library cannot enforce for you:

- **Deadlines are contexts.** The deadline of the context you pass is the RPC deadline, which for `Invoke`,
  `Execute` and `Fetch` is the wall-clock budget of the whole exchange. The context must outlive the body of a
  streamed response: cancelling it ends the response wherever it has got to.
- **A token has one scope.** A client holding a token of the wrong scope fails with `PERMISSION_DENIED`, an
  unknown token with `UNAUTHENTICATED`. Each role needs its own token.

## Examples

```go
ctl := edgerun.NewControl("http://orchestrator:8080", adminToken)
fn, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello"})
ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fn.Function.Id, Module: wasm, Abi: "wagi-1", Activate: true})
ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "hello.example.com", PathPrefix: "/", FunctionId: fn.Function.Id})

ing := edgerun.NewIngress("http://orchestrator:8080", ingressToken)
res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "hello.example.com", Path: "/"}})
if edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY) {
	// back off and try again
}
defer res.Body.Close()
w.WriteHeader(res.Status())
if _, err := io.Copy(w, res.Body); err != nil {
	// The status line is out: the only honest signal that the body is incomplete is to
	// abort the client's connection (panic(http.ErrAbortHandler) in a net/http handler).
}
```

A frontend that fails *before* the head maps the error to a status with `FrontendStatus(err, ctx.Err())`, the table
of `protocol.md` as code (it also answers whether to send `Retry-After: 1`, and 0 when the client has gone).

More, compiled with the library, in [`example_test.go`](../example_test.go): deploying, iterating, a frontend, and
a network service registering itself and heartbeating.

## Implementing a role instead

To be the *server* of a seam (a worker, an orchestrator, a network service, a frontend's upstream), use the
generated handlers in `gen/` directly (`workerv1connect.NewExecutionServiceHandler`, ...), and from the library:

- `Authenticate(Verifier{token: scope}, scopes...)`, the interceptor that gives you the protocol's `UNAUTHENTICATED`
  / `PERMISSION_DENIED` behaviour;
- `NewError(reason, ...)` and `InvalidArgument(...)`, which attach the canonical code and `ErrorInfo`;
- the `Scope*` constants.

Then point the conformance executable at it (`edgerun-conformance --role worker --target ...`, see
[`protocol.md`](protocol.md), "Conformance"): pass it and your implementation can replace the in-repo one.

For other languages, see [`other-languages.md`](other-languages.md).

## Stability

The protocols are versioned in their package names and guarded by `buf breaking`; `v1` does not change
incompatibly. The Go API follows the module's version, which is pre-1.0: expect the surface above to hold (it is
the protocols' surface) and the helpers around it (`Response`, options) to be refined.

edgerun's own components are built on the library: the frontend, the orchestrator (calling workers), the worker
(calling the orchestrator and network services), the network service (registering), the admin UI and `edgerunctl`.
Their tests use it too. Three places deliberately do not, because they are about the wire itself and a client that
hides it could not make their point: the conformance suites (`conformance/`), which depend on the generated protocol
code and nothing else of edgerun so that an implementation cannot also get the test wrong; the test of the
orchestrator's artifact stream chunking; and the test of the authentication interceptors themselves.
