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
  `iter.Seq2[T, error]`; they fetch pages as you consume them.
- **Streams as readers.** `Invoke`, `Execute` and `Fetch` return as soon as the response head arrives
  (`Status()`, `Header()`); the body is an `io.ReadCloser`. A failure after the head is returned by `Read`,
  never as `io.EOF`: a body that ends cleanly is complete. `Execution.Summary()` is available once the body has
  been read to its end.
- **Verified artifacts.** `Artifacts.GetArtifact` checks the bytes against their digest and returns
  `ErrDigestMismatch` rather than bytes you must not run.
- **Reasons.** Platform failures carry a `common.v1.ErrorReason`. `Reason(err)` and `IsReason(err, r)` read it;
  branch on that, never on the code or the message. `NewError` builds one, for the other side.

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
io.Copy(w, res.Body)
```

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

## Stability

The protocols are versioned in their package names and guarded by `buf breaking`; `v1` does not change
incompatibly. The Go API follows the module's version, which is pre-1.0: expect the surface above to hold (it is
the protocols' surface) and the helpers around it (`Response`, options) to be refined.

edgerun's own admin UI and `edgerunctl` are built on `NewControl`; the other in-repo components still call the
generated clients directly.
