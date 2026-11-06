# Using edgerun from other languages

The Go library ([`library.md`](library.md)) is one client of the protocols, not the way in. The protocols are
plain protobuf over Connect (HTTP, one POST per call, server streaming for responses), specified in
[`protocol.md`](protocol.md); anything that speaks Connect can be an edgerun component. A worker written in
plain Node.js from these files alone passes the conformance suite
([`examples/community/nodeworker`](../examples/community/nodeworker)); that is the existence proof.

## Getting stubs

The schemas are the [`proto/`](../proto) directory, a [buf](https://buf.build) module (`buf.yaml` at the
repository root). Nothing is published to a registry yet, so generate from a checkout. For JavaScript or
TypeScript with Connect-ES 2.x (which needs only `protoc-gen-es`; there is no separate connect plugin):

```yaml
# buf.gen.yaml
version: v2
plugins:
  - local: node_modules/.bin/protoc-gen-es
    out: gen
    opt: [target=js+dts, import_extension=js]
inputs:
  - directory: path/to/edgerun/proto
```

```sh
npm i @bufbuild/protobuf @connectrpc/connect @connectrpc/connect-node
npm i -D @bufbuild/buf @bufbuild/protoc-gen-es
npx buf generate
```

Other languages: any generator for protobuf messages plus a Connect (or gRPC-Web) client and server runtime.

## What you must do yourself

The Go library's helpers exist because every caller needs them; in another language they are yours to write,
and the rules are in `protocol.md`:

- the bearer token on every call (`Authorization: Bearer <token>`), one scope per token;
- errors: Connect code plus a `common.v1.ErrorInfo` detail carrying the reason; branch on the reason (the
  control protocol has none: its errors are plain codes);
- deadlines: the RPC deadline (`Connect-Timeout-Ms`) is the invocation's wall-clock budget;
- digests: verify `sha256:<hex>` of every artifact before using it;
- a response stream is exactly one head then chunks; a failure after the head must not look like a clean end.

## Judging your implementation

Build the conformance executable (needs a Go toolchain; `make conformance` embeds the test guests) and point it at
yours with the role it plays:

```sh
make conformance
bin/edgerun-conformance --role worker --target 127.0.0.1:9000 --print-config   # how to configure your worker for the suite
bin/edgerun-conformance --role worker --target 127.0.0.1:9000 --listen :7000 --stop-cmd 'kill -TERM $(cat worker.pid)'
```

Pass it and your implementation can replace the in-repo one.
