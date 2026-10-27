# edgerun

An edge function runtime written in Go. Functions are WebAssembly modules; Postgres is the
system of record; the admin UI is built with [webui](https://github.com/Olian04/webui).

The defining property is **swappability**: the frontend, orchestrator and worker are separate
programs that know each other only through strict, versioned protocols. Replace any one of them
(different language, different sandbox) and the others cannot tell, as long as the protocol is
kept.

```
 client ──HTTP──▶ frontend ──ingress.v1──▶ orchestrator ──worker.v1──▶ worker (wazero)
                                              ▲   ▲  │
                         admin UI / CLI ─control.v1┘  └─ worker.v1 (worker dials in)
                                                  ▼
                                              Postgres
```

| Component | Role | Talks to |
|---|---|---|
| frontend | terminates HTTP, forwards requests | orchestrator |
| orchestrator | routing, scheduling, all persistent state | Postgres, workers |
| worker | executes WASM modules under limits | orchestrator |
| admin | operator UI (webui) | orchestrator |

Only the orchestrator touches Postgres.

## Protocols

Defined in [`proto/edgerun`](proto), specified in [`docs/protocol.md`](docs/protocol.md), and
enforced by `buf lint` and `buf breaking` in CI. Start with the document.

## Status

Early. Protocols are defined; components are being built. See the milestones in the PR history.

## Development

Requires Go 1.27 (the `go` command downloads it automatically; webui needs it).

```sh
make generate   # regenerate gen/ from proto/
make proto-check # lint, breaking-change check, generated code is fresh
make test
```

Codegen tools (`buf`, `protoc-gen-go`, `protoc-gen-connect-go`) are pinned in `go.mod` and
`tools/go.mod`; nothing needs installing globally.

## License

MIT
