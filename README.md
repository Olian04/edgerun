# edgerun

An edge function runtime written in Go. Functions are WebAssembly modules run under wazero;
Postgres is the system of record; the admin UI is built with
[webui](https://github.com/Olian04/webui). Functions have no network until their operator lists
the destinations they may reach, and then they reach them only through a network service: one per network, each saying what it carries.

The defining property is **swappability**: the frontend, orchestrator and worker are separate
programs that know each other only through strict, versioned protocols. Replace any one of them
(different language, different sandbox) and the others cannot tell, as long as the protocol is
kept. Every role that implements a protocol has a conformance suite that judges it from outside, and
the end-to-end tests swap the real worker for a hand-written fake.

```
 client ──HTTP──▶ frontend ──ingress.v1──▶ orchestrator ──worker.v1──▶ worker (wazero) ──egress.v1──▶ network service ──▶ a network
                                              ▲   ▲  │ ▲                                                  (one per network; several may exist)
                         admin UI / CLI ─control.v1┘  │ └─ egress.v1: network services register and say what they carry
                                                      └─ worker.v1 (worker dials in)
                                                  ▼
                                              Postgres
```

| Component | Binary | Role | Talks to |
|---|---|---|---|
| frontend | `cmd/frontend` | terminates HTTP, forwards requests | orchestrator |
| orchestrator | `cmd/orchestrator` | routing, scheduling, all persistent state | Postgres, workers |
| worker | `cmd/worker` | executes WASM modules under limits | orchestrator |
| network service | `cmd/egress` | makes guests' outbound requests, for allowed destinations only, within what it carries | orchestrator (registers), a network |
| admin | `cmd/admin` | operator UI (webui) | orchestrator |
| CLI | `cmd/edgerunctl` | scripted operator access | orchestrator |

Each seam is one proto package and a component implements one side of it. Only the orchestrator
touches Postgres; the admin UI and the CLI are ordinary clients of `control.v1`.

## Try it

You need Go 1.27 (the `go` command fetches it) and the PostgreSQL server binaries
(`initdb`, `pg_ctl`). No Docker required.

```sh
scripts/devstack.sh start
```

That builds everything, starts a private Postgres and one of each component, and deploys the
[`examples/hello`](examples/hello) function:

```sh
curl -H 'Host: hello.localhost' 'http://127.0.0.1:18000/?name=you'     # Howdy, you!
```

The stack also runs two network services, one that carries `127.0.0.0/8` (standing in for a private network) and
one that carries everything else, and a `fetch` function that uses them (`edgerunctl networks` shows them):

```sh
curl -H 'Host: fetch.localhost' 'http://127.0.0.1:18000/?url=http://127.0.0.1:18081/healthz'   # allowed: 200
curl -H 'Host: fetch.localhost' 'http://127.0.0.1:18000/?url=http://127.0.0.1:19090/healthz'   # not listed: 403
```

Open <http://127.0.0.1:19090/> and sign in as `alice` / `admin` (or `bob` / `viewer`, read-only).
`scripts/devstack.sh stop` stops it, `clean` also deletes the data. The credentials are fixed
and public: this is for development only.

### Deploying your own function

Any WASI preview-1 command module works (the `wagi-1` ABI, specified in
[`docs/protocol.md`](docs/protocol.md)): the request arrives as CGI environment variables and
stdin, the response is written to stdout.

```go
// examples/hello/main.go, abridged
fmt.Print("Content-Type: text/plain; charset=utf-8\n\n")      // headers, blank line
fmt.Printf("%s, %s!\n", os.Getenv("GREETING"), name)             // then the body
```

```sh
GOOS=wasip1 GOARCH=wasm go build -o hello.wasm ./examples/hello

go build -o /tmp/edgerunctl ./cmd/edgerunctl && PATH=/tmp:$PATH
export EDGERUN_URL=http://127.0.0.1:18080 EDGERUN_ADMIN_TOKEN=dev-admin-token
edgerunctl create greeter
edgerunctl deploy greeter hello.wasm --activate    # new versions are immutable; deploy never edits one
edgerunctl route greeter.localhost / greeter
edgerunctl set greeter GREETING Howdy              # --secret encrypts the value at rest
edgerunctl activate greeter 1                      # rollback is just activating another version
```

Then `curl -H 'Host: greeter.localhost' http://127.0.0.1:18000/`. `edgerunctl` with no arguments lists every command. The admin UI does the same things.

### Giving a function a network

A function reaches nothing by default. Build it for the `wagi-2` ABI (the same as `wagi-1`, plus an
`edgerun_http` host module; see [`examples/fetch`](examples/fetch)), deploy it as `wagi-2`, and list where
it may go:

```sh
GOOS=wasip1 GOARCH=wasm go build -o fetch.wasm ./examples/fetch
edgerunctl create fetch
edgerunctl deploy fetch fetch.wasm --abi wagi-2 --activate
edgerunctl allow fetch api.example.com '*.cdn.example.com' 192.0.2.7:8443   # host, *.domain, or address[:port]
edgerunctl egress fetch                                                      # what it may reach
edgerunctl deny fetch api.example.com
```

Without a port an entry matches only the scheme's default (80, 443). The list belongs to the function, not to a
version, and applies to the next invocation. Each listed destination is given to the registered network service
that carries it, the most specific one: an exact host before a `*.domain` before an address range before a
catch-all. Whatever the list says, a service refuses loopback, private, link-local (cloud metadata), multicast and
other non-public addresses unless its operator allows a range with `--allow-network`, and carries only what it
declared with `--covers`. So one service can sit inside a private network (`--covers 10.0.0.0/8,*.corp.internal
--allow-network 10.0.0.0/8`), another on the public internet (`--covers '*'`), a third behind a proxy, all
serving the same orchestrators. The rules are in [`docs/protocol.md`](docs/protocol.md), "Network access".

## Configuration

Every setting is a flag and an environment variable (a flag wins). Tokens are bearer tokens with
one scope each; the same value must be given to both ends of a seam. Use TLS between components in
production: the tokens do not replace it.

### orchestrator

| Flag | Environment | Default | |
|---|---|---|---|
| `--database-url` | `EDGERUN_DATABASE_URL` | **required** | Postgres connection string. Migrations run at start under an advisory lock, so several instances may start together. |
| `--listen` | `EDGERUN_ORCHESTRATOR_LISTEN` | `:8080` | Serves ingress, fleet, artifact and control, plus `/healthz`. |
| `--ingress-token` | `EDGERUN_INGRESS_TOKEN` | | Presented by frontends. |
| `--worker-token` | `EDGERUN_WORKER_TOKEN` | | Presented by workers. |
| `--admin-token` | `EDGERUN_ADMIN_TOKEN` | | Presented by the admin UI and the CLI. |
| `--execution-token` | `EDGERUN_EXECUTION_TOKEN` | | Presented *to* workers. |
| `--network-token` | `EDGERUN_NETWORK_TOKEN` | | Presented by network services when they register. Without one none may, and functions have no network. |
| `--secret-key` | `EDGERUN_SECRET_KEY` | **required** | base64 of 32 random bytes; seals secret variables (AES-GCM). Losing it loses the secrets. |
| `--heartbeat-interval` | | `5s` | Dictated to workers. A worker is alive for three intervals. |
| `--max-request-bytes` | | `6 MiB` | Largest request body accepted; frontends learn it from `Describe`. |
| `--invocation-retention` | | `336h` | Older invocation records are deleted. |

The orchestrator is stateless: run as many as you like against one database.

### worker

| Flag | Environment | Default | |
|---|---|---|---|
| `--orchestrator-url` | `EDGERUN_ORCHESTRATOR_URL` | | |
| `--advertise-url` | `EDGERUN_WORKER_ADVERTISE_URL` | | How the orchestrator reaches this worker. |
| `--listen` | `EDGERUN_WORKER_LISTEN` | `:9000` | |
| `--worker-token` | `EDGERUN_WORKER_TOKEN` | | Presented to the orchestrator. |
| `--execution-token` | `EDGERUN_EXECUTION_TOKEN` | | Required of the orchestrator. |
| `--max-concurrency` | `EDGERUN_WORKER_MAX_CONCURRENCY` | `8` | Further requests get `NO_CAPACITY` at once; nothing queues. |
| `--compilation-cache-dir` | `EDGERUN_WORKER_COMPILATION_CACHE_DIR` | | Keeps compiled code across restarts. The first compile of a module costs about a second; later ones ~25 ms. |
| `--egress-token` | `EDGERUN_EGRESS_TOKEN` | | Presented to network services, which the orchestrator routes a function's outbound requests to (the worker needs no list of them). Without one, guests have no network and the worker offers `wagi-1` only. |
| `--label key=value` | | | Repeatable. |
| `--drain-timeout` | | `30s` | On SIGTERM a worker stops taking work, finishes what it has, deregisters. |

### egress (a network service)

| Flag | Environment | Default | |
|---|---|---|---|
| `--listen` | `EDGERUN_EGRESS_LISTEN` | `:8090` | |
| `--advertise-url` | `EDGERUN_EGRESS_ADVERTISE_URL` | the listen address | The URL workers reach it at. |
| `--orchestrator-url` | `EDGERUN_ORCHESTRATOR_URL` | | Where it registers. It keeps trying until it does, and deregisters when stopped. |
| `--network-token` | `EDGERUN_NETWORK_TOKEN` | | Presented to the orchestrator. |
| `--egress-token` | `EDGERUN_EGRESS_TOKEN` | | Required of workers. |
| `--covers` | `EDGERUN_EGRESS_COVERS` | | **Required.** What it carries, comma-separated: `*` (everything), hosts, `*.domain`, addresses, ranges such as `10.0.0.0/8`. It refuses anything else. |
| `--allow-network` | `EDGERUN_EGRESS_ALLOW_NETWORKS` | | A CIDR range it may connect to although it refuses it by default (repeatable; comma-separated in the variable). Put the service where the network is, and open only what functions need. |
| `--max-request-bytes` | | 1 MiB | Largest request body it forwards. |
| `--max-response-bytes` | | 4 MiB | Largest response body it returns. |

### frontend

| Flag | Environment | Default | |
|---|---|---|---|
| `--orchestrator-url` | `EDGERUN_ORCHESTRATOR_URL` | | |
| `--ingress-token` | `EDGERUN_INGRESS_TOKEN` | | |
| `--listen` | `EDGERUN_FRONTEND_LISTEN` | `:8000` | Public HTTP. |
| `--health-listen` | `EDGERUN_FRONTEND_HEALTH_LISTEN` | `:8081` | `/healthz`; empty disables. |
| `--request-timeout` | | `30s` | Whole-invocation budget. |
| `--trust-forwarded` | | off | Believe `X-Forwarded-*`. Only behind a proxy you control. |

### admin

| Flag | Environment | Default | |
|---|---|---|---|
| `--orchestrator-url` | `EDGERUN_ORCHESTRATOR_URL` | | |
| `--admin-token` | `EDGERUN_ADMIN_TOKEN` | | |
| `--listen` | `EDGERUN_ADMIN_LISTEN` | `:9090` | |
| `--users` | `EDGERUN_ADMIN_USERS` | | `name:role:bcrypt-hash` entries separated by `;`; role is `admin` or `viewer`. Make a hash with `admin hash-password`. |
| `--session-key` | `EDGERUN_ADMIN_SESSION_KEY` | random | base64 of 32+ bytes. Set it to keep sessions across restarts or share them between instances. |
| `--secure-cookies` | | off | Set when served over HTTPS. |
| `--auto-refresh` | | `10s` | The read-only pages (overview, workers, invocations) reload themselves this often, server-side via the `Refresh` header; pages with forms never do. `0` disables. |

webui has no authentication of its own, so the admin binary puts its own login in front of it:
bcrypt passwords, signed expiring session cookies, a per-IP login limiter, and a read-only role.

### edgerunctl

`--url` (`EDGERUN_URL`) is the orchestrator and `--token` (`EDGERUN_ADMIN_TOKEN`) the admin token.

## Protocols

Defined in [`proto/edgerun`](proto), specified in [`docs/protocol.md`](docs/protocol.md), and
enforced by `buf lint` and `buf breaking` in CI. The document covers what a schema cannot say:
error taxonomy and the frontend's HTTP mapping, deadlines, retry rules, capability negotiation,
limits and the guest ABI. To write your own frontend, orchestrator or worker, start there.

`internal/e2e` shows what a replacement must do: `swap_test.go` runs the unchanged orchestrator
against a worker written from scratch against `worker.v1`. And every role has a black-box suite,
`conformance/{frontend,orchestrator,worker,egress}`, that plays everything around the implementation and judges
what crosses the wire: pass it and you can replace the in-repo component. They run from one executable that is
pointed at an address and told the role:

```sh
make conformance      # bin/edgerun-conformance, with the test guests compiled in
bin/edgerun-conformance --role orchestrator --target 10.0.0.5:8080
bin/edgerun-conformance --role worker --target 10.0.0.6:9000 --listen :7000 --advertise 10.0.0.2 --print-config
```

`--print-config` says how to configure the implementation for the role (what the suite will present and require,
and where the suite is). See "Conformance" in [`docs/protocol.md`](docs/protocol.md).

## Library

`github.com/Olian04/edgerun` is a Go client library over the generated protocols, for extending edgerun from
outside: one typed client per seam (control, ingress, worker execution/fleet/artifacts, egress and its registry),
so a program of yours can be an operator, a frontend, a worker or a network service of its own.

```go
ctl := edgerun.NewControl("http://orchestrator:8080", adminToken)
fn, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello"})

ing := edgerun.NewIngress("http://orchestrator:8080", ingressToken)
res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "hello.example.com", Path: "/"}})
```

It is the protocols and nothing more (an RPC, a method), plus tokens, iterators over paged lists, streamed
bodies as readers and typed errors. See [`docs/library.md`](docs/library.md).

Not a Go shop? The protocols are plain protobuf over Connect; see [`docs/other-languages.md`](docs/other-languages.md)
(a worker in Node.js passes the conformance suite).

## Observability

Traces, metrics and logs are exported as [OpenTelemetry](https://opentelemetry.io) says: set the standard
`OTEL_EXPORTER_OTLP_ENDPOINT` (and friends) on any component and a request shows up as one trace from the frontend
to the origin a function called, with `edgerun.*` metrics and logs correlated to the trace. Off unless asked for;
the trace is passed on either way. See [`docs/observability.md`](docs/observability.md).

## Development

```sh
make generate     # regenerate gen/ from proto/
make proto-check  # lint, breaking-change check, generated code is fresh
make test         # needs PostgreSQL binaries, or EDGERUN_TEST_DATABASE_URL (a superuser DSN); includes every conformance suite
make build
make conformance  # bin/edgerun-conformance
```

`buf`, `protoc-gen-go` and `protoc-gen-connect-go` are pinned in `go.mod` and `tools/go.mod`;
nothing needs installing globally. Tests start their own private Postgres unless
`EDGERUN_TEST_DATABASE_URL` is set; `EDGERUN_REQUIRE_PG=1` turns a missing database into a failure
instead of a skip (CI sets both).

[`deploy/compose.yaml`](deploy/compose.yaml) runs the whole stack in containers. It was written
without a Docker daemon available, so it is **untested**; treat it as a starting point.

## Limits and known gaps

- Guests' network is HTTP and HTTPS only (no raw sockets, no WebSockets), buffered (a request up to 1 MiB, a
  response up to 4 MiB, at most 16 requests an invocation), and redirects are the guest's to follow. It exists
  only for `wagi-2` modules; a Go guest needs `//go:wasmimport` to reach it (see `examples/fetch`).
- The worker enforces deadlines by checking them in compiled code, which slows a tight loop several
  times over. That is the price of reliably stopping a spinning guest.
- The orchestrator proxies every response, so it is on the data path. It is stateless and scales
  horizontally; a lease-based direct path would be `ingress.v2`.
- A few rules cannot be judged by the conformance executable from outside (a name that resolves to both a
  public and a forbidden address needs control of DNS) and are covered by the reference implementation's own
  tests. The suites need the implementation to be configured to reach them; `--print-config` says how.
- Network services share one `egress` token with the workers, and one `network` token with the orchestrator:
  a service cannot be given its own credential per worker yet.
- The admin UI's auto-reload is the browser reloading a page every few seconds, and only on pages without
  forms.

## License

MIT
