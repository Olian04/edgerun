# edgerun

An edge function runtime written in Go. Functions are WebAssembly modules run under wazero;
Postgres is the system of record; the admin UI is built with
[webui](https://github.com/Olian04/webui).

The defining property is **swappability**: the frontend, orchestrator and worker are separate
programs that know each other only through strict, versioned protocols. Replace any one of them
(different language, different sandbox) and the others cannot tell, as long as the protocol is
kept. The end-to-end tests prove it by swapping the real worker for a hand-written fake.

```
 client ──HTTP──▶ frontend ──ingress.v1──▶ orchestrator ──worker.v1──▶ worker (wazero)
                                              ▲   ▲  │
                         admin UI / CLI ─control.v1┘  └─ worker.v1 (worker dials in)
                                                  ▼
                                              Postgres
```

| Component | Binary | Role | Talks to |
|---|---|---|---|
| frontend | `cmd/frontend` | terminates HTTP, forwards requests | orchestrator |
| orchestrator | `cmd/orchestrator` | routing, scheduling, all persistent state | Postgres, workers |
| worker | `cmd/worker` | executes WASM modules under limits | orchestrator |
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

## Configuration

Every setting is a flag and an environment variable (a flag wins). Tokens are bearer tokens with
one scope each; the same value must be given to both ends of a seam. Use TLS between components in
production: the tokens do not replace it.

### orchestrator

| Flag | Environment | Default | |
|---|---|---|---|
| `--database-url` | `EDGERUN_DATABASE_URL` | | Postgres connection string. Migrations run at start under an advisory lock, so several instances may start together. |
| `--listen` | `EDGERUN_ORCHESTRATOR_LISTEN` | `:8080` | Serves ingress, fleet, artifact and control, plus `/healthz`. |
| `--ingress-token` | `EDGERUN_INGRESS_TOKEN` | | Presented by frontends. |
| `--worker-token` | `EDGERUN_WORKER_TOKEN` | | Presented by workers. |
| `--admin-token` | `EDGERUN_ADMIN_TOKEN` | | Presented by the admin UI and the CLI. |
| `--execution-token` | `EDGERUN_EXECUTION_TOKEN` | | Presented *to* workers. |
| `--secret-key` | `EDGERUN_SECRET_KEY` | | base64 of 32 random bytes; seals secret variables (AES-GCM). Losing it loses the secrets. |
| `--heartbeat-interval` | | `5s` | Dictated to workers. A worker is alive for three intervals. |
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
| `--label key=value` | | | Repeatable. |
| `--drain-timeout` | | `30s` | On SIGTERM a worker stops taking work, finishes what it has, deregisters. |

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
against a worker written from scratch against `worker.v1`, and `conformance/worker` is a black-box
suite that any worker implementation can be run against.

## Development

```sh
make generate     # regenerate gen/ from proto/
make proto-check  # lint, breaking-change check, generated code is fresh
make test         # needs PostgreSQL binaries, or EDGERUN_TEST_DATABASE_URL (a superuser DSN)
make build
```

`buf`, `protoc-gen-go` and `protoc-gen-connect-go` are pinned in `go.mod` and `tools/go.mod`;
nothing needs installing globally. Tests start their own private Postgres unless
`EDGERUN_TEST_DATABASE_URL` is set; `EDGERUN_REQUIRE_PG=1` turns a missing database into a failure
instead of a skip (CI sets both).

[`deploy/compose.yaml`](deploy/compose.yaml) runs the whole stack in containers. It was written
without a Docker daemon available, so it is **untested**; treat it as a starting point.

## Limits and known gaps

- **webui is pre-1.0** and pinned to an exact version. It is confined to one binary, so replacing
  it touches nothing else.
- Guests have **no network** (WASI preview 1 has no sockets) and their response is buffered by the
  worker before it is streamed. Neither needs a protocol change to lift.
- The worker enforces deadlines by checking them in compiled code, which slows a tight loop several
  times over. That is the price of reliably stopping a spinning guest.
- The orchestrator proxies every response, so it is on the data path. It is stateless and scales
  horizontally; a lease-based direct path would be `ingress.v2`.
- Only the worker has a conformance suite; the orchestrator and frontend are covered by the
  end-to-end tests but not yet by a suite a foreign implementation could run.

## License

MIT
