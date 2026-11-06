# edgerun platform services

edgerun is a set of **components** (programs), and each component hosts a set of **platform services**. This
document is the reference for what those services are and how the components fit together through them. It is the
map; [`protocol.md`](protocol.md) is the territory (every rule of every service), the `.proto` files in
[`proto/edgerun`](../proto/edgerun) are the message shapes, and [`conformance/`](../conformance) is the judge.

## The idea

A **platform service** is exactly four things, and nothing is a service unless it has all four:

1. **One protocol**: a proto service (or, for the one service that faces the outside world, a written contract).
2. **The duties of its host**: what a component that hosts it must do, written in `protocol.md`.
3. **The duties of its client**: what a component that calls it must do, written beside the host's.
4. **One conformance suite** that judges a host from outside. Where the client's duties can be tested, the same
   package also judges a client against a scripted host.

A **component** is a process that hosts services and is a client of others. Components are replaceable because they
are defined by nothing but the services they host: an implementation that hosts the same services, keeping both
sides of every rule, is the same component to everyone else. The four **profiles** (frontend, orchestrator, worker,
network service) are the sets of services a component of that kind hosts; the admin UI and the CLI host nothing, and
are only clients.

## The catalogue

| Service | Protocol | Hosted by | Called by |
|---|---|---|---|
| **Edge** | HTTP; the checklist in [The Edge contract](protocol.md#the-edge-contract-what-a-frontend-does) | frontend | the outside world |
| **Ingress** | `ingress.v1` `IngressService` | orchestrator | frontend |
| **Execution** | `worker.v1` `ExecutionService` | worker | orchestrator |
| **Membership** | `membership.v1` `MembershipService` | orchestrator | worker, network service, operator (to list) |
| **Artifacts** | `worker.v1` `ArtifactService` | orchestrator | worker |
| **Egress** | `egress.v1` `EgressService` | network service | worker |
| **Catalog** | `control.v1` `FunctionService` | orchestrator | operator tools |
| **Routing** | `control.v1` `RouteService` | orchestrator | operator tools |
| **Plans** | `control.v1` `PlanService` | orchestrator | operator tools |
| **Records** | `control.v1` `RecordsService` | orchestrator | operator tools |

```
                 Edge                  Ingress                       Execution
  client ───────────────▶ frontend ───────────────▶ orchestrator ───────────────▶ worker
                                                     ▲  ▲  ▲  ▲                    │
          Catalog, Routing, Plans, Records (control.v1) ┘  │  │ Membership, Artifacts
                          operator tools ─────────────────┘  └──────────────────── worker ─ dials in
                                                     ▲  Membership                    │ Egress
                                                     └──────── network service ◀─────┘
```

Two things about the shape of the table. First, **a seam is a service, not a component pair**: the orchestrator
hosts six services, and it is the host of Membership whether the caller is a worker or a network service. Second,
every service is hosted by exactly one kind of component. Some services have more than one *kind* of client; none has
more than one kind of host.

### Components and what they host

| Component | Hosts | Is a client of |
|---|---|---|
| frontend | Edge | Ingress |
| orchestrator | Ingress, Membership, Artifacts, Catalog, Routing, Plans, Records | Execution |
| worker | Execution | Membership, Artifacts, Egress |
| network service | Egress | Membership |
| admin UI, CLI | nothing | Catalog, Routing, Plans, Records, Membership (list) |

Only the orchestrator touches Postgres. A component says which services it hosts in the `protocols` of the
`Implementation` it reports from `Describe`, as proto packages (`edgerun.ingress.v1`, `edgerun.membership.v1`, …):
peers negotiate by what a component declares, never by what it is called.

## The services

### Edge

The one service with no schema: the outside world speaks HTTP, and HTTP is already specified. What the platform adds
is **how HTTP maps onto `Invoke`** and back, so that every frontend treats a request the same way: which headers
survive, what `X-Forwarded-*` means, when a request is refused, how each platform error becomes a status.
[The Edge contract](protocol.md#the-edge-contract-what-a-frontend-does) writes this as a numbered checklist (`E1`,
`E2`, …) instead of a schema, and each rule can be judged by sending a request and watching the answer and the call
the frontend makes to its orchestrator. A change to the Edge contract is as much a compatibility event as a change to
a proto file: rules are added with new numbers, a rule that must go leaves its number unused, and a rule is never
reworded so as to turn a conforming frontend into a failing one.

### Ingress

`Invoke` carries one HTTP request to the orchestrator and streams the response back. The host (orchestrator) routes
by host and path, schedules, proxies, records the invocation and classifies every failure; the client (frontend)
supplies the request, its budget and its credentials, and treats the typed errors as the Edge contract says.

### Execution

`Execute` runs one module for one request under limits and streams the result. The host (worker) enforces the
deadline for real, admits no more than its capacity, and never runs bytes whose digest does not match; the client
(orchestrator) retries the next candidate on `NO_CAPACITY`. `Describe` is how a worker states its guest ABIs.

### Membership

Workers and network services **dial in** and register; the orchestrator never needs to be told where anything is.
One protocol, two kinds of member, one set of rules: a member registers once, heartbeats at the interval it was
given, is alive for three intervals, says `DRAINING` when it is leaving, and deregisters. The kind a caller may
register is decided by its token's scope, per call; only an operator lists. This is the service that makes a fleet
elastic, and it is the only way an orchestrator learns about workers and network services.

### Artifacts

`GetArtifact` streams a module by content digest. Digests make it trivially cacheable, and the client verifies the
hash and never runs bytes that do not match it.

### Egress

A guest has no network. `Fetch` is how a worker asks a network service to make one HTTP request for a guest: the
host decides whether the request may leave (its coverage, its address checks, its limits) and makes it. The
orchestrator, not the worker, picks which service carries which destination; Membership is how it learns what each
carries.

### Catalog, Routing, Plans, Records

The operator's view of the orchestrator, all in `control.v1`, split by what is being managed so that each can be
judged on its own:

- **Catalog** (`FunctionService`): functions, versions, environment, egress allow-lists, labels, and `Describe`,
  which tells a caller the orchestrator's implementation and limits.
- **Routing** (`RouteService`): which host and path reach which function.
- **Plans** (`PlanService`): validate and apply a sequence of changes as a unit.
- **Records** (`RecordsService`): the invocation records, listed or fetched by id, including the way from a
  response (or a failure) to its record.

## What every service owes, and how it is judged

- **Capability, not identity.** A component declares the protocol packages it hosts; clients decide from that.
- **Typed failure.** A service fails with the codes and reasons `protocol.md` lists for it, and nothing branches on
  the text of a message.
- **Authentication per call.** Every call carries a bearer token with a scope; a missing or unknown token is
  `UNAUTHENTICATED`, a valid token of the wrong scope is `PERMISSION_DENIED`. Where a service has several callers
  (Membership), the scope is judged per call.
- **A suite that can fail.** A conformance suite is only evidence if it is known to fail on a broken host. A new
  rule is introduced with a test, and the test is checked by breaking the implementation on purpose.

### Conformance, by service

Each service has its own suite in `conformance/<service>`, a package that depends on the generated protocol code and
nothing else. A suite is black-box: it plays every other component itself and looks only at what crosses the wire.
Run one service against a host with `edgerun-conformance --service <name> --target <address>`, or all of the services
a kind of component hosts with `--profile <component>`; see [Conformance in `protocol.md`](protocol.md#conformance).
Edge's suite judges a frontend against the numbered checklist, playing the orchestrator behind it, so it also
proves the frontend's duties as a client of Ingress: there is no separate client suite for Ingress.

## Adding a service

A change that adds a service must add, in the same change: the protocol (a proto service, or numbered rules if it
faces the outside), the host's and the client's duties in `protocol.md`, a row in the catalogue above and the
component's row in its table, and a conformance suite with the mutation test that shows it can fail. Resist adding
to an existing service what is really a new duty for a different caller: a service that serves two purposes is two
services whose rules will drift. Resist, too, adding a service for what one more RPC of an existing one says.
