# Findings: a network service over WireGuard, built as an outside developer

A probe, written by an agent acting as a community developer: an `egress.v1` network service whose upstream
connections all go through a userspace WireGuard tunnel (wireguard-go netstack, names resolved inside the tunnel),
registered with the orchestrator through the library. It used the library for everything the library offers and could
not import edgerun's own egress implementation. Each item says what became of it in this repository.

Code size: about 1,300 lines of protocol-rule code that has nothing to do with WireGuard (allow-list grammar 500,
address classes 125, the Fetch handler 410, registration lifecycle 250), about 650 of WireGuard glue and flags, about
1,900 of tests and test tooling.

Tests: `go test -race ./...` passes. A real tunnel without root (two in-process wireguard-go peers; the origin is
unreachable except through it, a relay shows no plaintext crosses the wire), the whole chain (real orchestrator and
worker, the wagi-2 `examples/fetch` guest: closed by default, allow-list denial, a forbidden address returned by DNS,
drain and deregister), and the conformance executable against the service (`--role egress`): all cases pass but one
that the suite skips unless its origin is on the service's loopback. Re-verified after the changes below.

**Library verdict.** `Authenticate`, `NewError`, `InvalidArgument`, the `Scope*` constants, `Registry` and `NewEgress`
worked first time with no gap. Everything the library does not do is where the work and the risk were, and all of it is
protocol obligation, not local policy: the grammars, the forbidden classes, pinning, bounding, error classification
and the lifecycle. The probe's advice, which I follow: do not add a lifecycle helper or stream helpers; put the rules
in the docs with exact tables and test vectors; expose pure functions in the library only if the table exists too.

## Findings, ranked, with what became of each

1. **Forbidden address classes: the docs named classes, not ranges (security-sensitive). Fixed.** The probe wrote 15 IPv4
   prefixes and an IPv6 rule from the prose, and the docs did not decide 0/8, 192.0.0.0/24, 192.88.99.0/24, NAT64, or how
   operator-allowed ranges meet mapped forms. The suite tested one address per class: mutants that narrowed 172.16/12,
   100.64/10, 198.18/15 and 224/4 still passed. Now: `protocol.md` has the exact table (IPv6 refused everywhere outside
   `2000::/3`, which includes NAT64) and the operator-allow rule; the suite tests the first and last address of every
   range and several embeddings (every narrowing mutant I tried is caught). The community service itself allowed NAT64 to a
   public IPv4 because the docs were silent; it now follows the table. Not done: exposing a `Forbidden(netip.Addr)` in the
   library. It is defensible once the table is in the docs; a library function would only help Go.
2. **Pinning versus wireguard-go (security-sensitive). Fixed (docs).** `tnet.DialContext("name:80")` resolves and dials
   in one call, so checking the resolved addresses and then dialing the name resolves twice and uses the second answer.
   The suite cannot see this (it has no DNS control). `protocol.md` now says: resolve once, check every address, connect to
   one of the addresses checked; a dial that takes a name is wrong; a service resolving through a tunnel or proxy must not
   consult the local resolver. The service splits lookup from dial and tests it (two DNS queries for the naive version,
   one for the correct one).
3. **The allow-list and coverage grammar had open corners (security-sensitive). Fixed.** About 500 lines hand-written, with
   undecided: bare IPv6 entries, legacy numeric hosts (`127.1`, `2130706433`, `0x7f.1`: resolvers read them as
   127.0.0.1, `net/url` as names), label charset and length, `*.com`, ports, trailing dots, zone ids, spaces in the URL
   (`net/url` keeps a space in the query and the request line is then malformed). Now: the rules are in the grammar text
   with accepted and refused test vectors, the reference **refuses a host whose last label is a number and any URL
   byte that is a space, control or non-ASCII (its own bug as well)**, and the suite has cases for each, `CONNECT`
   (refused) included.
4. **A tunnel-only service cannot be judged without a workaround (suite). Partly fixed.** One `--advertise` is both the
   registry URL and the origin host, origin ports are random, and a broken setup ran for 6+ minutes of 20 s timeouts.
   Fixed: the egress suite now fetches its own web server through the service first and stops with a message saying why
   if it cannot; `protocol.md` says how to judge a service behind a tunnel or proxy. Open: separate `--origin-advertise`
   and `--origin-ports` options (the probe's workaround was a TCP forwarder at the far end of the tunnel), and the
   name-resolution path is untested for services with their own resolver.
5. **Rules the suite enforced that the docs did not state. Fixed (docs).** Canonical coverage registered by the service
   itself, `Describe` with non-zero limits and a token, a declared over-large `Content-Length` refused before the head,
   never more than the bound delivered, exactly the bound allowed, `localhost` refused by name (in-tunnel DNS says
   NXDOMAIN, so resolution-based refusal gives `EGRESS_FAILED`), and the transport read cap for the request. All in
   `protocol.md` now. The reference refuses `localhost` and `*.localhost` by name too.
6. **Lifecycle under-specified (docs; library: no).** Which registrations count as refused, how long to keep serving after
   `DRAINING`, `reregister = true` while draining. Fixed: the docs say a draining service keeps answering for at least a
   heartbeat interval (grants are made when an invocation starts) and does not register again; **the reference did
   register again, and the worker did too, which would list it as ready while it was leaving: both fixed and tested**;
   the egress service gained `--drain-grace`. `ExampleRegistry` is now the complete lifecycle (retry, heartbeat,
   re-register, drain) and is deliberately not a library function.
7. **net/http defaults versus the protocol (docs). Fixed.** A service must switch off compression, ignore environment
   proxies, suppress the default `User-Agent`, not follow redirects, build the URL from validated parts. The docs now say
   the request the origin sees is the guest's with exactly the documented changes and nothing added; **the reference added
   Go's `User-Agent` and no longer does**, and the suite checks that nothing is added.
8. **Docs slips. Fixed.** `--secret-key` and `--database-url` marked required; the conformance executable also builds from
   a module (`go build github.com/Olian04/edgerun/cmd/edgerun-conformance`, without the guests).

Minor, open: `Authenticate` exposes no matched scope and no failure hook; a single shared `egress` token (already a
known gap).

## What worked well

The library was the smallest part of the job, by design. The suite, run before reading the docs again, found real
mistakes. Registration against the real orchestrator, `NewError`'s canonical codes and `Authenticate` (unary and
streaming, constant-time) all behaved as documented.

## Environment notes

The sandbox has a transparent proxy that completes TCP :80 handshakes to any 10.x address, so the tunnel test asserts
"not the origin's content and the origin heard nothing" rather than "connect fails". wireguard-go's netstack hides its
gVisor stack, which only affects test tooling.
