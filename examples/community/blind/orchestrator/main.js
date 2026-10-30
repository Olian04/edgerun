// An orchestrator for edgerun, in memory, written from docs/protocol.md alone.
//
//   node orchestrator/main.js --listen 127.0.0.1:8081 --ingress-token I --worker-token W --admin-token A \
//        --network-token N --execution-token X --heartbeat-interval 200ms
import http from "node:http";
import { randomUUID } from "node:crypto";
import { create } from "@bufbuild/protobuf";
import { DurationSchema, timestampFromDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { connectNodeAdapter, createConnectTransport } from "@connectrpc/connect-node";
import { IngressService, DescribeResponseSchema as IngressDescribeResponseSchema, InvokeResponseSchema, InvokeHeadSchema } from "../gen/edgerun/ingress/v1/ingress_pb.js";
import { ExecutionService, ExecuteRequestSchema, EgressGrantSchema } from "../gen/edgerun/worker/v1/execution_pb.js";
import { FleetService, RegisterResponseSchema, HeartbeatResponseSchema, DeregisterResponseSchema } from "../gen/edgerun/worker/v1/fleet_pb.js";
import { ArtifactService, GetArtifactResponseSchema } from "../gen/edgerun/worker/v1/artifact_pb.js";
import { WorkerState } from "../gen/edgerun/worker/v1/worker_pb.js";
import { ServiceState, RegistryService, RegisterResponseSchema as RegRegisterResponseSchema, HeartbeatResponseSchema as RegHeartbeatResponseSchema, DeregisterResponseSchema as RegDeregisterResponseSchema, ServiceInfoSchema } from "../gen/edgerun/egress/v1/registry_pb.js";
import { FunctionService } from "../gen/edgerun/control/v1/function_pb.js";
import { RouteService } from "../gen/edgerun/control/v1/route_pb.js";
import { PlanService } from "../gen/edgerun/control/v1/plan_pb.js";
import { InspectionService, DescribeResponseSchema as ControlDescribeResponseSchema, ListWorkersResponseSchema, GetInvocationResponseSchema, ListInvocationsResponseSchema, ListNetworkServicesResponseSchema } from "../gen/edgerun/control/v1/inspection_pb.js";
import { InvocationSchema, NetworkServiceStatusSchema, WorkerStatusSchema } from "../gen/edgerun/control/v1/model_pb.js";
import { EnvVarSchema, ErrorReason, ImplementationSchema } from "../gen/edgerun/common/v1/common_pb.js";
import { authInterceptor, bearer, errorInfo, invalidArgument, notFound, parseDuration, parseFlags, platformError } from "../lib/common.js";
import { canonicalCovers, carryRank, parseAllow, parseCover, rendezvous } from "../lib/egressgrammar.js";
import { Control, digestOf, fnMsg } from "./control.js";

const flags = parseFlags(process.argv.slice(2), {
  listen: "127.0.0.1:8081",
  "ingress-token": "ingress", "worker-token": "worker", "admin-token": "admin", "network-token": "network", "execution-token": "execute",
  "heartbeat-interval": "5s", "max-module-bytes": String(8 * 1024 * 1024), "max-request-body-bytes": String(6 * 1024 * 1024),
});
const HEARTBEAT_MS = parseDuration(flags["heartbeat-interval"]);
const MAX_MODULE = Number(flags["max-module-bytes"]);
const MAX_BODY = Number(flags["max-request-body-bytes"]);
const MiB = 1024 * 1024;

const control = new Control({ maxModuleBytes: MAX_MODULE });
const workers = new Map(); // id -> worker
const services = new Map(); // id -> network service
const invocations = []; // oldest first, each with .seq
let seq = 0;
const unreachable = new Map(); // endpoint -> until (ms)

const impl = create(ImplementationSchema, {
  name: "edgerun-orchestrator-blind", version: "0.1.0",
  protocols: ["edgerun.ingress.v1", "edgerun.worker.v1", "edgerun.control.v1", "edgerun.egress.v1"],
});
const durationMsg = () => create(DurationSchema, { seconds: BigInt(Math.floor(HEARTBEAT_MS / 1000)), nanos: Math.round((HEARTBEAT_MS % 1000) * 1e6) });
const alive = (x) => Date.now() - x.lastHeartbeat <= 3 * HEARTBEAT_MS;

function checkEndpoint(ep) {
  // not `new URL`: it reads "http:///x" as host "x"
  const m = /^https?:\/\/([^/?#]*)/i.exec(ep);
  const host = m && m[1].slice(m[1].lastIndexOf("@") + 1).replace(/:\d*$/, "");
  if (!m || !host || /[\s\u0000-\u001f\u007f]/.test(ep)) throw invalidArgument("endpoint must be an http(s) URL with a host");
  try {
    new URL(ep);
  } catch {
    throw invalidArgument("endpoint is not a URL");
  }
}

// =====================================================================================================
// Fleet
// =====================================================================================================
const fleetImpl = {
  register(req) {
    checkEndpoint(req.endpoint);
    const info = req.info;
    if (!info || info.abis.length === 0 || info.maxConcurrency <= 0) throw invalidArgument("a worker needs at least one ABI and a positive max_concurrency");
    for (const [id, w] of workers) if (w.endpoint === req.endpoint) workers.delete(id);
    const w = { id: randomUUID(), endpoint: req.endpoint, info, state: WorkerState.READY, inFlight: 0, lastHeartbeat: Date.now() };
    workers.set(w.id, w);
    unreachable.delete(req.endpoint);
    return create(RegisterResponseSchema, { workerId: w.id, heartbeatInterval: durationMsg() });
  },
  heartbeat(req) {
    if (req.state !== WorkerState.READY && req.state !== WorkerState.DRAINING) throw invalidArgument("state must be READY or DRAINING");
    const w = workers.get(req.workerId);
    if (!w) return create(HeartbeatResponseSchema, { reregister: true });
    w.state = req.state;
    w.inFlight = req.inFlight;
    w.lastHeartbeat = Date.now();
    return create(HeartbeatResponseSchema, { reregister: false });
  },
  deregister(req) {
    workers.delete(req.workerId);
    return create(DeregisterResponseSchema, {});
  },
};

const artifactImpl = {
  async *getArtifact(req) {
    const buf = control.state.artifacts.get(req.digest);
    if (!buf) throw platformError(ErrorReason.ARTIFACT_NOT_FOUND, `no artifact ${req.digest}`);
    for (let i = 0; i < buf.length; i += MiB) yield create(GetArtifactResponseSchema, { chunk: buf.subarray(i, i + MiB) });
  },
};

// =====================================================================================================
// Network service registry
// =====================================================================================================
const registryImpl = {
  register(req) {
    checkEndpoint(req.endpoint);
    let covers;
    try {
      covers = canonicalCovers(req.info?.covers ?? []);
    } catch (e) {
      throw invalidArgument(e.message);
    }
    for (const [id, s] of services) if (s.endpoint === req.endpoint) services.delete(id);
    const s = {
      id: randomUUID(), endpoint: req.endpoint, covers, parsed: covers.map(parseCover),
      implementation: req.info?.implementation, state: ServiceState.READY, lastHeartbeat: Date.now(),
    };
    services.set(s.id, s);
    return create(RegRegisterResponseSchema, { serviceId: s.id, heartbeatInterval: durationMsg() });
  },
  heartbeat(req) {
    if (req.state !== ServiceState.READY && req.state !== ServiceState.DRAINING) throw invalidArgument("state must be READY or DRAINING");
    const s = services.get(req.serviceId);
    if (!s) return create(RegHeartbeatResponseSchema, { reregister: true });
    s.state = req.state;
    s.lastHeartbeat = Date.now();
    return create(RegHeartbeatResponseSchema, { reregister: false });
  },
  deregister(req) {
    services.delete(req.serviceId);
    return create(RegDeregisterResponseSchema, {});
  },
};

/** The grants of an invocation: each allow entry with the live, ready service that carries it most specifically. */
function grantsFor(fn) {
  const live = [...services.values()].filter((s) => alive(s) && s.state === ServiceState.READY);
  const out = [];
  for (const entry of fn.egressAllow) {
    const a = parseAllow(entry);
    if (!a) continue;
    let best = null;
    for (const s of live) {
      const rank = carryRank(s.parsed, a);
      if (!rank) continue;
      const score = rendezvous(entry, s.endpoint);
      if (!best || rank[0] < best.rank[0] || (rank[0] === best.rank[0] && (rank[1] < best.rank[1] || (rank[1] === best.rank[1] && score > best.score))))
        best = { rank, score, s };
    }
    if (best) out.push(create(EgressGrantSchema, { allow: entry, service: best.s.endpoint }));
  }
  return out;
}

// =====================================================================================================
// Ingress
// =====================================================================================================
const TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const hasControl = (s) => /[\u0000-\u001f\u007f]/.test(s);

function judgeRequest(r) {
  if (!r) throw invalidArgument("no request");
  if (r.path === "" || !r.path.startsWith("/") || hasControl(r.path)) throw invalidArgument("malformed path");
  if (hasControl(r.rawQuery)) throw invalidArgument("malformed query");
  for (const h of r.headers) {
    if (!TOKEN.test(h.name)) throw invalidArgument("malformed header name");
    for (const b of h.value) if ((b < 0x20 && b !== 0x09) || b === 0x7f) throw invalidArgument("malformed header value");
  }
}

function normaliseHost(h) {
  h = h.toLowerCase();
  if (!h.startsWith("[")) h = h.replace(/:\d*$/, "");
  else h = h.replace(/\]:\d*$/, "]");
  return h.replace(/\.$/, "");
}

function hostRank(pattern, host) {
  if (pattern === "*") return [2, 0];
  if (pattern.startsWith("*.")) {
    const suf = pattern.slice(2);
    return host.endsWith("." + suf) ? [1, -suf.length] : null;
  }
  return pattern === host ? [0, 0] : null;
}
const prefixMatches = (prefix, path) => prefix === "/" || path === prefix || path.startsWith(prefix + "/");

function findRoute(host, path) {
  let best = null, bestKey = null;
  for (const r of control.state.routes.values()) {
    const hr = hostRank(r.hostPattern, host);
    if (!hr || !prefixMatches(r.pathPrefix, path)) continue;
    const key = [hr[0], hr[1], -r.pathPrefix.length];
    if (!bestKey || key[0] < bestKey[0] || (key[0] === bestKey[0] && (key[1] < bestKey[1] || (key[1] === bestKey[1] && key[2] < bestKey[2])))) {
      best = r;
      bestKey = key;
    }
  }
  return best;
}

const clients = new Map();
function workerClient(endpoint) {
  let c = clients.get(endpoint);
  if (!c) {
    const t = createConnectTransport({ baseUrl: endpoint, httpVersion: "1.1", interceptors: [bearer(flags["execution-token"])] });
    c = createClient(ExecutionService, t);
    clients.set(endpoint, c);
  }
  return c;
}

const NEVER_CONNECTED = new Set(["ECONNREFUSED", "ENOTFOUND", "EHOSTUNREACH", "ENETUNREACH", "EAI_AGAIN", "ECONNABORTED"]);
function causeCode(e) {
  for (let c = e; c; c = c.cause) if (typeof c.code === "string" && /^[A-Z_]+$/.test(c.code)) return c.code;
  return undefined;
}

function candidates(version, digest) {
  const live = [...workers.values()].filter((w) => alive(w) && w.state === WorkerState.READY);
  const able = live.filter((w) => w.info.abis.includes(version.abi));
  return { live, able };
}

const ingressImpl = {
  describe() {
    return create(IngressDescribeResponseSchema, { implementation: impl, maxRequestBodyBytes: BigInt(MAX_BODY) });
  },

  async *invoke(req, ctx) {
    const r = req.request;
    judgeRequest(r);
    if (r.body.length > MAX_BODY) throw platformError(ErrorReason.REQUEST_TOO_LARGE, "request body exceeds the limit");

    const route = findRoute(normaliseHost(r.host), r.path);
    const fn = route && control.state.functions.get(route.functionId);
    const version = fn && fn.versions.find((v) => v.id === fn.activeVersionId);
    if (!version) throw platformError(ErrorReason.FUNCTION_NOT_FOUND, "no function serves this request");

    // From here on a record is written, and every failure names it.
    const rec = {
      id: randomUUID(), requestId: req.requestId, functionId: fn.id, versionId: version.id, workerId: "",
      method: r.method, path: r.path, status: 0, errorReason: ErrorReason.UNSPECIFIED, durationMicros: 0n,
      peakMemoryBytes: 0n, stderr: new Uint8Array(), startTime: new Date(), traceId: "",
    };
    const tp = /^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$/.exec(ctx.requestHeader.get("traceparent") ?? "");
    if (tp && !/^0+$/.test(tp[1])) rec.traceId = tp[1];
    const traceHeaders = new Headers();
    if (rec.traceId) {
      traceHeaders.set("traceparent", ctx.requestHeader.get("traceparent"));
      if (ctx.requestHeader.get("tracestate")) traceHeaders.set("tracestate", ctx.requestHeader.get("tracestate"));
    }
    const started = process.hrtime.bigint();
    const fail = (reason, message) => {
      rec.errorReason = reason;
      rec.stderr = new TextEncoder().encode(message ?? "");
      return platformError(reason, message, rec.id);
    };
    const deadlineAt = ctx.timeoutMs() === undefined ? Infinity : Date.now() + ctx.timeoutMs();
    const expired = () => Date.now() >= deadlineAt - 5;
    let gotSummary = false;
    try {
      const { live, able } = candidates(version);
      if (live.length === 0) throw fail(ErrorReason.NO_CAPACITY, "no worker is ready");
      if (able.length === 0) throw fail(ErrorReason.ABI_MISMATCH, `no worker supports ${version.abi}`);
      const now = Date.now();
      const order = able
        .filter((w) => !((unreachable.get(w.endpoint) ?? 0) > now))
        .sort((a, b) => (rendezvous(version.digest, a.endpoint) < rendezvous(version.digest, b.endpoint) ? 1 : -1));

      const exec = create(ExecuteRequestSchema, {
        invocationId: rec.id, artifactDigest: version.digest, abi: version.abi,
        limits: version.limits ? { memoryLimitBytes: version.limits.memoryLimitBytes, maxResponseBytes: version.limits.maxResponseBytes } : undefined,
        env: [...fn.env.values()].sort((a, b) => (a.name < b.name ? -1 : 1)).map((e) => create(EnvVarSchema, { name: e.name, value: e.value })),
        request: r, egress: grantsFor(fn),
      });

      let iter, head, sawCapacity = false;
      for (let attempt = 0; attempt < Math.min(3, order.length) && !iter; attempt++) {
        const w = order[attempt];
        const call = workerClient(w.endpoint).execute(exec, { timeoutMs: ctx.timeoutMs(), signal: ctx.signal, headers: traceHeaders });
        const it = call[Symbol.asyncIterator]();
        try {
          const first = await it.next();
          if (first.done || first.value.part.case !== "head") throw fail(ErrorReason.INTERNAL, "the worker's stream did not begin with a head");
          head = first.value.part.value;
          iter = it;
          rec.workerId = w.id;
        } catch (e) {
          if (e instanceof ConnectError && errorInfo(e)?.invocationId === rec.id) throw e; // our own `fail`
          if (ctx.signal.aborted && !expired()) throw e; // the client went away
          if (expired()) throw fail(ErrorReason.DEADLINE_EXCEEDED, "the deadline expired");
          const info = errorInfo(e);
          rec.workerId = w.id;
          if (info?.reason === ErrorReason.NO_CAPACITY) {
            sawCapacity = true;
            continue;
          }
          if (info && info.reason !== ErrorReason.UNSPECIFIED) throw fail(info.reason, info.message || ConnectError.from(e).rawMessage);
          const cc = causeCode(e);
          if (cc && NEVER_CONNECTED.has(cc)) {
            unreachable.set(w.endpoint, Date.now() + 5000);
            sawCapacity = true;
            continue;
          }
          const ce = ConnectError.from(e);
          if (ce.code === Code.DeadlineExceeded) throw fail(ErrorReason.DEADLINE_EXCEEDED, "the deadline expired");
          console.error(`worker ${w.endpoint} failed: ${ce.rawMessage}`); // for the operator's log, not for the caller
          throw fail(ErrorReason.INTERNAL, "the worker failed");
        }
      }
      if (!iter) throw fail(ErrorReason.NO_CAPACITY, "no worker could take the invocation");

      rec.status = head.status;
      yield create(InvokeResponseSchema, { part: { case: "head", value: create(InvokeHeadSchema, { http: head, invocationId: rec.id }) } });
      try {
        for (;;) {
          const n = await iter.next();
          if (n.done) break;
          const p = n.value.part;
          if (p.case === "chunk") yield create(InvokeResponseSchema, { part: { case: "chunk", value: p.value } });
          else if (p.case === "summary") {
            gotSummary = true;
            rec.durationMicros = p.value.durationMicros;
            rec.peakMemoryBytes = p.value.peakMemoryBytes;
            rec.stderr = p.value.stderr;
          } else throw new Error("unexpected message in the worker's stream");
        }
      } catch (e) {
        if (e instanceof ConnectError && errorInfo(e)?.invocationId === rec.id) throw e;
        if (ctx.signal.aborted && !expired()) throw e;
        const info = errorInfo(e);
        if (expired()) throw fail(ErrorReason.DEADLINE_EXCEEDED, "the deadline expired");
        throw fail(info && info.reason !== ErrorReason.UNSPECIFIED ? info.reason : ErrorReason.INTERNAL, info?.message || "the worker's stream failed");
      }
      if (!gotSummary) throw fail(ErrorReason.INTERNAL, "the worker's stream ended without a summary");
    } finally {
      if (rec.durationMicros === 0n) rec.durationMicros = (process.hrtime.bigint() - started) / 1000n;
      invocations.push({ ...rec, seq: ++seq });
    }
  },
};

// =====================================================================================================
// Control
// =====================================================================================================
const mutating = (name) => (req) => control.mutate(name, req);
const functionImpl = {
  createFunction: mutating("createFunction"), deleteFunction: mutating("deleteFunction"), createVersion: mutating("createVersion"),
  activateVersion: mutating("activateVersion"), setEnv: mutating("setEnv"), deleteEnv: mutating("deleteEnv"),
  setEgress: mutating("setEgress"), setLabels: mutating("setLabels"), patchLabels: mutating("patchLabels"),
  getFunction: (r) => control.getFunction(r), listFunctions: (r) => control.listFunctions(r),
  listVersions: (r) => control.listVersions(r), listEnv: (r) => control.listEnv(r),
};
const routeImpl = {
  createRoute: mutating("createRoute"), deleteRoute: mutating("deleteRoute"), setRouteLabels: mutating("setRouteLabels"),
  patchRouteLabels: mutating("patchRouteLabels"), listRoutes: (r) => control.listRoutes(r),
};
const planImpl = {
  validate: (r) => control.plan(r, false),
  apply: (r) => control.plan(r, true),
};

const invMsg = (i) => create(InvocationSchema, { ...i, startTime: timestampFromDate(i.startTime) });
const inspectionImpl = {
  describe: () => create(ControlDescribeResponseSchema, { implementation: impl, maxModuleBytes: BigInt(MAX_MODULE) }),
  listWorkers: () =>
    create(ListWorkersResponseSchema, {
      workers: [...workers.values()].map((w) =>
        create(WorkerStatusSchema, { id: w.id, endpoint: w.endpoint, info: w.info, state: w.state, inFlight: w.inFlight, lastHeartbeatTime: timestampFromDate(new Date(w.lastHeartbeat)), alive: alive(w) })),
    }),
  getInvocation(req) {
    const i = invocations.find((x) => x.id === req.invocationId);
    if (!i) throw notFound("no such invocation record");
    return create(GetInvocationResponseSchema, { invocation: invMsg(i) });
  },
  listInvocations(req) {
    let items = invocations.filter((i) => (!req.functionId || i.functionId === req.functionId) && (!req.requestId || i.requestId === req.requestId));
    items = items.slice().reverse(); // newest first
    let start = 0;
    if (req.pageToken) {
      const m = /^inv\.(\d+)$/.exec(req.pageToken);
      if (!m) throw invalidArgument("page token is not one this server issued");
      start = items.findIndex((i) => i.seq < Number(m[1]));
      if (start < 0) start = items.length;
    }
    const size = Math.min(req.pageSize || 50, 500);
    const slice = items.slice(start, start + size);
    const more = start + size < items.length;
    return create(ListInvocationsResponseSchema, { invocations: slice.map(invMsg), nextPageToken: more && slice.length ? `inv.${slice[slice.length - 1].seq}` : "" });
  },
  listNetworkServices() {
    const list = [...services.values()].sort((a, b) => Buffer.compare(Buffer.from(a.endpoint), Buffer.from(b.endpoint)));
    return create(ListNetworkServicesResponseSchema, {
      services: list.map((s) =>
        create(NetworkServiceStatusSchema, {
          id: s.id, endpoint: s.endpoint, info: create(ServiceInfoSchema, { implementation: s.implementation, covers: s.covers }),
          state: s.state, lastHeartbeatTime: timestampFromDate(new Date(s.lastHeartbeat)), alive: alive(s),
        })),
    });
  },
};

// =====================================================================================================
// Wiring
// =====================================================================================================
const tokens = new Map([
  [flags["ingress-token"], "ingress"], [flags["worker-token"], "worker"], [flags["admin-token"], "admin"], [flags["network-token"], "network"],
]);
const guard = (scope) => ({ interceptors: [authInterceptor(tokens, [scope])] });

const margin = 1 * MiB;
const handlers = {
  ingress: connectNodeAdapter({
    readMaxBytes: MAX_BODY + margin,
    routes: (r) => r.service(IngressService, ingressImpl, guard("ingress")),
  }),
  control: connectNodeAdapter({
    readMaxBytes: MAX_MODULE + margin,
    routes: (r) => {
      r.service(FunctionService, functionImpl, guard("admin"));
      r.service(RouteService, routeImpl, guard("admin"));
      r.service(PlanService, planImpl, guard("admin"));
      r.service(InspectionService, inspectionImpl, guard("admin"));
    },
  }),
  other: connectNodeAdapter({
    readMaxBytes: 1 * MiB,
    routes: (r) => {
      r.service(FleetService, fleetImpl, guard("worker"));
      r.service(ArtifactService, artifactImpl, guard("worker"));
      r.service(RegistryService, registryImpl, guard("network"));
    },
  }),
};

const server = http.createServer((req, res) => {
  const u = req.url ?? "";
  if (u.startsWith("/edgerun.ingress.v1.")) return handlers.ingress(req, res);
  if (u.startsWith("/edgerun.control.v1.")) return handlers.control(req, res);
  return handlers.other(req, res);
});
const i = flags.listen.lastIndexOf(":");
server.listen(Number(flags.listen.slice(i + 1)), flags.listen.slice(0, i) || "0.0.0.0", () => console.error(`orchestrator listening on ${flags.listen}`));
