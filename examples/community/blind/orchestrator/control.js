// control.v1 semantics (docs/protocol.md "Control semantics", labels, validate-only, plans), in memory.
import { createHash, createHmac, randomBytes, randomUUID } from "node:crypto";
import { create } from "@bufbuild/protobuf";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import {
  CreateFunctionResponseSchema, GetFunctionResponseSchema, ListFunctionsResponseSchema, DeleteFunctionResponseSchema,
  CreateVersionResponseSchema, ListVersionsResponseSchema, ActivateVersionResponseSchema, SetEnvResponseSchema,
  DeleteEnvResponseSchema, ListEnvResponseSchema, SetEgressResponseSchema, SetLabelsResponseSchema,
  PatchLabelsResponseSchema,
} from "../gen/edgerun/control/v1/function_pb.js";
import {
  CreateRouteResponseSchema, ListRoutesResponseSchema, DeleteRouteResponseSchema, SetRouteLabelsResponseSchema,
  PatchRouteLabelsResponseSchema,
} from "../gen/edgerun/control/v1/route_pb.js";
import { EnvEntrySchema, FunctionSchema, FunctionVersionSchema, RouteSchema } from "../gen/edgerun/control/v1/model_pb.js";
import { LimitsSchema } from "../gen/edgerun/common/v1/common_pb.js";
import { StepResponseSchema, StepFailureSchema, ValidateResponseSchema, ApplyResponseSchema } from "../gen/edgerun/control/v1/plan_pb.js";
import { canonicalAllow } from "../lib/egressgrammar.js";
import { invalidArgument, lowerCodeName, notFound } from "../lib/common.js";

const alreadyExists = (m) => new ConnectError(m, Code.AlreadyExists);
const KEY = randomBytes(32); // revisions and page tokens
const hmac = (s) => createHmac("sha256", KEY).update(s).digest("hex");

export const digestOf = (buf) => "sha256:" + createHash("sha256").update(buf).digest("hex");

// ---- state -------------------------------------------------------------------------------------------
export function newState() {
  return { functions: new Map(), routes: new Map(), artifacts: new Map(), created: new Set() };
}
function cloneState(s) {
  const functions = new Map();
  for (const [k, f] of s.functions)
    functions.set(k, {
      ...f,
      labels: { ...f.labels },
      egressAllow: [...f.egressAllow],
      versions: f.versions.map((v) => ({ ...v, labels: { ...v.labels } })),
      env: new Map([...f.env].map(([n, e]) => [n, { ...e }])),
    });
  const routes = new Map([...s.routes].map(([k, r]) => [k, { ...r, labels: { ...r.labels } }]));
  return { functions, routes, artifacts: new Map(s.artifacts), created: new Set() };
}

// ---- messages ----------------------------------------------------------------------------------------
const limitsMsg = (l) => (l ? create(LimitsSchema, { memoryLimitBytes: l.memoryLimitBytes, maxResponseBytes: l.maxResponseBytes }) : undefined);
export const fnMsg = (f) =>
  create(FunctionSchema, {
    id: f.id, name: f.name, activeVersionId: f.activeVersionId, createTime: timestampFromDate(f.createTime),
    egressAllow: f.egressAllow, labels: f.labels,
  });
const verMsg = (v) =>
  create(FunctionVersionSchema, {
    id: v.id, functionId: v.functionId, number: BigInt(v.number), artifactDigest: v.digest, abi: v.abi,
    sizeBytes: BigInt(v.sizeBytes), limits: limitsMsg(v.limits), createTime: timestampFromDate(v.createTime), labels: v.labels,
  });
const routeMsg = (r) =>
  create(RouteSchema, {
    id: r.id, hostPattern: r.hostPattern, pathPrefix: r.pathPrefix, functionId: r.functionId,
    createTime: timestampFromDate(r.createTime), labels: r.labels,
  });
const envMsg = (fid, e) =>
  create(EnvEntrySchema, { name: e.name, value: e.secret ? "" : e.value, secret: e.secret, revision: revision(fid, e) });
const revision = (fid, e) => hmac(`rev\0${fid}\0${e.name}\0${e.secret ? 1 : 0}\0${e.value}`).slice(0, 32);

/** Blank every string equal to an id that the operation created: nothing says an id that names nothing. */
function blank(obj, ids) {
  if (ids.size === 0 || obj === null || typeof obj !== "object" || obj instanceof Uint8Array) return;
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (typeof v === "string") {
      if (ids.has(v)) obj[k] = "";
    } else if (Array.isArray(v)) v.forEach((x) => blank(x, ids));
    else if (v && typeof v === "object" && k !== "labels") blank(v, ids);
  }
}

// ---- validation helpers --------------------------------------------------------------------------------
const NAME = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;
const LABEL_KEY = /^[a-z0-9]([a-z0-9._/-]{0,61}[a-z0-9])?$/;
const LABEL_VALUE = /^[\x20-\x7e]{0,253}$/;
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const RESERVED_ENV = new Set([
  "GATEWAY_INTERFACE", "SERVER_PROTOCOL", "REQUEST_METHOD", "SERVER_NAME", "PATH_INFO", "QUERY_STRING",
  "REMOTE_ADDR", "CONTENT_LENGTH", "CONTENT_TYPE",
]);
const ABI = /^[a-z][a-z0-9-]{0,31}$/;
const BIND = /^[a-z][a-z0-9_]{0,31}$/;

function checkLabelKey(k) {
  if (!LABEL_KEY.test(k)) throw invalidArgument(`label key ${JSON.stringify(k)} is malformed`);
}
function checkLabels(labels) {
  const keys = Object.keys(labels ?? {});
  if (keys.length > 32) throw invalidArgument("at most 32 labels");
  for (const k of keys) {
    checkLabelKey(k);
    if (!LABEL_VALUE.test(labels[k])) throw invalidArgument(`value of label ${k} is malformed`);
  }
}

function normaliseHost(h) {
  if (h === "*") return h;
  const wild = h.startsWith("*.");
  const rest = wild ? h.slice(2) : h;
  const ok = /^(\[[0-9a-f:.]+\]|[a-z0-9_]([a-z0-9_.-]*[a-z0-9_])?)$/.test(rest) && !rest.includes("..");
  if (!ok) throw invalidArgument(`host pattern ${JSON.stringify(h)} is malformed`);
  return h;
}
function normalisePrefix(p) {
  if (!p.startsWith("/")) throw invalidArgument("path prefix must start with /");
  if (/\/\/|[?#\s\u0000-\u001f\u007f]/.test(p)) throw invalidArgument("path prefix is malformed");
  return p.length > 1 && p.endsWith("/") ? p.slice(0, -1) : p;
}

const getFn = (ws, id) => {
  const f = ws.functions.get(id);
  if (!f) throw notFound(`function ${JSON.stringify(id)} does not exist`);
  return f;
};
const getRoute = (ws, id) => {
  const r = ws.routes.get(id);
  if (!r) throw notFound(`route ${JSON.stringify(id)} does not exist`);
  return r;
};

// ---- paging --------------------------------------------------------------------------------------------
const DEFAULT_PAGE = 50, MAX_PAGE = 500;
function token(kind, after) {
  const body = Buffer.from(JSON.stringify(after)).toString("base64url");
  return `${kind}.${body}.${hmac(kind + "." + body)}`;
}
function untoken(kind, t) {
  const parts = t.split(".");
  if (parts.length !== 3 || parts[0] !== kind || hmac(kind + "." + parts[1]) !== parts[2])
    throw invalidArgument("page token is not one this server issued");
  return JSON.parse(Buffer.from(parts[1], "base64url").toString());
}
/** items are already in list order. isAfter(item, after) says whether the item comes after the token's position. */
function paged(items, req, kind, keyOf, isAfter, toMsg) {
  let start = 0;
  if (req.pageToken) {
    const after = untoken(kind, req.pageToken);
    start = items.findIndex((it) => isAfter(it, after));
    if (start < 0) start = items.length;
  }
  const size = Math.min(req.pageSize || DEFAULT_PAGE, MAX_PAGE);
  const slice = items.slice(start, start + size);
  const more = start + size < items.length;
  return { items: slice.map(toMsg), next: more && slice.length ? token(kind, keyOf(slice[slice.length - 1])) : "" };
}

function labelFilter(req) {
  const match = Object.entries(req.matchLabels ?? {});
  const has = req.hasLabelKeys ?? [];
  const lacks = req.lacksLabelKeys ?? [];
  for (const k of [...has, ...lacks]) checkLabelKey(k);
  for (const k of has) if (lacks.includes(k)) throw invalidArgument(`label key ${k} is both required and forbidden`);
  for (const k of lacks) if ((req.matchLabels ?? {})[k] !== undefined) throw invalidArgument(`label key ${k} is both matched and forbidden`);
  return (labels) =>
    match.every(([k, v]) => Object.hasOwn(labels, k) && labels[k] === v) &&
    has.every((k) => Object.hasOwn(labels, k)) &&
    lacks.every((k) => !Object.hasOwn(labels, k));
}

// ---- operations: (state, request, {maxModuleBytes}) -> response -----------------------------------------------
export const ops = {
  createFunction(ws, req) {
    if (!NAME.test(req.name)) throw invalidArgument("function name must be 1-63 lower-case letters, digits or '-', not starting or ending with '-'");
    checkLabels(req.labels);
    for (const f of ws.functions.values()) if (f.name === req.name) throw alreadyExists(`function ${req.name} already exists`);
    const f = { id: randomUUID(), name: req.name, activeVersionId: "", createTime: new Date(), egressAllow: [], labels: { ...req.labels }, versions: [], nextNumber: 1, env: new Map() };
    ws.functions.set(f.id, f);
    ws.created.add(f.id);
    return create(CreateFunctionResponseSchema, { function: fnMsg(f) });
  },

  deleteFunction(ws, req) {
    const f = getFn(ws, req.functionId);
    ws.functions.delete(f.id);
    for (const [id, r] of ws.routes) if (r.functionId === f.id) ws.routes.delete(id);
    return create(DeleteFunctionResponseSchema, {});
  },

  createVersion(ws, req, ctx) {
    if (req.module.length === 0) throw invalidArgument("module must not be empty");
    if (req.module.length > ctx.maxModuleBytes) throw invalidArgument(`module exceeds ${ctx.maxModuleBytes} bytes`);
    if (!ABI.test(req.abi)) throw invalidArgument("abi must be a short lower-case name");
    checkLabels(req.labels);
    const f = getFn(ws, req.functionId);
    const digest = digestOf(req.module);
    if (!ws.artifacts.has(digest)) ws.artifacts.set(digest, Buffer.from(req.module));
    const v = {
      id: randomUUID(), functionId: f.id, number: f.nextNumber++, digest, abi: req.abi, sizeBytes: req.module.length,
      limits: req.limits ? { memoryLimitBytes: req.limits.memoryLimitBytes, maxResponseBytes: req.limits.maxResponseBytes } : undefined,
      createTime: new Date(), labels: { ...req.labels },
    };
    f.versions.push(v);
    ws.created.add(v.id);
    if (req.activate) f.activeVersionId = v.id;
    return create(CreateVersionResponseSchema, { version: verMsg(v) });
  },

  activateVersion(ws, req) {
    const f = getFn(ws, req.functionId);
    const v = f.versions.find((x) => x.id === req.versionId);
    if (!v) throw notFound(`version ${JSON.stringify(req.versionId)} does not belong to the function`);
    f.activeVersionId = v.id;
    return create(ActivateVersionResponseSchema, { function: fnMsg(f) });
  },

  setEnv(ws, req) {
    const e = req.entry;
    if (!e) throw invalidArgument("entry is required");
    if (!ENV_NAME.test(e.name)) throw invalidArgument("environment variable name is malformed");
    if (RESERVED_ENV.has(e.name) || e.name.startsWith("HTTP_")) throw invalidArgument(`environment variable ${e.name} is reserved`);
    if (Buffer.byteLength(e.value) > 32 * 1024) throw invalidArgument("value exceeds 32 KiB");
    if (e.value.includes("\u0000")) throw invalidArgument("value contains NUL");
    const f = getFn(ws, req.functionId);
    if (!f.env.has(e.name) && f.env.size >= 64) throw invalidArgument("a function holds at most 64 variables");
    const stored = { name: e.name, value: e.value, secret: e.secret };
    f.env.set(e.name, stored);
    return create(SetEnvResponseSchema, { entry: envMsg(f.id, stored) });
  },

  deleteEnv(ws, req) {
    if (!ENV_NAME.test(req.name)) throw invalidArgument("environment variable name is malformed");
    const f = getFn(ws, req.functionId);
    if (!f.env.has(req.name)) throw notFound(`environment variable ${req.name} is not set`);
    f.env.delete(req.name);
    return create(DeleteEnvResponseSchema, {});
  },

  setEgress(ws, req) {
    let list;
    try {
      list = canonicalAllow(req.allow);
    } catch (e) {
      throw invalidArgument(e.message);
    }
    const f = getFn(ws, req.functionId);
    f.egressAllow = list;
    return create(SetEgressResponseSchema, { function: fnMsg(f) });
  },

  setLabels(ws, req) {
    checkLabels(req.labels);
    const f = getFn(ws, req.functionId);
    f.labels = { ...req.labels };
    return create(SetLabelsResponseSchema, { function: fnMsg(f) });
  },

  patchLabels(ws, req) {
    const labels = patched(req);
    const f = getFn(ws, req.functionId);
    f.labels = labels(f.labels);
    return create(PatchLabelsResponseSchema, { function: fnMsg(f) });
  },

  createRoute(ws, req) {
    const host = normaliseHost(req.hostPattern.toLowerCase());
    const prefix = normalisePrefix(req.pathPrefix);
    checkLabels(req.labels);
    getFn(ws, req.functionId);
    for (const r of ws.routes.values())
      if (r.hostPattern === host && r.pathPrefix === prefix) throw alreadyExists(`route ${host}${prefix} already exists`);
    const r = { id: randomUUID(), hostPattern: host, pathPrefix: prefix, functionId: req.functionId, createTime: new Date(), labels: { ...req.labels } };
    ws.routes.set(r.id, r);
    ws.created.add(r.id);
    return create(CreateRouteResponseSchema, { route: routeMsg(r) });
  },

  deleteRoute(ws, req) {
    const r = getRoute(ws, req.routeId);
    ws.routes.delete(r.id);
    return create(DeleteRouteResponseSchema, {});
  },

  setRouteLabels(ws, req) {
    checkLabels(req.labels);
    const r = getRoute(ws, req.routeId);
    r.labels = { ...req.labels };
    return create(SetRouteLabelsResponseSchema, { route: routeMsg(r) });
  },

  patchRouteLabels(ws, req) {
    const labels = patched(req);
    const r = getRoute(ws, req.routeId);
    r.labels = labels(r.labels);
    return create(PatchRouteLabelsResponseSchema, { route: routeMsg(r) });
  },
};

/** Form checks of a patch, then a function that applies it to the labels as they are (checking the limit). */
function patched(req) {
  const set = req.set ?? {}, remove = req.remove ?? [];
  checkLabels(set);
  for (const k of remove) checkLabelKey(k); // (assumed: a malformed key can never be there)
  for (const k of remove) if (Object.hasOwn(set, k)) throw invalidArgument(`label ${k} is both set and removed`);
  return (cur) => {
    const next = { ...cur };
    for (const k of remove) delete next[k];
    Object.assign(next, set);
    if (Object.keys(next).length > 32) throw invalidArgument("at most 32 labels");
    return next;
  };
}

// ---- the services ---------------------------------------------------------------------------------------------
export class Control {
  constructor({ maxModuleBytes }) {
    this.state = newState();
    this.maxModuleBytes = maxModuleBytes;
  }

  get ctx() {
    return { maxModuleBytes: this.maxModuleBytes };
  }

  /** One mutating call; validate_only runs the same code against a copy and blanks the created ids. */
  mutate(name, req) {
    if (req.validateOnly) {
      const ws = cloneState(this.state);
      const resp = ops[name](ws, req, this.ctx);
      blank(resp, ws.created);
      return resp;
    }
    return ops[name](this.state, req, this.ctx);
  }

  // ---- reads
  getFunction(req) {
    return create(GetFunctionResponseSchema, { function: fnMsg(getFn(this.state, req.functionId)) });
  }

  listFunctions(req) {
    const keep = labelFilter(req);
    let items = [...this.state.functions.values()].filter((f) => keep(f.labels) && (!req.name || f.name === req.name));
    items.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    const p = paged(items, req, "fn", (f) => f.name, (f, after) => f.name > after, fnMsg);
    return create(ListFunctionsResponseSchema, { functions: p.items, nextPageToken: p.next });
  }

  listVersions(req) {
    const keep = labelFilter(req);
    const f = getFn(this.state, req.functionId);
    const items = f.versions.filter((v) => keep(v.labels)).sort((a, b) => b.number - a.number);
    const p = paged(items, req, "ver", (v) => v.number, (v, after) => v.number < after, verMsg);
    return create(ListVersionsResponseSchema, { versions: p.items, nextPageToken: p.next });
  }

  listEnv(req) {
    const f = getFn(this.state, req.functionId);
    const names = [...f.env.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    return create(ListEnvResponseSchema, { entries: names.map((n) => envMsg(f.id, f.env.get(n))) });
  }

  listRoutes(req) {
    const keep = labelFilter(req);
    const items = [...this.state.routes.values()].filter((r) => keep(r.labels) && (!req.functionId || r.functionId === req.functionId));
    const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
    items.sort((a, b) => cmp(a.hostPattern, b.hostPattern) || cmp(a.pathPrefix, b.pathPrefix));
    const k = (r) => [r.hostPattern, r.pathPrefix];
    const p = paged(items, req, "route", k, (r, after) => cmp(k(r).join("\u0000"), after.join("\u0000")) > 0, routeMsg);
    return create(ListRoutesResponseSchema, { routes: p.items, nextPageToken: p.next });
  }

  // ---- plans
  plan(req, commit) {
    if (req.steps.length > 128) throw invalidArgument("a plan has at most 128 steps");
    const ws = cloneState(this.state);
    const bound = new Map();
    const results = [];
    let failure;
    for (let i = 0; i < req.steps.length; i++) {
      const step = req.steps[i];
      try {
        const kind = step.request.case;
        if (!kind) throw invalidArgument("step has no request");
        if (step.bind !== "") {
          if (!BIND.test(step.bind)) throw invalidArgument(`bind ${JSON.stringify(step.bind)} is malformed`);
          if (!["createFunction", "createVersion", "createRoute"].includes(kind)) throw invalidArgument("only a create step may bind");
          if (bound.has(step.bind)) throw invalidArgument(`bind ${step.bind} is already used`);
        }
        const r = step.request.value;
        for (const f of ["functionId", "versionId", "routeId"]) {
          if (typeof r[f] === "string" && r[f].startsWith("$")) {
            const id = bound.get(r[f].slice(1));
            if (id === undefined) throw invalidArgument(`${r[f]} was not bound by an earlier step`);
            r[f] = id;
          }
        }
        const before = new Set(ws.created);
        const resp = ops[kind](ws, r, this.ctx);
        if (step.bind !== "") {
          const made = [...ws.created].filter((x) => !before.has(x))[0];
          bound.set(step.bind, made);
        }
        if (!commit) blank(resp, ws.created);
        results.push(create(StepResponseSchema, { response: { case: kind, value: resp } }));
      } catch (e) {
        const ce = ConnectError.from(e);
        failure = create(StepFailureSchema, { step: i, code: lowerCodeName(ce.code), message: ce.rawMessage });
        break;
      }
    }
    if (commit) {
      if (failure) return create(ApplyResponseSchema, { failure });
      ws.created = new Set();
      this.state = ws;
      return create(ApplyResponseSchema, { results });
    }
    return create(ValidateResponseSchema, { results, failure });
  }
}
