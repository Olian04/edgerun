// A frontend for edgerun, written from docs/protocol.md ("Frontend behaviour") alone.
//
//   node frontend/main.js --orchestrator-url http://127.0.0.1:7000 --ingress-token T \
//        --request-timeout 3s --listen 127.0.0.1:8080 [--trust-forwarded]
import http from "node:http";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { IngressService, InvokeRequestSchema } from "../gen/edgerun/ingress/v1/ingress_pb.js";
import { ErrorReason, HeaderSchema, HttpRequestSchema } from "../gen/edgerun/common/v1/common_pb.js";
import { bearer, errorInfo, parseDuration, parseFlags } from "../lib/common.js";

const flags = parseFlags(process.argv.slice(2), {
  "orchestrator-url": process.env.EDGERUN_ORCHESTRATOR_URL,
  "ingress-token": process.env.EDGERUN_INGRESS_TOKEN,
  "request-timeout": process.env.EDGERUN_REQUEST_TIMEOUT ?? "30s",
  listen: process.env.EDGERUN_LISTEN ?? "127.0.0.1:8080",
});
const TRUST_FORWARDED = flags["trust-forwarded"] === "true";
const BUDGET_MS = parseDuration(flags["request-timeout"]);

const transport = createConnectTransport({
  baseUrl: flags["orchestrator-url"],
  httpVersion: "1.1",
  interceptors: [bearer(flags["ingress-token"])],
});
const ingress = createClient(IngressService, transport);

// ---- limits from Describe ------------------------------------------------------------------------
let maxBody = 6 * 1024 * 1024; // until the first answer
async function refreshLimits() {
  try {
    const d = await ingress.describe({}, { timeoutMs: 5000 });
    if (d.maxRequestBodyBytes > 0n) maxBody = Number(d.maxRequestBodyBytes);
    return true;
  } catch (e) {
    console.error("describe failed:", e.message);
    return false;
  }
}
(async () => {
  while (!(await refreshLimits())) await new Promise((r) => setTimeout(r, 500));
  setInterval(refreshLimits, 30_000).unref();
})();

// ---- request translation -------------------------------------------------------------------------
const HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection",
  "te", "trailer", "transfer-encoding", "upgrade",
]);
const FORWARD_CLAIMS = new Set(["x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "forwarded", "x-real-ip"]);
const TRACE = new Set(["traceparent", "tracestate", "baggage"]);
const TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const TRACEPARENT = /^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/;

/** Percent-decode once to bytes, then UTF-8. Returns null when it is not valid text. */
function decodePath(raw) {
  const bytes = [];
  for (let i = 0; i < raw.length; i++) {
    const c = raw.charCodeAt(i);
    if (c === 0x25) {
      const h = raw.slice(i + 1, i + 3);
      if (!/^[0-9A-Fa-f]{2}$/.test(h)) return null; // malformed escape: not decodable (assumption)
      bytes.push(parseInt(h, 16));
      i += 2;
    } else bytes.push(c);
  }
  let s;
  try {
    s = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(Uint8Array.from(bytes));
  } catch {
    return null;
  }
  if (/[\u0000-\u001f\u007f]/.test(s)) return null;
  return s;
}

function peerAddr(sock) {
  let ip = sock.remoteAddress ?? "";
  if (ip.startsWith("::ffff:") && ip.includes(".")) ip = ip.slice(7);
  const port = sock.remotePort ?? 0;
  return { ip, addr: ip.includes(":") ? `[${ip}]:${port}` : `${ip}:${port}` };
}

function freshId() {
  return randomUUID();
}

function pickRequestId(rawHeaders) {
  for (let i = 0; i < rawHeaders.length; i += 2) {
    if (rawHeaders[i].toLowerCase() === "x-request-id") {
      const v = rawHeaders[i + 1];
      // printable ASCII, at most 128 characters (and not empty)
      if (v.length > 0 && v.length <= 128 && /^[\x21-\x7e]+$/.test(v)) return v;
      return freshId();
    }
  }
  return freshId();
}

// ---- response helpers ------------------------------------------------------------------------------
function plain(res, status, text, extra = {}) {
  if (res.headersSent) return res.destroy();
  const body = Buffer.from(text + "\n");
  res.writeHead(status, {
    "content-type": "text/plain; charset=utf-8",
    "content-length": body.length,
    ...extra,
  });
  res.end(req_isHead(res) ? undefined : body);
}
const req_isHead = (res) => res.req?.method === "HEAD";

const GENERIC = {
  400: "bad request", 404: "not found", 413: "request entity too large", 500: "internal error",
  502: "bad gateway", 503: "service unavailable", 504: "gateway timeout",
};

// Map a failure before the head to [status, retryAfter, invocationId] or null for "client went away".
function classify(err, clientGone, timedOut) {
  const e = ConnectError.from(err);
  const info = errorInfo(e);
  const invId = info?.invocationId || "";
  if (info && info.reason !== ErrorReason.UNSPECIFIED) {
    switch (info.reason) {
      case ErrorReason.FUNCTION_NOT_FOUND: return [404, false, invId];
      case ErrorReason.NO_CAPACITY: return [503, true, invId];
      case ErrorReason.DEADLINE_EXCEEDED: return [504, false, invId];
      case ErrorReason.FUNCTION_TRAPPED: return [502, false, invId];
      case ErrorReason.ABI_MISMATCH: return [502, false, invId];
      case ErrorReason.REQUEST_TOO_LARGE: return [413, false, invId];
      case ErrorReason.RESPONSE_TOO_LARGE: return [502, false, invId];
      case ErrorReason.ARTIFACT_NOT_FOUND: return [502, false, invId];
      default: return [500, false, invId]; // INTERNAL, EGRESS_*, unknown
    }
  }
  if (timedOut || e.code === Code.DeadlineExceeded) return [504, false, invId];
  if (e.code === Code.Canceled) return clientGone ? null : [500, false, invId];
  if (e.code === Code.ResourceExhausted) return [413, false, invId];
  if (e.code === Code.Unavailable) return [503, true, invId];
  if (e.code === Code.Unauthenticated || e.code === Code.PermissionDenied) return [502, false, invId];
  return [500, false, invId];
}

// ---- the server ------------------------------------------------------------------------------------
const server = http.createServer({ keepAlive: true, requireHostHeader: false }, handle);
// Expect: 100-continue: do not invite a body we are going to refuse.
server.on("checkContinue", (req, res) => {
  if (!(Number(req.headers["content-length"]) > maxBody)) res.writeContinue();
  handle(req, res);
});
server.requestTimeout = 120_000;

async function handle(req, res) {
  const requestId = pickRequestId(req.rawHeaders);
  res.setHeader("x-request-id", requestId);
  const reply = (status, text, extra) => plain(res, status, GENERIC[status] ?? text, extra);

  try {
    await serve(req, res, requestId, reply);
  } catch (e) {
    console.error("handler error:", e);
    if (!res.headersSent) reply(500);
    else res.destroy();
  }
}

async function serve(req, res, requestId, reply) {
  const target = req.url ?? "";
  if (req.httpVersion === "1.1" && req.headers.host === undefined) return reply(400); // RFC 9112; the spec is silent

  // OPTIONS * is answered here and never forwarded.
  if (target === "*") {
    if (req.method === "OPTIONS") {
      res.writeHead(204, { allow: "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS", "x-request-id": requestId });
      return res.end();
    }
    return reply(400);
  }
  if (!target.startsWith("/")) return reply(400); // absolute-form, authority-form...
  // Request target must be printable ASCII.
  if (!/^[\x21-\x7e]*$/.test(target)) return reply(400);
  const q = target.indexOf("?");
  const rawPath = q < 0 ? target : target.slice(0, q);
  const rawQuery = q < 0 ? "" : target.slice(q + 1);
  const path = decodePath(rawPath);
  if (path === null || !path.startsWith("/")) return reply(400);

  // Headers: names lower-cased, values as bytes, in order.
  const headers = [];
  let traceparent = "";
  let tracestate = "";
  const rh = req.rawHeaders;
  for (let i = 0; i < rh.length; i += 2) {
    const name = rh[i].toLowerCase();
    const value = rh[i + 1];
    if (!TOKEN.test(name)) return reply(400);
    if (/[\u0000-\u0008\u000a-\u001f\u007f]/.test(value)) return reply(400);
    if (name === "traceparent") {
      if (!traceparent) traceparent = value.trim();
      continue;
    }
    if (name === "tracestate") {
      tracestate = tracestate ? tracestate + "," + value : value;
      continue;
    }
    if (TRACE.has(name)) continue;
    if (HOP.has(name)) continue;
    if (name === "host") continue; // carried in HttpRequest.host, not repeated (docs/protocol.md)
    if (FORWARD_CLAIMS.has(name) && !TRUST_FORWARDED) continue;
    headers.push({ name, value: Buffer.from(value, "latin1") });
  }
  const { ip, addr } = peerAddr(req.socket);
  const hostHeader = req.headers["host"] ?? "";
  const proto = req.socket.encrypted ? "https" : "http";
  if (TRUST_FORWARDED) {
    // append the peer to a client-supplied chain; keep proto/host when present
    const i = headers.findIndex((h) => h.name === "x-forwarded-for");
    if (i >= 0) headers[i] = { name: "x-forwarded-for", value: Buffer.concat([headers[i].value, Buffer.from(", " + ip)]) };
    else headers.push({ name: "x-forwarded-for", value: Buffer.from(ip) });
    if (!headers.some((h) => h.name === "x-forwarded-proto")) headers.push({ name: "x-forwarded-proto", value: Buffer.from(proto) });
    if (!headers.some((h) => h.name === "x-forwarded-host")) headers.push({ name: "x-forwarded-host", value: Buffer.from(hostHeader, "latin1") });
  } else {
    headers.push({ name: "x-forwarded-for", value: Buffer.from(ip) });
    headers.push({ name: "x-forwarded-proto", value: Buffer.from(proto) });
    headers.push({ name: "x-forwarded-host", value: Buffer.from(hostHeader, "latin1") });
  }

  // Body, bounded.
  const limit = maxBody;
  const declared = req.headers["content-length"];
  if (declared !== undefined && Number(declared) > limit) {
    res.setHeader("connection", "close");
    reply(413);
    req.destroy();
    return;
  }
  const chunks = [];
  let total = 0;
  try {
    for await (const c of req) {
      total += c.length;
      if (total > limit) {
        res.setHeader("connection", "close");
        reply(413);
        req.destroy();
        return;
      }
      chunks.push(c);
    }
  } catch {
    return; // client went away mid-body
  }
  const body = Buffer.concat(chunks);

  // Cancel the call if the client goes away.
  const ac = new AbortController();
  let clientGone = false;
  res.on("close", () => {
    if (!res.writableFinished) {
      clientGone = true;
      ac.abort();
    }
  });

  const callHeaders = new Headers();
  const tp = TRACEPARENT.exec(traceparent);
  if (tp && tp[1] !== "0".repeat(32) && tp[2] !== "0".repeat(16) && !traceparent.startsWith("ff-")) {
    callHeaders.set("traceparent", traceparent);
    if (tracestate) callHeaders.set("tracestate", tracestate);
  }

  const request = create(HttpRequestSchema, {
    method: req.method,
    host: hostHeader,
    path,
    rawQuery,
    headers: headers.map((h) => create(HeaderSchema, h)),
    body,
    remoteAddr: addr,
  });

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    ac.abort();
  }, BUDGET_MS + 250); // our own timer fires just after the wire deadline
  let stream;
  let headSent = false;
  try {
    stream = ingress.invoke(create(InvokeRequestSchema, { request, requestId }), {
      timeoutMs: BUDGET_MS,
      signal: ac.signal,
      headers: callHeaders,
    });
    const iter = stream[Symbol.asyncIterator]();
    let first = await iter.next();
    if (first.done) return plainFail(res, reply, 502); // clean end without a head
    const part = first.value.part;
    if (part.case !== "head") return plainFail(res, reply, 502);
    const head = part.value;
    const status = head.http?.status ?? 0;
    const outHeaders = [];
    let bad = status < 200 || status > 599;
    for (const h of head.http?.headers ?? []) {
      const name = h.name.toLowerCase();
      if (!TOKEN.test(name)) { bad = true; break; }
      const v = Buffer.from(h.value);
      if (v.some((b) => (b < 0x20 && b !== 0x09) || b === 0x7f)) { bad = true; break; }
      if (HOP.has(name) || name === "content-length" || name === "x-request-id" || name === "x-edgerun-invocation-id") continue;
      outHeaders.push(name, v.toString("latin1"));
    }
    if (bad) {
      ac.abort();
      return plainFail(res, reply, 502, head.invocationId);
    }
    outHeaders.push("x-request-id", requestId);
    if (head.invocationId) outHeaders.push("x-edgerun-invocation-id", head.invocationId);
    res.writeHead(status, outHeaders);
    headSent = true;
    res.write(Buffer.alloc(0)); // NOT flushHeaders(): Node then encodes header strings as UTF-8, not latin1
    for (;;) {
      const n = await iter.next();
      if (n.done) break;
      const p = n.value.part;
      if (p.case !== "chunk") throw new Error("unexpected message after head");
      if (p.value.length === 0) continue;
      if (!res.write(p.value)) {
        await Promise.race([once(res, "drain"), once(res, "close")]);
        if (res.destroyed) return;
      }
    }
    res.end();
  } catch (err) {
    if (headSent) {
      // Cannot change the status: abort the connection so the transfer is seen to fail.
      res.destroy();
      return;
    }
    const c = classify(err, clientGone, timedOut);
    if (c === null) return res.destroy();
    const [status, retry, invId] = c;
    const extra = {};
    if (retry) extra["retry-after"] = "1";
    if (invId) extra["x-edgerun-invocation-id"] = invId;
    reply(status, "", extra);
  } finally {
    clearTimeout(timer);
  }
}

function plainFail(res, reply, status, invId) {
  reply(status, "", invId ? { "x-edgerun-invocation-id": invId } : {});
}

// CONNECT and Upgrade requests are not forwarded.
server.on("connect", (req, sock) => {
  sock.end("HTTP/1.1 400 Bad Request\r\nconnection: close\r\ncontent-length: 0\r\n\r\n");
});
server.on("clientError", (err, sock) => {
  // The parser refused the request before we saw it (bad target, bad header, ...): no client id to echo, so a fresh one.
  if (sock.writable) sock.end(`HTTP/1.1 400 Bad Request\r\nx-request-id: ${freshId()}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`);
  else sock.destroy();
});

const [lh, lp] = (() => {
  const i = flags.listen.lastIndexOf(":");
  return [flags.listen.slice(0, i) || "0.0.0.0", Number(flags.listen.slice(i + 1))];
})();
server.listen(lp, lh, () => console.error(`frontend listening on ${lh}:${lp}`));
