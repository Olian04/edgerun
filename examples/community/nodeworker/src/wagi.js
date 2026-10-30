// The wagi-1 ABI, host side: building the guest's environment from a request
// and parsing the CGI-style response the guest writes to stdout.
// Pure functions, no I/O.
//
// Header values are BYTES here, in both directions (docs/protocol.md, "HTTP
// messages"): they are never decoded to a JS string on the way through.

import { randomBytes } from "node:crypto";

export class AbiViolation extends Error {}

const RESERVED = new Set([
  "GATEWAY_INTERFACE", "SERVER_PROTOCOL", "REQUEST_METHOD", "SERVER_NAME", "PATH_INFO",
  "QUERY_STRING", "REMOTE_ADDR", "CONTENT_LENGTH", "CONTENT_TYPE",
]);
export const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
export const DIGEST = /^sha256:[0-9a-f]{64}$/;

/** Names a user-defined variable may not take: the ABI's own, and the HTTP_* namespace. */
export const reservedEnvName = (n) => RESERVED.has(n) || n.startsWith("HTTP_");

const HTAB = 0x09, SP = 0x20, CR = 0x0d, LF = 0x0a, DEL = 0x7f;

/** A control character as "HTTP messages" means it: C0 and DEL (a header value may still hold a tab). */
const isControl = (c) => c < 0x20 || c === DEL;

/**
 * Text (a path, a query): no C0 control character and no DEL. That it is valid
 * UTF-8 is the decoder's business: protobuf-es refuses a proto3 string that is not.
 */
export const validText = (s) => {
  for (let i = 0; i < s.length; i++) if (isControl(s.charCodeAt(i))) return false;
  return true;
};

/** A header value is opaque bytes with no control character other than horizontal tab. */
export const validHeaderValue = (b) => {
  for (let i = 0; i < b.length; i++) if (b[i] !== HTAB && isControl(b[i])) return false;
  return true;
};

/** An RFC 9110 token. */
export const TOKEN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;

/**
 * Why a request is not well formed ("HTTP messages", "Who judges what"), or
 * null. A worker answers invalid_argument, with no reason, to one that is not.
 * Beyond the spec's list: the method, host and peer address become environment
 * variables, which cannot hold a NUL, so a NUL there is refused too.
 * @param {{method:string,host:string,path:string,rawQuery:string,headers:{name:string,value:Uint8Array}[],remoteAddr:string}} req
 * @returns {string|null}
 */
export function malformedRequest(req) {
  if (req.path === "" || req.path[0] !== "/") return `path ${JSON.stringify(req.path)} must start with '/'`;
  if (!validText(req.path)) return "the path is not text: it has a control character";
  if (!validText(req.rawQuery)) return "the query is not text: it has a control character";
  for (const h of req.headers) {
    if (!TOKEN.test(h.name)) return `header name ${JSON.stringify(h.name)} is not a token`;
    if (!validHeaderValue(h.value)) return `the value of header ${JSON.stringify(h.name)} has a control character`;
  }
  for (const [what, v] of [["method", req.method], ["host", req.host], ["remote_addr", req.remoteAddr]]) {
    if (v.includes("\0")) return `${what} contains NUL`;
  }
  return null;
}

/** Trace context the CLIENT sent: never handed to the guest ("Observability"); the platform's own replaces it. */
const CLIENT_TRACE_HEADERS = new Set(["traceparent", "tracestate", "baggage"]);
const TRACEPARENT = /^00-([0-9a-f]{32})-[0-9a-f]{16}-([0-9a-f]{2})$/;

/**
 * The guest's traceparent for an Execute call that carried one (W3C Trace
 * Context): the call's trace id and flags, with a span id of the worker's own.
 * Null when the call carried no valid trace.
 * @param {string|null|undefined} callTraceparent
 * @param {() => string} [newSpanId] 16 lower-case hex digits
 */
export function guestTraceparent(callTraceparent, newSpanId = () => randomBytes(8).toString("hex")) {
  const m = TRACEPARENT.exec((callTraceparent ?? "").trim().toLowerCase());
  if (!m || /^0+$/.test(m[1])) return null;
  return `00-${m[1]}-${newSpanId()}-${m[2]}`;
}

const COMMA = Buffer.from(", ");
const join = (a, b) => Buffer.concat([a, COMMA, b]);

/**
 * The guest's environment: name -> BYTES. Strings are encoded as UTF-8; header
 * values are carried untouched. It cannot be a map of JS strings (node:wasi
 * encodes those as UTF-8, and 0xE9 0xFF is not UTF-8), which is why
 * guest-thread.js serves environ_sizes_get and environ_get itself.
 * @param {{method:string,host:string,path:string,rawQuery:string,headers:{name:string,value:Uint8Array}[],remoteAddr:string}} req
 * @param {number} bodyLength
 * @param {{name:string,value:string}[]} userEnv
 * @param {string|null} [traceparent] the guest's own (see guestTraceparent); any the client sent is dropped
 * @returns {Map<string,Buffer>}
 */
export function buildEnv(req, bodyLength, userEnv, traceparent = null) {
  const utf8 = (s) => Buffer.from(s, "utf8");
  const env = new Map([
    ["GATEWAY_INTERFACE", utf8("CGI/1.1")],
    ["SERVER_PROTOCOL", utf8("HTTP/1.1")],
    ["REQUEST_METHOD", utf8(req.method)],
    ["SERVER_NAME", utf8(req.host)],
    ["PATH_INFO", utf8(req.path)],
    ["QUERY_STRING", utf8(req.rawQuery)],
    ["REMOTE_ADDR", utf8(req.remoteAddr)],
    ["CONTENT_LENGTH", utf8(String(bodyLength))],
  ]);
  let contentType;
  const repeated = new Map();
  for (const h of req.headers) {
    const name = h.name.toLowerCase();
    const value = Buffer.from(h.value.buffer, h.value.byteOffset, h.value.byteLength);
    if (name === "content-type") {
      // "request content-type, if any"; a repeated one is joined like any other.
      contentType = contentType === undefined ? value : join(contentType, value);
      continue;
    }
    // CONTENT_LENGTH above is the measured length: the client's own header is not repeated.
    if (name === "content-length") continue;
    if (CLIENT_TRACE_HEADERS.has(name)) continue;
    const key = "HTTP_" + name.toUpperCase().replaceAll("-", "_");
    repeated.set(key, repeated.has(key) ? join(repeated.get(key), value) : value);
  }
  if (contentType !== undefined) env.set("CONTENT_TYPE", contentType);
  for (const [k, v] of repeated) env.set(k, v);
  if (traceparent) env.set("HTTP_TRACEPARENT", utf8(traceparent));
  for (const e of userEnv) env.set(e.name, utf8(e.value)); // user-defined variables come after the ABI's own
  return env;
}

/** The `NAME=value` entries environ_get hands out, as bytes (without the terminating NUL). */
export const environEntries = (env) => [...env].map(([k, v]) => Buffer.concat([Buffer.from(k + "="), v]));

/** Space and horizontal tab only: not what String.prototype.trim does (it also takes U+00A0 and the rest). */
const trimmed = (b) => {
  let from = 0, to = b.length;
  while (from < to && (b[from] === SP || b[from] === HTAB)) from++;
  while (to > from && (b[to - 1] === SP || b[to - 1] === HTAB)) to--;
  return b.subarray(from, to);
};

/**
 * Parse what the guest wrote to stdout. Throws AbiViolation when it is not a
 * valid response (nothing written, headers never ended, bad Status line, a
 * control character in a header). A header's value is every byte after the
 * colon with surrounding spaces and tabs trimmed, and nothing else: no
 * decoding, no Unicode whitespace.
 * @param {Buffer} out
 */
export function parseResponse(out) {
  if (out.length === 0) throw new AbiViolation("the guest wrote no output");
  let status = 200;
  const headers = [];
  let pos = 0;
  for (;;) {
    const nl = out.indexOf(LF, pos);
    if (nl < 0) throw new AbiViolation("the guest's headers were not ended by a blank line");
    let end = nl;
    if (end > pos && out[end - 1] === CR) end--;
    const line = out.subarray(pos, end);
    pos = nl + 1;
    if (line.length === 0) break;
    const colon = line.indexOf(0x3a);
    if (colon <= 0) throw new AbiViolation(`malformed header line ${JSON.stringify(line.toString("latin1", 0, 80))}`);
    const name = line.toString("latin1", 0, colon);
    if (!TOKEN.test(name)) throw new AbiViolation(`malformed header name ${JSON.stringify(name.slice(0, 80))}`);
    const value = trimmed(line.subarray(colon + 1));
    if (!validHeaderValue(value)) throw new AbiViolation(`header ${name} has a control character in its value`);
    if (name.toLowerCase() === "status") {
      const text = value.toString("latin1");
      const m = /^(\d{3})(?:[ \t].*)?$/.exec(text);
      if (!m) throw new AbiViolation(`bad Status ${JSON.stringify(text.slice(0, 40))}`);
      status = Number(m[1]);
      if (status < 200 || status > 599) throw new AbiViolation(`Status ${status} is outside 200-599`);
      continue;
    }
    headers.push({ name: name.toLowerCase(), value: Uint8Array.from(value) });
  }
  return { status, headers, body: out.subarray(pos) };
}
