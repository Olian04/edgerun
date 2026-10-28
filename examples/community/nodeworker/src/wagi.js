// The wagi-1 ABI, host side: building the guest's environment from a request
// and parsing the CGI-style response the guest writes to stdout.
// Pure functions, no I/O.

export class AbiViolation extends Error {}

const RESERVED = new Set([
  "GATEWAY_INTERFACE", "SERVER_PROTOCOL", "REQUEST_METHOD", "SERVER_NAME", "PATH_INFO",
  "QUERY_STRING", "REMOTE_ADDR", "CONTENT_LENGTH", "CONTENT_TYPE",
]);
export const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
export const DIGEST = /^sha256:[0-9a-f]{64}$/;

/** Names a user-defined variable may not take: the ABI's own, and the HTTP_* namespace. */
export const reservedEnvName = (n) => RESERVED.has(n) || n.startsWith("HTTP_");

/**
 * @param {{method:string,host:string,path:string,rawQuery:string,headers:{name:string,value:string}[],remoteAddr:string}} req
 * @param {number} bodyLength
 * @param {{name:string,value:string}[]} userEnv
 * @returns {Record<string,string>}
 */
export function buildEnv(req, bodyLength, userEnv) {
  const env = {
    GATEWAY_INTERFACE: "CGI/1.1",
    SERVER_PROTOCOL: "HTTP/1.1",
    REQUEST_METHOD: req.method,
    SERVER_NAME: req.host,
    PATH_INFO: req.path,
    QUERY_STRING: req.rawQuery,
    REMOTE_ADDR: req.remoteAddr,
    CONTENT_LENGTH: String(bodyLength),
  };
  const repeated = new Map();
  for (const h of req.headers) {
    const name = h.name.toLowerCase();
    if (name === "content-type") {
      // "request content-type, if any"; a repeated one is joined like any other.
      env.CONTENT_TYPE = env.CONTENT_TYPE === undefined ? h.value : `${env.CONTENT_TYPE}, ${h.value}`;
      continue;
    }
    // CONTENT_LENGTH above is the measured length: the client's own header is not repeated.
    if (name === "content-length") continue;
    const key = "HTTP_" + name.toUpperCase().replaceAll("-", "_");
    repeated.set(key, repeated.has(key) ? `${repeated.get(key)}, ${h.value}` : h.value);
  }
  for (const [k, v] of repeated) env[k] = v;
  for (const e of userEnv) env[e.name] = e.value; // user-defined variables come after the ABI's own
  return env;
}

const TOKEN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;

/**
 * Parse what the guest wrote to stdout. Throws AbiViolation when it is not a
 * valid response (nothing written, headers never ended, bad Status line).
 * @param {Buffer} out
 */
export function parseResponse(out) {
  if (out.length === 0) throw new AbiViolation("the guest wrote no output");
  let status = 200;
  const headers = [];
  let pos = 0;
  for (;;) {
    const nl = out.indexOf(0x0a, pos);
    if (nl < 0) throw new AbiViolation("the guest's headers were not ended by a blank line");
    let end = nl;
    if (end > pos && out[end - 1] === 0x0d) end--;
    const line = out.toString("latin1", pos, end);
    pos = nl + 1;
    if (line === "") break;
    const colon = line.indexOf(":");
    if (colon <= 0) throw new AbiViolation(`malformed header line ${JSON.stringify(line.slice(0, 80))}`);
    const name = line.slice(0, colon);
    if (!TOKEN.test(name)) throw new AbiViolation(`malformed header name ${JSON.stringify(name.slice(0, 80))}`);
    const value = Buffer.from(line.slice(colon + 1).trim(), "latin1").toString("utf8");
    if (value.includes("\0")) throw new AbiViolation(`header ${name} contains NUL`);
    if (name.toLowerCase() === "status") {
      const m = /^(\d{3})(?:\s.*)?$/.exec(value);
      if (!m) throw new AbiViolation(`bad Status ${JSON.stringify(value.slice(0, 40))}`);
      status = Number(m[1]);
      if (status < 200 || status > 599) throw new AbiViolation(`Status ${status} is outside 200-599`);
      continue;
    }
    headers.push({ name: name.toLowerCase(), value });
  }
  return { status, headers, body: out.subarray(pos) };
}
