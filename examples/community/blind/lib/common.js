// Helpers shared by the blind frontend and orchestrator. Written from docs/protocol.md alone.
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { ErrorInfoSchema, ErrorReason } from "../gen/edgerun/common/v1/common_pb.js";

export { ErrorReason };

// ---- flags: --name value, or EDGERUN_NAME env -------------------------------------------------
export function parseFlags(argv, defaults = {}) {
  const out = { ...defaults };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    let [k, v] = a.slice(2).split("=", 2);
    if (v === undefined) {
      if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) v = argv[++i];
      else v = "true";
    }
    out[k] = v;
  }
  return out;
}

export function parseDuration(s) {
  const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(String(s));
  if (!m) throw new Error(`bad duration ${s}`);
  const n = parseFloat(m[1]);
  return n * { ms: 1, s: 1000, m: 60000, h: 3600000 }[m[2]];
}

// ---- errors ------------------------------------------------------------------------------------
// The canonical Connect code of each reason (docs/protocol.md, "Every reason has one canonical Connect code").
const REASON_CODE = {
  [ErrorReason.FUNCTION_NOT_FOUND]: Code.NotFound,
  [ErrorReason.ARTIFACT_NOT_FOUND]: Code.NotFound,
  [ErrorReason.NO_CAPACITY]: Code.ResourceExhausted,
  [ErrorReason.RESPONSE_TOO_LARGE]: Code.ResourceExhausted,
  [ErrorReason.DEADLINE_EXCEEDED]: Code.DeadlineExceeded,
  [ErrorReason.FUNCTION_TRAPPED]: Code.Aborted,
  [ErrorReason.ABI_MISMATCH]: Code.FailedPrecondition,
  [ErrorReason.REQUEST_TOO_LARGE]: Code.InvalidArgument,
  [ErrorReason.INTERNAL]: Code.Internal,
  [ErrorReason.EGRESS_DENIED]: Code.PermissionDenied,
  [ErrorReason.EGRESS_FAILED]: Code.Unavailable,
};

export function platformError(reason, message = "", invocationId = "") {
  const info = create(ErrorInfoSchema, { reason, message, invocationId });
  return new ConnectError(message || ErrorReason[reason] || "error", REASON_CODE[reason] ?? Code.Internal, undefined, [
    { desc: ErrorInfoSchema, value: info },
  ]);
}

export function invalidArgument(msg) {
  return new ConnectError(msg, Code.InvalidArgument);
}
export function notFound(msg) {
  return new ConnectError(msg, Code.NotFound);
}

/** The ErrorInfo of a ConnectError, or undefined. */
export function errorInfo(err) {
  const e = ConnectError.from(err);
  const d = e.findDetails(ErrorInfoSchema);
  return d.length ? d[0] : undefined;
}

// ---- auth ---------------------------------------------------------------------------------------
/**
 * Server interceptor. tokens: Map token -> scope. allowed: scopes that may call.
 * Missing/unknown token -> UNAUTHENTICATED; known token of another scope -> PERMISSION_DENIED.
 */
export function authInterceptor(tokens, allowed) {
  return (next) => async (req) => {
    const h = req.header.get("authorization") ?? "";
    const m = /^Bearer (.+)$/i.exec(h.trim());
    if (!m || !tokens.has(m[1])) throw new ConnectError("unauthenticated", Code.Unauthenticated);
    if (!allowed.includes(tokens.get(m[1]))) throw new ConnectError("permission denied", Code.PermissionDenied);
    return next(req);
  };
}

export function bearer(token) {
  return (next) => async (req) => {
    req.header.set("authorization", `Bearer ${token}`);
    return next(req);
  };
}

export function lowerCodeName(code) {
  return Code[code]
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .toLowerCase();
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
