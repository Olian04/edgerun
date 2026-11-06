import { createHash, timingSafeEqual } from "node:crypto";
import { Code, ConnectError } from "@connectrpc/connect";

const sha = (s) => createHash("sha256").update(s).digest();

/**
 * Server interceptor: every request (Describe included) must carry
 * `Authorization: Bearer <secret>` with the execution secret this worker was
 * configured with. Anything else -- no header, a malformed one, any other
 * token, including the worker's own orchestrator-facing token -- is
 * UNAUTHENTICATED. That is what the conformance suite requires; docs/protocol.md
 * would also allow PERMISSION_DENIED for "a valid token of the wrong scope"
 * (see FINDINGS.md).
 * @param {string} secret
 */
export function bearerAuth(secret) {
  const want = sha(secret);
  return (next) => async (req) => {
    const h = req.header.get("authorization");
    const m = h && /^Bearer\s+(\S+)\s*$/i.exec(h);
    if (!m) throw new ConnectError("missing or malformed bearer token", Code.Unauthenticated);
    if (!timingSafeEqual(want, sha(m[1]))) throw new ConnectError("invalid token", Code.Unauthenticated);
    return next(req);
  };
}
