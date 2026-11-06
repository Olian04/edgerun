// A JS stand-in for the orchestrator's worker-facing half (FleetService and
// ArtifactService), so the worker can be tested without Go or PostgreSQL.
import { createHash } from "node:crypto";
import http from "node:http";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { ArtifactService } from "../gen/edgerun/worker/v1/artifact_pb.js";
import { FleetService } from "../gen/edgerun/worker/v1/fleet_pb.js";
import { platformError, ErrorReason } from "../src/errors.js";
import { freePort } from "./stack.js";

export const digestOf = (bytes) => "sha256:" + createHash("sha256").update(bytes).digest("hex");

export async function fakeOrchestrator({ workerToken = "w-token", heartbeatMs = 50 } = {}) {
  const blobs = new Map();
  const events = []; // {kind: "register"|"heartbeat"|"deregister", ...} in arrival order
  let nextId = 1;
  const state = { forget: null, fetches: 0 };
  const known = new Set();
  const auth = (next) => async (req) => {
    if (req.header.get("authorization") !== `Bearer ${workerToken}`) throw new ConnectError("bad token", Code.Unauthenticated);
    return next(req);
  };
  const handler = connectNodeAdapter({
    interceptors: [auth],
    routes(router) {
      router.service(FleetService, {
        register(req) {
          const id = `w${nextId++}`;
          known.add(id);
          events.push({ kind: "register", id, endpoint: req.endpoint, info: req.info });
          return { workerId: id, heartbeatInterval: { seconds: 0n, nanos: heartbeatMs * 1e6 } };
        },
        heartbeat(req) {
          events.push({ kind: "heartbeat", id: req.workerId, state: req.state, inFlight: req.inFlight });
          if (state.forget === req.workerId || !known.has(req.workerId)) { known.delete(req.workerId); state.forget = null; return { reregister: true }; }
          return { reregister: false };
        },
        deregister(req) { known.delete(req.workerId); events.push({ kind: "deregister", id: req.workerId }); return {}; },
      });
      router.service(ArtifactService, {
        async *getArtifact(req) {
          state.fetches++;
          const b = blobs.get(req.digest);
          if (!b) throw platformError(ErrorReason.ARTIFACT_NOT_FOUND, "no such artifact");
          for (let o = 0; o < b.length; o += 1000) yield { chunk: b.subarray(o, o + 1000) }; // small chunks: reassembly is exercised
        },
      });
    },
  });
  const server = http.createServer(handler);
  const port = await freePort();
  await new Promise((r) => server.listen(port, "127.0.0.1", r));
  return {
    url: `http://127.0.0.1:${port}`, workerToken, events, state,
    add(bytes) { const d = digestOf(bytes); blobs.set(d, bytes); return d; },
    /** serve `bytes` under `digest` (to model a tampered artifact) */
    serveAs(digest, bytes) { blobs.set(digest, bytes); },
    close() { server.closeAllConnections(); return new Promise((r) => server.close(r)); },
  };
}
