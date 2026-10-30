// Assembles the worker: HTTP server (ExecutionService), module store, fleet client.
import http from "node:http";
import { createClient } from "@connectrpc/connect";
import { connectNodeAdapter, createConnectTransport } from "@connectrpc/connect-node";
import { ArtifactService } from "../gen/edgerun/worker/v1/artifact_pb.js";
import { ExecutionService } from "../gen/edgerun/worker/v1/execution_pb.js";
import { FleetService } from "../gen/edgerun/worker/v1/fleet_pb.js";
import { bearerAuth } from "./auth.js";
import { ModuleStore } from "./artifacts.js";
import { ExecutionImpl, SUPPORTED_ABIS } from "./execution.js";
import { FleetClient } from "./fleet.js";
import { invalidArgumentOnParseError } from "./parse-errors.js";

export const VERSION = "0.1.0";

/**
 * @param {{orchestratorUrl:string, workerToken:string, executionToken:string,
 *   listen:string, advertiseUrl:string, maxConcurrency:number, limits?:object,
 *   log?:(level:string,msg:string,fields?:object)=>void}} cfg
 */
export async function startWorker(cfg) {
  const log = cfg.log ?? (() => {});
  const info = {
    implementation: { name: "edgerun-worker-node", version: VERSION, protocols: ["edgerun.worker.v1"] },
    abis: SUPPORTED_ABIS,
    maxConcurrency: cfg.maxConcurrency,
    labels: { runtime: `node-${process.versions.node}` },
  };
  const orchestrator = createConnectTransport({
    baseUrl: cfg.orchestratorUrl,
    httpVersion: "1.1",
    interceptors: [(next) => (req) => { req.header.set("authorization", `Bearer ${cfg.workerToken}`); return next(req); }],
  });
  const store = new ModuleStore({ client: createClient(ArtifactService, orchestrator), log });
  const impl = new ExecutionImpl({ store, maxConcurrency: cfg.maxConcurrency, info, limits: cfg.limits, log });
  const fleet = new FleetClient({ client: createClient(FleetService, orchestrator), endpoint: cfg.advertiseUrl, info, impl, log });

  const rpc = connectNodeAdapter({
    readMaxBytes: 64 * 1024 * 1024,
    interceptors: [bearerAuth(cfg.executionToken)],
    routes(router) {
      router.service(ExecutionService, {
        describe: () => impl.describe(),
        execute: (req, ctx) => impl.execute(req, ctx),
      });
    },
  });
  const server = http.createServer((req, res) => {
    if (req.url === "/healthz") {
      res.statusCode = impl.state === "READY" ? 200 : 503;
      return void res.end(impl.state === "READY" ? "ok\n" : "draining\n");
    }
    invalidArgumentOnParseError(res);
    rpc(req, res);
  });
  const [host, port] = splitListen(cfg.listen);
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(port, host, resolve); });
  log("info", "worker listening", { addr: cfg.listen, advertise: cfg.advertiseUrl, maxConcurrency: cfg.maxConcurrency });
  fleet.start();

  let stopping = null;
  /** Graceful shutdown as SIGTERM does: report DRAINING, finish in-flight work, deregister, stop serving. */
  const stop = ({ drainTimeoutMs = 30_000 } = {}) => (stopping ??= (async () => {
    impl.state = "DRAINING";
    // Nothing to announce if we never registered (the orchestrator was never reachable).
    const registered = await Promise.race([fleet.registered.then(() => true), new Promise((r) => setTimeout(r, 1000, false).unref())]);
    if (registered) await fleet.drain();
    await Promise.race([impl.whenIdle(), new Promise((r) => setTimeout(r, drainTimeoutMs).unref())]);
    await fleet.deregister();
    server.close();
    server.closeIdleConnections();
    await new Promise((r) => setTimeout(r, 50));
    server.closeAllConnections();
  })());

  return { server, impl, store, fleet, stop, url: cfg.advertiseUrl };
}

function splitListen(addr) {
  const i = addr.lastIndexOf(":");
  return [addr.slice(0, i) || undefined, Number(addr.slice(i + 1))];
}
