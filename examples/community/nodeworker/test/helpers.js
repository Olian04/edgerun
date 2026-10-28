import { spawn } from "node:child_process";
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { FunctionService } from "../gen/edgerun/control/v1/function_pb.js";
import { InspectionService } from "../gen/edgerun/control/v1/inspection_pb.js";
import { RouteService } from "../gen/edgerun/control/v1/route_pb.js";
import { IngressService } from "../gen/edgerun/ingress/v1/ingress_pb.js";
import { projectRoot } from "./stack.js";

export function transport(baseUrl, token) {
  return createConnectTransport({
    baseUrl, httpVersion: "1.1",
    interceptors: [(next) => (req) => { req.header.set("authorization", `Bearer ${token}`); return next(req); }],
  });
}

export function controlClients(url, adminToken) {
  const t = transport(url, adminToken);
  return { functions: createClient(FunctionService, t), routes: createClient(RouteService, t), inspect: createClient(InspectionService, t) };
}
export const ingressClient = (url, token) => createClient(IngressService, transport(url, token));

/** Run src/main.js as a real process. */
export function spawnWorker({ orchestratorUrl, workerToken, executionToken, port, maxConcurrency = 2, extraEnv = {} }) {
  const proc = spawn(process.execPath, ["src/main.js",
    "--orchestrator-url", orchestratorUrl, "--worker-token", workerToken, "--execution-token", executionToken,
    "--listen", `127.0.0.1:${port}`, "--advertise-url", `http://127.0.0.1:${port}`, "--max-concurrency", String(maxConcurrency),
  ], { cwd: projectRoot, env: { ...process.env, ...extraEnv }, stdio: ["ignore", "pipe", "pipe"] });
  let log = "";
  proc.stdout.on("data", (d) => (log += d));
  proc.stderr.on("data", (d) => (log += d));
  const exited = new Promise((r) => proc.once("exit", (code, sig) => r({ code, sig })));
  return { proc, logs: () => log, exited, term: () => proc.kill("SIGTERM") };
}

export async function until(fn, ms = 15000, what = "condition") {
  const end = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

/** Collect an Invoke stream. */
export async function invoke(client, request, opts) {
  let head; const chunks = [];
  for await (const m of client.invoke({ request, requestId: "t-" + Math.random().toString(36).slice(2) }, opts)) {
    if (m.part.case === "head") head = m.part.value; else if (m.part.case === "chunk") chunks.push(m.part.value);
  }
  return { head, body: Buffer.concat(chunks) };
}
