// Echo a ~6 MiB body through the worker with a guest that has /echo (the conformance test guest).
import { readFileSync } from "node:fs";
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { ExecutionService } from "../gen/edgerun/worker/v1/execution_pb.js";
import { startWorker } from "../src/worker.js";
import { fakeOrchestrator } from "../test/fake-orchestrator.js";
const orch = await fakeOrchestrator();
const w = await startWorker({ orchestratorUrl: orch.url, workerToken: "w-token", executionToken: "x", listen: "127.0.0.1:22051", advertiseUrl: "http://127.0.0.1:22051", maxConcurrency: 2 });
const c = createClient(ExecutionService, createConnectTransport({ baseUrl: "http://127.0.0.1:22051", httpVersion: "1.1", writeMaxBytes: 64 << 20, readMaxBytes: 64 << 20, interceptors: [(n) => (r) => { r.header.set("authorization", "Bearer x"); return n(r); }] }));
const digest = orch.add(readFileSync(process.argv[2]));
const body = Buffer.alloc(5.9 * 1024 * 1024, "ab");
let got = 0, t = Date.now();
for await (const m of c.execute({ artifactDigest: digest, abi: "wagi-1", request: { method: "POST", host: "h", path: "/echo", headers: [], body } })) if (m.part.case === "chunk") got += m.part.value.length;
console.log("echoed", got, "of", body.length, "in", Date.now() - t, "ms");
await w.stop(); await orch.close();
