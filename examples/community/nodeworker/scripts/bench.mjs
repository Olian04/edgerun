// Rough latency numbers: node scripts/bench.mjs module.wasm   (the guest is invoked with PATH_INFO=/echo; use the conformance test guest or any wagi-1 module)
import { readFileSync } from "node:fs";
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { ExecutionService } from "../gen/edgerun/worker/v1/execution_pb.js";
import { startWorker } from "../src/worker.js";
import { fakeOrchestrator } from "../test/fake-orchestrator.js";
const orch = await fakeOrchestrator();
const w = await startWorker({ orchestratorUrl: orch.url, workerToken: "w-token", executionToken: "x", listen: "127.0.0.1:22050", advertiseUrl: "http://127.0.0.1:22050", maxConcurrency: 4 });
const c = createClient(ExecutionService, createConnectTransport({ baseUrl: "http://127.0.0.1:22050", httpVersion: "1.1", interceptors: [(n) => (r) => { r.header.set("authorization", "Bearer x"); return n(r); }] }));
const digest = orch.add(readFileSync(process.argv[2]));
const call = async () => { for await (const _ of c.execute({ artifactDigest: digest, abi: "wagi-1", request: { method: "GET", host: "h", path: "/echo", headers: [], body: new Uint8Array(0) } })) ; };
let t = Date.now(); await call(); console.log("first (fetch+compile+run)", Date.now() - t, "ms");
t = Date.now(); await call(); console.log("second", Date.now() - t, "ms");
t = Date.now(); for (let i = 0; i < 30; i++) await call(); console.log("sequential avg", (Date.now() - t) / 30, "ms");
t = Date.now(); for (let k = 0; k < 10; k++) await Promise.all([1,2,3,4].map(call)); console.log("4-wide avg per call", (Date.now() - t) / 40, "ms");
await w.stop(); await orch.close();
