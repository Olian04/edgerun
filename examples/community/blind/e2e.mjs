// End to end: blind frontend -> blind orchestrator -> a real worker. Run ./e2e.sh.
import { readFileSync } from "node:fs";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { FunctionService } from "./gen/edgerun/control/v1/function_pb.js";
import { RouteService } from "./gen/edgerun/control/v1/route_pb.js";
import { InspectionService } from "./gen/edgerun/control/v1/inspection_pb.js";
import { bearer } from "./lib/common.js";

const t = createConnectTransport({ baseUrl: "http://127.0.0.1:18081", httpVersion: "1.1", interceptors: [bearer("a")] });
const fns = createClient(FunctionService, t);
const routes = createClient(RouteService, t);
const insp = createClient(InspectionService, t);

const wasm = readFileSync(process.argv[2]);
const f = (await fns.createFunction({ name: "hello" })).function;
await fns.createVersion({ functionId: f.id, module: wasm, abi: "wagi-1", activate: true });
await routes.createRoute({ hostPattern: "*", pathPrefix: "/", functionId: f.id });
await fns.setEnv({ functionId: f.id, entry: { name: "GREETING", value: "hi", secret: false } });
for (let i = 0; i < 30 && (await insp.listWorkers({})).workers.length === 0; i++) await new Promise((r) => setTimeout(r, 200));
console.log("workers:", (await insp.listWorkers({})).workers.map((w) => [w.endpoint, w.alive]));

for (const [m, p, body] of [["GET", "/hello?x=1", undefined], ["POST", "/echo%20x", "payload"]]) {
  const r = await fetch("http://127.0.0.1:18080" + p, { method: m, body, headers: { "x-request-id": "rid-1", traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" } });
  console.log(m, p, r.status, [...r.headers].map(([k, v]) => `${k}=${v}`).join(" "));
  console.log((await r.text()).slice(0, 600));
}
const invs = (await insp.listInvocations({})).invocations;
console.log("records:", invs.map((i) => [i.requestId, i.status, i.errorReason, i.traceId, i.durationMicros]));
