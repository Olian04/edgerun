// End to end: blind frontend -> blind orchestrator -> a real worker. Run ./e2e.sh.
import { readFileSync } from "node:fs";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { FunctionService } from "./gen/edgerun/control/v1/function_pb.js";
import { RouteService } from "./gen/edgerun/control/v1/route_pb.js";
import { RecordsService } from "./gen/edgerun/control/v1/records_pb.js";
import { Code, ConnectError } from "@connectrpc/connect";
import { MembershipService, MemberKind, MemberState } from "./gen/edgerun/membership/v1/membership_pb.js";
import { bearer } from "./lib/common.js";

const t = createConnectTransport({ baseUrl: "http://127.0.0.1:18081", httpVersion: "1.1", interceptors: [bearer("a")] });
const fns = createClient(FunctionService, t);
const routes = createClient(RouteService, t);
const records = createClient(RecordsService, t);
const membership = createClient(MembershipService, t);
const listWorkers = async () => (await membership.listMembers({ kind: MemberKind.WORKER })).members;

const wasm = readFileSync(process.argv[2]);
const f = (await fns.createFunction({ name: "hello" })).function;
await fns.createVersion({ functionId: f.id, module: wasm, abi: "wagi-1", activate: true });
await routes.createRoute({ hostPattern: "*", pathPrefix: "/", functionId: f.id });
await fns.setEnv({ functionId: f.id, entry: { name: "GREETING", value: "hi", secret: false } });
for (let i = 0; i < 30 && (await listWorkers()).length === 0; i++) await new Promise((r) => setTimeout(r, 200));
console.log("workers:", (await listWorkers()).map((w) => [w.endpoint, w.alive]));

for (const [m, p, body] of [["GET", "/hello?x=1", undefined], ["POST", "/echo%20x", "payload"]]) {
  const r = await fetch("http://127.0.0.1:18080" + p, { method: m, body, headers: { "x-request-id": "rid-1", traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" } });
  console.log(m, p, r.status, [...r.headers].map(([k, v]) => `${k}=${v}`).join(" "));
  console.log((await r.text()).slice(0, 600));
}
const invs = (await records.listInvocations({})).invocations;
console.log("records:", invs.map((i) => [i.requestId, i.status, i.errorReason, i.traceId, i.durationMicros]));

// membership.v1: a token manages only its own kind of member; only admin lists.
const asScope = (tok) => createClient(MembershipService, createConnectTransport({ baseUrl: "http://127.0.0.1:18081", httpVersion: "1.1", interceptors: [bearer(tok)] }));
const expectCode = async (what, code, p) => {
  try { await p; } catch (e) { if (ConnectError.from(e).code === code) return console.log("ok:", what); throw e; }
  throw new Error(`${what}: expected ${Code[code]}`);
};
const svc = { endpoint: "http://127.0.0.1:19999", info: { case: "networkService", value: { covers: ["example.com"] } } };
await expectCode("worker token cannot register a network service", Code.PermissionDenied, asScope("w").register(svc));
await expectCode("network token cannot list", Code.PermissionDenied, asScope("n").listMembers({}));
await expectCode("unknown token", Code.Unauthenticated, asScope("nope").listMembers({}));
const reg = await asScope("n").register(svc);
await expectCode("worker token cannot heartbeat a network service", Code.PermissionDenied, asScope("w").heartbeat({ memberId: reg.memberId, state: MemberState.READY }));
console.log("unknown member reregisters:", (await asScope("n").heartbeat({ memberId: "nope", state: MemberState.READY })).reregister);
console.log("members:", (await membership.listMembers({})).members.map((m) => [MemberKind[m.kind], m.endpoint, MemberState[m.state], m.info.case]));
await asScope("n").deregister({ memberId: reg.memberId });
console.log("after deregister:", (await membership.listMembers({ kind: MemberKind.NETWORK_SERVICE })).members.length);
