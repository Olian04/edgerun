// End to end: a real orchestrator process (private PostgreSQL) and the Node
// worker as a real process. A function is deployed through control.v1 and
// invoked through ingress.v1; the Node worker executes it.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { after, before, test } from "node:test";
import { Code, ConnectError } from "@connectrpc/connect";
import { ErrorReason, reasonOf } from "../src/errors.js";
import { guests } from "./wasm.js";
import { WorkerState } from "../gen/edgerun/worker/v1/worker_pb.js";
import { controlClients, ingressClient, invoke, spawnWorker, until } from "./helpers.js";
import { freePort, startStack, tokens } from "./stack.js";

let stack, worker, ctl, ingress;

before(async () => {
  stack = await startStack();
  const port = await freePort();
  worker = spawnWorker({ orchestratorUrl: stack.url, workerToken: tokens.worker, executionToken: tokens.execution, port });
  ctl = controlClients(stack.url, tokens.admin);
  ingress = ingressClient(stack.url, tokens.ingress);
  await until(async () => (await ctl.inspect.listWorkers({})).workers.some((w) => w.state === WorkerState.READY), 15000, "the worker to register");
});

after(async () => {
  if (worker) { worker.term(); await worker.exited; }
  if (stack) await stack.stop();
});

test("the worker registers, advertising wagi-1 only", async () => {
  const { workers } = await ctl.inspect.listWorkers({});
  assert.equal(workers.length, 1);
  const info = workers[0].info;
  assert.deepEqual(info.abis, ["wagi-1"]);
  assert.equal(info.maxConcurrency, 2);
  assert.equal(info.implementation.name, "edgerun-worker-node");
});

test("deploy hello through control.v1, invoke through ingress.v1, the Node worker runs it", async () => {
  const { function: fn } = await ctl.functions.createFunction({ name: "hello" });
  const { version } = await ctl.functions.createVersion({ functionId: fn.id, module: readFileSync(stack.hello), abi: "wagi-1", activate: true });
  assert.match(version.artifactDigest, /^sha256:[0-9a-f]{64}$/);
  await ctl.functions.setEnv({ functionId: fn.id, entry: { name: "GREETING", value: "Howdy" } });
  await ctl.routes.createRoute({ hostPattern: "hello.localhost", pathPrefix: "/", functionId: fn.id });

  const { head, body } = await invoke(ingress, {
    method: "GET", host: "hello.localhost", path: "/greet", rawQuery: "name=node",
    headers: [{ name: "x-forwarded-for", value: new TextEncoder().encode("203.0.113.9") }], remoteAddr: "203.0.113.9:4444",
  });
  assert.equal(head.http.status, 200);
  assert.ok(head.invocationId);
  assert.equal(body.toString(), "Howdy, node!\nYou asked for GET /greet from 203.0.113.9.\n");

  // The orchestrator recorded which worker ran it, with the summary's numbers.
  const rec = await until(async () => (await ctl.inspect.listInvocations({ functionId: fn.id })).invocations[0], 10000, "an invocation record");
  assert.equal(rec.status, 200);
  assert.ok(rec.durationMicros > 0n);
  assert.ok(rec.peakMemoryBytes > 0n);
  assert.ok(rec.workerId);
});

test("the module is fetched once and served from the cache afterwards", async () => {
  const { head } = await invoke(ingress, { method: "GET", host: "hello.localhost", path: "/", rawQuery: "", headers: [], remoteAddr: "1.2.3.4:5" });
  assert.equal(head.http.status, 200);
  // (the worker is a separate process: its log says when it fetched)
  const fetched = (worker.logs().match(/artifact fetched/g) ?? []).length;
  assert.ok(fetched <= 1, `fetched ${fetched} times`);
});

test("a function with no route is FUNCTION_NOT_FOUND, not a worker problem", async () => {
  await assert.rejects(
    invoke(ingress, { method: "GET", host: "nope.localhost", path: "/", rawQuery: "", headers: [], remoteAddr: "1.2.3.4:5" }),
    (e) => ConnectError.from(e).code === Code.NotFound,
  );
});

async function deploy(name, module, abi = "wagi-1") {
  const { function: fn } = await ctl.functions.createFunction({ name });
  await ctl.functions.createVersion({ functionId: fn.id, module, abi, activate: true });
  await ctl.routes.createRoute({ hostPattern: `${name}.localhost`, pathPrefix: "/", functionId: fn.id });
  return fn;
}
const get = (host, opts) => invoke(ingress, { method: "GET", host, path: "/", rawQuery: "", headers: [], remoteAddr: "1.2.3.4:5" }, opts);
const reasonOfErr = (e) => reasonOf(ConnectError.from(e));

test("the worker's typed errors are what the orchestrator hands the frontend", async () => {
  await deploy("trap", guests.unreachable());
  await assert.rejects(get("trap.localhost"), (e) => reasonOfErr(e) === ErrorReason.FUNCTION_TRAPPED);

  await deploy("spin", guests.spin());
  await assert.rejects(get("spin.localhost", { headers: { "connect-timeout-ms": "700" } }), (e) => reasonOfErr(e) === ErrorReason.DEADLINE_EXCEEDED);

  await deploy("flood", guests.flood());
  await assert.rejects(get("flood.localhost"), (e) => reasonOfErr(e) === ErrorReason.RESPONSE_TOO_LARGE);

  await deploy("fetches", guests.respond(), "wagi-2"); // no worker offers wagi-2
  await assert.rejects(get("fetches.localhost"), (e) => reasonOfErr(e) === ErrorReason.ABI_MISMATCH);

  // the worker survived all of that
  assert.equal((await get("hello.localhost")).head.http.status, 200);
});

test("SIGTERM reports DRAINING, deregisters, and exits 0", async () => {
  worker.term();
  const { code } = await worker.exited;
  assert.equal(code, 0, worker.logs());
  const { workers } = await ctl.inspect.listWorkers({});
  assert.equal(workers.length, 0);
  assert.match(worker.logs(), /deregistered/);
});
