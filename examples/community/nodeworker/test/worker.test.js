// The worker in this process against a fake orchestrator, with hand-assembled
// guests: error mapping, limits, capacity, auth, fleet behaviour.
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { ExecutionService } from "../gen/edgerun/worker/v1/execution_pb.js";
import { WorkerState } from "../gen/edgerun/worker/v1/worker_pb.js";
import { ErrorReason, reasonOf } from "../src/errors.js";
import { startWorker } from "../src/worker.js";
import { fakeOrchestrator } from "./fake-orchestrator.js";
import { freePort } from "./stack.js";
import { until } from "./helpers.js";
import { guest, guests, exit, write } from "./wasm.js";

const EXEC = "x-token";
const MiB = 1024 * 1024;
let orch, w, exec;

before(async () => {
  orch = await fakeOrchestrator();
  const port = await freePort();
  w = await startWorker({ orchestratorUrl: orch.url, workerToken: orch.workerToken, executionToken: EXEC, listen: `127.0.0.1:${port}`, advertiseUrl: `http://127.0.0.1:${port}`, maxConcurrency: 2 });
  exec = clientWith(`http://127.0.0.1:${port}`, EXEC);
  await w.fleet.registered;
});
after(async () => { await w.stop(); await orch.close(); });

function clientWith(baseUrl, token) {
  return createClient(ExecutionService, createConnectTransport({
    baseUrl, httpVersion: "1.1",
    interceptors: [(next) => (req) => { if (token) req.header.set("authorization", `Bearer ${token}`); return next(req); }],
  }));
}

const bytes = (s) => Buffer.from(s, "latin1");
const request = (o = {}) => ({ method: "GET", host: "h.test", path: "/", rawQuery: "", headers: [], body: new Uint8Array(0), remoteAddr: "192.0.2.1:1", ...o });

/** Execute `wasm` and collect the stream (or the error). */
async function execute(wasm, { limits, env = [], req = {}, abi = "wagi-1", timeoutMs, digest, signal, client = exec } = {}) {
  digest ??= orch.add(wasm);
  const out = { chunks: [] };
  try {
    // The budget travels as the Connect-Timeout-Ms header, not as the client's own timer:
    // a client-side deadline would make the client give up at the same instant the
    // worker's typed error is due (docs/protocol.md, Transport).
    const headers = timeoutMs === undefined ? undefined : { "connect-timeout-ms": String(timeoutMs) };
    for await (const m of client.execute({ invocationId: "t", artifactDigest: digest, abi, limits, env, request: request(req) }, { headers, signal })) {
      if (m.part.case === "head") out.head = m.part.value;
      else if (m.part.case === "chunk") out.chunks.push(m.part.value);
      else if (m.part.case === "summary") out.summary = m.part.value;
    }
  } catch (e) { out.err = ConnectError.from(e); }
  out.body = Buffer.concat(out.chunks);
  return out;
}
const reason = (o) => reasonOf(o.err);

test("a valid response: head, body, summary", async () => {
  const o = await execute(guests.respond("Status: 201 Created\r\nX-A: 1\nX-A: 2\nContent-Type: text/plain\n\nbody!"));
  assert.equal(o.err, undefined);
  assert.equal(o.head.status, 201);
  assert.deepEqual(o.head.headers.map((h) => `${h.name}=${Buffer.from(h.value).toString("latin1")}`), ["x-a=1", "x-a=2", "content-type=text/plain"]);
  assert.equal(o.body.toString(), "body!");
  assert.ok(o.summary.durationMicros > 0n);
  assert.ok(o.summary.peakMemoryBytes >= 65536n);
});

test("stderr is returned as the summary's log; over 64 KiB only the tail is kept, and flagged", async () => {
  const o = await execute(guests.logs("log line\n"));
  assert.equal(Buffer.from(o.summary.stderr).toString(), "log line\n");
  assert.equal(o.summary.stderrTruncated, false);
  const line = "0123456789".repeat(99) + "\n"; // 991 bytes
  const big = await execute(guests.logs(line, 80)); // 79 280 bytes
  assert.equal(big.summary.stderr.length, 64 * 1024);
  assert.equal(big.summary.stderrTruncated, true);
  assert.equal(Buffer.from(big.summary.stderr).toString().slice(-line.length), line); // the end survives
});

test("exit code != 0 is FUNCTION_TRAPPED, with the stderr tail in the message", async () => {
  const o = await execute(guest([...write(2), ...exit(3)], { texts: ["going down"] }));
  assert.equal(reason(o), ErrorReason.FUNCTION_TRAPPED);
  assert.equal(o.err.code, Code.Aborted);
  assert.match(o.err.rawMessage, /code 3/);
  assert.match(o.err.rawMessage, /going down$/);
});

test("a trap is FUNCTION_TRAPPED", async () => {
  assert.equal(reason(await execute(guests.unreachable())), ErrorReason.FUNCTION_TRAPPED);
});

test("ABI violations: no output, no blank line, bad Status, bad header", async () => {
  for (const text of ["", "Content-Type: text/plain", "Status: banana\n\nx", "Status: 99\n\nx", "Status: 200\nnot a header\n\nx", "Status: 600\n\n"]) {
    const o = await execute(text === "" ? guests.nothing() : guests.respond(text));
    assert.equal(reason(o), ErrorReason.ABI_MISMATCH, JSON.stringify(text));
    assert.equal(o.err.code, Code.FailedPrecondition);
  }
});

test("a module that is not a wagi-1 command is ABI_MISMATCH, not run", async () => {
  const { importsEnv } = await import("./wasm.js");
  assert.equal(reason(await execute(importsEnv())), ErrorReason.ABI_MISMATCH);
  assert.equal(reason(await execute(new TextEncoder().encode("not wasm"))), ErrorReason.ABI_MISMATCH);
});

test("an ABI the worker does not run is ABI_MISMATCH (wagi-2 is not offered)", async () => {
  for (const abi of ["wagi-2", "wagi-99", ""]) assert.equal(reason(await execute(guests.respond(), { abi })), ErrorReason.ABI_MISMATCH, abi);
});

test("malformed requests are invalid_argument with no reason", async () => {
  const wasm = guests.respond();
  const digest = orch.add(wasm);
  for (const o of [
    await execute(wasm, { digest: "md5:abc" }),
    await execute(wasm, { digest: digest.toUpperCase() }),
    await execute(wasm, { env: [{ name: "1BAD", value: "x" }] }),
    await execute(wasm, { env: [{ name: "A-B", value: "x" }] }),
    await execute(wasm, { env: [{ name: "REQUEST_METHOD", value: "x" }] }),
    await execute(wasm, { env: [{ name: "HTTP_X", value: "x" }] }),
    await execute(wasm, { env: [{ name: "OK", value: "a\0b" }] }),
    // docs/protocol.md "HTTP messages": who judges what
    await execute(wasm, { req: { path: "" } }),
    await execute(wasm, { req: { path: "echo" } }),
    await execute(wasm, { req: { path: "/a\0" } }),
    await execute(wasm, { req: { path: "/a\nb" } }),
    await execute(wasm, { req: { path: "/a\x7f" } }),
    await execute(wasm, { req: { rawQuery: "a\nb" } }),
    await execute(wasm, { req: { headers: [{ name: "bad name", value: bytes("x") }] } }),
    await execute(wasm, { req: { headers: [{ name: "", value: bytes("x") }] } }),
    await execute(wasm, { req: { headers: [{ name: "x-a", value: bytes("a\r\nb: c") }] } }),
    await execute(wasm, { req: { headers: [{ name: "x-a", value: bytes("a\0b") }] } }),
    await execute(wasm, { req: { headers: [{ name: "x-a", value: bytes("a\x7f") }] } }),
  ]) {
    assert.equal(o.err?.code, Code.InvalidArgument);
    assert.equal(reason(o), ErrorReason.UNSPECIFIED);
  }
});

test("unknown artifact is ARTIFACT_NOT_FOUND (not_found)", async () => {
  const o = await execute(guests.respond(), { digest: "sha256:" + "0".repeat(64) });
  assert.equal(reason(o), ErrorReason.ARTIFACT_NOT_FOUND);
  assert.equal(o.err.code, Code.NotFound);
});

test("tampered artifact bytes are never run, and are not remembered", async () => {
  const right = guests.respond("Content-Type: t\n\nright");
  const wrong = guests.respond("Content-Type: t\n\nWRONG");
  const digest = orch.add(right);
  orch.serveAs(digest, wrong);
  const o = await execute(null, { digest });
  assert.equal(reason(o), ErrorReason.INTERNAL);
  assert.match(o.err.rawMessage, /verification/);
  orch.serveAs(digest, right);
  const o2 = await execute(null, { digest });
  assert.equal(o2.body.toString(), "right");
});

test("modules are cached by digest: one fetch for many invocations", async () => {
  const wasm = guests.respond("Content-Type: t\n\ncached");
  const digest = orch.add(wasm);
  const before = orch.state.fetches;
  await Promise.all([1, 2].map(() => execute(null, { digest })));
  await execute(null, { digest });
  assert.equal(orch.state.fetches - before, 1);
});

test("the guest's environment: CGI variables, HTTP_ headers joined, user variables after", async () => {
  // the env is covered end to end by the conformance suite's /env guest; here, the pure function
  const { buildEnv } = await import("../src/wagi.js");
  const env = buildEnv(request({ method: "POST", path: "/p", rawQuery: "a=b", headers: [
    { name: "content-type", value: bytes("text/x") }, { name: "x-multi", value: bytes("a") }, { name: "x-multi", value: bytes("b") },
    { name: "content-length", value: bytes("99") }, { name: "x-request-id", value: bytes("7") },
  ] }), 3, [{ name: "MINE", value: "1é" }]);
  assert.deepEqual(Object.fromEntries([...env].map(([k, v]) => [k, v.toString("latin1")])), {
    GATEWAY_INTERFACE: "CGI/1.1", SERVER_PROTOCOL: "HTTP/1.1", REQUEST_METHOD: "POST", SERVER_NAME: "h.test", PATH_INFO: "/p",
    QUERY_STRING: "a=b", REMOTE_ADDR: "192.0.2.1:1", CONTENT_LENGTH: "3", CONTENT_TYPE: "text/x",
    HTTP_X_MULTI: "a, b", HTTP_X_REQUEST_ID: "7", MINE: "1\xc3\xa9", // a user variable is text, UTF-8
  });
});

test("header values reach the guest byte-exact in HTTP_* (not UTF-8 re-encoded), joined with ', '", async () => {
  const o = await execute(guests.printEnv(), { req: {
    path: "/pé日", headers: [
      { name: "x-latin", value: bytes("caf\xe9") }, { name: "x-junk", value: bytes("\xe9\xff\x80") },
      { name: "x-dup", value: bytes("one") }, { name: "x-dup", value: bytes("\xe9two") }, { name: "x-tab", value: bytes("a\tb") },
    ],
  } });
  assert.equal(o.err, undefined);
  const env = o.body.toString("latin1").split("\0").filter(Boolean);
  for (const want of ["HTTP_X_LATIN=caf\xe9", "HTTP_X_JUNK=\xe9\xff\x80", "HTTP_X_DUP=one, \xe9two", "HTTP_X_TAB=a\tb", "PATH_INFO=/p\xc3\xa9\xe6\x97\xa5"]) {
    assert.ok(env.includes(want), `${JSON.stringify(want)} in ${JSON.stringify(env)}`);
  }
});

test("a guest's response header values are bytes: only space and tab are trimmed, control characters are ABI_MISMATCH", async () => {
  const o = await execute(guests.respond(bytes("Content-Type: t\nX-Latin: caf\xe9\nX-Junk:  \xff\xfe\x80 \t\nX-Nbsp: \xc2\xa0v\xc2\xa0\nX-Empty:\nX-Tab: a\tb\n\nraw")));
  assert.equal(o.err, undefined);
  const got = Object.fromEntries(o.head.headers.map((h) => [h.name, Buffer.from(h.value).toString("latin1")]));
  assert.deepEqual(got, { "content-type": "t", "x-latin": "caf\xe9", "x-junk": "\xff\xfe\x80", "x-nbsp": "\xc2\xa0v\xc2\xa0", "x-empty": "", "x-tab": "a\tb" });
  for (const bad of ["X-Bad: a\x01b", "X-Bad: a\x7fb", "X-Bad: a\rb", "X-Bad: a\x00b"]) {
    assert.equal(reason(await execute(guests.respond(bytes(`Content-Type: t\n${bad}\n\nbody`)))), ErrorReason.ABI_MISMATCH, JSON.stringify(bad));
  }
});

test("a path that is not UTF-8 on the wire is invalid_argument (the Go client cannot send one, so by hand)", async () => {
  const { create, toBinary } = await import("@bufbuild/protobuf");
  const { ExecuteRequestSchema } = await import("../gen/edgerun/worker/v1/execution_pb.js");
  const digest = orch.add(guests.respond());
  const msg = toBinary(ExecuteRequestSchema, create(ExecuteRequestSchema, { artifactDigest: digest, abi: "wagi-1", request: request({ path: "/ab" }) }));
  const at = Buffer.from(msg).indexOf("/ab");
  assert.ok(at > 0);
  msg[at + 1] = 0xff; // "/\xffb": not UTF-8
  const frame = Buffer.concat([Buffer.from([0, 0, 0, 0, 0]), msg]);
  frame.writeUInt32BE(msg.length, 1);
  for (const accept of ["identity", "gzip"]) { // an end-of-stream message may be compressed
    const res = await fetch(`http://127.0.0.1:${new URL(w.url).port}/edgerun.worker.v1.ExecutionService/Execute`, {
      method: "POST", body: frame,
      headers: { "content-type": "application/connect+proto", "connect-protocol-version": "1", "connect-accept-encoding": accept, authorization: `Bearer ${EXEC}` },
    });
    const out = Buffer.from(await res.arrayBuffer());
    // a failed stream is 200 with one envelope, flag 0x02: the end-of-stream message, JSON, carrying the error
    assert.equal(out[0] & 2, 2);
    assert.equal(out[0] & 1, 0);
    const end = JSON.parse(out.subarray(5, 5 + out.readUInt32BE(1)).toString());
    assert.equal(end.error.code, "invalid_argument", accept);
    assert.equal(end.error.details, undefined); // no ErrorInfo, so no reason
  }
});

test("trace context: the guest gets the call's trace with a span of its own, never the client's", async () => {
  const { buildEnv, guestTraceparent } = await import("../src/wagi.js");
  const tp = guestTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", () => "1111111111111111");
  assert.equal(tp, "00-4bf92f3577b34da6a3ce929d0e0e4736-1111111111111111-01");
  for (const bad of [null, "", "garbage", "00-" + "0".repeat(32) + "-00f067aa0ba902b7-01", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"]) assert.equal(guestTraceparent(bad), null, String(bad));
  const client = ["traceparent", "tracestate", "baggage"].map((name) => ({ name, value: bytes("client") }));
  const names = (m) => [...m.keys()].filter((k) => /TRACE|BAGGAGE/.test(k));
  assert.deepEqual(names(buildEnv(request({ headers: client }), 0, [], tp)), ["HTTP_TRACEPARENT"]);
  assert.equal(buildEnv(request({ headers: client }), 0, [], tp).get("HTTP_TRACEPARENT").toString(), tp);
  assert.deepEqual(names(buildEnv(request({ headers: client }), 0, [])), []);
});

test("RESPONSE_TOO_LARGE: the guest is stopped as soon as it crosses the limit, nothing is streamed", async () => {
  const t0 = Date.now();
  // This guest writes forever; it only ends because the worker stops it.
  const o = await execute(guests.flood(), { limits: { maxResponseBytes: 4096n }, timeoutMs: 20_000 });
  assert.equal(reason(o), ErrorReason.RESPONSE_TOO_LARGE);
  assert.equal(o.err.code, Code.ResourceExhausted);
  assert.equal(o.head, undefined);
  assert.ok(Date.now() - t0 < 5000);
});

test("headers count towards the response limit", async () => {
  const o = await execute(guests.respond("Content-Type: " + "a".repeat(200) + "\n\n"), { limits: { maxResponseBytes: 100n } });
  assert.equal(reason(o), ErrorReason.RESPONSE_TOO_LARGE);
});

test("DEADLINE_EXCEEDED: a spinning guest is really stopped and its slot comes back", async () => {
  const t0 = Date.now();
  const o = await execute(guests.spin(), { timeoutMs: 300 });
  assert.equal(reason(o), ErrorReason.DEADLINE_EXCEEDED);
  assert.equal(o.err.code, Code.DeadlineExceeded);
  assert.ok(Date.now() - t0 < 2000);
  assert.equal(w.impl.inFlight, 0);
});

test("memory: a guest cannot grow past its limit, even with no system call to notice", async () => {
  const o = await execute(guests.growOrTrap(4096), { limits: { memoryLimitBytes: BigInt(16 * MiB) } }); // wants 256 MiB, limit rounds to 16
  assert.equal(reason(o), ErrorReason.FUNCTION_TRAPPED);
  const ok = await execute(guests.growOrTrap(100), { limits: { memoryLimitBytes: BigInt(16 * MiB) } });
  assert.equal(ok.err, undefined);
});

test("capacity is never queued: NO_CAPACITY at once, slots come back", async () => {
  const slow = [guests.spin(), guests.spin()];
  const running = slow.map((g, i) => execute(g, { timeoutMs: 1500 + i }));
  await until(() => w.impl.inFlight === 2, 5000, "both slots busy");
  const t0 = Date.now();
  const o = await execute(guests.respond());
  assert.equal(reason(o), ErrorReason.NO_CAPACITY);
  assert.equal(o.err.code, Code.ResourceExhausted);
  assert.ok(Date.now() - t0 < 500);
  for (const r of await Promise.all(running)) assert.equal(reason(r), ErrorReason.DEADLINE_EXCEEDED);
  assert.equal((await execute(guests.respond())).err, undefined);
});

test("a caller that goes away stops the guest and frees the slot", async () => {
  const ac = new AbortController();
  const p = execute(guests.spin(), { signal: ac.signal, timeoutMs: 20_000 });
  await until(() => w.impl.inFlight === 1, 5000, "the guest to start");
  ac.abort();
  await p;
  await until(() => w.impl.inFlight === 0, 3000, "the slot to come back");
});

test("authentication: missing or wrong token is UNAUTHENTICATED, on Execute and on Describe", async () => {
  const url = `http://127.0.0.1:${new URL(w.url).port}`;
  for (const token of [null, "wrong", orch.workerToken]) {
    const c = clientWith(url, token);
    const o = await execute(guests.respond(), { client: c });
    assert.equal(o.err?.code, Code.Unauthenticated, String(token));
    await assert.rejects(c.describe({}), (e) => ConnectError.from(e).code === Code.Unauthenticated);
  }
  assert.ok((await exec.describe({})).info.abis.includes("wagi-1"));
});

test("Describe agrees with what was registered", async () => {
  const reg = orch.events.find((e) => e.kind === "register");
  const d = (await exec.describe({})).info;
  assert.deepEqual(d.abis, reg.info.abis);
  assert.equal(d.maxConcurrency, 2);
  assert.deepEqual(d.implementation.protocols, ["edgerun.worker.v1"]);
});

test("heartbeats at the dictated interval; the orchestrator forgetting the worker makes it register again", async () => {
  const hb = () => orch.events.filter((e) => e.kind === "heartbeat");
  const n = hb().length;
  await until(() => hb().length >= n + 3, 3000, "three heartbeats");
  assert.ok(hb().slice(-3).every((e) => e.state === WorkerState.READY));
  const regs = orch.events.filter((e) => e.kind === "register").length;
  orch.state.forget = orch.events.findLast((e) => e.kind === "register").id;
  await until(() => orch.events.filter((e) => e.kind === "register").length > regs, 3000, "re-registration");
  const ids = orch.events.filter((e) => e.kind === "register").map((e) => e.id);
  assert.notEqual(ids.at(-1), ids.at(-2));
});

test("shutdown: DRAINING first, in-flight work finishes, new work gets NO_CAPACITY, then Deregister", async () => {
  const busy = execute(guests.spin(), { timeoutMs: 1200 });
  await until(() => w.impl.inFlight === 1, 3000, "in flight");
  const stopping = w.stop();
  await until(() => orch.events.some((e) => e.kind === "heartbeat" && e.state === WorkerState.DRAINING), 3000, "DRAINING heartbeat");
  assert.equal(reason(await execute(guests.respond())), ErrorReason.NO_CAPACITY);
  assert.equal(reason(await busy), ErrorReason.DEADLINE_EXCEEDED); // allowed to run to its own end
  await stopping;
  const kinds = orch.events.map((e) => e.kind + ":" + (e.state ?? ""));
  const firstDraining = orch.events.findIndex((e) => e.kind === "heartbeat" && e.state === WorkerState.DRAINING);
  const dereg = orch.events.findIndex((e) => e.kind === "deregister");
  assert.ok(firstDraining >= 0 && dereg > firstDraining, kinds.join(" "));
});
