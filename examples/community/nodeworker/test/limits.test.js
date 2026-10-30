// The runner and its limits, against hand-assembled guests (no toolchain, no orchestrator).
import assert from "node:assert/strict";
import { test } from "node:test";
import { runGuest } from "../src/runner.js";
import { capMemory } from "../src/wasmpatch.js";
import { guest, guests } from "./wasm.js";

const MiB = 1024 * 1024;
const run = async (bytes, o = {}) => runGuest({
  module: await WebAssembly.compile(bytes), env: [], body: new Uint8Array(0),
  maxOutput: MiB, memLimit: 64 * MiB, stderrMax: 1024, ...o,
});

test("a guest that only grows memory and spins is stopped by the deadline", async () => {
  const t0 = Date.now();
  const out = await run(guests.spin(), { deadlineAt: Date.now() + 300 });
  assert.equal(out.type, "deadline");
  assert.ok(Date.now() - t0 < 2000);
});

test("without a cap, a guest can grow memory far past the limit and no system call notices", async () => {
  // This is the platform limit the memory cap in wasmpatch.js exists to close.
  const out = await run(guests.growThenSpin(4096), { deadlineAt: Date.now() + 500 }); // 256 MiB > 64 MiB limit
  assert.equal(out.type, "deadline");
});

test("capMemory: memory.grow beyond the limit fails inside the guest", async () => {
  const capped = capMemory(guests.growOrTrap(4096), (64 * MiB) / 65536);
  assert.ok(capped);
  const out = await run(capped.bytes); // grow fails -> -1 -> unreachable -> trap
  assert.equal(out.type, "trap");
  assert.match(out.message, /unreachable/i);
  const ok = await run(capMemory(guests.growOrTrap(100), (64 * MiB) / 65536).bytes);
  assert.equal(ok.type, "done"); // 100 pages (6.25 MiB) fit
});

const wasiStub = { wasi_snapshot_preview1: { fd_write() { return 0; }, proc_exit() {} } };

test("capMemory adds a maximum, keeps a lower existing one, and leaves other sections alone", async () => {
  const instantiate = async (bytes) => (await WebAssembly.instantiate(bytes, wasiStub)).instance;
  const capped = await instantiate(capMemory(guests.nothing(), 10).bytes);
  assert.deepEqual(Object.keys(capped.exports).sort(), ["_start", "memory"]);
  assert.equal(capped.exports.memory.grow(9), 1); // 1 -> 10 pages: allowed
  assert.throws(() => capped.exports.memory.grow(1), RangeError); // 11 > 10: refused

  const lower = await instantiate(capMemory(guest([], { memoryPages: 1, maxPages: 2 }), 1000).bytes);
  assert.throws(() => lower.exports.memory.grow(2), RangeError); // the module's own maximum (2) still holds
});

test("a trapping guest is reported as a trap, a guest writing nothing exits 0 with empty stdout", async () => {
  assert.equal((await run(guests.unreachable())).type, "trap");
  const out = await run(guests.nothing());
  // _start is not even called through wasi here (no imports), but the runner reports a clean exit
  assert.equal(out.type, "done");
  assert.equal(out.stdout.length, 0);
});
