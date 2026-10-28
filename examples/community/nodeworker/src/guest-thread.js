// Runs ONE wagi-1 guest instance inside a worker_threads Worker. The parent
// terminates this thread to enforce the deadline, so nothing here has to be
// cooperative: but the host calls below are written so that a terminate (or a
// limit) can never be stuck behind a blocking native call.
//
// node:wasi can only point guest stdio at real file descriptors, so a
// response limit that stops the guest "as soon as it crosses the limit" is not
// expressible with options alone. stdin/stdout/stderr (fd 0/1/2) and
// poll_oneoff are therefore re-implemented in JS on top of node:wasi's
// wasiImport; every other system call is node:wasi's.
import { openSync } from "node:fs";
import { parentPort, workerData } from "node:worker_threads";
import { WASI } from "node:wasi";

const { module, env, body, maxOutput, memLimit, stderrMax } = workerData;

const ESUCCESS = 0, EBADF = 8, EINVAL = 28;

class LimitExceeded extends Error {
  constructor(kind) { super(kind); this.kind = kind; }
}

const outChunks = [];
let outBytes = 0;
let errTail = Buffer.alloc(0);
let errTruncated = false;
let bodyPos = 0;
let peak = 0;
let memory; // set after instantiation

const devnull = openSync("/dev/null", "r+");
const wasi = new WASI({
  version: "preview1",
  args: ["guest"],
  env,
  returnOnExit: true,
  stdin: devnull, stdout: devnull, stderr: devnull, // never the process's own stdio
});
const sys = wasi.wasiImport;

function view() { return new DataView(memory.buffer); }

function noteMemory() {
  const n = memory.buffer.byteLength;
  if (n > peak) peak = n;
  if (n > memLimit) throw new LimitExceeded("memory");
}

// fd_write(fd, iovs, iovs_len, nwritten)
const nodeFdWrite = sys.fd_write;
sys.fd_write = (fd, iovs, iovsLen, nwrittenPtr) => {
  if (fd !== 1 && fd !== 2) return nodeFdWrite(fd, iovs, iovsLen, nwrittenPtr);
  const dv = view();
  const parts = [];
  let total = 0;
  for (let i = 0; i < iovsLen; i++) {
    const ptr = dv.getUint32(iovs + i * 8, true), len = dv.getUint32(iovs + i * 8 + 4, true);
    if (fd === 1 && outBytes + total + len > maxOutput) throw new LimitExceeded("output");
    parts.push(Buffer.from(memory.buffer, ptr, len));
    total += len;
  }
  const data = Buffer.concat(parts, total);
  if (fd === 1) {
    outChunks.push(data);
    outBytes += total;
  } else {
    errTail = Buffer.concat([errTail, data]);
    if (errTail.length > stderrMax) {
      errTail = errTail.subarray(errTail.length - stderrMax);
      errTruncated = true;
    }
  }
  dv.setUint32(nwrittenPtr, total, true);
  return ESUCCESS;
};

// fd_read(fd, iovs, iovs_len, nread): stdin is the request body, then EOF.
const nodeFdRead = sys.fd_read;
sys.fd_read = (fd, iovs, iovsLen, nreadPtr) => {
  if (fd !== 0) return nodeFdRead(fd, iovs, iovsLen, nreadPtr);
  const dv = view();
  let n = 0;
  for (let i = 0; i < iovsLen && bodyPos < body.length; i++) {
    const ptr = dv.getUint32(iovs + i * 8, true), len = dv.getUint32(iovs + i * 8 + 4, true);
    const k = Math.min(len, body.length - bodyPos);
    Buffer.from(memory.buffer, ptr, k).set(body.subarray(bodyPos, bodyPos + k));
    bodyPos += k;
    n += k;
  }
  dv.setUint32(nreadPtr, n, true);
  return ESUCCESS;
};

// poll_oneoff(in, out, nsubscriptions, nevents). node:wasi's implementation
// blocks the thread natively (see FINDINGS.md); this one sleeps in
// Atomics.wait, which Worker.terminate() can interrupt.
const sleeper = new Int32Array(new SharedArrayBuffer(4));
sys.poll_oneoff = (inPtr, outPtr, nsubs, neventsPtr) => {
  if (nsubs === 0) return EINVAL;
  const dv = view();
  const now = { 0: () => BigInt(Date.now()) * 1000000n, 1: () => process.hrtime.bigint() };
  const subs = [];
  let soonest = null;
  for (let i = 0; i < nsubs; i++) {
    const base = inPtr + i * 48;
    const userdata = dv.getBigUint64(base, true);
    const tag = dv.getUint8(base + 8);
    if (tag === 0) {
      const id = dv.getUint32(base + 16, true);
      const timeout = dv.getBigUint64(base + 24, true);
      const flags = dv.getUint16(base + 40, true);
      if (!now[id]) return EINVAL;
      const at = (flags & 1) ? timeout : now[id]() + timeout; // absolute or relative, on clock id
      const remaining = at - now[id]();
      subs.push({ userdata, tag, remaining });
      if (soonest === null || remaining < soonest) soonest = remaining;
    } else {
      subs.push({ userdata, tag, fd: dv.getUint32(base + 16, true) });
    }
  }
  const ready = subs.filter((s) => s.tag !== 0);
  if (ready.length === 0) {
    if (soonest > 0n) {
      const ms = Number(soonest / 1000000n) + (soonest % 1000000n ? 1 : 0);
      Atomics.wait(sleeper, 0, 0, ms);
    }
  }
  const done = ready.length ? ready.concat(subs.filter((s) => s.tag === 0 && s.remaining <= 0n))
    : subs.filter((s) => s.tag === 0 && s.remaining <= soonest);
  let n = 0;
  for (const s of done) {
    const o = outPtr + n * 32;
    dv.setBigUint64(o, s.userdata, true);
    dv.setUint16(o + 8, s.tag !== 0 && s.fd > 2 ? EBADF : ESUCCESS, true);
    dv.setUint8(o + 10, s.tag);
    dv.setBigUint64(o + 16, 0n, true);
    dv.setUint16(o + 24, 0, true);
    n++;
  }
  dv.setUint32(neventsPtr, n, true);
  return ESUCCESS;
};

// Memory: a wasm module defines its own memory, so the host cannot cap
// growth. The best node offers is to look after every system call (every
// guest runtime makes them regularly) and stop the guest if it has grown past
// the limit. See FINDINGS.md.
for (const name of Object.keys(sys)) {
  const f = sys[name];
  if (typeof f !== "function") continue;
  sys[name] = (...a) => {
    const r = f(...a);
    noteMemory();
    return r;
  };
}

function finish(msg, transfer = []) {
  parentPort.postMessage(msg, transfer);
}

try {
  const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
  memory = instance.exports.memory;
  peak = memory.buffer.byteLength;
  if (peak > memLimit) throw new LimitExceeded("memory");
  const exitCode = wasi.start(instance);
  noteMemory();
  const stdout = Buffer.concat(outChunks, outBytes);
  finish({ type: "done", exitCode, stdout, stderr: errTail, stderrTruncated: errTruncated, peak });
} catch (e) {
  const base = { stderr: errTail, stderrTruncated: errTruncated, peak };
  if (e instanceof LimitExceeded) finish({ type: "limit", kind: e.kind, ...base });
  else finish({ type: "trap", message: e instanceof Error ? `${e.name}: ${e.message}` : String(e), ...base });
}
