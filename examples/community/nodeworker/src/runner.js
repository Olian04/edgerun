// Runs a guest in its own worker_threads Worker and enforces the wall-clock
// deadline, cancellation, the output limit and (as far as node allows) the
// memory limit by terminating that thread.
import { Worker } from "node:worker_threads";

const THREAD = new URL("./guest-thread.js", import.meta.url);

/**
 * @typedef {{type:"done",exitCode:number,stdout:Buffer,stderr:Buffer,stderrTruncated:boolean,peak:number}
 *  | {type:"limit",kind:"output"|"memory",stderr:Buffer,stderrTruncated:boolean,peak:number}
 *  | {type:"trap",message:string,stderr:Buffer,stderrTruncated:boolean,peak:number}
 *  | {type:"deadline",stderr:Buffer,stderrTruncated:boolean,peak:number}
 *  | {type:"cancelled"}} Outcome
 */

/**
 * @param {{module:WebAssembly.Module, env:Uint8Array[] (NAME=value entries, bytes), body:Uint8Array,
 *   maxOutput:number, memLimit:number, stderrMax:number, deadlineAt?:number, signal?:AbortSignal}} o
 * @returns {Promise<Outcome>} resolves only after the thread has exited, so the
 *   caller may free its slot when this settles.
 */
export function runGuest(o) {
  return new Promise((resolve) => {
    const worker = new Worker(THREAD, {
      workerData: {
        module: o.module, env: o.env, body: o.body, maxOutput: o.maxOutput,
        memLimit: o.memLimit, stderrMax: o.stderrMax,
      },
      execArgv: ["--no-warnings"], // node:wasi is "experimental" and says so on stderr
      stdout: true, stderr: true, stdin: false,
      // Bounds the JS heap of the thread (not the guest's linear memory).
      resourceLimits: { maxOldGenerationSizeMb: 64, maxYoungGenerationSizeMb: 16 },
    });
    let outcome = null;
    let timer;
    const settle = (value) => { if (outcome === null) outcome = value; };
    const kill = (value) => { settle(value); worker.terminate(); };

    // Structured clone turns Buffers into plain Uint8Arrays.
    const buf = (u) => (u ? Buffer.from(u.buffer, u.byteOffset, u.byteLength) : Buffer.alloc(0));
    worker.once("message", (m) => settle({ ...m, stdout: buf(m.stdout), stderr: buf(m.stderr) }));
    worker.once("error", (e) => settle({ type: "trap", message: String(e?.message ?? e), stderr: Buffer.alloc(0), stderrTruncated: false, peak: 0 }));
    worker.once("exit", () => {
      clearTimeout(timer);
      o.signal?.removeEventListener("abort", onAbort);
      resolve(outcome ?? { type: "trap", message: "the guest thread exited without a result", stderr: Buffer.alloc(0), stderrTruncated: false, peak: 0 });
    });

    const onAbort = () => kill({ type: "cancelled" });
    if (o.signal) {
      if (o.signal.aborted) onAbort();
      else o.signal.addEventListener("abort", onAbort, { once: true });
    }
    if (o.deadlineAt !== undefined) {
      const empty = { stderr: Buffer.alloc(0), stderrTruncated: false, peak: 0 };
      timer = setTimeout(() => kill({ type: "deadline", ...empty }), Math.max(0, o.deadlineAt - Date.now()));
    }
  });
}
