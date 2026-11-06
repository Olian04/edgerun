// Pulls modules from the orchestrator's ArtifactService by digest, verifies
// sha256 before anything is compiled or run, and caches the compiled module by
// digest (content addressing makes the cache valid forever).
import { createHash } from "node:crypto";
import { Code, ConnectError } from "@connectrpc/connect";
import { AbiViolation } from "./wagi.js";
import { PAGE, capMemory } from "./wasmpatch.js";
import { ErrorReason, platformError, reasonOf } from "./errors.js";

export class ModuleStore {
  /**
   * @param {{client: any, maxModuleBytes?: number, maxCached?: number, log?: Function}} o
   *   client: connect client for edgerun.worker.v1.ArtifactService
   */
  constructor({ client, maxModuleBytes = 256 * 1024 * 1024, maxCached = 64, log = () => {} }) {
    this.client = client;
    this.maxModuleBytes = maxModuleBytes;
    this.maxCached = maxCached;
    this.log = log;
    /** @type {Map<string, Promise<Entry>>} insertion order = LRU order; failures are removed */
    this.entries = new Map();
    this.fetches = 0; // for tests: how many times the network was used
  }

  /**
   * The verified module compiled with its memory capped at memLimitBytes.
   * @returns {Promise<WebAssembly.Module>}
   */
  async get(digest, { memLimitBytes, signal, timeoutMs } = {}) {
    let p = this.entries.get(digest);
    if (p) {
      this.entries.delete(digest);
      this.entries.set(digest, p); // refresh LRU position
    } else {
      // One shared load per digest, independent of any one caller's deadline.
      p = this.#load(digest);
      this.entries.set(digest, p);
      p.catch(() => { if (this.entries.get(digest) === p) this.entries.delete(digest); });
      while (this.entries.size > this.maxCached) this.entries.delete(this.entries.keys().next().value);
    }
    return raceWith(p.then((e) => e.moduleFor(memLimitBytes)), signal, timeoutMs);
  }

  async #load(digest) {
    const want = digest.slice("sha256:".length);
    const hash = createHash("sha256");
    const chunks = [];
    let size = 0, tooBig = false;
    this.fetches++;
    try {
      for await (const part of this.client.getArtifact({ digest }, { timeoutMs: 60_000 })) {
        size += part.chunk.length;
        if (size > this.maxModuleBytes) { tooBig = true; break; }
        hash.update(part.chunk);
        chunks.push(part.chunk);
      }
    } catch (e) {
      if (reasonOf(e) === ErrorReason.ARTIFACT_NOT_FOUND) throw platformError(ErrorReason.ARTIFACT_NOT_FOUND, `artifact ${digest} not found`);
      if (e instanceof ConnectError && e.code === Code.Canceled) throw e;
      throw platformError(ErrorReason.INTERNAL, `fetching artifact ${digest}: ${ConnectError.from(e).rawMessage}`);
    }
    if (tooBig) throw platformError(ErrorReason.INTERNAL, `artifact ${digest} exceeds ${this.maxModuleBytes} bytes`);
    if (hash.digest("hex") !== want) {
      // Never compile, run or cache bytes that are not the bytes asked for.
      this.log("warn", "artifact digest mismatch", { digest });
      throw platformError(ErrorReason.INTERNAL, `artifact ${digest} failed verification: the bytes received do not hash to the requested digest`);
    }
    const bytes = Buffer.concat(chunks, size);
    const entry = new Entry(bytes);
    await entry.moduleFor(undefined); // validates: a bad module fails here, once
    this.log("info", "artifact fetched", { digest, bytes: size });
    return entry;
  }
}

/** Verified bytes plus the compiled module for each memory limit asked for. */
class Entry {
  constructor(bytes) { this.bytes = bytes; this.variants = new Map(); }

  moduleFor(memLimitBytes) {
    const key = memLimitBytes ?? 0;
    let p = this.variants.get(key);
    if (!p) {
      p = (async () => {
        const capped = memLimitBytes === undefined ? null : capMemory(this.bytes, Math.floor(memLimitBytes / PAGE));
        return compileCommand(capped ? capped.bytes : this.bytes);
      })();
      this.variants.set(key, p);
      p.catch(() => this.variants.delete(key));
    }
    return p;
  }
}

/** Compile and check that it is a wagi-1 command: `_start`, only WASI imports. */
async function compileCommand(bytes) {
  let module;
  try {
    module = await WebAssembly.compile(bytes);
  } catch (e) {
    throw platformError(ErrorReason.ABI_MISMATCH, `not a valid WebAssembly module: ${e.message}`);
  }
  try {
    for (const imp of WebAssembly.Module.imports(module)) {
      if (imp.module !== "wasi_snapshot_preview1") throw new AbiViolation(`imports ${imp.module}.${imp.name}; wagi-1 allows only wasi_snapshot_preview1`);
    }
    const start = WebAssembly.Module.exports(module).find((e) => e.name === "_start");
    if (!start || start.kind !== "function") throw new AbiViolation("no `_start` export: not a WASI command");
    if (!WebAssembly.Module.exports(module).some((e) => e.name === "memory" && e.kind === "memory")) {
      throw new AbiViolation("no `memory` export");
    }
  } catch (e) {
    if (e instanceof AbiViolation) throw platformError(ErrorReason.ABI_MISMATCH, e.message);
    throw e;
  }
  return module;
}

function raceWith(promise, signal, timeoutMs) {
  if (!signal && timeoutMs === undefined) return promise;
  return new Promise((resolve, reject) => {
    let timer;
    const done = () => { clearTimeout(timer); signal?.removeEventListener("abort", onAbort); };
    const onAbort = () => { done(); reject(new ConnectError("aborted", Code.Canceled)); };
    if (signal?.aborted) return onAbort();
    signal?.addEventListener("abort", onAbort, { once: true });
    if (timeoutMs !== undefined) timer = setTimeout(() => { done(); reject(new ConnectError("deadline", Code.DeadlineExceeded)); }, timeoutMs);
    promise.then((v) => { done(); resolve(v); }, (e) => { done(); reject(e); });
  });
}
