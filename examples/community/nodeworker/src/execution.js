// worker.v1 ExecutionService: Describe and Execute.
import { Code, ConnectError } from "@connectrpc/connect";
import { ErrorReason, invalidArgument, platformError } from "./errors.js";
import { runGuest } from "./runner.js";
import { AbiViolation, DIGEST, ENV_NAME, buildEnv, environEntries, guestTraceparent, malformedRequest, parseResponse, reservedEnvName } from "./wagi.js";

const MiB = 1024 * 1024;
const MEMORY_SIZES = [16, 32, 64, 128, 256, 512].map((m) => m * MiB);
const CHUNK = 256 * 1024;
const STDERR_TAIL = 64 * 1024;
const TRAP_MESSAGE_STDERR = 4 * 1024;

export const SUPPORTED_ABIS = ["wagi-1"];

export class ExecutionImpl {
  /**
   * @param {object} o
   * @param {import("./artifacts.js").ModuleStore} o.store
   * @param {number} o.maxConcurrency
   * @param {{defaultMemory?:number,maxMemory?:number,defaultResponse?:number,maxResponse?:number,fallbackDeadlineMs?:number}} [o.limits]
   */
  constructor({ store, maxConcurrency, info, limits = {}, log = () => {} }) {
    this.store = store;
    this.maxConcurrency = maxConcurrency;
    this.info = info;
    this.log = log;
    this.inFlight = 0;
    this.state = "READY"; // READY | DRAINING
    this.idle = [];
    this.limits = {
      defaultMemory: 128 * MiB, maxMemory: 512 * MiB,
      defaultResponse: 6 * MiB, maxResponse: 64 * MiB,
      fallbackDeadlineMs: 30_000, ...limits,
    };
  }

  describe() {
    return { info: this.info };
  }

  /** Resolves when nothing is in flight. */
  whenIdle() {
    return this.inFlight === 0 ? Promise.resolve() : new Promise((r) => this.idle.push(r));
  }

  #effective(l) {
    const mem = Number(l?.memoryLimitBytes ?? 0n);
    const memLimit = Math.min(this.limits.maxMemory, mem === 0 ? this.limits.defaultMemory : (MEMORY_SIZES.find((s) => s >= mem) ?? this.limits.maxMemory));
    const resp = Number(l?.maxResponseBytes ?? 0n);
    const maxOutput = Math.min(this.limits.maxResponse, resp === 0 ? this.limits.defaultResponse : resp);
    return { memLimit, maxOutput };
  }

  async *execute(req, ctx) {
    // 1. Shape. A malformed request is invalid_argument with no reason.
    if (!DIGEST.test(req.artifactDigest)) throw invalidArgument(`malformed artifact_digest ${JSON.stringify(req.artifactDigest)}`);
    if (!req.request) throw invalidArgument("request is missing");
    for (const e of req.env) {
      if (!ENV_NAME.test(e.name)) throw invalidArgument(`bad environment variable name ${JSON.stringify(e.name)}`);
      if (reservedEnvName(e.name)) throw invalidArgument(`environment variable ${e.name} collides with one the ABI sets`);
      if (e.value.includes("\0")) throw invalidArgument(`environment variable ${e.name} contains NUL`);
    }
    const malformed = malformedRequest(req.request); // docs/protocol.md "HTTP messages": who judges what
    if (malformed) throw invalidArgument(malformed);
    // 2. ABI.
    if (!SUPPORTED_ABIS.includes(req.abi)) throw platformError(ErrorReason.ABI_MISMATCH, `this worker runs ${SUPPORTED_ABIS.join(", ")}, not ${JSON.stringify(req.abi)}`);
    // 3. Capacity is never queued.
    if (this.state !== "READY" || this.inFlight >= this.maxConcurrency) {
      throw platformError(ErrorReason.NO_CAPACITY, this.state !== "READY" ? "worker is draining" : `all ${this.maxConcurrency} slots are busy`);
    }
    this.inFlight++;
    if (process.env.NODEWORKER_DEBUG) this.log("debug", "execute start", { path: req.request.path, inFlight: this.inFlight });
    const started = process.hrtime.bigint();
    try {
      const remaining = ctx.timeoutMs();
      const deadlineAt = Date.now() + (remaining ?? this.limits.fallbackDeadlineMs);
      const expired = () => platformError(ErrorReason.DEADLINE_EXCEEDED, "the invocation's deadline expired");
      if (remaining !== undefined && remaining <= 0) throw expired();

      // The caller going away (cancel) ends the invocation too.
      const abort = new AbortController();
      const onCtxAbort = () => abort.abort();
      ctx.signal.addEventListener("abort", onCtxAbort, { once: true });
      if (ctx.signal.aborted) abort.abort();
      try {
        // The deadline may be observed here, in the transport's own signal, or
        // in our timer: all of them mean the same typed error.
        const pastDeadline = () => Date.now() >= deadlineAt
          || (ctx.signal.reason instanceof ConnectError && ctx.signal.reason.code === Code.DeadlineExceeded);
        const { memLimit, maxOutput } = this.#effective(req.limits);
        let module;
        try {
          module = await this.store.get(req.artifactDigest, { memLimitBytes: memLimit, signal: abort.signal, timeoutMs: Math.max(0, deadlineAt - Date.now()) });
        } catch (e) {
          if (e instanceof ConnectError && (e.code === Code.DeadlineExceeded || (e.code === Code.Canceled && pastDeadline()))) throw expired();
          throw e;
        }
        const body = req.request.body;
        const env = environEntries(buildEnv(req.request, body.length, req.env, guestTraceparent(ctx.requestHeader.get("traceparent"))));
        const out = await runGuest({ module, env, body, maxOutput, memLimit, stderrMax: STDERR_TAIL, deadlineAt, signal: abort.signal });

        const tail = (b) => Buffer.from(b.subarray(Math.max(0, b.length - TRAP_MESSAGE_STDERR))).toString("utf8");
        switch (out.type) {
          case "cancelled":
            if (pastDeadline()) throw expired();
            throw new ConnectError("the call was cancelled", Code.Canceled);
          case "deadline":
            throw expired();
          case "limit":
            if (out.kind === "output") throw platformError(ErrorReason.RESPONSE_TOO_LARGE, `the guest wrote more than ${maxOutput} bytes`);
            throw platformError(ErrorReason.FUNCTION_TRAPPED, `the guest exceeded its memory limit of ${memLimit} bytes\n${tail(out.stderr)}`);
          case "trap":
            throw platformError(ErrorReason.FUNCTION_TRAPPED, `${out.message}\n${tail(out.stderr)}`);
        }
        if (out.exitCode !== 0) throw platformError(ErrorReason.FUNCTION_TRAPPED, `the guest exited with code ${out.exitCode}\n${tail(out.stderr)}`);
        let res;
        try {
          res = parseResponse(out.stdout);
        } catch (e) {
          if (e instanceof AbiViolation) throw platformError(ErrorReason.ABI_MISMATCH, `${e.message}\n${tail(out.stderr)}`);
          throw e;
        }
        yield { part: { case: "head", value: { status: res.status, headers: res.headers } } };
        for (let off = 0; off < res.body.length; off += CHUNK) {
          yield { part: { case: "chunk", value: res.body.subarray(off, off + CHUNK) } };
        }
        yield {
          part: {
            case: "summary",
            value: {
              durationMicros: (process.hrtime.bigint() - started) / 1000n,
              peakMemoryBytes: BigInt(out.peak),
              stderr: out.stderr,
              stderrTruncated: out.stderrTruncated,
            },
          },
        };
      } finally {
        ctx.signal.removeEventListener("abort", onCtxAbort);
        abort.abort(); // stop anything still running if the stream was abandoned
      }
    } finally {
      this.inFlight--;
      if (process.env.NODEWORKER_DEBUG) this.log("debug", "execute end", { inFlight: this.inFlight });
      if (this.inFlight === 0) for (const r of this.idle.splice(0)) r();
    }
  }
}
