// The worker's side of FleetService: register, heartbeat at the dictated
// interval, re-register when told, announce DRAINING and deregister on exit.
import { Code, ConnectError } from "@connectrpc/connect";
import { WorkerState } from "../gen/edgerun/worker/v1/worker_pb.js";

const sleep = (ms, signal) => new Promise((resolve) => {
  const t = setTimeout(resolve, ms);
  signal?.addEventListener("abort", () => { clearTimeout(t); resolve(); }, { once: true });
});

export class FleetClient {
  /**
   * @param {{client:any, endpoint:string, info:object, impl:{state:string,inFlight:number}, log?:Function}} o
   */
  constructor({ client, endpoint, info, impl, log = () => {} }) {
    this.client = client;
    this.endpoint = endpoint;
    this.info = info;
    this.impl = impl;
    this.log = log;
    this.workerId = "";
    this.intervalMs = 5000;
    this.stopped = new AbortController();
    this.loop = null;
    this.registered = new Promise((r) => { this.onRegistered = r; });
  }

  /** Register (retrying until it works, unless the orchestrator refuses) then heartbeat forever. */
  start() {
    this.loop = this.#run();
    return this.registered;
  }

  async #register() {
    let delay = 200;
    for (;;) {
      if (this.stopped.signal.aborted) return false;
      try {
        const res = await this.client.register({ endpoint: this.endpoint, info: this.info }, { timeoutMs: 10_000 });
        this.workerId = res.workerId;
        const d = res.heartbeatInterval;
        if (d) this.intervalMs = Number(d.seconds) * 1000 + d.nanos / 1e6;
        this.log("info", "registered", { workerId: this.workerId, heartbeatMs: this.intervalMs });
        this.onRegistered(this.workerId);
        return true;
      } catch (e) {
        const err = ConnectError.from(e);
        // A refused registration is a configuration problem for the operator,
        // not something to hammer the orchestrator with.
        const refused = [Code.InvalidArgument, Code.Unauthenticated, Code.PermissionDenied].includes(err.code);
        this.log(refused ? "error" : "warn", "register failed", { code: Code[err.code], message: err.rawMessage });
        await sleep(refused ? 30_000 : delay, this.stopped.signal);
        delay = Math.min(delay * 2, 5000);
      }
    }
  }

  async #run() {
    if (!(await this.#register())) return;
    while (!this.stopped.signal.aborted) {
      await sleep(this.intervalMs, this.stopped.signal);
      if (this.stopped.signal.aborted) return;
      await this.heartbeat();
    }
  }

  async heartbeat() {
    if (process.env.NODEWORKER_DEBUG) this.log("debug", "heartbeat", { state: this.impl.state, inFlight: this.impl.inFlight });
    try {
      const res = await this.client.heartbeat({
        workerId: this.workerId,
        state: this.impl.state === "READY" ? WorkerState.READY : WorkerState.DRAINING,
        inFlight: this.impl.inFlight,
      }, { timeoutMs: Math.max(1000, this.intervalMs) });
      if (res.reregister) {
        this.log("info", "orchestrator does not know this worker; registering again");
        await this.#register();
      }
    } catch (e) {
      this.log("warn", "heartbeat failed", { message: ConnectError.from(e).rawMessage });
    }
  }

  /** Report DRAINING now (state must already be flipped on impl). */
  drain() { return this.heartbeat(); }

  /** Stop heartbeating and leave the fleet. Idempotent. */
  async deregister() {
    this.stopped.abort();
    if (!this.workerId) return;
    try {
      await this.client.deregister({ workerId: this.workerId }, { timeoutMs: 5000 });
      this.log("info", "deregistered", { workerId: this.workerId });
    } catch (e) {
      this.log("warn", "deregister failed", { message: ConnectError.from(e).rawMessage });
    }
  }
}
