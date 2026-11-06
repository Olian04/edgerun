// The worker's side of MembershipService: register, heartbeat at the dictated
// interval, re-register when told, announce DRAINING and deregister on exit.
import { Code, ConnectError } from "@connectrpc/connect";
import { MemberState } from "../gen/edgerun/membership/v1/membership_pb.js";

const sleep = (ms, signal) => new Promise((resolve) => {
  const t = setTimeout(resolve, ms);
  signal?.addEventListener("abort", () => { clearTimeout(t); resolve(); }, { once: true });
});

export class MembershipClient {
  /**
   * @param {{client:any, endpoint:string, info:object, impl:{state:string,inFlight:number}, log?:Function}} o
   */
  constructor({ client, endpoint, info, impl, log = () => {} }) {
    this.client = client;
    this.endpoint = endpoint;
    this.info = info;
    this.impl = impl;
    this.log = log;
    this.memberId = "";
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
        const res = await this.client.register({ endpoint: this.endpoint, info: { case: "worker", value: this.info } }, { timeoutMs: 10_000 });
        this.memberId = res.memberId;
        const d = res.heartbeatInterval;
        if (d) this.intervalMs = Number(d.seconds) * 1000 + d.nanos / 1e6;
        this.log("info", "registered", { memberId: this.memberId, heartbeatMs: this.intervalMs });
        this.onRegistered(this.memberId);
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
        memberId: this.memberId,
        state: this.impl.state === "READY" ? MemberState.READY : MemberState.DRAINING,
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

  /** Stop heartbeating and leave the membership. Idempotent. */
  async deregister() {
    this.stopped.abort();
    if (!this.memberId) return;
    try {
      await this.client.deregister({ memberId: this.memberId }, { timeoutMs: 5000 });
      this.log("info", "deregistered", { memberId: this.memberId });
    } catch (e) {
      this.log("warn", "deregister failed", { message: ConnectError.from(e).rawMessage });
    }
  }
}
