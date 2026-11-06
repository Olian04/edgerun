#!/usr/bin/env node
// edgerun worker in Node.js. Flags mirror edgerun's own worker; each has an
// EDGERUN_* environment variable equivalent.
import { parseArgs } from "node:util";
import { startWorker } from "./worker.js";

const { values: v } = parseArgs({
  options: {
    "orchestrator-url": { type: "string" },
    "worker-token": { type: "string" },
    "execution-token": { type: "string" },
    "egress-token": { type: "string" }, // accepted for parity; wagi-2 is not offered
    "max-concurrency": { type: "string" },
    "advertise-url": { type: "string" },
    listen: { type: "string" },
  },
});
const pick = (flag, env, dflt) => v[flag] ?? process.env[env] ?? dflt;

const cfg = {
  orchestratorUrl: pick("orchestrator-url", "EDGERUN_ORCHESTRATOR_URL"),
  workerToken: pick("worker-token", "EDGERUN_WORKER_TOKEN"),
  executionToken: pick("execution-token", "EDGERUN_EXECUTION_TOKEN"),
  listen: pick("listen", "EDGERUN_WORKER_LISTEN", ":9000"),
  advertiseUrl: pick("advertise-url", "EDGERUN_WORKER_ADVERTISE_URL"),
  maxConcurrency: Number(pick("max-concurrency", "EDGERUN_WORKER_MAX_CONCURRENCY", "4")),
};
for (const [k, val] of Object.entries(cfg)) {
  if (val === undefined || val === "") { console.error(`missing configuration: ${k}`); process.exit(2); }
}
if (!(cfg.maxConcurrency >= 1)) { console.error("max-concurrency must be >= 1"); process.exit(2); }
cfg.log = (level, msg, fields = {}) => console.error(JSON.stringify({ t: new Date().toISOString(), level, msg, ...fields }));

const w = await startWorker(cfg);
for (const sig of ["SIGTERM", "SIGINT"]) {
  process.once(sig, async () => {
    cfg.log("info", "shutting down", { signal: sig });
    await w.stop();
    process.exit(0);
  });
}
