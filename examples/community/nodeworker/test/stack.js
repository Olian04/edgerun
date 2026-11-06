// Starts a real edgerun orchestrator (separate process, private PostgreSQL) for
// tests. Needs the orchestrator binary (EDGERUN_ORCHESTRATOR_BIN, or a Go
// toolchain to build ./cmd/orchestrator) and PostgreSQL server binaries
// (initdb, pg_ctl) -- the same prerequisites as scripts/devstack.sh.
import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, chmodSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
export const repoRoot = path.resolve(here, "../../../..");
export const projectRoot = path.resolve(here, "..");

export const tokens = {
  ingress: "t-ingress", worker: "t-worker", admin: "t-admin", execution: "t-execution", network: "t-network",
};

const portLo = Number(process.env.NODEWORKER_PORT_LO ?? 22040), portHi = Number(process.env.NODEWORKER_PORT_HI ?? 22059);

export async function freePort() {
  for (let p = portLo; p <= portHi; p++) {
    const ok = await new Promise((resolve) => {
      const s = net.createServer().once("error", () => resolve(false)).listen(p, "127.0.0.1", () => s.close(() => resolve(true)));
    });
    if (ok && !taken.has(p)) { taken.add(p); return p; }
  }
  throw new Error(`no free port in ${portLo}-${portHi}`);
}
const taken = new Set();

function pgBin() {
  try { return path.dirname(execFileSync("which", ["initdb"], { encoding: "utf8" }).trim()); } catch {}
  const base = "/usr/lib/postgresql";
  if (existsSync(base)) {
    const v = readdirSync(base).sort().pop();
    if (v) return path.join(base, v, "bin");
  }
  throw new Error("PostgreSQL server binaries (initdb) not found");
}

const asPg = (cmd, args, opts = {}) =>
  process.getuid() === 0 ? execFileSync("runuser", ["-u", "postgres", "--", cmd, ...args], opts) : execFileSync(cmd, args, opts);

async function waitHttp(url, ms = 20000) {
  const end = Date.now() + ms;
  for (;;) {
    try { if ((await fetch(url)).ok) return; } catch {}
    if (Date.now() > end) throw new Error(`${url} did not come up`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

export function sh(cmd, args, opts) {
  return execFileSync(cmd, args, { encoding: "utf8", ...opts });
}

/** Build (once) what the stack needs from this repository with the Go toolchain. */
export function buildBinaries(dir) {
  const orch = process.env.EDGERUN_ORCHESTRATOR_BIN ?? path.join(dir, "orchestrator");
  if (!process.env.EDGERUN_ORCHESTRATOR_BIN) sh("go", ["build", "-o", orch, "./cmd/orchestrator"], { cwd: repoRoot });
  const hello = process.env.EDGERUN_HELLO_WASM ?? path.join(dir, "hello.wasm");
  if (!process.env.EDGERUN_HELLO_WASM) {
    sh("go", ["build", "-trimpath", "-o", hello, "./examples/hello"], { cwd: repoRoot, env: { ...process.env, GOOS: "wasip1", GOARCH: "wasm", CGO_ENABLED: "0" } });
  }
  return { orch, hello };
}

export async function startStack() {
  const dir = mkdtempSync(path.join(tmpdir(), "nodeworker-e2e-"));
  chmodSync(dir, 0o755);
  const { orch, hello } = buildBinaries(dir);
  const pg = pgBin();
  const pgdata = path.join(dir, "pgdata");
  const run = path.join(dir, "run");
  mkdirSync(run);
  if (process.getuid() === 0) sh("chown", ["postgres:postgres", dir, run]);
  asPg(path.join(pg, "initdb"), ["-D", pgdata, "-A", "trust", "-U", "postgres", "--no-sync", "-E", "UTF8"], { stdio: "ignore" });
  asPg(path.join(pg, "pg_ctl"), ["-D", pgdata, "-l", path.join(dir, "postgres.log"), "-o", `-c listen_addresses='' -c unix_socket_directories=${run} -c fsync=off`, "-w", "start"], { stdio: "ignore" });
  asPg(path.join(pg, "createdb"), ["-h", run, "-U", "postgres", "edgerun"]);

  const cleanup = () => {
    try { asPg(path.join(pg, "pg_ctl"), ["-D", pgdata, "-m", "fast", "stop"], { stdio: "ignore" }); } catch {}
    rmSync(dir, { recursive: true, force: true });
  };
  let proc;
  try {
    const orchPort = await freePort();
    proc = spawn(orch, [], {
      env: {
        ...process.env,
        EDGERUN_DATABASE_URL: `postgres://postgres@/edgerun?host=${run}`,
        EDGERUN_ORCHESTRATOR_LISTEN: `127.0.0.1:${orchPort}`,
        EDGERUN_INGRESS_TOKEN: tokens.ingress, EDGERUN_WORKER_TOKEN: tokens.worker, EDGERUN_ADMIN_TOKEN: tokens.admin,
        EDGERUN_EXECUTION_TOKEN: tokens.execution, EDGERUN_NETWORK_TOKEN: tokens.network,
        EDGERUN_SECRET_KEY: Buffer.alloc(32, 7).toString("base64"),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let log = "";
    proc.stdout.on("data", (d) => (log += d));
    proc.stderr.on("data", (d) => (log += d));
    const url = `http://127.0.0.1:${orchPort}`;
    await waitHttp(`${url}/healthz`);
    return {
      url, hello, dir, orchPort, logs: () => log,
      async stop() {
        proc.kill("SIGTERM");
        await new Promise((r) => { proc.once("exit", r); setTimeout(r, 5000).unref(); });
        cleanup();
      },
    };
  } catch (e) {
    proc?.kill("SIGKILL");
    cleanup();
    throw e;
  }
}
