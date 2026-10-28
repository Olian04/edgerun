#!/usr/bin/env bash
# Runs the edgerun worker conformance suite against this worker.
#   EDGERUN_CONFORMANCE=/path/to/edgerun-conformance scripts/conformance.sh [extra suite flags, e.g. --run REGEX]
# Ports default to 22010 (worker) and 22011 (suite); override with WORKER_PORT / SUITE_PORT.
set -uo pipefail
cd "$(dirname "$0")/.."
SUITE="${EDGERUN_CONFORMANCE:?set EDGERUN_CONFORMANCE to the edgerun-conformance executable}"
WP="${WORKER_PORT:-22010}"; SP="${SUITE_PORT:-22011}"
TMP="$(mktemp -d)"; trap 'kill "$(cat "$TMP/pid" 2>/dev/null)" 2>/dev/null; rm -rf "$TMP"' EXIT
CONC="${MAX_CONCURRENCY:-2}"   # the suite's default for --max-concurrency
# The tokens are what `--print-config` prints for the default options.
start_worker() {
  node src/main.js --orchestrator-url "http://127.0.0.1:$SP" \
    --worker-token conformance-worker-token --execution-token conformance-execution-token \
    --max-concurrency "$CONC" --advertise-url "http://127.0.0.1:$WP" --listen "127.0.0.1:$WP" \
    2>"${WORKER_LOG:-$TMP/worker.log}" &
  echo $! >"$TMP/pid"
}
start_worker
"$SUITE" --role worker --target "127.0.0.1:$WP" --listen "127.0.0.1:$SP" \
  --max-concurrency "$CONC" --stop-cmd "kill -TERM \$(cat $TMP/pid)" "$@"
rc=$?
exit $rc
