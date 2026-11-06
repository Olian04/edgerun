#!/bin/sh
# Run the orchestrator conformance suite against the blind orchestrator.
# Needs bin/edgerun-conformance at the repo root (make conformance).
# Usage: ./run-orchestrator.sh [extra edgerun-conformance flags, e.g. --run Name]
cd "$(dirname "$0")" || exit 1
node orchestrator/main.js --listen 127.0.0.1:8081 --ingress-token conformance-ingress-token \
  --worker-token conformance-worker-token --admin-token conformance-admin-token \
  --network-token conformance-network-token --execution-token conformance-execution-token \
  --heartbeat-interval 200ms > /tmp/orch-app.log 2>&1 &
APP=$!
sleep 1
../../../bin/edgerun-conformance --role orchestrator --target 127.0.0.1:8081 "$@" > /tmp/orch-suite.log 2>&1
kill $APP
