#!/bin/sh
# Blind frontend + blind orchestrator + the Node community worker, with a test guest.
# Usage: ./e2e.sh path/to/guest.wasm
cd "$(dirname "$0")" || exit 1
node orchestrator/main.js --listen 127.0.0.1:18081 --ingress-token i --worker-token w --admin-token a --network-token n --execution-token x --heartbeat-interval 1s &
O=$!
node frontend/main.js --orchestrator-url http://127.0.0.1:18081 --ingress-token i --request-timeout 10s --listen 127.0.0.1:18080 &
F=$!
(cd "${NODEWORKER_DIR:-../nodeworker}" && node src/main.js --orchestrator-url http://127.0.0.1:18081 --worker-token w --execution-token x --max-concurrency 2 --advertise-url http://127.0.0.1:18082 --listen 127.0.0.1:18082) &
W=$!
sleep 1
node e2e.mjs "$1"
kill $O $F $W
