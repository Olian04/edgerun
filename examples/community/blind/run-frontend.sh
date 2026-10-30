#!/bin/sh
# Run the frontend conformance suite against the blind frontend.
# Needs bin/edgerun-conformance at the repo root (make conformance).
# Usage: ./run-frontend.sh [extra edgerun-conformance flags, e.g. --run Name]
cd "$(dirname "$0")" || exit 1
../../../bin/edgerun-conformance --role frontend --target 127.0.0.1:8080 --listen 127.0.0.1:7000 "$@" > /tmp/fe-suite.log 2>&1 &
SUITE=$!
sleep 1
node frontend/main.js --orchestrator-url http://127.0.0.1:7000 --ingress-token conformance-ingress-token --request-timeout 3s --listen 127.0.0.1:8080 > /tmp/fe-app.log 2>&1 &
APP=$!
wait $SUITE
kill $APP
