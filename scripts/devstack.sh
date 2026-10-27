#!/usr/bin/env bash
# Runs a complete edgerun stack locally, as real processes, for development and
# demos: a private PostgreSQL, the orchestrator, a worker, the frontend and the
# admin UI, with the hello example deployed.
#
#   scripts/devstack.sh start     build, start everything, print where to look
#   scripts/devstack.sh stop      stop everything and keep the data
#   scripts/devstack.sh clean     stop and delete all data
#
# Needs: Go, and the PostgreSQL server binaries (initdb, pg_ctl). Not for
# production: the credentials are fixed and printed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR="${EDGERUN_DEV_DIR:-${TMPDIR:-/tmp}/edgerun-dev}"
BIN="$DIR/bin"; LOG="$DIR/log"; RUN="$DIR/run"; PGDATA="$DIR/pgdata"

ORCH_PORT="${ORCH_PORT:-18080}"
FRONTEND_PORT="${FRONTEND_PORT:-18000}"
HEALTH_PORT="${HEALTH_PORT:-18081}"
WORKER_PORT="${WORKER_PORT:-19000}"
ADMIN_PORT="${ADMIN_PORT:-19090}"

INGRESS_TOKEN=dev-ingress-token
WORKER_TOKEN=dev-worker-token
ADMIN_TOKEN=dev-admin-token
EXEC_TOKEN=dev-execution-token
# 32 bytes, base64: dev only.
SECRET_KEY=ZGV2LW9ubHktc2VjcmV0LWtleS0zMi1ieXRlcyEhISE=

pg_bin() {
  if command -v initdb >/dev/null; then dirname "$(command -v initdb)"; return; fi
  ls -d /usr/lib/postgresql/*/bin 2>/dev/null | tail -1
}

as_pg() { if [ "$(id -u)" = 0 ]; then runuser -u postgres -- "$@"; else "$@"; fi; }

stop() {
  for p in admin frontend worker orchestrator; do
    if [ -f "$RUN/$p.pid" ]; then
      kill "$(cat "$RUN/$p.pid")" 2>/dev/null || true
      rm -f "$RUN/$p.pid"
    fi
  done
  # Give the processes their graceful shutdown (workers drain, then deregister).
  sleep 1
  if [ -d "$PGDATA" ]; then as_pg "$(pg_bin)/pg_ctl" -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true; fi
}

start() {
  command -v go >/dev/null || { echo "Go is required" >&2; exit 1; }
  PGB="$(pg_bin)"; [ -x "$PGB/initdb" ] || { echo "PostgreSQL server binaries not found (initdb)" >&2; exit 1; }
  mkdir -p "$BIN" "$LOG" "$RUN"
  stop

  echo "==> building"
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/orchestrator ./cmd/worker ./cmd/frontend ./cmd/admin ./cmd/edgerunctl \
    && GOOS=wasip1 GOARCH=wasm CGO_ENABLED=0 go build -trimpath -o "$DIR/hello.wasm" ./examples/hello)

  echo "==> PostgreSQL"
  if [ "$(id -u)" = 0 ]; then mkdir -p "$DIR" && chown postgres:postgres "$DIR" "$RUN" 2>/dev/null || true; chmod 755 "$DIR"; fi
  if [ ! -d "$PGDATA" ]; then as_pg "$PGB/initdb" -D "$PGDATA" -A trust -U postgres --no-sync -E UTF8 >/dev/null; fi
  as_pg "$PGB/pg_ctl" -D "$PGDATA" -l "$DIR/postgres.log" -o "-c listen_addresses='' -c unix_socket_directories=$DIR -c fsync=off" -w start >/dev/null
  as_pg "$PGB/psql" -h "$DIR" -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='edgerun'" | grep -q 1 \
    || as_pg "$PGB/createdb" -h "$DIR" -U postgres edgerun
  DSN="postgres://postgres@/edgerun?host=$DIR"

  ADMIN_USERS="alice:admin:$(echo admin | "$BIN/admin" hash-password 2>/dev/null);bob:viewer:$(echo viewer | "$BIN/admin" hash-password 2>/dev/null)"

  echo "==> starting"
  EDGERUN_DATABASE_URL="$DSN" EDGERUN_ORCHESTRATOR_LISTEN="127.0.0.1:$ORCH_PORT" \
  EDGERUN_INGRESS_TOKEN=$INGRESS_TOKEN EDGERUN_WORKER_TOKEN=$WORKER_TOKEN EDGERUN_ADMIN_TOKEN=$ADMIN_TOKEN \
  EDGERUN_EXECUTION_TOKEN=$EXEC_TOKEN EDGERUN_SECRET_KEY=$SECRET_KEY \
    nohup "$BIN/orchestrator" >"$LOG/orchestrator.log" 2>&1 & echo $! >"$RUN/orchestrator.pid"
  wait_http "http://127.0.0.1:$ORCH_PORT/healthz" orchestrator

  EDGERUN_ORCHESTRATOR_URL="http://127.0.0.1:$ORCH_PORT" EDGERUN_WORKER_TOKEN=$WORKER_TOKEN EDGERUN_EXECUTION_TOKEN=$EXEC_TOKEN \
  EDGERUN_WORKER_LISTEN="127.0.0.1:$WORKER_PORT" EDGERUN_WORKER_ADVERTISE_URL="http://127.0.0.1:$WORKER_PORT" \
    nohup "$BIN/worker" >"$LOG/worker.log" 2>&1 & echo $! >"$RUN/worker.pid"

  EDGERUN_ORCHESTRATOR_URL="http://127.0.0.1:$ORCH_PORT" EDGERUN_INGRESS_TOKEN=$INGRESS_TOKEN \
  EDGERUN_FRONTEND_LISTEN="127.0.0.1:$FRONTEND_PORT" EDGERUN_FRONTEND_HEALTH_LISTEN="127.0.0.1:$HEALTH_PORT" \
    nohup "$BIN/frontend" >"$LOG/frontend.log" 2>&1 & echo $! >"$RUN/frontend.pid"

  EDGERUN_ORCHESTRATOR_URL="http://127.0.0.1:$ORCH_PORT" EDGERUN_ADMIN_TOKEN=$ADMIN_TOKEN \
  EDGERUN_ADMIN_LISTEN="127.0.0.1:$ADMIN_PORT" EDGERUN_ADMIN_USERS="$ADMIN_USERS" \
    nohup "$BIN/admin" >"$LOG/admin.log" 2>&1 & echo $! >"$RUN/admin.pid"
  wait_http "http://127.0.0.1:$ADMIN_PORT/healthz" admin

  export EDGERUN_URL="http://127.0.0.1:$ORCH_PORT" EDGERUN_ADMIN_TOKEN=$ADMIN_TOKEN
  if ! "$BIN/edgerunctl" functions | grep -q '^hello '; then
    "$BIN/edgerunctl" create hello >/dev/null
    "$BIN/edgerunctl" deploy hello "$DIR/hello.wasm" --activate >/dev/null
    "$BIN/edgerunctl" route hello.localhost / hello >/dev/null
    "$BIN/edgerunctl" set hello GREETING Howdy >/dev/null
  fi

  cat <<MSG

edgerun is running.

  Admin UI      http://127.0.0.1:$ADMIN_PORT/          sign in: alice / admin   (viewer: bob / viewer)
  Try it        curl -H 'Host: hello.localhost' 'http://127.0.0.1:$FRONTEND_PORT/?name=you'
  CLI           EDGERUN_URL=http://127.0.0.1:$ORCH_PORT EDGERUN_ADMIN_TOKEN=$ADMIN_TOKEN $BIN/edgerunctl functions
  Logs          $LOG  (PostgreSQL: $DIR/postgres.log)
  Stop          scripts/devstack.sh stop
MSG
}

wait_http() {
  local url="$1" what="$2"
  for _ in $(seq 1 100); do curl -fs "$url" >/dev/null 2>&1 && return; sleep 0.2; done
  echo "$what did not come up; see $LOG/$what.log" >&2; tail -5 "$LOG/$what.log" >&2 || true; exit 1
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  clean) stop; rm -rf "$DIR" ;;
  *) echo "usage: $0 start|stop|clean" >&2; exit 2 ;;
esac
