#!/bin/zsh
set -euo pipefail

APP_ROOT="/Users/laurencedeer/Desktop/BuiltWith"
BACKEND_URL="http://127.0.0.1:8765/api/health"
FRONTEND_URL="http://127.0.0.1:5173/"
BACKEND_LOG="/tmp/builtwith-backend.log"
FRONTEND_LOG="/tmp/builtwith-frontend.log"

cd "$APP_ROOT"

function is_up() {
  local url="$1"
  curl -fsS "$url" >/dev/null 2>&1
}

function wait_for_url() {
  local url="$1"
  local label="$2"
  local attempts="${3:-40}"
  for ((i=1; i<=attempts; i++)); do
    if is_up "$url"; then
      return 0
    fi
    sleep 0.5
  done
  echo "$label did not become ready in time." >&2
  return 1
}

function start_backend() {
  if is_up "$BACKEND_URL"; then
    echo "Backend already running."
    return 0
  fi

  echo "Starting backend..."
  nohup "$APP_ROOT/.venv/bin/python3" -m uvicorn backend.main:app --host 127.0.0.1 --port 8765 >"$BACKEND_LOG" 2>&1 &
  wait_for_url "$BACKEND_URL" "Backend"
}

function start_frontend() {
  if is_up "$FRONTEND_URL"; then
    echo "Frontend already running."
    return 0
  fi

  echo "Starting frontend..."
  nohup npm --prefix "$APP_ROOT/frontend" run dev -- --host 127.0.0.1 --port 5173 >"$FRONTEND_LOG" 2>&1 &
  wait_for_url "$FRONTEND_URL" "Frontend"
}

start_backend
start_frontend

echo "Opening Lead Console..."
open "$FRONTEND_URL"
