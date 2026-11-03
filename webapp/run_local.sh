#!/usr/bin/env bash
# Run DataCruises webapp locally.
#
# Starts in parallel:
#   - FastAPI on :8000 (uvicorn, backed by uv-managed Python 3.11)
#   - Vite dev server on :5173 (proxies /api/* → :8000)
#
# Open http://localhost:5173 in a browser.
#
# Prereqs:
#   - uv on PATH         (brew install uv  OR  curl -LsSf https://astral.sh/uv/install.sh | sh)
#   - node + npm on PATH (brew install node)
#
# Usage: ./run_local.sh
set -euo pipefail

cd "$(dirname "$0")"

# Env ------------------------------------------------------------------
if [[ -f .env ]]; then
  echo "→ loading .env"
  set -a; source .env; set +a
elif [[ -f .env.example ]]; then
  echo "→ no .env found; loading .env.example (copy to .env to customize)"
  set -a; source .env.example; set +a
fi

# Prereq checks --------------------------------------------------------
command -v uv  >/dev/null 2>&1 || { echo "✗ uv not found  (brew install uv)"  >&2; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "✗ npm not found (brew install node)" >&2; exit 1; }

# Backend deps ---------------------------------------------------------
echo "→ uv sync (backend)"
uv sync --quiet

# Frontend deps --------------------------------------------------------
if [[ ! -d frontend/node_modules ]]; then
  echo "→ npm install (frontend) — first-run only"
  (cd frontend && npm install --silent)
fi

# Start both ----------------------------------------------------------
pids=()
cleanup() {
  echo
  echo "→ shutting down"
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "→ backend  (FastAPI)  :  http://localhost:8000"
uv run uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000 &
pids+=($!)

echo "→ frontend (Vite dev) :  http://localhost:5173   ← open this"
(cd frontend && npm run dev) &
pids+=($!)

echo
echo "Press Ctrl-C to stop both."

wait
