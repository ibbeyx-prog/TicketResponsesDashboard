#!/usr/bin/env bash
# Start Coverage Eye on NetOps (dashboard + bot). Run from repo root on the server.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env — copy deploy/netops/.env.netops.example and fill secrets." >&2
  exit 1
fi

docker compose -f docker-compose.yml -f docker-compose.netops.yml up -d --build

echo "Waiting for bot health..."
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:8000/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "--- Local bot health ---"
curl -sS "http://127.0.0.1:8000/health" | python3 -m json.tool || curl -sS "http://127.0.0.1:8000/health"

echo ""
echo "Next: reload nginx if you changed deploy/netops/nginx-coverage-eye.conf"
echo "Then run: py -3 scripts/verify_netops_bot.py"
