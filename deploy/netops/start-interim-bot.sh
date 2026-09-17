#!/usr/bin/env bash
# Telethon-only bot (no public webhook). Run on NetOps until nginx /webhook is live.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env with TELEGRAM_TOKEN, TG_API_*, TELEGRAM_GROUP_CHAT_ID, Supabase." >&2
  exit 1
fi

docker compose -f docker-compose.yml -f docker-compose.interim-netops.yml up -d --build bot

echo "Waiting for bot..."
for _ in $(seq 1 40); do
  if curl -sf "http://127.0.0.1:8000/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

curl -sS "http://127.0.0.1:8000/health" | python3 -m json.tool
echo ""
docker logs coverage-eye-bot --tail 25 2>&1 | grep -E "Telethon|webhook|ingest" || docker logs coverage-eye-bot --tail 15
