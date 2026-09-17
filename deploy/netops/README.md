# NetOps deploy runbook

Public dashboard: `https://netops.dhiraagu.com.mv/coverage-eye/`  
Bot webhook: `https://netops.dhiraagu.com.mv/webhook` (not under `/coverage-eye/`)

## Interim (field replies now, no nginx /webhook yet)

Railway webhook may be down (502) while the dashboard is on NetOps. Run the **bot container with Telethon only** on the NetOps host (outbound internet only; no public URL):

```bash
./deploy/netops/start-interim-bot.sh
```

Expect `/health` → `"ingest_mode": "telethon_only"`, `"telethon_group_ingest": "on"`, and logs: `Telethon sidecar started`. Engineers’ swipe-replies in the field group flow to Supabase again.

When nginx `/webhook` is ready, switch to `docker-compose.netops.yml` and unset `BOT_TELETHON_ONLY`.

## 1. Environment on the server

On the NetOps host, in the repo directory:

```bash
cp deploy/netops/.env.netops.example .env
# Edit .env — fill TELEGRAM_*, TG_API_*, SUPABASE_*, TELEGRAM_WEBHOOK_SECRET
```

Required bot variables:

| Variable | Example |
|----------|---------|
| `WEBHOOK_BASE_URL` | `https://netops.dhiraagu.com.mv` |
| `TELEGRAM_WEBHOOK_SECRET` | (16–32 char secret) |
| `TELEGRAM_TOKEN` | BotFather token |
| `TELEGRAM_GROUP_CHAT_ID` | Field supergroup id |
| `TG_API_ID` / `TG_API_HASH` | https://my.telegram.org |

## 2. Nginx

Copy `deploy/netops/nginx-coverage-eye.conf` into your nginx config for `netops.dhiraagu.com.mv`, then:

```bash
sudo nginx -t && sudo systemctl reload nginx
```

## 3. Start dashboard + bot

```bash
chmod +x deploy/netops/up.sh
./deploy/netops/up.sh
```

Or manually:

```bash
docker compose -f docker-compose.yml -f docker-compose.netops.yml up -d --build
```

Both containers must be running:

- `coverage-eye-dashboard` → `127.0.0.1:8501`
- `coverage-eye-bot` → `127.0.0.1:8000`

## 4. Verify

From the server (or any machine with `.env`):

```bash
curl -sS https://netops.dhiraagu.com.mv/health | python3 -m json.tool
```

Expect:

```json
{
  "status": "ok",
  "webhook_url_configured": "yes",
  "telegram_callback_url": "https://netops.dhiraagu.com.mv/webhook",
  "telethon_group_ingest": "on"
}
```

Full check (health + webhook probe):

```bash
py -3 scripts/verify_netops_bot.py --origin https://netops.dhiraagu.com.mv
```

## 5. Bot logs (on the server)

```bash
docker logs coverage-eye-bot --tail 80
```

Look for:

- `Telegram set_webhook succeeded`
- `Telethon sidecar started: group ingest`

If Telethon is off, field swipe-replies will not reach Supabase.
