# NetOps deploy (Streamlit + Telegram bot)

**Runbook (copy-paste):** [`deploy/netops/README.md`](../deploy/netops/README.md)

Public dashboard: `https://netops.dhiraagu.com.mv/coverage-eye/`  
Field replies and unattended nudges: **separate bot process** (`bot.py` on port **8000**).

Quick start on the server:

```bash
cp deploy/netops/.env.netops.example .env   # fill secrets
# install deploy/netops/nginx-coverage-eye.conf into nginx, reload
./deploy/netops/up.sh
py -3 scripts/verify_netops_bot.py --origin https://netops.dhiraagu.com.mv
```

The Streamlit `[server]` block (`baseUrlPath`, `corsAllowedOrigins`) only fixes the **browser WebSocket** to the dashboard. It does **not** receive Telegram updates.

## What broke when only the dashboard was redeployed

Symptoms (example 2026-09-16):

- Dashboard **Assignment** rows appear in Supabase (`ticket_attendance_logs` action `Assignment`).
- Assignment posts appear in the field Telegram group (`assignment_telegram_message_id` set).
- **No** `Response` rows in `ticket_attendance_logs` when engineers swipe-reply.
- Last successful field capture may stop the day the **bot** service was down or misconfigured.

## Required production layout

| Traffic | Backend | Notes |
|--------|---------|--------|
| `GET/WS /coverage-eye/*` | Streamlit `:8501` | `baseUrlPath = "coverage-eye"` in `.streamlit/config.toml` |
| `POST /webhook` | Bot (uvicorn) `:8000` | Telegram Bot API callback — **root path**, not under `/coverage-eye/` |
| `GET /health` | Bot `:8000` | Liveness; check `telethon_group_ingest` |

Example nginx (conceptual):

```nginx
location /coverage-eye/ {
    proxy_pass http://127.0.0.1:8501/coverage-eye/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location = /webhook {
    proxy_pass http://127.0.0.1:8000/webhook;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location = /health {
    proxy_pass http://127.0.0.1:8000/health;
}
```

Do **not** set `WEBHOOK_BASE_URL` to `https://netops.dhiraagu.com.mv/coverage-eye` — that registers the wrong callback URL.

Use either:

- `WEBHOOK_BASE_URL=https://netops.dhiraagu.com.mv` → callback `https://netops.dhiraagu.com.mv/webhook`, or  
- `WEBHOOK_FULL_URL=https://netops.dhiraagu.com.mv/webhook`

Plus `TELEGRAM_WEBHOOK_SECRET` (same value on bot and in Telegram `setWebhook`).

## Telethon sidecar (required for swipe-replies)

Coordinators and engineers usually **do not @mention the bot**. With BotFather **privacy mode** on, the webhook alone does **not** see those group messages. The bot container must run the **Telethon sidecar** with:

- `TG_API_ID` / `TG_API_HASH`
- `TELEGRAM_TOKEN`
- `TELEGRAM_GROUP_CHAT_ID`

Persist `telethon_sidecar_session` (Docker volume) so redeploys do not silently disable ingest.

After deploy, check bot logs for:

- `Telegram set_webhook succeeded`
- `Telethon sidecar started: group ingest`

Or from a machine with `.env`:

```bash
py -3 restore_webhook.py --probe
curl -sS https://netops.dhiraagu.com.mv/health
```

Expect `"telethon_group_ingest": "on"` and `"status": "ok"`.

## Dashboard vs bot env

Both containers need Supabase vars. **Only the bot container** needs webhook + Telethon vars. The dashboard container needs Telegram vars only if you post assignments from the UI (`notify_telegram_group`).

## Local diagnostic

```bash
py -3 diagnose_field_reply.py
```

If assignments exist but `Response` logs stop after a deploy date, fix the **bot** service and Telethon session first — not Streamlit CORS.
