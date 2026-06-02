# AGENTS.md

## Cursor Cloud specific instructions

### Product

**NetOps Coverage Eye** (`TicketResponsesDashboard`): Python monolith with two dev processes — **FastAPI Telegram bot** (`bot.py`, port **8000**) and **Streamlit Command Center** (`app.py`, port **8501**). Both talk to a **hosted Supabase** project (no local Postgres in-repo). See `.env.example` for all variables.

### Python environment

- Pin: `.python-version` → **3.11**; Cloud VMs often ship **3.12**, which works with `requirements.txt`.
- Use the repo venv: `/workspace/.venv/bin/python` (created on first setup).
- If `python3 -m venv` fails with `ensurepip` missing, one-time: `sudo apt-get install -y python3.12-venv`.

### Local Windows folder (canonical dev copy)

Many operators keep the live project at:

`C:\Users\ibrahim_ali\Desktop\CSM\TELEBOT`

That folder should be the **same git repo** as `/workspace`. Sync **code** with `git pull` / `git push`. Sync **secrets and local-only files** separately (they are gitignored):

| Local (TELEBOT) | Cloud (`/workspace`) |
|-----------------|----------------------|
| `.env` | `.env` (via Cursor Secrets + `scripts/write_dotenv_from_environment.sh`) |
| `.streamlit/secrets.toml` | `.streamlit/secrets.toml` (optional upload) |
| `venv\` | `.venv\` (recreated by VM update script) |
| `telethon_*_session.session*` | optional; can regenerate |
| `logs\` | not needed on cloud |

On Windows, from the TELEBOT folder: `.\scripts\sync-local-telebot.ps1 -CheckOnly`  
After adding Cursor Secrets on cloud: `bash scripts/write_dotenv_from_environment.sh`

### Configuration

- Copy `.env.example` → `.env` and set at minimum: `SUPABASE_URL`, `SUPABASE_KEY`, `TELEGRAM_TOKEN`.
- `bot.py` **refuses to import** without Supabase + Telegram token; Streamlit **starts** without them but login/queues need Supabase.
- Verify Supabase: `/workspace/.venv/bin/python scripts/check_supabase_connection.py`
- Default seed users (after migration `20260520_dashboard_users.sql`): `admin` / `ibeyx`, password `ChangeMeNow!`

### Running services (manual — not in VM update script)

Start long-lived processes in **tmux** (see Cloud Agent shell rules), e.g.:

```bash
# Dashboard
tmux -f /exec-daemon/tmux.portal.conf new-session -d -s streamlit-dashboard -c /workspace \
  -- /workspace/.venv/bin/streamlit run app.py --server.headless true --server.port 8501 --server.address 0.0.0.0

# Bot (requires .env)
tmux -f /exec-daemon/tmux.portal.conf new-session -d -s ticket-bot -c /workspace \
  -- /workspace/.venv/bin/python -m uvicorn bot:app --host 0.0.0.0 --port 8000
```

- Bot health: `curl http://127.0.0.1:8000/health`
- Telegram webhooks need a **public HTTPS URL on port 8000** (not 8501); local dev uses ngrok on 8000 per `.env.example`.
- Do **not** set `TELEGRAM_DELETE_WEBHOOK_ON_SHUTDOWN=true` locally unless you intend to clear Telegram’s webhook.

### Lint / tests

- No project linter config or automated test suite in-repo (only `.pytest_cache/` in `.gitignore`).
- Smoke checks: import stack, `scripts/check_supabase_connection.py`, HTTP `GET /health` (bot), Streamlit on `:8501`.

### Optional Telethon

Set `TG_API_ID`, `TG_API_HASH`, `TELEGRAM_GROUP_CHAT_ID` for group privacy mode and delete-based UNDO; started inside the bot process, not a separate service.
