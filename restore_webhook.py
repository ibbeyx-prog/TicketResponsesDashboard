#!/usr/bin/env python3
"""Register Telegram webhook from ``.env`` (same URL rules as ``bot.py``).

Run from project root::

    .\\venv\\Scripts\\python.exe restore_webhook.py

Requires ``TELEGRAM_TOKEN``, ``TELEGRAM_WEBHOOK_SECRET``, and either
``WEBHOOK_BASE_URL`` or ``RAILWAY_PUBLIC_DOMAIN``. Starting ``bot.py`` with
uvicorn also calls ``set_webhook`` on startup — use this script if you only
want to point Telegram at your tunnel without restarting the whole bot.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv


def _resolve_webhook_url() -> str | None:
    base = (os.getenv("WEBHOOK_BASE_URL") or "").strip().rstrip("/")
    if base:
        return f"{base}/webhook"
    domain = (os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip()
    if not domain:
        return None
    if not domain.startswith(("http://", "https://")):
        domain = f"https://{domain}"
    return f"{domain.rstrip('/')}/webhook"


def main() -> int:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", encoding="utf-8-sig")

    token = (os.getenv("TELEGRAM_TOKEN") or "").strip()
    secret = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    webhook_url = _resolve_webhook_url()

    if not token:
        print("ERROR: TELEGRAM_TOKEN is not set.", file=sys.stderr)
        return 1
    if not webhook_url:
        print(
            "ERROR: Set WEBHOOK_BASE_URL (e.g. https://your-ngrok.ngrok-free.dev) "
            "or RAILWAY_PUBLIC_DOMAIN so the webhook URL can be built.",
            file=sys.stderr,
        )
        return 1
    if not secret:
        print(
            "ERROR: TELEGRAM_WEBHOOK_SECRET is required whenever a webhook URL is used.",
            file=sys.stderr,
        )
        return 1

    api = f"https://api.telegram.org/bot{token}"
    payload = {
        "url": webhook_url,
        "secret_token": secret,
        "drop_pending_updates": False,
    }
    with httpx.Client(timeout=30.0) as client:
        r = client.post(f"{api}/setWebhook", json=payload)
        data = r.json()

    if not data.get("ok"):
        print("ERROR: setWebhook failed:", json.dumps(data, indent=2), file=sys.stderr)
        return 1

    print("setWebhook OK →", webhook_url)

    with httpx.Client(timeout=30.0) as client:
        info = client.get(f"{api}/getWebhookInfo").json()
    result = info.get("result") or {}
    print("getWebhookInfo:")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
