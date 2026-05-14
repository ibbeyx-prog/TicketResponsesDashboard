#!/usr/bin/env python3
"""Register Telegram webhook from ``.env`` (same URL rules as ``bot.py``).

Run from project root::

    py -3 restore_webhook.py
    .\\venv\\Scripts\\python.exe restore_webhook.py

Requires ``TELEGRAM_TOKEN``, ``TELEGRAM_WEBHOOK_SECRET``, and either
``WEBHOOK_BASE_URL`` or ``RAILWAY_PUBLIC_DOMAIN``. Starting ``bot.py`` with
uvicorn also calls ``set_webhook`` on startup — use this script if you only
want to point Telegram at your tunnel without restarting the whole bot.

Probe your public service (no ``setWebhook``)::

    py -3 restore_webhook.py --probe

This GETs ``/health`` and POSTs a minimal Telegram-shaped JSON to ``/webhook``
(with ``X-Telegram-Bot-Api-Secret-Token`` when ``TELEGRAM_WEBHOOK_SECRET`` is set),
then prints ``getWebhookInfo`` if ``TELEGRAM_TOKEN`` is set.
"""

from __future__ import annotations

import argparse
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


def _resolve_public_base() -> str | None:
    """Same host as webhook, without the ``/webhook`` path (for ``/health``)."""
    u = _resolve_webhook_url()
    if not u:
        return None
    if u.endswith("/webhook"):
        return u[: -len("/webhook")].rstrip("/") or None
    return u.rstrip("/")


def _cmd_probe() -> int:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", encoding="utf-8-sig")

    base = _resolve_public_base()
    secret = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    token = (os.getenv("TELEGRAM_TOKEN") or "").strip()

    if not base:
        print(
            "ERROR: Set WEBHOOK_BASE_URL or RAILWAY_PUBLIC_DOMAIN so the service URL can be built.",
            file=sys.stderr,
        )
        return 1

    with httpx.Client(timeout=30.0) as client:
        h = client.get(f"{base}/health")
        print(f"GET {base}/health → {h.status_code} {h.text[:500]}")

        payload = {
            "update_id": 999_999_001,
            "message": {
                "message_id": 1,
                "date": 0,
                "chat": {"id": 1, "type": "private"},
                "from": {"id": 1, "is_bot": False, "first_name": "WebhookProbe"},
                "text": "/start",
            },
        }
        headers = {"Content-Type": "application/json"}
        if secret:
            headers["X-Telegram-Bot-Api-Secret-Token"] = secret
        w = client.post(f"{base}/webhook", json=payload, headers=headers)
        print(f"POST {base}/webhook → {w.status_code} {w.text[:500]}")

    if token:
        api = f"https://api.telegram.org/bot{token}"
        with httpx.Client(timeout=30.0) as client:
            info = client.get(f"{api}/getWebhookInfo").json()
        result = info.get("result") or {}
        print("getWebhookInfo:")
        print(json.dumps(result, indent=2))
    else:
        print("(TELEGRAM_TOKEN unset — skipped getWebhookInfo)")

    return 0 if h.status_code == 200 and w.status_code == 200 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Register or probe Telegram webhook.")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="GET /health and POST a dummy update to /webhook (no setWebhook).",
    )
    args = parser.parse_args()
    if args.probe:
        return _cmd_probe()

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
