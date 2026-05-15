#!/usr/bin/env python3
"""Register Telegram webhook from ``.env`` (same URL rules as ``webhook_config`` / ``bot.py``).

Run from project root::

    py -3 restore_webhook.py
    .\\venv\\Scripts\\python.exe restore_webhook.py

Requires ``TELEGRAM_TOKEN``, ``TELEGRAM_WEBHOOK_SECRET``, and a webhook URL from
``RAILWAY_PUBLIC_DOMAIN`` (recommended on Railway), ``WEBHOOK_BASE_URL``, or
``WEBHOOK_FULL_URL``. Starting ``bot.py`` with uvicorn also calls ``set_webhook``
on startup — use this script if you want to re-point Telegram without restarting
the bot.

Probe your public service (no ``setWebhook``)::

    py -3 restore_webhook.py --probe

This GETs ``/health`` and POSTs a minimal Telegram-shaped JSON to ``/webhook``
(with ``X-Telegram-Bot-Api-Secret-Token`` when ``TELEGRAM_WEBHOOK_SECRET`` is set),
then prints ``getWebhookInfo`` if ``TELEGRAM_TOKEN`` is set.

Clear the webhook (Telegram stops POSTing to your server; only ``TELEGRAM_TOKEN`` required)::

    py -3 restore_webhook.py --delete

Optional: set ``TELEGRAM_DELETE_WEBHOOK_DROP_PENDING=1`` in ``.env`` to drop the pending queue when deleting.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

from webhook_config import resolve_public_origin_for_probe, resolve_telegram_webhook_url


def _cmd_probe() -> int:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", encoding="utf-8-sig", override=True)

    base = resolve_public_origin_for_probe()
    secret = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    token = (os.getenv("TELEGRAM_TOKEN") or "").strip()

    if not base:
        print(
            "ERROR: Set RAILWAY_PUBLIC_DOMAIN (Railway hostname), WEBHOOK_BASE_URL, "
            "or WEBHOOK_FULL_URL so the service URL can be built.",
            file=sys.stderr,
        )
        return 1

    if not secret:
        print(
            "WARNING: TELEGRAM_WEBHOOK_SECRET is empty after loading project .env; "
            "POST /webhook will be rejected (401).",
            file=sys.stderr,
        )

    with httpx.Client(timeout=30.0) as client:
        h = client.get(f"{base}/health")
        print(f"GET {base}/health -> {h.status_code} {h.text[:500]}")

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
        print(f"POST {base}/webhook -> {w.status_code} {w.text[:500]}")
        if w.status_code == 401:
            if secret:
                print(
                    f"Hint: probe sent X-Telegram-Bot-Api-Secret-Token using local .env secret "
                    f"(length {len(secret)}). If that matches Railway, redeploy the bot service "
                    "so the running process reloads TELEGRAM_WEBHOOK_SECRET, or remove stray "
                    "quotes/spaces in the Railway variable value.",
                    file=sys.stderr,
                )
            else:
                print(
                    "Hint: 401 with no secret sent — set TELEGRAM_WEBHOOK_SECRET in the project "
                    ".env next to restore_webhook.py (same value as Railway).",
                    file=sys.stderr,
                )

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


def _cmd_delete() -> int:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", encoding="utf-8-sig", override=True)

    token = (os.getenv("TELEGRAM_TOKEN") or "").strip()
    if not token:
        print("ERROR: TELEGRAM_TOKEN is not set.", file=sys.stderr)
        return 1

    api = f"https://api.telegram.org/bot{token}"
    drop = (os.getenv("TELEGRAM_DELETE_WEBHOOK_DROP_PENDING") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    payload: dict[str, bool] = {"drop_pending_updates": drop}
    with httpx.Client(timeout=30.0) as client:
        r = client.post(f"{api}/deleteWebhook", json=payload)
        data = r.json()

    if not data.get("ok"):
        print("ERROR: deleteWebhook failed:", json.dumps(data, indent=2), file=sys.stderr)
        return 1

    print("deleteWebhook OK (drop_pending_updates=%s)" % drop)
    with httpx.Client(timeout=30.0) as client:
        info = client.get(f"{api}/getWebhookInfo").json()
    print("getWebhookInfo:")
    print(json.dumps(info.get("result") or info, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Register or probe Telegram webhook.")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="GET /health and POST a dummy update to /webhook (no setWebhook).",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Call deleteWebhook (clears callback URL). Only TELEGRAM_TOKEN required.",
    )
    args = parser.parse_args()
    if args.probe:
        return _cmd_probe()
    if args.delete:
        return _cmd_delete()

    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", encoding="utf-8-sig", override=True)

    token = (os.getenv("TELEGRAM_TOKEN") or "").strip()
    secret = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    webhook_url = resolve_telegram_webhook_url()

    if not token:
        print("ERROR: TELEGRAM_TOKEN is not set.", file=sys.stderr)
        return 1
    if not webhook_url:
        print(
            "ERROR: Set RAILWAY_PUBLIC_DOMAIN (e.g. myservice.up.railway.app), "
            "WEBHOOK_BASE_URL, or WEBHOOK_FULL_URL so the webhook URL can be built.",
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
        "allowed_updates": [
            "message",
            "edited_message",
            "channel_post",
            "edited_channel_post",
        ],
    }
    with httpx.Client(timeout=30.0) as client:
        r = client.post(f"{api}/setWebhook", json=payload)
        data = r.json()

    if not data.get("ok"):
        print("ERROR: setWebhook failed:", json.dumps(data, indent=2), file=sys.stderr)
        return 1

    print("setWebhook OK ->", webhook_url)

    with httpx.Client(timeout=30.0) as client:
        info = client.get(f"{api}/getWebhookInfo").json()
    result = info.get("result") or {}
    print("getWebhookInfo:")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
