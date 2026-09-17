#!/usr/bin/env python3
"""Verify NetOps bot: public /health and optional webhook probe.

From project root with ``.env`` loaded::

    py -3 scripts/verify_netops_bot.py
    py -3 scripts/verify_netops_bot.py --origin https://netops.dhiraagu.com.mv

Exits 0 when ``telethon_group_ingest`` is ``on`` and webhook URL is configured.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from webhook_config import resolve_public_origin_for_probe, resolve_telegram_webhook_url


def _fail(msg: str) -> int:
    print(f"FAIL: {msg}", file=sys.stderr)
    return 1


def main() -> int:
    load_dotenv(_ROOT / ".env", encoding="utf-8-sig", override=True)
    parser = argparse.ArgumentParser(description="Verify NetOps bot health + webhook config")
    parser.add_argument(
        "--origin",
        default=None,
        help="Public site origin (default: from WEBHOOK_BASE_URL / env)",
    )
    parser.add_argument("--skip-probe", action="store_true", help="Only GET /health")
    args = parser.parse_args()

    origin = (args.origin or resolve_public_origin_for_probe() or "").strip().rstrip("/")
    if not origin:
        return _fail(
            "Set WEBHOOK_BASE_URL=https://netops.dhiraagu.com.mv in .env or pass --origin"
        )

    webhook_url = resolve_telegram_webhook_url()
    print(f"Configured Telegram callback: {webhook_url or '(none)'}")
    if webhook_url and "/coverage-eye" in webhook_url:
        return _fail("Webhook URL must not include /coverage-eye — fix WEBHOOK_BASE_URL")

    health_url = f"{origin}/health"
    print(f"GET {health_url}")
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            resp = client.get(health_url)
    except httpx.HTTPError as exc:
        return _fail(f"Could not reach {health_url}: {exc}")

    print(f"HTTP {resp.status_code}")
    try:
        body = resp.json()
        print(json.dumps(body, indent=2))
    except json.JSONDecodeError:
        print(resp.text[:500])
        return _fail("/health did not return JSON — check nginx routes to bot :8000")

    if resp.status_code != 200:
        return _fail(f"/health returned {resp.status_code}")

    if body.get("status") != "ok":
        return _fail(f"status={body.get('status')!r}")

    if body.get("webhook_url_configured") != "yes":
        return _fail("webhook_url_configured is not 'yes' — set WEBHOOK_BASE_URL on bot container")

    if body.get("telethon_group_ingest") != "on":
        return _fail(
            "telethon_group_ingest is not 'on' — set TG_API_ID, TG_API_HASH, TELEGRAM_GROUP_CHAT_ID"
        )

    if args.skip_probe:
        print("OK: health checks passed.")
        return 0

    print("\nRunning restore_webhook.py --probe (local POST + getWebhookInfo)...")
    import subprocess

    proc = subprocess.run(
        [sys.executable, str(_ROOT / "restore_webhook.py"), "--probe"],
        cwd=str(_ROOT),
        check=False,
    )
    if proc.returncode != 0:
        return _fail("restore_webhook.py --probe failed — see output above")
    print("OK: NetOps bot verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
