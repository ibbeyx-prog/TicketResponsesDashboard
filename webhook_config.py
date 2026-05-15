"""Derive the Telegram Bot API webhook HTTPS URL from process environment.

**Recommended on Railway:** enable a public domain for the bot service, then set
only ``RAILWAY_PUBLIC_DOMAIN`` (hostname like ``myservice.up.railway.app``) and
``TELEGRAM_WEBHOOK_SECRET``. This module builds ``https://<host>/webhook``.

**Optional overrides**

* ``WEBHOOK_BASE_URL`` — full site origin ``https://host`` (no ``/webhook``); we append ``/webhook``.
* ``WEBHOOK_FULL_URL`` — exact callback ``https://host/webhook`` (highest priority).

Accidental ``.../webhook/webhook`` suffixes on origins are normalized away.
"""

from __future__ import annotations

import os


def _collapse_double_webhook_path(raw: str) -> str:
    u = raw.strip().rstrip("/")
    while "/webhook/webhook" in u:
        u = u.replace("/webhook/webhook", "/webhook")
    return u


def _strip_trailing_webhook_from_origin(raw: str) -> str:
    """``https://host/webhook`` → ``https://host`` (repeat while suffix present)."""
    u = raw.strip().rstrip("/")
    suf = "/webhook"
    while u.lower().endswith(suf):
        u = u[: -len(suf)].rstrip("/")
    return u


def resolve_telegram_webhook_url() -> str | None:
    """Return the HTTPS URL Telegram should POST updates to, or ``None`` if unset."""
    full = (os.getenv("WEBHOOK_FULL_URL") or "").strip()
    if full:
        u = _collapse_double_webhook_path(full)
        return u or None

    base = (os.getenv("WEBHOOK_BASE_URL") or "").strip()
    if base:
        b = _strip_trailing_webhook_from_origin(base)
        if not b:
            return None
        return f"{b.rstrip('/')}/webhook"

    rail = (os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip()
    if rail:
        d = _strip_trailing_webhook_from_origin(rail)
        if not d.startswith(("http://", "https://")):
            d = f"https://{d}"
        return f"{d.rstrip('/')}/webhook"

    return None


def resolve_public_origin_for_probe() -> str | None:
    """Same host as the webhook without the ``/webhook`` path (for ``GET /health``)."""
    u = resolve_telegram_webhook_url()
    if not u:
        return None
    if u.lower().endswith("/webhook"):
        return u[: -len("/webhook")].rstrip("/") or None
    return u.rstrip("/")
