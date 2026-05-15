"""
Outbound Telegram posts for the Streamlit Command Center (``notify_telegram_group``).

Two transports:

1. **Telethon** (bot user) — when ``TG_API_ID`` and ``TG_API_HASH`` are set.
   Also needs ``TG_BOT_TOKEN`` (or ``TELEGRAM_TOKEN``) and ``TG_GROUP_ID``
   (or ``TELEGRAM_GROUP_CHAT_ID``): numeric id or public ``@groupname``.

2. **python-telegram-bot** (HTTP Bot API) — when API id/hash are **not**
   configured; only bot token + group id are required.

Command Center assignments are **plain text** (no HTML): line 1 is
``@handle <category> <ticket_id>`` with normal spaces so the bot assignment
regex matches without any special separators.
"""

from __future__ import annotations

import os
from pathlib import Path

from telegram import Bot
from telethon import TelegramClient

# Session file lives next to this module (add ``*.session`` to ``.gitignore``).
_SESSION_BASE = Path(__file__).resolve().parent / "telethon_bot_session"


def _at_username(username: str) -> str:
    u = username.strip()
    return u if u.startswith("@") else f"@{u}"


def _build_assignment_notify_text(
    assigned_to: str,
    ticket_id: str,
    category: str,
    additional_info: str | None = None,
) -> str:
    """Plain-text assignment body for the field Telegram group (Command Center).

    One message only — line 1 must stay ``@user <Category> <ticket>`` for the bot.
    """
    handle = _at_username(assigned_to)
    line1 = f"{handle} {category} {ticket_id}"
    note = (additional_info or "").strip()
    parts: list[str] = [line1]
    if note:
        parts.append(note)
    parts.append("")
    parts.append("Field team: reply to THIS message with your update (text or photo).")
    return "\n".join(parts)


def _env_str(*names: str) -> str:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return ""


def normalize_telegram_group_id_paste(raw: str) -> str:
    """Strip whitespace, BOM/zero-width, and one layer of matching quotes (copy/paste)."""
    s = raw.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").strip()
    if len(s) >= 2:
        if (s[0] == s[-1] == '"') or (s[0] == s[-1] == "'"):
            s = s[1:-1].strip()
        elif s.startswith("\u201c") and s.endswith("\u201d"):
            s = s[1:-1].strip()
        elif s.startswith("\u2018") and s.endswith("\u2019"):
            s = s[1:-1].strip()
    return s.strip()


def _parse_group_entity(raw: str) -> int | str:
    s = normalize_telegram_group_id_paste(raw)
    if s.startswith("@"):
        return s
    return int(s)


def _coalesce_group_id(group_id: int | str | None) -> str:
    if group_id is not None:
        s = str(group_id).strip()
        if s:
            return s
    return _env_str(
        "TG_GROUP_ID",
        "TELEGRAM_GROUP_ID",
        "TELEGRAM_GROUP_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        "GROUP_CHAT_ID",
        "FIELD_GROUP_CHAT_ID",
    )


async def _send_via_ptb(
    *,
    bot_token: str,
    group_entity: int | str,
    text: str,
    parse_mode: str | None,
) -> None:
    async with Bot(bot_token) as bot:
        await bot.send_message(
            chat_id=group_entity,
            text=text,
            parse_mode=parse_mode,
        )


async def _send_via_telethon(
    *,
    api_id: int,
    api_hash: str,
    bot_token: str,
    group_entity: int | str,
    text: str,
    parse_mode: str | None,
) -> None:
    client = TelegramClient(str(_SESSION_BASE), api_id, api_hash)
    try:
        await client.start(bot_token=bot_token)
        await client.send_message(group_entity, text, parse_mode=parse_mode)
    finally:
        if client.is_connected():
            await client.disconnect()


async def notify_telegram_group(
    username: str,
    ticket_id: str,
    category: str,
    *,
    additional_info: str | None = None,
    api_id: str | int | None = None,
    api_hash: str | None = None,
    bot_token: str | None = None,
    group_id: int | str | None = None,
) -> None:
    """Notify the field Telegram group after a dashboard assignment upsert.

    Uses **Telethon** when ``TG_API_ID`` + ``TG_API_HASH`` are set; otherwise
    the HTTP Bot API (``python-telegram-bot``).

    Parameters override environment variables. Env fallbacks:

    - ``TG_API_ID`` / ``TELEGRAM_API_ID``
    - ``TG_API_HASH`` / ``TELEGRAM_API_HASH``
    - ``TG_BOT_TOKEN`` / ``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_TOKEN``
    - ``TG_GROUP_ID`` / ``TELEGRAM_GROUP_ID`` / ``TELEGRAM_GROUP_CHAT_ID``
      / ``TELEGRAM_CHAT_ID`` / ``GROUP_CHAT_ID``
    """
    token = (bot_token or "").strip() or _env_str("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
    group_raw = _coalesce_group_id(group_id)
    if not token or not group_raw:
        raise ValueError(
            "Missing bot token or group id. Set TG_BOT_TOKEN (or TELEGRAM_TOKEN) "
            "and TG_GROUP_ID (or TELEGRAM_GROUP_CHAT_ID), or pass them as arguments."
        )

    api_id_res = api_id
    if api_id_res is None or (isinstance(api_id_res, str) and not str(api_id_res).strip()):
        api_id_res = _env_str("TG_API_ID", "TELEGRAM_API_ID") or None
    api_hash_res = (api_hash or "").strip() or _env_str("TG_API_HASH", "TELEGRAM_API_HASH")

    text = _build_assignment_notify_text(
        username, ticket_id, category, additional_info=additional_info
    )
    entity = _parse_group_entity(group_raw)

    use_telethon = bool(
        api_id_res is not None
        and str(api_id_res).strip()
        and api_hash_res
    )

    if use_telethon:
        api_int = int(str(api_id_res).strip())
        await _send_via_telethon(
            api_id=api_int,
            api_hash=api_hash_res,
            bot_token=token,
            group_entity=entity,
            text=text,
            parse_mode=None,
        )
        return

    await _send_via_ptb(
        bot_token=token,
        group_entity=entity,
        text=text,
        parse_mode=None,
    )


# Backward-compatible name used by earlier commits.
send_telegram_assignment = notify_telegram_group
