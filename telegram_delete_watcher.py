"""Optional Telethon listener: revert dashboard when a field reply is deleted in Telegram.

The Bot API does not deliver message-deleted updates in groups. When
``TG_API_ID`` + ``TG_API_HASH`` are set, we keep a Telethon client connected
and call the undo callback on ``MessageDeleted`` events (best-effort).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger("telegram_delete_watcher")

_SESSION = Path(__file__).resolve().parent / "telethon_delete_listener_session"


def _env_str(*names: str) -> str:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return ""


async def start_delete_listener(
    undo_callback: Callable[[int, int], bool],
) -> Any | None:
    """Connect Telethon and watch deletions. Returns client or None if not configured."""
    api_raw = _env_str("TG_API_ID", "TELEGRAM_API_ID")
    api_hash = _env_str("TG_API_HASH", "TELEGRAM_API_HASH")
    token = _env_str("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
    if not api_raw or not api_hash or not token:
        log.info(
            "Telethon delete listener disabled (need TG_API_ID, TG_API_HASH, TELEGRAM_TOKEN)"
        )
        return None

    try:
        from telethon import TelegramClient, events
    except ImportError:
        log.warning("Telethon not installed; delete listener disabled")
        return None

    client = TelegramClient(str(_SESSION), int(api_raw), api_hash)
    await client.start(bot_token=token)

    @client.on(events.MessageDeleted)
    async def _on_deleted(event: events.MessageDeleted.Event) -> None:
        chat_id = event.chat_id
        if chat_id is None:
            return
        for mid in event.deleted_ids:
            try:
                undone = await asyncio.to_thread(undo_callback, int(chat_id), int(mid))
                if undone:
                    log.info(
                        "field response undone after Telegram delete chat=%s msg=%s",
                        chat_id,
                        mid,
                    )
            except Exception:
                log.exception(
                    "undo after Telegram delete failed chat=%s msg=%s", chat_id, mid
                )

    log.info("Telethon delete listener started (field reply undo window)")
    return client
