"""Telethon sidecar: read **all** field-group messages (privacy-mode safe).

The Bot API webhook only receives group messages when @BotFather **privacy mode**
is OFF (or the bot is @mentioned). Coordinators post ``@Dissiby Category ticket``
without mentioning the bot, and engineers swipe-reply to those lines — so we
subscribe with Telethon (``TG_API_ID`` + ``TG_API_HASH`` + bot token) and feed
the same PTB handlers as the webhook.

Also handles ``MessageDeleted`` for field-reply undo (replaces delete-only watcher).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

log = logging.getLogger("telethon_sidecar")

_SESSION = Path(__file__).resolve().parent / "telethon_sidecar_session"


def _env_str(*names: str) -> str:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return ""


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _field_group_entity() -> int | str | None:
    raw = _env_str(
        "TELEGRAM_GROUP_CHAT_ID",
        "TG_GROUP_ID",
        "TELEGRAM_GROUP_ID",
        "TELEGRAM_CHAT_ID",
    )
    if not raw:
        return None
    if raw.startswith("@"):
        return raw
    try:
        return int(raw)
    except ValueError:
        return raw


def _user_dict(user: object | None) -> dict[str, object] | None:
    if user is None:
        return None
    out: dict[str, object] = {
        "id": int(getattr(user, "id", 0)),
        "is_bot": bool(getattr(user, "bot", False)),
    }
    if getattr(user, "username", None):
        out["username"] = str(user.username)
    if getattr(user, "first_name", None):
        out["first_name"] = str(user.first_name)
    if getattr(user, "last_name", None):
        out["last_name"] = str(user.last_name)
    return out


def _chat_dict(chat: object, chat_id: int) -> dict[str, object]:
    out: dict[str, object] = {"id": int(chat_id)}
    if getattr(chat, "broadcast", False) and not getattr(chat, "megagroup", False):
        out["type"] = "channel"
    elif getattr(chat, "megagroup", False) or getattr(chat, "gigagroup", False):
        out["type"] = "supergroup"
    else:
        out["type"] = "group"
    title = getattr(chat, "title", None)
    if title:
        out["title"] = str(title)
    return out


async def _telethon_plain_text(msg: object) -> str:
    """Expand Telethon mention entities so assignment regex sees ``@username``."""
    text = (getattr(msg, "message", None) or "").strip()
    entities = getattr(msg, "entities", None) or []
    if not text or not entities:
        return text
    try:
        from telethon.tl.types import MessageEntityMentionName
    except ImportError:
        return text
    client = getattr(msg, "_client", None) or getattr(msg, "client", None)
    for ent in sorted(entities, key=lambda e: e.offset, reverse=True):
        if not isinstance(ent, MessageEntityMentionName):
            continue
        user = getattr(ent, "user", None)
        if user is None and getattr(ent, "user_id", None) and client is not None:
            try:
                user = await client.get_entity(int(ent.user_id))
            except Exception:
                user = None
        username = getattr(user, "username", None) if user else None
        if not username:
            continue
        start, end = int(ent.offset), int(ent.offset + ent.length)
        text = text[:start] + f"@{username}" + text[end:]
    return text


async def _telethon_message_dict(
    msg: object,
    *,
    chat: object,
    chat_id: int,
) -> dict[str, object] | None:
    """Build a Bot-API-shaped message object from a Telethon message."""
    from telethon.tl.types import Message as TLMessage

    if not isinstance(msg, TLMessage):
        return None
    text = await _telethon_plain_text(msg)
    has_media = bool(getattr(msg, "photo", None) or getattr(msg, "document", None))
    if not text and not has_media:
        return None

    sender = await msg.get_sender()
    body: dict[str, object] = {
        "message_id": int(msg.id),
        "date": int(msg.date.timestamp()),
        "chat": _chat_dict(chat, chat_id),
    }
    user = _user_dict(sender)
    if user:
        body["from"] = user
    if text:
        body["text"] = text
    if getattr(msg, "edit_date", None):
        body["edit_date"] = int(msg.edit_date.timestamp())

    reply = getattr(msg, "reply_to", None)
    reply_id = getattr(reply, "reply_to_msg_id", None) if reply else None
    if reply_id:
        parent = await msg.get_reply_message()
        if parent:
            parent_sender = await parent.get_sender()
            parent_text = await _telethon_plain_text(parent)
            parent_body: dict[str, object] = {
                "message_id": int(parent.id),
                "date": int(parent.date.timestamp()),
                "chat": _chat_dict(chat, chat_id),
            }
            pu = _user_dict(parent_sender)
            if pu:
                parent_body["from"] = pu
            if parent_text:
                parent_body["text"] = parent_text
            body["reply_to_message"] = parent_body

    return body


async def _event_to_update_dict(event: object) -> dict[str, object] | None:
    msg = getattr(event, "message", None)
    if msg is None:
        return None
    chat = await event.get_chat()
    chat_id = int(getattr(event, "chat_id", 0) or 0)
    if not chat_id:
        return None
    message_dict = await _telethon_message_dict(msg, chat=chat, chat_id=chat_id)
    if not message_dict:
        return None
    update_id = int(time.time() * 1_000_000) % 2_000_000_000
    if getattr(msg, "edit_date", None):
        return {"update_id": update_id, "edited_message": message_dict}
    return {"update_id": update_id, "message": message_dict}


async def start_telethon_sidecar(
    *,
    undo_callback: Callable[[int, int], bool],
    on_update_dict: Callable[[dict[str, object]], Awaitable[None]],
    on_media_field_reply: Callable[[object], Awaitable[bool]] | None = None,
) -> Any | None:
    """One Telethon client: group ingest + delete undo. Returns client or None."""
    if not _truthy_env("TELEGRAM_GROUP_TELETHON_INGEST", default=True):
        log.info("Telethon group ingest disabled (TELEGRAM_GROUP_TELETHON_INGEST=false)")
        return None

    api_raw = _env_str("TG_API_ID", "TELEGRAM_API_ID")
    api_hash = _env_str("TG_API_HASH", "TELEGRAM_API_HASH")
    token = _env_str("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
    group_entity = _field_group_entity()
    if not api_raw or not api_hash or not token:
        log.info(
            "Telethon sidecar disabled (need TG_API_ID, TG_API_HASH, TELEGRAM_TOKEN)"
        )
        return None
    if group_entity is None:
        log.warning(
            "Telethon sidecar disabled: TELEGRAM_GROUP_CHAT_ID not set — "
            "bot cannot read the field group when privacy mode is on"
        )
        return None

    try:
        from telethon import TelegramClient, events
    except ImportError:
        log.warning("Telethon not installed; group ingest disabled")
        return None

    client = TelegramClient(str(_SESSION), int(api_raw), api_hash)
    await client.start(bot_token=token)

    @client.on(events.NewMessage(chats=group_entity))
    async def _on_new_message(event: events.NewMessage.Event) -> None:
        await _dispatch_event(event, on_update_dict, on_media_field_reply)

    @client.on(events.MessageEdited(chats=group_entity))
    async def _on_edited_message(event: events.MessageEdited.Event) -> None:
        await _dispatch_event(event, on_update_dict, on_media_field_reply)

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

    log.info(
        "Telethon sidecar started: group ingest + delete undo (chat=%s)",
        str(group_entity)[:40],
    )
    return client


async def _dispatch_event(
    event: object,
    on_update_dict: Callable[[dict[str, object]], Awaitable[None]],
    on_media_field_reply: Callable[[object], Awaitable[bool]] | None = None,
) -> None:
    try:
        if on_media_field_reply is not None:
            try:
                if await on_media_field_reply(event):
                    return
            except Exception:
                log.exception("Telethon media field-reply handler failed")
        data = await _event_to_update_dict(event)
        if not data:
            return
        await on_update_dict(data)
    except Exception:
        log.exception("Telethon sidecar failed to dispatch group message")
