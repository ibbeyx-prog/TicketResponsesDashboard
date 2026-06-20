#!/usr/bin/env python3
"""One-time backfill: scan a field-group assignment thread and save missed photos.

Sales cases (``dashboard_sales_cases``) were not captured by the bot before the
sales field-reply fix. This script uses **Telethon** to:

1. Find the assignment message for a ``case_ref`` (e.g. ``2026041910000131``).
2. Walk swipe-replies on that message (photos / image documents).
3. Upload images to Supabase Storage and append ``Response`` attendance rows.

**Session:** Telegram **bot** accounts usually **cannot** read full group history
or other members' old replies. Use a **user** Telethon session (group member):

    # .env — same api id/hash as the dashboard; your phone for one-time login
    TG_API_ID=...
    TG_API_HASH=...
    TG_PHONE=+960...
    TELEGRAM_GROUP_CHAT_ID=-100...

    py -3 scripts/backfill_sales_case_photos_from_telegram.py 2026041910000131
    py -3 scripts/backfill_sales_case_photos_from_telegram.py 2026041910000131 --apply

On first run Telethon prompts for the login code (and 2FA if enabled). Session
is stored as ``telethon_backfill_session.session`` in the repo root.

Optional: pass ``--assignment-msg-id`` if you already know the parent message id.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("backfill_sales_photos")

_SESSION_USER = _ROOT / "telethon_backfill_session"
_SESSION_BOT = _ROOT / "telethon_bot_session"
_TICKET_ID_RE = re.compile(r"\b(\d{16}|\d{9})\b")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or ""
SALES_CASES_TABLE = (
    os.getenv("SALES_CASES_TABLE") or "dashboard_sales_cases"
).strip()
ATTENDANCE_LOGS_TABLE = (
    os.getenv("ATTENDANCE_LOGS_TABLE") or "ticket_attendance_logs"
).strip()
TICKET_PHOTOS_BUCKET = (
    os.getenv("TICKET_PHOTOS_BUCKET") or "ticket-photos"
).strip()

_supabase_client: Any = None


def _supabase() -> Any:
    global _supabase_client
    if _supabase_client is None:
        from supabase import create_client

        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase_client


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _assignee_handle(raw: object) -> str:
    assignee = str(raw or "").strip()
    if not assignee:
        return "@unknown"
    return assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"


def _field_responded_by_value(*, assigned_to: object, replier_label: str) -> str | None:
    assignee = str(assigned_to or "").strip().lstrip("@").casefold()
    replier = str(replier_label or "").strip().lstrip("@").casefold()
    if not replier or replier == assignee:
        return None
    handle = str(replier_label or "").strip().lstrip("@")
    return f"@{handle}" if handle else None


def _public_storage_object_url(bucket: str, object_path: str) -> str:
    raw = _supabase().storage.from_(bucket).get_public_url(object_path)
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict):
        url = raw.get("publicUrl") or raw.get("publicURL")
        if url:
            return str(url)
    safe = object_path.lstrip("/")
    return f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{safe}"


def _storage_upload_ticket_photo(
    ticket_number: str, image_bytes: bytes, content_type: str
) -> str:
    ct = content_type.lower()
    if "png" in ct:
        suffix, mime = "png", "image/png"
    elif "webp" in ct:
        suffix, mime = "webp", "image/webp"
    else:
        suffix, mime = "jpg", "image/jpeg"
    object_path = f"{ticket_number}/{uuid.uuid4().hex}.{suffix}"
    _supabase().storage.from_(TICKET_PHOTOS_BUCKET).upload(
        path=object_path,
        file=image_bytes,
        file_options={"content-type": mime, "upsert": "true"},
    )
    return _public_storage_object_url(TICKET_PHOTOS_BUCKET, object_path)


def _insert_attendance_log(
    *,
    ticket_number: str,
    member_username: str,
    note: str | None,
    photo_url: str | None,
    telegram_chat_id: int,
    telegram_message_id: int,
) -> None:
    row = {
        "ticket_number": ticket_number,
        "member_username": member_username,
        "action_type": "Response",
        "note": note,
        "photo_url": photo_url,
        "timestamp": _utc_now_iso(),
        "telegram_chat_id": int(telegram_chat_id),
        "telegram_message_id": int(telegram_message_id),
    }
    _supabase().table(ATTENDANCE_LOGS_TABLE).insert(row).execute()


def _get_sales_case_by_ref(case_ref: str) -> dict | None:
    res = (
        _supabase()
        .table(SALES_CASES_TABLE)
        .select("case_ref, assigned_to, status, description, additional_info")
        .eq("case_ref", case_ref)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def _env_str(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _parse_group_entity(raw: str) -> int | str:
    from bot_utils import normalize_telegram_group_id_paste

    s = normalize_telegram_group_id_paste(raw)
    if s.startswith("@"):
        return s
    return int(s)


def _message_has_image(msg: object) -> bool:
    if getattr(msg, "photo", None):
        return True
    doc = getattr(msg, "document", None)
    if doc is None:
        return False
    mime = (getattr(doc, "mime_type", None) or "").strip().lower()
    return mime.startswith("image/")


def _media_content_type(msg: object) -> str:
    if getattr(msg, "photo", None):
        return "image/jpeg"
    doc = getattr(msg, "document", None)
    if doc is not None:
        mime = (getattr(doc, "mime_type", None) or "").strip()
        if mime.startswith("image/"):
            return mime
    return "image/jpeg"


async def _plain_text(msg: object) -> str:
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


def _existing_response_keys(case_ref: str) -> set[tuple[int, int]]:
    try:
        res = (
            _supabase()
            .table(ATTENDANCE_LOGS_TABLE)
            .select("telegram_chat_id, telegram_message_id, photo_url")
            .eq("ticket_number", case_ref)
            .eq("action_type", "Response")
            .execute()
        )
    except Exception:
        log.exception("Could not load existing attendance logs for %s", case_ref)
        return set()
    out: set[tuple[int, int]] = set()
    for row in res.data or []:
        chat_id = row.get("telegram_chat_id")
        msg_id = row.get("telegram_message_id")
        if chat_id is not None and msg_id is not None:
            out.add((int(chat_id), int(msg_id)))
    return out


async def _connect_client(*, use_bot: bool) -> Any:
    from telethon import TelegramClient

    api_raw = _env_str("TG_API_ID", "TELEGRAM_API_ID")
    api_hash = _env_str("TG_API_HASH", "TELEGRAM_API_HASH")
    if not api_raw or not api_hash:
        raise SystemExit("Set TG_API_ID and TG_API_HASH in .env")

    api_id = int(api_raw)
    if use_bot:
        token = _env_str("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
        if not token:
            raise SystemExit("Bot session requires TELEGRAM_TOKEN (or TG_BOT_TOKEN)")
        client = TelegramClient(str(_SESSION_BOT), api_id, api_hash)
        await client.start(bot_token=token)
        log.info("Connected with bot Telethon session (%s)", _SESSION_BOT.name)
        return client

    phone = _env_str("TG_PHONE", "TELEGRAM_PHONE")
    client = TelegramClient(str(_SESSION_USER), api_id, api_hash)
    if phone:
        await client.start(phone=phone)
    else:
        log.info(
            "No TG_PHONE in .env — Telethon will prompt for phone / code interactively."
        )
        await client.start()
    log.info("Connected with user Telethon session (%s)", _SESSION_USER.name)
    return client


async def _find_assignment_message(
    client: Any,
    entity: int | str,
    case_ref: str,
    *,
    search_limit: int,
    assignee_hint: str | None,
    assignment_msg_id: int | None,
) -> Any | None:
    from telethon.tl.types import Message as TLMessage

    if assignment_msg_id is not None:
        msg = await client.get_messages(entity, ids=int(assignment_msg_id))
        if isinstance(msg, TLMessage):
            blob = (msg.text or msg.message or "").strip()
            if case_ref in blob:
                log.info(
                    "Using assignment message id=%s (explicit)",
                    assignment_msg_id,
                )
                return msg
            log.warning(
                "Message %s does not contain case_ref %s — searching anyway",
                assignment_msg_id,
                case_ref,
            )
            return msg

    assignee_key = (assignee_hint or "").strip().lstrip("@").casefold()
    candidates: list[tuple[int, Any]] = []

    async for message in client.iter_messages(entity, limit=search_limit):
        if not isinstance(message, TLMessage):
            continue
        blob = (message.text or message.message or "").strip()
        if case_ref not in blob:
            continue
        if not _TICKET_ID_RE.search(blob):
            continue
        score = 0
        if assignee_key and assignee_key in blob.casefold():
            score += 2
        if "assigned by" in blob.casefold():
            score += 1
        if blob.casefold().startswith("@"):
            score += 1
        candidates.append((score, message))

    if not candidates:
        return None

    candidates.sort(key=lambda pair: (pair[0], int(pair[1].id)), reverse=True)
    best_score, best = candidates[0]
    log.info(
        "Found assignment message id=%s score=%s preview=%r",
        best.id,
        best_score,
        ((best.text or best.message or "")[:120]),
    )
    if len(candidates) > 1:
        log.info(
            "(%s other messages also mention %s; picked highest score / newest)",
            len(candidates) - 1,
            case_ref,
        )
    return best


async def _iter_thread_replies(
    client: Any,
    entity: int | str,
    parent_id: int,
    *,
    reply_limit: int,
) -> list[Any]:
    from telethon.tl.types import Message as TLMessage

    replies: list[Any] = []
    async for msg in client.iter_messages(
        entity, reply_to=int(parent_id), limit=reply_limit
    ):
        if isinstance(msg, TLMessage):
            replies.append(msg)
    replies.sort(key=lambda m: int(m.id))
    return replies


async def _backfill_reply(
    *,
    case_ref: str,
    msg: object,
    assignee_handle: str,
    chat_id: int,
    apply: bool,
    seen_keys: set[tuple[int, int]],
) -> bool:
    if not _message_has_image(msg):
        return False

    message_id = int(getattr(msg, "id", 0) or 0)
    if not message_id:
        return False

    key = (int(chat_id), message_id)
    if key in seen_keys:
        log.info("Skip msg=%s — already in attendance log", message_id)
        return False

    sender = await msg.get_sender()
    username = getattr(sender, "username", None) if sender else None
    first = str(getattr(sender, "first_name", None) or "") if sender else ""
    last = str(getattr(sender, "last_name", None) or "") if sender else ""
    replier_label = (
        f"@{username}" if username else " ".join(p for p in (first, last) if p).strip()
    ) or "unknown"

    caption = (getattr(msg, "message", None) or "").strip()
    content_type = _media_content_type(msg)

    log.info(
        "Photo reply msg=%s from %s caption=%r apply=%s",
        message_id,
        replier_label,
        caption[:80] if caption else None,
        apply,
    )

    if not apply:
        return True

    client = getattr(msg, "_client", None) or getattr(msg, "client", None)
    if client is None:
        log.error("No Telethon client on message — cannot download media")
        return False

    try:
        raw = await client.download_media(msg, bytes)
        image_bytes = bytes(raw) if raw else b""
    except Exception:
        log.exception("Download failed for msg=%s", message_id)
        return False

    if not image_bytes:
        log.warning("Empty download for msg=%s", message_id)
        return False

    try:
        upload_url = _storage_upload_ticket_photo(
            case_ref, image_bytes, content_type
        )
    except Exception:
        log.exception("Storage upload failed for msg=%s", message_id)
        return False

    responded_by = _field_responded_by_value(
        assigned_to=assignee_handle,
        replier_label=replier_label.lstrip("@"),
    )
    note = caption or "(photo backfill)"
    if responded_by:
        note = (
            f"Responded by {responded_by}: {note}"
            if caption
            else f"Responded by {responded_by}"
        )

    _insert_attendance_log(
        ticket_number=case_ref,
        member_username=_assignee_handle(assignee_handle),
        note=note,
        photo_url=upload_url,
        telegram_chat_id=int(chat_id),
        telegram_message_id=message_id,
    )
    seen_keys.add(key)
    log.info("Saved msg=%s → %s", message_id, upload_url)
    return True


async def _run(args: argparse.Namespace) -> int:
    case_ref = str(args.case_ref).strip()
    if not case_ref:
        log.error("case_ref is required")
        return 1

    group_raw = _env_str(
        "TELEGRAM_GROUP_CHAT_ID",
        "TG_GROUP_ID",
        "TELEGRAM_GROUP_ID",
        "TELEGRAM_CHAT_ID",
    )
    if not group_raw:
        log.error("Set TELEGRAM_GROUP_CHAT_ID (or TG_GROUP_ID) in .env")
        return 1

    if not (SUPABASE_URL and SUPABASE_KEY):
        log.error("SUPABASE_URL and SUPABASE_KEY required in .env")
        return 1

    sales_row = _get_sales_case_by_ref(case_ref)
    if not sales_row:
        log.error("Sales case %s not found in dashboard_sales_cases", case_ref)
        return 2

    assignee = str(sales_row.get("assigned_to") or "").strip()
    if not assignee:
        log.error("Case %s has no assigned_to — cannot label backfill", case_ref)
        return 2

    log.info(
        "Case %s assignee=%s status=%s apply=%s",
        case_ref,
        assignee,
        sales_row.get("status"),
        args.apply,
    )

    entity = _parse_group_entity(group_raw)
    client = await _connect_client(use_bot=bool(args.bot_session))
    seen_keys = _existing_response_keys(case_ref)

    try:
        assignment = await _find_assignment_message(
            client,
            entity,
            case_ref,
            search_limit=int(args.search_limit),
            assignee_hint=assignee,
            assignment_msg_id=args.assignment_msg_id,
        )
        if assignment is None:
            log.error(
                "No assignment message found for %s in the last %s messages. "
                "Increase --search-limit, pass --assignment-msg-id, or use a "
                "**user** session (drop --bot-session).",
                case_ref,
                args.search_limit,
            )
            return 3

        chat_id = int(getattr(assignment, "chat_id", None) or entity)
        replies = await _iter_thread_replies(
            client,
            entity,
            int(assignment.id),
            reply_limit=int(args.reply_limit),
        )
        log.info(
            "Assignment msg=%s — %s direct replies in thread",
            assignment.id,
            len(replies),
        )

        photo_count = 0
        saved = 0
        for reply in replies:
            if not _message_has_image(reply):
                continue
            photo_count += 1
            if await _backfill_reply(
                case_ref=case_ref,
                msg=reply,
                assignee_handle=assignee,
                chat_id=chat_id,
                apply=bool(args.apply),
                seen_keys=seen_keys,
            ):
                if args.apply:
                    saved += 1
                else:
                    saved += 1  # would save

        if photo_count == 0:
            log.warning(
                "No photo replies on assignment msg=%s. "
                "Confirm the engineer swipe-replied to that message (not a standalone post).",
                assignment.id,
            )
            return 4

        if args.apply:
            log.info("Backfill complete: %s photo(s) saved for %s", saved, case_ref)
        else:
            log.info(
                "Dry run: %s photo reply(s) found. Re-run with --apply to upload.",
                photo_count,
            )
        return 0
    finally:
        if client.is_connected():
            await client.disconnect()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "case_ref",
        nargs="?",
        default="2026041910000131",
        help="Sales case_ref / ticket number (default: 2026041910000131)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Upload photos and write attendance logs (default: dry run)",
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=500,
        help="How many recent group messages to scan for the assignment line",
    )
    parser.add_argument(
        "--reply-limit",
        type=int,
        default=100,
        help="Max swipe-replies to read on the assignment message",
    )
    parser.add_argument(
        "--assignment-msg-id",
        type=int,
        default=None,
        help="Skip search and use this Telegram message id as the assignment parent",
    )
    parser.add_argument(
        "--bot-session",
        action="store_true",
        help="Use bot token session (often cannot read reply history — user session is preferred)",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
