"""
Telegram bot (webhook) + Supabase.

Database expectations
=====================

1) ``ticket_responses`` — append-only log of operator replies (used by /respond).

2) ``tickets`` — one row per ticket. Driven by the assignment-message flow
   (``@user <Category> <ticket_number>``). Recommended DDL:

       create table if not exists public.tickets (
         ticket_number text primary key,
         assigned_to text,
         task_category text check (task_category in (
           'Coverage Check',
           'Femto Installation',
           'Repeater Installation',
           'Femto Recover',
           'Femto Fault',
           'Repeater Fault'
         )),
         status text default 'Pending',
         field_response text,
         photo_url text,
         responded_at timestamptz,
         created_at timestamptz default now(),
         updated_at timestamptz default now()
       );

       alter table public.tickets enable row level security;
       -- Service-role key bypasses RLS; tighten policies if using anon key.

   Existing deployments: ``alter table public.tickets add column if not exists
   responded_at timestamptz;``

   Set ``TICKETS_TABLE=tickets`` (default) to point the bot at a different name.

   Field engineers complete a task by **replying** to an assignment message with
   plain text and/or a photo. Photos are stored in the Storage bucket
   ``ticket-photos`` (override with ``TICKET_PHOTOS_BUCKET``). Create the bucket
   in the Supabase dashboard (or SQL) and grant the service role upload/read as
   needed.

3) ``bot_sessions`` (optional) — durable /respond state across restarts:

       create table if not exists public.bot_sessions (
         telegram_user_id bigint primary key,
         chat_id bigint,
         active_ticket text,
         updated_at timestamptz default now()
       );

       alter table public.bot_sessions enable row level security;

   Set ``BOT_SESSIONS_TABLE=bot_sessions`` (default). If the table is missing,
   the bot falls back to in-memory ``user_data`` only.
"""

from __future__ import annotations

import hmac
import logging
import os
import re
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from supabase import create_client
from telegram import BotCommand, Message, Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ticket_bot")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PORT = int(os.getenv("PORT", "8000"))
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
BOT_SESSIONS_TABLE = (os.getenv("BOT_SESSIONS_TABLE") or "bot_sessions").strip()
TICKETS_TABLE = (os.getenv("TICKETS_TABLE") or "tickets").strip()
TICKET_PHOTOS_BUCKET = (os.getenv("TICKET_PHOTOS_BUCKET") or "ticket-photos").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY")
if not TELEGRAM_TOKEN:
    raise ValueError("Missing TELEGRAM_TOKEN")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# If False after first DB error, skip Supabase session reads/writes for this process.
_use_db_sessions = True

_EXTRA_ALLOWED_USERS: frozenset[str] = frozenset({"dissiby"})

# Reasonable bounds for a ticket identifier carried in a Telegram command.
_MAX_TICKET_ID_LEN = 128


def _normalize_username(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = name.strip().lstrip("@").lower()
    return cleaned if cleaned else None


def _effective_allowed_handles() -> frozenset[str] | None:
    raw = os.getenv("TELEGRAM_ALLOWED_USERNAMES")
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    parsed: set[str] = set()
    for part in trimmed.split(","):
        norm = _normalize_username(part)
        if norm:
            parsed.add(norm)
    return frozenset(parsed | set(_EXTRA_ALLOWED_USERS))


def _validate_ticket_id(raw: str) -> str | None:
    cleaned = raw.strip()
    if not cleaned or len(cleaned) > _MAX_TICKET_ID_LEN:
        return None
    # Disallow whitespace inside the identifier; ticket ids are single tokens.
    if any(ch.isspace() for ch in cleaned):
        return None
    return cleaned


# Pattern: "@user <Category> <ticket_number>" where ticket_number is 16 or 9
# digits. Used both by the message filter (filters.Regex) and by the handler to
# iterate over multiple assignments inside a single message.
_ASSIGNMENT_TASK_CATEGORIES: tuple[str, ...] = (
    "Coverage Check",
    "Femto Installation",
    "Repeater Installation",
    "Femto Recover",
    "Femto Fault",
    "Repeater Fault",
)

_ASSIGNMENT_PATTERN: re.Pattern[str] = re.compile(
    r"(@\w+)\s+("
    + "|".join(re.escape(cat) for cat in _ASSIGNMENT_TASK_CATEGORIES)
    + r")\s+(\d{16}|\d{9})"
)


class _ReplyToAssignmentFilter(filters.MessageFilter):
    """True when the message is a reply and the parent contains an assignment pattern."""

    def filter(self, message: Message) -> bool:
        parent = message.reply_to_message
        if not parent:
            return False
        blob = f"{parent.text or ''}\n{parent.caption or ''}"
        return bool(_ASSIGNMENT_PATTERN.search(blob))


def _is_sender_allowed(update: Update) -> bool:
    handles = _effective_allowed_handles()
    if handles is None:
        return True
    sender = update.effective_user.username if update.effective_user else None
    key = _normalize_username(sender)
    return bool(key and key in handles)


def _telegram_user_id(update: Update) -> int | None:
    user = update.effective_user
    return int(user.id) if user else None


def _chat_id(update: Update) -> int | None:
    chat = update.effective_chat
    return int(chat.id) if chat else None


def _disable_db_sessions(reason: str) -> None:
    global _use_db_sessions
    if _use_db_sessions:
        _use_db_sessions = False
        log.warning("Disabling Supabase session store: %s", reason)


def _db_get_active_ticket(user_id: int) -> str | None:
    if not _use_db_sessions:
        return None
    try:
        res = (
            supabase.table(BOT_SESSIONS_TABLE)
            .select("active_ticket")
            .eq("telegram_user_id", user_id)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return None
        ticket = rows[0].get("active_ticket")
        return str(ticket) if ticket is not None else None
    except Exception as exc:
        _disable_db_sessions(f"read bot_sessions failed: {exc}")
        return None


def _db_set_active_ticket(user_id: int, chat_id: int | None, ticket_id: str) -> None:
    if not _use_db_sessions:
        return
    row: dict[str, Any] = {
        "telegram_user_id": user_id,
        "active_ticket": ticket_id,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if chat_id is not None:
        row["chat_id"] = chat_id
    try:
        supabase.table(BOT_SESSIONS_TABLE).upsert(row).execute()
    except Exception as exc:
        _disable_db_sessions(f"upsert bot_sessions failed: {exc}")


def _db_clear_active_ticket(user_id: int) -> None:
    if not _use_db_sessions:
        return
    try:
        supabase.table(BOT_SESSIONS_TABLE).delete().eq("telegram_user_id", user_id).execute()
    except Exception as exc:
        _disable_db_sessions(f"delete bot_sessions failed: {exc}")


def _db_get_ticket(ticket_number: str) -> dict[str, Any] | None:
    res = (
        supabase.table(TICKETS_TABLE)
        .select("ticket_number, assigned_to, task_category, status")
        .eq("ticket_number", ticket_number)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def _sender_matches_assigned_to(assigned_to_db: object, replier_username: str | None) -> bool:
    if not replier_username:
        return False
    db_key = _normalize_username(str(assigned_to_db)) if assigned_to_db is not None else None
    return bool(db_key and db_key == _normalize_username(replier_username))


def _resolve_ticket_from_assignment_reply(parent_blob: str, replier_username: str | None) -> str | None:
    """Pick the ticket_number from the parent assignment message for this replier."""
    matches = list(_ASSIGNMENT_PATTERN.finditer(parent_blob))
    if not matches:
        return None
    if not replier_username:
        return None
    replier_key = _normalize_username(replier_username)
    if not replier_key:
        return None
    for m in matches:
        ticket_number = m.group(3)
        try:
            row = _db_get_ticket(ticket_number)
        except Exception:
            log.exception("tickets lookup failed during field-reply resolution: %s", ticket_number)
            continue
        if row and _sender_matches_assigned_to(row.get("assigned_to"), replier_username):
            return ticket_number
    if len(matches) == 1:
        m = matches[0]
        if _normalize_username(m.group(1)) == replier_key:
            return m.group(3)
    return None


def _public_storage_object_url(bucket: str, object_path: str) -> str:
    raw = supabase.storage.from_(bucket).get_public_url(object_path)
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict):
        url = raw.get("publicUrl") or raw.get("publicURL")
        if url:
            return str(url)
    url_attr = getattr(raw, "public_url", None) or getattr(raw, "publicUrl", None)
    if url_attr:
        return str(url_attr)
    safe = object_path.lstrip("/")
    return f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{safe}"


def _storage_upload_ticket_photo(ticket_number: str, image_bytes: bytes, content_type: str) -> str:
    ct = content_type.lower()
    if "png" in ct:
        suffix = "png"
        mime = "image/png"
    elif "webp" in ct:
        suffix = "webp"
        mime = "image/webp"
    else:
        suffix = "jpg"
        mime = "image/jpeg"
    object_path = f"{ticket_number}/{uuid.uuid4().hex}.{suffix}"
    file_opts: dict[str, str] = {"content-type": mime, "upsert": "true"}
    supabase.storage.from_(TICKET_PHOTOS_BUCKET).upload(
        path=object_path,
        file=image_bytes,
        file_options=file_opts,
    )
    return _public_storage_object_url(TICKET_PHOTOS_BUCKET, object_path)


def _db_complete_ticket_field_response(
    ticket_number: str,
    *,
    field_response: str | None,
    photo_url: str | None = None,
    update_photo_url: bool = False,
) -> None:
    responded_at = datetime.now(timezone.utc).isoformat()
    updates: dict[str, Any] = {
        "status": "Completed",
        "responded_at": responded_at,
        "field_response": field_response,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if update_photo_url:
        updates["photo_url"] = photo_url
    supabase.table(TICKETS_TABLE).update(updates).eq("ticket_number", ticket_number).execute()


def _db_insert_assignment(ticket_number: str, assigned_to: str, task_category: str) -> None:
    row = {
        "ticket_number": ticket_number,
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
    }
    supabase.table(TICKETS_TABLE).insert(row).execute()


def _db_reassign_ticket(ticket_number: str, assigned_to: str, task_category: str) -> None:
    """Overwrite assigned_to / task_category and reset prior work for a re-assignment.

    Resets the task fully for the new assignee: ``status`` goes back to
    ``"Pending"`` and the previous ``field_response`` / ``photo_url`` are
    nullified, regardless of what the ticket looked like before.
    """
    updates = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    supabase.table(TICKETS_TABLE).update(updates).eq("ticket_number", ticket_number).execute()


async def _get_active_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str | None:
    uid = _telegram_user_id(update)
    if uid is None:
        return None
    db_ticket = _db_get_active_ticket(uid)
    if db_ticket:
        context.user_data["active_ticket"] = db_ticket
        return db_ticket
    return context.user_data.get("active_ticket")


async def _set_active_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE, ticket_id: str) -> None:
    context.user_data["active_ticket"] = ticket_id
    uid = _telegram_user_id(update)
    if uid is not None:
        _db_set_active_ticket(uid, _chat_id(update), ticket_id)


async def _clear_active_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("active_ticket", None)
    uid = _telegram_user_id(update)
    if uid is not None:
        _db_clear_active_ticket(uid)


async def _reply_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("This chat is not available.")


_HELP_TEXT = (
    "Ticket response bot\n"
    "\n"
    "Field work (assignee):\n"
    "  Reply to your assignment message with text and/or a photo.\n"
    "  Text → saved as field_response; photo → uploaded to ticket-photos; ticket → Completed.\n"
    "\n"
    "Operator /respond workflow:\n"
    "  1) /respond <ticket_id> — pick the ticket you want to reply to\n"
    "  2) Send a single text message — it is saved as your response\n"
    "  3) The active ticket is cleared automatically after a successful save\n"
    "\n"
    "Commands:\n"
    "  /respond <ticket_id> — start a reply for a ticket\n"
    "  /active — show the ticket you are currently replying to\n"
    "  /cancel — clear the active ticket without saving\n"
    "  /help — show this message"
)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if update.message:
        await update.message.reply_text(_HELP_TEXT)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if update.message:
        await update.message.reply_text(_HELP_TEXT)


async def active_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return
    ticket_id = await _get_active_ticket(update, context)
    if ticket_id:
        await update.message.reply_text(
            f"Active ticket: {ticket_id}\nSend a text message to save your response, or /cancel."
        )
    else:
        await update.message.reply_text("No active ticket. Start with /respond <ticket_id>.")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    had_ticket = await _get_active_ticket(update, context)
    await _clear_active_ticket(update, context)
    if update.message:
        if had_ticket:
            await update.message.reply_text(f"Cleared active ticket: {had_ticket}")
        else:
            await update.message.reply_text("No active ticket to clear.")


async def respond_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return
    if not context.args:
        await update.message.reply_text("Usage: /respond <ticket_id>")
        return

    ticket_id = _validate_ticket_id(context.args[0])
    if not ticket_id:
        await update.message.reply_text(
            "Invalid ticket id. It must be a single non-empty token "
            f"(max {_MAX_TICKET_ID_LEN} chars, no whitespace)."
        )
        return

    await _set_active_ticket(update, context, ticket_id)
    await update.message.reply_text(
        f"Active ticket set: {ticket_id}\n"
        "Send a text message to save your response, or /cancel."
    )


async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return

    ticket_id = await _get_active_ticket(update, context)
    if not ticket_id:
        await update.message.reply_text("Start with /respond <ticket_id>.")
        return

    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("Empty message — send some text to save as the response.")
        return

    username = update.effective_user.username if update.effective_user else None
    user_handle = f"@{username}" if username else "unknown_user"

    try:
        await context.bot.send_chat_action(chat_id=update.message.chat_id, action=ChatAction.TYPING)
    except Exception:
        # Non-fatal — typing indicator is purely cosmetic.
        pass

    payload = {
        "ticket_id": ticket_id,
        "user_handle": user_handle,
        "response_data": text,
    }
    try:
        supabase.table("ticket_responses").insert(payload).execute()
    except Exception as exc:
        log.exception("Supabase insert failed: %s", exc)
        await update.message.reply_text(
            f"Could not save response for ticket {ticket_id}. "
            "It is still active — try again, or /cancel to abort."
        )
        return

    await _clear_active_ticket(update, context)
    await update.message.reply_text(f"Saved response for ticket {ticket_id}.")


async def handle_field_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Assignee completes a task by replying to the assignment message (text and/or photo).

    Not gated by ``TELEGRAM_ALLOWED_USERNAMES`` — the replier must match ``assigned_to``
    on the ``tickets`` row (Telegram ``@username``, case-insensitive).
    """
    msg = update.message
    if not msg or not msg.reply_to_message:
        return

    parent = msg.reply_to_message
    parent_blob = f"{parent.text or ''}\n{parent.caption or ''}"
    username = update.effective_user.username if update.effective_user else None

    ticket_number = _resolve_ticket_from_assignment_reply(parent_blob, username)
    if not ticket_number:
        if not username:
            await msg.reply_text(
                "Could not match this reply to a ticket. "
                "Set a Telegram username so it matches assigned_to on the ticket."
            )
        else:
            await msg.reply_text(
                "Could not match this reply to a ticket for your username. "
                "Reply to the assignment message that names your @handle."
            )
        return

    try:
        row = _db_get_ticket(ticket_number)
    except Exception:
        log.exception("tickets lookup failed for field reply: %s", ticket_number)
        await msg.reply_text(f"Database error while loading ticket {ticket_number}.")
        return

    if not row:
        await msg.reply_text(f"No ticket record found for {ticket_number}.")
        return

    if not _sender_matches_assigned_to(row.get("assigned_to"), username):
        await msg.reply_text("You are not the assignee for that ticket.")
        return

    has_photo = bool(msg.photo)
    caption_or_text = (msg.caption or msg.text or "").strip() or None

    if not has_photo and not caption_or_text:
        await msg.reply_text(
            "Send a text message or a photo (optional caption) to complete this task."
        )
        return

    if has_photo:
        largest = msg.photo[-1]
        try:
            await context.bot.send_chat_action(
                chat_id=msg.chat_id,
                action=ChatAction.UPLOAD_PHOTO,
            )
        except Exception:
            pass
        try:
            tg_file = await context.bot.get_file(largest.file_id)
            raw = await tg_file.download_as_bytearray()
            image_bytes = bytes(raw)
            upload_url = _storage_upload_ticket_photo(ticket_number, image_bytes, "image/jpeg")
        except Exception:
            log.exception("photo download or storage upload failed")
            await msg.reply_text("Could not upload the photo. Please try again.")
            return
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                photo_url=upload_url,
                update_photo_url=True,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await msg.reply_text(
                f"Photo uploaded but ticket {ticket_number} could not be updated in the database."
            )
            return
        await msg.reply_text(f"Ticket {ticket_number} marked Completed (photo saved).")
    else:
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                update_photo_url=False,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await msg.reply_text(f"Could not update ticket {ticket_number}.")
            return
        await msg.reply_text(f"Ticket {ticket_number} marked Completed.")

    if await _get_active_ticket(update, context) == ticket_number:
        await _clear_active_ticket(update, context)


async def handle_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detect ``@user <Category> <ticket_number>`` patterns and upsert tickets.

    A single message may contain multiple assignments; each match is processed
    independently and the results are reported back as a single reply.
    """
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message or not update.message.text:
        return

    matches = list(_ASSIGNMENT_PATTERN.finditer(update.message.text))
    if not matches:
        return  # Filter shouldn't have triggered, but be defensive.

    lines: list[str] = []
    for m in matches:
        assigned_to = m.group(1)        # e.g. "@john"
        task_category = m.group(2)      # e.g. "Femto Installation"
        ticket_number = m.group(3)      # 16- or 9-digit string

        try:
            existing = _db_get_ticket(ticket_number)
        except Exception as exc:
            log.exception("tickets lookup failed for %s: %s", ticket_number, exc)
            lines.append(f"• Lookup failed for ticket {ticket_number}.")
            continue

        try:
            if existing is None:
                _db_insert_assignment(ticket_number, assigned_to, task_category)
                lines.append(
                    f"• Assigned ticket {ticket_number} ({task_category}) to {assigned_to}."
                )
            else:
                _db_reassign_ticket(ticket_number, assigned_to, task_category)
                prev_assignee = existing.get("assigned_to") or "—"
                prev_status = existing.get("status") or "—"
                lines.append(
                    f"• Re-assigned ticket {ticket_number} ({task_category}) "
                    f"from {prev_assignee} to {assigned_to}. "
                    f"Status reset to Pending (was {prev_status}); "
                    "previous response and photo cleared."
                )
        except Exception as exc:
            log.exception("tickets upsert failed for %s: %s", ticket_number, exc)
            lines.append(f"• Failed to record assignment for ticket {ticket_number}.")

    header = (
        "Processed assignment:"
        if len(lines) == 1
        else f"Processed {len(lines)} assignments:"
    )
    await update.message.reply_text(header + "\n" + "\n".join(lines))


async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Friendly fallback when the user sends non-text while a ticket is active."""
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return
    ticket_id = await _get_active_ticket(update, context)
    if ticket_id:
        await update.message.reply_text(
            "Only text responses are supported right now. "
            f"Send a text message for ticket {ticket_id}, or /cancel."
        )
    else:
        await update.message.reply_text("Send /respond <ticket_id> to start a reply.")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Handler error: update=%s", update)


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Show help"),
            BotCommand("help", "Show help"),
            BotCommand("respond", "Start a reply for a ticket"),
            BotCommand("active", "Show the current active ticket"),
            BotCommand("cancel", "Clear active ticket"),
        ]
    )


def _build_bot_app() -> Application:
    bot_app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )
    bot_app.add_error_handler(error_handler)
    bot_app.add_handler(CommandHandler("start", start_cmd))
    bot_app.add_handler(CommandHandler("help", help_cmd))
    bot_app.add_handler(CommandHandler("active", active_cmd))
    bot_app.add_handler(CommandHandler("cancel", cancel_cmd))
    bot_app.add_handler(CommandHandler("respond", respond_cmd))
    _field_reply_filter = (
        filters.REPLY
        & ~filters.COMMAND
        & (filters.PHOTO | filters.TEXT)
        & _ReplyToAssignmentFilter()
    )
    bot_app.add_handler(MessageHandler(_field_reply_filter, handle_field_reply))
    # Assignment messages take priority over the generic /respond text flow.
    # Within a handler group only the first matching handler runs, so this must
    # be registered before the catch-all text handler below.
    bot_app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.Regex(_ASSIGNMENT_PATTERN),
            handle_assignment,
        )
    )
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_input))
    # Anything else (photos, documents, voice, stickers, ...) gets a friendly fallback.
    bot_app.add_handler(
        MessageHandler(
            (filters.ATTACHMENT | filters.PHOTO | filters.VOICE | filters.VIDEO | filters.Sticker.ALL)
            & ~filters.COMMAND,
            handle_non_text,
        )
    )
    return bot_app


def _resolve_webhook_url() -> str | None:
    if WEBHOOK_BASE_URL:
        return f"{WEBHOOK_BASE_URL.rstrip('/')}/webhook"
    if RAILWAY_PUBLIC_DOMAIN:
        domain = RAILWAY_PUBLIC_DOMAIN
        if not domain.startswith(("http://", "https://")):
            domain = f"https://{domain}"
        return f"{domain.rstrip('/')}/webhook"
    return None


def _verify_webhook_secret(request: Request) -> None:
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    header = request.headers.get("X-Telegram-Bot-Api-Secret-Token") or ""
    if not hmac.compare_digest(header.encode("utf-8"), TELEGRAM_WEBHOOK_SECRET.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")


bot_app = _build_bot_app()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await bot_app.initialize()
    await bot_app.start()

    webhook_url = _resolve_webhook_url()
    if webhook_url:
        secret = TELEGRAM_WEBHOOK_SECRET or None
        await bot_app.bot.set_webhook(
            url=webhook_url,
            secret_token=secret,
            drop_pending_updates=False,
        )
        log.info("Webhook registered: %s", webhook_url)
    else:
        log.warning(
            "WEBHOOK_BASE_URL or RAILWAY_PUBLIC_DOMAIN not set; Telegram webhook not registered."
        )

    try:
        yield
    finally:
        await bot_app.bot.delete_webhook(drop_pending_updates=False)
        await bot_app.stop()
        await bot_app.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook")
async def webhook_handler(request: Request) -> dict[str, str]:
    _verify_webhook_secret(request)

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from None

    update = Update.de_json(data, bot_app.bot)
    if not update:
        raise HTTPException(status_code=400, detail="Invalid Telegram update payload")

    # Always ack with 200 so Telegram does not endlessly retry on transient
    # handler failures. The handler logs the exception via error_handler.
    try:
        await bot_app.process_update(update)
    except Exception:
        log.exception("process_update failed for update_id=%s", getattr(update, "update_id", None))

    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT)
