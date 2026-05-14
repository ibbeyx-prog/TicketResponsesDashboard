"""
Telegram bot (webhook) + Supabase.

Database expectations
=====================

1) ``ticket_responses`` — append-only log of operator replies (used by /respond).

2) ``tickets_active`` — one row per ticket (current state). Driven by the
   assignment-message flow (``@user <Category> <ticket_number>``). DDL::

       create table if not exists public.tickets_active (
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
         last_assigned_at timestamptz default now(),
         created_at timestamptz default now(),
         updated_at timestamptz default now()
       );

   See ``supabase/migrations/20260512_history_and_rename.sql`` for the full
   set of changes (rename from ``tickets``, add ``last_assigned_at``, create
   the history table, RLS policies for anon).

   Override the table name with ``TICKETS_TABLE`` (default ``tickets_active``).

   Field engineers submit work by **replying** to an assignment message with
   plain text and/or a photo. Photos are stored in the Storage bucket
   ``ticket-photos`` (override with ``TICKET_PHOTOS_BUCKET``). A field reply
   moves the ticket to ``status='Open'`` (admin review queue); only the
   admin/ops team marks tickets ``'Completed'`` (or sends them back to
   ``'Open'``) from the dashboard.

   Dashboard Command Center posts are **plain text**: line 1 is
   ``@user <Category> <ticket_number>`` (normal spaces) so the same reply
   listener matches and updates Supabase.

   Operators run ``/chatid`` in the field group (as an allowed user when
   ``TELEGRAM_ALLOWED_USERNAMES`` is set) to print ``TELEGRAM_GROUP_CHAT_ID`` /
   ``TG_GROUP_ID`` for Railway or Streamlit secrets.

2a) ``ticket_attendance_logs`` — append-only history. Every assignment writes
    one row (``action_type='Assignment'``); every field response writes one row
    (``action_type='Response'`` with ``note`` + optional ``photo_url``). Override
    with ``ATTENDANCE_LOGS_TABLE`` (default ``ticket_attendance_logs``).

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
import html
import logging
import os
import re
import secrets
import string
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
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

from webhook_config import resolve_telegram_webhook_url

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, encoding="utf-8-sig", override=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ticket_bot")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
PORT = int(os.getenv("PORT", "8000"))
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
# Telegram ``secret_token`` must match ``[A-Za-z0-9_-]{1,256}`` (Bot API).
_WEBHOOK_SECRET_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
BOT_SESSIONS_TABLE = (os.getenv("BOT_SESSIONS_TABLE") or "bot_sessions").strip()
TICKETS_TABLE = (os.getenv("TICKETS_TABLE") or "tickets_active").strip()
ATTENDANCE_LOGS_TABLE = (
    os.getenv("ATTENDANCE_LOGS_TABLE") or "ticket_attendance_logs"
).strip()
TICKET_PHOTOS_BUCKET = (os.getenv("TICKET_PHOTOS_BUCKET") or "ticket-photos").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    missing = [k for k, v in (("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_KEY", SUPABASE_KEY)) if not v]
    raise ValueError(
        f"Missing {', '.join(missing)}. "
        f"Checked process env and {_ENV_PATH} (exists={_ENV_PATH.exists()}). "
        "See .env.example for all supported keys."
    )
if not TELEGRAM_TOKEN:
    raise ValueError(
        "Missing TELEGRAM_TOKEN. "
        f"Checked process env and {_ENV_PATH} (exists={_ENV_PATH.exists()}). "
        "See .env.example for all supported keys."
    )

_webhook_url_configured = resolve_telegram_webhook_url() is not None
if _webhook_url_configured and not TELEGRAM_WEBHOOK_SECRET:
    alphabet = string.ascii_letters + string.digits + "_-"
    example = "".join(secrets.choice(alphabet) for _ in range(32))
    raise ValueError(
        "TELEGRAM_WEBHOOK_SECRET is required whenever a webhook URL is set "
        "(RAILWAY_PUBLIC_DOMAIN, WEBHOOK_BASE_URL, or WEBHOOK_FULL_URL). "
        "Telegram sends it back as the X-Telegram-Bot-Api-Secret-Token header "
        "on every webhook POST so random clients cannot POST fake updates to "
        "your /webhook URL. "
        "Use 1–256 characters from A–Z, a–z, 0–9, underscore, hyphen only. "
        f"Example (generate your own): {example}"
    )
if TELEGRAM_WEBHOOK_SECRET and not _WEBHOOK_SECRET_PATTERN.fullmatch(TELEGRAM_WEBHOOK_SECRET):
    raise ValueError(
        "TELEGRAM_WEBHOOK_SECRET must be 1–256 characters from "
        "A–Z, a–z, 0–9, underscore, hyphen only (Telegram Bot API rule)."
    )

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# If False after first DB error, skip Supabase session reads/writes for this process.
_use_db_sessions = True

_EXTRA_ALLOWED_USERS: frozenset[str] = frozenset({"dissiby"})


def _truthy_env(key: str) -> bool:
    return (os.getenv(key) or "").strip().lower() in ("1", "true", "yes", "on")


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

_CATEGORY_ALTS: str = "|".join(re.escape(cat) for cat in _ASSIGNMENT_TASK_CATEGORIES)

# Telegram usernames: 5–32 chars, [A-Za-z0-9_], must start with a letter. Avoid
# ``\\w`` (Unicode "letters") so odd scripts cannot steal the @-capture.
_ASSIGNMENT_HANDLE = r"@[A-Za-z][A-Za-z0-9_]{3,31}"
_NEXT_ASSIGNMENT_HEAD = rf"(?:{_ASSIGNMENT_HANDLE})\s+(?:{_CATEGORY_ALTS})\s+[0-9]"

# Groups:
#   1 = @username, 2 = category, 3 = ticket_number (9 or 16 digits),
#   4 / named "info" = any additional text after the ticket number, up to
#       the next assignment header (so multi-assignment messages still
#       split cleanly) or the end of the message.
# `[\s\S]*?` is the canonical "match anything, including newlines, non-greedy"
# pattern; we don't use `re.DOTALL` so the rest of the pattern keeps its
# default semantics.
_ASSIGNMENT_PATTERN: re.Pattern[str] = re.compile(
    rf"({_ASSIGNMENT_HANDLE})\s+({_CATEGORY_ALTS})\s+((?:[0-9]{{16}})|(?:[0-9]{{9}}))"
    rf"(?P<info>[\s\S]*?)"
    rf"(?=(?:{_NEXT_ASSIGNMENT_HEAD})|\Z)"
)


def _normalize_assignment_blob(blob: str) -> str:
    """Make coordinator / Telegram punctuation friendlier for regex matching.

    Non-breaking spaces (``\\xa0``) and other unicode spaces often appear
    when messages are copied from spreadsheets; they break ``\\s``-based
    patterns if left in place.

    Dashboard / bot posts may use HTML parse mode; Telegram sometimes
    delivers ``reply_to_message.text`` with tags still present. Strip those
    so ``@user <Category> <ticket>`` on line 1 matches reliably.
    """
    if not blob:
        return ""
    s = str(blob).replace("\ufeff", "")
    s = html.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)
    # Command Center uses U+00BB (») between @handle, category, and ticket on line 1.
    s = s.replace("\u00bb", " ")
    s = s.replace("\u00a0", " ").replace("\u200b", "").replace("\u200c", "")
    s = s.replace("\u200e", "").replace("\u200f", "")  # LRM / RLM around mentions
    s = re.sub(r"[\u202A-\u202E\u2066-\u2069]", "", s)  # bidi embedding (can break ticket digits)
    s = re.sub(r"[\u1680\u180e\u2000-\u200a\u202f\u205f\u3000]", " ", s)
    # Emoji / variation selectors sometimes attach to the category token.
    s = re.sub(r"[\uFE00-\uFE0F]", "", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s


def _parent_assignment_blob(parent: object) -> str:
    """Text used to match ``@user <Category> <ticket>`` on the replied-to message.

    PTB 21+ may set ``reply_to_message`` to ``InaccessibleMessage`` (no text);
    ignore those instead of treating them as empty ``Message`` payloads.
    """
    if parent is None or not isinstance(parent, Message):
        return ""
    return _normalize_assignment_blob(
        f"{parent.text or ''}\n{parent.caption or ''}"
    )


def _clean_assignment_info(raw: str | None) -> str | None:
    """Tidy the trailing additional-info capture from the assignment regex.

    Strips leading / trailing whitespace and collapses any run of blank
    lines so multi-line address blocks survive but stray separators don't.
    Returns ``None`` if nothing useful remains.
    """
    if not raw:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    # Collapse 3+ consecutive newlines down to a single blank line.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned


class _ReplyToAssignmentFilter(filters.MessageFilter):
    """True when the message is a reply and the parent contains an assignment pattern."""

    def filter(self, message: Message) -> bool:
        parent = message.reply_to_message
        if not parent:
            return False
        blob = _parent_assignment_blob(parent)
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


def _is_group_chat(update: Update) -> bool:
    """Return True for group/supergroup/channel chats.

    The operator preference is for the bot to ingest data from group chats
    silently — no replies, no usage hints — and only chat in private DMs.
    """
    chat = update.effective_chat
    return bool(chat and chat.type in ("group", "supergroup", "channel"))


async def _reply(update: Update, text: str, **kwargs) -> None:
    """Send a reply, but stay silent in group chats to avoid noise.

    Use this in every handler instead of ``update.message.reply_text`` so the
    group-silent invariant lives in one place.
    """
    if _is_group_chat(update):
        return
    msg = update.effective_message
    if msg is None:
        return
    await msg.reply_text(text, **kwargs)


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
    parent_blob = _normalize_assignment_blob(parent_blob)
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


_TICKETS_MISSING_COLUMNS: set[str] = set()


def _strip_missing_ticket_columns(payload: dict[str, Any]) -> dict[str, Any]:
    if not _TICKETS_MISSING_COLUMNS:
        return payload
    return {k: v for k, v in payload.items() if k not in _TICKETS_MISSING_COLUMNS}


def _parse_missing_column(message: str) -> str | None:
    """Extract the column name from a PostgREST missing-column error.

    Handles both error shapes PostgREST has emitted historically:

    * Legacy SQL error (``42703``):
      ``column tickets_active.additional_info does not exist``
    * Schema cache error (``PGRST204``):
      ``Could not find the 'additional_info' column of 'tickets_active'
      in the schema cache``
    """
    m = re.search(r"column [\w\.]*?\.?(\w+) does not exist", message)
    if m:
        return m.group(1)
    m = re.search(r"Could not find the '(\w+)' column", message)
    if m:
        return m.group(1)
    return None


def _execute_ticket_update(
    payload: dict[str, Any], ticket_number: str
) -> None:
    """Run an UPDATE on tickets, retrying if optional columns are missing."""
    attempt = _strip_missing_ticket_columns(payload)
    last_err: Exception | None = None
    for _ in range(4):
        try:
            # PostgREST ``SyncFilterRequestBuilder`` (``update().eq(...)``) does not
            # support chaining ``.select()`` like a standalone SELECT. Chaining it
            # raises ``AttributeError`` before ``execute()``, so the DB never
            # updates — field replies looked successful in logs only up to the
            # failing line. Match ``app.py`` ``_cc_execute_ticket_update``: PATCH
            # and rely on PostgREST errors for real failures.
            supabase.table(TICKETS_TABLE).update(attempt).eq(
                "ticket_number", ticket_number
            ).execute()
            return
        except Exception as exc:
            text = str(exc)
            col = _parse_missing_column(text)
            if not col or col not in attempt:
                last_err = exc
                break
            _TICKETS_MISSING_COLUMNS.add(col)
            log.warning(
                "tickets table is missing column %r; dropping it from updates "
                "for the rest of this process. Apply the pending migration "
                "for `public.%s` to surface this column in the dashboard.",
                col,
                TICKETS_TABLE,
            )
            attempt = {k: v for k, v in attempt.items() if k != col}
            last_err = exc
    if last_err is not None:
        raise last_err


def _utc_now_iso() -> str:
    """Single source of truth for ISO-8601 UTC timestamps stored in Supabase."""
    return datetime.now(timezone.utc).isoformat()


def _db_insert_attendance_log(
    *,
    ticket_number: str,
    member_username: str,
    action_type: str,
    note: str | None = None,
    photo_url: str | None = None,
) -> None:
    """Append a row to ``ticket_attendance_logs``.

    Logging is best-effort: a failure here MUST NOT break the user-visible
    flow (assignment upsert or response capture), so exceptions are caught
    and logged rather than re-raised. The active row is the source of truth
    for current state; the log is the source of truth for history.
    """
    row = {
        "ticket_number": ticket_number,
        "member_username": member_username,
        "action_type": action_type,
        "note": note,
        "photo_url": photo_url,
        "timestamp": _utc_now_iso(),
    }
    try:
        supabase.table(ATTENDANCE_LOGS_TABLE).insert(row).execute()
    except Exception:
        log.exception(
            "Failed to insert attendance log (ticket=%s, member=%s, action=%s)",
            ticket_number,
            member_username,
            action_type,
        )


def _db_complete_ticket_field_response(
    ticket_number: str,
    *,
    field_response: str | None,
    photo_url: str | None = None,
    update_photo_url: bool = False,
    responder_username: str | None = None,
) -> None:
    """Record a field response and move the ticket into the admin review queue.

    A field reply is *not* the final state anymore -- it lands as ``Open``
    so the ops/admin team can review the photo+note on the dashboard and
    decide whether to mark the ticket ``Completed`` (or send it back to
    ``Open`` after re-review). The bot never sets ``Completed`` itself.
    """
    responded_at = _utc_now_iso()
    updates: dict[str, Any] = {
        "status": "Open",
        "responded_at": responded_at,
        "field_response": field_response,
        "updated_at": responded_at,
    }
    if update_photo_url:
        updates["photo_url"] = photo_url
    _execute_ticket_update(updates, ticket_number)

    if responder_username:
        _db_insert_attendance_log(
            ticket_number=ticket_number,
            member_username=responder_username,
            action_type="Response",
            note=field_response,
            photo_url=photo_url if update_photo_url else None,
        )


def _db_insert_assignment(
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
) -> None:
    now_iso = _utc_now_iso()
    row = {
        "ticket_number": ticket_number,
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
    }
    # `last_assigned_at` and `additional_info` are recent additions; if the
    # column hasn't been migrated yet on a given environment, drop it and
    # retry. Each missing column gets one strip-and-retry, so we cope with
    # both being absent without an infinite loop.
    for _ in range(4):
        try:
            supabase.table(TICKETS_TABLE).insert(row).execute()
            break
        except Exception as exc:
            col = _parse_missing_column(str(exc))
            if not col or col not in row:
                raise
            _TICKETS_MISSING_COLUMNS.add(col)
            row.pop(col, None)
    else:
        # All retries exhausted (extremely unlikely; we only retry while
        # PostgREST keeps telling us about new missing columns).
        raise RuntimeError(
            f"insert into {TICKETS_TABLE} failed: too many missing columns"
        )

    _db_insert_attendance_log(
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
    )


def _db_reassign_ticket(
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
) -> None:
    """Overwrite assigned_to / task_category and reset prior work for a re-assignment.

    Resets the task fully for the new assignee: ``status`` goes back to
    ``"Pending"``, the previous ``field_response`` / ``photo_url`` are
    nullified, ``additional_info`` is overwritten with whatever came on the
    new assignment message (or NULLed out if none provided), and
    ``last_assigned_at`` is refreshed so the dashboard's "Days to Look
    Back" filter sees this as a recent event.
    """
    now_iso = _utc_now_iso()
    updates = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
        "updated_at": now_iso,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
    }
    _execute_ticket_update(updates, ticket_number)

    _db_insert_attendance_log(
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
    )


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
        await _reply(update, "This chat is not available.")


_HELP_TEXT = (
    "Ticket response bot\n"
    "\n"
    "Field work (assignee):\n"
    "  Reply to your assignment message with text and/or a photo.\n"
    "  Text → saved as field_response; photo → uploaded to ticket-photos; ticket → Open (admin review).\n"
    "\n"
    "Groups + Telegram bot privacy:\n"
    "  If the bot never sees your reply, turn privacy OFF in @BotFather, or set\n"
    "  TELEGRAM_GROUP_REPLY_BRIDGE=1 so the bot posts a follow-up line you reply to.\n"
    "\n"
    "Operator /respond workflow:\n"
    "  1) /respond <ticket_id> — pick the ticket you want to reply to\n"
    "  2) Send a single text message — it is saved as your response\n"
    "  3) The active ticket is cleared automatically after a successful save\n"
    "\n"
    "Commands:\n"
    "  /start, /help — always available in private (confirm the bot is online)\n"
    "  /respond <ticket_id> — start a reply for a ticket (may require allowlist)\n"
    "  /active — show the ticket you are currently replying to\n"
    "  /cancel — clear the active ticket without saving\n"
    "  /chatid — show this chat's id (for TELEGRAM_GROUP_CHAT_ID; posts in groups)\n"
    "  /help — show this message"
)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Public entrypoint: do **not** gate on ``TELEGRAM_ALLOWED_USERNAMES``.

    Otherwise users without a Telegram @username (or anyone not on the list)
    see no usable reply in private — they think the bot is dead. Operator
    commands stay gated separately.
    """
    if update.message:
        await _reply(update, _HELP_TEXT)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Same as ``/start`` — always available so help works in private."""
    if update.message:
        await _reply(update, _HELP_TEXT)


async def active_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return
    ticket_id = await _get_active_ticket(update, context)
    if ticket_id:
        await _reply(update, 
            f"Active ticket: {ticket_id}\nSend a text message to save your response, or /cancel."
        )
    else:
        await _reply(update, "No active ticket. Start with /respond <ticket_id>.")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    had_ticket = await _get_active_ticket(update, context)
    await _clear_active_ticket(update, context)
    if update.message:
        if had_ticket:
            await _reply(update, f"Cleared active ticket: {had_ticket}")
        else:
            await _reply(update, "No active ticket to clear.")


async def chatid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show ``chat.id`` for ``TELEGRAM_GROUP_CHAT_ID`` (Streamlit Command Center).

    Deliberately uses ``reply_text`` in groups (not ``_reply``) so operators
    see the id in the field chat without deleting the webhook or using
    ``getUpdates``.
    """
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return

    cid = int(chat.id)
    ctype = chat.type or "unknown"
    raw_username = getattr(chat, "username", None)
    uname = raw_username.strip() if isinstance(raw_username, str) else ""

    lines = [
        "For TELEGRAM_GROUP_CHAT_ID (Streamlit Command Center), use:",
        str(cid),
        f"(this chat is a {ctype})",
    ]
    if uname:
        lines.append(f"Or use the public handle: @{uname}")

    await msg.reply_text("\n".join(lines))


async def respond_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return
    if not context.args:
        await _reply(update, "Usage: /respond <ticket_id>")
        return

    ticket_id = _validate_ticket_id(context.args[0])
    if not ticket_id:
        await _reply(update, 
            "Invalid ticket id. It must be a single non-empty token "
            f"(max {_MAX_TICKET_ID_LEN} chars, no whitespace)."
        )
        return

    await _set_active_ticket(update, context, ticket_id)
    await _reply(update, 
        f"Active ticket set: {ticket_id}\n"
        "Send a text message to save your response, or /cancel."
    )


async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.info(
        "handle_input fired: chat=%s user=@%s text=%r",
        _chat_id(update),
        (update.effective_user.username if update.effective_user else None),
        (update.message.text[:120] if update.message and update.message.text else None),
    )
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return

    ticket_id = await _get_active_ticket(update, context)
    if not ticket_id:
        await _reply(update, "Start with /respond <ticket_id>.")
        return

    text = (update.message.text or "").strip()
    if not text:
        await _reply(update, "Empty message — send some text to save as the response.")
        return

    username = update.effective_user.username if update.effective_user else None
    user_handle = f"@{username}" if username else "unknown_user"

    if not _is_group_chat(update):
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
        await _reply(update, 
            f"Could not save response for ticket {ticket_id}. "
            "It is still active — try again, or /cancel to abort."
        )
        return

    await _clear_active_ticket(update, context)
    await _reply(update, f"Saved response for ticket {ticket_id}.")


async def handle_field_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Assignee completes a task by replying to the assignment message (text and/or photo).

    Not gated by ``TELEGRAM_ALLOWED_USERNAMES`` — the replier must match ``assigned_to``
    on the ``tickets`` row (Telegram ``@username``, case-insensitive).
    """
    msg = update.message
    parent_preview = ""
    if msg and msg.reply_to_message:
        pr = msg.reply_to_message
        if isinstance(pr, Message):
            parent_preview = ((pr.text or "") + "\n" + (pr.caption or ""))[:200]
        else:
            parent_preview = f"<{type(pr).__name__}>"
    log.info(
        "handle_field_reply fired: chat=%s user=@%s text=%r parent=%r",
        _chat_id(update),
        (update.effective_user.username if update.effective_user else None),
        (msg.text[:120] if msg and msg.text else (msg.caption[:120] if msg and msg.caption else None)),
        parent_preview,
    )
    if not msg or not msg.reply_to_message:
        return

    parent = msg.reply_to_message
    if not isinstance(parent, Message):
        log.warning(
            "field_reply ignored: reply parent is not a readable Message (type=%s)",
            type(parent).__name__,
        )
        return

    parent_blob = _parent_assignment_blob(parent)
    username = update.effective_user.username if update.effective_user else None

    ticket_number = _resolve_ticket_from_assignment_reply(parent_blob, username)
    if not ticket_number:
        match_n = len(list(_ASSIGNMENT_PATTERN.finditer(parent_blob)))
        log.warning(
            "field_reply no ticket match chat=%s user=@%s parent_matches=%s parent_head=%r",
            _chat_id(update),
            username,
            match_n,
            parent_blob[:400],
        )
        if not username:
            await _reply(update, 
                "Could not match this reply to a ticket. "
                "Set a Telegram username so it matches assigned_to on the ticket."
            )
        else:
            await _reply(update, 
                "Could not match this reply to a ticket for your username. "
                "Reply to the assignment message that names your @handle."
            )
        return

    try:
        row = _db_get_ticket(ticket_number)
    except Exception:
        log.exception("tickets lookup failed for field reply: %s", ticket_number)
        await _reply(update, f"Database error while loading ticket {ticket_number}.")
        return

    if not row:
        await _reply(update, f"No ticket record found for {ticket_number}.")
        return

    if not _sender_matches_assigned_to(row.get("assigned_to"), username):
        log.warning(
            "field_reply assignee mismatch ticket=%s db_assigned_to=%r replier=@%s",
            ticket_number,
            row.get("assigned_to"),
            username,
        )
        await _reply(update, "You are not the assignee for that ticket.")
        return

    has_photo = bool(msg.photo)
    image_doc = (
        msg.document
        if msg.document and (msg.document.mime_type or "").startswith("image/")
        else None
    )
    caption_or_text = (msg.caption or msg.text or "").strip() or None

    if not has_photo and not image_doc and not caption_or_text:
        await _reply(update, 
            "Send a text message or a photo (optional caption) to complete this task."
        )
        return

    if has_photo:
        largest = msg.photo[-1]
        if not _is_group_chat(update):
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
            await _reply(update, "Could not upload the photo. Please try again.")
            return
        responder_handle = f"@{username}" if username else "@unknown"
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                photo_url=upload_url,
                update_photo_url=True,
                responder_username=responder_handle,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await _reply(update, 
                f"Photo uploaded but ticket {ticket_number} could not be updated in the database."
            )
            return
        log.info(
            "field response saved ticket=%s chat=%s photo=1",
            ticket_number,
            _chat_id(update),
        )
        await _reply(update, f"Ticket {ticket_number} sent for admin review (photo saved).")
    elif image_doc is not None:
        if not _is_group_chat(update):
            try:
                await context.bot.send_chat_action(
                    chat_id=msg.chat_id,
                    action=ChatAction.UPLOAD_PHOTO,
                )
            except Exception:
                pass
        try:
            tg_file = await context.bot.get_file(image_doc.file_id)
            raw = await tg_file.download_as_bytearray()
            image_bytes = bytes(raw)
            mime = (image_doc.mime_type or "image/jpeg").split(";")[0].strip()
            upload_url = _storage_upload_ticket_photo(ticket_number, image_bytes, mime)
        except Exception:
            log.exception("document image download or storage upload failed")
            await _reply(update, "Could not upload the image file. Please try again.")
            return
        responder_handle = f"@{username}" if username else "@unknown"
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                photo_url=upload_url,
                update_photo_url=True,
                responder_username=responder_handle,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await _reply(update, 
                f"Image uploaded but ticket {ticket_number} could not be updated in the database."
            )
            return
        log.info(
            "field response saved ticket=%s chat=%s document_image=1",
            ticket_number,
            _chat_id(update),
        )
        await _reply(update, f"Ticket {ticket_number} sent for admin review (image saved).")
    else:
        responder_handle = f"@{username}" if username else "@unknown"
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                update_photo_url=False,
                responder_username=responder_handle,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await _reply(update, f"Could not update ticket {ticket_number}.")
            return
        log.info(
            "field response saved ticket=%s chat=%s photo=0",
            ticket_number,
            _chat_id(update),
        )
        await _reply(update, f"Ticket {ticket_number} sent for admin review.")

    if await _get_active_ticket(update, context) == ticket_number:
        await _clear_active_ticket(update, context)


async def handle_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detect ``@user <Category> <ticket_number>`` patterns and upsert tickets.

    A single message may contain multiple assignments; each match is processed
    independently and the results are reported back as a single reply.
    """
    log.info(
        "handle_assignment fired: chat=%s user=@%s text=%r",
        _chat_id(update),
        (update.effective_user.username if update.effective_user else None),
        (update.message.text[:120] if update.message and update.message.text else None),
    )
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message or not update.message.text:
        return

    # Dashboard (and TELEGRAM_GROUP_REPLY_BRIDGE) post assignment-shaped lines
    # as the bot user. Those rows are already written in Supabase; skip so we
    # do not append duplicate attendance logs or re-run reassignment logic.
    if update.effective_user and update.effective_user.id == context.bot.id:
        return

    text = _normalize_assignment_blob(update.message.text)
    matches = list(_ASSIGNMENT_PATTERN.finditer(text))
    if not matches:
        return  # Filter shouldn't have triggered, but be defensive.

    lines: list[str] = []
    for m in matches:
        assigned_to = m.group(1)        # e.g. "@john"
        task_category = m.group(2)      # e.g. "Femto Installation"
        ticket_number = m.group(3)      # 16- or 9-digit string
        additional_info = _clean_assignment_info(m.group("info"))

        try:
            existing = _db_get_ticket(ticket_number)
        except Exception as exc:
            log.exception("tickets lookup failed for %s: %s", ticket_number, exc)
            lines.append(f"• Lookup failed for ticket {ticket_number}.")
            continue

        info_suffix = " (with extra info)" if additional_info else ""

        try:
            if existing is None:
                _db_insert_assignment(
                    ticket_number,
                    assigned_to,
                    task_category,
                    additional_info=additional_info,
                )
                lines.append(
                    f"• Assigned ticket {ticket_number} ({task_category}) "
                    f"to {assigned_to}{info_suffix}."
                )
            else:
                _db_reassign_ticket(
                    ticket_number,
                    assigned_to,
                    task_category,
                    additional_info=additional_info,
                )
                prev_assignee = existing.get("assigned_to") or "—"
                prev_status = existing.get("status") or "—"
                lines.append(
                    f"• Re-assigned ticket {ticket_number} ({task_category}) "
                    f"from {prev_assignee} to {assigned_to}{info_suffix}. "
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
    await _reply(update, header + "\n" + "\n".join(lines))

    # In groups with bot privacy ON, Telegram does not deliver replies to
    # *other users'* messages to the bot. Optional bridge: duplicate the
    # assignment line(s) in a bot-owned message so the field team can reply
    # *to the bot* (which privacy still allows).
    if (
        _truthy_env("TELEGRAM_GROUP_REPLY_BRIDGE")
        and _is_group_chat(update)
        and update.message
        and matches
        and lines
        and not all("failed" in ln.lower() for ln in lines)
    ):
        bridge_lines = [f"{m.group(1)} {m.group(2)} {m.group(3)}" for m in matches]
        bridge_text = (
            "\n".join(bridge_lines)
            + "\n\nField team: reply HERE (to this bot message) with text or photo."
        )
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=bridge_text,
                reply_to_message_id=update.message.message_id,
                disable_notification=True,
            )
        except Exception as exc:
            log.warning("TELEGRAM_GROUP_REPLY_BRIDGE send failed: %s", exc)


async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Friendly fallback when the user sends non-text while a ticket is active."""
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.message:
        return
    ticket_id = await _get_active_ticket(update, context)
    if ticket_id:
        await _reply(update, 
            "Only text responses are supported right now. "
            f"Send a text message for ticket {ticket_id}, or /cancel."
        )
    else:
        await _reply(update, "Send /respond <ticket_id> to start a reply.")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Handler error: update=%s", update)


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Show help"),
            BotCommand("help", "Show help"),
            BotCommand("chatid", "Show this chat id for TELEGRAM_GROUP_CHAT_ID"),
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
    bot_app.add_handler(CommandHandler("chatid", chatid_cmd))
    bot_app.add_handler(CommandHandler("active", active_cmd))
    bot_app.add_handler(CommandHandler("cancel", cancel_cmd))
    bot_app.add_handler(CommandHandler("respond", respond_cmd))
    _field_reply_media = (
        filters.PHOTO
        | filters.TEXT
        | filters.Document.JPG
        | filters.Document.MimeType("image/png")
        | filters.Document.MimeType("image/webp")
    )
    _field_reply_filter = (
        filters.REPLY
        & ~filters.COMMAND
        & _field_reply_media
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


def _verify_webhook_secret(request: Request) -> None:
    """Reject webhook POSTs that are not from Telegram (wrong / missing secret).

    When ``TELEGRAM_WEBHOOK_SECRET`` is unset (no webhook URL configured in
    env), we skip the check so local experiments can hit ``/webhook`` without
    Telegram headers. When it **is** set — required if a webhook URL is
    configured (``WEBHOOK_FULL_URL``, ``WEBHOOK_BASE_URL``, or
    ``RAILWAY_PUBLIC_DOMAIN``) — every POST must carry the matching
    ``X-Telegram-Bot-Api-Secret-Token`` header.
    """
    if not TELEGRAM_WEBHOOK_SECRET:
        return
    header = request.headers.get("X-Telegram-Bot-Api-Secret-Token") or ""
    if not hmac.compare_digest(
        header.encode("utf-8"), TELEGRAM_WEBHOOK_SECRET.encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")


bot_app = _build_bot_app()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await bot_app.initialize()
    await bot_app.start()

    webhook_url = resolve_telegram_webhook_url()
    if webhook_url:
        try:
            await bot_app.bot.set_webhook(
                url=webhook_url,
                secret_token=TELEGRAM_WEBHOOK_SECRET,
                drop_pending_updates=False,
            )
        except Exception:
            log.exception(
                "Telegram set_webhook failed (url=%s). Process keeps running so "
                "/health stays up; fix env or network and redeploy or run "
                "restore_webhook.py.",
                webhook_url,
            )
        else:
            log.info("Telegram set_webhook succeeded: %s", webhook_url)
            try:
                wh = await bot_app.bot.get_webhook_info()
                err = (wh.last_error_message or "").strip()
                if err:
                    log.warning(
                        "Telegram getWebhookInfo reports a delivery error (fix URL/TLS or secret): %s",
                        err[:500],
                    )
                log.info(
                    "Telegram webhook status: url=%r pending_updates=%s",
                    wh.url,
                    wh.pending_update_count,
                )
            except Exception:
                log.exception("Telegram get_webhook_info failed after set_webhook")
    else:
        log.warning(
            "No webhook URL configured (set RAILWAY_PUBLIC_DOMAIN, WEBHOOK_BASE_URL, or "
            "WEBHOOK_FULL_URL); Telegram webhook not registered."
        )

    try:
        yield
    finally:
        # Never call ``delete_webhook`` by default: every local ``uvicorn``
        # shutdown (Ctrl+C, killing a duplicate process, IDE stop) would
        # clear Telegram's webhook for *this bot token*, so the next group
        # message never reaches *any* running instance until something calls
        # ``set_webhook`` again. Rolling deploys on Railway have the same race.
        # Opt in explicitly when you really want Telegram to stop delivering.
        if _truthy_env("TELEGRAM_DELETE_WEBHOOK_ON_SHUTDOWN"):
            await bot_app.bot.delete_webhook(drop_pending_updates=False)
        await bot_app.stop()
        await bot_app.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    """Shallow root so probes to ``/`` do not 404; use ``/health`` for webhook hints."""
    return {"service": "ticket_bot", "health": "/health", "webhook": "/webhook"}


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness for load balancers; includes whether a webhook URL is configured in env.

    Does not call Telegram on every request (avoid rate limits). After deploy,
    read startup logs for ``set_webhook`` / ``get_webhook_info``, or run
    ``py -3 restore_webhook.py --probe`` from a machine with ``.env``.
    """
    url = resolve_telegram_webhook_url()
    return {
        "status": "ok",
        "webhook_url_configured": "yes" if url else "no",
        # What Telegram is told to POST to (after normalizing env). Compare to
        # getWebhookInfo.url if you still see 404 — they must match exactly.
        "telegram_callback_url": url or "",
    }


@app.post("/webhook")
@app.post("/webhook/")
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
