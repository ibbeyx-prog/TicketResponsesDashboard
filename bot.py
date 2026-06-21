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
         task_category text not null,
         status text default 'Daily Task',
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

   Each ``ticket_number`` (9 or 16 digits) is **unique** in ``tickets_active``.
   Field engineers submit work by **replying** to an assignment message with
   plain text and/or a photo. Photos are stored in the Storage bucket
   ``ticket-photos`` (override with ``TICKET_PHOTOS_BUCKET``). A field reply
   moves the ticket to ``status='Open'`` (admin review queue).

   **Wrong reply within ~1 hour:** delete the Telegram message (Telethon listener
   when ``TG_API_ID``/``TG_API_HASH`` are set) or reply ``UNDO`` to that message;
   the dashboard clears the response and sets the ticket back to ``Daily Task``.

   Only the admin/ops team marks tickets ``'Resolved'`` (or sends them back to
   ``'Open'``) from the dashboard.

   Dashboard Command Center posts are **plain text**: line 1 is
   ``@user <Category> <ticket_number>`` (normal spaces) so the same reply
   listener matches and updates Supabase.

   Operators run ``/chatid`` in the field group to print ``TELEGRAM_GROUP_CHAT_ID`` /
   ``TG_GROUP_ID`` for Railway or Streamlit secrets (group chats do not require
   ``TELEGRAM_ALLOWED_USERNAMES``).

   The bot does **not** post operational confirmations in field groups (no
   assignment/field-reply ack spam). Use the Streamlit dashboard (Daily Task / On Hold /
   Open / Log, plus toasts on new attendance-log rows) instead.

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

import asyncio
import hmac
import html
import logging
import os
import re
import secrets
import string
import sys
import time
import unicodedata
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from supabase_client import create_supabase_client, resolve_supabase_config
from telegram import BotCommand, Message, MessageEntity, Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from task_categories import (
    DEFAULT_ASSIGNMENT_TASK_CATEGORIES,
    fetch_task_category_names,
    resolve_task_category,
    sync_ticket_categories_into_table,
    task_categories_table,
)
from unattended import (
    CRON_SECRET,
    STATUS_DAILY_TASK,
    UNATTENDED_POLL_MINUTES,
    nudge_message,
    run_unattended_close,
    run_unattended_nudges,
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

_sb_cfg = resolve_supabase_config(env_path=_ENV_PATH, probe=True)
SUPABASE_URL = (
    _sb_cfg.url if _sb_cfg else (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
)
SUPABASE_KEY = _sb_cfg.key if _sb_cfg else (os.getenv("SUPABASE_KEY") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
PORT = int(os.getenv("PORT", "8000"))
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
# Telegram ``secret_token`` must match ``[A-Za-z0-9_-]{1,256}`` (Bot API).
_WEBHOOK_SECRET_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
BOT_SESSIONS_TABLE = (os.getenv("BOT_SESSIONS_TABLE") or "bot_sessions").strip()
TICKETS_TABLE = (os.getenv("TICKETS_TABLE") or "tickets_active").strip()
SALES_CASES_TABLE = (
    os.getenv("SALES_CASES_TABLE") or "dashboard_sales_cases"
).strip()
_SALES_STATUS_RESOLVED = "Resolved"
STATUS_ON_HOLD = "On Hold"
_FIELD_REPLY_STATUSES = frozenset({STATUS_DAILY_TASK, "Open", STATUS_ON_HOLD})
ATTENDANCE_LOGS_TABLE = (
    os.getenv("ATTENDANCE_LOGS_TABLE") or "ticket_attendance_logs"
).strip()
TICKET_VISITS_TABLE = (os.getenv("TICKET_VISITS_TABLE") or "ticket_visits").strip()
TICKET_PHOTOS_BUCKET = (os.getenv("TICKET_PHOTOS_BUCKET") or "ticket-photos").strip()
TASK_CATEGORIES_TABLE = task_categories_table().strip()
FIELD_RESPONSE_UNDO_MINUTES = max(
    1,
    int((os.getenv("FIELD_RESPONSE_UNDO_MINUTES") or "60").strip() or "60"),
)
FIELD_RESPONSE_UNDO_WINDOW = timedelta(minutes=FIELD_RESPONSE_UNDO_MINUTES)
_UNDO_TRIGGER_RE = re.compile(r"^(/undo|undo)$", re.IGNORECASE)

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

supabase = create_supabase_client(SUPABASE_URL, SUPABASE_KEY)

# If False after first DB error, skip Supabase session reads/writes for this process.
_use_db_sessions = True

_EXTRA_ALLOWED_USERS: frozenset[str] = frozenset({"dissiby"})


def _truthy_env(key: str) -> bool:
    return (os.getenv(key) or "").strip().lower() in ("1", "true", "yes", "on")


if TICKETS_TABLE.casefold() == "tickets" and not _truthy_env("ALLOW_LEGACY_TICKETS_TABLE"):
    log.warning(
        "TICKETS_TABLE is set to the legacy name %r. The dashboard defaults to "
        "`tickets_active`; the bot will write a different table until you set "
        "TICKETS_TABLE=tickets_active on Railway (or remove it), redeploy, and "
        "run the Supabase migration `20260518_sync_legacy_tickets_into_tickets_active.sql` "
        "if you need rows merged. To silence this warning while you migrate, set "
        "ALLOW_LEGACY_TICKETS_TABLE=1.",
        TICKETS_TABLE,
    )


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


# Extra spellings accepted in Telegram text; normalized to a canonical DB value.
_ASSIGNMENT_CATEGORY_SYNONYMS: dict[str, str] = {
    "Femto Recovery": "Femto Recover",
    "Coverage issue": "Coverage Check",
    "Coverage Issue": "Coverage Check",
    "Coverage Issues": "Coverage Check",
}

# Pattern: "@user <Category> <ticket_number>" — rebuilt when categories change in Supabase.
_ASSIGNMENT_HANDLE = r"@[A-Za-z][A-Za-z0-9_]{3,31}"
_assignment_categories_cache: tuple[str, ...] = DEFAULT_ASSIGNMENT_TASK_CATEGORIES
_assignment_pattern_cache: re.Pattern[str] | None = None
_categories_cache_at: float = 0.0
_CATEGORIES_CACHE_TTL_SEC = 3.0

# Coordinator line: @user, any category text, then a 9/16-digit ticket (may be on the next line).
_COORDINATOR_ASSIGNMENT_HINT = re.compile(
    rf"{_ASSIGNMENT_HANDLE}[\s\S]+?(?<!\d)(?:\d{{16}}|\d{{9}})(?!\d)",
    re.IGNORECASE,
)
_ASSIGNMENT_HANDLE_PATTERN = re.compile(_ASSIGNMENT_HANDLE, re.IGNORECASE)
_TICKET_IN_ASSIGNMENT_RE = re.compile(r"(?<!\d)(\d{16}|\d{9})(?!\d)")
_STANDALONE_FIELD_TICKET_RE = re.compile(r"^\s*(\d{16}|\d{9})\b")


def _fetch_task_categories_from_db() -> tuple[str, ...] | None:
    try:
        names, missing = fetch_task_category_names(
            supabase, include_defaults_if_empty=False
        )
    except Exception:
        log.exception("Failed to load task categories from %s", TASK_CATEGORIES_TABLE)
        return None
    if missing:
        return None
    return tuple(names) if names else None


def _compile_assignment_pattern(categories: tuple[str, ...]) -> re.Pattern[str]:
    spellings = categories + tuple(_ASSIGNMENT_CATEGORY_SYNONYMS.keys())
    # Longest labels first so e.g. "Femto Installation" wins over "Femto Fault".
    ordered = sorted(spellings, key=len, reverse=True)
    category_alts = "|".join(re.escape(cat) for cat in ordered)
    next_head = rf"(?:{_ASSIGNMENT_HANDLE})\s+(?:{category_alts})\s+[0-9]"
    return re.compile(
        rf"({_ASSIGNMENT_HANDLE})\s+({category_alts})\s+((?:[0-9]{{16}})|(?:[0-9]{{9}}))"
        rf"(?P<info>[\s\S]*?)"
        rf"(?=(?:{next_head})|\Z)",
        re.IGNORECASE,
    )


def _refresh_assignment_categories(*, force: bool = False) -> tuple[str, ...]:
    global _assignment_categories_cache, _assignment_pattern_cache, _categories_cache_at
    if (
        not force
        and _assignment_pattern_cache is not None
        and (time.monotonic() - _categories_cache_at) < _CATEGORIES_CACHE_TTL_SEC
    ):
        return _assignment_categories_cache
    loaded = _fetch_task_categories_from_db()
    if loaded:
        categories = loaded
    else:
        categories = _DEFAULT_ASSIGNMENT_TASK_CATEGORIES
    _assignment_categories_cache = categories
    _assignment_pattern_cache = _compile_assignment_pattern(categories)
    _categories_cache_at = time.monotonic()
    return _assignment_categories_cache


def _assignment_task_categories() -> tuple[str, ...]:
    return _refresh_assignment_categories()


def _assignment_pattern() -> re.Pattern[str]:
    _refresh_assignment_categories()
    assert _assignment_pattern_cache is not None
    return _assignment_pattern_cache


def _category_phrase_in_blob(norm: str) -> bool:
    """Match full category labels only (avoid ``recover`` inside ``responded``)."""
    lower = norm.lower()
    for cat in _assignment_task_categories():
        phrase = re.escape(cat.lower())
        if re.search(rf"(?<![a-z0-9]){phrase}(?![a-z0-9])", lower):
            return True
    return False


def _looks_like_coordinator_assignment(blob: str) -> bool:
    """True when text resembles ``@user <Category> <ticket>`` (category not in regex).

    Also accepts coordinator lines where the handle is only a Telegram **mention**
    (no literal ``@`` in ``message`` text) but a known category and ticket id are present —
    common for two-line posts like ``Coverage Check`` then ``100625230 …``.

    Messages that **start with a ticket id** (field status updates) are never assignments.
    """
    if not blob:
        return False
    if _COORDINATOR_ASSIGNMENT_HINT.search(blob):
        return True
    norm = _normalize_assignment_blob(blob)
    if _STANDALONE_FIELD_TICKET_RE.match(norm):
        return False
    ticket_m = _TICKET_IN_ASSIGNMENT_RE.search(norm)
    if not ticket_m:
        return False
    if not _category_phrase_in_blob(norm):
        return False
    return bool(norm[: ticket_m.start()].strip())


def _refresh_assignment_categories_if_plausible(blob: str) -> None:
    """Reload category list when text looks like a coordinator assignment.

    The assignment MessageHandler filter runs before ``handle_assignment``. If we
    only refresh categories inside the handler, a category added in the dashboard
    can be invisible to the filter for up to ``_CATEGORIES_CACHE_TTL_SEC``.
    """
    if _looks_like_coordinator_assignment(blob):
        _refresh_assignment_categories(force=True)


def _parse_coordinator_assignments(
    blob: str,
) -> list[tuple[str, str, str, str | None]]:
    """Parse ``@user``, category label, ticket id, and trailing notes from one message.

    Does not require the category to appear in the assignment regex alternation —
    new dashboard categories and line breaks between category and ticket still work.
    """
    blob = _normalize_assignment_blob(blob)
    if not blob:
        return []
    parsed: list[tuple[str, str, str, str | None]] = []
    pos = 0
    while pos < len(blob):
        hm = _ASSIGNMENT_HANDLE_PATTERN.search(blob, pos)
        if not hm:
            break
        assigned_to = hm.group(0)
        after = hm.end()
        tm = _TICKET_IN_ASSIGNMENT_RE.search(blob, after)
        if not tm:
            break
        category_raw = re.sub(r"\s+", " ", blob[after : tm.start()].strip())
        if not category_raw:
            pos = hm.end() + 1
            continue
        ticket_number = tm.group(1)
        next_hm = _ASSIGNMENT_HANDLE_PATTERN.search(blob, tm.end())
        info_end = next_hm.start() if next_hm else len(blob)
        additional_info = _clean_assignment_info(blob[tm.end() : info_end])
        parsed.append((assigned_to, category_raw, ticket_number, additional_info))
        pos = tm.end()
    return parsed


def _message_to_assignment_blob(msg: Message | None) -> str:
    """Build assignment text from message body, normalizing Telegram mention entities."""
    if not msg:
        return ""
    text = msg.text or ""
    cap = msg.caption or ""
    if msg.entities and text:
        for ent in sorted(msg.entities, key=lambda e: e.offset, reverse=True):
            if ent.type != MessageEntity.TEXT_MENTION or not ent.user:
                continue
            username = (ent.user.username or "").strip()
            if not username:
                continue
            start = ent.offset
            end = ent.offset + ent.length
            text = text[:start] + f"@{username}" + text[end:]
    return _normalize_assignment_blob(f"{text}\n{cap}")


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
    # Full-width / compatibility digits (Excel, some keyboards) → ASCII so
    # ``[0-9]{9}`` / ``{16}`` ticket captures work.
    s = unicodedata.normalize("NFKC", s)
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
    # Put ticket numbers that start a line onto the same line as the category when
    # coordinators paste "@user Category" then "123456789 details" on the next line.
    s = re.sub(
        r"(?<=\S)\n(?=(?:\d{16}|\d{9})(?:\s|$))",
        " ",
        s,
    )
    return s


def _parent_assignment_blob(parent: object) -> str:
    """Text used to match ``@user <Category> <ticket>`` on the replied-to message.

    PTB 21+ may set ``reply_to_message`` to ``InaccessibleMessage`` (no text);
    ignore those instead of treating them as empty ``Message`` payloads.
    """
    if parent is None or not isinstance(parent, Message):
        return ""
    if isinstance(parent, Message):
        return _message_to_assignment_blob(parent)
    return ""


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


class _StandaloneFieldReplyFilter(filters.MessageFilter):
    """Group text starting with a ticket id + notes (no swipe-reply)."""

    def filter(self, message: Message) -> bool:
        if not message or message.reply_to_message:
            return False
        text = (message.text or "").strip()
        if not text:
            return False
        return _parse_standalone_field_response(text) is not None


class _ReplyToAssignmentFilter(filters.MessageFilter):
    """True when the message is a reply and the parent contains an assignment pattern."""

    def filter(self, message: Message) -> bool:
        parent = message.reply_to_message
        if not parent:
            return False
        blob = _parent_assignment_blob(parent)
        _refresh_assignment_categories_if_plausible(blob)
        return _looks_like_coordinator_assignment(blob)


class _CoordinatorAssignmentFilter(filters.MessageFilter):
    """Body text or **photo caption** contains ``@user <Category> <ticket>``."""

    def filter(self, message: Message) -> bool:
        if not message:
            return False
        t = message.text or ""
        cap = message.caption or ""
        if not (t.strip() or cap.strip()):
            return False
        blob = _message_to_assignment_blob(message)
        _refresh_assignment_categories_if_plausible(blob)
        return _looks_like_coordinator_assignment(blob)


def _is_sender_allowed(update: Update) -> bool:
    handles = _effective_allowed_handles()
    if handles is None:
        return True
    sender = update.effective_user.username if update.effective_user else None
    key = _normalize_username(sender)
    return bool(key and key in handles)


def _is_assignment_or_group_ops_allowed(update: Update) -> bool:
    """Whether an assignment line in the field chat may be ingested.

    ``TELEGRAM_ALLOWED_USERNAMES`` is intended to gate ``/respond`` and related
    **private** operator commands — not coordinators posting
    ``@user <Category> <ticket>`` in the shared group. Those messages must
    still reach Supabase or the dashboard never gets assignments from chat.
    """
    if _is_group_chat(update):
        return True
    return _is_sender_allowed(update)


def _telegram_user_id(update: Update) -> int | None:
    user = update.effective_user
    return int(user.id) if user else None


def _telegram_replier_label(update: Update) -> str:
    """Best-effort label for who sent a message (works without a public @username)."""
    user = update.effective_user
    if not user:
        return "unknown"
    if user.username:
        return f"@{user.username}"
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    if name:
        return name
    return f"Telegram user {user.id}"


def _field_responded_by_value(*, assigned_to: object, replier_label: str) -> str | None:
    """Set only when the Telegram **sender** is not the ticket assignee.

    ``@mentions`` inside the message body (e.g. tagging assigner ``@ibeyx``) are
    ignored — they stay in ``field_response`` only.
    """
    assignee = str(assigned_to or "").strip()
    if not assignee or replier_label in ("", "unknown"):
        return None
    assignee_at = assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"
    if _normalize_username(replier_label) == _normalize_username(assignee_at):
        return None
    if replier_label.lower() == assignee_at.lower():
        return None
    if replier_label.lower() == assignee_at.lstrip("@").lower():
        return None
    label = replier_label.strip()
    return label if label.startswith("@") else f"@{label.lstrip('@')}"


def _chat_id(update: Update) -> int | None:
    chat = update.effective_chat
    return int(chat.id) if chat else None


def _skip_bot_own_assignment_echo(update: Update) -> bool:
    """Ignore dashboard assignment posts echoed back via webhook (prevents duplicate bridge)."""
    msg = update.effective_message
    user = update.effective_user
    if not msg or not user or not _is_group_chat(update):
        return False
    try:
        bot_id = bot_app.bot.id
    except Exception:
        return False
    if user.id != bot_id:
        return False
    blob = _message_to_assignment_blob(msg)
    if not blob:
        return False
    _refresh_assignment_categories_if_plausible(blob)
    return _looks_like_coordinator_assignment(blob)


_MESSAGE_DEDUP: dict[tuple[int, int, int], float] = {}
_MESSAGE_DEDUP_TTL_SEC = 180.0


def _message_dedup_key(
    chat_id: int, message_id: int, edit_date: int | None
) -> tuple[int, int, int]:
    return (chat_id, message_id, int(edit_date or 0))


def _prune_message_dedup(now: float) -> None:
    stale = [
        k
        for k, t in _MESSAGE_DEDUP.items()
        if now - t > _MESSAGE_DEDUP_TTL_SEC
    ]
    for k in stale:
        _MESSAGE_DEDUP.pop(k, None)


def _claim_telegram_message(
    chat_id: int, message_id: int, edit_date: int | None
) -> bool:
    """Return True if this (chat, message, edit) was not processed recently."""
    now = time.monotonic()
    _prune_message_dedup(now)
    key = _message_dedup_key(chat_id, message_id, edit_date)
    if key in _MESSAGE_DEDUP:
        return False
    _MESSAGE_DEDUP[key] = now
    return True


async def ingest_telegram_update(update: Update) -> None:
    """Single entry for webhook + Telethon group ingest (with deduplication)."""
    msg = update.effective_message
    if msg and msg.chat_id and msg.message_id:
        if not _claim_telegram_message(
            int(msg.chat_id), int(msg.message_id), msg.edit_date
        ):
            log.debug(
                "skip duplicate telegram message chat=%s msg=%s edit=%s",
                msg.chat_id,
                msg.message_id,
                msg.edit_date,
            )
            return

    if _skip_bot_own_assignment_echo(update):
        log.info(
            "skip bot-own assignment echo update_id=%s (dashboard already posted)",
            update.update_id,
        )
        return

    msg = update.effective_message
    if msg and _is_group_chat(update):
        blob = _message_to_assignment_blob(msg) if isinstance(msg, Message) else ""
        if blob and _looks_like_coordinator_assignment(blob):
            log.info(
                "ingest assignment-shaped group message chat=%s msg=%s head=%r",
                getattr(msg, "chat_id", None),
                getattr(msg, "message_id", None),
                blob[:100],
            )

    await bot_app.process_update(update)


def _log_incoming_update(update: Update) -> None:
    """Trace webhook delivery (Railway logs) without logging secrets."""
    msg = update.effective_message
    if not msg:
        log.info("webhook update_id=%s (no message payload)", update.update_id)
        return
    user = update.effective_user
    log.info(
        "webhook update_id=%s chat=%s chat_type=%s user=@%s reply=%s photo=%s text=%r",
        update.update_id,
        _chat_id(update),
        getattr(update.effective_chat, "type", None),
        user.username if user else None,
        bool(msg.reply_to_message),
        bool(msg.photo),
        (msg.text or msg.caption or "")[:120],
    )


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


async def _group_assignment_ack(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ticket_number: str,
    assigned_to: str,
) -> None:
    """Reserved: we intentionally do not post assignment confirmations in groups."""

    return


async def _group_field_nudge(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> None:
    """Reserved: field hints stay off the group chat (check dashboard / logs)."""

    return


async def _group_field_ack(
    update: Update, context: ContextTypes.DEFAULT_TYPE, ticket_number: str
) -> None:
    """Reserved: field-reply confirmations are dashboard-only (no group spam)."""

    return


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


def _db_get_sales_case_by_ref(case_ref: str) -> dict[str, Any] | None:
    res = (
        supabase.table(SALES_CASES_TABLE)
        .select(
            "case_ref, assigned_to, assigned_to_2, status, description, "
            "additional_info, field_response, photo_url, field_responded_by, responded_at"
        )
        .eq("case_ref", case_ref)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def _sales_field_reply_eligible(case_ref: str) -> bool:
    """Sales cases with a field assignee (not Resolved) can receive bot field replies."""
    try:
        row = _db_get_sales_case_by_ref(case_ref)
    except Exception:
        log.exception("sales lookup failed for field reply: %s", case_ref)
        return False
    if not row:
        return False
    if str(row.get("status") or "").strip() == _SALES_STATUS_RESOLVED:
        return False
    return bool(str(row.get("assigned_to") or "").strip())


def _field_reply_lookup(case_ref: str) -> dict[str, Any] | None:
    """``tickets_active`` row, else ``dashboard_sales_cases`` (tagged ``_field_source``)."""
    try:
        row = _db_get_ticket(case_ref)
    except Exception:
        row = None
    if row:
        row["_field_source"] = "ticket"
        return row
    try:
        sales = _db_get_sales_case_by_ref(case_ref)
    except Exception:
        sales = None
    if sales:
        sales["_field_source"] = "sales"
        return sales
    return None


def _field_reply_row_accepting_response(row: dict[str, Any]) -> bool:
    if row.get("_field_source") == "sales":
        if str(row.get("status") or "").strip() == _SALES_STATUS_RESOLVED:
            return False
        return bool(str(row.get("assigned_to") or "").strip())
    return str(row.get("status") or "").strip() in _FIELD_REPLY_STATUSES


def _sender_matches_assigned_to(assigned_to_db: object, replier_username: str | None) -> bool:
    if not replier_username:
        return False
    db_key = _normalize_username(str(assigned_to_db)) if assigned_to_db is not None else None
    return bool(db_key and db_key == _normalize_username(replier_username))


def _ticket_field_reply_eligible(ticket_number: str) -> bool:
    """Pending / On Hold / Open tickets, or active sales cases, can receive field completion."""
    try:
        row = _db_get_ticket(ticket_number)
    except Exception:
        log.exception("tickets lookup failed for field reply: %s", ticket_number)
        return False
    if row:
        return str(row.get("status") or "").strip() in _FIELD_REPLY_STATUSES
    return _sales_field_reply_eligible(ticket_number)


def _resolve_ticket_from_assignment_reply(
    parent_blob: str,
    reply_text: str | None,
) -> str | None:
    """Pick ticket from the replied-to assignment; trust ``@user`` on that line.

    Field staff often reply from rotating test phones (different Telegram accounts).
    A swipe-reply to ``@Dissiby … ticket`` counts as Dissiby's response regardless
    of the replier's @username.
    """
    parent_blob = _normalize_assignment_blob(parent_blob)
    _refresh_assignment_categories_if_plausible(parent_blob)
    parsed = _parse_coordinator_assignments(parent_blob)
    if not parsed:
        return None

    ids_in_reply = _extract_ticket_ids(reply_text) if reply_text else []
    if ids_in_reply:
        for tid in ids_in_reply:
            for _assigned_to, _category_raw, ticket_number, _info in parsed:
                if ticket_number == tid and _ticket_field_reply_eligible(ticket_number):
                    return ticket_number
        if len(ids_in_reply) == 1 and _ticket_field_reply_eligible(ids_in_reply[0]):
            return ids_in_reply[0]
        return None

    if len(parsed) == 1:
        _assigned_to, _category_raw, ticket_number, _info = parsed[0]
        if _ticket_field_reply_eligible(ticket_number):
            return ticket_number
        return None

    pending_on_parent: list[str] = []
    for _assigned_to, _category_raw, ticket_number, _info in parsed:
        try:
            row = _db_get_ticket(ticket_number)
        except Exception:
            row = None
        if row and str(row.get("status") or "").strip() == STATUS_DAILY_TASK:
            pending_on_parent.append(ticket_number)
        elif not row and _sales_field_reply_eligible(ticket_number):
            pending_on_parent.append(ticket_number)
    if len(pending_on_parent) == 1:
        return pending_on_parent[0]
    if len(pending_on_parent) > 1:
        log.warning(
            "field_reply ambiguous: %s assignment lines, %s still Pending; "
            "include ticket number in reply",
            len(parsed),
            len(pending_on_parent),
        )
    return None


_TICKET_ID_PATTERN: re.Pattern[str] = re.compile(r"\b(\d{16}|\d{9})\b")


def _extract_ticket_ids(*blobs: str | None) -> list[str]:
    """Collect unique 9/16-digit ticket ids from one or more message bodies (order preserved)."""
    seen: set[str] = set()
    ordered: list[str] = []
    for blob in blobs:
        if not blob:
            continue
        norm = _normalize_assignment_blob(blob)
        for m in _TICKET_ID_PATTERN.finditer(norm):
            tid = m.group(1)
            if tid not in seen:
                seen.add(tid)
                ordered.append(tid)
    return ordered


def _resolve_ticket_by_unique_id(
    parent_blob: str,
    reply_text: str | None,
    replier_username: str | None,
    *,
    trust_assignment_parent: bool = False,
) -> str | None:
    """Resolve using ticket_number (globally unique) from parent and/or reply text."""
    ids = _extract_ticket_ids(parent_blob, reply_text)
    if not ids:
        return None

    if trust_assignment_parent:
        existing = [tid for tid in ids if _ticket_field_reply_eligible(tid)]
        if len(existing) == 1:
            return existing[0]
        pending = []
        for tid in ids:
            try:
                row = _db_get_ticket(tid)
            except Exception:
                row = None
            if row and str(row.get("status") or "").strip() == STATUS_DAILY_TASK:
                pending.append(tid)
            elif not row and _sales_field_reply_eligible(tid):
                pending.append(tid)
        if len(pending) == 1:
            return pending[0]
        if len(existing) > 1:
            log.info(
                "field_reply: multiple ticket ids on assignment parent, using %s among %s",
                existing[0],
                existing,
            )
            return existing[0]
        return None

    if not replier_username:
        return None

    matched: list[str] = []
    for tid in ids:
        try:
            row = _db_get_ticket(tid)
        except Exception:
            log.exception("tickets lookup failed for ticket id %s", tid)
            row = None
        if not row:
            try:
                row = _db_get_sales_case_by_ref(tid)
            except Exception:
                log.exception("sales lookup failed for case ref %s", tid)
                row = None
        if not row:
            continue
        if _sender_matches_assigned_to(row.get("assigned_to"), replier_username):
            matched.append(tid)

    if len(matched) == 1:
        return matched[0]
    if len(matched) > 1:
        log.info(
            "field_reply: multiple ticket ids for @%s, using %s among %s",
            replier_username,
            matched[0],
            matched,
        )
        return matched[0]
    if len(ids) == 1:
        log.warning(
            "field_reply: ticket %s found in message but assignee does not match @%s",
            ids[0],
            replier_username,
        )
    return None


def _pending_tickets_for_assignee(replier_username: str | None) -> list[dict[str, Any]]:
    if not replier_username:
        return []
    try:
        res = (
            supabase.table(TICKETS_TABLE)
            .select("ticket_number, assigned_to, last_assigned_at")
            .eq("status", STATUS_DAILY_TASK)
            .execute()
        )
    except Exception:
        log.exception("pending tickets lookup failed for @%s", replier_username)
        return []
    return [
        r
        for r in (res.data or [])
        if _sender_matches_assigned_to(r.get("assigned_to"), replier_username)
    ]


def _pending_field_targets_for_assignee(replier_username: str | None) -> list[str]:
    """Open CSM tickets and active sales cases awaiting field completion for one assignee."""
    out: list[str] = []
    for r in _pending_tickets_for_assignee(replier_username):
        tn = str(r.get("ticket_number") or "").strip()
        if tn:
            out.append(tn)
    if not replier_username:
        return out
    try:
        res = (
            supabase.table(SALES_CASES_TABLE)
            .select("case_ref, assigned_to, status")
            .neq("status", _SALES_STATUS_RESOLVED)
            .execute()
        )
    except Exception:
        log.exception("pending sales cases lookup failed for @%s", replier_username)
    else:
        for r in res.data or []:
            if not _sender_matches_assigned_to(r.get("assigned_to"), replier_username):
                continue
            cref = str(r.get("case_ref") or "").strip()
            if cref:
                out.append(cref)
    return out


def _resolve_ticket_single_pending_for_assignee(replier_username: str | None) -> str | None:
    """Use the sole Pending row for this assignee (never guess among several)."""
    pending = _pending_field_targets_for_assignee(replier_username)
    if len(pending) == 1:
        return pending[0]
    if len(pending) > 1:
        log.warning(
            "field_reply ambiguous: @%s has %s pending tickets; require reply to "
            "assignment or ticket id in message",
            replier_username,
            len(pending),
        )
    return None


def _parse_standalone_field_response(text: str) -> tuple[str, str] | None:
    """Parse ``<ticket_id> <notes>`` when the engineer posts without swipe-reply.

    Field staff often reply with the ticket number first::

        2020051772000001 was responded for this task
        @ibeyx need to attend tomorrow …

  The ``@ibeyx`` here tags the **assigner/coordinator** in the note — it is kept in
  ``field_response`` text. ``field_responded_by`` is only the **Telegram account**
  that sent the message (when different from ``assigned_to``).
    """
    norm = _normalize_assignment_blob(text)
    if not norm:
        return None
    m = _STANDALONE_FIELD_TICKET_RE.match(norm)
    if not m:
        return None
    ticket_number = m.group(1)
    field_text = norm[m.end() :].strip()
    if not field_text:
        return None
    if not _ticket_field_reply_eligible(ticket_number):
        return None
    return ticket_number, field_text


def _resolve_ticket_for_field_reply(
    parent_blob: str,
    replier_username: str | None,
    reply_text: str | None,
) -> str | None:
    """Match a field reply to exactly one ticket (ticket_number is unique in the DB)."""
    if reply_text:
        ids_in_reply = _extract_ticket_ids(reply_text)
        if len(ids_in_reply) == 1 and _ticket_field_reply_eligible(ids_in_reply[0]):
            log.info(
                "field_reply matched ticket %s via ticket id in reply (any phone)",
                ids_in_reply[0],
            )
            return ids_in_reply[0]

    norm_parent = _normalize_assignment_blob(parent_blob)
    trust_parent = _looks_like_coordinator_assignment(norm_parent)

    if trust_parent:
        ticket = _resolve_ticket_from_assignment_reply(norm_parent, reply_text)
        if ticket:
            log.info(
                "field_reply matched ticket %s via assignment parent (trust assignee line)",
                ticket,
            )
            return ticket
        ticket = _resolve_ticket_by_unique_id(
            parent_blob,
            reply_text,
            replier_username,
            trust_assignment_parent=True,
        )
        if ticket:
            log.info("field_reply matched ticket %s via id on assignment parent", ticket)
        return ticket

    ticket = _resolve_ticket_by_unique_id(parent_blob, reply_text, replier_username)
    if ticket:
        log.info("field_reply matched unique ticket_number %s", ticket)
        return ticket
    ticket = _resolve_ticket_from_assignment_reply(parent_blob, reply_text)
    if ticket:
        return ticket
    ticket = _resolve_ticket_single_pending_for_assignee(replier_username)
    if ticket:
        log.info(
            "field_reply matched ticket %s via sole Pending row for @%s",
            ticket,
            replier_username,
        )
    return ticket


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
_SALES_MISSING_COLUMNS: set[str] = set()


def _strip_missing_ticket_columns(payload: dict[str, Any]) -> dict[str, Any]:
    if not _TICKETS_MISSING_COLUMNS:
        return payload
    return {k: v for k, v in payload.items() if k not in _TICKETS_MISSING_COLUMNS}


def _is_duplicate_key_error(exc: Exception) -> bool:
    """Detect Postgres unique / duplicate-key violations from PostgREST."""
    t = str(exc).lower()
    return "23505" in t or "duplicate key" in t or "unique constraint" in t


def _canonical_task_category(raw: str) -> str | None:
    """Map synonym / typo labels to canonical ``task_category`` values from Supabase."""
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if not s:
        return None
    if s in _ASSIGNMENT_CATEGORY_SYNONYMS:
        s = _ASSIGNMENT_CATEGORY_SYNONYMS[s]

    hit = resolve_task_category(s, _assignment_task_categories())
    if hit:
        return hit

    _refresh_assignment_categories(force=True)
    hit = resolve_task_category(s, _assignment_task_categories())
    if hit:
        return hit

    loaded = _fetch_task_categories_from_db()
    if loaded:
        hit = resolve_task_category(s, loaded)
        if hit:
            return hit

    aliases = {
        "femto recovery": "Femto Recover",
        "femto-installation": "Femto Installation",
        "repeater installation": "Repeater Installation",
        "repeater-installation": "Repeater Installation",
        "coverage": "Coverage Check",
        "coverage check": "Coverage Check",
        "coverage issue": "Coverage Check",
        "coverage issues": "Coverage Check",
    }
    return aliases.get(s.lower())


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
            if "status" in attempt:
                verify = (
                    supabase.table(TICKETS_TABLE)
                    .select("ticket_number, status")
                    .eq("ticket_number", ticket_number)
                    .limit(1)
                    .execute()
                )
                rows = verify.data or []
                if not rows:
                    raise RuntimeError(
                        f"ticket {ticket_number} not found after update "
                        f"(check TICKETS_TABLE={TICKETS_TABLE!r})"
                    )
                got = str(rows[0].get("status") or "").strip()
                want = str(attempt["status"]).strip()
                if got != want:
                    raise RuntimeError(
                        f"ticket {ticket_number} status is {got!r}, expected {want!r} "
                        "(Supabase RLS may be blocking UPDATE for this bot key)"
                    )
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


def _strip_missing_sales_columns(payload: dict[str, Any]) -> dict[str, Any]:
    if not _SALES_MISSING_COLUMNS:
        return payload
    return {k: v for k, v in payload.items() if k not in _SALES_MISSING_COLUMNS}


def _execute_sales_update(payload: dict[str, Any], case_ref: str) -> None:
    """Run an UPDATE on sales cases, retrying if optional columns are missing."""
    attempt = _strip_missing_sales_columns(payload)
    last_err: Exception | None = None
    for _ in range(4):
        try:
            supabase.table(SALES_CASES_TABLE).update(attempt).eq(
                "case_ref", case_ref
            ).execute()
            return
        except Exception as exc:
            text = str(exc)
            col = _parse_missing_column(text)
            if not col or col not in attempt:
                last_err = exc
                break
            _SALES_MISSING_COLUMNS.add(col)
            log.warning(
                "sales table is missing column %r; dropping it from updates "
                "for the rest of this process. Apply migration "
                "`20260717_sales_case_field_response.sql` if needed.",
                col,
            )
            attempt = {k: v for k, v in attempt.items() if k != col}
            last_err = exc
    if last_err is not None:
        raise last_err


def _utc_now_iso() -> str:
    """Single source of truth for ISO-8601 UTC timestamps stored in Supabase."""
    return datetime.now(timezone.utc).isoformat()


def _parse_db_timestamptz(raw: object) -> datetime | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _within_field_response_undo_window(responded_at_raw: object) -> bool:
    responded = _parse_db_timestamptz(responded_at_raw)
    if responded is None:
        return True
    return datetime.now(timezone.utc) - responded <= FIELD_RESPONSE_UNDO_WINDOW


def _db_undo_field_response_by_telegram_message(chat_id: int, message_id: int) -> bool:
    """Clear a field reply from Supabase if it matches a Telegram message within the undo window."""
    cid = int(chat_id)
    mid = int(message_id)

    res = (
        supabase.table(TICKETS_TABLE)
        .select("*")
        .eq("last_response_telegram_chat_id", cid)
        .eq("last_response_telegram_message_id", mid)
        .limit(1)
        .execute()
    )
    row = (res.data or [None])[0]
    ticket_number: str | None = str(row.get("ticket_number")) if row else None

    if not row:
        log_res = (
            supabase.table(ATTENDANCE_LOGS_TABLE)
            .select("ticket_number, timestamp")
            .eq("telegram_chat_id", cid)
            .eq("telegram_message_id", mid)
            .eq("action_type", "Response")
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        log_row = (log_res.data or [None])[0]
        if not log_row:
            return False
        ticket_number = str(log_row.get("ticket_number") or "").strip() or None
        if not ticket_number:
            return False
        ticket_res = (
            supabase.table(TICKETS_TABLE)
            .select("*")
            .eq("ticket_number", ticket_number)
            .limit(1)
            .execute()
        )
        row = (ticket_res.data or [None])[0]
        if not row:
            return False
        if row.get("last_response_telegram_message_id") not in (mid, None):
            pinned = row.get("last_response_telegram_message_id")
            if pinned is not None and int(pinned) != mid:
                return False

    if not row or not ticket_number:
        return False

    if not _within_field_response_undo_window(row.get("responded_at")):
        log.info(
            "field response undo expired ticket=%s chat=%s msg=%s",
            ticket_number,
            cid,
            mid,
        )
        return False

    now_iso = _utc_now_iso()
    updates = {
        "status": STATUS_DAILY_TASK,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "responded_at": None,
        "updated_at": now_iso,
        "last_response_telegram_chat_id": None,
        "last_response_telegram_message_id": None,
    }
    _execute_ticket_update(updates, ticket_number)

    try:
        supabase.table(ATTENDANCE_LOGS_TABLE).delete().eq(
            "telegram_chat_id", cid
        ).eq("telegram_message_id", mid).execute()
    except Exception:
        log.exception(
            "Failed to delete attendance log for undone telegram message %s/%s",
            cid,
            mid,
        )

    _db_insert_attendance_log(
        ticket_number=ticket_number,
        member_username="@telegram-undo",
        action_type="ResponseUndone",
        note=(
            f"Field reply removed (Telegram message {mid} deleted or UNDO) "
            f"within {FIELD_RESPONSE_UNDO_MINUTES} minute window."
        ),
    )
    log.info(
        "field response undone ticket=%s chat=%s msg=%s",
        ticket_number,
        cid,
        mid,
    )
    return True


def _db_insert_attendance_log(
    *,
    ticket_number: str,
    member_username: str,
    action_type: str,
    note: str | None = None,
    photo_url: str | None = None,
    telegram_chat_id: int | None = None,
    telegram_message_id: int | None = None,
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
    if telegram_chat_id is not None:
        row["telegram_chat_id"] = int(telegram_chat_id)
    if telegram_message_id is not None:
        row["telegram_message_id"] = int(telegram_message_id)
    try:
        supabase.table(ATTENDANCE_LOGS_TABLE).insert(row).execute()
    except Exception:
        log.exception(
            "Failed to insert attendance log (ticket=%s, member=%s, action=%s)",
            ticket_number,
            member_username,
            action_type,
        )


def _normalize_visit_assignee(raw: object) -> str:
    s = str(raw or "").strip().lstrip("@")
    return f"@{s.lower()}" if s else ""


def _bot_visits_deactivate_ticket(ticket_number: str) -> None:
    try:
        supabase.table(TICKET_VISITS_TABLE).update({"is_active": False}).eq(
            "ticket_number", str(ticket_number).strip()
        ).eq("is_active", True).execute()
    except Exception:
        pass


def _bot_visits_open_new(
    ticket_number: str,
    assignee: str,
    *,
    visit_start: str | None = None,
) -> None:
    """Insert a new open visit row (outcome = 'assigned', is_active = true)."""
    tn = str(ticket_number).strip()
    engineer = _normalize_visit_assignee(assignee)
    if not tn or not engineer:
        return
    try:
        _bot_visits_deactivate_ticket(tn)
        supabase.table(TICKET_VISITS_TABLE).insert(
            {
                "ticket_number": tn,
                "assignee": engineer,
                "visit_start": visit_start or _utc_now_iso(),
                "visit_end": None,
                "outcome": "assigned",
                "closed_by": "bot",
                "is_active": True,
            }
        ).execute()
    except Exception:
        log.exception("visit open failed for ticket %s", ticket_number)


def _bot_visits_reassign(
    ticket_number: str,
    new_assignee: str,
    *,
    now_iso: str | None = None,
) -> None:
    ts = now_iso or _utc_now_iso()
    try:
        payload = {
            "visit_end": ts,
            "outcome": "reassigned",
            "closed_by": "bot",
            "is_active": False,
        }
        tn = str(ticket_number).strip()
        supabase.table(TICKET_VISITS_TABLE).update(payload).eq(
            "ticket_number", tn
        ).eq("is_active", True).execute()
        supabase.table(TICKET_VISITS_TABLE).update(payload).eq(
            "ticket_number", tn
        ).is_("visit_end", "null").execute()
    except Exception:
        log.exception("visit reassign close failed for ticket %s", ticket_number)
    _bot_visits_open_new(ticket_number, new_assignee, visit_start=ts)


def _bot_visits_close_responded(
    ticket_number: str,
    *,
    assignee: str,
    response_note: str | None,
    photo_url: str | None,
    visit_end: str,
) -> None:
    """Best-effort: close the active visit row as 'responded' for this engineer."""
    try:
        payload = {
            "visit_end": visit_end,
            "outcome": "responded",
            "response_note": response_note,
            "photo_url": photo_url,
            "closed_by": "bot",
            "is_active": False,
        }
        tn = str(ticket_number).strip()
        engineer = _normalize_visit_assignee(assignee)
        supabase.table(TICKET_VISITS_TABLE).update(payload).eq(
            "ticket_number", tn
        ).eq("assignee", engineer).eq("is_active", True).execute()
        supabase.table(TICKET_VISITS_TABLE).update(payload).eq(
            "ticket_number", tn
        ).eq("is_active", True).execute()
        supabase.table(TICKET_VISITS_TABLE).update(payload).eq(
            "ticket_number", tn
        ).eq("assignee", engineer).is_("visit_end", "null").execute()
        supabase.table(TICKET_VISITS_TABLE).update(payload).eq(
            "ticket_number", tn
        ).is_("visit_end", "null").execute()
    except Exception:
        log.exception("visit close failed for ticket %s", ticket_number)


def _db_complete_ticket_field_response(
    ticket_number: str,
    *,
    field_response: str | None,
    photo_url: str | None = None,
    update_photo_url: bool = False,
    responder_username: str | None = None,
    field_responded_by: str | None = None,
    telegram_chat_id: int | None = None,
    telegram_message_id: int | None = None,
) -> None:
    """Record a field response and move the ticket into the admin review queue.

    A field reply is *not* the final state anymore -- it lands as ``Open``
    so the ops/admin team can review the photo+note on the dashboard and
    decide whether to mark the ticket ``Resolved`` (or send it back to
    ``Open`` after re-review). The bot never sets ``Resolved`` itself.
    """
    row = _db_get_ticket(ticket_number)
    if row:
        status = str(row.get("status") or "").strip()
        if status == "Open" and str(row.get("field_response") or "").strip():
            log.info(
                "field reply ignored — ticket %s already in Needs Review",
                ticket_number,
            )
            return

    responded_at = _utc_now_iso()
    sales_row: dict[str, Any] | None = None
    if row:
        updates: dict[str, Any] = {
            "status": "Open",
            "responded_at": responded_at,
            "field_response": field_response,
            "updated_at": responded_at,
        }
        if update_photo_url:
            updates["photo_url"] = photo_url
        if telegram_chat_id is not None:
            updates["last_response_telegram_chat_id"] = int(telegram_chat_id)
        if telegram_message_id is not None:
            updates["last_response_telegram_message_id"] = int(telegram_message_id)
        if field_responded_by:
            updates["field_responded_by"] = field_responded_by
        else:
            updates["field_responded_by"] = None
        _execute_ticket_update(updates, ticket_number)
        try:
            row = _db_get_ticket(ticket_number)
            if row and str(row.get("status") or "").strip() != "Open":
                log.error(
                    "ticket %s status is still %r after field response update",
                    ticket_number,
                    row.get("status"),
                )
        except Exception:
            log.exception("post-update verify failed for ticket %s", ticket_number)
    else:
        sales_row = _db_get_sales_case_by_ref(ticket_number)
        if not sales_row:
            raise RuntimeError(
                f"ticket {ticket_number} not found in {TICKETS_TABLE!r} "
                f"or {SALES_CASES_TABLE!r}"
            )
        existing_fr = str(sales_row.get("field_response") or "").strip()
        existing_photo = str(sales_row.get("photo_url") or "").strip()
        if existing_fr and existing_photo.startswith("http"):
            log.info(
                "field reply ignored — sales case %s already has response+photo",
                ticket_number,
            )
            return
        payload: dict[str, Any] = {
            "updated_at": responded_at,
            "responded_at": responded_at,
            "field_response": field_response,
        }
        if update_photo_url:
            payload["photo_url"] = photo_url
        if field_responded_by:
            payload["field_responded_by"] = field_responded_by
        _execute_sales_update(payload, ticket_number)
        row = sales_row
        log.info(
            "field response saved sales case=%s photo=%s",
            ticket_number,
            bool(update_photo_url and photo_url),
        )

    if responder_username:
        log_note = field_response
        if field_responded_by:
            prefix = f"Responded by {field_responded_by}"
            log_note = f"{prefix}: {field_response}" if field_response else prefix
        _db_insert_attendance_log(
            ticket_number=ticket_number,
            member_username=responder_username,
            action_type="Response",
            note=log_note,
            photo_url=photo_url if update_photo_url else None,
            telegram_chat_id=telegram_chat_id,
            telegram_message_id=telegram_message_id,
        )
    # Phase 2: close the active visit as 'responded' for this engineer.
    visit_assignee = (
        field_responded_by
        or responder_username
        or (str(row.get("assigned_to") or "") if row else "")
    )
    if visit_assignee:
        _bot_visits_close_responded(
            ticket_number,
            assignee=visit_assignee,
            response_note=field_response,
            photo_url=photo_url if update_photo_url else None,
            visit_end=responded_at,
        )


def _assignment_telegram_refs(update: Update) -> tuple[int | None, int | None]:
    """Store group assignment message ids (for edits / future tooling)."""
    msg = update.effective_message
    if not msg or not _is_group_chat(update):
        return None, None
    return int(msg.chat_id), int(msg.message_id)


def _db_insert_assignment(
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    assignment_telegram_chat_id: int | None = None,
    assignment_telegram_message_id: int | None = None,
) -> None:
    now_iso = _utc_now_iso()
    row = {
        "ticket_number": ticket_number,
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": STATUS_DAILY_TASK,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
        "dashboard_assigned_by": None,
        "unattended_nudge_sent_at": None,
    }
    if assignment_telegram_chat_id is not None:
        row["assignment_telegram_chat_id"] = int(assignment_telegram_chat_id)
    if assignment_telegram_message_id is not None:
        row["assignment_telegram_message_id"] = int(assignment_telegram_message_id)
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

    verify = (
        supabase.table(TICKETS_TABLE)
        .select("ticket_number")
        .eq("ticket_number", ticket_number)
        .limit(1)
        .execute()
    )
    if not (verify.data or []):
        raise RuntimeError(
            f"insert into {TICKETS_TABLE} for {ticket_number} did not persist "
            f"(check Supabase RLS / TICKETS_TABLE={TICKETS_TABLE!r})"
        )
    log.info("assignment inserted ticket=%s assignee=%s", ticket_number, assigned_to)

    _db_insert_attendance_log(
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
    )
    _bot_visits_open_new(ticket_number, assigned_to, visit_start=now_iso)


def _db_record_sales_assignment_from_telegram(
    case_ref: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    assignment_telegram_chat_id: int | None = None,
    assignment_telegram_message_id: int | None = None,
) -> None:
    """Record coordinator Telegram assignment for a sales-only case (no CSM ticket row)."""
    sales_row = _db_get_sales_case_by_ref(case_ref)
    if not sales_row:
        raise RuntimeError(
            f"sales case {case_ref} not found in {SALES_CASES_TABLE!r}"
        )
    now_iso = _utc_now_iso()
    prev_assignee = str(sales_row.get("assigned_to") or "").strip()
    same_assignee = bool(
        prev_assignee
        and _normalize_username(prev_assignee) == _normalize_username(assigned_to)
    )
    payload: dict[str, Any] = {
        "assigned_to": assigned_to,
        "field_task_category": task_category,
        "updated_at": now_iso,
    }
    if additional_info:
        payload["additional_info"] = additional_info
    if not same_assignee:
        payload["last_assigned_at"] = now_iso
        payload["field_response"] = None
        payload["photo_url"] = None
        payload["field_responded_by"] = None
        payload["responded_at"] = None
    if assignment_telegram_chat_id is not None:
        payload["assignment_telegram_chat_id"] = int(assignment_telegram_chat_id)
    if assignment_telegram_message_id is not None:
        payload["assignment_telegram_message_id"] = int(assignment_telegram_message_id)
    _execute_sales_update(payload, case_ref)
    _db_insert_attendance_log(
        ticket_number=case_ref,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
        telegram_chat_id=assignment_telegram_chat_id,
        telegram_message_id=assignment_telegram_message_id,
    )
    if not same_assignee:
        _bot_visits_open_new(case_ref, assigned_to, visit_start=now_iso)
    log.info(
        "sales assignment recorded case=%s assignee=%s same=%s",
        case_ref,
        assigned_to,
        same_assignee,
    )


def _db_reassign_ticket(
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    assignment_telegram_chat_id: int | None = None,
    assignment_telegram_message_id: int | None = None,
) -> None:
    """Overwrite assigned_to / task_category and reset prior work for a re-assignment.

    Resets the task fully for the new assignee: ``status`` goes back to
    ``STATUS_DAILY_TASK``, the previous ``field_response`` / ``photo_url`` are
    nullified, ``additional_info`` is overwritten with whatever came on the
    new assignment message (or NULLed out if none provided), and
    ``last_assigned_at`` is refreshed so the dashboard's "Days to Look
    Back" filter sees this as a recent event.
    """
    now_iso = _utc_now_iso()
    updates = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": STATUS_DAILY_TASK,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "responded_at": None,
        "updated_at": now_iso,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
        "dashboard_assigned_by": None,
        "unattended_nudge_sent_at": None,
    }
    if assignment_telegram_chat_id is not None:
        updates["assignment_telegram_chat_id"] = int(assignment_telegram_chat_id)
    if assignment_telegram_message_id is not None:
        updates["assignment_telegram_message_id"] = int(assignment_telegram_message_id)
    _execute_ticket_update(updates, ticket_number)

    _db_insert_attendance_log(
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
    )
    _bot_visits_reassign(ticket_number, assigned_to, now_iso=now_iso)


def _same_assignment_target(
    existing: dict,
    *,
    assigned_to: str,
    task_category: str,
) -> bool:
    ex_a = str(existing.get("assigned_to") or "").strip().lstrip("@").lower()
    new_a = assigned_to.strip().lstrip("@").lower()
    ex_c = str(existing.get("task_category") or "").strip().lower()
    new_c = task_category.strip().lower()
    return ex_a == new_a and ex_c == new_c


def _db_update_assignment_from_telegram(
    ticket_number: str,
    *,
    assigned_to: str,
    task_category: str,
    additional_info: str | None = None,
    assignment_telegram_chat_id: int | None = None,
    assignment_telegram_message_id: int | None = None,
) -> None:
    """Refresh assignment notes and Telegram refs without clearing a field response."""
    now_iso = _utc_now_iso()
    updates: dict[str, Any] = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "additional_info": additional_info,
        "updated_at": now_iso,
        "last_assigned_at": now_iso,
    }
    if assignment_telegram_chat_id is not None:
        updates["assignment_telegram_chat_id"] = int(assignment_telegram_chat_id)
    if assignment_telegram_message_id is not None:
        updates["assignment_telegram_message_id"] = int(assignment_telegram_message_id)
    _execute_ticket_update(updates, ticket_number)
    _db_insert_attendance_log(
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="AssignmentUpdated",
        note=additional_info,
    )


def _resolve_field_group_chat_id() -> int | str | None:
    raw = (
        os.getenv("TELEGRAM_GROUP_CHAT_ID")
        or os.getenv("TG_GROUP_ID")
        or os.getenv("TELEGRAM_GROUP_ID")
        or ""
    ).strip()
    if not raw:
        return None
    if raw.startswith("@"):
        return raw
    try:
        return int(raw)
    except ValueError:
        return raw


async def _send_unattended_nudge_telegram(row: dict) -> None:
    chat_id = _resolve_field_group_chat_id()
    if chat_id is None:
        log.warning("skip telegram nudge: TELEGRAM_GROUP_CHAT_ID not set")
        return
    text = nudge_message(
        assigned_to=str(row.get("assigned_to") or ""),
        ticket_number=str(row.get("ticket_number") or ""),
        task_category=str(row.get("task_category") or ""),
    )
    await bot_app.bot.send_message(chat_id=chat_id, text=text)


def _verify_cron_secret(request: Request) -> None:
    if not CRON_SECRET:
        raise HTTPException(status_code=503, detail="CRON_SECRET not configured")
    header = (request.headers.get("X-Cron-Secret") or "").strip()
    if not header:
        auth = (request.headers.get("Authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            header = auth[7:].strip()
    if not hmac.compare_digest(header.encode("utf-8"), CRON_SECRET.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid cron secret")


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
    msg = update.effective_message
    if not msg:
        return
    if _is_group_chat(update):
        # Never lecture the field group about allowlists — operators use the dashboard.
        return
    await _reply(update, "This chat is not available.")


_HELP_TEXT = (
    "Ticket response bot\n"
    "\n"
    "Field work (assignee):\n"
    "  Reply to your assignment message with text and/or a photo.\n"
    "  Text → saved as field_response; photo → uploaded to ticket-photos; ticket → Open (admin review).\n"
    f"  Wrong reply within {FIELD_RESPONSE_UNDO_MINUTES} min: delete the message in Telegram "
    "(when Telethon is configured) or reply UNDO to that message.\n"
    "\n"
    "Groups + Telegram bot privacy:\n"
    "  Coordinators: post ``@user <Category> <ticket>`` (ticket may be on the next line).\n"
    "  Field team: swipe-reply to that assignment (any phone; assignee from the line).\n"
    "  Set TG_API_ID + TG_API_HASH + TELEGRAM_GROUP_CHAT_ID so Telethon reads the group\n"
    "  even when @BotFather privacy mode is ON; or turn privacy OFF for webhook-only.\n"
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
    if update.effective_message:
        await _reply(update, _HELP_TEXT)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Same as ``/start`` — always available so help works in private."""
    if update.effective_message:
        await _reply(update, _HELP_TEXT)


async def active_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not update.effective_message:
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
    if update.effective_message:
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
    if not _is_group_chat(update) and not _is_sender_allowed(update):
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
    if not update.effective_message:
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
    if _is_group_chat(update):
        return
    em = update.effective_message
    log.info(
        "handle_input fired: chat=%s user=@%s text=%r",
        _chat_id(update),
        (update.effective_user.username if update.effective_user else None),
        ((em.text or "")[:120] if em else None),
    )
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not em:
        return

    ticket_id = await _get_active_ticket(update, context)
    if not ticket_id:
        await _reply(update, "Start with /respond <ticket_id>.")
        return

    text = (em.text or "").strip()
    if not text:
        await _reply(update, "Empty message — send some text to save as the response.")
        return

    username = update.effective_user.username if update.effective_user else None
    user_handle = f"@{username}" if username else "unknown_user"

    if not _is_group_chat(update):
        try:
            await context.bot.send_chat_action(chat_id=em.chat_id, action=ChatAction.TYPING)
        except Exception:
            # Non-fatal — typing indicator is purely cosmetic.
            pass

    row = _db_get_ticket(ticket_id)
    if row:
        try:
            _db_complete_ticket_field_response(
                ticket_id,
                field_response=text,
                update_photo_url=False,
                responder_username=user_handle,
                telegram_chat_id=int(em.chat_id),
                telegram_message_id=int(em.message_id),
            )
        except Exception:
            log.exception("ticket field completion failed for /respond flow: %s", ticket_id)
            await _reply(update, f"Could not update ticket {ticket_id} in the dashboard.")
            return
        await _clear_active_ticket(update, context)
        await _reply(update, f"Ticket {ticket_id} sent for admin review.")
        return

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
    await _reply(update, f"Saved response for ticket {ticket_id} (legacy log only).")


def _assignee_handle(assigned_to: object) -> str:
    assignee = str(assigned_to or "").strip()
    if not assignee:
        return "@unknown"
    return assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"


async def _apply_field_completion(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    ticket_number: str,
    *,
    username: str | None,
    assigned_to: str | None = None,
    field_response_override: str | None = None,
) -> None:
    """Save text/photo for ``ticket_number`` and set status Open (admin review)."""
    msg = update.effective_message
    if not msg:
        return

    try:
        ticket_row = _field_reply_lookup(ticket_number)
    except Exception:
        ticket_row = None
    db_assignee = assigned_to or (ticket_row or {}).get("assigned_to")
    assignee_handle = _assignee_handle(db_assignee)
    replier_label = _telegram_replier_label(update)
    responded_by = _field_responded_by_value(
        assigned_to=db_assignee,
        replier_label=replier_label,
    )

    tg_chat_id = int(msg.chat_id)
    tg_message_id = int(msg.message_id)

    has_photo = bool(msg.photo)
    image_doc = (
        msg.document
        if msg.document and (msg.document.mime_type or "").startswith("image/")
        else None
    )
    caption_or_text = (field_response_override or msg.caption or msg.text or "").strip() or None

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
        upload_url: str | None = None
        try:
            tg_file = await context.bot.get_file(largest.file_id)
            raw = await tg_file.download_as_bytearray()
            image_bytes = bytes(raw)
            upload_url = _storage_upload_ticket_photo(ticket_number, image_bytes, "image/jpeg")
        except Exception:
            log.exception("photo download or storage upload failed for %s", ticket_number)
            if not caption_or_text:
                await _reply(update, "Could not upload the photo. Try again or send a text update.")
                return
            log.warning(
                "saving text-only field response for %s after photo upload failed",
                ticket_number,
            )
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                photo_url=upload_url,
                update_photo_url=bool(upload_url),
                responder_username=assignee_handle,
                field_responded_by=responded_by,
                telegram_chat_id=tg_chat_id,
                telegram_message_id=tg_message_id,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await _reply(update, 
                f"Photo uploaded but ticket {ticket_number} could not be updated in the database."
            )
            return
        log.info(
            "field response saved ticket=%s chat=%s photo=1 responded_by=%s",
            ticket_number,
            _chat_id(update),
            responded_by or assignee_handle,
        )
        await _group_field_ack(update, context, ticket_number)
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
        upload_url = None
        try:
            tg_file = await context.bot.get_file(image_doc.file_id)
            raw = await tg_file.download_as_bytearray()
            image_bytes = bytes(raw)
            mime = (image_doc.mime_type or "image/jpeg").split(";")[0].strip()
            upload_url = _storage_upload_ticket_photo(ticket_number, image_bytes, mime)
        except Exception:
            log.exception("document image upload failed for %s", ticket_number)
            if not caption_or_text:
                await _reply(update, "Could not upload the image. Try again or send text.")
                return
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                photo_url=upload_url,
                update_photo_url=bool(upload_url),
                responder_username=assignee_handle,
                field_responded_by=responded_by,
                telegram_chat_id=tg_chat_id,
                telegram_message_id=tg_message_id,
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
            responded_by or assignee_handle,
        )
        await _group_field_ack(update, context, ticket_number)
        await _reply(update, f"Ticket {ticket_number} sent for admin review (image saved).")
    else:
        try:
            _db_complete_ticket_field_response(
                ticket_number,
                field_response=caption_or_text,
                update_photo_url=False,
                responder_username=assignee_handle,
                field_responded_by=responded_by,
                telegram_chat_id=tg_chat_id,
                telegram_message_id=tg_message_id,
            )
        except Exception:
            log.exception("ticket field completion update failed")
            await _reply(update, f"Could not update ticket {ticket_number}.")
            return
        log.info(
            "field response saved ticket=%s chat=%s photo=0 responded_by=%s",
            ticket_number,
            _chat_id(update),
            responded_by or assignee_handle,
        )
        await _group_field_ack(update, context, ticket_number)
        await _reply(update, f"Ticket {ticket_number} sent for admin review.")

    if await _get_active_ticket(update, context) == ticket_number:
        await _clear_active_ticket(update, context)


async def _telethon_message_plain_text(msg: object) -> str:
    """Telethon message body with ``MessageEntityMentionName`` expanded to ``@username``."""
    text = (getattr(msg, "message", None) or "").strip()
    entities = getattr(msg, "entities", None) or []
    if not text or not entities:
        return text
    try:
        from telethon.tl.types import MessageEntityMentionName
    except ImportError:
        return text
    for ent in sorted(entities, key=lambda e: e.offset, reverse=True):
        if not isinstance(ent, MessageEntityMentionName):
            continue
        user = getattr(ent, "user", None)
        if user is None and getattr(ent, "user_id", None):
            client = getattr(msg, "_client", None) or getattr(msg, "client", None)
            if client is not None:
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


def _replier_label_from_username(username: str | None, *, first_name: str = "", last_name: str = "", user_id: int | None = None) -> str:
    if username:
        u = username.strip().lstrip("@")
        return f"@{u}" if u else "unknown"
    name = " ".join(p for p in (first_name, last_name) if p).strip()
    if name:
        return name
    return f"Telegram user {user_id}" if user_id is not None else "unknown"


def _telethon_reply_media_content_type(msg: object) -> str:
    """Best-effort MIME for Telethon photo/document field replies."""
    if getattr(msg, "photo", None):
        return "image/jpeg"
    doc = getattr(msg, "document", None)
    if doc is not None:
        mime = (getattr(doc, "mime_type", None) or "").strip()
        if mime.startswith("image/"):
            return mime
    return "image/jpeg"


def _resolve_ticket_for_media_reply(
    parent_blob: str,
    replier_username: str | None,
    reply_text: str | None,
) -> str | None:
    """Match swipe-reply photos (incl. alternate phones) to one ticket."""
    ticket_number = _resolve_ticket_for_field_reply(
        parent_blob, replier_username, reply_text
    )
    if ticket_number:
        return ticket_number
    ids = _extract_ticket_ids(parent_blob, reply_text)
    eligible = [tid for tid in ids if _ticket_field_reply_eligible(tid)]
    if len(eligible) == 1:
        log.info(
            "media reply matched ticket %s via ticket id on parent/caption",
            eligible[0],
        )
        return eligible[0]
    return None


async def ingest_telethon_field_media_reply(event: object) -> bool:
    """Handle swipe-reply photos from test phones when sidecar cannot build PTB photo updates."""
    from telethon.tl.types import Message as TLMessage

    msg = getattr(event, "message", None)
    if not isinstance(msg, TLMessage) or not getattr(msg, "reply_to", None):
        return False
    has_photo = bool(getattr(msg, "photo", None))
    doc = getattr(msg, "document", None)
    has_image_doc = bool(
        doc and (getattr(doc, "mime_type", None) or "").startswith("image/")
    )
    if not has_photo and not has_image_doc:
        return False

    parent = await msg.get_reply_message()
    if not parent:
        log.warning("telethon media reply: could not load parent message")
        return False
    parent_blob = _normalize_assignment_blob(await _telethon_message_plain_text(parent))

    sender = await msg.get_sender()
    username = getattr(sender, "username", None) if sender else None
    replier_label = _replier_label_from_username(
        str(username) if username else None,
        first_name=str(getattr(sender, "first_name", None) or "") if sender else "",
        last_name=str(getattr(sender, "last_name", None) or "") if sender else "",
        user_id=int(getattr(sender, "id", 0)) if sender else None,
    )
    reply_text = (getattr(msg, "message", None) or "").strip() or None
    ticket_number = _resolve_ticket_for_media_reply(parent_blob, username, reply_text)
    if not ticket_number:
        log.warning(
            "telethon media reply: no ticket match user=%s parent=%r caption=%r",
            replier_label,
            parent_blob[:200],
            (reply_text or "")[:80],
        )
        return False

    try:
        row = _field_reply_lookup(ticket_number)
    except Exception:
        log.exception("telethon media reply: db lookup failed %s", ticket_number)
        return False
    if not row or not _field_reply_row_accepting_response(row):
        return False

    client = getattr(event, "client", None)
    image_bytes = b""
    if client is not None:
        try:
            raw = await client.download_media(msg, bytes)
            image_bytes = bytes(raw) if raw else b""
        except Exception:
            log.exception("telethon media download failed ticket=%s", ticket_number)

    upload_url: str | None = None
    if image_bytes:
        content_type = _telethon_reply_media_content_type(msg)
        try:
            upload_url = _storage_upload_ticket_photo(
                ticket_number, image_bytes, content_type
            )
        except Exception:
            log.exception("telethon media storage upload failed ticket=%s", ticket_number)

    if not upload_url and not reply_text:
        log.warning(
            "telethon media reply: no bytes and no caption ticket=%s replier=%s",
            ticket_number,
            replier_label,
        )
        return False

    assignee_handle = _assignee_handle(row.get("assigned_to"))
    responded_by = _field_responded_by_value(
        assigned_to=row.get("assigned_to"),
        replier_label=replier_label,
    )
    if not responded_by and row.get("field_responded_by"):
        responded_by = str(row.get("field_responded_by") or "").strip() or None
    field_response = reply_text
    if not field_response:
        existing = str(
            row.get("field_response") or row.get("additional_info") or ""
        ).strip()
        field_response = existing or None
    if not field_response and upload_url:
        field_response = "(photo)"
    chat_id = int(getattr(event, "chat_id", 0) or 0)
    _db_complete_ticket_field_response(
        ticket_number,
        field_response=field_response,
        photo_url=upload_url,
        update_photo_url=bool(upload_url),
        responder_username=assignee_handle,
        field_responded_by=responded_by,
        telegram_chat_id=chat_id or None,
        telegram_message_id=int(msg.id),
    )
    log.info(
        "telethon media field reply saved ticket=%s replier=%s assignee=%s",
        ticket_number,
        responded_by or replier_label,
        assignee_handle,
    )
    return True


async def handle_field_undo_reply(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Reply ``UNDO`` to the field's own response message to clear the dashboard (1h window)."""
    msg = update.effective_message
    if not msg or not msg.reply_to_message:
        return
    text = (msg.text or "").strip()
    if not _UNDO_TRIGGER_RE.match(text):
        return

    parent_id = int(msg.reply_to_message.message_id)
    chat_id = int(msg.chat_id)
    ok = _db_undo_field_response_by_telegram_message(chat_id, parent_id)
    if ok:
        await _reply(
            update,
            "Response removed from the dashboard. You can send a new reply to the assignment.",
        )
    else:
        await _reply(
            update,
            f"Nothing to undo (wrong message, older than {FIELD_RESPONSE_UNDO_MINUTES} minutes, "
            "or ticket already processed).",
        )


async def handle_group_standalone_field_reply(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Group message ``<ticket_id> <notes>`` without swipe-reply (common field habit)."""
    if not _is_group_chat(update):
        return
    msg = update.effective_message
    if not msg or msg.reply_to_message:
        return
    text = (msg.text or "").strip()
    if not text:
        return

    parsed = _parse_standalone_field_response(text)
    if not parsed:
        return

    ticket_number, field_text = parsed
    username = update.effective_user.username if update.effective_user else None
    log.info(
        "standalone field_reply chat=%s user=@%s ticket=%s text=%r",
        _chat_id(update),
        username,
        ticket_number,
        field_text[:120],
    )

    try:
        row = _field_reply_lookup(ticket_number)
    except Exception:
        log.exception("standalone field_reply db lookup failed %s", ticket_number)
        return
    if not row:
        log.warning("standalone field_reply: %s not in dashboard", ticket_number)
        return
    if not _field_reply_row_accepting_response(row):
        log.info(
            "standalone field_reply: %s status=%s — skip",
            ticket_number,
            row.get("status"),
        )
        return

    await _apply_field_completion(
        update,
        context,
        ticket_number,
        username=username,
        assigned_to=str(row.get("assigned_to") or ""),
        field_response_override=field_text,
    )


async def handle_field_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Assignee completes by replying to the assignment message (original group flow).

    Works for assignments posted by a coordinator in the group **or** by the
  dashboard (bot account). Not gated on ``TELEGRAM_ALLOWED_USERNAMES``.
    """
    msg = update.effective_message
    if not msg or not msg.reply_to_message:
        return

    username = update.effective_user.username if update.effective_user else None
    parent = msg.reply_to_message
    parent_blob = _parent_assignment_blob(parent) if isinstance(parent, Message) else ""
    reply_text = (msg.caption or msg.text or "").strip() or None

    log.info(
        "handle_field_reply fired: chat=%s user=@%s text=%r parent=%r trust_assignee_line=%s",
        _chat_id(update),
        username,
        (reply_text[:120] if reply_text else None),
        parent_blob[:160],
        _looks_like_coordinator_assignment(_normalize_assignment_blob(parent_blob)),
    )

    trust_assignment = _looks_like_coordinator_assignment(
        _normalize_assignment_blob(parent_blob)
    )
    has_reply_photo = bool(msg.photo) or (
        msg.document
        and (msg.document.mime_type or "").startswith("image/")
    )
    if has_reply_photo:
        ticket_number = _resolve_ticket_for_media_reply(
            parent_blob, username, reply_text
        )
    else:
        ticket_number = _resolve_ticket_for_field_reply(
            parent_blob, username, reply_text
        )
    if not ticket_number:
        log.warning(
            "field_reply no ticket match chat=%s user=@%s parent_head=%r",
            _chat_id(update),
            username,
            parent_blob[:400],
        )
        if _is_group_chat(update):
            await _group_field_nudge(
                update,
                context,
                "Swipe-reply to the assignment line (@user Category ticket_number). "
                "If several tickets are in one message, include the ticket number in your reply.",
            )
        else:
            await _reply(
                update,
                "Could not match this reply. Swipe-reply to the assignment message, "
                "or include the ticket number.",
            )
        return

    try:
        row = _field_reply_lookup(ticket_number)
    except Exception:
        log.exception("field reply db lookup failed: %s", ticket_number)
        await _reply(update, f"Database error while loading case {ticket_number}.")
        return

    if not row:
        await _reply(
            update,
            f"No ticket or sales case record found for {ticket_number}.",
        )
        return

    if not _field_reply_row_accepting_response(row):
        label = str(row.get("status") or "").strip() or "—"
        await _reply(
            update,
            f"Case {ticket_number} is already {label} — no new field reply needed.",
        )
        return

    ids_in_reply = _extract_ticket_ids(reply_text) if reply_text else []
    explicit_ticket_in_reply = ticket_number in ids_in_reply
    allow_any_phone = (
        trust_assignment or explicit_ticket_in_reply or has_reply_photo
    )
    if not allow_any_phone and not _sender_matches_assigned_to(
        row.get("assigned_to"), username
    ):
        log.warning(
            "field_reply assignee mismatch ticket=%s db=%r replier=@%s",
            ticket_number,
            row.get("assigned_to"),
            username,
        )
        await _reply(update, "You are not the assignee for that ticket.")
        return

    standalone_parsed = (
        _parse_standalone_field_response(reply_text) if reply_text else None
    )
    field_override = standalone_parsed[1] if standalone_parsed else None

    await _apply_field_completion(
        update,
        context,
        ticket_number,
        username=username,
        assigned_to=str(row.get("assigned_to") or ""),
        field_response_override=field_override,
    )


async def handle_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detect ``@user <Category> <ticket_number>`` patterns and upsert tickets.

    Uses ``effective_message`` so **channel posts** and **photo captions** work,
    not only plain group text messages.
    """
    msg = update.effective_message
    body_preview = ""
    if msg:
        body_preview = ((msg.text or "") + "\n" + (msg.caption or "")).strip()[:120]
    log.info(
        "handle_assignment fired: chat=%s user=@%s body=%r",
        _chat_id(update),
        (update.effective_user.username if update.effective_user else None),
        body_preview,
    )
    if not _is_assignment_or_group_ops_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not msg:
        return
    t = msg.text or ""
    cap = msg.caption or ""
    if not (t.strip() or cap.strip()):
        return

    # Dashboard posts assignment-shaped lines as the bot user. Those rows are
    # already written in Supabase; skip so we
    # do not append duplicate attendance logs or re-run reassignment logic.
    if update.effective_user and update.effective_user.id == context.bot.id:
        return

    text = _message_to_assignment_blob(msg)
    _refresh_assignment_categories_if_plausible(text)
    parsed = _parse_coordinator_assignments(text)
    tg_chat_id, tg_msg_id = _assignment_telegram_refs(update)
    if not parsed:
        if _looks_like_coordinator_assignment(text):
            log.warning(
                "assignment-shaped message did not parse: chat=%s body=%r",
                _chat_id(update),
                text[:200],
            )
            if _is_group_chat(update):
                await _group_field_nudge(
                    update,
                    context,
                    "Could not parse that assignment. Use: "
                    "@engineer Category 9-or-16-digit-ticket (ticket may be on the next line).",
                )
        return

    lines: list[str] = []
    for assigned_to, task_category_raw, ticket_number, additional_info in parsed:

        task_category = _canonical_task_category(task_category_raw)
        if not task_category:
            log.warning(
                "unknown task_category %r for ticket %s (categories=%s)",
                task_category_raw,
                ticket_number,
                _assignment_task_categories(),
            )
            cat_err = (
                f"Unknown category {task_category_raw!r} for ticket {ticket_number}. "
                f"Add it under **Edit categories** on the dashboard, then retry. "
                f"Known: {', '.join(_assignment_task_categories())}."
            )
            lines.append(f"• {cat_err}")
            if _is_group_chat(update):
                await _group_field_nudge(update, context, cat_err[:350])
            continue

        try:
            existing = _db_get_ticket(ticket_number)
            sales_row = None
            if existing is None:
                sales_row = _db_get_sales_case_by_ref(ticket_number)
        except Exception as exc:
            log.exception("lookup failed for %s: %s", ticket_number, exc)
            lines.append(f"• Lookup failed for ticket {ticket_number}.")
            continue

        info_suffix = " (with extra info)" if additional_info else ""

        try:
            if existing is None and sales_row is not None:
                _db_record_sales_assignment_from_telegram(
                    ticket_number,
                    assigned_to,
                    task_category,
                    additional_info=additional_info,
                    assignment_telegram_chat_id=tg_chat_id,
                    assignment_telegram_message_id=tg_msg_id,
                )
                lines.append(
                    f"• Assigned sales case {ticket_number} ({task_category}) "
                    f"to {assigned_to}{info_suffix}."
                )
                await _group_assignment_ack(update, context, ticket_number, assigned_to)
            elif existing is None:
                try:
                    _db_insert_assignment(
                        ticket_number,
                        assigned_to,
                        task_category,
                        additional_info=additional_info,
                        assignment_telegram_chat_id=tg_chat_id,
                        assignment_telegram_message_id=tg_msg_id,
                    )
                except Exception as insert_exc:
                    if _is_duplicate_key_error(insert_exc):
                        log.warning(
                            "duplicate insert for %s, retrying as reassign: %s",
                            ticket_number,
                            insert_exc,
                        )
                        existing = _db_get_ticket(ticket_number)
                        if existing is None:
                            raise insert_exc
                        _db_reassign_ticket(
                            ticket_number,
                            assigned_to,
                            task_category,
                            additional_info=additional_info,
                            assignment_telegram_chat_id=tg_chat_id,
                            assignment_telegram_message_id=tg_msg_id,
                        )
                        prev_assignee = existing.get("assigned_to") or "—"
                        prev_status = existing.get("status") or "—"
                        lines.append(
                            f"• Re-assigned ticket {ticket_number} ({task_category}) "
                            f"from {prev_assignee} to {assigned_to}{info_suffix}. "
                            f"Status reset to Daily Task (was {prev_status}); "
                            "previous response and photo cleared."
                        )
                    else:
                        raise insert_exc
                else:
                    lines.append(
                        f"• Assigned ticket {ticket_number} ({task_category}) "
                        f"to {assigned_to}{info_suffix}."
                    )
                await _group_assignment_ack(update, context, ticket_number, assigned_to)
            else:
                prev_status = str(existing.get("status") or "").strip()
                # Only keep field_response when refreshing notes on an **Open** ticket
                # (admin review). A new coordinator post for **Pending** / Unattended /
                # Resolved is always a fresh field visit → full reassign (clears reply).
                keep_open_field_work = prev_status == "Open" and _same_assignment_target(
                    existing,
                    assigned_to=assigned_to,
                    task_category=task_category,
                )
                if keep_open_field_work:
                    _db_update_assignment_from_telegram(
                        ticket_number,
                        assigned_to=assigned_to,
                        task_category=task_category,
                        additional_info=additional_info,
                        assignment_telegram_chat_id=tg_chat_id,
                        assignment_telegram_message_id=tg_msg_id,
                    )
                    log.info(
                        "assignment notes updated ticket=%s status=Open (field work kept)",
                        ticket_number,
                    )
                    lines.append(
                        f"• Updated assignment notes for {ticket_number} "
                        f"({task_category}) — kept **Open** and field response."
                    )
                else:
                    _db_reassign_ticket(
                        ticket_number,
                        assigned_to,
                        task_category,
                        additional_info=additional_info,
                        assignment_telegram_chat_id=tg_chat_id,
                        assignment_telegram_message_id=tg_msg_id,
                    )
                    prev_assignee = existing.get("assigned_to") or "—"
                    lines.append(
                        f"• Re-assigned ticket {ticket_number} ({task_category}) "
                        f"from {prev_assignee} to {assigned_to}{info_suffix}. "
                        f"Status reset to Daily Task (was {prev_status}); "
                        "previous response and photo cleared."
                    )
                await _group_assignment_ack(update, context, ticket_number, assigned_to)
        except Exception as exc:
            log.exception("tickets upsert failed for %s: %s", ticket_number, exc)
            lines.append(f"• Failed to record assignment for ticket {ticket_number}.")
            if _is_group_chat(update):
                await _group_field_nudge(
                    update,
                    context,
                    f"Could not save ticket {ticket_number} to the dashboard. "
                    f"Check Railway logs. Supabase said: {str(exc)[:200]}",
                )

    header = (
        "Processed assignment:"
        if len(lines) == 1
        else f"Processed {len(lines)} assignments:"
    )
    await _reply(update, header + "\n" + "\n".join(lines))


async def handle_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fallback for unsupported types; in groups, photos may be field completions."""
    em = update.effective_message
    if not em:
        return

    if _is_group_chat(update) and not em.reply_to_message:
        username = update.effective_user.username if update.effective_user else None
        caption = (em.caption or "").strip()
        standalone = _parse_standalone_field_response(caption) if caption else None
        has_photo = bool(em.photo) or (
            em.document
            and (em.document.mime_type or "").startswith("image/")
        )
        if has_photo and standalone:
            ticket_number, field_text = standalone
            try:
                row = _field_reply_lookup(ticket_number)
            except Exception:
                row = None
            if row and _field_reply_row_accepting_response(row):
                log.info(
                    "handle_non_text: group photo+caption standalone for %s",
                    ticket_number,
                )
                await _apply_field_completion(
                    update,
                    context,
                    ticket_number,
                    username=username,
                    assigned_to=str(row.get("assigned_to") or ""),
                    field_response_override=field_text,
                )
                return
        if has_photo and username:
            ticket_number = _resolve_ticket_for_field_reply("", username, None)
            if ticket_number:
                try:
                    row = _field_reply_lookup(ticket_number)
                except Exception:
                    row = None
                if row and _sender_matches_assigned_to(row.get("assigned_to"), username):
                    log.info(
                        "handle_non_text: group photo completion for %s",
                        ticket_number,
                    )
                    await _apply_field_completion(
                        update, context, ticket_number, username=username
                    )
                    return

    if _is_group_chat(update):
        return

    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
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
    me = await application.bot.get_me()
    log.info("Telegram bot identity: @%s id=%s", me.username, me.id)
    group_raw = (
        os.getenv("TELEGRAM_GROUP_CHAT_ID")
        or os.getenv("TG_GROUP_ID")
        or os.getenv("TELEGRAM_GROUP_ID")
        or ""
    ).strip()
    if group_raw:
        log.info("TELEGRAM_GROUP_CHAT_ID (configured): %s", group_raw[:80])
    else:
        log.warning(
            "TELEGRAM_GROUP_CHAT_ID is not set on this service — field group posts "
            "from the dashboard may use a different host's secrets."
        )
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
    # Assignment lines can be plain text, a photo/document **caption**, or a channel post.
    # Do not restrict to ``filters.TEXT`` only — that misses most caption-only assignments.
    _assignment_msg_filter = (
        (
            filters.UpdateType.MESSAGE
            | filters.UpdateType.EDITED_MESSAGE
            | filters.UpdateType.CHANNEL_POST
            | filters.UpdateType.EDITED_CHANNEL_POST
        )
        & ~filters.COMMAND
        & _CoordinatorAssignmentFilter()
    )
    bot_app.add_handler(MessageHandler(_assignment_msg_filter, handle_assignment))
    # Field swipe-reply to an assignment (works for coordinator or dashboard posts).
    bot_app.add_handler(
        MessageHandler(
            filters.REPLY & filters.TEXT & filters.Regex(_UNDO_TRIGGER_RE),
            handle_field_undo_reply,
        )
    )
    _field_reply_filter = filters.REPLY & ~filters.COMMAND & _field_reply_media
    bot_app.add_handler(MessageHandler(_field_reply_filter, handle_field_reply))
    bot_app.add_handler(
        MessageHandler(
            ~filters.REPLY & ~filters.COMMAND & _StandaloneFieldReplyFilter(),
            handle_group_standalone_field_reply,
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


async def _unattended_background_loop() -> None:
    """Close/nudge Daily Task tickets on a timer (no external Railway cron required)."""
    await asyncio.sleep(30)
    interval_sec = max(300.0, UNATTENDED_POLL_MINUTES * 60.0)
    while True:
        try:
            close_stats = run_unattended_close(
                supabase,
                tickets_table=TICKETS_TABLE,
                attendance_table=ATTENDANCE_LOGS_TABLE,
            )
            if close_stats.get("closed"):
                log.info("unattended auto-close: %s", close_stats)
            nudge_stats = await run_unattended_nudges(
                supabase,
                tickets_table=TICKETS_TABLE,
                attendance_table=ATTENDANCE_LOGS_TABLE,
                send_telegram=_send_unattended_nudge_telegram,
            )
            if nudge_stats.get("sent"):
                log.info("unattended auto-nudge: %s", nudge_stats)
        except Exception:
            log.exception("unattended background tick failed")
        await asyncio.sleep(interval_sec)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await bot_app.initialize()
    await bot_app.start()

    try:
        sync_ticket_categories_into_table(
            supabase,
            tickets_table=TICKETS_TABLE,
            categories_table=TASK_CATEGORIES_TABLE,
        )
        _refresh_assignment_categories(force=True)
    except Exception:
        log.exception("Initial task category load failed")

    telethon_sidecar_client = None

    async def _on_telethon_group_update(data: dict[str, object]) -> None:
        update = Update.de_json(data, bot_app.bot)
        if not update:
            log.warning("Telethon sidecar: could not build Update from message")
            return
        _log_incoming_update(update)
        try:
            await ingest_telegram_update(update)
        except Exception:
            log.exception("ingest_telegram_update failed (telethon sidecar)")

    try:
        from telethon_sidecar import start_telethon_sidecar

        telethon_sidecar_client = await start_telethon_sidecar(
            undo_callback=_db_undo_field_response_by_telegram_message,
            on_update_dict=_on_telethon_group_update,
            on_media_field_reply=ingest_telethon_field_media_reply,
        )
    except Exception:
        log.exception("Telethon sidecar failed to start")

    webhook_url = resolve_telegram_webhook_url()
    if webhook_url:
        try:
            await bot_app.bot.set_webhook(
                url=webhook_url,
                secret_token=TELEGRAM_WEBHOOK_SECRET,
                drop_pending_updates=False,
                allowed_updates=[
                    "message",
                    "edited_message",
                    "channel_post",
                    "edited_channel_post",
                ],
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

    unattended_task = asyncio.create_task(_unattended_background_loop())
    log.info(
        "Unattended worker started (poll every %.0f min; close after assign-day cutoff %02d:%02d UTC+5)",
        UNATTENDED_POLL_MINUTES,
        int(os.getenv("ASSIGN_DAY_CUTOFF_HOUR", "23")),
        int(os.getenv("ASSIGN_DAY_CUTOFF_MINUTE", "59")),
    )

    try:
        yield
    finally:
        unattended_task.cancel()
        try:
            await unattended_task
        except asyncio.CancelledError:
            pass
        if telethon_sidecar_client is not None:
            try:
                await telethon_sidecar_client.disconnect()
            except Exception:
                log.exception("Telethon sidecar disconnect failed")
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
        "tickets_table": TICKETS_TABLE,
        # Assignment / field-reply confirmations are never posted to group chats;
        # use the Streamlit dashboard (queues + Log + toasts). If you still see
        # old "Recorded …" / "✓ Ticket …" lines in Telegram, redeploy this service.
        "group_operational_replies": "off",
        "unattended_nudge_hours": str(os.getenv("UNATTENDED_NUDGE_HOURS", "6")),
        "unattended_poll_minutes": str(UNATTENDED_POLL_MINUTES),
        "telethon_group_ingest": (
            "on"
            if _env_str("TG_API_ID", "TELEGRAM_API_ID")
            and _env_str("TG_API_HASH", "TELEGRAM_API_HASH")
            and _env_str("TELEGRAM_GROUP_CHAT_ID", "TG_GROUP_ID")
            else "off"
        ),
    }


def _env_str(*names: str) -> str:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return ""


@app.post("/cron/unattended-nudge")
async def cron_unattended_nudge(request: Request) -> dict[str, object]:
    """Send 6h reminders for Pending tickets (Railway cron / external scheduler)."""
    _verify_cron_secret(request)
    stats = await run_unattended_nudges(
        supabase,
        tickets_table=TICKETS_TABLE,
        attendance_table=ATTENDANCE_LOGS_TABLE,
        send_telegram=_send_unattended_nudge_telegram,
    )
    return {"status": "ok", **stats}


@app.post("/cron/unattended-close")
async def cron_unattended_close(request: Request) -> dict[str, object]:
    """Mark auto-unattended Daily Task tickets and move them to Needs Review (Open)."""
    _verify_cron_secret(request)
    stats = run_unattended_close(
        supabase,
        tickets_table=TICKETS_TABLE,
        attendance_table=ATTENDANCE_LOGS_TABLE,
    )
    return {"status": "ok", **stats}


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

    _log_incoming_update(update)

    # Always ack with 200 so Telegram does not endlessly retry on transient
    # handler failures. The handler logs the exception via error_handler.
    try:
        await ingest_telegram_update(update)
    except Exception:
        log.exception("process_update failed for update_id=%s", getattr(update, "update_id", None))

    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT)
