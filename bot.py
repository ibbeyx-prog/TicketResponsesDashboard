import os

from dotenv import load_dotenv
from supabase import create_client
from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
if not TELEGRAM_TOKEN:
    raise ValueError("Missing TELEGRAM_TOKEN in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Always authorized (normalized, no @). Optional TELEGRAM_ALLOWED_USERNAMES in .env merges
# with these (comma-separated handles). Leave env unset / empty for open access.
_EXTRA_ALLOWED_USERS: frozenset[str] = frozenset({"dissiby"})


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
    parsed = {_normalize_username(part) for part in trimmed.split(",")}
    parsed.discard(None)
    return frozenset(set(parsed) | set(_EXTRA_ALLOWED_USERS))


def _is_sender_allowed(update: Update) -> bool:
    handles = _effective_allowed_handles()
    if handles is None:
        return True
    sender = (
        update.effective_user.username if update.effective_user else None
    )
    key = _normalize_username(sender)
    return bool(key and key in handles)


async def _reply_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("This chat is not available.")


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    await update.message.reply_text(
        "Hi. Run /respond <ticket_id>, then send your message."
    )


async def respond_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    if not context.args:
        await update.message.reply_text("Try: /respond <ticket_id>")
        return

    ticket_id = context.args[0]
    context.user_data["active_ticket"] = ticket_id
    # No reply — staff sends the task update next; bot ingests silently.


async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_sender_allowed(update):
        await _reply_unauthorized(update, context)
        return
    ticket_id = context.user_data.get("active_ticket")
    if not ticket_id:
        await update.message.reply_text("Start with /respond <ticket_id>.")
        return

    username = update.effective_user.username
    user_handle = f"@{username}" if username else "unknown_user"
    text = update.message.text or ""

    payload = {
        "ticket_id": ticket_id,
        "user_handle": user_handle,
        "response_data": text,
    }
    supabase.table("ticket_responses").insert(payload).execute()

    # Clear only the active ticket to avoid accidental duplicate submissions.
    context.user_data.pop("active_ticket", None)
    # No Telegram reply — data is stored; dashboard reads from Supabase.


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Help"),
            BotCommand("respond", "Continue with a ticket"),
        ]
    )


def main() -> None:
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("respond", respond_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_input))

    app.run_polling()


if __name__ == "__main__":
    main()
