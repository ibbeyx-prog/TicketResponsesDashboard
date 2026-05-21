"""Unattended assignment workflow — 6h nudge and end-of-assign-day auto-close."""

from __future__ import annotations

import logging
import os
from datetime import datetime, time, timedelta, timezone
from typing import Any

log = logging.getLogger("unattended")

STATUS_UNATTENDED = "Unattended"
STATUS_DAILY_TASK = "Daily Task"

UNATTENDED_NUDGE_HOURS = float(os.getenv("UNATTENDED_NUDGE_HOURS", "6"))
ASSIGN_DAY_CUTOFF_HOUR = int(os.getenv("ASSIGN_DAY_CUTOFF_HOUR", "23"))
ASSIGN_DAY_CUTOFF_MINUTE = int(os.getenv("ASSIGN_DAY_CUTOFF_MINUTE", "59"))
# UTC+5 — match app.py LOCAL_TZ
OPS_TZ = timezone(timedelta(hours=5))

CRON_SECRET = (
    os.getenv("CRON_SECRET", "").strip()
    or os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
)


def _parse_ts(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except (TypeError, ValueError):
        return None


def to_ops_local(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(OPS_TZ)


def assign_day_cutoff_time() -> time:
    return time(
        max(0, min(23, ASSIGN_DAY_CUTOFF_HOUR)),
        max(0, min(59, ASSIGN_DAY_CUTOFF_MINUTE)),
    )


def has_field_response_since_assign(row: dict) -> bool:
    """True if the ticket has a field response at or after ``last_assigned_at``."""
    assigned_at = _parse_ts(row.get("last_assigned_at"))
    responded_at = _parse_ts(row.get("responded_at"))
    if assigned_at and responded_at and responded_at >= assigned_at:
        return True
    return False


def should_close_as_unattended(row: dict, *, now: datetime | None = None) -> bool:
    """Pending with no same-day (or prior-day) field response after assign-day cutoff."""
    if str(row.get("status") or "").strip() != STATUS_DAILY_TASK:
        return False
    if has_field_response_since_assign(row):
        return False
    assigned_at = _parse_ts(row.get("last_assigned_at"))
    if not assigned_at:
        return False
    now = now or datetime.now(timezone.utc)
    assign_local = to_ops_local(assigned_at)
    now_local = to_ops_local(now)
    assign_date = assign_local.date()
    today = now_local.date()
    cutoff = assign_day_cutoff_time()
    if assign_date < today:
        return True
    if assign_date == today and now_local.time() >= cutoff:
        return True
    return False


def should_send_nudge(row: dict, *, now: datetime | None = None) -> bool:
    """Pending, no response, same assign day, past nudge delay, nudge not sent yet."""
    if str(row.get("status") or "").strip() != STATUS_DAILY_TASK:
        return False
    if has_field_response_since_assign(row):
        return False
    if row.get("unattended_nudge_sent_at"):
        return False
    assigned_at = _parse_ts(row.get("last_assigned_at"))
    if not assigned_at:
        return False
    now = now or datetime.now(timezone.utc)
    assign_local = to_ops_local(assigned_at)
    now_local = to_ops_local(now)
    if assign_local.date() != now_local.date():
        return False
    if should_close_as_unattended(row, now=now):
        return False
    return (now - assigned_at) >= timedelta(hours=UNATTENDED_NUDGE_HOURS)


def nudge_message(*, assigned_to: str, ticket_number: str, task_category: str) -> str:
    handle = assigned_to if str(assigned_to).startswith("@") else f"@{assigned_to}"
    cat = (task_category or "").strip() or "—"
    hours = (
        int(UNATTENDED_NUDGE_HOURS)
        if UNATTENDED_NUDGE_HOURS == int(UNATTENDED_NUDGE_HOURS)
        else UNATTENDED_NUDGE_HOURS
    )
    return (
        f"Reminder {handle}: ticket {ticket_number} ({cat}) — "
        f"no field response after {hours}h. "
        "Please swipe-reply to the assignment message with text and/or a photo."
    )


def _fetch_pending_tickets(client: Any, *, tickets_table: str) -> list[dict]:
    res = (
        client.table(tickets_table)
        .select(
            "ticket_number, assigned_to, task_category, status, "
            "last_assigned_at, responded_at, unattended_nudge_sent_at"
        )
        .eq("status", STATUS_DAILY_TASK)
        .limit(500)
        .execute()
    )
    return list(res.data or [])


async def run_unattended_nudges(
    client: Any,
    *,
    tickets_table: str,
    attendance_table: str,
    send_telegram: Any | None = None,
) -> dict[str, int]:
    """Send nudges for eligible Pending tickets. ``send_telegram`` is async ``(row) -> None``."""
    pending = _fetch_pending_tickets(client, tickets_table=tickets_table)
    now_iso = datetime.now(timezone.utc).isoformat()
    sent = 0
    skipped = 0
    for row in pending:
        if not str(row.get("assigned_to") or "").strip():
            skipped += 1
            continue
        if not should_send_nudge(row):
            skipped += 1
            continue
        ticket = str(row.get("ticket_number") or "")
        if not ticket:
            continue
        if send_telegram is not None:
            try:
                await send_telegram(row)
            except Exception:
                log.exception("nudge telegram failed for %s", ticket)
                continue
        try:
            client.table(tickets_table).update(
                {"unattended_nudge_sent_at": now_iso, "updated_at": now_iso}
            ).eq("ticket_number", ticket).execute()
            client.table(attendance_table).insert(
                {
                    "ticket_number": ticket,
                    "member_username": str(row.get("assigned_to") or "@system"),
                    "action_type": "Nudge",
                    "note": nudge_message(
                        assigned_to=str(row.get("assigned_to") or ""),
                        ticket_number=ticket,
                        task_category=str(row.get("task_category") or ""),
                    ),
                    "timestamp": now_iso,
                }
            ).execute()
        except Exception:
            log.exception("nudge db update failed for %s", ticket)
            continue
        sent += 1
    return {"sent": sent, "skipped": skipped, "scanned": len(pending)}


def run_unattended_close(
    client: Any,
    *,
    tickets_table: str,
    attendance_table: str,
) -> dict[str, int]:
    """Move eligible Pending tickets to ``Unattended``."""
    pending = _fetch_pending_tickets(client, tickets_table=tickets_table)
    now_iso = datetime.now(timezone.utc).isoformat()
    closed = 0
    for row in pending:
        if not should_close_as_unattended(row):
            continue
        ticket = str(row.get("ticket_number") or "")
        if not ticket:
            continue
        try:
            client.table(tickets_table).update(
                {
                    "status": STATUS_UNATTENDED,
                    "updated_at": now_iso,
                }
            ).eq("ticket_number", ticket).execute()
            client.table(attendance_table).insert(
                {
                    "ticket_number": ticket,
                    "member_username": "@system",
                    "action_type": "AutoUnattended",
                    "note": "No field response before assign-day cutoff; closed from Daily Task.",
                    "timestamp": now_iso,
                }
            ).execute()
            closed += 1
        except Exception:
            log.exception("auto-unattended close failed for %s", ticket)
    return {"closed": closed, "scanned": len(pending)}
