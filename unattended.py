"""Unattended assignment workflow — 6h nudge and end-of-assign-day auto-close."""

from __future__ import annotations

import logging
import os
from datetime import datetime, time, timedelta, timezone
from typing import Any

log = logging.getLogger("unattended")

STATUS_UNATTENDED = "Unattended"
STATUS_NEEDS_REVIEW = "Open"
STATUS_DAILY_TASK = "Daily Task"
# Legacy rows may still say Pending before migration 20260626.
DAILY_TASK_STATUSES: tuple[str, ...] = (STATUS_DAILY_TASK, "Pending")


def is_daily_task_status(status: object) -> bool:
    return str(status or "").strip() in DAILY_TASK_STATUSES


UNATTENDED_NUDGE_HOURS = float(os.getenv("UNATTENDED_NUDGE_HOURS", "6"))
UNATTENDED_POLL_MINUTES = float(os.getenv("UNATTENDED_POLL_MINUTES", "15"))
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
    if not is_daily_task_status(row.get("status")):
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
    if not is_daily_task_status(row.get("status")):
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


def should_show_dashboard_cutoff_warning(row: dict, *, now: datetime | None = None) -> bool:
    """Dashboard banner: Daily Task, no response since assign, same assign day, past nudge lead time."""
    if not is_daily_task_status(row.get("status")):
        return False
    if has_field_response_since_assign(row):
        return False
    if should_close_as_unattended(row, now=now):
        return False
    assigned_at = _parse_ts(row.get("last_assigned_at"))
    if not assigned_at:
        return False
    now = now or datetime.now(timezone.utc)
    assign_local = to_ops_local(assigned_at)
    now_local = to_ops_local(now)
    if assign_local.date() != now_local.date():
        return False
    hours = (now - assigned_at).total_seconds() / 3600
    return hours >= max(0.0, UNATTENDED_NUDGE_HOURS - 0.5)


def nudge_message(*, assigned_to: str, ticket_number: str, task_category: str) -> str:
    handle = assigned_to if str(assigned_to).startswith("@") else f"@{assigned_to}"
    cat = (task_category or "").strip() or "—"
    return (
        f"Reminder {handle}: ticket {ticket_number} ({cat}) — "
        "no field response, please update."
    )


def _fetch_daily_task_tickets(
    client: Any,
    *,
    tickets_table: str,
    nudge_not_sent: bool = False,
    not_marked_unattended: bool = False,
) -> list[dict]:
    """Daily Task rows for unattended cron (optional idempotency filters)."""
    q = (
        client.table(tickets_table)
        .select(
            "ticket_number, assigned_to, task_category, status, "
            "last_assigned_at, responded_at, unattended_nudge_sent_at, "
            "marked_unattended_at"
        )
        .in_("status", list(DAILY_TASK_STATUSES))
    )
    if nudge_not_sent:
        q = q.is_("unattended_nudge_sent_at", "null")
    if not_marked_unattended:
        q = q.is_("marked_unattended_at", "null")
    res = q.limit(500).execute()
    return list(res.data or [])


async def run_unattended_nudges(
    client: Any,
    *,
    tickets_table: str,
    attendance_table: str,
    send_telegram: Any | None = None,
) -> dict[str, int]:
    """Send nudges for eligible Pending tickets.

    ``send_telegram`` is async ``(row) -> None`` or
    ``(row) -> (chat_id, message_id) | None`` so the nudge Telegram message
    can be stored for field-reply capture.
    """
    pending = _fetch_daily_task_tickets(
        client, tickets_table=tickets_table, nudge_not_sent=True
    )
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
        tg_ref: tuple[int, int] | None = None
        if send_telegram is not None:
            try:
                result = await send_telegram(row)
                if (
                    isinstance(result, tuple)
                    and len(result) == 2
                    and result[0] is not None
                    and result[1] is not None
                ):
                    tg_ref = (int(result[0]), int(result[1]))
            except Exception:
                log.exception("nudge telegram failed for %s", ticket)
                continue
        payload: dict[str, object] = {
            "unattended_nudge_sent_at": now_iso,
            "updated_at": now_iso,
        }
        if tg_ref is not None:
            payload["nudge_telegram_chat_id"] = tg_ref[0]
            payload["nudge_telegram_message_id"] = tg_ref[1]
        try:
            client.table(tickets_table).update(payload).eq(
                "ticket_number", ticket
            ).execute()
        except Exception as exc:
            # Pre-migration DBs may lack nudge_telegram_* — retry without them.
            msg = str(exc).lower()
            if tg_ref is not None and (
                "nudge_telegram_" in msg or "pgrst204" in msg or "42703" in msg
            ):
                payload.pop("nudge_telegram_chat_id", None)
                payload.pop("nudge_telegram_message_id", None)
                try:
                    client.table(tickets_table).update(payload).eq(
                        "ticket_number", ticket
                    ).execute()
                except Exception:
                    log.exception("nudge db update failed for %s", ticket)
                    continue
            else:
                log.exception("nudge db update failed for %s", ticket)
                continue
        try:
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
            log.exception("nudge attendance log failed for %s", ticket)
            # Ticket already marked nudged; count as sent.
        sent += 1
    return {"sent": sent, "skipped": skipped, "scanned": len(pending)}


TICKET_VISITS_TABLE = (
    os.getenv("TICKET_VISITS_TABLE") or "public.ticket_visits"
).strip()


def _close_open_visits_unattended(
    client: Any,
    *,
    visits_table: str,
    ticket_number: str,
    visit_end: str,
) -> None:
    """Close active visit rows as unattended when auto-closing a ticket."""
    tn = str(ticket_number).strip()
    payload = {
        "visit_end": visit_end,
        "outcome": "unattended",
        "closed_by": "system",
        "is_active": False,
    }
    for filt in (
        lambda q: q.eq("ticket_number", tn).eq("is_active", True),
        lambda q: q.eq("ticket_number", tn).is_("visit_end", "null"),
    ):
        try:
            filt(client.table(visits_table).update(payload)).execute()
        except Exception:
            log.exception("visit unattended close failed for %s", tn)


def run_unattended_close(
    client: Any,
    *,
    tickets_table: str,
    attendance_table: str,
    visits_table: str | None = None,
) -> dict[str, int]:
    """Mark eligible Daily Task tickets as unattended and move to **Needs Review** (Open).

    Sets ``marked_unattended_at`` once (permanent metric). Status becomes Open for admin.
    """
    pending = _fetch_daily_task_tickets(client, tickets_table=tickets_table)
    now_iso = datetime.now(timezone.utc).isoformat()
    visits_tbl = (visits_table or TICKET_VISITS_TABLE).strip()
    closed = 0
    for row in pending:
        if not should_close_as_unattended(row):
            continue
        ticket = str(row.get("ticket_number") or "")
        if not ticket:
            continue
        try:
            payload: dict[str, object] = {
                "status": STATUS_NEEDS_REVIEW,
                "updated_at": now_iso,
            }
            if not row.get("marked_unattended_at"):
                payload["marked_unattended_at"] = now_iso
            client.table(tickets_table).update(payload).eq("ticket_number", ticket).execute()
            _close_open_visits_unattended(
                client,
                visits_table=visits_tbl,
                ticket_number=ticket,
                visit_end=now_iso,
            )
            client.table(attendance_table).insert(
                {
                    "ticket_number": ticket,
                    "member_username": "@system",
                    "action_type": "AutoUnattended",
                    "note": (
                        "No field response before assign-day cutoff; "
                        "marked unattended and moved to Needs Review."
                    ),
                    "timestamp": now_iso,
                }
            ).execute()
            closed += 1
        except Exception:
            log.exception("auto-unattended close failed for %s", ticket)
    return {"closed": closed, "scanned": len(pending)}
