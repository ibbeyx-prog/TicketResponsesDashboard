#!/usr/bin/env python3
"""Audit (and optionally repair) tickets affected by bot ingest gaps.

Usage:
  py -3 scripts/audit_bot_gap_dates.py --date 2026-09-16
  py -3 scripts/audit_bot_gap_dates.py --date 2026-09-17 --focus daily-task
  py -3 scripts/audit_bot_gap_dates.py --date 2026-09-16 --apply
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", encoding="utf-8-sig")

OPS_TZ = timezone(timedelta(hours=5))


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=OPS_TZ)
    end = start + timedelta(days=1)
    return start, end


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


def _in_range(ts: datetime | None, start: datetime, end: datetime) -> bool:
    if ts is None:
        return False
    return start <= ts.astimezone(OPS_TZ) < end


def _response_text_from_note(note: str) -> str:
    note = (note or "").strip()
    if not note:
        return ""
    m = re.match(r"^Responded by [^:]+:\s*(.*)$", note, re.I | re.S)
    if m:
        return m.group(1).strip()
    return note


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (Maldives UTC+5 calendar day)")
    parser.add_argument(
        "--focus",
        choices=("all", "field-response", "daily-task"),
        default="all",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply safe repairs (field response from logs; daily task from TicketQueued)",
    )
    args = parser.parse_args()

    import os
    from supabase import create_client

    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = os.getenv("SUPABASE_KEY") or ""
    tickets_tbl = os.getenv("TICKETS_TABLE", "tickets_active")
    logs_tbl = os.getenv("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")
    visits_tbl = os.getenv("TICKET_VISITS_TABLE", "ticket_visits")

    if not url or not key:
        print("SUPABASE_URL and SUPABASE_KEY required", file=sys.stderr)
        return 1

    d = date.fromisoformat(args.date)
    start, end = _day_bounds(d)
    print(f"=== Audit {d.isoformat()} ({start.isoformat()} – {end.isoformat()}) ===\n")

    client = create_client(url, key)

    logs = (
        client.table(logs_tbl)
        .select("ticket_number,member_username,action_type,timestamp,note")
        .gte("timestamp", start.astimezone(timezone.utc).isoformat())
        .lt("timestamp", end.astimezone(timezone.utc).isoformat())
        .order("timestamp")
        .execute()
    ).data or []

    tickets = (
        client.table(tickets_tbl).select("*").execute()
    ).data or []
    by_ticket = {str(r.get("ticket_number") or "").strip(): r for r in tickets}

    visits = (
        client.table(visits_tbl)
        .select("ticket_number,assignee,outcome,visit_start,visit_end,is_active,response_note")
        .execute()
    ).data or []

    day_logs = [L for L in logs if _in_range(_parse_ts(L.get("timestamp")), start, end)]
    responses = [L for L in day_logs if str(L.get("action_type")) == "Response"]
    queued = [L for L in day_logs if str(L.get("action_type")) == "TicketQueued"]

    print(f"Logs on day: {len(day_logs)} (Response={len(responses)}, TicketQueued={len(queued)})")

    field_issues: list[dict] = []
    if args.focus in ("all", "field-response"):
        for L in responses:
            tn = str(L.get("ticket_number") or "").strip()
            if not tn:
                continue
            row = by_ticket.get(tn)
            if not row:
                field_issues.append({"ticket": tn, "issue": "response_log_but_no_ticket_row", "log": L})
                continue
            assigned_at = _parse_ts(row.get("last_assigned_at"))
            resp_log_at = _parse_ts(L.get("timestamp"))
            status = str(row.get("status") or "")
            fr = str(row.get("field_response") or "").strip()
            responded_at = _parse_ts(row.get("responded_at"))
            marked = row.get("marked_unattended_at")

            after_assign = (
                assigned_at is None
                or resp_log_at is None
                or resp_log_at >= assigned_at
            )
            if not after_assign:
                continue

            needs_fix = False
            reasons: list[str] = []
            if status in ("Daily Task", "Pending"):
                needs_fix = True
                reasons.append("still_daily_task")
            if marked and after_assign:
                needs_fix = True
                reasons.append("marked_unattended_despite_response_log")
            if status == "Unattended":
                needs_fix = True
                reasons.append("status_unattended")
            if after_assign and not fr:
                needs_fix = True
                reasons.append("missing_field_response")
            if after_assign and responded_at and assigned_at and responded_at < assigned_at:
                needs_fix = True
                reasons.append("stale_responded_at")

            visit_bad = [
                v
                for v in visits
                if str(v.get("ticket_number")) == tn
                and str(v.get("outcome") or "") == "unattended"
                and _in_range(_parse_ts(v.get("visit_start")), start - timedelta(days=2), end + timedelta(days=1))
            ]
            if visit_bad and after_assign:
                needs_fix = True
                reasons.append(f"visit_unattended({len(visit_bad)})")

            if needs_fix:
                field_issues.append(
                    {
                        "ticket": tn,
                        "status": status,
                        "assigned_to": row.get("assigned_to"),
                        "reasons": reasons,
                        "log_note": (L.get("note") or "")[:120],
                        "log_at": L.get("timestamp"),
                        "member": L.get("member_username"),
                    }
                )

        # Unattended closed on this day without any response log after assign
        for row in tickets:
            tn = str(row.get("ticket_number") or "").strip()
            if not tn:
                continue
            mu = _parse_ts(row.get("marked_unattended_at"))
            if not _in_range(mu, start, end):
                continue
            has_resp = any(
                str(r.get("ticket_number")) == tn for r in responses
            )
            if has_resp:
                continue
            if str(row.get("status") or "") in ("Daily Task", "Pending"):
                field_issues.append(
                    {
                        "ticket": tn,
                        "status": row.get("status"),
                        "issue": "marked_unattended_day_but_still_daily_task",
                    }
                )

    daily_issues: list[dict] = []
    if args.focus in ("all", "daily-task"):
        for L in queued:
            tn = str(L.get("ticket_number") or "").strip()
            if not tn:
                continue
            row = by_ticket.get(tn)
            if not row:
                daily_issues.append({"ticket": tn, "issue": "queued_log_missing_ticket"})
                continue
            status = str(row.get("status") or "")
            la = _parse_ts(row.get("last_assigned_at"))
            log_at = _parse_ts(L.get("timestamp"))
            if status not in ("Daily Task", "Pending"):
                daily_issues.append(
                    {
                        "ticket": tn,
                        "issue": "queued_but_not_daily_task",
                        "status": status,
                        "last_assigned_at": row.get("last_assigned_at"),
                        "log_at": L.get("timestamp"),
                    }
                )
            elif log_at and la and abs((log_at - la).total_seconds()) > 3600:
                daily_issues.append(
                    {
                        "ticket": tn,
                        "issue": "daily_task_assign_time_mismatch",
                        "last_assigned_at": row.get("last_assigned_at"),
                        "log_at": L.get("timestamp"),
                    }
                )

        # Assigned in range but not showing as daily task (by last_assigned_at)
        for row in tickets:
            tn = str(row.get("ticket_number") or "").strip()
            if not tn:
                continue
            la = _parse_ts(row.get("last_assigned_at"))
            if not _in_range(la, start, end):
                continue
            status = str(row.get("status") or "")
            if status not in ("Daily Task", "Pending"):
                if not any(str(q.get("ticket_number")) == tn for q in queued):
                    daily_issues.append(
                        {
                            "ticket": tn,
                            "issue": "assigned_today_wrong_status_no_queue_log",
                            "status": status,
                            "last_assigned_at": row.get("last_assigned_at"),
                        }
                    )

    print("\n--- Field response / unattended issues ---")
    if not field_issues:
        print("(none flagged)")
    else:
        seen = set()
        for item in field_issues:
            tn = item["ticket"]
            if tn in seen:
                continue
            seen.add(tn)
            print(f"  {tn}: {item}")

    print("\n--- Daily Task visibility issues ---")
    if not daily_issues:
        print("(none flagged)")
    else:
        for item in daily_issues:
            print(f"  {item['ticket']}: {item}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to repair where possible.")
        return 0

    if field_issues:
        from bot import _db_complete_ticket_field_response, _field_responded_by_value

        fixed = 0
        for item in field_issues:
            tn = item.get("ticket")
            if not tn or item.get("issue") == "response_log_but_no_ticket_row":
                continue
            reasons = item.get("reasons") or []
            if not reasons and item.get("issue") != "marked_unattended_day_but_still_daily_task":
                continue
            note = item.get("log_note") or ""
            text = _response_text_from_note(note)
            if not text:
                print(f"  SKIP {tn}: no text in response log")
                continue
            row = by_ticket.get(tn) or {}
            assignee = str(row.get("assigned_to") or item.get("member") or "")
            handle = assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"
            member = str(item.get("member") or handle)
            if not member.startswith("@"):
                member = f"@{member.lstrip('@')}"
            frb = _field_responded_by_value(
                assigned_to=assignee,
                replier_label=member.lstrip("@"),
            )
            print(f"  FIX field response: {tn} -> {text[:60]!r}")
            _db_complete_ticket_field_response(
                tn,
                field_response=text,
                update_photo_url=False,
                responder_username=member,
                field_responded_by=frb or member,
            )
            # Clear erroneous unattended flag when we had a same-day response
            if row.get("marked_unattended_at"):
                client.table(tickets_tbl).update(
                    {"marked_unattended_at": None}
                ).eq("ticket_number", tn).execute()
            fixed += 1
        print(f"Field response repairs attempted: {fixed}")

    if daily_issues:
        repaired = 0
        for item in daily_issues:
            tn = item.get("ticket")
            issue = item.get("issue")
            if issue not in (
                "queued_but_not_daily_task",
                "assigned_today_wrong_status_no_queue_log",
            ):
                continue
            row = by_ticket.get(tn) or {}
            log_at = item.get("log_at")
            la = log_at or item.get("last_assigned_at") or row.get("last_assigned_at")
            payload = {
                "status": "Daily Task",
                "last_assigned_at": la,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            print(f"  FIX daily task: {tn} status -> Daily Task (last_assigned_at={la})")
            client.table(tickets_tbl).update(payload).eq("ticket_number", tn).execute()
            repaired += 1
        print(f"Daily task repairs attempted: {repaired}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
