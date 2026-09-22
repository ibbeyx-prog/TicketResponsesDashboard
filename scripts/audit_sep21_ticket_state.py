#!/usr/bin/env python3
"""Current DB state for tickets assigned on a UTC+5 calendar day."""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", encoding="utf-8-sig")
LOCAL = timezone(timedelta(hours=5))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default="2026-09-21")
    args = p.parse_args()
    target = date.fromisoformat(args.date)
    rs = datetime.combine(target, time.min, tzinfo=LOCAL).astimezone(timezone.utc)
    re = datetime.combine(target, time(23, 59, 59, 999999), tzinfo=LOCAL).astimezone(
        timezone.utc
    )
    next_noon = datetime.combine(
        date.fromordinal(target.toordinal() + 1), time(12, 0), tzinfo=LOCAL
    ).astimezone(timezone.utc)

    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = os.getenv("SUPABASE_KEY") or ""
    tickets_tbl = os.getenv("TICKETS_TABLE", "tickets_active")
    logs_tbl = os.getenv("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")
    if not url or not key:
        print("SUPABASE_URL / SUPABASE_KEY required", file=sys.stderr)
        return 1

    client = create_client(url, key)
    assign_logs = (
        client.table(logs_tbl)
        .select("ticket_number,timestamp,member_username")
        .eq("action_type", "Assignment")
        .gte("timestamp", rs.isoformat())
        .lte("timestamp", re.isoformat())
        .execute()
        .data
        or []
    )
    assign_tickets = sorted({str(x["ticket_number"]) for x in assign_logs if x.get("ticket_number")})
    print(f"=== Assignments on {target} (UTC+5): {len(assign_tickets)} ===\n")
    needs_repair: list[str] = []
    missing: list[str] = []
    for tn in assign_tickets:
        row = (
            client.table(tickets_tbl)
            .select(
                "ticket_number,status,assigned_to,field_response,responded_at,"
                "last_assigned_at,marked_unattended_at"
            )
            .eq("ticket_number", tn)
            .limit(1)
            .execute()
            .data
        )
        row = row[0] if row else None
        if not row:
            print(f"{tn}: NOT IN {tickets_tbl}")
            continue
        fr = str(row.get("field_response") or "").strip()
        resp_logs = (
            client.table(logs_tbl)
            .select("timestamp,note")
            .eq("action_type", "Response")
            .eq("ticket_number", tn)
            .gte("timestamp", rs.isoformat())
            .lte("timestamp", next_noon.isoformat())
            .execute()
            .data
            or []
        )
        status = str(row.get("status") or "")
        marked = row.get("marked_unattended_at")
        print(
            f"{tn} status={status} assignee={row.get('assigned_to')} "
            f"field_response={'yes' if fr else 'no'} responded_at={row.get('responded_at')} "
            f"marked_unattended={bool(marked)} response_logs={len(resp_logs)}"
        )
        if fr and status not in ("Open", "Resolved"):
            needs_repair.append(tn)
        if marked and fr:
            needs_repair.append(tn)
        if not fr and not resp_logs:
            missing.append(tn)

    print(f"\nHas response but wrong status/marked: {sorted(set(needs_repair))}")
    print(f"No field_response and no Response log (still missing): {sorted(set(missing))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
