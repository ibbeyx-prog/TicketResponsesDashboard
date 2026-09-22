"""Audit field Response capture vs Assignment for a UTC+5 calendar day."""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date, datetime, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

import pandas as pd

import app as a  # noqa: E402


def day_bounds(d: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    rs = pd.Timestamp(datetime.combine(d, time.min, tzinfo=a.LOCAL_TZ)).tz_convert("UTC")
    re = pd.Timestamp(
        datetime.combine(d, time(23, 59, 59, 999999), tzinfo=a.LOCAL_TZ)
    ).tz_convert("UTC")
    return rs, re


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default="2026-09-21")
    args = p.parse_args()
    target = date.fromisoformat(args.date)
    rs, re = day_bounds(target)
    # Include next morning through noon UTC+5 (late replies / timezone)
    next_day = pd.Timestamp(
        datetime.combine(
            date.fromordinal(target.toordinal() + 1),
            time(12, 0, 0),
            tzinfo=a.LOCAL_TZ,
        )
    ).tz_convert("UTC")

    client = a._get_supabase_client()
    logs = (
        client.table(a.ATTENDANCE_LOGS_TABLE)
        .select("timestamp,action_type,ticket_number,member_username,note")
        .gte("timestamp", rs.isoformat())
        .lte("timestamp", next_day.isoformat())
        .order("timestamp")
        .execute()
        .data
        or []
    )

    print(f"=== Attendance logs {target} .. next day noon ({a.LOCAL_TZ_LABEL}) ===")
    by_type = Counter(x["action_type"] for x in logs)
    for k, v in sorted(by_type.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {k}: {v}")

    assigns = [x for x in logs if x.get("action_type") == "Assignment"]
    responses = [x for x in logs if x.get("action_type") == "Response"]
    auto = [x for x in logs if x.get("action_type") == "AutoUnattended"]

    day_assigns = [x for x in assigns if rs.isoformat() <= x["timestamp"] <= re.isoformat()]
    day_responses = [x for x in responses if rs.isoformat() <= x["timestamp"] <= re.isoformat()]
    morning_after = [
        x
        for x in responses
        if re.isoformat() < x["timestamp"] <= next_day.isoformat()
    ]

    print(f"\nAssignment on {target}: {len(day_assigns)}")
    print(f"Response on {target}: {len(day_responses)}")
    print(f"Response next morning (through noon): {len(morning_after)}")
    print(f"AutoUnattended in extended window: {len(auto)}")

    assign_tickets = {str(x.get("ticket_number") or "").strip() for x in day_assigns}
    assign_tickets.discard("")

    print("\n--- Per-ticket Sep21 assignment vs Response (any time in window) ---")
    resp_by_ticket = Counter(
        str(x.get("ticket_number") or "").strip()
        for x in responses
        if str(x.get("ticket_number") or "").strip()
    )
    for tn in sorted(assign_tickets):
        n_resp = resp_by_ticket.get(tn, 0)
        print(f"  {tn}: responses_in_window={n_resp}")

    df = a._fetch_tickets_cached()
    if not df.empty and "last_assigned_at" in df.columns:
        la = a._parse_ts(df["last_assigned_at"])
        mask = la.notna() & (la >= rs) & (la <= re)
        sub = df.loc[mask]
        print(f"\n--- tickets_active last_assigned on {target}: {len(sub)} ---")
        for _, row in sub.iterrows():
            tn = str(row.get("ticket_number") or "")
            has = a._ticket_row_has_field_response(row)
            print(
                f"  {tn} status={row.get('status')} has_field_row={has} "
                f"responded_at={row.get('responded_at')} "
                f"tg_msg={row.get('assignment_telegram_message_id')}"
            )


if __name__ == "__main__":
    main()
