#!/usr/bin/env python3
"""Repair Sep 2026 bot-ingest gap: false unattended + missing Daily Task queue.

Sep 16: zero Telegram ``Response`` logs — auto-unattended at assign-day cutoff treated
tickets as no-reply. Resets tickets that still have no field response back to
**Daily Task** and clears ``marked_unattended_at`` from the Sep 16 19:02 UTC batch.

Sep 17: assignments that received field responses are correctly **Open** (Needs Review);
only clears stale ``marked_unattended_at`` when ``responded_at >= last_assigned_at``.

Usage:
  py -3 scripts/repair_bot_outage_sep2026.py           # report only
  py -3 scripts/repair_bot_outage_sep2026.py --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", encoding="utf-8-sig")

# AutoUnattended batch end of Maldives assign-day 2026-09-16 (19:02 UTC)
SEP16_CUTOFF_BATCH = "2026-09-16T19:02:10.827882+00:00"

# Tickets closed in that batch with no field response (audit 2026-09-17)
SEP16_RESET_TO_DAILY = [
    "2021120572000295",
    "2021040672000174",
    "2020062572000216",
    "100750949",
    "100737692",
    "2022051672000401",
]


def _parse_ts(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    import os
    from supabase import create_client

    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = os.getenv("SUPABASE_KEY") or ""
    tickets_tbl = os.getenv("TICKETS_TABLE", "tickets_active")
    visits_tbl = os.getenv("TICKET_VISITS_TABLE", "ticket_visits")
    logs_tbl = os.getenv("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")

    if not url or not key:
        print("SUPABASE_URL / SUPABASE_KEY required", file=sys.stderr)
        return 1

    client = create_client(url, key)
    now_iso = datetime.now(timezone.utc).isoformat()

    print("=== Sep 16 bot gap (no Response logs on 2026-09-16) ===\n")
    logs16 = (
        client.table(logs_tbl)
        .select("action_type")
        .gte("timestamp", "2026-09-15T19:00:00+00:00")
        .lt("timestamp", "2026-09-16T19:00:00+00:00")
        .execute()
    ).data or []
    resp16 = sum(1 for x in logs16 if x.get("action_type") == "Response")
    print(f"Response logs on Maldives calendar 2026-09-16: {resp16}")

    print("\n--- Reset candidates (no field_response, Sep 16 assign-day unattended) ---")
    for tn in SEP16_RESET_TO_DAILY:
        row = (
            client.table(tickets_tbl).select("*").eq("ticket_number", tn).limit(1).execute()
        ).data
        row = row[0] if row else None
        if not row:
            print(f"  {tn}: NOT FOUND")
            continue
        fr = str(row.get("field_response") or "").strip()
        print(
            f"  {tn}: status={row.get('status')} assignee={row.get('assigned_to')} "
            f"fr={'yes' if fr else 'no'} marked={row.get('marked_unattended_at')}"
        )

    print("\n--- Sep 17: stale marked_unattended with valid response ---")
    sep17_clear: list[str] = []
    tickets = (
        client.table(tickets_tbl)
        .select("ticket_number,status,field_response,responded_at,last_assigned_at,marked_unattended_at")
        .gte("last_assigned_at", "2026-09-17T00:00:00+00:00")
        .lt("last_assigned_at", "2026-09-18T00:00:00+00:00")
        .execute()
    ).data or []
    for row in tickets:
        tn = str(row.get("ticket_number"))
        la = _parse_ts(row.get("last_assigned_at"))
        ra = _parse_ts(row.get("responded_at"))
        fr = str(row.get("field_response") or "").strip()
        marked = row.get("marked_unattended_at")
        if fr and la and ra and ra >= la and marked:
            sep17_clear.append(tn)
            print(f"  {tn}: clear marked (has response after Sep 17 assign)")

    print("\n--- Sep 17 Daily Task queue ---")
    daily17 = [r for r in tickets if str(r.get("status")) in ("Daily Task", "Pending")]
    print(f"  Tickets with last_assigned on 2026-09-17 still Daily Task: {len(daily17)}")
    for row in daily17:
        print(f"    {row.get('ticket_number')} {row.get('assigned_to')}")

    open17 = [r for r in tickets if str(r.get("status")) == "Open" and str(r.get("field_response") or "").strip()]
    print(f"  Assigned Sep 17 then field reply (Needs Review / Open): {len(open17)} (expected, not a bug)")

    if not args.apply:
        print("\nDry run. Use --apply to write fixes.")
        return 0

    fixed = 0
    for tn in SEP16_RESET_TO_DAILY:
        row = (
            client.table(tickets_tbl).select("*").eq("ticket_number", tn).limit(1).execute()
        ).data
        if not row:
            continue
        row = row[0]
        if str(row.get("field_response") or "").strip():
            print(f"  SKIP {tn}: already has field_response")
            continue
        assignee = str(row.get("assigned_to") or "").strip()
        client.table(tickets_tbl).update(
            {
                "status": "Daily Task",
                "marked_unattended_at": None,
                "updated_at": now_iso,
            }
        ).eq("ticket_number", tn).execute()
        # Re-open visit row for performance metrics (best-effort)
        try:
            eng = assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"
            close_payload = {
                "visit_end": now_iso,
                "outcome": "reassigned",
                "closed_by": "dashboard-admin",
                "is_active": False,
            }
            client.table(visits_tbl).update(close_payload).eq(
                "ticket_number", tn
            ).eq("outcome", "unattended").is_("visit_end", "null").execute()
            client.table(visits_tbl).insert(
                {
                    "ticket_number": tn,
                    "assignee": eng,
                    "visit_start": row.get("last_assigned_at") or now_iso,
                    "outcome": "assigned",
                    "is_active": True,
                }
            ).execute()
        except Exception as exc:
            print(f"  WARN visit repair {tn}: {exc}")
        client.table(logs_tbl).insert(
            {
                "ticket_number": tn,
                "member_username": "@dashboard-admin",
                "action_type": "StatusChange",
                "note": (
                    "Bot outage 2026-09-16: cleared false unattended, "
                    "restored Daily Task for field work (no Response ingested that day)."
                ),
                "timestamp": now_iso,
            }
        ).execute()
        print(f"  FIXED {tn} -> Daily Task, cleared marked_unattended_at")
        fixed += 1

    # 100752531: already Daily Task; only clear Sep 16 batch flag
    for tn in ("100752531",):
        client.table(tickets_tbl).update(
            {"marked_unattended_at": None, "updated_at": now_iso}
        ).eq("ticket_number", tn).execute()
        print(f"  FIXED {tn}: cleared marked_unattended_at (already Daily Task)")

    for tn in sep17_clear:
        client.table(tickets_tbl).update(
            {"marked_unattended_at": None, "updated_at": now_iso}
        ).eq("ticket_number", tn).execute()
        print(f"  FIXED {tn}: cleared stale marked_unattended_at after field response")

    print(f"\nDone. Sep16 daily-task resets: {fixed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
