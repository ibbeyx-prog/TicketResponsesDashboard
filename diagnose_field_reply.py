#!/usr/bin/env python3
"""Check why field replies may not show on the dashboard.

Run from project root::

    py -3 diagnose_field_reply.py

Reads ``.env`` (same as bot + dashboard) and prints:
- Which ``TICKETS_TABLE`` is configured
- Recent tickets by status
- Recent ``Response`` rows in attendance logs
- Tickets still ``Pending`` but with a newer Response log (bot/DB mismatch)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
TICKETS_TABLE = (os.getenv("TICKETS_TABLE") or "tickets_active").strip()
LOGS_TABLE = (os.getenv("ATTENDANCE_LOGS_TABLE") or "ticket_attendance_logs").strip()


def main() -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY required in .env", file=sys.stderr)
        return 1

    print("=== Field reply / dashboard diagnostic ===\n")
    print(f"TICKETS_TABLE={TICKETS_TABLE}")
    print(f"ATTENDANCE_LOGS_TABLE={LOGS_TABLE}")
    print(f"SUPABASE_URL={SUPABASE_URL[:48]}...")
    bridge = (os.getenv("TELEGRAM_GROUP_REPLY_BRIDGE") or "").strip()
    print(f"TELEGRAM_GROUP_REPLY_BRIDGE={bridge or '(not set)'}")
    print(
        "\nReminder: after a field reply the ticket moves Pending -> Open. "
        "On the dashboard use the **Open** queue, not Pending.\n"
    )

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    try:
        tickets = (
            client.table(TICKETS_TABLE)
            .select(
                "ticket_number,status,assigned_to,field_response,responded_at,updated_at,last_assigned_at"
            )
            .order("updated_at", desc=True)
            .limit(25)
            .execute()
        ).data or []
    except Exception as exc:
        print(f"ERROR reading {TICKETS_TABLE}: {exc}", file=sys.stderr)
        return 1

    if not tickets:
        print(f"No rows in {TICKETS_TABLE}.")
        return 0

    by_status: dict[str, int] = {}
    for row in tickets:
        st = str(row.get("status") or "?")
        by_status[st] = by_status.get(st, 0) + 1
    print("Recent tickets (top 25 by updated_at) — status counts:")
    for st, n in sorted(by_status.items()):
        print(f"  {st}: {n}")

    print("\nLatest rows:")
    for row in tickets[:8]:
        print(
            f"  {row.get('ticket_number')}  status={row.get('status')}  "
            f"assigned_to={row.get('assigned_to')}  "
            f"responded_at={row.get('responded_at')}"
        )

    try:
        logs = (
            client.table(LOGS_TABLE)
            .select("ticket_number,member_username,action_type,timestamp,note")
            .eq("action_type", "Response")
            .order("timestamp", desc=True)
            .limit(15)
            .execute()
        ).data or []
    except Exception as exc:
        print(f"\nWARN: could not read {LOGS_TABLE}: {exc}")
        logs = []

    if logs:
        print(f"\nRecent Response logs ({LOGS_TABLE}):")
        for row in logs[:8]:
            note = (row.get("note") or "")[:60]
            print(
                f"  {row.get('timestamp')}  ticket={row.get('ticket_number')}  "
                f"@{row.get('member_username')}  note={note!r}"
            )

    ticket_status = {str(r["ticket_number"]): str(r.get("status") or "") for r in tickets}
    mismatches = []
    for row in logs:
        tid = str(row.get("ticket_number") or "")
        if tid and ticket_status.get(tid) == "Pending":
            mismatches.append(tid)

    if mismatches:
        print(
            "\n*** MISMATCH: Response logged but ticket still Pending "
            f"(bot update may have failed): {', '.join(dict.fromkeys(mismatches))}"
        )
        print("    Check Railway bot logs for 'ticket field completion update failed'.")
        print("    Ensure RLS allows UPDATE on tickets for the bot's Supabase key.")
    else:
        print("\nNo obvious Pending+Response mismatch in recent data.")

    open_count = sum(1 for r in tickets if str(r.get("status")) == "Open")
    if open_count:
        print(f"\n{open_count} recent ticket(s) are Open — dashboard should show them under **Open**, not Pending.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
