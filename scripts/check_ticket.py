#!/usr/bin/env python3
"""Quick ticket lookup: py -3 scripts/check_ticket.py <ticket_number>"""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

from supabase import create_client  # noqa: E402

import os

def main() -> int:
    tid = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
    if not tid:
        print("Usage: check_ticket.py <ticket_number>")
        return 1
    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = os.getenv("SUPABASE_KEY") or ""
    tbl = os.getenv("TICKETS_TABLE", "tickets_active")
    logs_tbl = os.getenv("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")
    c = create_client(url, key)
    rows = c.table(tbl).select("*").eq("ticket_number", tid).limit(1).execute().data or []
    print(f"=== {tbl} / {tid} ===")
    if not rows:
        print("NOT FOUND")
    else:
        for k, v in sorted(rows[0].items()):
            print(f"  {k}: {v}")
    log_rows = (
        c.table(logs_tbl)
        .select("timestamp,action_type,member_username,note,photo_url")
        .eq("ticket_number", tid)
        .order("timestamp", desc=True)
        .limit(15)
        .execute()
        .data
        or []
    )
    print(f"=== {logs_tbl} ({len(log_rows)} rows) ===")
    for L in log_rows:
        note = (L.get("note") or "")[:80]
        print(
            f"  {L.get('timestamp')}  {L.get('action_type')}  "
            f"{L.get('member_username')}  note={note!r}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
