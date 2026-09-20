#!/usr/bin/env python3
"""Ground-truth assignment counts for a calendar day (UTC+5)."""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", encoding="utf-8-sig")

OPS = timezone(timedelta(hours=5))


def norm_handle(h: str) -> str:
    h = (h or "").strip()
    if not h:
        return ""
    return h if h.startswith("@") else f"@{h}"


def main() -> int:
    import os
    from supabase import create_client

    d = date.fromisoformat(sys.argv[1] if len(sys.argv) > 1 else "2026-09-17")
    start = datetime(d.year, d.month, d.day, tzinfo=OPS)
    end = start + timedelta(days=1)
    start_u = start.astimezone(timezone.utc).isoformat()
    end_u = end.astimezone(timezone.utc).isoformat()

    c = create_client(os.getenv("SUPABASE_URL", "").rstrip("/"), os.getenv("SUPABASE_KEY", ""))
    tickets = c.table("tickets_active").select("ticket_number,assigned_to,last_assigned_at,status").execute().data or []
    logs = (
        c.table("ticket_attendance_logs")
        .select("ticket_number,member_username,action_type,timestamp")
        .gte("timestamp", start_u)
        .lt("timestamp", end_u)
        .execute()
    ).data or []
    visits = (
        c.table("ticket_visits")
        .select("ticket_number,assignee,visit_start")
        .gte("visit_start", start_u)
        .lt("visit_start", end_u)
        .execute()
    ).data or []

    la_counts: Counter[str] = Counter()
    for r in tickets:
        la = r.get("last_assigned_at")
        if not la:
            continue
        ts = datetime.fromisoformat(str(la).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if not (start <= ts.astimezone(OPS) < end):
            continue
        if str(r.get("status") or "").strip() != "Daily Task":
            continue
        eng = norm_handle(str(r.get("assigned_to") or ""))
        if eng:
            la_counts[eng.casefold()] += 1

    log_counts: Counter[str] = Counter()
    log_tickets: dict[str, set[str]] = defaultdict(set)
    for L in logs:
        if L.get("action_type") != "Assignment":
            continue
        eng = norm_handle(str(L.get("member_username") or ""))
        if eng:
            log_counts[eng.casefold()] += 1
            log_tickets[eng.casefold()].add(str(L.get("ticket_number")))

    visit_counts: Counter[str] = Counter()
    for v in visits:
        eng = norm_handle(str(v.get("assignee") or ""))
        if eng:
            visit_counts[eng.casefold()] += 1

    print(f"=== {d.isoformat()} (UTC+5) ===\n")
    print("By last_assigned_at (unique tickets):")
    for k, n in sorted(la_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {n}")
    print("\nBy assignment logs (Assignment only):")
    for k, n in sorted(log_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {n} tickets={len(log_tickets[k])}")
    print("\nBy ticket_visits.visit_start:")
    for k, n in sorted(visit_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
