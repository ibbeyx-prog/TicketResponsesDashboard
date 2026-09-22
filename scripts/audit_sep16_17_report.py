#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", encoding="utf-8-sig")

OPS = timezone(timedelta(hours=5))


def bounds(d: date):
    s = datetime(d.year, d.month, d.day, tzinfo=OPS)
    return s, s + timedelta(days=1)


def parse(v):
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def in_day(ts, d: date) -> bool:
    if not ts:
        return False
    a, b = bounds(d)
    return a <= ts.astimezone(OPS) < b


def main() -> None:
    import os
    from supabase import create_client

    c = create_client(os.getenv("SUPABASE_URL", "").rstrip("/"), os.getenv("SUPABASE_KEY", ""))
    T, L = "tickets_active", "ticket_attendance_logs"
    d16, d17 = date(2026, 9, 16), date(2026, 9, 17)

    tickets = c.table(T).select("*").execute().data or []
    logs16 = (
        c.table(L)
        .select("*")
        .gte("timestamp", bounds(d16)[0].astimezone(timezone.utc).isoformat())
        .lt("timestamp", bounds(d16)[1].astimezone(timezone.utc).isoformat())
        .execute()
    ).data or []
    logs17 = (
        c.table(L)
        .select("*")
        .gte("timestamp", bounds(d17)[0].astimezone(timezone.utc).isoformat())
        .lt("timestamp", bounds(d17)[1].astimezone(timezone.utc).isoformat())
        .execute()
    ).data or []

    print("=== Sep 16 action types ===")
    print(Counter(x["action_type"] for x in logs16))
    print("=== Sep 17 action types (top) ===")
    print(Counter(x["action_type"] for x in logs17).most_common(15))

    marked16 = [r for r in tickets if in_day(parse(r.get("marked_unattended_at")), d16)]
    print(f"\nmarked_unattended_at on Sep 16: {len(marked16)}")
    for r in marked16:
        fr = str(r.get("field_response") or "")[:50]
        print(
            f"  {r['ticket_number']}  status={r.get('status')}  assignee={r.get('assigned_to')}  fr={fr!r}"
        )

    still_daily = [
        r
        for r in tickets
        if str(r.get("status")) in ("Daily Task", "Pending")
        and in_day(parse(r.get("last_assigned_at")), d16)
    ]
    print(f"\nStill Daily Task with assign on Sep 16: {len(still_daily)}")
    for r in still_daily:
        print(f"  {r['ticket_number']}  {r.get('assigned_to')}  marked={r.get('marked_unattended_at')}")

    open17 = [
        r
        for r in tickets
        if str(r.get("status")) == "Open"
        and in_day(parse(r.get("last_assigned_at")), d17)
    ]
    print(f"\nOpen + last_assigned Sep 17 (likely missing Daily Task): {len(open17)}")
    by_num = {str(r["ticket_number"]): r for r in tickets}
    for r in open17:
        tn = str(r["ticket_number"])
        la = parse(r.get("last_assigned_at"))
        ra = parse(r.get("responded_at"))
        after = bool(ra and la and ra >= la)
        qlogs = [
            x
            for x in logs17
            if str(x.get("ticket_number")) == tn
            and str(x.get("action_type"))
            in ("TicketQueued", "Assigned", "Reassigned", "ReassignedFromOpen")
        ]
        print(
            f"  {tn}  assignee={r.get('assigned_to')}  "
            f"responded_at>=assign={after}  field_response={bool(r.get('field_response'))}  "
            f"queue_logs={len(qlogs)}"
        )

    print("\nSep 17 Response logs (ticket status):")
    for x in logs17:
        if x.get("action_type") != "Response":
            continue
        tn = str(x.get("ticket_number"))
        row = by_num.get(tn, {})
        print(f"  {tn}  status={row.get('status')}  note={(x.get('note') or '')[:60]!r}")


if __name__ == "__main__":
    main()
