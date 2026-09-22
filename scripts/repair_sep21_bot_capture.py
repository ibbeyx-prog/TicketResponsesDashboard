#!/usr/bin/env python3
"""Repair 2026-09-21 field replies after bot ingest outage.

When the Railway bot came back, Telegram delivered a backlog of swipe-replies.
Some tickets landed in Open with ``responded_at`` / Response logs but still show
``marked_unattended_at``, or photo-only replies missing ``photo_url`` / text.

This script, for tickets **assigned on the given UTC+5 day**, when a Response log
exists through the next morning:

- Clears ``marked_unattended_at``
- Sets ``status`` to ``Open`` (Needs Review)
- Syncs ``field_response`` from the latest Response log note (when empty)
- Syncs ``photo_url`` from the latest Response log with a photo
- Closes active visit rows as ``responded`` (best-effort via bot helper)

Tickets with no Response log are unchanged (still need a manual re-reply or
``recover_ticket_from_group.py``).

Usage:
  py -3 scripts/repair_sep21_bot_capture.py --date 2026-09-21
  py -3 scripts/repair_sep21_bot_capture.py --date 2026-09-21 --apply
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", encoding="utf-8-sig")
LOCAL = timezone(timedelta(hours=5))

_RESPONDED_BY_PREFIX = re.compile(r"^Responded by .+?:\s*", re.IGNORECASE)


def _note_to_field_text(note: object) -> str | None:
    n = str(note or "").strip()
    if not n:
        return None
    if n.lower().startswith("responded by ") and ": " not in n:
        return None
    n = _RESPONDED_BY_PREFIX.sub("", n).strip()
    return n or None


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
    parser.add_argument("--date", default="2026-09-21")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

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

    from supabase import create_client

    client = create_client(url, key)
    now_iso = datetime.now(timezone.utc).isoformat()

    assign_logs = (
        client.table(logs_tbl)
        .select("ticket_number")
        .eq("action_type", "Assignment")
        .gte("timestamp", rs.isoformat())
        .lte("timestamp", re.isoformat())
        .execute()
        .data
        or []
    )
    ticket_numbers = sorted(
        {str(x["ticket_number"]).strip() for x in assign_logs if x.get("ticket_number")}
    )
    print(f"=== Repair field capture for assignments on {target} ({len(ticket_numbers)} tickets) ===\n")

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from bot import _bot_visits_close_responded, _db_get_ticket

    fixed = 0
    skipped_no_response = 0
    for tn in ticket_numbers:
        resp_logs = (
            client.table(logs_tbl)
            .select("timestamp,note,photo_url,member_username")
            .eq("action_type", "Response")
            .eq("ticket_number", tn)
            .gte("timestamp", rs.isoformat())
            .lte("timestamp", next_noon.isoformat())
            .order("timestamp")
            .execute()
            .data
            or []
        )
        if not resp_logs:
            skipped_no_response += 1
            print(f"  SKIP {tn}: no Response log (not captured yet)")
            continue

        row = _db_get_ticket(tn)
        if not row:
            print(f"  SKIP {tn}: not in {tickets_tbl}")
            continue

        latest = resp_logs[-1]
        earliest = resp_logs[0]
        note_text = _note_to_field_text(latest.get("note"))
        photo_url = None
        for entry in reversed(resp_logs):
            p = str(entry.get("photo_url") or "").strip()
            if p.startswith("http"):
                photo_url = p
                break

        fr = str(row.get("field_response") or "").strip()
        updates: dict[str, object] = {"updated_at": now_iso}
        if not fr and note_text:
            updates["field_response"] = note_text
        if photo_url and not str(row.get("photo_url") or "").strip().startswith("http"):
            updates["photo_url"] = photo_url
        clear_marked = bool(row.get("marked_unattended_at"))
        status = str(row.get("status") or "").strip()
        if status in ("Daily Task", "Pending"):
            updates["status"] = "Open"
        ra = _parse_ts(row.get("responded_at"))
        ea = _parse_ts(earliest.get("timestamp"))
        if not ra and ea:
            updates["responded_at"] = ea.isoformat()

        assignee = str(row.get("assigned_to") or "").strip()
        responder = str(latest.get("member_username") or assignee).strip()

        print(
            f"  FIX {tn}: status {status!r} -> Open, clear marked, "
            f"resp_logs={len(resp_logs)}, field_text={'yes' if note_text else 'no'}, "
            f"photo={'yes' if photo_url else 'no'}"
        )

        if args.apply:
            client.table(tickets_tbl).update(updates).eq("ticket_number", tn).execute()
            if clear_marked:
                # Supabase client may omit null in mixed PATCH payloads; second call clears flag.
                client.table(tickets_tbl).update(
                    {"marked_unattended_at": None, "updated_at": now_iso}
                ).eq("ticket_number", tn).execute()
            visit_end = str(latest.get("timestamp") or now_iso)
            _bot_visits_close_responded(
                tn,
                assignee=responder or assignee,
                response_note=note_text or fr or None,
                photo_url=photo_url,
                visit_end=visit_end,
            )
            client.table(logs_tbl).insert(
                {
                    "ticket_number": tn,
                    "member_username": "@dashboard-admin",
                    "action_type": "StatusChange",
                    "note": (
                        f"Bot outage {target}: reconciled field reply after delayed "
                        "Telegram webhook delivery (cleared false unattended)."
                    ),
                    "timestamp": now_iso,
                }
            ).execute()
            fixed += 1

    if not args.apply:
        print(f"\nDry run. Would fix {len(ticket_numbers) - skipped_no_response} tickets.")
        print(f"Skipped (no Response log): {skipped_no_response}")
        print("Use --apply to write.")
        return 0

    print(f"\nDone. Updated {fixed} tickets; skipped {skipped_no_response} without Response logs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
