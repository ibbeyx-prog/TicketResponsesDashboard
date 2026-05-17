#!/usr/bin/env python3
"""Preview which tickets qualify for field-response backfill.

Only tickets with ``last_assigned_at`` **on/after bot group deploy** are eligible.
Pre-deploy work is excluded — no historical backfill onto the dashboard.

Set deploy time in:
  - ``BOT_GROUP_DEPLOYED_AT`` in ``.env`` (UTC ISO), and/or
  - ``deploy_cutoff`` in ``supabase/migrations/20260529_backfill_field_responded_by.sql``

Run::

    py -3 backfill_field_responses.py

Apply fixes via Supabase SQL editor (edit deploy date in that migration first).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

DEFAULT_DEPLOY_CUTOFF = "2026-05-17T00:00:00+00:00"
SQL_PATH = _ROOT / "supabase" / "migrations" / "20260529_backfill_field_responded_by.sql"


def _parse_deploy_cutoff() -> datetime:
    raw = (os.getenv("BOT_GROUP_DEPLOYED_AT") or DEFAULT_DEPLOY_CUTOFF).strip()
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def main() -> int:
    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_KEY")):
        print("ERROR: SUPABASE_URL and SUPABASE_KEY required in .env", file=sys.stderr)
        return 1

    deploy = _parse_deploy_cutoff()
    if deploy.tzinfo is None:
        deploy = deploy.replace(tzinfo=timezone.utc)

    from supabase import create_client

    client = create_client(
        os.environ["SUPABASE_URL"].strip().rstrip("/"),
        os.environ["SUPABASE_KEY"].strip(),
    )

    print("=== Field response backfill (post-deploy only) ===\n")
    print(f"BOT_GROUP_DEPLOYED_AT (UTC): {deploy.isoformat()}")
    print(f"SQL file: {SQL_PATH}\n")

    tickets = (
        client.table("tickets_active")
        .select(
            "ticket_number,status,assigned_to,last_assigned_at,"
            "field_response,field_responded_by,responded_at"
        )
        .gte("last_assigned_at", deploy.isoformat())
        .order("last_assigned_at", desc=True)
        .execute()
    ).data or []

    logs = (
        client.table("ticket_attendance_logs")
        .select("ticket_number,action_type,member_username,note,timestamp")
        .eq("action_type", "Response")
        .execute()
    ).data or []
    log_by_ticket = {str(r["ticket_number"]): r for r in logs}

    print(f"Tickets assigned on/after deploy: {len(tickets)}")
    if not tickets:
        print("Nothing to backfill for this deploy window.")
        return 0

    for row in tickets[:25]:
        tid = str(row.get("ticket_number") or "")
        la = row.get("last_assigned_at") or "—"
        st = row.get("status") or "—"
        has_log = "yes" if tid in log_by_ticket else "no"
        print(
            f"  {tid}  status={st}  assigned={row.get('assigned_to')}  "
            f"last_assigned_at={la}  response_log={has_log}"
        )
    if len(tickets) > 25:
        print(f"  … and {len(tickets) - 25} more")

    pending_with_log = [
        str(t["ticket_number"])
        for t in tickets
        if str(t.get("status")) == "Pending" and str(t.get("ticket_number")) in log_by_ticket
    ]
    if pending_with_log:
        print(
            f"\nPost-deploy Pending + Response log (SQL would repair): "
            f"{', '.join(pending_with_log)}"
        )

    print(
        "\nPre-deploy tickets are intentionally excluded. "
        "Edit deploy_cutoff in the SQL migration, then run it in Supabase."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
