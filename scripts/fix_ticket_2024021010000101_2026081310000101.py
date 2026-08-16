#!/usr/bin/env python3
"""Correct mistaken cross-assignment between 2024021010000101 and 2026081310000101."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

OLD_TICKET = "2024021010000101"
NEW_TICKET = "2026081310000101"
ASSIGNEE = "@FatrixShaquiell"

OLD_INFO = (
    "- H. Nasrussabaa Hulhangubai (Ground Foor)  Zidhan Mohamed    "
    "/ 9553300 / 9550330 / 7393300"
)
NEW_INFO = "M. Gulisthaanuge, Fiyaathoshi Magu, 7th Floor - Sofvan 7222847"

CORRECT_OLD_ASSIGNED_AT = "2026-08-13T05:02:10.39415+00:00"
WRONG_ASSIGNED_AT = "2026-08-13T05:15:22.981549+00:00"
TG_CHAT = -1001428974576
TG_ASSIGN_MSG = 104000

VISIT_WRONG_ID = 803
VISIT_OLD_REOPEN_ID = 802


def main() -> int:
    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_KEY")):
        print("SUPABASE_URL and SUPABASE_KEY required", file=sys.stderr)
        return 1

    from supabase import create_client

    client = create_client(
        os.environ["SUPABASE_URL"].rstrip("/"),
        os.environ["SUPABASE_KEY"].strip(),
    )

    existing_new = (
        client.table("tickets_active")
        .select("ticket_number")
        .eq("ticket_number", NEW_TICKET)
        .limit(1)
        .execute()
    )
    if existing_new.data:
        print(f"{NEW_TICKET} already exists — aborting.")
        return 2

    old = (
        client.table("tickets_active")
        .select("*")
        .eq("ticket_number", OLD_TICKET)
        .single()
        .execute()
    ).data
    if not old:
        print(f"{OLD_TICKET} not found — aborting.")
        return 3

    # 1) Restore the long-running ticket with Device Recovery details.
    client.table("tickets_active").update(
        {
            "task_category": "Device Recovery",
            "additional_info": OLD_INFO,
            "assigned_to": ASSIGNEE,
            "last_assigned_at": CORRECT_OLD_ASSIGNED_AT,
            "assignment_telegram_chat_id": None,
            "assignment_telegram_message_id": None,
        }
    ).eq("ticket_number", OLD_TICKET).execute()

    # 2) Create the new Coverage Check ticket (today's mistaken assignment).
    client.table("tickets_active").insert(
        {
            "ticket_number": NEW_TICKET,
            "assigned_to": ASSIGNEE,
            "task_category": "Coverage Check",
            "status": "Daily Task",
            "additional_info": NEW_INFO,
            "last_assigned_at": WRONG_ASSIGNED_AT,
            "assignment_telegram_chat_id": TG_CHAT,
            "assignment_telegram_message_id": TG_ASSIGN_MSG,
            "field_response": None,
            "field_responded_by": None,
            "photo_url": None,
        }
    ).execute()

    # 3) Move the mistaken visit cycle to the new ticket.
    client.table("ticket_visits").update({"ticket_number": NEW_TICKET}).eq(
        "id", VISIT_WRONG_ID
    ).execute()

    # 4) Re-open the correct visit on the old ticket.
    client.table("ticket_visits").update(
        {
            "visit_end": None,
            "is_active": True,
            "outcome": "assigned",
        }
    ).eq("id", VISIT_OLD_REOPEN_ID).execute()

    # 5) Audit trail.
    for ticket, note in (
        (
            OLD_TICKET,
            "Data correction: restored Device Recovery assignment for Nasrussabaa/Zidhan "
            f"(removed mistaken Gulisthaanuge details → {NEW_TICKET}).",
        ),
        (
            NEW_TICKET,
            f"Data correction: created ticket for Coverage Check — {NEW_INFO}",
        ),
    ):
        client.table("ticket_attendance_logs").insert(
            {
                "ticket_number": ticket,
                "member_username": "ibeyx",
                "action_type": "AdminCorrection",
                "note": note,
            }
        ).execute()

    print("Fixed:")
    print(f"  {OLD_TICKET} -> Device Recovery")
    print(f"  {NEW_TICKET} -> Coverage Check / {NEW_INFO}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
