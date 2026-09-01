#!/usr/bin/env python3
"""Renumber mistaken ticket 2025091407781057 -> 100737692."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

OLD_TICKET = "2025091407781057"
NEW_TICKET = "100737692"


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
        print(f"{NEW_TICKET} already exists — nothing to do.")
        return 0

    old = (
        client.table("tickets_active")
        .select("*")
        .eq("ticket_number", OLD_TICKET)
        .limit(1)
        .execute()
    ).data
    if not old:
        print(f"{OLD_TICKET} not found — aborting.")
        return 3

    row = dict(old[0])
    row.pop("id", None)
    row["ticket_number"] = NEW_TICKET
    photo = row.get("photo_url")
    if isinstance(photo, str) and OLD_TICKET in photo:
        row["photo_url"] = photo.replace(OLD_TICKET, NEW_TICKET)

    client.table("tickets_active").insert(row).execute()

    client.table("ticket_visits").update({"ticket_number": NEW_TICKET}).eq(
        "ticket_number", OLD_TICKET
    ).execute()

    client.table("ticket_attendance_logs").update({"ticket_number": NEW_TICKET}).eq(
        "ticket_number", OLD_TICKET
    ).execute()

    client.table("tickets_active").delete().eq("ticket_number", OLD_TICKET).execute()

    client.table("ticket_attendance_logs").insert(
        {
            "ticket_number": NEW_TICKET,
            "member_username": "ibeyx",
            "action_type": "AdminCorrection",
            "note": f"Renumbered ticket from mistaken ID {OLD_TICKET} to correct ID {NEW_TICKET}.",
        }
    ).execute()

    print(f"Renumbered {OLD_TICKET} -> {NEW_TICKET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
