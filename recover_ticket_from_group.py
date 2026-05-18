#!/usr/bin/env python3
"""Apply a missed field response when Telegram history cannot be read by the bot.

**Important:** Telegram **bot accounts cannot scroll group chat history**
(``GetHistory`` / search are blocked). After redeploy, the live bot only
captures **new** messages via Telethon ``NewMessage`` events.

To recover an older reply you can:

1. Ask the engineer to **swipe-reply again** on the assignment line (best).
2. Apply a known reply manually::

       py -3 recover_ticket_from_group.py 100625946 --text "on the way" --apply

3. (Advanced) Use a **user** Telethon session (not bot token) to scan history —
   not included here.

Usage::

    py -3 recover_ticket_from_group.py 100625946 --text "your reply" --apply
    py -3 recover_ticket_from_group.py 100625946 --text "on the way" --apply --responded-by "@test_handle"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env", encoding="utf-8-sig")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("recover_ticket")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ticket_number", help="9 or 16 digit ticket id")
    parser.add_argument(
        "--text",
        required=True,
        help="Field reply text to store (from Telegram screenshot / engineer)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write to Supabase (required to change dashboard)",
    )
    parser.add_argument(
        "--responded-by",
        default="",
        help="Optional label for field_responded_by (e.g. alternate phone @handle)",
    )
    args = parser.parse_args()

    if not args.apply:
        log.info("Dry run. Add --apply to update Supabase.")
        log.info("  ticket=%s  field_response=%r", args.ticket_number, args.text)
        return 0

    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_KEY")):
        log.error("SUPABASE_URL and SUPABASE_KEY required in .env")
        return 1

    from bot import (
        _db_complete_ticket_field_response,
        _db_get_ticket,
        _field_responded_by_value,
    )

    ticket = args.ticket_number.strip()
    row = _db_get_ticket(ticket)
    if not row:
        log.error("Ticket %s not found in tickets_active.", ticket)
        return 2

    assignee = str(row.get("assigned_to") or "")
    responded_by_label = (args.responded_by or "Alternate phone").strip()
    field_responded_by = _field_responded_by_value(
        assigned_to=assignee,
        replier_label=responded_by_label.lstrip("@"),
    )
    if not field_responded_by and responded_by_label.lower() not in (
        assignee.lower(),
        assignee.lstrip("@").lower(),
    ):
        field_responded_by = responded_by_label

    assignee_handle = assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"

    _db_complete_ticket_field_response(
        ticket,
        field_response=args.text.strip(),
        update_photo_url=False,
        responder_username=assignee_handle,
        field_responded_by=field_responded_by,
    )

    log.info(
        "Updated %s → Open | field_response=%r | field_responded_by=%r",
        ticket,
        args.text.strip(),
        field_responded_by,
    )
    log.info("Refresh dashboard → Open queue.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
