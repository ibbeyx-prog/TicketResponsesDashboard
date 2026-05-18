"""Clear stale field-response fields on Pending tickets (after reassign-in-Telegram bug).

Usage:
  python fix_stale_pending_row.py 100616936 100627136 2020051772000001
"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", encoding="utf-8-sig")

from bot import TICKETS_TABLE, _execute_ticket_update, _utc_now_iso  # noqa: E402


def main(argv: list[str]) -> None:
    tickets = [a.strip() for a in argv[1:] if a.strip()]
    if not tickets:
        print("Pass ticket numbers, e.g. python fix_stale_pending_row.py 100616936")
        raise SystemExit(1)

    now = _utc_now_iso()
    for tn in tickets:
        _execute_ticket_update(
            {
                "field_response": None,
                "field_responded_by": None,
                "photo_url": None,
                "responded_at": None,
                "updated_at": now,
            },
            tn,
        )
        print(f"Cleared stale response fields on {tn} (status unchanged).")


if __name__ == "__main__":
    main(sys.argv)
