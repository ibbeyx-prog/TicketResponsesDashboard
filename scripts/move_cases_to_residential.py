"""One-off: move resort/sales cases to residential tickets (preserve Resolved timestamps)."""
from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

from supabase_client import get_cached_supabase_client, resolve_supabase_config  # noqa: E402

CASES = [
    "2026050310000043",
    "2026060127749364",
    "2026053127784626",
    "2026060170000009",
    "100671037",
    "2026060170000007",
]


def main() -> None:
    cfg = resolve_supabase_config()
    client = get_cached_supabase_client(cfg.url, cfg.key)
    res = (
        client.table("dashboard_sales_cases")
        .select("*")
        .in_("case_ref", CASES)
        .execute()
    )
    rows = res.data or []
    print(f"found {len(rows)} sales/resort cases")
    for row in rows:
        ref = str(row["case_ref"])
        existing = (
            client.table("tickets_active")
            .select("ticket_number")
            .eq("ticket_number", ref)
            .limit(1)
            .execute()
        )
        if existing.data:
            xfer = (
                client.table("ticket_attendance_logs")
                .select("id")
                .eq("ticket_number", ref)
                .eq("action_type", "TransferredFromSales")
                .limit(1)
                .execute()
            )
            if not xfer.data:
                client.table("ticket_attendance_logs").insert(
                    {
                        "ticket_number": ref,
                        "member_username": "@dashboard-admin",
                        "action_type": "TransferredFromSales",
                        "note": (
                            "Moved from Resort/Sales Cases to Residential "
                            "(duplicate removed from sales)."
                        ),
                        "timestamp": row.get("updated_at"),
                    }
                ).execute()
            client.table("dashboard_sales_cases").delete().eq("id", row["id"]).execute()
            print(f"{ref} OK — removed sales duplicate (already in tickets_active)")
            continue
        task = (
            str(row.get("field_task_category") or row.get("sales_category") or "")
            .strip()
            or "Coverage Check"
        )
        info = (
            str(
                row.get("additional_info")
                or row.get("description")
                or row.get("close_note")
                or ""
            ).strip()
            or None
        )
        ticket = {
            "ticket_number": ref,
            "assigned_to": row.get("assigned_to"),
            "assigned_to_2": row.get("assigned_to_2"),
            "task_category": task,
            "outcome_category": task,
            "status": "Resolved",
            "field_response": row.get("field_response"),
            "field_responded_by": row.get("field_responded_by"),
            "additional_info": info,
            "responded_at": row.get("responded_at"),
            "updated_at": row.get("updated_at"),
            "created_at": row.get("created_at"),
            "last_assigned_at": row.get("last_assigned_at"),
        }
        client.table("tickets_active").insert(ticket).execute()
        client.table("ticket_attendance_logs").insert(
            {
                "ticket_number": ref,
                "member_username": "@dashboard-admin",
                "action_type": "TransferredFromSales",
                "note": (
                    "Moved from Resort/Sales Cases to Residential (Resolved). "
                    "Timestamps preserved."
                ),
                "timestamp": row.get("updated_at"),
            }
        ).execute()
        client.table("dashboard_sales_cases").delete().eq("id", row["id"]).execute()
        print(f"{ref} OK -> residential Resolved (updated_at={row.get('updated_at')})")

    verify = (
        client.table("tickets_active")
        .select("ticket_number, status, responded_at, updated_at")
        .in_("ticket_number", CASES)
        .execute()
    )
    print("verify tickets:", verify.data)
    left = (
        client.table("dashboard_sales_cases")
        .select("case_ref")
        .in_("case_ref", CASES)
        .execute()
    )
    print("remaining sales:", left.data)


if __name__ == "__main__":
    main()
