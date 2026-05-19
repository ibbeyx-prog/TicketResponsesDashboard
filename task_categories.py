"""Task categories — Supabase ``dashboard_task_categories`` is the source of truth."""

from __future__ import annotations

import os
import re
from typing import Any

DEFAULT_ASSIGNMENT_TASK_CATEGORIES: tuple[str, ...] = (
    "Coverage Check",
    "Femto Installation",
    "Repeater Installation",
    "Femto Recover",
    "Femto Fault",
    "Repeater Fault",
    "Voice / Data issue",
    "Femto Swap",
    "Pico Installation",
    "Pole / EM installation",
    "IBS",
    "Sector / Nodeb Installation",
    "Lamp Site / Fault",
    "Mobile Fault / Swap",
    "Optimization / Logfile",
    "Customer moved out",
    "MEGA Survey",
    "OFF Male",
    "Continue Installation",
    "Follow-Up Installation",
)


def task_categories_table() -> str:
    return (os.getenv("TASK_CATEGORIES_TABLE") or "dashboard_task_categories").strip()


def normalize_task_category_name(raw: str) -> str:
    s = " ".join((raw or "").strip().split())
    if not s:
        raise ValueError("Enter a category name.")
    if len(s) > 64:
        raise ValueError("Category name is too long (max 64 characters).")
    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9 \-/&]*$", s):
        raise ValueError(
            "Use letters, digits, spaces, and - / & only (must start with a letter or digit)."
        )
    return s


def fetch_task_category_names(
    client: Any,
    *,
    table: str | None = None,
    include_defaults_if_empty: bool = True,
) -> tuple[list[str], bool]:
    """Return ``(names, table_missing)`` from Supabase ordered for pickers."""
    tbl = table or task_categories_table()
    try:
        res = (
            client.table(tbl)
            .select("name")
            .order("sort_order")
            .order("name")
            .execute()
        )
    except Exception as exc:
        msg = str(exc).lower()
        if "does not exist" in msg or "pgrst205" in msg or "42p01" in msg:
            return [], True
        raise
    names = [str(r["name"]).strip() for r in (res.data or []) if r.get("name")]
    if not names and include_defaults_if_empty:
        return list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES), False
    return names, False


def upsert_task_category(client: Any, name: str, *, table: str | None = None) -> None:
    """Insert category into Supabase (idempotent)."""
    norm = normalize_task_category_name(name)
    tbl = table or task_categories_table()
    client.table(tbl).upsert(
        {"name": norm, "sort_order": 0},
        on_conflict="name",
    ).execute()


def delete_task_category(client: Any, name: str, *, table: str | None = None) -> None:
    tbl = table or task_categories_table()
    client.table(tbl).delete().eq("name", name).execute()


def sync_ticket_categories_into_table(
    client: Any,
    *,
    tickets_table: str,
    categories_table: str | None = None,
) -> int:
    """Upsert distinct ``task_category`` values from tickets into the categories table."""
    tbl = categories_table or task_categories_table()
    try:
        res = client.table(tickets_table).select("task_category").execute()
    except Exception:
        return 0
    added = 0
    seen: set[str] = set()
    for row in res.data or []:
        raw = str(row.get("task_category") or "").strip()
        if not raw:
            continue
        try:
            norm = normalize_task_category_name(raw)
        except ValueError:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            upsert_task_category(client, norm, table=tbl)
            added += 1
        except Exception:
            pass
    return added


def resolve_task_category(raw: str, known: tuple[str, ...] | list[str]) -> str | None:
    """Case-insensitive match of ``raw`` to a known category label."""
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if not s:
        return None
    if s in known:
        return s
    key_lower = s.lower()
    for cat in known:
        if cat.lower() == key_lower:
            return cat
    return None
