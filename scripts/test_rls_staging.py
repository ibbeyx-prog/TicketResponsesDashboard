#!/usr/bin/env python3
"""Exercise anon-key RLS on tickets_active and dashboard_field_engineers.

Uses SUPABASE_URL + SUPABASE_KEY from .env by default. Override with
STAGING_SUPABASE_URL / STAGING_SUPABASE_KEY when testing a branch.

Set EXPECT_POST_MIGRATION=1 to assert post-20260620_tighten_rls behavior
(INSERT/UPDATE/DELETE blocked on tickets_active; DELETE blocked on engineers).
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from supabase import create_client  # noqa: E402


@dataclass
class TestResult:
    name: str
    passed: bool
    expected: str
    detail: str


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _resolve_config() -> tuple[str, str, str]:
    load_dotenv(ROOT / ".env", encoding="utf-8-sig")
    url = (
        os.getenv("STAGING_SUPABASE_URL")
        or os.getenv("SUPABASE_URL")
        or ""
    ).strip().rstrip("/")
    key = (
        os.getenv("STAGING_SUPABASE_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    ).strip()
    source = "STAGING_*" if os.getenv("STAGING_SUPABASE_URL") else ".env"
    return url, key, source


def _is_rls_denied(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "row-level security",
            "permission denied",
            "42501",
            "new row violates row-level security",
            "violates row-level security policy",
        )
    )


def _run_select(client: Any) -> TestResult:
    expected = "success"
    try:
        resp = client.table("tickets_active").select("ticket_number").limit(1).execute()
        ok = resp.data is not None
        detail = f"rows={len(resp.data or [])}"
        return TestResult("anon SELECT tickets_active", ok, expected, detail)
    except Exception as exc:
        return TestResult("anon SELECT tickets_active", False, expected, str(exc))


def _run_insert(client: Any, *, post_migration: bool) -> TestResult:
    expected = "FAIL (RLS)" if post_migration else "success or FAIL"
    ticket_number = f"RLS-TEST-{uuid.uuid4().hex[:12]}"
    payload = {
        "ticket_number": ticket_number,
        "task_category": "RLS Test",
        "status": "Daily Task",
    }
    try:
        resp = client.table("tickets_active").insert(payload).execute()
        inserted = bool(resp.data)
        if post_migration:
            return TestResult(
                "anon INSERT tickets_active",
                not inserted,
                expected,
                f"unexpected insert succeeded: {resp.data}",
            )
        return TestResult(
            "anon INSERT tickets_active",
            inserted,
            "success (pre-migration)",
            f"inserted ticket_number={ticket_number}",
        )
    except Exception as exc:
        if post_migration:
            passed = _is_rls_denied(exc)
            return TestResult("anon INSERT tickets_active", passed, expected, str(exc))
        return TestResult(
            "anon INSERT tickets_active",
            _is_rls_denied(exc),
            "FAIL (RLS)",
            str(exc),
        )


def _run_ticket_update(client: Any, *, post_migration: bool) -> TestResult:
    expected = "FAIL (RLS)" if post_migration else "success or FAIL"
    try:
        rows = (
            client.table("tickets_active")
            .select("ticket_number")
            .limit(1)
            .execute()
            .data
            or []
        )
        if not rows:
            return TestResult(
                "anon UPDATE tickets_active",
                False,
                expected,
                "no ticket row available to update",
            )
        ticket_number = rows[0]["ticket_number"]
        resp = (
            client.table("tickets_active")
            .update({"status": "Open"})
            .eq("ticket_number", ticket_number)
            .execute()
        )
        updated = bool(resp.data)
        if post_migration:
            return TestResult(
                "anon UPDATE tickets_active",
                not updated,
                expected,
                f"unexpected update succeeded on {ticket_number}",
            )
        return TestResult(
            "anon UPDATE tickets_active",
            updated,
            "success (pre-migration)",
            f"updated ticket_number={ticket_number}",
        )
    except Exception as exc:
        if post_migration:
            return TestResult(
                "anon UPDATE tickets_active",
                _is_rls_denied(exc),
                expected,
                str(exc),
            )
        return TestResult("anon UPDATE tickets_active", False, expected, str(exc))


def _run_engineer_delete(client: Any, *, post_migration: bool) -> TestResult:
    expected = "FAIL (RLS)" if post_migration else "success or FAIL"
    dummy_username = f"rls-delete-{uuid.uuid4().hex[:8]}"
    try:
        resp = (
            client.table("dashboard_field_engineers")
            .delete()
            .eq("username", dummy_username)
            .execute()
        )
        if post_migration:
            return TestResult(
                "anon DELETE dashboard_field_engineers",
                True,
                expected,
                f"delete returned (no row): count={len(resp.data or [])}",
            )
        return TestResult(
            "anon DELETE dashboard_field_engineers",
            True,
            "success (pre-migration policy allows DELETE)",
            f"delete allowed; count={len(resp.data or [])}",
        )
    except Exception as exc:
        if post_migration:
            return TestResult(
                "anon DELETE dashboard_field_engineers",
                _is_rls_denied(exc),
                expected,
                str(exc),
            )
        return TestResult(
            "anon DELETE dashboard_field_engineers",
            False,
            "success (pre-migration)",
            str(exc),
        )


def _run_engineer_soft_update(client: Any) -> TestResult:
    expected = "success (UPDATE policy exists)"
    try:
        rows = (
            client.table("dashboard_field_engineers")
            .select("username,is_active")
            .limit(1)
            .execute()
            .data
            or []
        )
        if not rows:
            return TestResult(
                "anon UPDATE is_active dashboard_field_engineers",
                False,
                expected,
                "no engineer row available to update",
            )
        username = rows[0]["username"]
        current = rows[0].get("is_active")
        target = not bool(current)
        resp = (
            client.table("dashboard_field_engineers")
            .update({"is_active": target})
            .eq("username", username)
            .execute()
        )
        ok = bool(resp.data)
        # Restore original value so we do not leave staging dirty.
        client.table("dashboard_field_engineers").update({"is_active": current}).eq(
            "username", username
        ).execute()
        return TestResult(
            "anon UPDATE is_active dashboard_field_engineers",
            ok,
            expected,
            f"username={username!r} toggled is_active {current!r}->{target!r} then restored",
        )
    except Exception as exc:
        return TestResult(
            "anon UPDATE is_active dashboard_field_engineers",
            False,
            expected,
            str(exc),
        )


def main() -> int:
    url, key, source = _resolve_config()
    post_migration = _env_bool("EXPECT_POST_MIGRATION")

    if not url or not key:
        print(
            json.dumps(
                {
                    "error": "SUPABASE_URL and SUPABASE_KEY required (.env or STAGING_* overrides)",
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1

    project_ref = url.split("//", 1)[-1].split(".", 1)[0]
    client = create_client(url, key)

    results = [
        _run_select(client),
        _run_insert(client, post_migration=post_migration),
        _run_ticket_update(client, post_migration=post_migration),
        _run_engineer_delete(client, post_migration=post_migration),
        _run_engineer_soft_update(client),
    ]

    report = {
        "project_ref": project_ref,
        "supabase_url": url,
        "credentials_source": source,
        "expect_post_migration": post_migration,
        "tests": [
            {
                "name": r.name,
                "passed": r.passed,
                "expected": r.expected,
                "detail": r.detail,
            }
            for r in results
        ],
        "all_passed": all(r.passed for r in results),
    }
    print(json.dumps(report, indent=2))
    return 0 if report["all_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
