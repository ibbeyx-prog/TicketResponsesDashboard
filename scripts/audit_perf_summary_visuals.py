#!/usr/bin/env python3
"""Audit Performance Summary visuals — assignment daily, combo chart, resolution KPIs."""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

env_path = ROOT / ".env"
if env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(env_path, encoding="utf-8-sig")

import app as a  # noqa: E402


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")


def _ok(msg: str) -> None:
    print(f"OK: {msg}")


def _sidebar_range_today() -> tuple[pd.Timestamp, pd.Timestamp]:
    today = pd.Timestamp.now(tz=a.LOCAL_TZ).date()
    return a._local_date_start(today), a._local_date_end(today)


def _audit_assignment_logs_today() -> bool:
    d = pd.Timestamp.now(tz=a.LOCAL_TZ).date()
    rs = a._local_date_start(d)
    re = a._local_date_end(d)
    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    credit = a._perf_engineer_credit_to_label_map()
    day_counts, res_by, rsr_by, _ = a._perf_assignment_task_day_counts(
        df_all,
        sales,
        range_start=rs,
        range_end=re,
        credit_to_label=credit,
    )
    logs = a._fetch_assignment_task_logs_in_range_cached(rs.isoformat(), re.isoformat())
    log_n = 0
    if not logs.empty:
        log_n = int((logs["action_type"].astype(str) == "Assignment").sum())
    dash_n = sum(day_counts.values())
    if dash_n > log_n + 3:
        _fail(f"today tasks {dash_n} vs Assignment logs {log_n} — unexpected gap")
        return False
    note = " (+ gap fill)" if dash_n > log_n else ""
    _ok(
        f"today assignment tasks {dash_n} (logs {log_n}{note}, "
        f"res {sum(res_by.values())}, rsr {sum(rsr_by.values())})"
    )
    return True


def _audit_engineer_combo(focus: str, rs: pd.Timestamp, re: pd.Timestamp) -> bool:
    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    visits = a._perf_load_overview_visits_history(df_all)
    assigned_ids = a._perf_focus_assigned_id_set(
        df_all,
        sales,
        focus=focus,
        range_start=rs,
        range_end=re,
        visits_history=visits,
    )
    bundle = a._perf_weekly_attended_bundle(
        df_all,
        sales if sales is not None else pd.DataFrame(),
        range_start=rs,
        range_end=re,
        focus=focus,
    )
    detail = bundle.get("detail")
    detail_df = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()
    scoped = a._perf_filter_detail_to_assigned_ids(detail_df, assigned_ids)
    daily_df = a._perf_daily_assignment_tasks_by_engineer_df(
        df_all, sales, range_start=rs, range_end=re
    )
    combo = a._perf_build_focus_daily_combo_df(
        scoped,
        daily_df,
        focus,
        range_start=rs,
        range_end=re,
        df_all=df_all,
        sales_all=sales,
    )
    exec_m = a._perf_weekly_executive_metrics(scoped)
    tasks_sum = int(combo["tasks"].sum())
    filtered = a._perf_filter_daily_assignment_df(daily_df, focus)
    if not filtered.empty:
        expect_tasks = int(filtered["tasks"].sum())
    else:
        expect_tasks = 0
    if tasks_sum != expect_tasks:
        _fail(f"{focus} combo tasks sum {tasks_sum} != filtered daily {expect_tasks}")
        return False
    label = a._perf_engineer_chart_label(focus)
    credit = a._perf_engineer_credit_to_label_map()
    expect_attended = sum(
        n
        for (day, eng), n in a._perf_daily_assignment_log_attended_by_engineer_day(
            df_all, sales, range_start=rs, range_end=re, credit_to_label=credit
        ).items()
        if eng == label
    )
    expect_resolved = sum(a._perf_daily_field_resolved_activity_by_day(scoped).values())
    attended_combo = int(combo["attended"].sum())
    resolved_combo = int(combo["field_resolved"].sum())
    if attended_combo != expect_attended:
        _fail(f"{focus} combo attended sum {attended_combo} != visit-cycle {expect_attended}")
        return False
    if resolved_combo != expect_resolved:
        _fail(f"{focus} combo resolved sum {resolved_combo} != activity-day {expect_resolved}")
        return False
    total_kpi = int(exec_m.get("total") or 0)
    resolved_kpi = int(exec_m.get("resolved") or 0)
    try:
        import altair as alt

        plot = combo.copy()
        plot["Engineer"] = focus
        ch = a._weekly_altair_focus_daily_combo_chart(
            plot,
            task_color="#3b82f6",
            tooltips=[alt.Tooltip("day:T"), alt.Tooltip("tasks:Q"), alt.Tooltip("field_resolved:Q")],
        )
        ch.to_dict()
    except Exception as exc:
        _fail(f"{focus} Altair combo chart: {exc}")
        return False
    _ok(
        f"{focus} combo aligned (tasks={tasks_sum}, assign-cohort attended={attended_combo}, "
        f"resolved={resolved_combo}; period KPI attended={total_kpi} resolved={resolved_kpi})"
    )
    return True


def main() -> int:
    if not os.getenv("SUPABASE_URL"):
        print("SKIP: no SUPABASE_URL — run with .env for live audit")
        return 0

    print("=== Performance Summary visual audit ===\n")
    ok = True
    ok &= _audit_assignment_logs_today()

    rs = a._local_date_start(date(2026, 9, 1))
    re = a._local_date_end(date(2026, 9, 28))
    for eng in ("@Dissiby", "@Nallu10", "@FatrixShaquiell"):
        ok &= _audit_engineer_combo(eng, rs, re)

    team = a._perf_team_assignment_summary_df(
        a._fetch_tickets_cached(),
        a._fetch_sales_cases_cached(),
        range_start=rs,
        range_end=re,
    )
    if team.empty:
        _fail("team assignment table empty")
        ok = False
    else:
        _ok(f"team table {len(team)} rows Sep 1–28")

    print("\n=== Result:", "PASS" if ok else "ISSUES FOUND", "===")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
