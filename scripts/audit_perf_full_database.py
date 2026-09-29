#!/usr/bin/env python3
"""Full-database Performance Summary invariants (assignment, combo, resolution trend)."""
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


def _fail(msg: str, issues: list[str]) -> None:
    issues.append(msg)
    print(f"FAIL: {msg}")


def _ok(msg: str) -> None:
    print(f"OK: {msg}")


def _engineer_focus_labels() -> list[str]:
    credit = a._perf_engineer_credit_to_label_map()
    labels = sorted(set(credit.values()), key=str.lower)
    admin = a._perf_team_assignment_table_label(a._SC_SALES_OVERVIEW_ADMIN_LABEL)
    if admin not in labels:
        labels.append(admin)
    return labels


def _audit_gap_fill_invariants(
    rs: pd.Timestamp,
    re: pd.Timestamp,
    issues: list[str],
) -> None:
    """Response gap-fill must not credit tasks when last_assigned_at is before response day."""
    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    credit = a._perf_engineer_credit_to_label_map()
    _, _, _, tickets_by_day = a._perf_assignment_task_day_counts(
        df_all, sales, range_start=rs, range_end=re, credit_to_label=credit
    )
    rlogs = a._fetch_field_response_logs_in_range_cached(rs.isoformat(), re.isoformat())
    if rlogs.empty:
        _ok("gap-fill invariant (no response logs in range)")
        return
    sales_refs = a._perf_sales_case_ref_set(sales)
    sales_rows: dict[str, pd.Series] = {}
    if sales is not None and not sales.empty and "case_ref" in sales.columns:
        for _, row in sales.iterrows():
            ref = str(row.get("case_ref") or "").strip()
            if ref:
                sales_rows[ref] = row
    ticket_rows: dict[str, pd.Series] = {}
    if not df_all.empty and "ticket_number" in df_all.columns:
        for _, row in df_all.iterrows():
            tn = str(row.get("ticket_number") or "").strip()
            if tn:
                ticket_rows[tn] = row

    ts = a._parse_ts(rlogs["timestamp"])
    in_range = ts.notna() & (ts >= rs) & (ts <= re)
    bad = 0
    for idx, row in rlogs.loc[in_range].iterrows():
        note = str(row.get("note") or "").strip()
        photo = str(row.get("photo_url") or "").strip()
        if not note and not photo.startswith("http"):
            continue
        tn = str(row.get("ticket_number") or "").strip()
        if not tn:
            continue
        resp_day = a._to_local(pd.Series([ts.loc[idx]])).iloc[0].date()
        if tn in sales_refs:
            case_row = sales_rows.get(tn)
            last_d = a._perf_row_last_assigned_local_day(case_row)
        else:
            last_d = a._perf_row_last_assigned_local_day(ticket_rows.get(tn))
        if last_d is None or last_d >= resp_day:
            continue
        for (task_day, eng), tix in tickets_by_day.items():
            if task_day != resp_day or tn not in tix:
                continue
            for credit_key in a._perf_response_log_credit_keys(row, None):
                label = credit.get(credit_key)
                if label == eng:
                    bad += 1
                    _fail(
                        f"gap-fill task on {resp_day} for {tn} ({eng}) "
                        f"but last_assigned_at day {last_d}",
                        issues,
                    )
                    break
    if bad == 0:
        _ok(f"gap-fill invariant ({int(in_range.sum())} response rows checked)")


def _audit_engineer_combo(
    focus_label: str,
    rs: pd.Timestamp,
    re: pd.Timestamp,
    issues: list[str],
) -> bool:
    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    visits = a._perf_load_overview_visits_history(df_all)
    focus = focus_label
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
        assigned_ids=assigned_ids,
    )
    if combo.empty:
        return True
    label = a._perf_engineer_chart_label(focus)
    credit = a._perf_engineer_credit_to_label_map()
    expect_attended = sum(
        n
        for (day, eng), n in a._perf_daily_assignment_log_attended_by_engineer_day(
            df_all,
            sales,
            range_start=rs,
            range_end=re,
            credit_to_label=credit,
            limit_ticket_ids=assigned_ids,
        ).items()
        if eng == label
    )
    resolved_cycle = a._perf_daily_assignment_log_field_resolved_by_engineer_day(
        df_all,
        sales,
        range_start=rs,
        range_end=re,
        credit_to_label=credit,
        detail_df=scoped,
        limit_ticket_ids=assigned_ids,
    )
    expect_resolved = sum(n for (_, eng), n in resolved_cycle.items() if eng == label)
    attended_combo = int(combo["attended"].sum())
    resolved_combo = int(combo["field_resolved"].sum())
    ok = True
    if attended_combo != expect_attended:
        _fail(
            f"{focus_label} attended sum {attended_combo} != assign-cycle {expect_attended}",
            issues,
        )
        ok = False
    if resolved_combo != expect_resolved:
        _fail(
            f"{focus_label} resolved sum {resolved_combo} != assign-day {expect_resolved}",
            issues,
        )
        ok = False
    if int(combo["field_resolved"].max() or 0) > int(combo["attended"].max() or 0):
        day_bad = combo.loc[combo["field_resolved"] > combo["attended"]]
        for _, r in day_bad.iterrows():
            d = pd.to_datetime(r["day"], utc=True).tz_convert(a.LOCAL_TZ).date()
            _fail(
                f"{focus_label} on {d}: resolved {int(r['field_resolved'])} > attended "
                f"{int(r['attended'])}",
                issues,
            )
        ok = False
    return ok


def main() -> int:
    if not os.getenv("SUPABASE_URL"):
        print("SKIP: no SUPABASE_URL")
        return 0

    issues: list[str] = []
    print("=== Full-database Performance audit ===\n")

    today = pd.Timestamp.now(tz=a.LOCAL_TZ).date()
    rs = a._local_date_start(date(2026, 5, 1))
    re = a._local_date_end(today)

    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    credit = a._perf_engineer_credit_to_label_map()
    day_counts, res_by, rsr_by, _ = a._perf_assignment_task_day_counts(
        df_all, sales, range_start=rs, range_end=re, credit_to_label=credit
    )
    logs = a._fetch_assignment_task_logs_in_range_cached(rs.isoformat(), re.isoformat())
    log_assign_n = 0
    if not logs.empty:
        log_assign_n = int((logs["action_type"].astype(str) == "Assignment").sum())
    dash_n = sum(day_counts.values())
    if dash_n > log_assign_n + 500:
        _fail(
            f"May–today tasks {dash_n} vs Assignment logs {log_assign_n} "
            f"(gap > 500 — investigate)",
            issues,
        )
    else:
        _ok(
            f"May–today tasks {dash_n} (Assignment logs {log_assign_n}, "
            f"res {sum(res_by.values())}, rsr {sum(rsr_by.values())})"
        )

    sep_rs = a._local_date_start(date(2026, 9, 1))
    sep_re = a._local_date_end(min(today, date(2026, 9, 30)))
    _audit_gap_fill_invariants(sep_rs, sep_re, issues)

    team = a._perf_team_assignment_summary_df(df_all, sales, range_start=sep_rs, range_end=sep_re)
    if team.empty:
        _fail("team assignment table empty for September", issues)
    else:
        _ok(f"team table {len(team)} rows (Sep 2026)")

    if not team.empty:
        for _, trow in team.iterrows():
            eng_label = str(trow.get("Engineer") or "").strip()
            tasks = int(trow.get("Tasks") or 0)
            if tasks <= 0 or not eng_label:
                continue
            focus = eng_label.lstrip("🔒 ").strip()
            if _audit_engineer_combo(focus, sep_rs, sep_re, issues):
                print(f"OK: {focus} Sep combo invariants")

    trend = a._perf_range_resolution_trend(
        df_all, sales, range_start=sep_rs, range_end=sep_re, focus="All"
    )
    if isinstance(trend, pd.DataFrame) and not trend.empty:
        if (trend["field_resolved"] > trend["total"]).any():
            _fail("team resolution trend has day with resolved > attended", issues)
        else:
            _ok("team resolution trend assign-day counts")

    print("\n=== Result:", "PASS" if not issues else f"{len(issues)} ISSUE(S)", "===")
    for item in issues[:20]:
        print(" ", item)
    if len(issues) > 20:
        print(f"  ... and {len(issues) - 20} more")
    return 0 if not issues else 1


if __name__ == "__main__":
    raise SystemExit(main())
