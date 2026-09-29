#!/usr/bin/env python3
"""Forensic audit: @Dissiby daily combo vs assignments for specific UTC+5 days."""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv

load_dotenv(ROOT / ".env", encoding="utf-8-sig")

import app as a  # noqa: E402

FOCUS = "@Dissiby"
DAYS = (date(2026, 9, 16), date(2026, 9, 27))
RS = a._local_date_start(date(2026, 9, 1))
RE = a._local_date_end(date(2026, 9, 28))


def main() -> None:
    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    visits = a._perf_load_overview_visits_history(df_all)
    assigned_ids = a._perf_focus_assigned_id_set(
        df_all, sales, focus=FOCUS, range_start=RS, range_end=RE, visits_history=visits
    )
    bundle = a._perf_weekly_attended_bundle(
        df_all, sales if sales is not None else pd.DataFrame(), range_start=RS, range_end=RE, focus=FOCUS
    )
    detail = bundle.get("detail")
    detail_df = a._perf_filter_detail_to_assigned_ids(
        detail if isinstance(detail, pd.DataFrame) else pd.DataFrame(), assigned_ids
    )
    daily_df = a._perf_daily_assignment_tasks_by_engineer_df(df_all, sales, range_start=RS, range_end=RE)
    credit = a._perf_engineer_credit_to_label_map()
    day_counts, _, _, tickets_by_day = a._perf_assignment_task_day_counts(
        df_all, sales, range_start=RS, range_end=RE, credit_to_label=credit
    )
    combo = a._perf_build_focus_daily_combo_df(
        detail_df,
        daily_df,
        FOCUS,
        range_start=RS,
        range_end=RE,
        df_all=df_all,
        sales_all=sales,
    )
    label = a._perf_engineer_chart_label(FOCUS)

    for d in DAYS:
        row = combo.loc[
            pd.to_datetime(combo["day"], utc=True).dt.tz_convert(a.LOCAL_TZ).dt.date == d
        ]
        tasks_chart = int(row["tasks"].iloc[0]) if len(row) else 0
        att_chart = int(row["attended"].iloc[0]) if len(row) else 0
        res_chart = int(row["field_resolved"].iloc[0]) if len(row) else 0
        tasks_log = int(day_counts.get((d, label), 0))

        print(f"\n=== {d.isoformat()} {FOCUS} ===")
        print(f"Chart: tasks={tasks_chart} attended={att_chart} resolved={res_chart}")
        print(f"Assignment log+gap tasks (engineer-day): {tasks_log}")

        # Assignment logs that day
        rs = a._local_date_start(d)
        re = a._local_date_end(d)
        logs = a._fetch_assignment_task_logs_in_range_cached(rs.isoformat(), re.isoformat())
        if not logs.empty:
            ts = a._parse_ts(logs["timestamp"])
            in_d = ts.notna() & (ts >= rs) & (ts <= re)
            sub = logs.loc[in_d]
            print("Assignment logs:")
            for _, L in sub.iterrows():
                eng = str(L.get("member_username") or "")
                if a._perf_person_credit_key(eng) != a._perf_person_credit_key(FOCUS):
                    continue
                print(f"  {L.get('timestamp')} tn={L.get('ticket_number')} {eng}")

        # Attended detail rows by activity day
        if not detail_df.empty:
            print("Attended detail (activity day = this date):")
            for _, r in detail_df.drop_duplicates(subset=["ID"]).iterrows():
                ad = a._perf_activity_local_day(r)
                if ad != d:
                    continue
                print(
                    f"  ID={r.get('ID')} Closure={r.get('Closure')} "
                    f"Activity={r.get('Activity (local)')} Status={r.get('Status')}"
                )

        # Tickets assigned this day (any source)
        print("Tickets with assignment task credit this day:")
        assigned_today: set[str] = set()
        for (day, eng), n in day_counts.items():
            if day == d and eng == label:
                pass
        for _, L in (logs.loc[in_d].iterrows() if not logs.empty else []):
            eng = str(L.get("member_username") or "")
            if a._perf_person_credit_key(eng) == a._perf_person_credit_key(FOCUS):
                tn = str(L.get("ticket_number") or "").strip()
                if tn:
                    assigned_today.add(tn)
        for tn in sorted(assigned_today):
            in_detail = tn in set(detail_df["ID"].astype(str)) if not detail_df.empty else False
            act_day = None
            if in_detail and not detail_df.empty:
                sub = detail_df.loc[detail_df["ID"].astype(str).eq(tn)].iloc[0]
                act_day = a._perf_activity_local_day(sub)
            print(f"  {tn} in_attended_detail={in_detail} activity_day={act_day}")


if __name__ == "__main__":
    main()
