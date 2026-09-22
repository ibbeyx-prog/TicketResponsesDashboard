"""Compare daily assignment tasks vs unattended cycles for one UTC+5 calendar day."""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

import app as a  # noqa: E402


def _day_range(d: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    rs = pd.Timestamp(datetime.combine(d, time.min, tzinfo=a.LOCAL_TZ)).tz_convert("UTC")
    re = pd.Timestamp(
        datetime.combine(d, time(23, 59, 59, 999999), tzinfo=a.LOCAL_TZ)
    ).tz_convert("UTC")
    return rs, re


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default="2026-09-21", help="Calendar day in UTC+5 (YYYY-MM-DD)")
    args = p.parse_args()
    target = date.fromisoformat(args.date)
    rs, re = _day_range(target)

    df_all = a._fetch_tickets_cached()
    raw_sales = a._fetch_sales_cases_cached()
    sales = raw_sales if raw_sales is not None else pd.DataFrame()
    visits = a._perf_load_overview_visits_history(df_all)

    counts = a._perf_overview_unattended_counts_by_credit(
        df_all, focus="All", visits=visits, range_start=rs, range_end=re
    )
    detail_rows = a._perf_unattended_assignment_rows(
        df_all, focus="All", visits=visits, range_start=rs, range_end=re
    )

    credit_to_label = a._perf_engineer_credit_to_label_map()
    day_counts, _, _ = a._perf_assignment_task_day_counts_memo(
        df_all,
        sales,
        range_start=rs,
        range_end=re,
        credit_to_label=credit_to_label,
    )
    tasks_total = sum(day_counts.values())
    unattended_total = sum(counts.values())

    print(f"=== {target.isoformat()} ({a.LOCAL_TZ_LABEL}) ===")
    print(f"Assignment tasks (attendance log / gap-fill): {tasks_total}")
    print(f"Unattended assignment cycles (visit + cutoff rules): {unattended_total}")
    print()
    print("Engineer | Tasks | Unattended")
    labels: set[str] = set()
    for (_, eng), _n in day_counts.items():
        labels.add(eng)
    for ck in counts:
        labels.add(credit_to_label.get(ck, ck))
    for eng in sorted(labels, key=str.lower):
        tasks = sum(n for (_d, e), n in day_counts.items() if e == eng)
        ck = a._perf_person_credit_key(str(eng).lstrip("@"))
        unatt = counts.get(ck, 0)
        if tasks or unatt:
            print(f"  {eng:22} {tasks:4} {unatt:4}")

    print()
    print("Unattended detail breakdown:")
    print("  Case kind:", dict(Counter(r["Case kind"] for r in detail_rows)))
    print("  Visit outcome:", dict(Counter(r["Visit outcome"] for r in detail_rows)))
    print()
    for r in detail_rows:
        print(
            f"  {r['Ticket']:16} {r['Assign day']}  {r['Case kind']:16} "
            f"{r['Visit outcome']:12} status={r['Status']}"
        )

    # Cumulative week Mon..target (matches partial "this week" if today is target)
    mon = target - timedelta(days=target.weekday())
    wrs, _ = _day_range(mon)
    counts_week = a._perf_overview_unattended_counts_by_credit(
        df_all, focus="All", visits=visits, range_start=wrs, range_end=re
    )
    day_counts_week, _, _ = a._perf_assignment_task_day_counts_memo(
        df_all,
        sales,
        range_start=wrs,
        range_end=re,
        credit_to_label=credit_to_label,
    )
    print()
    print(f"Week {mon} .. {target} (inclusive):")
    print(f"  Tasks: {sum(day_counts_week.values())}")
    print(f"  Unattended cycles: {sum(counts_week.values())}")


if __name__ == "__main__":
    main()
