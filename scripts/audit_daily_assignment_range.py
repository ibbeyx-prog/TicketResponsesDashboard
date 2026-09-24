"""Compare Daily assignment task counts vs raw Assignment logs for a date range."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

env_path = ROOT / ".env"
if env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(env_path)

import app as a  # noqa: E402


def main() -> None:
    range_start = pd.Timestamp("2026-05-17", tz="UTC")
    range_end = pd.Timestamp("2026-09-26 23:59:59", tz="UTC")
    logs = a._fetch_assignment_task_logs_in_range_cached(
        range_start.isoformat(),
        range_end.isoformat(),
    )
    print(f"Assignment logs fetched: {len(logs)}")
    if not logs.empty and "timestamp" in logs.columns:
        ts = a._parse_ts(logs["timestamp"])
        print(f"  timestamp min: {ts.min()}")
        print(f"  timestamp max: {ts.max()}")

    df_all = a._fetch_tickets_cached()
    sales = a._fetch_sales_cases_cached()
    credit = a._perf_engineer_credit_to_label_map()
    day_counts, res_by, rsr_by = a._perf_assignment_task_day_counts(
        df_all,
        sales,
        range_start=range_start,
        range_end=range_end,
        credit_to_label=credit,
    )
    days = sorted({d for d, _ in day_counts})
    print(f"Daily task rows (engineer-day): {len(day_counts)}")
    print(f"  calendar span: {days[0] if days else '-'} to {days[-1] if days else '-'}")
    print(f"  residential cycles: {sum(res_by.values())}")
    print(f"  resort cycles: {sum(rsr_by.values())}")
    by_eng: dict[str, int] = {}
    for (_, eng), n in day_counts.items():
        by_eng[eng] = by_eng.get(eng, 0) + n
    print("  tasks by engineer (sum of daily counts):")
    for eng in sorted(by_eng, key=str.lower):
        print(f"    {eng}: {by_eng[eng]}")


if __name__ == "__main__":
    main()
