"""Deep audit: @fatrixshaquiell unattended on 2026-09-27 (UTC+5)."""
from __future__ import annotations

import sys
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)
import app as a  # noqa: E402

ENGINEER = "@FatrixShaquiell"
DAY = date(2026, 9, 27)


def day_range(d: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    rs = pd.Timestamp(datetime.combine(d, time.min, tzinfo=a.LOCAL_TZ)).tz_convert("UTC")
    re = pd.Timestamp(
        datetime.combine(d, time(23, 59, 59, 999999), tzinfo=a.LOCAL_TZ)
    ).tz_convert("UTC")
    return rs, re


def main() -> None:
    rs, re = day_range(DAY)
    df_all = a._fetch_tickets_cached()
    visits = a._perf_load_overview_visits_history(df_all)
    credit = a._perf_person_credit_key(ENGINEER)

    counts_all = a._perf_overview_unattended_counts_by_credit(
        df_all, focus="All", visits=visits, range_start=rs, range_end=re
    )
    counts_focus = a._perf_overview_unattended_counts_by_credit(
        df_all, focus=ENGINEER, visits=visits, range_start=rs, range_end=re
    )
    rows = a._perf_unattended_assignment_rows(
        df_all, focus=ENGINEER, visits=visits, range_start=rs, range_end=re
    )

    credit_to_label = a._perf_engineer_credit_to_label_map()
    _, res_by, _ = a._perf_assignment_task_day_counts_memo(
        df_all,
        a._fetch_sales_cases_cached(),
        range_start=rs,
        range_end=re,
        credit_to_label=credit_to_label,
    )
    label = credit_to_label.get(credit, credit)
    tasks = int(res_by.get(label, 0))

    print(f"=== {ENGINEER} on {DAY} ({a.LOCAL_TZ_LABEL}) ===")
    print(f"Assignment tasks (log/gap-fill): {tasks}")
    print(f"Unattended (focus={ENGINEER}): {counts_focus.get(credit, 0)}")
    print(f"Unattended (all engineers map): {counts_all.get(credit, 0)}")
    print()
    print("Detail rows:")
    for r in rows:
        print(f"  {r}")

    # Manual walk: every visit cycle for this engineer on this day
    print("\n--- Visit cycles (visit_start in range) ---")
    prepared = a._perf_prepare_visits_df(visits)
    ticket_nums = set(a._perf_overview_ticket_numbers(df_all))
    tickets_by_num = {
        str(row.get("ticket_number") or "").strip(): row
        for _, row in df_all.iterrows()
        if str(row.get("ticket_number") or "").strip()
    }
    seen: set[str] = set()
    for _, visit in prepared.iterrows():
        tn = str(visit.get("ticket_number") or "").strip()
        if tn not in ticket_nums:
            continue
        eng = a._perf_person_credit_key(a._perf_norm_member(visit.get("assignee")))
        if eng != credit:
            continue
        vs = a._parse_ts(visit.get("visit_start"))
        if pd.isna(vs) or vs < rs or vs > re:
            continue
        unatt = a._perf_overview_visit_cycle_unattended(visit, tickets_by_num.get(tn))
        dedupe = a._perf_overview_unattended_cycle_dedupe_key(visit, tn)
        resp = a._perf_visit_cycle_had_field_response(visit, tickets_by_num.get(tn))
        print(
            f"  {tn} outcome={visit.get('outcome')} active={visit.get('is_active')} "
            f"unatt={unatt} responded={resp} dedupe={dedupe} in_seen={dedupe in seen}"
        )
        if unatt:
            seen.add(dedupe)

    print("\n--- Pending fallback (last_assigned_at in range, no active assigned visit) ---")
    for tn in ticket_nums:
        row = tickets_by_num.get(tn)
        if row is None:
            continue
        primary = a._perf_person_credit_key(a._perf_norm_member(row.get("assigned_to")))
        if primary != credit:
            continue
        if not a._ticket_pending_unattended_eligible_row(row):
            continue
        la = a._parse_ts(row.get("last_assigned_at"))
        if pd.isna(la) or la < rs or la > re:
            continue
        assign_day = la.tz_convert(a.LOCAL_TZ).date().isoformat()
        dedupe = f"pending:{tn}:{primary}:{assign_day}"
        print(f"  {tn} last_assigned={la} pending_dedupe={dedupe} in_seen={dedupe in seen}")


if __name__ == "__main__":
    main()
