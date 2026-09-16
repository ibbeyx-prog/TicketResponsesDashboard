"""One-off engineer performance audit using dashboard metric definitions."""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

import app as a  # noqa: E402

ENGINEER = "@FatrixShaquiell"
AUDIT_MONTH = date(2026, 9, 1)


def main() -> None:
    rs, re, d0, d1 = a._perf_calendar_month_for_date(AUDIT_MONTH)
    period = f"{d0.isoformat()} – {d1.isoformat()} ({a.LOCAL_TZ_LABEL})"

    df_all = a._fetch_tickets_cached()
    sales_all = a._fetch_sales_cases_cached()
    visits = a._perf_load_overview_visits_history(df_all)

    bundle = a._perf_weekly_attended_bundle(
        df_all,
        sales_all if sales_all is not None else pd.DataFrame(),
        range_start=rs,
        range_end=re,
        focus=ENGINEER,
    )
    detail = bundle.get("detail")
    detail_df = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()
    exec_metrics = a._perf_weekly_executive_metrics(detail_df)

    focus_metrics = a._perf_summary_focus_engineer_metrics_with_reconciliation(
        df_all,
        sales_all,
        focus=ENGINEER,
        range_start=rs,
        range_end=re,
        attended_yours_total=int(exec_metrics.get("total") or 0),
        visits_history=visits,
    )
    focus_metrics.pop("_assigned_ids", None)
    derived = a._perf_summary_derived_metrics(focus_metrics)

    team_df = a._perf_team_assignment_summary_df(
        df_all,
        sales_all,
        range_start=rs,
        range_end=re,
    )
    team_row = None
    if not team_df.empty:
        key = a._perf_person_credit_key(ENGINEER)
        for _, row in team_df.iterrows():
            if a._perf_person_credit_key(row.get("Engineer")) == key:
                team_row = row.to_dict()
                break

    # Handed-off / still-open ticket ids (assigned in range, credit elsewhere)
    assign_m = a._perf_engineer_range_assignment_metrics(
        df_all,
        sales_all,
        focus=ENGINEER,
        range_start=rs,
        range_end=re,
        visits_history=visits,
    )
    assigned_ids = assign_m.get("_assigned_ids", frozenset())
    recon = a._perf_summary_assigned_reconciliation(
        df_all,
        sales_all,
        focus=ENGINEER,
        range_start=rs,
        range_end=re,
        assigned_ids=assigned_ids if isinstance(assigned_ids, frozenset) else frozenset(),
        attended_yours_total=int(exec_metrics.get("total") or 0),
    )
    attended_by = a._perf_attended_ticket_ids_credited_to(
        df_all,
        sales_all,
        focus=ENGINEER,
        range_start=rs,
        range_end=re,
    )
    attended_any = a._perf_attended_ticket_ids_in_range(
        df_all, sales_all, range_start=rs, range_end=re
    )
    handed_ids = sorted(assigned_ids - attended_by & attended_any)
    still_open = sorted(set(assigned_ids) - attended_any)

    closure_breakdown = {}
    if not detail_df.empty and "Closure" in detail_df.columns:
        id_first = detail_df.drop_duplicates(subset=["ID"], keep="first")
        closure_breakdown = (
            id_first["Closure"].astype(str).value_counts().to_dict()
        )

    team_all = team_df.to_dict(orient="records") if not team_df.empty else []
    unattended_rows = a._perf_unattended_assignment_rows(
        df_all,
        focus=ENGINEER,
        visits=visits,
        range_start=rs,
        range_end=re,
    )

    report = {
        "engineer": ENGINEER,
        "period": period,
        "range_utc": [rs.isoformat(), re.isoformat()],
        "attended": {
            "unique_cases": int(exec_metrics.get("total") or 0),
            "csm_rows": int(bundle.get("n_csm") or 0),
            "sales_rows": int(bundle.get("n_sales") or 0),
            "resolution_rate_pct": int(exec_metrics.get("resolution_rate") or 0),
            "closure_breakdown": closure_breakdown,
        },
        "assignment": {k: int(v) for k, v in focus_metrics.items() if isinstance(v, (int, float))},
        "derived_ratios": derived,
        "reconciliation": {k: int(v) for k, v in recon.items()},
        "handed_off_ticket_ids_sample": handed_ids[:30],
        "handed_off_count": len(handed_ids),
        "still_open_assigned_ids_sample": still_open[:30],
        "team_table_row": team_row,
        "team_assignment_all_engineers": team_all,
        "unattended_assignment_cases_in_period": unattended_rows,
        "detail_ticket_ids": sorted(detail_df["ID"].astype(str).unique().tolist())
        if not detail_df.empty
        else [],
    }
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
