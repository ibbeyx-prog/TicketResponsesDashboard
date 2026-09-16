"""Audit how Performance *attended* is counted vs visits / handled (live Supabase)."""
from __future__ import annotations

import json
import os
import sys
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


def main() -> None:
    rs, re = a._get_perf_range()
    period = (
        f"{rs.tz_convert(a.LOCAL_TZ).strftime('%Y-%m-%d')} – "
        f"{re.tz_convert(a.LOCAL_TZ).strftime('%Y-%m-%d')} ({a.LOCAL_TZ_LABEL})"
    )

    df_all = a._fetch_tickets_cached()
    sales_all = a._fetch_sales_cases_cached()
    visits = a._perf_load_overview_visits_history(df_all)

    team_df = a._perf_team_assignment_summary_df(
        df_all, sales_all, range_start=rs, range_end=re
    )
    attended_map = a._perf_attended_unique_counts_by_credit(
        df_all, sales_all, range_start=rs, range_end=re
    )

    visit_responded: dict[str, int] = {}
    prepared = a._perf_prepare_visits_df(visits) if not visits.empty else pd.DataFrame()
    if not prepared.empty and "visit_start" in prepared.columns:
        vs = a._parse_ts(prepared["visit_start"])
        in_range = vs.notna() & (vs >= rs) & (vs <= re)
        responded = prepared.loc[
            in_range & prepared["outcome"].astype(str).str.strip().eq("responded")
        ]
        for assignee, grp in responded.groupby("assignee", sort=False):
            ck = a._perf_person_credit_key(assignee)
            if "ticket_number" in grp.columns:
                visit_responded[ck] = int(grp["ticket_number"].astype(str).nunique())
            else:
                visit_responded[ck] = int(len(grp))

    bundle = a._perf_weekly_attended_bundle(
        df_all,
        sales_all if sales_all is not None else pd.DataFrame(),
        range_start=rs,
        range_end=re,
        focus="All",
    )
    detail = bundle.get("detail")
    detail_df = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()

    by_status: dict[str, int] = {}
    by_closure: dict[str, int] = {}
    if not detail_df.empty:
        id_first = detail_df.drop_duplicates(subset=["ID"], keep="first")
        if "Status" in id_first.columns:
            by_status = id_first["Status"].astype(str).value_counts().to_dict()
        if "Closure" in id_first.columns:
            by_closure = id_first["Closure"].astype(str).value_counts().to_dict()

    engineers: list[dict[str, object]] = []
    if not team_df.empty:
        for _, row in team_df.iterrows():
            eng = str(row.get("Engineer") or "")
            ck = a._perf_person_credit_key(eng)
            attended_col = int(row.get("Attended") or 0)
            attended_map_n = int(attended_map.get(ck, 0))
            ids = a._perf_attended_ticket_ids_credited_to(
                df_all,
                sales_all,
                focus=eng,
                range_start=rs,
                range_end=re,
            )
            engineers.append(
                {
                    "engineer": eng,
                    "team_table_attended": attended_col,
                    "attended_map": attended_map_n,
                    "attended_ids_count": len(ids),
                    "visit_responded_tickets": int(visit_responded.get(ck, 0)),
                    "unique_assigned": int(row.get("Unique tickets") or 0),
                    "unattended": int(row.get("Unattended") or 0),
                    "match": attended_col == attended_map_n == len(ids),
                }
            )

    report = {
        "period": period,
        "range_utc": [rs.isoformat(), re.isoformat()],
        "rules_version": "Performance attended bundle (_perf_csm_attended_in_week + sales)",
        "team_unique_attended_cases": int(detail_df["ID"].astype(str).nunique())
        if not detail_df.empty
        else 0,
        "bundle_total_unique": int(bundle.get("total") or 0),
        "by_attended_status_unique_cases": by_status,
        "by_closure_unique_cases": by_closure,
        "engineer_reconciliation": engineers,
        "notes": [
            "Attended != Visit responded: visits count assignee on responded cycles; attended uses ticket status + responded_at/follow_up_at/updated_at in range.",
            "Attended != Unattended: unattended is assignment cycles without response; attended is cases reaching attended statuses in range.",
            "Shared co-assign: same case ID can credit two engineers in Attended column; team unique case total dedupes by ID.",
        ],
    }
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
