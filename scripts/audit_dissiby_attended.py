"""Deep audit: @Dissiby attended vs assigned vs visits (Performance range)."""
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

FOCUS = sys.argv[1] if len(sys.argv) > 1 else "@Dissiby"


def main() -> None:
    # Match screenshot: likely calendar month — use September 2026 if perf preset is month
    from datetime import date

    rs, re, d0, d1 = a._perf_calendar_month_for_date(date(2026, 9, 1))
    period = f"{d0} – {d1} ({a.LOCAL_TZ_LABEL})"

    df_all = a._fetch_tickets_cached()
    sales_all = a._fetch_sales_cases_cached()
    visits = a._perf_load_overview_visits_history(df_all)

    team_df = a._perf_team_assignment_summary_df(
        df_all, sales_all, range_start=rs, range_end=re
    )
    team_row = None
    fk = a._perf_person_credit_key(FOCUS)
    if not team_df.empty:
        for _, row in team_df.iterrows():
            if a._perf_person_credit_key(row.get("Engineer")) == fk:
                team_row = row.to_dict()
                break

    bundle = a._perf_weekly_attended_bundle(
        df_all,
        sales_all if sales_all is not None else pd.DataFrame(),
        range_start=rs,
        range_end=re,
        focus=FOCUS,
    )
    detail = bundle.get("detail")
    detail_df = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()
    detail_focus = a._perf_filter_attended_detail_by_focus(detail_df, FOCUS)

    attended_ids = sorted(
        a._perf_attended_ticket_ids_credited_to(
            df_all, sales_all, focus=FOCUS, range_start=rs, range_end=re
        )
    )

    assign_m = a._perf_engineer_range_assignment_metrics(
        df_all,
        sales_all,
        focus=FOCUS,
        range_start=rs,
        range_end=re,
        visits_history=visits,
    )
    assigned_ids = assign_m.get("_assigned_ids", frozenset())
    if not isinstance(assigned_ids, frozenset):
        assigned_ids = frozenset(assigned_ids)

    # Assigned in range + field response but NOT in attended credit
    csm_all = a._perf_csm_attended_in_week(df_all, range_start=rs, range_end=re)
    sales_att = a._perf_sales_attended_in_week(
        sales_all if sales_all is not None else pd.DataFrame(),
        range_start=rs,
        range_end=re,
    )

    gaps: list[dict[str, object]] = []
    tickets_by_num = {
        str(r.get("ticket_number") or "").strip(): r
        for _, r in df_all.iterrows()
        if str(r.get("ticket_number") or "").strip()
    }

    for tn in sorted(assigned_ids):
        if tn in attended_ids:
            continue
        row = tickets_by_num.get(tn)
        if row is None:
            gaps.append({"id": tn, "track": "?", "reason": "not in ticket snapshot"})
            continue
        status = str(row.get("status") or "")
        has_resp = a._ticket_row_has_field_response(row)
        r2 = row.copy()
        r2["_attended_status"] = status
        ts = a._perf_weekly_attended_ts(pd.DataFrame([r2])).iloc[0]
        in_attended_pool = tn in set(
            csm_all["ticket_number"].astype(str).str.strip().tolist()
            if not csm_all.empty
            else []
        )
        gaps.append(
            {
                "id": tn,
                "track": "CSM",
                "status": status,
                "has_field_response": has_resp,
                "responded_at": str(row.get("responded_at") or "")[:19],
                "in_csm_attended_pool": in_attended_pool,
                "attended_ts_in_range": bool(
                    pd.notna(ts) and rs <= ts <= re
                )
                if pd.notna(ts)
                else False,
                "why_not_attended": _why_not_attended(row, rs, re, csm_all),
            }
        )

    # Attended but not assigned in range
    outside = sorted(set(attended_ids) - set(assigned_ids))

    # Responded visits in range for dissiby not in attended
    prepared = a._perf_prepare_visits_df(visits) if not visits.empty else pd.DataFrame()
    visit_responded_ids: set[str] = set()
    if not prepared.empty:
        vs = a._parse_ts(prepared["visit_start"])
        in_range = vs.notna() & (vs >= rs) & (vs <= re)
        key = a._perf_norm_member(FOCUS)
        for _, v in prepared.loc[in_range].iterrows():
            if a._perf_norm_member(v.get("assignee")) != key:
                continue
            if str(v.get("outcome") or "").strip() != "responded":
                continue
            tn = str(v.get("ticket_number") or "").strip()
            if tn:
                visit_responded_ids.add(tn)

    visit_not_attended = sorted(visit_responded_ids - set(attended_ids))

    attended_rows = []
    if not detail_focus.empty:
        id_first = detail_focus.drop_duplicates(subset=["ID"], keep="first")
        for _, r in id_first.iterrows():
            attended_rows.append(
                {
                    "id": str(r.get("ID")),
                    "track": str(r.get("Track")),
                    "closure": str(r.get("Closure")),
                    "status": str(r.get("Status")),
                    "activity": str(r.get("Activity (local)")),
                    "in_assigned_range": str(r.get("ID")) in assigned_ids,
                }
            )

    print(
        json.dumps(
            {
                "engineer": FOCUS,
                "period": period,
                "range_utc": [rs.isoformat(), re.isoformat()],
                "team_table_row": team_row,
                "bundle_total_focus": int(bundle.get("total") or 0),
                "attended_ids_count": len(attended_ids),
                "assigned_unique": len(assigned_ids),
                "assignment_tasks": int(assign_m.get("assignment_cycles_in_range") or 0),
                "unattended_cycles": int(
                    a._perf_overview_unattended_counts_by_credit(
                        df_all,
                        focus=FOCUS,
                        visits=visits,
                        range_start=rs,
                        range_end=re,
                    ).get(fk, 0)
                ),
                "attended_ticket_list": attended_rows,
                "attended_ids": attended_ids,
                "assigned_not_attended_count": len(gaps),
                "assigned_not_attended_sample": gaps[:25],
                "attended_outside_assigned": outside,
                "visit_responded_not_attended": visit_not_attended[:25],
                "explain": (
                    "Attended counts unique cases reaching attended STATUS with timestamp IN RANGE "
                    "(Needs Review+response, Investigation, On Hold, Resolved). "
                    "Unique assigned counts tickets/cycles ASSIGNED in range — many stay Daily Task until later. "
                    "Attended + Unattended does NOT sum to Unique tickets (different units)."
                ),
            },
            indent=2,
            default=str,
        )
    )


def _why_not_attended(
    row: pd.Series,
    rs: pd.Timestamp,
    re: pd.Timestamp,
    csm_pool: pd.DataFrame,
) -> str:
    status = str(row.get("status") or "").strip()
    tn = str(row.get("ticket_number") or "").strip()
    if status == a.STATUS_DAILY_TASK:
        if a._ticket_row_has_field_response(row):
            return "still Daily Task despite field data — status/timestamp may not have moved to Open"
        return "Daily Task, no field response yet"
    if status.casefold() == "open":
        if not a._ticket_row_has_field_response(row):
            return "Open without field response (excluded from attended)"
        ts = a._parse_ts(row.get("responded_at"))
        if pd.isna(ts) or ts < rs or ts > re:
            return f"Needs Review but responded_at outside range ({row.get('responded_at')})"
        return "Open+response in range but not credited — check assignee/filter"
    if tn and not csm_pool.empty and tn not in csm_pool["ticket_number"].astype(str).tolist():
        ts = a._parse_ts(row.get("updated_at"))
        if pd.notna(ts) and (ts < rs or ts > re):
            return f"{status}: status activity outside range"
        return f"{status}: not in attended pool for range"
    return f"{status}: still open work or timing"


if __name__ == "__main__":
    main()
