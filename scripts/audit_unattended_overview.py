"""Audit Performance Overview Unattended counts against live Supabase data."""
from __future__ import annotations

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
    df_all = a._fetch_tickets_cached()
    visits = a._perf_load_overview_visits_history(df_all)
    counts = a._perf_overview_unattended_counts_by_credit(
        df_all, focus="All", visits=visits
    )
    flagged = (
        int(a._ticket_marked_unattended_mask(df_all).sum()) if not df_all.empty else 0
    )

    prepared = a._perf_prepare_visits_df(visits) if not visits.empty else pd.DataFrame()
    tickets_by_num: dict[str, pd.Series] = {}
    for _, row in df_all.iterrows():
        tn = str(row.get("ticket_number") or "").strip()
        if tn:
            tickets_by_num[tn] = row

    details: dict[str, list[dict[str, object]]] = {k: [] for k in counts}
    seen: set[str] = set()

    if not prepared.empty:
        for _, visit in prepared.iterrows():
            tn = str(visit.get("ticket_number") or "").strip()
            ticket_row = tickets_by_num.get(tn)
            if not a._perf_overview_visit_cycle_unattended(visit, ticket_row):
                continue
            dedupe = a._perf_overview_unattended_cycle_dedupe_key(visit, tn)
            if dedupe in seen:
                continue
            for ck in a._perf_visit_cycle_unattended_credit_keys(visit):
                if ck not in details:
                    details[ck] = []
                seen.add(dedupe)
                details[ck].append(
                    {
                        "ticket": tn,
                        "outcome": str(visit.get("outcome") or ""),
                        "visit_start": str(visit.get("visit_start") or "")[:19],
                        "is_active": bool(visit.get("is_active"))
                        if "is_active" in visit.index
                        else None,
                        "status": str(ticket_row.get("status") if ticket_row is not None else ""),
                    }
                )

    ticket_nums = set(a._perf_overview_ticket_numbers(df_all))
    tickets_with_visits = (
        set(prepared["ticket_number"].astype(str).str.strip())
        if not prepared.empty
        else set()
    )
    for tn in ticket_nums:
        row = tickets_by_num[tn]
        if not a.is_daily_task_status(row.get("status")):
            continue
        if a._ticket_has_field_response_since_assign_row(row):
            continue
        primary = a._perf_person_credit_key(a._perf_norm_member(row.get("assigned_to")))
        if not primary or primary == "(unknown)":
            continue
        if tn in tickets_with_visits and not prepared.empty:
            active = False
            sub = prepared[prepared["ticket_number"].astype(str).str.strip().eq(tn)]
            for _, visit in sub.iterrows():
                if str(visit.get("outcome") or "").strip() != "assigned":
                    continue
                if not (bool(visit.get("is_active")) if "is_active" in visit.index else False):
                    continue
                if a._perf_person_credit_key(a._perf_norm_member(visit.get("assignee"))) == primary:
                    active = True
                    break
            if active:
                continue
        assigned_ts = a._parse_ts(row.get("last_assigned_at"))
        assign_day = (
            assigned_ts.tz_convert(a.LOCAL_TZ).date().isoformat()
            if pd.notna(assigned_ts)
            else str(row.get("last_assigned_at") or "")[:10]
        )
        dedupe = f"pending:{tn}:{primary}:{assign_day}"
        if dedupe in seen:
            continue
        if primary not in details:
            details[primary] = []
        seen.add(dedupe)
        details[primary].append(
            {
                "ticket": tn,
                "outcome": "pending_daily_task",
                "visit_start": str(row.get("last_assigned_at") or "")[:19],
                "is_active": None,
                "status": str(row.get("status") or ""),
            }
        )

    print("=== OVERVIEW UNATTENDED AUDIT ===")
    print(f"Flagged backlog (marked_unattended_at): {flagged}")
    print(f"Overview assignment cases total: {sum(counts.values())}")
    print()
    for eng in sorted(counts, key=lambda k: (-counts[k], k.lower())):
        print(f"{eng}: {counts[eng]} cases")

    dissiby_keys = [k for k in counts if "dissiby" in k.lower()]
    for dk in dissiby_keys:
        items = details.get(dk, [])
        print()
        print(f"=== {dk} DETAIL ({len(items)} cases) ===")
        by_outcome: dict[str, int] = {}
        for it in items:
            o = str(it["outcome"])
            by_outcome[o] = by_outcome.get(o, 0) + 1
        print("By source:", by_outcome)
        for it in items:
            print(
                f"  {it['ticket']} | {it['outcome']} | {it['visit_start']} | status={it['status']}"
            )

    print()
    print("=== FLAGGED BACKLOG BY ASSIGNEE (marked_unattended_at) ===")
    marked = df_all.loc[a._ticket_marked_unattended_mask(df_all)] if not df_all.empty else pd.DataFrame()
    if not marked.empty:
        prim = marked["assigned_to"].map(a._perf_norm_member).value_counts()
        for eng, n in prim.items():
            print(f"  primary {eng}: {n}")
        if "assigned_to_2" in marked.columns:
            sec = marked["assigned_to_2"].map(a._perf_norm_member)
            sec = sec[sec.notna() & ~sec.astype(str).eq("(unknown)")]
            if not sec.empty:
                print("  secondary (shared flag on ticket, not overview credit):")
                for eng, n in sec.value_counts().items():
                    print(f"    {eng}: {n}")

    print()
    print("=== ALL ENGINEERS WITH OVERVIEW UNATT > 0 ===")
    if not prepared.empty:
        for eng in sorted(prepared["assignee"].dropna().unique(), key=str.lower):
            sub = prepared[prepared["assignee"] == eng]
            ck = a._perf_person_credit_key(a._perf_norm_member(eng))
            overview_n = counts.get(ck, 0)
            reassigned_n = int(sub["outcome"].astype(str).eq("reassigned").sum())
            unattended_n = int(sub["outcome"].astype(str).eq("unattended").sum())
            active_assigned = int(
                (
                    sub["outcome"].astype(str).eq("assigned")
                    & sub.get("is_active", pd.Series(False, index=sub.index)).fillna(False)
                ).sum()
            )
            print(
                f"  {ck}: overview={overview_n} | visits reassigned={reassigned_n} "
                f"unattended={unattended_n} active_assigned={active_assigned}"
            )


if __name__ == "__main__":
    main()
