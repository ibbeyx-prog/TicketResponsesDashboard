import os, sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import app as a

rs, re, _, _ = a._perf_calendar_month_for_date(date(2026, 9, 1))
FOCUS = "@Dissiby"
df = a._fetch_tickets_cached()
csm = a._perf_csm_attended_in_week(df, range_start=rs, range_end=re)
ids = [
    "100337966",
    "100739796",
    "100716668",
    "2022030872000312",
    "100733721",
    "100745742",
]
for tn in ids:
    row = df.loc[df["ticket_number"].astype(str).eq(tn)]
    if row.empty:
        print(tn, "missing")
        continue
    r = row.iloc[0]
    in_pool = (
        tn in csm["ticket_number"].astype(str).tolist() if not csm.empty else False
    )
    ass = a._perf_attended_credit_assignees(r)
    cred = a._perf_row_attended_credited_to_person(r, FOCUS)
    print(
        tn,
        "status",
        r.get("status"),
        "a1",
        r.get("assigned_to"),
        "a2",
        r.get("assigned_to_2"),
        "frb",
        r.get("field_responded_by"),
        "shared",
        a.get_credit_type(r),
        "in_pool",
        in_pool,
        "credit_to",
        ass,
        "dissiby_ok",
        cred,
    )
