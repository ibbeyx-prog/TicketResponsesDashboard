"""
Streamlit control room for ticket ops (reads ``tickets`` from Supabase).

Run: ``streamlit run app.py``

Requires the same env as the bot: ``SUPABASE_URL``, ``SUPABASE_KEY``,
optional ``TICKETS_TABLE`` (default ``tickets``). Copy ``.env.example`` to
``.env`` in this folder (UTF-8).
"""

from __future__ import annotations

import os
from datetime import timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client

LOCAL_TZ = timezone(timedelta(hours=5))
LOCAL_TZ_LABEL = "UTC+5"
_TS_COLS: tuple[str, ...] = ("created_at", "updated_at", "responded_at")

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, encoding="utf-8-sig")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
TICKETS_TABLE = (os.getenv("TICKETS_TABLE") or "tickets").strip()


@st.cache_resource(show_spinner=False)
def _get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


_ORDER_COLUMN_CANDIDATES: tuple[str, ...] = ("updated_at", "responded_at", "created_at")


@st.cache_resource(show_spinner=False)
def _get_order_column() -> str:
    """Pick the best existing timestamp column to sort by.

    Cached so we only probe once per Streamlit session. Falls back to
    ``created_at``, which is part of the documented DDL.
    """
    client = _get_supabase_client()
    for col in _ORDER_COLUMN_CANDIDATES:
        try:
            client.table(TICKETS_TABLE).select(col).limit(1).execute()
            return col
        except Exception:
            continue
    return "created_at"


def _fetch_tickets() -> pd.DataFrame:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()
    client = _get_supabase_client()
    order_col = _get_order_column()
    res = client.table(TICKETS_TABLE).select("*").order(order_col, desc=True).execute()
    rows = res.data or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _to_local(series: pd.Series) -> pd.Series:
    return _parse_ts(series).dt.tz_convert(LOCAL_TZ)


def _format_local(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in _TS_COLS:
        if col in out.columns:
            out[col] = _to_local(out[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return out


def _format_duration(seconds: float | None) -> str:
    if seconds is None or pd.isna(seconds) or seconds < 0:
        return "—"
    if seconds < 3600:
        return f"{int(seconds // 60)} min"
    if seconds < 86400:
        h = seconds / 3600
        return f"{h:.1f} h"
    d = int(seconds // 86400)
    h = int((seconds % 86400) // 3600)
    return f"{d} d {h} h"


DEFAULT_REFRESH_MINUTES = 5
MIN_REFRESH_MINUTES = 1
MAX_REFRESH_MINUTES = 60


def _sidebar_controls() -> tuple[bool, int]:
    """Return (auto_enabled, interval_minutes)."""
    with st.sidebar:
        st.header("Refresh")
        auto = st.toggle(
            "Auto-refresh",
            value=True,
            help=f"Re-fetch from Supabase on a timer ({LOCAL_TZ_LABEL}).",
        )
        interval_minutes = st.slider(
            "Interval (minutes)",
            min_value=MIN_REFRESH_MINUTES,
            max_value=MAX_REFRESH_MINUTES,
            value=DEFAULT_REFRESH_MINUTES,
            step=1,
            disabled=not auto,
        )
        if st.button("Refresh Data", use_container_width=True):
            st.rerun()
    return auto, int(interval_minutes)


def main() -> None:
    st.set_page_config(page_title="Ticket Control Room", layout="wide")
    st.title("Ticket Control Room")

    if not SUPABASE_URL or not SUPABASE_KEY:
        missing = [k for k, v in (("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_KEY", SUPABASE_KEY)) if not v]
        st.error(
            f"Missing {', '.join(missing)}. "
            f"Checked process env and `{_ENV_PATH}` "
            f"(exists={_ENV_PATH.exists()}). "
            "Copy `.env.example` to `.env` and fill in values."
        )
        return

    auto, interval_minutes = _sidebar_controls()
    run_every = timedelta(minutes=interval_minutes) if auto else None

    @st.fragment(run_every=run_every)
    def _dashboard_fragment() -> None:
        _render_dashboard(auto=auto, interval_minutes=interval_minutes)

    _dashboard_fragment()


def _render_dashboard(*, auto: bool, interval_minutes: int) -> None:
    now_local = pd.Timestamp.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    unit = "minute" if interval_minutes == 1 else "minutes"
    refresh_note = (
        f"auto-refresh **every {interval_minutes} {unit}**"
        if auto
        else "auto-refresh **off**"
    )
    st.caption(
        f"Table: `{TICKETS_TABLE}` · times in **{LOCAL_TZ_LABEL}** · "
        f"now {now_local} · {refresh_note}"
    )

    df = _fetch_tickets()
    if df.empty:
        st.warning("No rows returned (empty table or connection issue).")
        return

    if "status" not in df.columns:
        st.error("The tickets table has no `status` column.")
        return

    status = df["status"].astype(str).str.strip()
    pending_mask = status.eq("Pending")
    completed_mask = status.eq("Completed")

    total_pending = int(pending_mask.sum())

    avg_seconds: float | None = None
    completed_df = df[completed_mask].copy()
    if not completed_df.empty and "created_at" in completed_df.columns and "responded_at" in completed_df.columns:
        c = _parse_ts(completed_df["created_at"])
        r = _parse_ts(completed_df["responded_at"])
        valid = c.notna() & r.notna()
        if valid.any():
            delta = (r[valid] - c[valid]).dt.total_seconds()
            avg_seconds = float(delta.mean())

    st.header("Overview")
    m1, m2 = st.columns(2)
    with m1:
        st.metric("Total Pending", f"{total_pending:,}")
    with m2:
        st.metric("Average completion time", _format_duration(avg_seconds))

    st.divider()

    tab_pending, tab_completed = st.tabs(["Pending", "Completed"])

    with tab_pending:
        pend = df[pending_mask].copy()
        st.subheader("Pending tickets")
        if pend.empty:
            st.info("No pending tickets.")
        else:
            if "created_at" in pend.columns:
                pend["_created"] = _parse_ts(pend["created_at"])
                now_local = pd.Timestamp.now(tz=LOCAL_TZ)
                pend["_stale"] = pend["_created"].notna() & (
                    (now_local - pend["_created"]) > pd.Timedelta(hours=24)
                )
            else:
                pend["_stale"] = False

            view_cols = [c for c in pend.columns if not c.startswith("_")]
            view = _format_local(pend[view_cols])

            def _row_red(_row: pd.Series) -> list[str]:
                stale = bool(pend.loc[_row.name, "_stale"]) if "_stale" in pend.columns else False
                color = "background-color: #ffcccc" if stale else ""
                return [color] * len(_row)

            try:
                styled = view.style.apply(_row_red, axis=1)
                st.dataframe(styled, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(view, use_container_width=True, hide_index=True)
            st.caption(
                f"Rows with a **red** background are pending for **more than 24 hours** "
                f"since `created_at` (compared against current {LOCAL_TZ_LABEL} time)."
            )

    with tab_completed:
        done = df[completed_mask].copy()
        st.subheader("Completed tickets")
        if done.empty:
            st.info("No completed tickets.")
        else:
            show_cols = [
                c
                for c in [
                    "ticket_number",
                    "assigned_to",
                    "task_category",
                    "field_response",
                    "photo_url",
                    "created_at",
                    "responded_at",
                ]
                if c in done.columns
            ]
            st.dataframe(
                _format_local(done[show_cols]),
                use_container_width=True,
                hide_index=True,
            )

            st.subheader("Field photos")
            if "photo_url" not in done.columns:
                st.caption("No `photo_url` column.")
            else:
                ph = done["photo_url"].astype(str).str.strip()
                photo_rows = done[ph.str.startswith("http")]
                if photo_rows.empty:
                    st.caption("No `photo_url` values to display.")
                else:
                    for _, row in photo_rows.iterrows():
                        url = str(row.get("photo_url") or "").strip()
                        if not url or not url.startswith("http"):
                            continue
                        tid = row.get("ticket_number", "—")
                        cap = row.get("field_response") or ""
                        with st.container():
                            st.markdown(f"**Ticket `{tid}`**")
                            try:
                                st.image(
                                    url,
                                    caption=str(cap)[:500] if cap else None,
                                    use_container_width=True,
                                )
                            except Exception as exc:
                                st.warning(f"Could not load image for ticket `{tid}`: {exc}")
                            st.divider()


if __name__ == "__main__":
    main()
