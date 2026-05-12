"""
Streamlit control room for ticket ops (reads ``tickets`` from Supabase).

Run: ``streamlit run app.py``

Requires the same env as the bot: ``SUPABASE_URL``, ``SUPABASE_KEY``,
optional ``TICKETS_TABLE`` (default ``tickets``).

Configuration sources, checked in this order:
  1. Process environment (set by the shell, Railway, Docker, etc.).
  2. ``.env`` file next to this script (auto-loaded by python-dotenv).
  3. ``st.secrets`` -- used by Streamlit Community Cloud, where the
     "Secrets" pane in app settings is the only place to put credentials.

For Streamlit Cloud, paste this TOML in *Manage app -> Settings -> Secrets*::

    SUPABASE_URL = "https://<project>.supabase.co"
    SUPABASE_KEY = "<service-role-or-anon-key>"
    DASHBOARD_PASSWORD = "<pick a strong password>"
    # TICKETS_TABLE = "tickets"

Authentication: a single shared password gates the dashboard. It is read from
``DASHBOARD_PASSWORD`` (env / ``st.secrets``). If unset, the app refuses to
render -- never serve this dashboard unauthenticated.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from datetime import timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client

LOCAL_TZ = timezone(timedelta(hours=5))
LOCAL_TZ_LABEL = "UTC+5"
_TS_COLS: tuple[str, ...] = (
    "created_at",
    "updated_at",
    "responded_at",
    "last_assigned_at",
    "timestamp",
)

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH, encoding="utf-8-sig")


def _read_setting(key: str, default: str = "") -> str:
    """Return ``key`` from process env, falling back to ``st.secrets``.

    ``st.secrets`` raises ``StreamlitSecretNotFoundError`` when no secrets
    file/section exists (typical on a fresh local run); we treat that the
    same as "not set".
    """
    value = os.getenv(key)
    if value is None or value == "":
        try:
            value = st.secrets[key]  # type: ignore[index]
        except Exception:
            value = default
    return str(value or default).strip()


SUPABASE_URL = _read_setting("SUPABASE_URL").rstrip("/")
SUPABASE_KEY = _read_setting("SUPABASE_KEY")
TICKETS_TABLE = _read_setting("TICKETS_TABLE", "tickets_active") or "tickets_active"
ATTENDANCE_LOGS_TABLE = (
    _read_setting("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")
    or "ticket_attendance_logs"
)

# Session keys — namespaced so we never collide with other widgets / demos,
# and so a stale boolean from an older app version cannot bypass the gate.
_AUTH_OK_KEY = "_ticket_dashboard_auth_ok"
_AUTH_PWD_VER_KEY = "_ticket_dashboard_auth_pwd_ver"


def _password_fingerprint(secret: str) -> str:
    """Opaque token derived from the configured password (not reversible).

    Stored alongside the session flag so rotating ``DASHBOARD_PASSWORD``
    invalidates existing browser sessions without needing server-side logout.
    """
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _read_dashboard_password() -> str:
    """Read ``DASHBOARD_PASSWORD`` after Streamlit has started (env + secrets)."""
    return _read_setting("DASHBOARD_PASSWORD")


def _check_password() -> None:
    """Block until the viewer has a valid password session.

    ``main()`` must call ``st.set_page_config`` **before** this function so
    Streamlit's "first command" rule is satisfied even when we read
    ``st.session_state`` here.

    The shared password is read from ``DASHBOARD_PASSWORD`` (env / ``st.secrets``).
    If unset, the dashboard refuses to render — failing closed.
    """
    configured_pw = _read_dashboard_password()
    if not configured_pw:
        st.error("`DASHBOARD_PASSWORD` is not configured — the dashboard is locked.")
        on_cloud = str(_ENV_PATH).startswith("/mount/src/")
        if on_cloud:
            st.info(
                "Open *Manage app -> Settings -> Secrets* and add:\n\n"
                "```toml\n"
                'DASHBOARD_PASSWORD = "<pick a strong password>"\n'
                "```\n"
                "Save -- the app reboots automatically."
            )
        else:
            st.info(
                "Add `DASHBOARD_PASSWORD=<password>` to your `.env` "
                "(or export it in your shell) and restart the dashboard."
            )
        st.stop()

    fp = _password_fingerprint(configured_pw)
    if (
        st.session_state.get(_AUTH_OK_KEY) is True
        and st.session_state.get(_AUTH_PWD_VER_KEY) == fp
    ):
        return

    st.session_state.pop(_AUTH_OK_KEY, None)
    st.session_state.pop(_AUTH_PWD_VER_KEY, None)

    st.title("Ticket Control Room")
    st.caption("Sign in to continue.")

    with st.form("login_form", clear_on_submit=False):
        pwd = st.text_input("Password", type="password", autocomplete="current-password")
        submitted = st.form_submit_button("Sign in", use_container_width=True)

    if submitted:
        if hmac.compare_digest(pwd, configured_pw):
            st.session_state[_AUTH_OK_KEY] = True
            st.session_state[_AUTH_PWD_VER_KEY] = fp
            st.rerun()
        else:
            st.error("Incorrect password.")

    st.stop()


@st.cache_resource(show_spinner=False)
def _get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


_ORDER_COLUMN_CANDIDATES: tuple[str, ...] = (
    "last_assigned_at",
    "updated_at",
    "responded_at",
    "created_at",
)


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


def _fetch_attendance(
    *,
    ticket_number: str | None = None,
    member_query: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Read attendance logs.

    At least one of ``ticket_number`` / ``member_query`` should be set; if both
    are ``None`` the function returns the latest ``limit`` rows for general
    browsing. ``member_query`` is matched case-insensitively against
    ``member_username`` using ``ilike``.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()
    client = _get_supabase_client()
    q = client.table(ATTENDANCE_LOGS_TABLE).select("*")
    if ticket_number:
        q = q.eq("ticket_number", ticket_number.strip())
    if member_query:
        cleaned = member_query.strip().lstrip("@")
        if cleaned:
            q = q.ilike("member_username", f"%{cleaned}%")
    try:
        res = q.order("timestamp", desc=True).limit(limit).execute()
    except Exception:
        return pd.DataFrame()
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

DEFAULT_LOOKBACK_DAYS = 7
MIN_LOOKBACK_DAYS = 1
MAX_LOOKBACK_DAYS = 30


def _sidebar_controls() -> tuple[bool, int, int]:
    """Return (auto_enabled, interval_minutes, lookback_days)."""
    with st.sidebar:
        st.header("Filters")
        lookback_days = st.slider(
            "Days to Look Back",
            min_value=MIN_LOOKBACK_DAYS,
            max_value=MAX_LOOKBACK_DAYS,
            value=DEFAULT_LOOKBACK_DAYS,
            step=1,
            help=(
                "Only show tickets whose **last assignment** happened within "
                "this window. Counts and tabs honour this filter."
            ),
        )

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
        st.divider()
        if st.button("Log out", use_container_width=True):
            st.session_state.pop(_AUTH_OK_KEY, None)
            st.session_state.pop(_AUTH_PWD_VER_KEY, None)
            st.rerun()
    return auto, int(interval_minutes), int(lookback_days)


def main() -> None:
    # Must be the first Streamlit command every run (login + dashboard).
    st.set_page_config(page_title="Ticket Control Room", layout="wide")

    _check_password()

    st.title("Ticket Control Room")

    if not SUPABASE_URL or not SUPABASE_KEY:
        missing = [k for k, v in (("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_KEY", SUPABASE_KEY)) if not v]
        on_cloud = str(_ENV_PATH).startswith("/mount/src/")
        st.error(f"Missing {', '.join(missing)}.")
        if on_cloud:
            st.info(
                "Detected Streamlit Community Cloud. "
                "Open *Manage app -> Settings -> Secrets* and paste:\n\n"
                "```toml\n"
                'SUPABASE_URL = "https://<project>.supabase.co"\n'
                'SUPABASE_KEY = "<service-role-or-anon-key>"\n'
                "```\n"
                "Save -- the app reboots automatically."
            )
        else:
            st.info(
                f"Checked process env and `{_ENV_PATH}` "
                f"(exists={_ENV_PATH.exists()}). "
                "Copy `.env.example` to `.env` and fill in values, "
                "or set the variables in your shell before running `streamlit run app.py`."
            )
        return

    auto, interval_minutes, lookback_days = _sidebar_controls()
    run_every = timedelta(minutes=interval_minutes) if auto else None

    @st.fragment(run_every=run_every)
    def _dashboard_fragment() -> None:
        _render_dashboard(
            auto=auto,
            interval_minutes=interval_minutes,
            lookback_days=lookback_days,
        )

    _dashboard_fragment()


def _apply_lookback(df: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Return rows whose ``last_assigned_at`` (or fallbacks) is within window.

    Falls back to ``updated_at`` then ``created_at`` so legacy rows that
    pre-date the ``last_assigned_at`` column are still surfaced rather than
    silently dropped from the dashboard.
    """
    if df.empty:
        return df
    for col in ("last_assigned_at", "updated_at", "created_at"):
        if col in df.columns:
            ts = _parse_ts(df[col])
            cutoff = pd.Timestamp.now(tz=LOCAL_TZ).tz_convert("UTC") - pd.Timedelta(days=lookback_days)
            mask = ts.notna() & (ts >= cutoff)
            return df[mask].copy()
    return df


def _render_dashboard(
    *,
    auto: bool,
    interval_minutes: int,
    lookback_days: int,
) -> None:
    now_local = pd.Timestamp.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    unit = "minute" if interval_minutes == 1 else "minutes"
    refresh_note = (
        f"auto-refresh **every {interval_minutes} {unit}**"
        if auto
        else "auto-refresh **off**"
    )
    day_word = "day" if lookback_days == 1 else "days"
    st.caption(
        f"Table: `{TICKETS_TABLE}` · history: `{ATTENDANCE_LOGS_TABLE}` · "
        f"window: **last {lookback_days} {day_word}** · "
        f"times in **{LOCAL_TZ_LABEL}** · now {now_local} · {refresh_note}"
    )

    df_all = _fetch_tickets()
    if df_all.empty:
        st.warning("No rows returned (empty table or connection issue).")
        _render_attendance_tab()  # search still useful even with no active rows
        return

    if "status" not in df_all.columns:
        st.error(f"The `{TICKETS_TABLE}` table has no `status` column.")
        return

    df = _apply_lookback(df_all, lookback_days)

    status = df["status"].astype(str).str.strip() if not df.empty else pd.Series(dtype=str)
    pending_mask = status.eq("Pending") if not df.empty else pd.Series(dtype=bool)
    completed_mask = status.eq("Completed") if not df.empty else pd.Series(dtype=bool)

    total_pending = int(pending_mask.sum()) if not df.empty else 0

    avg_seconds: float | None = None
    if not df.empty:
        completed_df = df[completed_mask].copy()
        if (
            not completed_df.empty
            and "created_at" in completed_df.columns
            and "responded_at" in completed_df.columns
        ):
            c = _parse_ts(completed_df["created_at"])
            r = _parse_ts(completed_df["responded_at"])
            valid = c.notna() & r.notna()
            if valid.any():
                delta = (r[valid] - c[valid]).dt.total_seconds()
                avg_seconds = float(delta.mean())

    st.header("Overview")
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Total Pending", f"{total_pending:,}")
    with m2:
        st.metric("Average completion time", _format_duration(avg_seconds))
    with m3:
        st.metric(f"Tickets in last {lookback_days} {day_word}", f"{len(df):,}")

    st.divider()

    tab_pending, tab_completed, tab_attendance = st.tabs(
        ["Pending", "Completed", "Attendance"]
    )

    with tab_pending:
        st.subheader("Pending tickets")
        st.caption(
            "Reassigned tickets reset to **Pending** here, even if they were "
            "previously Completed, and remain pending until a new response is "
            "logged in the attendance history."
        )
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            pend = df[pending_mask].copy()
            if pend.empty:
                st.info(f"No pending tickets in the last {lookback_days} {day_word}.")
            else:
                if "created_at" in pend.columns:
                    pend["_created"] = _parse_ts(pend["created_at"])
                    now_utc = pd.Timestamp.now(tz=LOCAL_TZ).tz_convert("UTC")
                    pend["_stale"] = pend["_created"].notna() & (
                        (now_utc - pend["_created"]) > pd.Timedelta(hours=24)
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
        st.subheader("Completed tickets")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            done = df[completed_mask].copy()
            if done.empty:
                st.info(f"No completed tickets in the last {lookback_days} {day_word}.")
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
                        "last_assigned_at",
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

    with tab_attendance:
        _render_attendance_tab()


def _render_attendance_tab() -> None:
    """Search bar + timeline view backed by ``ticket_attendance_logs``."""
    st.subheader("Attendance history")
    st.caption(
        "Search by **ticket number** (exact) or **@username** (case-insensitive, "
        "partial). Leave both blank to see the 100 most recent log entries across "
        "all tickets."
    )

    with st.form("attendance_search_form", clear_on_submit=False):
        c1, c2 = st.columns([2, 2])
        with c1:
            ticket_q = st.text_input(
                "Ticket number",
                placeholder="e.g. 1234567890123567",
                key="att_ticket_q",
            )
        with c2:
            member_q = st.text_input(
                "Member (@username)",
                placeholder="e.g. @Mular_as",
                key="att_member_q",
            )
        submitted = st.form_submit_button("Search", use_container_width=True)

    ticket_clean = (ticket_q or "").strip()
    member_clean = (member_q or "").strip()

    if submitted and not ticket_clean and not member_clean:
        st.info("Enter a ticket number or @username to refine the search.")

    logs = _fetch_attendance(
        ticket_number=ticket_clean or None,
        member_query=member_clean or None,
        limit=100 if not ticket_clean and not member_clean else 500,
    )

    if logs.empty:
        st.info("No attendance records match.")
        return

    show_cols = [
        c
        for c in (
            "timestamp",
            "ticket_number",
            "member_username",
            "action_type",
            "note",
            "photo_url",
        )
        if c in logs.columns
    ]
    table = _format_local(logs[show_cols])
    st.dataframe(table, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Timeline")
    for _, row in logs.iterrows():
        member = row.get("member_username") or "unknown"
        action = row.get("action_type") or "?"
        tid = row.get("ticket_number") or "—"
        when_local = ""
        ts_raw = row.get("timestamp")
        if pd.notna(ts_raw):
            try:
                when_local = pd.Timestamp(ts_raw).tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                when_local = str(ts_raw)

        with st.container(border=True):
            st.markdown(
                f"**{member}** · `{action}` · ticket `{tid}` · "
                f"{when_local} {LOCAL_TZ_LABEL}"
            )
            note = row.get("note")
            if isinstance(note, str) and note.strip():
                st.write(note)
            photo = row.get("photo_url")
            if isinstance(photo, str) and photo.startswith("http"):
                try:
                    st.image(photo, use_container_width=True)
                except Exception as exc:
                    st.warning(f"Could not load image: {exc}")


# Streamlit executes this file as the app script; do not hide ``main()`` behind
# ``if __name__ == "__main__"`` — some run modes leave ``__name__`` unset to
# ``"__main__"``, which skips the gate entirely and looks like "no password".
main()
