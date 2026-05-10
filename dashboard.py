import os
from datetime import timedelta
from urllib.parse import urlparse

import httpx
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client


load_dotenv()

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Missing SUPABASE_URL or SUPABASE_KEY in .env")
    st.stop()

_parsed = urlparse(SUPABASE_URL)
if _parsed.scheme not in ("https", "http") or not _parsed.netloc:
    st.error(
        "SUPABASE_URL must be a full URL, for example "
        "**https://your-project-ref.supabase.co** (no spaces or quotes)."
    )
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

REFRESH_EVERY = timedelta(minutes=5)

st.set_page_config(page_title="Ticket Responses Dashboard", layout="wide")
st.title("Ticket Responses Dashboard")
st.caption(
    "Data comes from field-team replies via the Telegram bot, stored in Supabase, "
    "and displayed here. This page reloads ticket data automatically every 5 minutes."
)


def _format_dt(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    out = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.fillna("")


@st.cache_data(ttl=REFRESH_EVERY)
def load_responses() -> pd.DataFrame:
    result = (
        supabase.table("ticket_responses")
        .select("ticket_id,user_handle,response_data,created_at")
        .order("created_at", desc=True)
        .execute()
    )
    rows = result.data or []
    base_cols = ["ticket_id", "user_handle", "response_data", "created_at"]
    if not rows:
        return pd.DataFrame(columns=base_cols)
    return pd.DataFrame(rows)


def _with_assign_dates(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Assign date = first reply time from this staff (@handle) on this ticket (full history)."""
    if df_raw.empty:
        return df_raw
    out = df_raw.copy()
    out["created_dt"] = pd.to_datetime(out["created_at"], errors="coerce")
    first_per_pair = (
        out.dropna(subset=["ticket_id", "user_handle"])
        .groupby(["ticket_id", "user_handle"], as_index=False)["created_dt"]
        .min()
        .rename(columns={"created_dt": "assign_dt"})
    )
    out = out.merge(first_per_pair, on=["ticket_id", "user_handle"], how="left")
    return out


def _ticket_option_label(opt: str) -> str:
    if opt == "All":
        return "All tickets"
    s = str(opt)
    return f'Ticket "{s}" \u2192 {len(s)} digits'


def _normalize_handle(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    s = str(value).strip()
    if s.lower() in ("nan", "none"):
        return ""
    return s.lstrip("@").lower()


def _user_option_label(opt: str) -> str:
    if opt == "All":
        return "All assignees (@user)"
    s = str(opt).strip()
    if not s:
        return opt
    return s if s.startswith("@") else f"@{s}"


def _safe_load_responses() -> pd.DataFrame | None:
    try:
        return load_responses()
    except (httpx.ConnectError, httpx.TimeoutException, OSError) as exc:
        st.error(
            "Cannot connect to Supabase (network or DNS). "
            "**Name or service not known** usually means the hostname in "
            "`SUPABASE_URL` is wrong or this environment cannot reach the internet.\n\n"
            f"- Check `SUPABASE_URL` in Secrets / `.env` (exactly "
            "`https://<project-ref>.supabase.co`).\n"
            "- In Codespaces / devcontainers, confirm outbound HTTPS is allowed.\n"
            "- Try from the terminal: "
            "`python -c \"import socket; "
            "print(socket.gethostbyname('<your-host>.supabase.co'))\"`\n\n"
            f"Technical detail: `{exc}`"
        )
        return None
    except Exception:
        raise


@st.fragment(run_every=REFRESH_EVERY)
def dashboard_panel() -> None:
    df_raw = _safe_load_responses()
    if df_raw is None:
        return
    df = _with_assign_dates(df_raw)
    ticket_ids = (
        sorted(df["ticket_id"].dropna().astype(str).unique().tolist()) if not df.empty else []
    )
    user_handles = (
        sorted(df["user_handle"].dropna().astype(str).unique().tolist())
        if not df.empty
        else []
    )

    st.subheader("Filters")

    selected_user = st.selectbox(
        "Filter by assignee (@user)",
        ["All"] + user_handles,
        format_func=_user_option_label,
        key="filter_assignee_handle",
        help="Narrows rows to replies from this Telegram username. Combines with ticket filters below.",
    )

    st.subheader("Search ticket number")
    with st.form("ticket_search_form", clear_on_submit=False):
        search_col, button_col = st.columns([4, 1])
        with search_col:
            search_ticket_id = st.text_input(
                "Search Ticket ID", placeholder="e.g. 2024080710000034"
            )
        with button_col:
            st.write("")
            search_clicked = st.form_submit_button("Search")

    selected_ticket = st.selectbox(
        "Filter by Ticket ID",
        ["All"] + ticket_ids,
        format_func=_ticket_option_label,
        key="filter_ticket_id",
    )

    if df.empty:
        st.info("No ticket responses found yet.")
        return

    st.caption(
        "Use **assignee** + **ticket** filters together. Columns: "
        "**Assign to** = @user. **Ticket status** = reply text. **Updated date and time** = reply time."
    )

    filtered_df = df
    if selected_user != "All":
        assignee_key = _normalize_handle(selected_user)
        filtered_df = filtered_df[
            filtered_df["user_handle"].map(_normalize_handle) == assignee_key
        ]

    if search_clicked and search_ticket_id.strip():
        ticket_query = search_ticket_id.strip()
        filtered_df = filtered_df[
            filtered_df["ticket_id"]
            .astype(str)
            .str.contains(ticket_query, case=False, na=False)
        ]
    else:
        if selected_ticket != "All":
            filtered_df = filtered_df[
                filtered_df["ticket_id"].astype(str) == selected_ticket
            ]

    if filtered_df.empty:
        st.warning("No rows match the current filters.")
        return

    display_df = filtered_df.copy()
    display_df["Assign date"] = _format_dt(display_df["assign_dt"])
    display_df["Ticket"] = display_df["ticket_id"].astype(str)
    display_df["Assign to"] = display_df["user_handle"].astype(str)
    display_df["Ticket status"] = display_df["response_data"].astype(str).fillna("")
    display_df["Updated date and time"] = _format_dt(display_df["created_dt"])

    visible_columns = [
        "Assign date",
        "Ticket",
        "Assign to",
        "Ticket status",
        "Updated date and time",
    ]
    display_df = display_df[visible_columns]

    st.dataframe(display_df, use_container_width=True)


dashboard_panel()

if st.button("Refresh"):
    st.cache_data.clear()
    st.rerun()

