"""
Field Ticket Ops — Streamlit dashboard for field ticket operations (Supabase).

Run: ``streamlit run app.py``

Requires the same env as the bot: ``SUPABASE_URL``, ``SUPABASE_KEY``,
optional ``TICKETS_TABLE`` (default ``tickets_active``).

Configuration sources, checked in this order:
  1. Process environment (set by the shell, Railway, Docker, etc.).
  2. ``.env`` file next to this script (auto-loaded by python-dotenv).
  3. ``st.secrets`` -- used by Streamlit Community Cloud, where the
     "Secrets" pane in app settings is the only place to put credentials.

For Streamlit Cloud, paste this TOML in *Manage app -> Settings -> Secrets*::

    SUPABASE_URL = "https://<project>.supabase.co"
    SUPABASE_KEY = "<service-role-or-anon-key>"
    DASHBOARD_PASSWORD = "<pick a strong password>"
    TELEGRAM_TOKEN = "<same bot token as the webhook service>"
    TELEGRAM_GROUP_CHAT_ID = "-1001234567890"
    # Optional Telethon (see bot_utils.py); if set, outbound posts use Telethon:
    # TG_API_ID = "12345678"
    # TG_API_HASH = "<from https://my.telegram.org>"
    # TG_BOT_TOKEN = "<optional; defaults to TELEGRAM_TOKEN>"
    # TG_GROUP_ID = "-1001234567890"
    # Or for a public supergroup with a @username:
    # TELEGRAM_GROUP_CHAT_ID = "@my_field_team"
    # TICKETS_TABLE = "tickets"

**Command Center** (sidebar assign) writes to Supabase then posts into the
field Telegram group via ``notify_telegram_group`` in ``bot_utils.py``:

- **Simple:** ``TELEGRAM_TOKEN`` + ``TELEGRAM_GROUP_CHAT_ID`` (HTTP Bot API).
- **Telethon:** also set ``TG_API_ID`` and ``TG_API_HASH`` (and optionally
  ``TG_BOT_TOKEN`` / ``TG_GROUP_ID`` as aliases). A session file
  ``telethon_bot_session.session`` is created next to ``bot_utils.py``.

The first line of the outbound message is ``@handle <category> <ticket>`` (plain
text, normal spaces) so the assignment regex in ``bot.py`` matches field replies.

You do **not** need to delete your webhook to find a chat id. That advice only
applies if you are using ``getUpdates`` in a browser while a webhook is active
(Telegram will not fill ``getUpdates`` in that mode). Use a numeric id from any
update your bot already receives, an id bot in the group, or ``@groupusername``
if the supergroup is public.

Authentication: a single shared password gates the dashboard. It is read from
``DASHBOARD_PASSWORD`` (env / ``st.secrets``). If unset, the app refuses to
render -- never serve this dashboard unauthenticated.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from bot_utils import (
    NOTIFY_BUILD_ID,
    normalize_telegram_group_id_paste,
    notify_telegram_group,
)
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


def _mapping_scalar_get(container: object, key: str) -> str | None:
    """Read ``key`` from a dict-like or attribute-style Streamlit secrets subsection."""
    if container is None:
        return None
    if isinstance(container, dict):
        if key in container:
            v = container[key]
        else:
            lk = key.lower()
            v = next((container[k] for k in container if str(k).lower() == lk), None)
    else:
        try:
            v = container[key]  # type: ignore[index]
        except Exception:
            v = getattr(container, key, None)
    if v in (None, ""):
        return None
    s = str(v).strip()
    return s or None


def _read_setting(key: str, default: str = "") -> str:
    """Return ``key`` from process env, falling back to ``st.secrets``.

    Also checks nested tables some teams use in Secrets TOML, e.g.::

        [telegram]
        TELEGRAM_GROUP_CHAT_ID = "-5149869288"

    ``st.secrets`` raises ``StreamlitSecretNotFoundError`` when no secrets
    file/section exists (typical on a fresh local run); we treat that the
    same as "not set".
    """
    value = os.getenv(key)
    if value not in (None, ""):
        return str(value).strip()

    try:
        v = st.secrets[key]  # type: ignore[index]
        if v not in (None, "") and str(v).strip() != "":
            return str(v).strip()
    except Exception:
        pass

    for sect in ("telegram", "TELEGRAM", "tg"):
        try:
            nested = st.secrets[sect]  # type: ignore[index]
        except Exception:
            continue
        v = _mapping_scalar_get(nested, key)
        if v:
            return v

    return str(default or "").strip()


def _read_nested_secret_sections(*keys: str) -> str:
    """Return first non-empty ``keys`` entry under ``[telegram]`` / ``[TELEGRAM]`` / ``[tg]``."""
    for sect in ("telegram", "TELEGRAM", "tg"):
        try:
            nested = st.secrets[sect]  # type: ignore[index]
        except Exception:
            continue
        for k in keys:
            v = _mapping_scalar_get(nested, k)
            if v:
                return v
    return ""


def _read_telegram_group_chat_raw() -> str:
    """Resolve field-group chat id from env and ``st.secrets`` (top-level + common sub-tables)."""
    primary = (
        _read_setting("TG_GROUP_ID").strip()
        or _read_setting("TELEGRAM_GROUP_ID").strip()
        or _read_setting("TELEGRAM_GROUP_CHAT_ID").strip()
        or _read_setting("TELEGRAM_CHAT_ID").strip()
        or _read_setting("GROUP_CHAT_ID").strip()
        or _read_setting("FIELD_GROUP_CHAT_ID").strip()
    )
    if primary:
        return primary
    nested_keys = (
        "TG_GROUP_ID",
        "TELEGRAM_GROUP_ID",
        "TELEGRAM_GROUP_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        "GROUP_CHAT_ID",
        "FIELD_GROUP_CHAT_ID",
        "group_chat_id",
        "group_id",
        "field_group_chat_id",
        "telegram_group_chat_id",
    )
    return _read_nested_secret_sections(*nested_keys).strip()


SUPABASE_URL = _read_setting("SUPABASE_URL").rstrip("/")
SUPABASE_KEY = _read_setting("SUPABASE_KEY")
TICKETS_TABLE = _read_setting("TICKETS_TABLE", "tickets_active") or "tickets_active"
ATTENDANCE_LOGS_TABLE = (
    _read_setting("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")
    or "ticket_attendance_logs"
)
FIELD_ENGINEERS_TABLE = (
    _read_setting("FIELD_ENGINEERS_TABLE", "dashboard_field_engineers")
    or "dashboard_field_engineers"
)

# Keep in sync with ``_ASSIGNMENT_TASK_CATEGORIES`` in ``bot.py``.
ASSIGNMENT_TASK_CATEGORIES: tuple[str, ...] = (
    "Coverage Check",
    "Femto Installation",
    "Repeater Installation",
    "Femto Recover",
    "Femto Fault",
    "Repeater Fault",
)

_TICKETS_MISSING_COLUMNS: set[str] = set()
_CC_FLASH_KEY = "_ticket_dashboard_cc_flash"
# Latest ``ticket_attendance_logs.timestamp`` the dashboard has already "seen"
# (for toast when Telegram/bot appends a new row).
_DASH_LAST_ATTENDANCE_TS_KEY = "_dash_last_seen_attendance_ts"
_CC_SESSION_TOKEN_KEY = "_ticket_dashboard_cc_bot_token_session"
_CC_SESSION_GROUP_KEY = "cc_cmd_center_telegram_group_id"

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

    st.title("Field Ticket Ops")
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


class _TableMissingError(RuntimeError):
    """Raised when a Supabase table the dashboard depends on is missing.

    Carries the table name so the UI can render a precise message
    pointing the operator at the right migration.
    """

    def __init__(self, table: str, original: Exception):
        super().__init__(f"table `{table}` is missing (PostgREST said: {original})")
        self.table = table
        self.original = original


def _looks_like_missing_table_error(exc: Exception) -> bool:
    """Detect PostgREST's `42P01 relation does not exist` errors.

    Works against both the new dict-style ``APIError`` and older string-only
    forms so we don't need to import postgrest's exception class.
    """
    text = str(exc)
    return (
        "42P01" in text
        or "does not exist" in text
        or "Could not find the table" in text
    )


def _fetch_ticket_row(ticket_number: str) -> dict | None:
    """Load one ticket by id (ignores lookback filter)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    tid = (ticket_number or "").strip()
    if not tid:
        return None
    client = _get_supabase_client()
    res = (
        client.table(TICKETS_TABLE)
        .select("*")
        .eq("ticket_number", tid)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def _fetch_pending_with_response_mismatch() -> list[str]:
    """Tickets still Pending but with a recent Response log (bot update likely failed)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    client = _get_supabase_client()
    try:
        pending = (
            client.table(TICKETS_TABLE)
            .select("ticket_number")
            .eq("status", "Pending")
            .limit(200)
            .execute()
        ).data or []
        if not pending:
            return []
        ids = [str(r["ticket_number"]) for r in pending if r.get("ticket_number")]
        logs = (
            client.table(ATTENDANCE_LOGS_TABLE)
            .select("ticket_number")
            .eq("action_type", "Response")
            .in_("ticket_number", ids)
            .execute()
        ).data or []
        logged = {str(r["ticket_number"]) for r in logs if r.get("ticket_number")}
        return sorted(logged)
    except Exception:
        return []


def _fetch_tickets() -> pd.DataFrame:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()
    client = _get_supabase_client()
    order_col = _get_order_column()
    try:
        res = client.table(TICKETS_TABLE).select("*").order(order_col, desc=True).execute()
    except Exception as exc:
        if _looks_like_missing_table_error(exc):
            raise _TableMissingError(TICKETS_TABLE, exc) from exc
        raise
    rows = res.data or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _fetch_ticket_photos(
    ticket_numbers: list[str] | None = None,
    *,
    limit_per_ticket: int = 200,
) -> dict[str, list[dict]]:
    """Return every Response photo per ticket, newest-first.

    Reads ``ticket_attendance_logs`` so the gallery shows the full history,
    not just the single ``photo_url`` currently pinned on
    ``tickets_active``. If ``ticket_numbers`` is None we fetch all photos
    in the table (capped); otherwise we restrict to those IDs.

    Returns ``{ticket_number: [ {url, note, member, when}, ... ]}``.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {}
    client = _get_supabase_client()
    q = (
        client.table(ATTENDANCE_LOGS_TABLE)
        .select("ticket_number, member_username, note, photo_url, timestamp")
        .eq("action_type", "Response")
        .not_.is_("photo_url", "null")
    )
    if ticket_numbers:
        ids = [t for t in dict.fromkeys(ticket_numbers) if t]
        if not ids:
            return {}
        q = q.in_("ticket_number", ids)
    try:
        res = q.order("timestamp", desc=True).limit(limit_per_ticket * max(len(ticket_numbers or [1]), 1)).execute()
    except Exception:
        return {}
    grouped: dict[str, list[dict]] = {}
    for row in res.data or []:
        url = str(row.get("photo_url") or "").strip()
        if not url.startswith("http"):
            continue
        tid = row.get("ticket_number") or "—"
        grouped.setdefault(tid, []).append(
            {
                "url": url,
                "note": row.get("note"),
                "member": row.get("member_username"),
                "when": row.get("timestamp"),
            }
        )
    return grouped


def _set_ticket_status(
    ticket_number: str,
    *,
    new_status: str,
    log_action: str | None = None,
    actor: str = "@dashboard-admin",
    note: str | None = None,
) -> None:
    """Flip a ticket's ``status`` and (optionally) append a history log row.

    Used by the admin controls in the dashboard. We touch only ``status`` /
    ``updated_at`` so we don't accidentally clobber the field team's
    ``field_response`` or ``photo_url``. The reverse-direction (Reopen)
    intentionally does **not** clear ``responded_at`` so we keep the
    original first-response timestamp for downstream metrics.
    """
    client = _get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {"status": new_status, "updated_at": now_iso}
    client.table(TICKETS_TABLE).update(payload).eq(
        "ticket_number", str(ticket_number)
    ).execute()

    if log_action:
        try:
            client.table(ATTENDANCE_LOGS_TABLE).insert(
                {
                    "ticket_number": str(ticket_number),
                    "member_username": actor,
                    "action_type": log_action,
                    "note": note,
                    "timestamp": now_iso,
                }
            ).execute()
        except Exception:
            # Don't fail the status change if the history table is missing
            # or temporarily unavailable; the status update itself succeeded.
            pass


def _delete_ticket(ticket_number: str, *, actor: str = "@dashboard-admin") -> None:
    """Delete the ticket row but keep its attendance history.

    The ``ticket_attendance_logs.ticket_number`` foreign key is **not**
    cascaded, so the history rows remain queryable from the Log tab even
    after the active ticket is gone. We also append a ``Deleted`` log
    entry so the audit trail explicitly records the removal.
    """
    client = _get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        client.table(ATTENDANCE_LOGS_TABLE).insert(
            {
                "ticket_number": str(ticket_number),
                "member_username": actor,
                "action_type": "Deleted",
                "note": "Ticket row deleted from dashboard. History retained.",
                "timestamp": now_iso,
            }
        ).execute()
    except Exception:
        # Don't block the delete if the history table is missing.
        pass
    client.table(TICKETS_TABLE).delete().eq(
        "ticket_number", str(ticket_number)
    ).execute()


def _ticket_options_for_admin(df: pd.DataFrame) -> list[str]:
    """Sorted ticket numbers for admin pickers (newest activity first)."""
    if "ticket_number" not in df.columns or df.empty:
        return []
    sort_col = next(
        (c for c in ("responded_at", "last_assigned_at", "updated_at", "created_at") if c in df.columns),
        None,
    )
    ordered = df.sort_values(sort_col, ascending=False) if sort_col else df
    return [str(t) for t in ordered["ticket_number"].astype(str).tolist() if t]


def _delete_ticket_error_ui(picked: str, exc: Exception) -> None:
    err = str(exc).lower()
    if "42501" in str(exc) or "permission denied" in err or "row-level security" in err:
        st.error(
            f"Could not delete ticket **{picked}**: the database denied DELETE "
            "(Row Level Security). Apply the migration "
            "`supabase/migrations/20260514_tickets_active_anon_delete.sql` "
            "in the Supabase SQL editor, then try again."
        )
    else:
        st.error(f"Could not delete ticket {picked}: {exc}")


def _render_admin_ticket_toolbar(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    title: str = "Admin — ticket",
    caption: str | None = None,
    status_actions: tuple[tuple[str, str, str], ...] = (),
    allow_delete: bool = True,
) -> None:
    """One compact row: pick ticket → pick action → Apply.

    ``status_actions`` entries are ``(radio_label, new_status, log_action)``.
    Delete is optional; it requires the small confirm checkbox below the radio.
    Everything lives in a **collapsed** expander so the main table stays clean.
    """
    options = _ticket_options_for_admin(df)
    if not options:
        return

    radio_labels: list[str] = [a[0] for a in status_actions]
    if allow_delete:
        radio_labels.append("Delete row")

    with st.expander(title, expanded=False):
        if caption:
            st.caption(caption)
        picked = st.selectbox("Ticket", options=options, key=f"{key_prefix}_sb_ticket")

        if not radio_labels:
            return

        if radio_labels == ["Delete row"]:
            choice = "Delete row"
            st.caption("Removes the active row; history stays in the Log tab.")
        else:
            choice = st.radio(
                "Action",
                options=radio_labels,
                horizontal=True,
                key=f"{key_prefix}_radio",
            )

        confirm_del = False
        if choice == "Delete row":
            confirm_del = st.checkbox(
                "Confirm delete",
                value=False,
                key=f"{key_prefix}_del_confirm",
            )

        if st.button("Apply", key=f"{key_prefix}_apply", type="primary"):
            if choice == "Delete row":
                if not confirm_del:
                    st.warning("Check **Confirm delete** first.")
                    return
                try:
                    _delete_ticket(picked)
                except Exception as exc:
                    _delete_ticket_error_ui(picked, exc)
                    return
                st.success(f"{picked} deleted (history kept in Log).")
                st.rerun()

            matched = next((a for a in status_actions if a[0] == choice), None)
            if not matched:
                st.error("Unknown action.")
                return
            _, new_status, log_action = matched
            try:
                _set_ticket_status(
                    picked,
                    new_status=new_status,
                    log_action=log_action,
                )
            except Exception as exc:
                st.error(f"Could not update {picked}: {exc}")
                return
            st.success(f"{picked} → **{new_status}**.")
            st.rerun()


def _fetch_attendance(
    *,
    ticket_number: str | None = None,
    member_query: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
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
    except Exception as exc:
        if _looks_like_missing_table_error(exc):
            raise _TableMissingError(ATTENDANCE_LOGS_TABLE, exc) from exc
        return pd.DataFrame()
    rows = res.data or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _fetch_latest_attendance_timestamp() -> datetime | None:
    """Return newest log row timestamp, or None if table empty / unreadable."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    client = _get_supabase_client()
    try:
        res = (
            client.table(ATTENDANCE_LOGS_TABLE)
            .select("timestamp")
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
    except Exception:
        return None
    rows = res.data or []
    if not rows:
        return None
    raw = rows[0].get("timestamp")
    if raw is None:
        return None
    ts = pd.to_datetime(raw, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def _maybe_toast_new_telegram_activity() -> None:
    """Toast when ``ticket_attendance_logs`` grows (assignments + field replies)."""
    try:
        latest = _fetch_latest_attendance_timestamp()
    except Exception:
        return
    if latest is None:
        return
    prev_raw = st.session_state.get(_DASH_LAST_ATTENDANCE_TS_KEY)
    latest_iso = latest.isoformat()
    if prev_raw is None:
        st.session_state[_DASH_LAST_ATTENDANCE_TS_KEY] = latest_iso
        return
    try:
        prev = pd.to_datetime(prev_raw, utc=True, errors="coerce")
    except Exception:
        st.session_state[_DASH_LAST_ATTENDANCE_TS_KEY] = latest_iso
        return
    if pd.isna(prev):
        st.session_state[_DASH_LAST_ATTENDANCE_TS_KEY] = latest_iso
        return
    prev_dt = prev.to_pydatetime()
    if latest > prev_dt:
        st.toast(
            "New activity — check **Pending**, **Open**, or **Log**.",
            icon="📥",
        )
        st.session_state[_DASH_LAST_ATTENDANCE_TS_KEY] = latest_iso


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


_ASSIGNMENT_ID_PATTERN = re.compile(r"^(?:\d{9}|\d{16})$")


def _cc_utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cc_parse_missing_column(message: str) -> str | None:
    m = re.search(r"column [\w\.]*?\.?(\w+) does not exist", message)
    if m:
        return m.group(1)
    m = re.search(r"Could not find the '(\w+)' column", message)
    if m:
        return m.group(1)
    return None


def _cc_strip_missing_ticket_columns(payload: dict) -> dict:
    if not _TICKETS_MISSING_COLUMNS:
        return payload
    return {k: v for k, v in payload.items() if k not in _TICKETS_MISSING_COLUMNS}


def _cc_normalize_handle(raw: str) -> str:
    """Return ``@username`` for Supabase / Telegram."""
    cleaned = raw.strip().lstrip("@")
    if not cleaned:
        raise ValueError("Username is empty.")
    if len(cleaned) > 32:
        raise ValueError("Username is too long (max 32 characters).")
    if not re.match(r"^[A-Za-z0-9_]+$", cleaned):
        raise ValueError(
            "Username must contain only letters, digits, and underscores (no spaces)."
        )
    return "@" + cleaned


def _cc_validate_ticket_number(raw: str) -> str:
    cleaned = raw.strip()
    if not _ASSIGNMENT_ID_PATTERN.fullmatch(cleaned):
        raise ValueError(
            "Ticket number must be exactly **9** or **16** digits "
            "(same rule as Telegram assignment messages)."
        )
    return cleaned


def _cc_fetch_ticket_minimal(client, ticket_number: str) -> dict | None:
    res = (
        client.table(TICKETS_TABLE)
        .select("ticket_number, assigned_to, task_category, status")
        .eq("ticket_number", ticket_number)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def _cc_insert_attendance_log(
    client,
    *,
    ticket_number: str,
    member_username: str,
    action_type: str,
    note: str | None = None,
) -> None:
    row = {
        "ticket_number": ticket_number,
        "member_username": member_username,
        "action_type": action_type,
        "note": note,
        "photo_url": None,
        "timestamp": _cc_utc_now_iso(),
    }
    try:
        client.table(ATTENDANCE_LOGS_TABLE).insert(row).execute()
    except Exception:
        pass


def _cc_execute_ticket_update(client, payload: dict, ticket_number: str) -> None:
    attempt = _cc_strip_missing_ticket_columns(dict(payload))
    last_err: Exception | None = None
    for _ in range(4):
        try:
            client.table(TICKETS_TABLE).update(attempt).eq(
                "ticket_number", ticket_number
            ).execute()
            return
        except Exception as exc:
            text = str(exc)
            col = _cc_parse_missing_column(text)
            if not col or col not in attempt:
                last_err = exc
                break
            _TICKETS_MISSING_COLUMNS.add(col)
            attempt = {k: v for k, v in attempt.items() if k != col}
            last_err = exc
    if last_err is not None:
        raise last_err


def _cc_insert_assignment(
    client,
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
) -> None:
    now_iso = _cc_utc_now_iso()
    row: dict = {
        "ticket_number": ticket_number,
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
    }
    for _ in range(4):
        try:
            client.table(TICKETS_TABLE).insert(row).execute()
            break
        except Exception as exc:
            col = _cc_parse_missing_column(str(exc))
            if not col or col not in row:
                raise
            _TICKETS_MISSING_COLUMNS.add(col)
            row.pop(col, None)
    else:
        raise RuntimeError(
            f"insert into {TICKETS_TABLE} failed: too many missing-column retries"
        )

    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
    )


def _cc_reassign_ticket(
    client,
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
) -> None:
    now_iso = _cc_utc_now_iso()
    updates = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
        "updated_at": now_iso,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
    }
    _cc_execute_ticket_update(client, updates, ticket_number)

    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=additional_info,
    )


def _cc_upsert_assignment(
    assigned_to: str,
    ticket_number: str,
    task_category: str,
    *,
    additional_info: str | None = None,
) -> str:
    """Insert or reassign; ``assigned_to`` is ``@username``. Returns a short summary."""
    client = _get_supabase_client()
    existing = _cc_fetch_ticket_minimal(client, ticket_number)
    if existing is None:
        _cc_insert_assignment(
            client,
            ticket_number,
            assigned_to,
            task_category,
            additional_info=additional_info,
        )
        return f"Created ticket **{ticket_number}** and logged assignment."
    _cc_reassign_ticket(
        client,
        ticket_number,
        assigned_to,
        task_category,
        additional_info=additional_info,
    )
    prev_assignee = existing.get("assigned_to") or "—"
    return (
        f"Re-assigned **{ticket_number}** from {prev_assignee} to {assigned_to}; "
        "status reset to Pending."
    )


def _parse_telegram_group_chat_id(raw: str) -> tuple[int | str | None, str | None]:
    """Return a ``chat_id`` for ``send_message`` (int or ``@public_group``).

    Telegram accepts a negative numeric id or ``@channelusername`` for public
    chats. No webhook deletion is required to configure this.
    """
    s = normalize_telegram_group_id_paste(raw)
    if not s:
        return None, None
    if s.startswith("@"):
        return s, None
    try:
        return int(s), None
    except ValueError:
        return None, (
            "Use a numeric id (e.g. -100…) or a public supergroup @username "
            "(must start with @)."
        )


def _normalize_engineer_dir_handle(raw: str) -> str:
    """Normalize a directory entry: strip ``@``, validate Telegram username rules."""
    cleaned = raw.strip().lstrip("@")
    if not cleaned:
        raise ValueError("Handle is empty.")
    if len(cleaned) > 32:
        raise ValueError("Handle is too long (max 32 characters).")
    if not re.match(r"^[A-Za-z0-9_]+$", cleaned):
        raise ValueError("Use only letters, digits, and underscores.")
    return cleaned


def _try_fetch_field_engineer_usernames() -> tuple[list[str], bool]:
    """Return ``(usernames_without_at, table_missing)`` sorted case-insensitively."""
    client = _get_supabase_client()
    try:
        res = (
            client.table(FIELD_ENGINEERS_TABLE)
            .select("username")
            .order("username")
            .execute()
        )
    except Exception as exc:
        if _looks_like_missing_table_error(exc):
            return [], True
        raise
    rows = res.data or []
    names = [str(r["username"]) for r in rows if r.get("username")]
    return sorted(set(names), key=str.lower), False


def _insert_field_engineer(username: str) -> None:
    client = _get_supabase_client()
    client.table(FIELD_ENGINEERS_TABLE).insert({"username": username}).execute()


def _delete_field_engineer(username: str) -> None:
    client = _get_supabase_client()
    client.table(FIELD_ENGINEERS_TABLE).delete().eq("username", username).execute()


def _field_team_directory_ui() -> tuple[list[str], bool]:
    """Field team directory in a collapsible expander; return ``(names, table_missing)``."""
    names, missing = _try_fetch_field_engineer_usernames()
    with st.expander("Field team", expanded=False):
        if missing:
            st.info(
                f"Add table `{FIELD_ENGINEERS_TABLE}` in Supabase (see migration "
                f"`supabase/migrations/20260515_dashboard_field_engineers.sql`), "
                "then refresh. Until then, use the **Field engineer** box in the assign form."
            )
            return names, True
        if not names:
            st.caption("No handles yet — add below.")
        for u in names:
            c1, c2 = st.columns([14, 1])
            with c1:
                st.markdown(f"**@{u}**")
            with c2:
                hkey = hashlib.sha256(u.encode("utf-8")).hexdigest()[:16]
                if st.button(
                    "🗑",
                    key=f"fe_rm_{hkey}",
                    help=f"Remove @{u}",
                    type="secondary",
                ):
                    try:
                        _delete_field_engineer(u)
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
        add_left, add_right = st.columns([6, 1])
        with add_left:
            st.text_input(
                "New handle",
                key="fe_new_handle",
                placeholder="engineer_name",
                label_visibility="collapsed",
            )
        with add_right:
            if st.button("+", key="fe_add_btn", help="Add to team", type="secondary"):
                raw = str(st.session_state.get("fe_new_handle") or "").strip()
                if not raw:
                    st.warning("Enter a handle first.")
                else:
                    try:
                        norm = _normalize_engineer_dir_handle(raw)
                        existing, _ = _try_fetch_field_engineer_usernames()
                        if any(e.lower() == norm.lower() for e in existing):
                            st.warning(f"**@{norm}** is already in the list (case-insensitive).")
                        else:
                            _insert_field_engineer(norm)
                            st.session_state.pop("fe_new_handle", None)
                            st.rerun()
                    except ValueError as ve:
                        st.error(str(ve))
                    except Exception as exc:
                        err = str(exc).lower()
                        if "duplicate" in err or "23505" in str(exc) or "unique" in err:
                            st.warning("That handle is already in the directory.")
                        else:
                            st.error(str(exc))
    return names, False


def _sidebar_command_center() -> None:
    flash = st.session_state.pop(_CC_FLASH_KEY, None)
    if flash:
        st.success(flash)

    st.header("Command Center")
    token_env = (
        _read_setting("TG_BOT_TOKEN").strip()
        or _read_setting("TELEGRAM_BOT_TOKEN").strip()
        or _read_setting("TELEGRAM_TOKEN").strip()
    )
    env_chat_raw = _read_telegram_group_chat_raw()
    env_group_parsed: int | str | None = None
    env_group_warn: str | None = None
    if env_chat_raw:
        env_group_parsed, env_group_warn = _parse_telegram_group_chat_id(env_chat_raw)
        if env_group_warn:
            st.warning(
                "Group id from env / Streamlit Secrets is invalid. "
                + env_group_warn
                + " Fix **TELEGRAM_GROUP_CHAT_ID** (or **TG_GROUP_ID**) in Secrets / `.env` and restart."
            )
    env_group_ok = env_group_parsed is not None

    fe_names, fe_missing = _field_team_directory_ui()

    pick_choice: str | None = None
    fe_handle_raw = ""
    token_session = ""
    chat_session = ""
    with st.form("cc_assign_form"):
        # Session token only when env/Secrets lack it.
        if not token_env:
            token_session = st.text_input(
                "Bot token (this session only)",
                type="password",
                key=_CC_SESSION_TOKEN_KEY,
                placeholder="Paste TELEGRAM_TOKEN if missing from .env / Secrets",
                help="Not saved to disk. Prefer **TELEGRAM_TOKEN** in `.env` or Streamlit Secrets.",
            )
        if not env_group_ok:
            chat_session = st.text_input(
                "Group chat id (only if missing from Secrets)",
                key=_CC_SESSION_GROUP_KEY,
                placeholder="-5149869288 or -100… or @YourPublicGroup",
                help="Hidden when **TELEGRAM_GROUP_CHAT_ID** (or nested `[telegram]` keys) is valid. "
                "Paste the group id here if the app still cannot read Secrets, then Assign.",
            )
        if fe_names and not fe_missing:
            pick_choice = st.selectbox(
                "Field engineer",
                options=[f"@{n}" for n in fe_names],
                index=0,
                help="Pick a handle from the directory (expand **Field team** above to edit the list).",
            )
        else:
            fe_handle_raw = st.text_input(
                "Field engineer",
                placeholder="ibeyx",
                help="Telegram username (with or without @). "
                "Add `dashboard_field_engineers` in Supabase to pick from a saved list instead.",
            )
        tid_raw = st.text_input(
            "Ticket number",
            placeholder="9 or 16 digits",
            help="Unique per task (9 or 16 digits). Shown in Telegram line 1 and used to match field replies.",
        )
        cat = st.selectbox("Task category", options=list(ASSIGNMENT_TASK_CATEGORIES))
        additional_info_raw = st.text_area(
            "Additional info",
            placeholder="Optional — site context, access details, etc.",
            height=88,
            help="Saved to the ticket's **additional_info** column and the same text appears in the Telegram assignment.",
        )
        submitted = st.form_submit_button("Assign", type="primary", use_container_width=True)

    if not submitted:
        return

    try:
        if fe_names and not fe_missing:
            if pick_choice is None or not str(pick_choice).strip():
                st.error("Pick a field engineer from the list.")
                return
            handle = _cc_normalize_handle(pick_choice)
        else:
            if not fe_handle_raw.strip():
                st.error("Enter a field engineer Telegram username.")
                return
            handle = _cc_normalize_handle(fe_handle_raw)
        tid = _cc_validate_ticket_number(tid_raw)
    except ValueError as exc:
        st.error(str(exc))
        return

    additional_info_val = (additional_info_raw or "").strip() or None

    # Form widgets with ``key=`` sometimes leave return values empty on submit;
    # merge ``st.session_state`` (updated when the form posts).
    token = token_env or (
        str(token_session).strip()
        or str(st.session_state.get(_CC_SESSION_TOKEN_KEY, "")).strip()
    )
    # Prefer env/Secrets; allow one form override when missing or invalid there.
    chat_raw = (env_chat_raw if env_group_ok else "") or (
        str(chat_session).strip()
        or str(st.session_state.get(_CC_SESSION_GROUP_KEY, "")).strip()
    )
    chat_id: int | str | None = None
    chat_parse_err: str | None = None
    if chat_raw:
        chat_id, chat_parse_err = _parse_telegram_group_chat_id(chat_raw)
    if chat_parse_err:
        st.warning(chat_parse_err)

    if not token or chat_id is None:
        missing_bits: list[str] = []
        if not token:
            missing_bits.append(
                "no bot token — set **TELEGRAM_TOKEN** (or **TELEGRAM_BOT_TOKEN** / **TG_BOT_TOKEN**)"
            )
        if not chat_raw:
            missing_bits.append(
                "no group id — set **TELEGRAM_GROUP_CHAT_ID** (or **TG_GROUP_ID** / **FIELD_GROUP_CHAT_ID**), "
                "including under `[telegram]` in Secrets if you use a subsection, "
                "or paste into **Group chat id (only if missing from Secrets)** in this form"
            )
        elif chat_id is None:
            missing_bits.append(
                f"group id **{chat_raw[:72]}** is not a valid integer or **@** public username"
            )
        st.error(
            "Cannot post to Telegram yet. " + " · ".join(missing_bits) + ". "
            "If the bot token is missing, use the **session-only** token field in this form or set "
            "**TELEGRAM_TOKEN** in Secrets. "
            "For the group, use top-level Secrets keys or `[telegram]` / `group_chat_id` style keys; "
            "restart after editing Secrets. If the id still is not picked up, paste it in the "
            "**Group chat id (only if missing from Secrets)** field and Assign again."
        )
        return

    try:
        summary = _cc_upsert_assignment(handle, tid, cat, additional_info=additional_info_val)
    except Exception as exc:
        st.error(f"Supabase upsert failed: {exc}")
        return

    try:
        asyncio.run(
            notify_telegram_group(
                handle,
                tid,
                cat,
                additional_info=additional_info_val,
                api_id=_read_setting("TG_API_ID") or _read_setting("TELEGRAM_API_ID") or None,
                api_hash=_read_setting("TG_API_HASH") or _read_setting("TELEGRAM_API_HASH") or None,
                bot_token=token or None,
                group_id=chat_id,
            )
        )
    except Exception as exc:
        st.warning(f"{summary} Telegram post failed (saved in Supabase): {exc}")
        return

    st.session_state[_CC_FLASH_KEY] = (
        f"{summary} Posted to Telegram ({NOTIFY_BUILD_ID}, one message)."
    )
    st.rerun()


DEFAULT_REFRESH_MINUTES = 1
MIN_REFRESH_MINUTES = 1
MAX_REFRESH_MINUTES = 60

DEFAULT_LOOKBACK_DAYS = 7
MIN_LOOKBACK_DAYS = 1
MAX_LOOKBACK_DAYS = 30


def _sidebar_controls() -> tuple[bool, int, int]:
    """Return (auto_enabled, interval_minutes, lookback_days)."""
    with st.sidebar:
        _sidebar_command_center()
        st.divider()
        st.header("Filters")
        lookback_days = st.slider(
            "Days to Look Back",
            min_value=MIN_LOOKBACK_DAYS,
            max_value=MAX_LOOKBACK_DAYS,
            value=DEFAULT_LOOKBACK_DAYS,
            step=1,
            help=(
                "Only show tickets with **any activity** (assignment, field "
                "response, admin update, or creation) within this window. "
                "Counts and tabs honour this filter."
            ),
        )

        st.header("Refresh")
        auto = st.toggle(
            "Auto-refresh",
            value=True,
            help=(
                f"Re-fetch from Supabase on a timer ({LOCAL_TZ_LABEL}). "
                "When new rows appear in the attendance log (Telegram or Command Center), "
                "you get a short toast so the field group can stay quiet."
            ),
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
            _get_supabase_client.clear()
            st.session_state.pop(_DASH_LAST_ATTENDANCE_TS_KEY, None)
            st.rerun()
        st.caption(
            f"After a field reply in Telegram, tickets move **Pending → Open**. "
            f"Use the **Open** queue below (not Pending). Table: `{TICKETS_TABLE}`."
        )
        lookup = st.text_input(
            "Look up ticket #",
            placeholder="9 or 16 digits",
            key="dash_ticket_lookup",
        )
        if lookup.strip():
            row = _fetch_ticket_row(lookup.strip())
            if row:
                st.success(
                    f"**{row.get('ticket_number')}** — status **{row.get('status')}**, "
                    f"assigned **{row.get('assigned_to')}**, "
                    f"responded_at={row.get('responded_at') or '—'}"
                )
            else:
                st.warning(f"No row in `{TICKETS_TABLE}` for that ticket number.")
        st.divider()
        if st.button("Log out", use_container_width=True):
            st.session_state.pop(_AUTH_OK_KEY, None)
            st.session_state.pop(_AUTH_PWD_VER_KEY, None)
            st.rerun()
    return auto, int(interval_minutes), int(lookback_days)


def main() -> None:
    # Must be the first Streamlit command every run (login + dashboard).
    st.set_page_config(page_title="Field Ticket Ops", layout="wide")

    _check_password()

    st.title("Field Ticket Ops")

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

    _inject_bon_theme()

    auto, interval_minutes, lookback_days = _sidebar_controls()
    run_every = timedelta(minutes=interval_minutes) if auto else None

    @st.fragment(run_every=run_every)
    def _dashboard_fragment() -> None:
        _render_dashboard(lookback_days=lookback_days)

    _dashboard_fragment()


PHOTO_THUMB_WIDTH = 220  # px — tight enough that 3 fit per row on a laptop

# BONFamily-inspired UI: charcoal base (#121212), Light Oak (#D7B491) accents.
_BON_THEME_CSS = """
<style>
    :root {
        --bon-bg: #121212;
        --bon-panel: #1a1a1a;
        --bon-card: #1e1e1e;
        --bon-oak: #D7B491;
        --bon-text: #e8e6e3;
        --bon-muted: #a39e97;
        --bon-font: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }
    /* One UI font everywhere Streamlit allows (tables, metrics, forms, markdown). */
    .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stHeader"],
    [data-testid="stSidebar"],
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] *,
    [data-testid="stMetric"],
    [data-testid="stMetric"] *,
    [data-testid="stDataFrame"],
    [data-testid="stDataFrame"] *,
    .stTabs [data-baseweb="tab-list"],
    .stTabs [data-baseweb="tab"],
    [data-baseweb="select"],
    [data-baseweb="input"],
    [data-baseweb="textarea"],
    [data-baseweb="typo-body"],
    [data-baseweb="typo-label"],
    .stTextInput input,
    .stTextArea textarea,
    .stSelectbox [data-baseweb="select"] > div,
    .stMultiSelect [data-baseweb="select"] > div,
    .stNumberInput input,
    .stSlider [data-baseweb="slider"] label,
    .stCheckbox label,
    .stRadio label,
    .stToggle label,
    .stDateInput input,
    .stTimeInput input,
    .stButton > button {
        font-family: var(--bon-font) !important;
    }
    .stApp {
        background-color: var(--bon-bg);
        color: var(--bon-text);
    }
    [data-testid="stHeader"] {
        background-color: rgba(18, 18, 18, 0.96);
        border-bottom: 1px solid var(--bon-oak);
    }
    [data-testid="stSidebar"] {
        background-color: var(--bon-panel);
        border-right: 1px solid var(--bon-oak);
    }
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] span {
        color: var(--bon-text);
    }
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border-color: var(--bon-oak) !important;
        border-radius: 14px !important;
        background-color: var(--bon-card) !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        background-color: var(--bon-panel);
        border-radius: 10px;
        padding: 4px;
        gap: 4px;
        border: 1px solid rgba(215, 180, 145, 0.35);
    }
    .stTabs [data-baseweb="tab"] {
        color: var(--bon-muted);
        border-radius: 8px;
    }
    .stTabs [aria-selected="true"] {
        background-color: var(--bon-oak) !important;
        color: #121212 !important;
    }
    /* Queue switcher (segmented control): more space between choices than tabs */
    div[data-baseweb="segmented-control"] {
        gap: 12px !important;
        flex-wrap: wrap !important;
    }
    div[data-baseweb="segmented-control"] button {
        padding: 10px 20px !important;
        min-height: 2.75rem !important;
    }
    .stButton > button {
        border-radius: 10px !important;
        border: 1px solid var(--bon-oak) !important;
        background-color: #2a2420 !important;
        color: var(--bon-oak) !important;
        font-weight: 600;
    }
    .stButton > button:hover {
        background-color: var(--bon-oak) !important;
        color: #121212 !important;
    }
    div[data-testid="stExpander"] details {
        border: 1px solid var(--bon-oak);
        border-radius: 14px;
        background-color: var(--bon-card);
    }
    div[data-testid="stExpander"] summary {
        color: var(--bon-oak);
    }
    [data-testid="stMetric"] {
        background: var(--bon-card);
        padding: 10px 14px;
        border-radius: 12px;
        border: 1px solid rgba(215, 180, 145, 0.35);
    }
    [data-testid="stMetric"] label { color: var(--bon-muted) !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--bon-oak) !important;
    }
    .stMarkdown a { color: var(--bon-oak); }
    [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
</style>
"""


def _inject_bon_theme() -> None:
    st.markdown(_BON_THEME_CSS, unsafe_allow_html=True)


def _format_when(when: object) -> str:
    if when is None:
        return ""
    try:
        return pd.Timestamp(when).tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        try:
            return pd.Timestamp(when).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(when)


def _render_photo_grid(photos: list[dict], cols_per_row: int = 3) -> None:
    """Render a list of photo dicts in a fixed-width thumbnail grid.

    Each thumbnail shows ``member · timestamp`` underneath, an inline note
    (truncated), and an "Open full size" link. Keeps photos compact so
    multiple shots from the same ticket fit on one screen.
    """
    if not photos:
        return
    for chunk_start in range(0, len(photos), cols_per_row):
        chunk = photos[chunk_start : chunk_start + cols_per_row]
        cols = st.columns(cols_per_row)
        for slot, photo in enumerate(chunk):
            with cols[slot]:
                with st.container(border=True):
                    try:
                        st.image(photo["url"], width=PHOTO_THUMB_WIDTH)
                    except Exception as exc:
                        st.warning(f"Could not load image: {exc}")
                    meta_bits = []
                    if photo.get("member"):
                        meta_bits.append(str(photo["member"]))
                    when = _format_when(photo.get("when"))
                    if when:
                        meta_bits.append(f"{when} {LOCAL_TZ_LABEL}")
                    if meta_bits:
                        st.caption(" · ".join(meta_bits))
                    note = photo.get("note")
                    if isinstance(note, str) and note.strip():
                        trimmed = note.strip()
                        if len(trimmed) > 140:
                            trimmed = trimmed[:140] + "…"
                        st.markdown(trimmed)
                    st.markdown(f"[Open full size]({photo['url']})")


def _dataframe_column_config(df: pd.DataFrame) -> dict:
    """Streamlit ``column_config`` that renders ``photo_url`` as a clickable link.

    Falls back to no config if Streamlit is too old to support
    ``column_config.LinkColumn`` (rare, but keeps the table viewable).
    """
    if "photo_url" not in df.columns:
        return {}
    try:
        return {
            "photo_url": st.column_config.LinkColumn(
                "photo_url",
                help="Click to open the field photo in a new tab.",
                display_text="Open photo",
            ),
        }
    except Exception:
        return {}


def _apply_lookback(df: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Return rows with **any recent activity** within the lookback window.

    We used to filter on ``last_assigned_at`` only whenever that column
    existed. That hid tickets whose assignment was old but which had a
    fresh field response (``Open``), admin status change, or DB
    ``updated_at`` — making the dashboard look "stuck" even though
    Supabase had new data.

    Now each row is kept if the **latest** of the available activity
    timestamps (assignment, response, generic update, or creation) falls
    inside the window.
    """
    if df.empty:
        return df
    cols = [
        c
        for c in (
            "last_assigned_at",
            "responded_at",
            "updated_at",
            "created_at",
        )
        if c in df.columns
    ]
    if not cols:
        return df
    stacked = pd.concat([_parse_ts(df[c]) for c in cols], axis=1)
    ref = stacked.max(axis=1, skipna=True)
    cutoff = pd.Timestamp.now(tz=LOCAL_TZ).tz_convert("UTC") - pd.Timedelta(days=lookback_days)
    # Keep rows with no parseable timestamps so a bad/legacy cell does not wipe
    # the whole dashboard (everything became NaT → empty frame).
    mask = ref.isna() | (ref >= cutoff)
    return df[mask].copy()


def _queue_segment_label(name: str, count: int) -> str:
    return f"{name} ({count})" if count else name


def _queue_segment_base(label: str | None) -> str:
    """Map segmented-control label (with optional count) back to queue name."""
    if not label:
        return "Pending"
    for base in ("Pending", "Open", "Completed", "Log"):
        if label == base or label.startswith(f"{base} ("):
            return base
    return "Pending"


def _render_dashboard(
    *,
    lookback_days: int,
) -> None:
    day_word = "day" if lookback_days == 1 else "days"
    refreshed_at = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    st.caption(f"Data from Supabase table `{TICKETS_TABLE}` · last refresh **{refreshed_at} {LOCAL_TZ_LABEL}**")

    try:
        df_all = _fetch_tickets()
        _maybe_toast_new_telegram_activity()
    except _TableMissingError as missing:
        _render_missing_table_help(missing.table)
        return
    except Exception as exc:
        st.error(f"Could not load tickets from Supabase: {exc}")
        st.caption(
            "Check `SUPABASE_URL` / `SUPABASE_KEY` / `TICKETS_TABLE` in env or Streamlit "
            "Secrets. If the key is the **anon** publishable key, ensure RLS policies on "
            f"`{TICKETS_TABLE}` allow **select** (and **update** for Command Center). "
            "Apply pending SQL migrations if this project recently renamed `tickets` → "
            "`tickets_active`."
        )
        return

    if df_all.empty:
        st.warning("No rows returned (empty table or connection issue).")
        _render_attendance_tab()  # search still useful even with no active rows
        return

    if "status" not in df_all.columns:
        st.error(f"The `{TICKETS_TABLE}` table has no `status` column.")
        return

    df = _apply_lookback(df_all, lookback_days)
    if len(df_all) > len(df):
        st.caption(
            f"Showing **{len(df)}** of **{len(df_all)}** tickets in the last "
            f"{lookback_days} day(s). Increase **Days to Look Back** in the sidebar "
            "if a Telegram assignment is missing."
        )

    mismatches = _fetch_pending_with_response_mismatch()
    if mismatches:
        st.error(
            f"**{len(mismatches)}** ticket(s) have a Response in the log but are still **Pending** "
            f"in `{TICKETS_TABLE}` (e.g. {', '.join(mismatches[:5])}). "
            "The Railway bot could not UPDATE the row — check bot logs and apply "
            "`supabase/migrations/20260516_tickets_active_anon_policies.sql`."
        )

    status = df["status"].astype(str).str.strip() if not df.empty else pd.Series(dtype=str)
    pending_mask = status.eq("Pending") if not df.empty else pd.Series(dtype=bool)
    open_mask = status.eq("Open") if not df.empty else pd.Series(dtype=bool)
    completed_mask = status.eq("Completed") if not df.empty else pd.Series(dtype=bool)

    total_pending = int(pending_mask.sum()) if not df.empty else 0
    total_open = int(open_mask.sum()) if not df.empty else 0

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
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Pending", f"{total_pending:,}")
    with m2:
        st.metric("Awaiting admin review", f"{total_open:,}")
    with m3:
        st.metric("Avg completion time", _format_duration(avg_seconds))
    with m4:
        st.metric(f"Tickets in last {lookback_days} {day_word}", f"{len(df):,}")

    st.divider()

    if total_open > 0:
        st.info(
            f"**{total_open}** ticket(s) have a field reply and are in **Open** "
            f"(awaiting your review). They leave **Pending** after the bot saves the response — "
            f"switch the queue below to **Open ({total_open})**."
        )

    queue_options = [
        _queue_segment_label("Pending", total_pending),
        _queue_segment_label("Open", total_open),
        _queue_segment_label("Completed", int(completed_mask.sum()) if not df.empty else 0),
        "Log",
    ]
    open_label = _queue_segment_label("Open", total_open)
    pending_label = _queue_segment_label("Pending", total_pending)
    prev_open = int(st.session_state.get("_dash_prev_open_count", 0))
    if total_open > prev_open:
        st.session_state["dash_queue_segmented"] = open_label
    elif "dash_queue_segmented" not in st.session_state:
        st.session_state["dash_queue_segmented"] = (
            open_label if total_open > 0 else pending_label
        )
    current_seg = st.session_state.get("dash_queue_segmented")
    if current_seg not in queue_options:
        st.session_state["dash_queue_segmented"] = (
            open_label if total_open > 0 else pending_label
        )
    st.session_state["_dash_prev_open_count"] = total_open

    queue_picked = st.segmented_control(
        "Ticket queues",
        options=queue_options,
        key="dash_queue_segmented",
        help=(
            "Pending = assigned, waiting on field. Open = field replied in Telegram, "
            "needs your review. Use sidebar **Refresh now** if auto-refresh is off."
        ),
    )
    queue_view = _queue_segment_base(queue_picked)
    st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

    if queue_view == "Pending":
        st.subheader("Assigned tasks (pending)")
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
                _render_admin_ticket_toolbar(
                    pend,
                    key_prefix="assigned",
                    title="Admin",
                    caption="Delete removes the active row; history stays in the Log tab.",
                    status_actions=(),
                    allow_delete=True,
                )

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
                    color = "background-color: rgba(215, 180, 145, 0.12); color: #f0e6dc" if stale else ""
                    return [color] * len(_row)

                try:
                    styled = view.style.apply(_row_red, axis=1)
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(view, use_container_width=True, hide_index=True)
                st.caption(
                    f"Rows with a **subtle oak tint** are pending for **more than 24 hours** "
                    f"since `created_at` (compared against current {LOCAL_TZ_LABEL} time)."
                )

    elif queue_view == "Open":
        st.subheader("Open — awaiting admin review")
        st.caption(
            "Field team has responded. Review the response below and pick a "
            "ticket to **Mark Completed** once it's verified, or leave it Open "
            "for follow-up."
        )
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            open_df = df[open_mask].copy()
            if open_df.empty:
                st.info(f"No tickets awaiting admin review in the last {lookback_days} {day_word}.")
            else:
                _render_admin_ticket_toolbar(
                    open_df,
                    key_prefix="open",
                    title="Admin",
                    caption="Mark reviewed as Completed, or delete the row.",
                    status_actions=(
                        ("Mark Completed", "Completed", "Completed"),
                    ),
                    allow_delete=True,
                )

                open_show = [
                    c
                    for c in [
                        "ticket_number",
                        "assigned_to",
                        "task_category",
                        "additional_info",
                        "field_response",
                        "photo_url",
                        "created_at",
                        "last_assigned_at",
                        "responded_at",
                    ]
                    if c in open_df.columns
                ]
                open_view = _format_local(open_df[open_show].copy())
                st.dataframe(
                    open_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config=_dataframe_column_config(open_view),
                )

                st.subheader("Gallery view — field photos awaiting review")
                _render_field_photos_section(open_df)

    elif queue_view == "Completed":
        st.subheader("Completed tickets")
        st.caption(
            "Only tickets the admin team has signed off on appear here. If "
            "something needs another look, send it back to **Open**."
        )
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            done = df[completed_mask].copy()
            if done.empty:
                st.info(f"No completed tickets in the last {lookback_days} {day_word}.")
            else:
                _render_admin_ticket_toolbar(
                    done,
                    key_prefix="completed",
                    title="Admin",
                    caption="Send back for more field work, or delete the row.",
                    status_actions=(
                        ("Send back to Open", "Open", "Reopened"),
                    ),
                    allow_delete=True,
                )

                show_cols = [
                    c
                    for c in [
                        "ticket_number",
                        "assigned_to",
                        "task_category",
                        "additional_info",
                        "field_response",
                        "photo_url",
                        "created_at",
                        "last_assigned_at",
                        "responded_at",
                    ]
                    if c in done.columns
                ]
                done_view = _format_local(done[show_cols])
                st.dataframe(
                    done_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config=_dataframe_column_config(done_view),
                )

                st.subheader("Gallery view — completed field photos")
                _render_field_photos_section(done)

    elif queue_view == "Log":
        _render_attendance_tab()
    else:
        st.warning("Unknown queue view; pick Pending, Open, Completed, or Log.")


def _render_field_photos_section(done: pd.DataFrame) -> None:
    """Group every Response photo per ticket and render as a thumbnail grid.

    Photo history comes from ``ticket_attendance_logs`` (every Response
    with a non-null ``photo_url``). For tickets that only ever had the
    single "pinned" ``photo_url`` on ``tickets_active`` -- e.g. before the
    storage migration was applied -- we still surface that one so nothing
    disappears from view.
    """
    ticket_ids = [
        str(t)
        for t in (done.get("ticket_number") if "ticket_number" in done.columns else [])
        if t is not None
    ]
    if not ticket_ids:
        st.caption("No completed tickets to show photos for.")
        return

    grouped = _fetch_ticket_photos(ticket_ids)

    # Fallback: stitch in the pinned tickets_active.photo_url if its URL
    # isn't already represented in the log history for that ticket. This
    # covers older rows recorded before the bot started logging Responses.
    if "photo_url" in done.columns:
        for _, row in done.iterrows():
            tid = str(row.get("ticket_number") or "").strip()
            pinned = str(row.get("photo_url") or "").strip()
            if not tid or not pinned.startswith("http"):
                continue
            existing_urls = {p["url"] for p in grouped.get(tid, [])}
            if pinned in existing_urls:
                continue
            grouped.setdefault(tid, []).append(
                {
                    "url": pinned,
                    "note": row.get("field_response"),
                    "member": row.get("assigned_to"),
                    "when": row.get("responded_at") or row.get("updated_at"),
                }
            )

    tickets_with_photos = [t for t in ticket_ids if grouped.get(t)]
    if not tickets_with_photos:
        st.caption("No field photos uploaded yet for these tickets.")
        return

    st.caption(
        f"**{len(tickets_with_photos)}** ticket(s) with photos. "
        f"Each ticket groups every photo the assignee has submitted, "
        f"newest first."
    )
    info_lookup: dict[str, str] = {}
    if "additional_info" in done.columns and "ticket_number" in done.columns:
        for _, row in done.iterrows():
            tid = str(row.get("ticket_number") or "").strip()
            info = row.get("additional_info")
            if tid and isinstance(info, str) and info.strip():
                info_lookup[tid] = info.strip()

    for tid in tickets_with_photos:
        photos = grouped.get(tid, [])
        with st.expander(f"Ticket {tid} — {len(photos)} photo(s)", expanded=False):
            info_text = info_lookup.get(tid)
            if info_text:
                st.caption("**Additional info from assignment:**")
                st.markdown(info_text)
                st.divider()
            _render_photo_grid(photos, cols_per_row=3)


def _render_missing_table_help(table: str) -> None:
    """Friendly screen shown when a required Supabase table is missing.

    Triggered the first time the new dashboard code is deployed against an
    old database that hasn't run ``20260512_history_and_rename.sql`` yet.
    """
    st.error(f"Supabase table **`{table}`** is missing.")
    st.markdown(
        "This usually means the **history migration hasn't been applied yet** "
        "in Supabase. Until it runs, the dashboard can't read its data."
    )
    st.markdown("**Fix:** open Supabase -> SQL Editor and run:")
    st.code(
        """-- supabase/migrations/20260512_history_and_rename.sql
-- (copy from the repo and paste here, then press Run)
""",
        language="sql",
    )
    st.caption(
        "After the migration succeeds, refresh this page. If you intentionally "
        "renamed the table, update `TICKETS_TABLE` / `ATTENDANCE_LOGS_TABLE` "
        "in this app's secrets to match."
    )


def _render_attendance_tab() -> None:
    """Search bar + timeline view backed by ``ticket_attendance_logs``."""
    st.subheader("Log — search & timeline")
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

    try:
        logs = _fetch_attendance(
            ticket_number=ticket_clean or None,
            member_query=member_clean or None,
            limit=100 if not ticket_clean and not member_clean else 500,
        )
    except _TableMissingError as missing:
        _render_missing_table_help(missing.table)
        return

    if logs.empty:
        st.info("No log entries match.")
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
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        column_config=_dataframe_column_config(table),
    )

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
                    st.image(photo, width=PHOTO_THUMB_WIDTH)
                except Exception as exc:
                    st.warning(f"Could not load image: {exc}")
                st.markdown(f"[Open photo in a new tab]({photo})")


# Streamlit executes this file as the app script; do not hide ``main()`` behind
# ``if __name__ == "__main__"`` — some run modes leave ``__name__`` unset to
# ``"__main__"``, which skips the gate entirely and looks like "no password".
main()
