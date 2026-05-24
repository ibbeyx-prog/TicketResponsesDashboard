"""
NetOps Coverage Eye — Streamlit dashboard for field ticket operations (Supabase).

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
    # Apply migration 20260520_dashboard_users.sql — per-user login (recommended).
    # Default seed users: admin & ibeyx / password ChangeMeNow! (change after first login).
    # Legacy fallback if no users yet:
    # DASHBOARD_PASSWORD = "<shared-password>"
    # DASHBOARD_OPERATOR_ALLOWLIST = "alice,bob"
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

Authentication: per-user **username + password** in Supabase table
``dashboard_users`` (see migration ``20260520_dashboard_users.sql``). Legacy
shared ``DASHBOARD_PASSWORD`` is only used when no dashboard users exist yet.
Command Center assignments store the signed-in **operator_id** in
``dashboard_assigned_by``. Use **Forgot password** on the login screen to get a
one-time reset code (15 minutes).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import html
import hmac
import json
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from cryptography.fernet import Fernet
import altair as alt
from bot_utils import (
    NOTIFY_BUILD_ID,
    AssignmentTelegramRef,
    delete_telegram_assignment_message,
    find_assignment_telegram_ref,
    normalize_telegram_group_id_paste,
    notify_telegram_group,
    update_telegram_assignment_message,
)
from task_categories import (
    DEFAULT_ASSIGNMENT_TASK_CATEGORIES,
    delete_task_category,
    fetch_task_category_names,
    normalize_task_category_name,
    sync_ticket_categories_into_table,
    task_categories_table,
    upsert_task_category,
)
from unattended import (
    OPS_TZ,
    STATUS_DAILY_TASK,
    STATUS_UNATTENDED,
    UNATTENDED_NUDGE_HOURS,
)

STATUS_UNDER_INVESTIGATION = "Under Investigation"
STATUS_ON_HOLD = "On Hold"
STATUS_RESOLVED = "Resolved"

# Active field queues: always visible even when outside the sidebar time range.
_ACTIVE_QUEUE_STATUSES: frozenset[str] = frozenset(
    {STATUS_DAILY_TASK, "Open", STATUS_ON_HOLD, STATUS_UNDER_INVESTIGATION}
)
_REASSIGNABLE_STATUSES: frozenset[str] = frozenset(_ACTIVE_QUEUE_STATUSES)
_LEGACY_STATUS_ALIASES: dict[str, str] = {
    "pending": STATUS_DAILY_TASK,
    "completed": STATUS_RESOLVED,
    "no answer": STATUS_ON_HOLD,
    "unavailable": STATUS_ON_HOLD,
}

# --- Sales Cases (separate track from field tickets; admin-first) ---
SALES_REGION_CODES: tuple[str, ...] = (
    "SOC",
    "EOC",
    "KOC",
    "LOC",
    "AOC",
    "GOC",
    "CENTRAL",
)
SALES_PRIORITY_OPTIONS: tuple[str, ...] = ("Strategic", "High", "Urgent", "Standard")
DEFAULT_SALES_CASE_CATEGORIES: tuple[str, ...] = (
    "QOS Issue",
    "Call Drop Issues",
    "Coverage Issues",
    "Mobile Data Issues",
    "Voice Call Issues",
)
SC_STATUS_SALES_TICKET = "Sales ticket"
SC_STATUS_INVESTIGATION = "Investigation"
SC_STATUS_REGIONAL = "Regional for site visit"
SC_STATUS_DESIGN = "Design"
SC_STATUS_RESOLVED = STATUS_RESOLVED
# Shown together under the Investigation queue (no separate Regional column).
_SC_INVESTIGATION_QUEUE_STATUSES: tuple[str, ...] = (
    SC_STATUS_INVESTIGATION,
    SC_STATUS_REGIONAL,
)

# Older rows / labels (mapped in UI until backfill migration runs).
_SC_LEGACY_STATUS_MAP: dict[str, str] = {
    "Sales intake": SC_STATUS_SALES_TICKET,
    "Admin triage": SC_STATUS_INVESTIGATION,
    "System check": SC_STATUS_INVESTIGATION,
    "Dispatch approved": SC_STATUS_INVESTIGATION,
    "Awaiting field": SC_STATUS_REGIONAL,
    "Field in progress": SC_STATUS_REGIONAL,
    "Admin review": SC_STATUS_DESIGN,
    "Closed": SC_STATUS_RESOLVED,
    "Residential customer": SC_STATUS_SALES_TICKET,
}


def _sc_effective_status(raw: object) -> str:
    s = str(raw or "").strip()
    return _SC_LEGACY_STATUS_MAP.get(s, s)


def _sc_status_actions_for_queue(queue_status: str) -> tuple[tuple[str, str], ...]:
    """(Action menu label, target status) for the active sales queue toolbar."""
    if queue_status == SC_STATUS_SALES_TICKET:
        return (
            ("Investigation", SC_STATUS_INVESTIGATION),
            ("Design", SC_STATUS_DESIGN),
            ("Resolved", SC_STATUS_RESOLVED),
        )
    if queue_status == SC_STATUS_INVESTIGATION:
        return (
            ("Design", SC_STATUS_DESIGN),
            ("Resolved", SC_STATUS_RESOLVED),
        )
    if queue_status == SC_STATUS_DESIGN:
        return (("Resolved", SC_STATUS_RESOLVED),)
    return ()


def _sc_status_actions_for_case(cur_status: str) -> tuple[tuple[str, str], ...]:
    """Action menu options for one selected case (row status may differ from queue view)."""
    if cur_status == SC_STATUS_SALES_TICKET:
        return (
            ("Investigation", SC_STATUS_INVESTIGATION),
            ("Design", SC_STATUS_DESIGN),
            ("Resolved", SC_STATUS_RESOLVED),
        )
    if cur_status in (SC_STATUS_INVESTIGATION, SC_STATUS_REGIONAL):
        return (
            ("Design", SC_STATUS_DESIGN),
            ("Resolved", SC_STATUS_RESOLVED),
        )
    if cur_status == SC_STATUS_DESIGN:
        return (("Resolved", SC_STATUS_RESOLVED),)
    return ()


def _sc_patch_with_action_comment(
    payload: dict,
    comment: str | None,
    *,
    on_resolve: bool = False,
) -> dict:
    out = dict(payload)
    note = (comment or "").strip() or None
    if note:
        out["additional_info"] = note
        if on_resolve:
            out["close_note"] = note
    return out


def _sc_apply_status_advance(
    row_id: str,
    *,
    r0: pd.Series,
    target_status: str,
    op: str,
    action_comment: str | None = None,
) -> str | None:
    """Move case to another queue. Returns an error message, or None on success."""
    cur = _sc_effective_status(r0.get("status"))
    if target_status == SC_STATUS_INVESTIGATION:
        if cur != SC_STATUS_SALES_TICKET:
            return f"Case is **{cur}** — **Investigation** from **Sales Ticket** only."
        payload = _sc_patch_with_action_comment(
            {"status": SC_STATUS_INVESTIGATION, "admin_owner": op},
            action_comment,
        )
        _sales_cases_update_row(row_id, payload)
        return None
    if target_status == SC_STATUS_DESIGN:
        if cur not in (
            SC_STATUS_SALES_TICKET,
            SC_STATUS_INVESTIGATION,
            SC_STATUS_REGIONAL,
        ):
            return f"Case is **{cur}** — cannot move to **Design**."
        payload = _sc_patch_with_action_comment(
            {"status": SC_STATUS_DESIGN},
            action_comment,
        )
        _sales_cases_update_row(row_id, payload)
        return None
    if target_status == SC_STATUS_RESOLVED:
        if cur not in (
            SC_STATUS_SALES_TICKET,
            SC_STATUS_INVESTIGATION,
            SC_STATUS_REGIONAL,
            SC_STATUS_DESIGN,
        ):
            return f"Case is **{cur}** — cannot **Resolve**."
        payload = _sc_patch_with_action_comment(
            {"status": SC_STATUS_RESOLVED},
            action_comment,
            on_resolve=True,
        )
        try:
            _sales_cases_update_row(row_id, payload)
        except Exception as exc:
            err = str(exc).lower()
            if "close_note" in err and ("column" in err or "schema" in err):
                return (
                    "Could not save comment — apply "
                    "`supabase/migrations/20260621_sales_case_close_note.sql` in Supabase."
                )
            raise
        return None
    _sales_cases_update_row(row_id, {"status": target_status})
    return None


def _sc_advance_selected_case(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    case_options: list[str],
    target_status: str,
    op: str,
    success_flash: str,
) -> None:
    picked = _require_selected_sales_cases(
        key_prefix=key_prefix, options=case_options, exactly_one=True
    )
    if not picked:
        return
    row_id, _cref, r0 = _picked_sales_case_from_selection(
        df, key_prefix=key_prefix, case_options=case_options
    )
    if not row_id or r0 is None:
        return
    action_comment = str(st.session_state.get("sc_action_comment", "")).strip() or None
    err = _sc_apply_status_advance(
        row_id,
        r0=r0,
        target_status=target_status,
        op=op,
        action_comment=action_comment,
    )
    if err:
        st.warning(err)
        return
    _get_supabase_client.clear()
    _sc_set_sales_flash(success_flash)
    st.rerun()


def _sc_sync_case_note_widgets(*, row_id: str, row: pd.Series) -> None:
    if st.session_state.get("sc_note_synced_id") == row_id:
        return
    st.session_state["sc_note_synced_id"] = row_id
    note = str(row.get("additional_info") or "")
    st.session_state["sc_case_note"] = note
    st.session_state["sc_close_note"] = str(row.get("close_note") or "")
    st.session_state["sc_action_comment"] = ""
    st.session_state["sc_edit_admin_notes"] = note


_SC_SALES_FLASH_KEY = "_dash_sales_cases_flash"
_SC_SALES_FLASH_LEVEL_KEY = "_dash_sales_cases_flash_level"

from dotenv import load_dotenv
from supabase_client import (
    get_cached_supabase_client,
    is_transient_supabase_error,
    resolve_supabase_config,
    test_supabase_connection,
)

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


_sb_cfg = resolve_supabase_config(
    env_path=_ENV_PATH,
    read_env=_read_setting,
    probe=False,
)
SUPABASE_URL = (_sb_cfg.url if _sb_cfg else _read_setting("SUPABASE_URL")).rstrip("/")
SUPABASE_KEY = _sb_cfg.key if _sb_cfg else _read_setting("SUPABASE_KEY")
_SUPABASE_KEY_SOURCE = _sb_cfg.key_source if _sb_cfg else "SUPABASE_KEY"
TICKETS_TABLE = _read_setting("TICKETS_TABLE", "tickets_active") or "tickets_active"
SALES_CASES_TABLE = (
    _read_setting("SALES_CASES_TABLE", "dashboard_sales_cases")
    or "dashboard_sales_cases"
)
ATTENDANCE_LOGS_TABLE = (
    _read_setting("ATTENDANCE_LOGS_TABLE", "ticket_attendance_logs")
    or "ticket_attendance_logs"
)
FIELD_ENGINEERS_TABLE = (
    _read_setting("FIELD_ENGINEERS_TABLE", "dashboard_field_engineers")
    or "dashboard_field_engineers"
)
TASK_CATEGORIES_TABLE = task_categories_table()
_CATEGORIES_SYNCED_ONCE_KEY = "_dashboard_categories_synced_once"

_TICKETS_MISSING_COLUMNS: set[str] = set()
_CC_FLASH_KEY = "_ticket_dashboard_cc_flash"
_CC_FLASH_LEVEL_KEY = "_ticket_dashboard_cc_flash_level"


def _cc_set_flash(message: str, *, level: str = "success") -> None:
    """Persist a sidebar message across ``st.rerun()`` (success / warning / error)."""
    st.session_state[_CC_FLASH_KEY] = message
    st.session_state[_CC_FLASH_LEVEL_KEY] = level


def _cc_show_flash() -> None:
    message = st.session_state.pop(_CC_FLASH_KEY, None)
    if not message:
        return
    level = str(st.session_state.pop(_CC_FLASH_LEVEL_KEY, "success") or "success")
    if level == "error":
        st.error(message)
    elif level == "warning":
        st.warning(message)
    else:
        st.success(message)


# Latest ``ticket_attendance_logs.timestamp`` the dashboard has already "seen"
# (for toast when Telegram/bot appends a new row).
_DASH_LAST_ATTENDANCE_TS_KEY = "_dash_last_seen_attendance_ts"
_DASH_RANGE_FROM_KEY = "_dash_range_from_utc"
_DASH_RANGE_TO_KEY = "_dash_range_to_utc"
_DASH_TIME_PRESET_KEY = "_dash_time_preset"
_DASH_TIME_PRESET_OPTIONS: tuple[str, ...] = (
    "Today",
    "Last 7 days",
    "Last 30 days",
    "Pick dates",
)
_DASH_SEARCH_FROM_DATE_KEY = "_dash_search_from_date"
_DASH_SEARCH_TO_DATE_KEY = "_dash_search_to_date"
_DASH_PREV_PRESET_KEY = "_dash_prev_preset"
_LEGACY_TIME_PRESET_MAP: dict[str, str] = {
    "Last 24 hours": "Today",
    "Single day": "Pick dates",
    "Custom range": "Pick dates",
}
_DASH_MAIN_NAV_KEY = "_dash_main_nav"
_DASH_TICKET_QUEUE_KEY = "_dash_ticket_queue"
_DASH_SALES_QUEUE_KEY = "_dash_sales_queue"
_DASH_PENDING_MAIN_NAV_KEY = "_dash_pending_main_nav"
_DASH_PENDING_TICKET_QUEUE_KEY = "_dash_pending_ticket_queue"
_DASH_PENDING_SALES_QUEUE_KEY = "_dash_pending_sales_queue"
_DASH_MAIN_NAV_OPTIONS: tuple[str, ...] = ("Tickets", "Sales Cases", "Log", "Performance")
_CC_SESSION_TOKEN_KEY = "_ticket_dashboard_cc_bot_token_session"
_CC_SESSION_GROUP_KEY = "cc_cmd_center_telegram_group_id"
_CC_FE_SELECT_KEY = "cc_fe_select"
_CC_FE_MANUAL_KEY = "cc_fe_manual"
_CC_TICKET_INPUT_KEY = "cc_ticket_number"
_CC_ASSIGN_NOTES_KEY = "cc_assign_notes"
_CC_CLEAR_ASSIGN_KEY = "_cc_clear_assign_form"
_CC_ADD_UNASSIGNED_KEY = "cc_add_unassigned"
_CC_CATEGORY_SELECT_KEY = "cc_category_select"
_CC_CATEGORY_SELECT_PENDING_KEY = "_cc_category_select_pending"
_SC_CC_ST_REF_KEY = "sc_cc_st_case_ref"
_SC_CC_ST_ACCOUNT_KEY = "sc_cc_st_account"
_SC_CC_ST_OWNER_KEY = "sc_cc_st_owner"
_SC_CC_ST_PRIORITY_KEY = "sc_cc_st_priority"
_SC_CC_ST_REGION_KEY = "sc_cc_st_region"
_SC_CC_ST_SCAT_KEY = "sc_cc_st_scat"
_SC_CC_ST_DESC_KEY = "sc_cc_st_desc"
_SC_CC_CLEAR_ST_INTAKE_KEY = "_sc_cc_clear_st_intake"
_CC_SIDEBAR_TAB_KEY = "_cc_sidebar_tab"
CC_TAB_CSM = "CSM"
CC_TAB_SALES = "SALES"
_CC_SIDEBAR_TAB_OPTIONS: tuple[str, ...] = (CC_TAB_CSM, CC_TAB_SALES)


def _assignment_edit_session_keys(prefix: str) -> dict[str, str]:
    return {
        "engineer": f"{prefix}_edit_engineer",
        "category": f"{prefix}_edit_category",
        "notes": f"{prefix}_edit_notes",
        "sync_tg": f"{prefix}_edit_sync_tg",
        "show": f"{prefix}_show_assignment_edit",
        "synced_ticket": f"{prefix}_edit_synced_ticket",
    }


def _reassign_session_keys(prefix: str) -> dict[str, str]:
    return {
        "engineer": f"{prefix}_reassign_engineer",
        "category": f"{prefix}_reassign_category",
        "notes": f"{prefix}_reassign_notes",
        "sync_tg": f"{prefix}_reassign_sync_tg",
        "show": f"{prefix}_show_reassign",
        "synced_ticket": f"{prefix}_reassign_synced_ticket",
    }


def _clear_reassign_panels_except(active_prefix: str) -> None:
    """Close reassign forms from other queues so status checks stay aligned."""
    for prefix in ("assigned", "open", "on_hold", "investigation"):
        if prefix != active_prefix:
            st.session_state.pop(_reassign_session_keys(prefix)["show"], None)


def _manual_field_response_session_keys(prefix: str) -> dict[str, str]:
    return {
        "show": f"{prefix}_show_manual_field_response",
        "text": f"{prefix}_mfr_text",
        "responded_by": f"{prefix}_mfr_responded_by",
        "synced_ticket": f"{prefix}_mfr_synced_ticket",
    }


def _dash_normalize_handle(handle: str) -> str:
    return handle.strip().lstrip("@").lower()


def _resolve_field_responded_by(assigned_to: object, replier_label: str) -> str | None:
    """Set only when the replier is not the ticket assignee (matches bot.py)."""
    assignee = str(assigned_to or "").strip()
    label = (replier_label or "").strip()
    if not assignee or not label:
        return None
    assignee_at = assignee if assignee.startswith("@") else f"@{assignee.lstrip('@')}"
    if _dash_normalize_handle(label) == _dash_normalize_handle(assignee_at):
        return None
    return label if label.startswith("@") else f"@{label.lstrip('@')}"


def _sync_assignment_edit_widgets(
    *,
    keys: dict[str, str],
    picked: str,
    current_handle: str,
    current_cat: str,
    current_notes: str,
    cats: list[str],
    fe_names: list[str],
    fe_missing: bool,
) -> None:
    """Push the selected ticket's values into edit widgets (Streamlit keeps stale keys)."""
    if st.session_state.get(keys["synced_ticket"]) == picked:
        return
    st.session_state[keys["synced_ticket"]] = picked
    if fe_names and not fe_missing:
        fe_opts = [f"@{n}" for n in fe_names]
        default_fe = (
            f"@{current_handle}"
            if current_handle and f"@{current_handle}" in fe_opts
            else (fe_opts[0] if fe_opts else "")
        )
        st.session_state[keys["engineer"]] = default_fe
    else:
        st.session_state[keys["engineer"]] = current_handle
    st.session_state[keys["category"]] = (
        current_cat if current_cat in cats else (cats[0] if cats else "")
    )
    st.session_state[keys["notes"]] = current_notes

# Session keys — namespaced so we never collide with other widgets / demos,
# and so a stale boolean from an older app version cannot bypass the gate.
_AUTH_OK_KEY = "_ticket_dashboard_auth_ok"
_AUTH_PWD_VER_KEY = "_ticket_dashboard_auth_pwd_ver"
_AUTH_USERNAME_KEY = "_ticket_dashboard_auth_username"
_OPERATOR_ID_KEY = "_ticket_dashboard_operator_id"
_LOGIN_VIEW_KEY = "_ticket_dashboard_login_view"
_LOGIN_USER_WIDGET_KEY = "login_username_widget"
_LOGIN_PWD_WIDGET_KEY = "login_password_widget"
_LOGIN_OID_WIDGET_KEY = "login_operator_id_widget"
_LOGIN_SAVE_PW_KEY = "login_save_password"
_LOGIN_REMEMBER_BOOT_KEY = "_login_remember_bootstrapped"
_MIN_DASHBOARD_PASSWORD_LEN = 8
_MAX_OPERATOR_ID_LEN = 64
_MAX_DASHBOARD_USERNAME_LEN = 48


def _remember_login_fernet() -> Fernet:
    pepper = (
        _read_setting("DASHBOARD_SESSION_SECRET")
        or _read_setting("SUPABASE_KEY")
        or "ticket-dashboard-remember"
    )
    key = base64.urlsafe_b64encode(hashlib.sha256(pepper.encode()).digest())
    return Fernet(key)


def _encode_remembered_login(*, username: str, password: str) -> str:
    payload = json.dumps({"u": username, "p": password})
    return _remember_login_fernet().encrypt(payload.encode()).decode()


def _decode_remembered_login(token: str) -> tuple[str, str] | None:
    try:
        raw = _remember_login_fernet().decrypt(token.encode())
        data = json.loads(raw.decode())
        username = str(data.get("u", "")).strip()
        password = str(data.get("p", ""))
        if username and password:
            return username, password
    except Exception:
        pass
    return None


def _login_remember_bootstrap() -> None:
    """Load saved credentials from browser localStorage (encrypted token)."""
    if st.session_state.get(_LOGIN_REMEMBER_BOOT_KEY):
        return

    qp = st.query_params
    if qp.get("_lr") == "1":
        token = str(qp.get("_lt") or "")
        pair = _decode_remembered_login(token) if token else None
        if pair:
            st.session_state[_LOGIN_USER_WIDGET_KEY] = pair[0]
            st.session_state[_LOGIN_OID_WIDGET_KEY] = pair[0]
            st.session_state[_LOGIN_PWD_WIDGET_KEY] = pair[1]
            st.session_state[_LOGIN_SAVE_PW_KEY] = True
        st.session_state[_LOGIN_REMEMBER_BOOT_KEY] = True
        try:
            del st.query_params["_lr"]
            del st.query_params["_lt"]
        except Exception:
            pass
        return

    if st.session_state.get("_login_remember_js_ran"):
        st.session_state[_LOGIN_REMEMBER_BOOT_KEY] = True
        return
    st.session_state["_login_remember_js_ran"] = True

    components.html(
        """
        <script>
        (function () {
          const KEY = "fto_remember_v1";
          const loc = window.parent.location;
          const params = new URLSearchParams(loc.search);
          if (params.get("_lr") === "1") return;
          const token = localStorage.getItem(KEY);
          if (!token) return;
          const u = new URL(loc.href);
          u.searchParams.set("_lr", "1");
          u.searchParams.set("_lt", token);
          window.parent.location.replace(u.toString());
        })();
        </script>
        """,
        height=0,
    )


def _login_remember_persist(*, username: str, password: str) -> None:
    token = _encode_remembered_login(username=username, password=password)
    safe = json.dumps(token)
    components.html(
        f"""
        <script>
        localStorage.setItem("fto_remember_v1", {safe});
        </script>
        """,
        height=0,
    )


def _login_remember_clear() -> None:
    components.html(
        """
        <script>
        localStorage.removeItem("fto_remember_v1");
        </script>
        """,
        height=0,
    )


def _password_fingerprint(secret: str) -> str:
    """Opaque token derived from the configured password (not reversible).

    Stored alongside the session flag so rotating ``DASHBOARD_PASSWORD``
    invalidates existing browser sessions without needing server-side logout.
    """
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _auth_session_fingerprint(*, username: str, operator_id: str) -> str:
    pepper = (
        _read_setting("DASHBOARD_SESSION_SECRET")
        or _read_setting("SUPABASE_KEY")
        or "ticket-dashboard-session"
    )
    payload = f"{username.casefold()}|{operator_id}|{pepper}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _clear_auth_session() -> None:
    for key in (
        _AUTH_OK_KEY,
        _AUTH_PWD_VER_KEY,
        _AUTH_USERNAME_KEY,
        _OPERATOR_ID_KEY,
        _LOGIN_REMEMBER_BOOT_KEY,
        "_login_remember_js_ran",
    ):
        st.session_state.pop(key, None)


def _complete_auth_session(*, username: str, operator_id: str, session_fp: str) -> None:
    st.session_state[_AUTH_OK_KEY] = True
    st.session_state[_AUTH_PWD_VER_KEY] = session_fp
    st.session_state[_AUTH_USERNAME_KEY] = username
    st.session_state[_OPERATOR_ID_KEY] = operator_id


def _normalize_dashboard_username(raw: str) -> str:
    s = "".join(
        ch
        for ch in (raw or "").strip().lower()
        if ch.isalnum() or ch in "._-"
    )
    if not s:
        raise ValueError("Enter a **username**.")
    if len(s) > _MAX_DASHBOARD_USERNAME_LEN:
        raise ValueError(
            f"Username is too long (max {_MAX_DASHBOARD_USERNAME_LEN} characters)."
        )
    return s


def _maybe_probe_alternate_supabase_key() -> None:
    """Once per session, try ``SUPABASE_ANON_KEY`` if the primary key cannot connect."""
    global SUPABASE_KEY, _SUPABASE_KEY_SOURCE
    if st.session_state.get("_dash_sb_key_probed"):
        return
    st.session_state["_dash_sb_key_probed"] = True
    cfg = resolve_supabase_config(
        env_path=_ENV_PATH,
        read_env=_read_setting,
        probe=True,
    )
    if not cfg:
        return
    if cfg.key != SUPABASE_KEY:
        SUPABASE_KEY = cfg.key
        _SUPABASE_KEY_SOURCE = cfg.key_source
        _get_supabase_client.clear()


def _dashboard_users_configured() -> bool | None:
    """``True``/``False`` when Supabase answers; ``None`` when unreachable (timeout)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    _maybe_probe_alternate_supabase_key()
    try:
        res = _get_supabase_client().rpc("dashboard_users_configured").execute()
        return bool(res.data)
    except Exception as exc:
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return None
        return False


def _rpc_dashboard_verify_login(username: str, password: str) -> dict:
    res = (
        _get_supabase_client()
        .rpc(
            "dashboard_verify_login",
            {"p_username": username, "p_password": password},
        )
        .execute()
    )
    data = res.data
    if isinstance(data, dict):
        return data
    return {}


def _rpc_dashboard_request_password_reset(username: str) -> dict:
    res = (
        _get_supabase_client()
        .rpc(
            "dashboard_request_password_reset",
            {"p_username": username},
        )
        .execute()
    )
    data = res.data
    if isinstance(data, dict):
        return data
    return {}


def _rpc_dashboard_reset_password(
    username: str, reset_code: str, new_password: str
) -> dict:
    res = (
        _get_supabase_client()
        .rpc(
            "dashboard_reset_password",
            {
                "p_username": username,
                "p_reset_code": reset_code,
                "p_new_password": new_password,
            },
        )
        .execute()
    )
    data = res.data
    if isinstance(data, dict):
        return data
    return {}


_DEFAULT_DASHBOARD_ADMIN_USERNAMES = "admin,ibeyx"


def _dashboard_admin_usernames() -> frozenset[str]:
    raw = _read_setting("DASHBOARD_ADMIN_USERNAMES", _DEFAULT_DASHBOARD_ADMIN_USERNAMES)
    if not raw:
        return frozenset({"admin", "ibeyx"})
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return frozenset(parts) if parts else frozenset({"admin", "ibeyx"})


def _session_dashboard_username() -> str | None:
    raw = st.session_state.get(_AUTH_USERNAME_KEY)
    if not raw:
        return None
    return str(raw).strip().lower()


def _is_dashboard_admin() -> bool:
    u = _session_dashboard_username()
    return bool(u and u in _dashboard_admin_usernames())


def _normalize_dashboard_operator_id(raw: str) -> str:
    s = " ".join((raw or "").strip().split())
    if not s:
        raise ValueError("Enter an **operator display name**.")
    if len(s) > _MAX_OPERATOR_ID_LEN:
        raise ValueError(
            f"Operator name is too long (max {_MAX_OPERATOR_ID_LEN} characters)."
        )
    if any(ord(ch) < 32 for ch in s):
        raise ValueError("Operator name contains invalid characters.")
    return s


def _rpc_dashboard_admin_list_users(admin_username: str, admin_password: str) -> dict:
    res = (
        _get_supabase_client()
        .rpc(
            "dashboard_admin_list_users",
            {
                "p_admin_username": admin_username,
                "p_admin_password": admin_password,
            },
        )
        .execute()
    )
    data = res.data
    return data if isinstance(data, dict) else {}


def _rpc_dashboard_admin_create_user(
    *,
    admin_username: str,
    admin_password: str,
    new_username: str,
    operator_id: str,
    new_password: str,
) -> dict:
    res = (
        _get_supabase_client()
        .rpc(
            "dashboard_admin_create_user",
            {
                "p_admin_username": admin_username,
                "p_admin_password": admin_password,
                "p_new_username": new_username,
                "p_new_operator_id": operator_id,
                "p_new_password": new_password,
            },
        )
        .execute()
    )
    data = res.data
    return data if isinstance(data, dict) else {}


def _rpc_dashboard_admin_set_user_active(
    *,
    admin_username: str,
    admin_password: str,
    target_username: str,
    is_active: bool,
) -> dict:
    res = (
        _get_supabase_client()
        .rpc(
            "dashboard_admin_set_user_active",
            {
                "p_admin_username": admin_username,
                "p_admin_password": admin_password,
                "p_target_username": target_username,
                "p_is_active": is_active,
            },
        )
        .execute()
    )
    data = res.data
    return data if isinstance(data, dict) else {}


def _dashboard_admin_error_message(err: str) -> str:
    messages = {
        "forbidden": "Incorrect admin password.",
        "invalid_username": "Invalid username (use lowercase letters, digits, . _ -).",
        "invalid_operator_id": "Invalid operator display name.",
        "weak_password": f"Password must be at least {_MIN_DASHBOARD_PASSWORD_LEN} characters.",
        "username_taken": "That username already exists.",
        "operator_id_taken": "That operator display name is already in use.",
        "cannot_deactivate_self": "You cannot deactivate your own account while signed in.",
        "last_active_user": "Cannot deactivate the last active account.",
        "not_found": "User not found.",
    }
    return messages.get(err, f"Could not complete action ({err or 'unknown'}).")


def _render_dashboard_team_accounts() -> None:
    admin_user = _session_dashboard_username()
    if not admin_user:
        return

    with st.expander("Team Accounts (Admin)", expanded=False, key="bon_box_team_accounts"):
        st.caption(
            "Create dashboard logins for your team. "
            "Re-enter **your** password to confirm each action."
        )
        admin_pw = st.text_input(
            "Your Password (Confirm)",
            type="password",
            key="dash_team_admin_pw",
            autocomplete="current-password",
        )

        view_tab, add_tab = st.tabs(["Accounts", "Add user"])

        with view_tab:
            if st.button("Refresh List", key="dash_team_refresh", use_container_width=True):
                if not admin_pw:
                    st.warning("Enter your password first.")
                else:
                    try:
                        payload = _rpc_dashboard_admin_list_users(
                            admin_user, admin_pw
                        )
                    except Exception as exc:
                        st.error(f"Could not load accounts: {exc}")
                    else:
                        if not payload.get("ok"):
                            st.error(
                                _dashboard_admin_error_message(
                                    str(payload.get("error") or "")
                                )
                            )
                        else:
                            st.session_state["_dash_team_users_cache"] = (
                                payload.get("users") or []
                            )
                            st.rerun()

            users = st.session_state.get("_dash_team_users_cache")
            if not users:
                st.caption("Click **Refresh List** to load accounts.")
            else:
                for row in users:
                    uname = str(row.get("username") or "")
                    opid = str(row.get("operator_id") or "")
                    active = bool(row.get("is_active", True))
                    label = f"**{uname}** → {opid}"
                    if not active:
                        label += " _(disabled)_"
                    c_info, c_act = st.columns([4, 1], gap="small")
                    with c_info:
                        st.markdown(label)
                    with c_act:
                        btn_label = "Enable" if not active else "Disable"
                        hkey = hashlib.sha256(uname.encode("utf-8")).hexdigest()[:12]
                        if st.button(
                            btn_label,
                            key=f"dash_team_toggle_{hkey}",
                            use_container_width=True,
                        ):
                            if not admin_pw:
                                st.warning("Enter your password first.")
                            else:
                                try:
                                    payload = _rpc_dashboard_admin_set_user_active(
                                        admin_username=admin_user,
                                        admin_password=admin_pw,
                                        target_username=uname,
                                        is_active=not active,
                                    )
                                except Exception as exc:
                                    st.error(str(exc))
                                else:
                                    if not payload.get("ok"):
                                        st.error(
                                            _dashboard_admin_error_message(
                                                str(payload.get("error") or "")
                                            )
                                        )
                                    else:
                                        st.session_state.pop(
                                            "_dash_team_users_cache", None
                                        )
                                        st.rerun()

        with add_tab:
            with st.form("dash_team_create_form", clear_on_submit=True):
                new_user = st.text_input(
                    "Username",
                    placeholder="e.g. ali.ops",
                    help="Lowercase login name.",
                )
                new_op = st.text_input(
                    "Operator display name",
                    placeholder="Shown on assignments (often same as username)",
                )
                new_pw = st.text_input("Temporary Password", type="password")
                confirm_pw = st.text_input("Confirm Password", type="password")
                submitted = st.form_submit_button(
                    "Create account", use_container_width=True
                )

            if not submitted:
                return
            if not admin_pw:
                st.error("Enter your password under **Your Password (Confirm)** first.")
                return
            try:
                uname = _normalize_dashboard_username(new_user)
            except ValueError as ve:
                st.error(str(ve))
                return
            try:
                opid = _normalize_dashboard_operator_id(new_op or uname)
            except ValueError as ve:
                st.error(str(ve))
                return
            if len(new_pw or "") < _MIN_DASHBOARD_PASSWORD_LEN:
                st.error(
                    f"Password must be at least {_MIN_DASHBOARD_PASSWORD_LEN} characters."
                )
                return
            if new_pw != confirm_pw:
                st.error("Passwords do not match.")
                return
            try:
                payload = _rpc_dashboard_admin_create_user(
                    admin_username=admin_user,
                    admin_password=admin_pw,
                    new_username=uname,
                    operator_id=opid,
                    new_password=new_pw,
                )
            except Exception as exc:
                st.error(f"Could not create account: {exc}")
                return
            if not payload.get("ok"):
                st.error(
                    _dashboard_admin_error_message(str(payload.get("error") or ""))
                )
                return
            st.session_state.pop("_dash_team_users_cache", None)
            st.success(
                f"Created **{uname}** (operator **{opid}**). "
                "Share the temporary password securely; they can change it via "
                "**Forgot password** on the login screen."
            )


def _render_login_page_styles() -> None:
    st.markdown(
        """
        <style>
        /* Login — BON brand */
        .stApp,
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"],
        [data-testid="stMain"],
        [data-testid="stAppViewContainer"] section.main {
            background: #000 !important;
        }
        [data-testid="stHeader"] {
            border-bottom: none !important;
            box-shadow: none !important;
            background: transparent !important;
        }
        [data-testid="stAppViewContainer"] {
            min-height: 100vh;
        }
        [data-testid="stMain"] {
            min-height: 100vh;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
        }
        [data-testid="stAppViewContainer"] section.main {
            width: 100%;
            min-height: 100vh;
            display: grid !important;
            place-items: center !important;
            padding: 0 !important;
            position: relative;
            overflow: hidden;
        }
        [data-testid="stAppViewContainer"] section.main::before {
            content: "";
            position: fixed;
            inset: 0;
            z-index: 0;
            pointer-events: none;
            background:
                radial-gradient(ellipse 80% 50% at 20% 40%, rgba(241, 90, 41, 0.12), transparent 55%),
                radial-gradient(ellipse 70% 45% at 80% 60%, rgba(0, 179, 198, 0.1), transparent 50%),
                radial-gradient(ellipse 50% 40% at 50% 100%, rgba(247, 147, 30, 0.08), transparent 45%);
        }
        [data-testid="stAppViewContainer"] section.main .block-container {
            width: min(30rem, 92vw) !important;
            max-width: 30rem !important;
            margin: 0 auto !important;
            padding: 0.5rem 0.75rem 1.5rem !important;
            position: relative;
            z-index: 2;
        }
        div.st-key-login_panel,
        div.st-key-login_panel > div {
            width: min(30rem, 92vw) !important;
            max-width: 30rem !important;
            margin-left: auto !important;
            margin-right: auto !important;
        }
        div.st-key-login_shell,
        div.st-key-login_shell [data-testid="stVerticalBlockBorderWrapper"] {
            width: 100% !important;
            max-width: 30rem !important;
            margin-left: auto !important;
            margin-right: auto !important;
            background: rgba(0, 0, 0, 0.55) !important;
            border: 1px solid rgba(255, 255, 255, 0.12) !important;
            border-radius: 1rem !important;
            backdrop-filter: blur(12px);
            box-shadow: 0 0 40px rgba(0, 179, 198, 0.08);
        }
        [data-testid="stAppViewContainer"] section.main .block-container [data-testid="stMarkdownContainer"] {
            width: 100% !important;
            text-align: center !important;
        }
        h2.bon-login-title {
            font-size: clamp(2rem, 6vw, 2.75rem) !important;
            font-weight: 800 !important;
            text-align: center !important;
            width: 100% !important;
            margin: 0 auto 0.35rem auto !important;
            padding: 0 !important;
            letter-spacing: -0.02em;
        }
        .bon-login-line {
            display: block !important;
        }
        .bon-login-word-netops {
            color: #F7931E !important;
            -webkit-text-fill-color: #F7931E !important;
        }
        .bon-login-word-coverage,
        .bon-login-word-eye {
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
        }
        p.bon-login-sub {
            font-size: 0.9rem !important;
            color: #a39e97 !important;
            text-align: center !important;
            width: 100% !important;
            margin: 0 auto 0.85rem auto !important;
        }
        [data-testid="stAppViewContainer"] section.main .block-container [data-testid="stCaptionContainer"] {
            text-align: center;
        }
        [data-testid="stAppViewContainer"] section.main [data-testid="stForm"] {
            border: none !important;
            padding: 0 !important;
        }
        [data-testid="stAppViewContainer"] section.main button[kind="primary"] {
            background: linear-gradient(90deg, #F15A29, #F7931E) !important;
            border: 1px solid rgba(255,255,255,0.1) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _operator_id_allowlist() -> frozenset[str] | None:
    """If ``DASHBOARD_OPERATOR_ALLOWLIST`` is set, only those IDs may sign in."""
    raw = _read_setting("DASHBOARD_OPERATOR_ALLOWLIST")
    if not raw:
        return None
    parsed = {p.strip().casefold() for p in raw.split(",") if p.strip()}
    return frozenset(parsed) if parsed else None


def _normalize_operator_id(raw: str) -> str:
    """Validate Operator ID from the login form (no newlines / control chars)."""
    s = "".join(
        ch for ch in (raw or "").strip() if ch.isprintable() and ch not in "\r\n\t\0"
    )
    if not s:
        raise ValueError("Enter an **Operator ID** (your name or team login).")
    if len(s) > _MAX_OPERATOR_ID_LEN:
        raise ValueError(f"Operator ID is too long (max {_MAX_OPERATOR_ID_LEN} characters).")
    allow = _operator_id_allowlist()
    if allow is not None and s.casefold() not in allow:
        raise ValueError(
            "That Operator ID is not permitted. Check **DASHBOARD_OPERATOR_ALLOWLIST** "
            "in `.env` or Streamlit Secrets."
        )
    return s


def _session_operator_id() -> str | None:
    raw = st.session_state.get(_OPERATOR_ID_KEY)
    if raw in (None, ""):
        return None
    s = str(raw).strip()
    return s or None


def _cc_assignment_log_note(additional_info: str | None, operator_id: str) -> str | None:
    """Prefix attendance-log note with who used Command Center."""
    parts: list[str] = [f"Dashboard operator: {operator_id.strip()}"]
    note = (additional_info or "").strip()
    if note:
        parts.append(note)
    return "\n\n".join(parts)


def _read_dashboard_password() -> str:
    """Read ``DASHBOARD_PASSWORD`` after Streamlit has started (env + secrets)."""
    return _read_setting("DASHBOARD_PASSWORD")


def _render_login_sign_in(*, per_user: bool, legacy_password: str) -> None:
    _login_remember_bootstrap()

    with st.container(border=True, key="login_shell"):
        with st.form("login_form", clear_on_submit=False):
            if per_user:
                user = st.text_input(
                    "Username",
                    placeholder="your login name",
                    autocomplete="username",
                    key=_LOGIN_USER_WIDGET_KEY,
                )
                pwd = st.text_input(
                    "Password",
                    type="password",
                    autocomplete="current-password",
                    key=_LOGIN_PWD_WIDGET_KEY,
                )
            else:
                st.caption(
                    "**Local / legacy login** — not your Telegram username. "
                    "Use the shared password from `.env` → `DASHBOARD_PASSWORD`."
                )
                user = ""
                pwd = st.text_input(
                    "Password",
                    type="password",
                    placeholder="Value of DASHBOARD_PASSWORD in .env",
                    autocomplete="current-password",
                    key=_LOGIN_PWD_WIDGET_KEY,
                )
                oid = st.text_input(
                    "Operator ID",
                    placeholder="Your name, e.g. ibeyx",
                    key=_LOGIN_OID_WIDGET_KEY,
                )
            st.checkbox(
                "Save Password on This Device",
                key=_LOGIN_SAVE_PW_KEY,
                help="Stores an encrypted login token in this browser only.",
            )
            submitted = st.form_submit_button("Sign In", use_container_width=True)

    if not submitted:
        return

    if per_user:
        try:
            uname = _normalize_dashboard_username(user)
        except ValueError as ve:
            st.error(str(ve))
            return
        if not pwd:
            st.error("Enter your password.")
            return
        try:
            payload = _rpc_dashboard_verify_login(uname, pwd)
        except Exception as exc:
            st.error(f"Could not verify login: {exc}")
            return
        if not payload.get("ok"):
            st.error("Incorrect username or password.")
            return
        op = str(payload.get("operator_id") or uname).strip()
        fp = _auth_session_fingerprint(username=uname, operator_id=op)
        _complete_auth_session(username=uname, operator_id=op, session_fp=fp)
        if st.session_state.get(_LOGIN_SAVE_PW_KEY):
            _login_remember_persist(username=uname, password=pwd)
        else:
            _login_remember_clear()
        st.session_state[_LOGIN_VIEW_KEY] = "sign_in"
        st.rerun()
        return

    if not hmac.compare_digest(pwd, legacy_password):
        st.error("Incorrect password.")
        return
    try:
        op = _normalize_operator_id(oid)
    except ValueError as ve:
        st.error(str(ve))
        return
    fp = _password_fingerprint(legacy_password)
    _complete_auth_session(username=op.casefold(), operator_id=op, session_fp=fp)
    st.rerun()


def _render_login_forgot_request() -> None:
    with st.container(border=True, key="login_shell"):
        st.caption("Enter your username. If the account exists, you will get a reset code.")
        with st.form("login_forgot_request_form", clear_on_submit=False):
            user = st.text_input("Username", placeholder="your login name")
            submitted = st.form_submit_button("Get reset code", use_container_width=True)

    if not submitted:
        return
    try:
        uname = _normalize_dashboard_username(user)
    except ValueError as ve:
        st.error(str(ve))
        return
    try:
        payload = _rpc_dashboard_request_password_reset(uname)
    except Exception as exc:
        st.error(f"Could not start reset: {exc}")
        return

    if payload.get("message") == "code_issued" and payload.get("reset_code"):
        st.session_state["_dash_reset_username"] = uname
        st.session_state["_dash_reset_code_display"] = str(payload["reset_code"])
        st.session_state[_LOGIN_VIEW_KEY] = "forgot_reset"
        st.rerun()
        return

    st.info(
        "If that username exists, a reset code can be issued. "
        "Check the spelling or contact your admin."
    )


def _render_login_forgot_reset() -> None:
    uname = str(st.session_state.get("_dash_reset_username", ""))
    shown_code = st.session_state.get("_dash_reset_code_display")
    with st.container(border=True, key="login_shell"):
        if shown_code:
            st.success(
                f"Reset code for **{uname}**: `{shown_code}` "
                f"(valid **15 minutes**). Enter it below with your new password."
            )
        with st.form("login_forgot_reset_form", clear_on_submit=False):
            user = st.text_input("Username", value=uname or "")
            code = st.text_input("Reset Code", placeholder="8-character code")
            new_pw = st.text_input("New Password", type="password")
            confirm_pw = st.text_input("Confirm New Password", type="password")
            submitted = st.form_submit_button("Set new password", use_container_width=True)

    if not submitted:
        return
    try:
        uname_norm = _normalize_dashboard_username(user)
    except ValueError as ve:
        st.error(str(ve))
        return
    if len(new_pw or "") < _MIN_DASHBOARD_PASSWORD_LEN:
        st.error(f"Password must be at least {_MIN_DASHBOARD_PASSWORD_LEN} characters.")
        return
    if new_pw != confirm_pw:
        st.error("New passwords do not match.")
        return
    try:
        payload = _rpc_dashboard_reset_password(
            uname_norm,
            (code or "").strip().upper(),
            new_pw,
        )
    except Exception as exc:
        st.error(f"Could not reset password: {exc}")
        return
    if not payload.get("ok"):
        st.error("Invalid or expired reset code. Request a new code.")
        return
    st.session_state.pop("_dash_reset_username", None)
    st.session_state.pop("_dash_reset_code_display", None)
    st.session_state[_LOGIN_VIEW_KEY] = "sign_in"
    st.success("Password updated. Sign in with your new password.")
    st.rerun()


def _check_password() -> None:
    """Block until the viewer has a valid session (per-user or legacy shared password)."""
    per_user_state = _dashboard_users_configured()
    legacy_pw = _read_dashboard_password()

    if per_user_state is None:
        # Supabase timeout — allow local shared-password login when configured.
        if legacy_pw:
            per_user = False
        else:
            st.error("Cannot reach **Supabase** (connection timed out).")
            st.info(
                "Fix network/VPN/firewall, or set **`DASHBOARD_PASSWORD`** in `.env` "
                "for offline shared-password login while developing locally."
            )
            st.stop()
    else:
        per_user = bool(per_user_state)
        if per_user:
            legacy_pw = ""

    if per_user:
        auth_ready = True
    elif legacy_pw:
        auth_ready = True
    else:
        auth_ready = False

    if not auth_ready:
        st.error("Dashboard login is not configured.")
        on_cloud = str(_ENV_PATH).startswith("/mount/src/")
        if on_cloud:
            st.info(
                "Apply migration `supabase/migrations/20260520_dashboard_users.sql` "
                "in Supabase, then sign in with a dashboard user. "
                "Or set legacy `DASHBOARD_PASSWORD` in Secrets until users exist."
            )
        else:
            st.info(
                "Run migration `20260520_dashboard_users.sql` in Supabase SQL editor "
                "and add users to `dashboard_users`, or set `DASHBOARD_PASSWORD` for "
                "legacy shared-password mode."
            )
        st.stop()

    session_fp = st.session_state.get(_AUTH_PWD_VER_KEY)
    if (
        st.session_state.get(_AUTH_OK_KEY) is True
        and session_fp
        and _session_operator_id()
        and st.session_state.get(_AUTH_USERNAME_KEY)
    ):
        uname = str(st.session_state.get(_AUTH_USERNAME_KEY, ""))
        op = str(st.session_state.get(_OPERATOR_ID_KEY, ""))
        expected = (
            _auth_session_fingerprint(username=uname, operator_id=op)
            if per_user
            else _password_fingerprint(legacy_pw)
        )
        if hmac.compare_digest(str(session_fp), expected):
            return

    _clear_auth_session()

    _inject_bon_theme()
    _render_login_page_styles()

    view = st.session_state.get(_LOGIN_VIEW_KEY, "sign_in")
    if view not in ("sign_in", "forgot_request", "forgot_reset"):
        view = "sign_in"

    with st.container(key="login_panel"):
        st.markdown(
            '<h2 class="bon-login-title">'
            '<span class="bon-login-line">'
            '<span class="bon-login-word-netops">NetOps</span></span>'
            '<span class="bon-login-line">'
            '<span class="bon-login-word-coverage">Coverage</span> '
            '<span class="bon-login-word-eye">Eye</span></span></h2>'
            '<p class="bon-login-sub">Sign in to continue.</p>',
            unsafe_allow_html=True,
        )

        if per_user_state is None and legacy_pw:
            st.warning(
                "Supabase is unreachable from this PC — using **shared password** login. "
                "Ticket data will not load until the connection works."
            )

        _render_login_supabase_status()

        if per_user:
            c1, c2 = st.columns(2)
            with c1:
                if st.button(
                    "Sign in",
                    use_container_width=True,
                    type="primary" if view == "sign_in" else "secondary",
                ):
                    st.session_state[_LOGIN_VIEW_KEY] = "sign_in"
                    st.rerun()
            with c2:
                if st.button(
                    "Forgot Password",
                    use_container_width=True,
                    type="primary" if view != "sign_in" else "secondary",
                ):
                    st.session_state[_LOGIN_VIEW_KEY] = "forgot_request"
                    st.rerun()

        if view == "sign_in":
            _render_login_sign_in(per_user=per_user, legacy_password=legacy_pw)
        elif view == "forgot_reset":
            _render_login_forgot_reset()
        else:
            _render_login_forgot_request()

    st.stop()


_SUPABASE_HTTP_TIMEOUT_SEC = float(os.getenv("SUPABASE_HTTP_TIMEOUT_SEC", "25"))
_DASH_SUPABASE_DOWN_KEY = "_dash_supabase_unreachable"


def _render_login_supabase_status() -> None:
    """Show whether this PC can reach Supabase with the configured API key."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    cache_key = "_dash_login_sb_status"
    status = st.session_state.get(cache_key)
    if status is None:
        _maybe_probe_alternate_supabase_key()
        status = test_supabase_connection(SUPABASE_URL, SUPABASE_KEY)
        st.session_state[cache_key] = status
    if status.get("ok"):
        return
    err = status.get("error")
    detail = str(status.get("detail") or "")
    if err == "transient":
        st.error(
            "Cannot reach Supabase from this PC (timeout/firewall/VPN). "
            "Allow **python.exe** through the firewall, try another network, or set "
            "`HTTPS_PROXY` if you use a corporate proxy. "
            "Run: `python scripts/check_supabase_connection.py`"
        )
        if detail:
            st.caption(detail[:200])
    else:
        st.error(
            "Supabase rejected the API key. In Supabase → Project Settings → API, copy "
            "the **anon public** key into `.env` as `SUPABASE_KEY` "
            "(or legacy JWT into `SUPABASE_ANON_KEY`)."
        )
        if detail:
            st.caption(detail[:200])


@st.cache_resource(show_spinner=False)
def _get_supabase_client():
    return get_cached_supabase_client(
        SUPABASE_URL,
        SUPABASE_KEY,
        timeout_sec=_SUPABASE_HTTP_TIMEOUT_SEC,
    )


def _note_supabase_unreachable(exc: Exception | None = None) -> None:
    st.session_state[_DASH_SUPABASE_DOWN_KEY] = True
    if exc is not None:
        st.session_state[f"{_DASH_SUPABASE_DOWN_KEY}_detail"] = str(exc)[:240]


def _render_supabase_unreachable_banner() -> None:
    if not st.session_state.get(_DASH_SUPABASE_DOWN_KEY):
        return
    detail = str(st.session_state.get(f"{_DASH_SUPABASE_DOWN_KEY}_detail", "") or "")
    extra = f" ({detail})" if detail else ""
    st.warning(
        "Cannot reach **Supabase** — connection timed out or was blocked"
        f"{extra}. Check internet, VPN/firewall, and `SUPABASE_URL` / `SUPABASE_KEY` "
        "in `.env`. Lists stay empty until the database is reachable."
    )


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
        except Exception as exc:
            if is_transient_supabase_error(exc):
                _note_supabase_unreachable(exc)
                return "created_at"
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
    try:
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
    except Exception as exc:
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return None
        raise


def _apply_manual_field_response(
    ticket_number: str,
    *,
    field_response: str,
    responded_by_input: str | None = None,
) -> None:
    """Record a field reply from the dashboard when Telegram ingest missed it."""
    text = (field_response or "").strip()
    if not text:
        raise ValueError("Field response text is required.")

    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket {ticket_number} not found.")
    status = str(row.get("status") or "").strip()
    if status not in (STATUS_DAILY_TASK, "Open", STATUS_ON_HOLD):
        raise ValueError(
            f"Ticket is {status}; manual response is only allowed while "
            "Daily Task, On Hold, or Open."
        )

    assignee_raw = str(row.get("assigned_to") or "").strip()
    assignee = (
        assignee_raw
        if assignee_raw.startswith("@")
        else (f"@{assignee_raw.lstrip('@')}" if assignee_raw else "@unknown")
    )
    field_responded_by = _resolve_field_responded_by(
        assignee_raw, (responded_by_input or "").strip()
    )

    client = _get_supabase_client()
    now_iso = _cc_utc_now_iso()
    payload: dict[str, object] = {
        "field_response": text,
        "updated_at": now_iso,
        "field_responded_by": field_responded_by,
    }
    if status in (STATUS_DAILY_TASK, STATUS_ON_HOLD):
        payload["status"] = "Open"
        payload["responded_at"] = now_iso
    elif not row.get("responded_at"):
        payload["responded_at"] = now_iso

    _cc_execute_ticket_update(client, payload, ticket_number)

    op = _session_operator_id() or _session_dashboard_username() or "dashboard-admin"
    action = "entry" if status in (STATUS_DAILY_TASK, STATUS_ON_HOLD) else "update"
    log_note = text
    if field_responded_by:
        log_note = f"Responded by {field_responded_by}: {text}"
    log_note = f"Manual dashboard {action} by {op}: {log_note}"

    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=assignee,
        action_type="Response",
        note=log_note,
    )


def _parse_ts_value(value: object) -> datetime | None:
    """Parse one ISO timestamp from Supabase (UTC-aware)."""
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        py = ts.to_pydatetime()
        if py.tzinfo is None:
            return py.replace(tzinfo=timezone.utc)
        return py
    except (TypeError, ValueError):
        return None


def _response_metadata_for_current_assignment(row: dict) -> bool:
    """True when row fields look like a field reply for *this* assignment cycle."""
    assigned_at = _parse_ts_value(row.get("last_assigned_at"))
    responded_at = _parse_ts_value(row.get("responded_at"))

    if str(row.get("field_response") or "").strip():
        if assigned_at is None or responded_at is None:
            return True
        return responded_at >= assigned_at

    if row.get("photo_url"):
        if assigned_at is None or responded_at is None:
            return True
        return responded_at >= assigned_at

    if responded_at is not None:
        if assigned_at is None:
            return True
        return responded_at >= assigned_at

    return False


def _move_to_on_hold(ticket_number: str, *, operator_id: str) -> None:
    """Admin-only: set status **On Hold** and clear field reply for chase."""
    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    status = _normalize_ticket_status_value(row.get("status"))
    allowed = ("Open", STATUS_UNDER_INVESTIGATION, STATUS_DAILY_TASK, STATUS_ON_HOLD)
    if status not in allowed:
        raw = str(row.get("status") or "").strip()
        raise ValueError(
            f"Ticket **{ticket_number}** is **{raw or '—'}** — "
            "**On Hold** from **Needs Review**, **Investigation**, or **Daily Task** only."
        )
    if not str(row.get("assigned_to") or "").strip():
        raise ValueError(
            f"Ticket **{ticket_number}** has no assignee — use **Edit assignment** first."
        )
    client = _get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()
    from_label = {
        "Open": "Needs Review",
        STATUS_UNDER_INVESTIGATION: "Under Investigation",
        STATUS_DAILY_TASK: STATUS_DAILY_TASK,
        STATUS_ON_HOLD: STATUS_ON_HOLD,
    }.get(status, status)
    _cc_execute_ticket_update(
        client,
        {
            "status": STATUS_ON_HOLD,
            "field_response": None,
            "field_responded_by": None,
            "photo_url": None,
            "responded_at": None,
            "unattended_nudge_sent_at": None,
            "follow_up_at": None,
            "follow_up_note": None,
            "updated_at": now_iso,
        },
        ticket_number,
    )
    _cc_ensure_reassign_cleared_response_fields(client, ticket_number)
    try:
        client.table(ATTENDANCE_LOGS_TABLE).insert(
            {
                "ticket_number": str(ticket_number),
                "member_username": f"@{operator_id.lstrip('@')}",
                "action_type": "OnHold",
                "note": f"Moved to **On Hold** from {from_label} (field reply cleared).",
                "timestamp": now_iso,
            }
        ).execute()
    except Exception:
        pass


def _navigate_to_on_hold_queue() -> None:
    st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Tickets"
    st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_ON_HOLD


def _fetch_pending_with_response_mismatch() -> list[str]:
    """Daily Task tickets that look stuck after a field reply (bot UPDATE likely failed).

    Ignores **old** Response log rows and stale ``responded_at`` from before
    ``last_assigned_at`` — e.g. after **Reassign** for next-day work.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    client = _get_supabase_client()
    try:
        pending = (
            client.table(TICKETS_TABLE)
            .select(
                "ticket_number, field_response, photo_url, responded_at, last_assigned_at"
            )
            .eq("status", STATUS_DAILY_TASK)
            .limit(200)
            .execute()
        ).data or []
        if not pending:
            return []
        mismatches: list[str] = []
        needs_log_check: list[dict] = []
        for row in pending:
            tn = str(row.get("ticket_number") or "").strip()
            if not tn:
                continue
            if _response_metadata_for_current_assignment(row):
                mismatches.append(tn)
                continue
            needs_log_check.append(row)

        if not needs_log_check:
            return sorted(set(mismatches))

        ids = [str(r["ticket_number"]) for r in needs_log_check if r.get("ticket_number")]
        logs = (
            client.table(ATTENDANCE_LOGS_TABLE)
            .select("ticket_number, timestamp")
            .eq("action_type", "Response")
            .in_("ticket_number", ids)
            .execute()
        ).data or []
        resp_by_ticket: dict[str, list[datetime]] = {}
        for entry in logs:
            tn = str(entry.get("ticket_number") or "").strip()
            if not tn:
                continue
            ts = _parse_ts_value(entry.get("timestamp"))
            if ts is not None:
                resp_by_ticket.setdefault(tn, []).append(ts)

        for row in needs_log_check:
            tn = str(row.get("ticket_number") or "").strip()
            if not tn or tn in mismatches:
                continue
            assigned_at = _parse_ts_value(row.get("last_assigned_at"))
            resp_times = resp_by_ticket.get(tn) or []
            if assigned_at is None:
                if resp_times:
                    mismatches.append(tn)
                continue
            for resp_at in resp_times:
                if resp_at >= assigned_at:
                    mismatches.append(tn)
                    break

        return sorted(set(mismatches))
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
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return pd.DataFrame()
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
    payload: dict[str, object] = {"status": new_status, "updated_at": now_iso}
    if new_status == STATUS_DAILY_TASK:
        payload["unattended_nudge_sent_at"] = None
    if new_status != STATUS_UNDER_INVESTIGATION:
        payload["follow_up_at"] = None
        payload["follow_up_note"] = None
    _cc_execute_ticket_update(client, payload, str(ticket_number))

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


def _delete_ticket(
    ticket_number: str,
    *,
    actor: str = "@dashboard-admin",
    delete_telegram: bool = False,
) -> str | None:
    """Delete the ticket row but keep its attendance history.

    The ``ticket_attendance_logs.ticket_number`` foreign key is **not**
    cascaded, so the history rows remain queryable from the Log tab even
    after the active ticket is gone. We also append a ``Deleted`` log
    entry so the audit trail explicitly records the removal.

    Returns an optional warning when ``delete_telegram`` was requested but failed.
    """
    tg_warn: str | None = None
    if delete_telegram:
        row = _fetch_ticket_row(ticket_number)
        if row:
            try:
                tg_warn = asyncio.run(
                    _cc_delete_assignment_from_telegram(
                        row=row,
                        ticket_number=ticket_number,
                    )
                )
            except Exception as exc:
                tg_warn = f"Telegram delete failed: {exc}"

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
    return tg_warn


async def _cc_delete_assignment_from_telegram(
    *,
    row: dict,
    ticket_number: str,
) -> str | None:
    """Delete linked assignment message (best effort). Returns warning text or None."""
    token, chat_id = _cc_resolve_telegram_credentials()
    if not token or chat_id is None:
        return "Telegram not configured — assignment message left in the group."

    tg_chat = row.get("assignment_telegram_chat_id")
    tg_msg = row.get("assignment_telegram_message_id")
    api_id = _read_setting("TG_API_ID") or _read_setting("TELEGRAM_API_ID") or None
    api_hash = _read_setting("TG_API_HASH") or _read_setting("TELEGRAM_API_HASH") or None

    if tg_chat is None or tg_msg is None:
        found = await find_assignment_telegram_ref(
            ticket_number,
            group_id=chat_id,
            bot_token=token,
            api_id=api_id,
            api_hash=api_hash,
        )
        if found:
            tg_chat, tg_msg = found.chat_id, found.message_id

    if tg_chat is None or tg_msg is None:
        return "No linked assignment message found in the group."

    try:
        await delete_telegram_assignment_message(
            int(tg_chat),
            int(tg_msg),
            api_id=api_id,
            api_hash=api_hash,
            bot_token=token,
        )
    except Exception as exc:
        return str(exc)
    return None


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


def _ticket_search_session_key(key_prefix: str) -> str:
    return f"{key_prefix}_ticket_search"


def _ticket_pick_session_key(key_prefix: str) -> str:
    return f"{key_prefix}_ticket_pick"


def _ticket_selection_session_key(key_prefix: str) -> str:
    return f"{key_prefix}_selected_tickets"


def _get_selected_queue_tickets(key_prefix: str, options: list[str]) -> list[str]:
    allowed = set(options)
    raw = st.session_state.get(_ticket_selection_session_key(key_prefix), [])
    if not isinstance(raw, list):
        return []
    return [str(t) for t in raw if str(t) in allowed]


def _require_selected_tickets(
    *,
    key_prefix: str,
    options: list[str],
    exactly_one: bool = False,
) -> list[str] | None:
    selected = _get_selected_queue_tickets(key_prefix, options)
    if not selected:
        st.error(
            "Tick **Select** on at least one ticket in the table above, "
            "then click the action."
        )
        return None
    if exactly_one and len(selected) != 1:
        st.error(
            f"Select **exactly one** ticket for this action "
            f"({len(selected)} selected now)."
        )
        return None
    return selected


def _picked_ticket_from_selection(
    *,
    key_prefix: str,
    ticket_options: list[str],
) -> str | None:
    selected = _require_selected_tickets(
        key_prefix=key_prefix, options=ticket_options, exactly_one=True
    )
    return selected[0] if selected else None


def _ticket_search_session_key(key_prefix: str) -> str:
    return f"{key_prefix}_ticket_search"


def _filter_df_by_ticket_number(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Keep rows whose ticket_number contains the search text (digits OK)."""
    raw = (query or "").strip()
    if not raw or df.empty or "ticket_number" not in df.columns:
        return df
    lower = raw.lower()
    digits = re.sub(r"\D", "", raw)
    tn = df["ticket_number"].fillna("").astype(str)
    mask = tn.str.lower().str.contains(re.escape(lower), regex=True, na=False)
    if digits:
        mask = mask | tn.str.contains(re.escape(digits), regex=True, na=False)
    return df[mask]


def _filter_df_by_case_ref(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Keep sales rows whose case_ref (ticket #) matches the search text."""
    raw = (query or "").strip()
    if not raw or df.empty or "case_ref" not in df.columns:
        return df
    lower = raw.lower()
    digits = re.sub(r"\D", "", raw)
    cref = df["case_ref"].fillna("").astype(str)
    mask = cref.str.lower().str.contains(re.escape(lower), regex=True, na=False)
    if digits:
        mask = mask | cref.str.contains(re.escape(digits), regex=True, na=False)
    return df[mask]


def _filter_sales_cases_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Search ticket #, resort/company, sales owner, or category."""
    raw = (query or "").strip()
    if not raw or df.empty:
        return df
    lower = raw.lower()
    mask = pd.Series(False, index=df.index)
    for col in ("case_ref", "account_name", "sales_owner", "sales_category"):
        if col not in df.columns:
            continue
        series = df[col].fillna("").astype(str)
        mask = mask | series.str.lower().str.contains(re.escape(lower), regex=True, na=False)
    digits = re.sub(r"\D", "", raw)
    if digits and "case_ref" in df.columns:
        cref = df["case_ref"].fillna("").astype(str)
        mask = mask | cref.str.contains(re.escape(digits), regex=True, na=False)
    return df[mask]


def _sort_investigation_by_follow_up(df: pd.DataFrame) -> pd.DataFrame:
    """Follow-up cases first; within that, oldest ``follow_up_at`` on top."""
    if df.empty or "follow_up_at" not in df.columns:
        return df
    out = df.copy()
    out["_fu_ts"] = _parse_ts(out["follow_up_at"])
    out["_has_fu"] = out["_fu_ts"].notna()
    return (
        out.sort_values(
            ["_has_fu", "_fu_ts"],
            ascending=[False, True],
            na_position="last",
        )
        .drop(columns=["_has_fu", "_fu_ts"])
    )


def _follow_up_display_label(row: pd.Series) -> str:
    """Visible marker for investigation rows marked via **Mark follow-up**."""
    if "follow_up_at" not in row.index:
        return ""
    fu = row.get("follow_up_at")
    if fu is None or (isinstance(fu, float) and pd.isna(fu)):
        return ""
    if str(fu).strip() in ("", "None", "NaT"):
        return ""
    parsed = _parse_ts(pd.Series([fu]))
    when = (
        _to_local(parsed).iloc[0].strftime("%d %b %H:%M")
        if parsed.notna().iloc[0]
        else str(fu)[:16]
    )
    note = str(row.get("follow_up_note") or "").strip()
    if len(note) > 40:
        note = note[:37] + "…"
    return f"● {when}" + (f" — {note}" if note else "")


def _render_selectable_ticket_table(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    cols: tuple[str, ...],
    highlight_follow_up: bool = False,
) -> list[str]:
    """Table with a **Select** checkbox per row; returns chosen ticket numbers."""
    options = _ticket_options_for_admin(df)
    if not options:
        st.caption("No tickets in this queue.")
        return []

    search_q = st.text_input(
        "Search Ticket #",
        placeholder="Enter ticket number…",
        key=_ticket_search_session_key(key_prefix),
    )
    work = _sort_investigation_by_follow_up(df) if highlight_follow_up else df
    filtered = _filter_df_by_ticket_number(work, search_q)
    if highlight_follow_up and "follow_up_at" in filtered.columns:
        fu_count = int(_parse_ts(filtered["follow_up_at"]).notna().sum())
        if fu_count:
            st.caption(
                f"**{fu_count}** follow-up case(s) pinned to the top (●). "
                "Oldest follow-up first — chase these before newer ones."
            )
    if (search_q or "").strip() and len(filtered) < len(work):
        st.caption(f"Showing **{len(filtered)}** of **{len(work)}** tickets.")
    if filtered.empty and (search_q or "").strip():
        st.info("No tickets match that ticket number.")
        return []

    options = _ticket_options_for_admin(filtered)
    view = _ticket_queue_view(filtered, cols=cols)
    if highlight_follow_up and not view.empty and "follow_up_at" in filtered.columns:
        view.insert(
            0,
            "Follow-up",
            filtered.apply(_follow_up_display_label, axis=1).tolist(),
        )
    sel_key = _ticket_selection_session_key(key_prefix)
    if sel_key not in st.session_state:
        st.session_state[sel_key] = []

    if "ticket_number" not in view.columns:
        st.dataframe(view, use_container_width=True, hide_index=True)
        return []

    prev = set(_get_selected_queue_tickets(key_prefix, options))
    table = view.copy()
    table.insert(0, "Select", table["ticket_number"].astype(str).isin(prev))

    disabled_cols = [c for c in table.columns if c != "Select"]
    col_cfg = {
        "Select": st.column_config.CheckboxColumn(
            "Select",
            help="Tick, then use the action buttons above",
            default=False,
        ),
        **_dataframe_column_config(view),
    }
    if "Follow-up" in view.columns:
        col_cfg["Follow-up"] = st.column_config.TextColumn(
            "Follow-up",
            help="● = tracked individual follow-up (Needs Review → Follow-up). Blank = general Under Investigation.",
            width="medium",
        )
    edited = st.data_editor(
        table,
        hide_index=True,
        use_container_width=True,
        key=f"{key_prefix}_ticket_select_editor",
        column_config=col_cfg,
        disabled=disabled_cols,
    )

    selected = [
        str(t)
        for t in edited.loc[edited["Select"] == True, "ticket_number"].astype(str).tolist()
        if str(t) in options
    ]
    st.session_state[sel_key] = selected
    if selected:
        shown = ", ".join(selected[:6])
        extra = f" (+{len(selected) - 6} more)" if len(selected) > 6 else ""
        st.caption(f"**{len(selected)}** selected: {shown}{extra}")
    else:
        st.caption("Tick **Select** on ticket(s), then choose an action above.")
    return selected


_TICKET_PICK_PLACEHOLDER = "__choose_ticket__"


def _ticket_row_map(df: pd.DataFrame) -> dict[str, dict]:
    if df.empty or "ticket_number" not in df.columns:
        return {}
    out: dict[str, dict] = {}
    for _, row in df.iterrows():
        tn = str(row.get("ticket_number") or "").strip()
        if tn:
            out[tn] = row.to_dict()
    return out


def _ticket_display_label(ticket_number: str, row: dict | None) -> str:
    """One-line label: ticket · assignee · category · site note."""
    if not row:
        return ticket_number
    assignee = str(row.get("assigned_to") or "—").strip()
    cat = str(row.get("task_category") or "").strip()
    extra = str(
        row.get("additional_info") or row.get("field_response") or ""
    ).strip()
    extra = re.sub(r"\s+", " ", extra)
    if len(extra) > 48:
        extra = extra[:45] + "…"
    parts = [ticket_number, assignee]
    if cat:
        parts.append(cat)
    if extra:
        parts.append(extra)
    return " · ".join(parts)


def _move_to_investigation(
    ticket_number: str,
    *,
    follow_up: bool,
    note: str | None = None,
    operator_id: str,
) -> None:
    """Needs Review → Under Investigation (general park, or tracked individual follow-up)."""
    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    status = str(row.get("status") or "").strip()
    if status not in ("Open", STATUS_DAILY_TASK, STATUS_ON_HOLD):
        raise ValueError(
            f"Ticket **{ticket_number}** is **{status}** — "
            "move from **Needs Review**, **Daily Task**, or **On Hold** only."
        )
    if follow_up and status != "Open":
        raise ValueError(
            "Follow-up tracking is only when moving from **Needs Review**."
        )

    client = _get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()
    note_text = (note or "").strip() or None
    payload: dict[str, object] = {
        "status": STATUS_UNDER_INVESTIGATION,
        "updated_at": now_iso,
    }
    if follow_up:
        payload["follow_up_at"] = now_iso
        payload["follow_up_note"] = note_text
        log_action = "MarkedForFollowUp"
        log_note = note_text or "Individual follow-up from Needs Review."
    else:
        payload["follow_up_at"] = None
        payload["follow_up_note"] = None
        log_action = "MovedToInvestigation"
        if status == STATUS_DAILY_TASK:
            log_note = "Moved to Under Investigation from Daily Task (no field assign)."
        elif status == STATUS_ON_HOLD:
            log_note = "Moved to Under Investigation from On Hold."
        else:
            log_note = (
                "Moved to Under Investigation from Needs Review "
                "(no follow-up tracking)."
            )

    _cc_execute_ticket_update(client, payload, ticket_number)

    try:
        client.table(ATTENDANCE_LOGS_TABLE).insert(
            {
                "ticket_number": str(ticket_number),
                "member_username": f"@{operator_id.lstrip('@')}",
                "action_type": log_action,
                "note": log_note,
                "timestamp": now_iso,
            }
        ).execute()
    except Exception:
        pass


def _mark_ticket_for_follow_up(
    ticket_number: str,
    *,
    note: str | None,
    operator_id: str,
) -> None:
    """Individual follow-up: Under Investigation + ``follow_up_at`` / note (● in queue)."""
    _move_to_investigation(
        ticket_number,
        follow_up=True,
        note=note,
        operator_id=operator_id,
    )


def _filter_ticket_df_for_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    raw = (query or "").strip()
    if not raw or df.empty:
        return df
    lower = raw.lower()
    digits = re.sub(r"\D", "", raw)

    def _row_matches(row: pd.Series) -> bool:
        tn = str(row.get("ticket_number") or "")
        if lower in tn.lower() or (digits and digits in tn):
            return True
        blob = " ".join(
            [
                str(row.get("assigned_to") or ""),
                str(row.get("task_category") or ""),
                str(row.get("additional_info") or ""),
                str(row.get("field_response") or ""),
            ]
        ).lower()
        return lower in blob or bool(digits and digits in blob)

    return df[df.apply(_row_matches, axis=1)]


def _render_admin_ticket_picker(df: pd.DataFrame, *, key_prefix: str) -> list[str]:
    """Search + readable pick list (no ticket pre-selected)."""
    options = _ticket_options_for_admin(df)
    if not options:
        return options

    row_map = _ticket_row_map(df)
    search_key = _ticket_search_session_key(key_prefix)
    pick_key = _ticket_pick_session_key(key_prefix)

    st.text_input(
        "Search",
        placeholder="Ticket #, @engineer, category, site…",
        key=search_key,
    )
    query = str(st.session_state.get(search_key, ""))
    filtered_df = _filter_ticket_df_for_search(df, query) if query.strip() else df
    filtered_ids = _ticket_options_for_admin(filtered_df)
    if not filtered_ids:
        st.caption("No match — try fewer characters or check the table below.")
        return options

    pick_ids = [_TICKET_PICK_PLACEHOLDER, *filtered_ids]
    current = str(st.session_state.get(pick_key, _TICKET_PICK_PLACEHOLDER))
    if current not in pick_ids and current in options:
        pick_ids = [_TICKET_PICK_PLACEHOLDER, current, *filtered_ids]

    def _fmt(ticket_id: str) -> str:
        if ticket_id == _TICKET_PICK_PLACEHOLDER:
            return "— Choose ticket —"
        return _ticket_display_label(ticket_id, row_map.get(ticket_id))

    st.selectbox(
        "Ticket",
        options=pick_ids,
        format_func=_fmt,
        key=pick_key,
    )
    chosen = str(st.session_state.get(pick_key, _TICKET_PICK_PLACEHOLDER))
    if chosen and chosen != _TICKET_PICK_PLACEHOLDER and chosen in options:
        st.caption(f"Selected **{chosen}**")
    elif len(filtered_ids) == 1:
        st.caption(f"1 match — choose **{filtered_ids[0]}** in the list.")
    return options


def _resolve_picked_ticket(*, key_prefix: str, options: list[str]) -> str | None:
    """Ticket chosen in the picker, or resolved from search text alone."""
    pick_key = _ticket_pick_session_key(key_prefix)
    chosen = str(st.session_state.get(pick_key, _TICKET_PICK_PLACEHOLDER))
    if chosen and chosen != _TICKET_PICK_PLACEHOLDER and chosen in options:
        return chosen
    query = str(st.session_state.get(_ticket_search_session_key(key_prefix), ""))
    return _resolve_ticket_in_queue(query, options)


def _resolve_ticket_in_queue(query: str, options: list[str]) -> str | None:
    """Match ticket # against this queue (exact or trailing digits)."""
    raw = (query or "").strip()
    if not raw or not options:
        return None
    if raw in options:
        return raw
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    if digits in options:
        return digits
    # Prefer unique suffix match (e.g. last 9 digits).
    suffix_hits = [o for o in options if o.endswith(digits)]
    if len(suffix_hits) == 1:
        return suffix_hits[0]
    if len(digits) >= 9:
        full_hits = [o for o in options if digits in o]
        if len(full_hits) == 1:
            return full_hits[0]
    return None


def _filter_tickets_for_search(query: str, options: list[str], *, limit: int = 12) -> list[str]:
    """Suggestions while typing (subset of queue options)."""
    raw = (query or "").strip()
    if not raw:
        return []
    lower = raw.lower()
    digits = re.sub(r"\D", "", raw)
    hits: list[str] = []
    for o in options:
        if o in hits:
            continue
        if lower in o.lower() or (digits and digits in o):
            hits.append(o)
        if len(hits) >= limit:
            break
    return hits


def _require_queue_ticket(*, key_prefix: str, options: list[str]) -> str | None:
    """Validated ticket from search + pick list, or None after showing an error."""
    picked = _resolve_picked_ticket(key_prefix=key_prefix, options=options)
    if picked:
        return picked
    st.error(
        "Search if needed, then choose a ticket from the list "
        "(not “— Choose ticket —”). Nothing changes until you pick one."
    )
    return None


def _picked_ticket_from_search(
    *,
    key_prefix: str,
    ticket_options: list[str],
) -> str | None:
    """Ticket for sub-forms (edit / reassign / record response)."""
    picked = _resolve_picked_ticket(key_prefix=key_prefix, options=ticket_options)
    if picked:
        return picked
    st.info("Search and **choose a ticket** in the list above to continue.")
    return None


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


def _apply_admin_ticket_action(
    *,
    picked: str,
    choice: str,
    confirm_del: bool,
    status_actions: tuple[tuple[str, str, str], ...],
    do_rerun: bool = True,
    delete_telegram: bool = False,
) -> bool:
    """Apply one admin action. Returns True on success."""
    if choice == "Delete row":
        if not confirm_del:
            st.warning("Check **Yes, remove permanently** first.")
            return False
        try:
            tg_warn = _delete_ticket(picked, delete_telegram=delete_telegram)
        except Exception as exc:
            _delete_ticket_error_ui(picked, exc)
            return False
        if do_rerun:
            msg = f"{picked} deleted (history kept in Log)."
            if delete_telegram and not tg_warn:
                msg += " Telegram assignment message removed."
            elif tg_warn:
                st.warning(f"{picked}: {tg_warn}")
            st.success(msg)
            st.rerun()
        return True

    matched = next((a for a in status_actions if a[0] == choice), None)
    if not matched:
        st.error("Unknown action.")
        return False
    _, new_status, log_action = matched
    try:
        if log_action == "OnHold":
            if not _is_dashboard_admin():
                st.error("Only dashboard admins can move tickets to **On Hold**.")
                return False
            op = _session_operator_id()
            if not op:
                st.error("Sign in again — operator session is missing.")
                return False
            _move_to_on_hold(picked, operator_id=op)
            _navigate_to_on_hold_queue()
        elif (
            new_status == STATUS_UNDER_INVESTIGATION
            and log_action == "MovedToInvestigation"
        ):
            op = _session_operator_id()
            if not op:
                st.error("Sign in again — operator session is missing.")
                return False
            _move_to_investigation(
                picked,
                follow_up=False,
                operator_id=op,
            )
        else:
            _set_ticket_status(
                picked,
                new_status=new_status,
                log_action=log_action,
            )
    except ValueError as exc:
        st.error(str(exc))
        return False
    except Exception as exc:
        st.error(f"Could not update {picked}: {exc}")
        return False
    if do_rerun:
        msg = (
            f"{picked} → **On Hold**."
            if log_action == "OnHold"
            else f"{picked} → **{new_status}**."
        )
        st.success(msg)
        st.rerun()
    return True


def _render_ticket_delete_popover(
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
    compact: bool = False,
) -> None:
    """Secondary remove flow — popover, confirm checkbox, disabled until checked."""
    label = "Remove" if compact else "Remove…"
    with st.popover(label, use_container_width=True):
        picked_list = _get_selected_queue_tickets(key_prefix, options)
        if not picked_list:
            st.caption("Select ticket(s) in the table, then open Remove again.")
            return
        st.markdown("**" + "**, **".join(picked_list[:12]) + "**")
        if len(picked_list) > 12:
            st.caption(f"+ {len(picked_list) - 12} more")
        st.caption("Removes from queue · **Log** keeps history.")
        confirm_del = st.checkbox(
            "Yes, Remove Permanently",
            value=False,
            key=f"{key_prefix}_del_confirm",
        )
        token_ok, chat_ok = _cc_resolve_telegram_credentials()
        can_del_tg = bool(token_ok and chat_ok is not None)
        delete_tg = st.checkbox(
            "Also Delete Telegram Assignment Message",
            value=False,
            disabled=not can_del_tg,
            key=f"{key_prefix}_del_tg",
            help="Removes the bot's linked assignment post from the field group when possible.",
        )
        if not can_del_tg:
            st.caption("Set **TELEGRAM_TOKEN** and **TELEGRAM_GROUP_CHAT_ID** to enable.")
        if st.button(
            "Delete",
            key=f"{key_prefix}_del_btn",
            type="secondary",
            use_container_width=True,
            disabled=not confirm_del,
        ):
            ok = 0
            tg_warnings: list[str] = []
            for picked in picked_list:
                try:
                    tg_warn = _delete_ticket(
                        picked,
                        delete_telegram=bool(delete_tg),
                    )
                    if tg_warn:
                        tg_warnings.append(f"**{picked}**: {tg_warn}")
                    ok += 1
                except Exception as exc:
                    _delete_ticket_error_ui(picked, exc)
            if ok:
                msg = f"Removed **{ok}** ticket(s) (history kept in Log)."
                if delete_tg and not tg_warnings:
                    msg += " Telegram assignment message(s) removed when linked."
                st.success(msg)
                for w in tg_warnings:
                    st.warning(w)
                st.session_state[_ticket_selection_session_key(key_prefix)] = []
                st.rerun()


def _sync_manual_field_response_widgets(
    *,
    keys: dict[str, str],
    picked: str,
    row: dict,
) -> None:
    if st.session_state.get(keys["synced_ticket"]) == picked:
        return
    st.session_state[keys["text"]] = str(row.get("field_response") or "")
    responded = str(row.get("field_responded_by") or "").strip()
    st.session_state[keys["responded_by"]] = responded
    st.session_state[keys["synced_ticket"]] = picked


def _render_manual_field_response_editor(
    *,
    key_prefix: str,
    edit_key_prefix: str,
    ticket_options: list[str],
    allowed_statuses: tuple[str, ...] = (STATUS_DAILY_TASK, "Open"),
    save_label: str = "Save",
) -> None:
    """Admin form: record or correct a field reply (Daily Task → Open, or update Open)."""
    keys = _manual_field_response_session_keys(edit_key_prefix)
    if not st.session_state.get(keys["show"]):
        return

    picked = _picked_ticket_from_selection(
        key_prefix=key_prefix, ticket_options=ticket_options
    )
    if not picked:
        return

    row = _fetch_ticket_row(picked)
    if not row:
        st.warning("Ticket not found.")
        return
    status = str(row.get("status") or "").strip()
    if status not in allowed_statuses:
        labels = " or **".join(allowed_statuses)
        st.info(f"Pick a **{labels}** ticket to record a field response.")
        return

    _sync_manual_field_response_widgets(keys=keys, picked=picked, row=row)

    assignee = str(row.get("assigned_to") or "—")
    if status == STATUS_DAILY_TASK:
        st.caption(
            f"Record a field reply for **{picked}** (assignee {assignee}). "
            "Use when the bot did not capture the Telegram reply. Saves to **Open**."
        )
    else:
        st.caption(
            f"Update field response for **{picked}** (assignee {assignee}). "
            "Corrects text or **Responded by** on an Open ticket."
        )

    with st.form(f"{edit_key_prefix}_mfr_form", border=True):
        st.text_area(
            "Field Response",
            placeholder="Paste or type what the engineer replied in Telegram",
            height=120,
            key=keys["text"],
        )
        st.text_input(
            "Responded By (Optional)",
            placeholder="e.g. @DHRTemsX6 if they used a test phone",
            help="Leave empty when the assignee replied from their own account.",
            key=keys["responded_by"],
        )
        c_save, c_cancel = st.columns(2)
        with c_save:
            submit = st.form_submit_button(
                save_label, type="primary", use_container_width=True
            )
        with c_cancel:
            cancel = st.form_submit_button("Cancel", use_container_width=True)

    if cancel:
        st.session_state[keys["show"]] = False
        st.rerun()
    if submit:
        try:
            _apply_manual_field_response(
                picked,
                field_response=str(st.session_state.get(keys["text"]) or ""),
                responded_by_input=str(st.session_state.get(keys["responded_by"]) or "")
                or None,
            )
        except Exception as exc:
            st.error(str(exc))
            return
        st.session_state[keys["show"]] = False
        if status == STATUS_DAILY_TASK:
            st.success(f"{picked} → **Open** (field response saved).")
        else:
            st.success(f"{picked}: field response updated.")
        _get_supabase_client.clear()
        st.rerun()


def _render_mark_follow_up_popover(*, key_prefix: str, options: list[str]) -> None:
    """Tracked individual follow-up (●) — one Open ticket, optional note."""
    with st.popover("Follow-up", use_container_width=True):
        picked = _get_selected_queue_tickets(key_prefix, options)
        if not picked:
            st.caption("Select **one** ticket in the table, then open this again.")
            return
        if len(picked) != 1:
            st.caption("Select **exactly one** ticket — follow-up is per case.")
            return
        ticket = picked[0]
        st.markdown(f"**{ticket}**")
        st.caption(
            "Tracked case: shows **●** in Investigation and stays pinned on top. "
            "Use **Under Investigation** in the action menu for general review without tracking."
        )
        note = st.text_area(
            "Follow-up Note (Optional)",
            placeholder="e.g. Revisit Tuesday — waiting for site access",
            key=f"{key_prefix}_follow_up_note",
            height=72,
        )
        if st.button(
            "Confirm follow-up",
            key=f"{key_prefix}_follow_up_confirm",
            type="primary",
            use_container_width=True,
        ):
            op = _session_operator_id()
            if not op:
                st.error("Sign in again — operator session is missing.")
                return
            try:
                _mark_ticket_for_follow_up(ticket, note=note, operator_id=op)
            except ValueError as exc:
                st.error(str(exc))
                return
            except Exception as exc:
                st.error(f"Could not mark follow-up: {exc}")
                return
            st.session_state[_ticket_selection_session_key(key_prefix)] = []
            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Tickets"
            st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_UNDER_INVESTIGATION
            st.session_state[_CC_FLASH_KEY] = (
                f"**{ticket}** → **Under Investigation** (follow-up tracked ●)."
            )
            st.session_state[_CC_FLASH_LEVEL_KEY] = "success"
            st.rerun()


def _render_admin_ticket_toolbar(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    caption: str | None = None,
    status_actions: tuple[tuple[str, str, str], ...] = (),
    allow_delete: bool = True,
    allow_edit_assignment: bool = False,
    allow_manual_field_response: bool = False,
    allow_reassign: bool = False,
    allow_mark_follow_up: bool = False,
) -> None:
    """One compact row: Select all, Clear, status + admin actions (+ Remove)."""
    options = _ticket_options_for_admin(df)
    if not options:
        return

    if caption:
        st.caption(caption)

    if not _is_dashboard_admin():
        status_actions = tuple(a for a in status_actions if a[2] != "OnHold")
    status_labels = [a[0] for a in status_actions]
    edit_keys = _assignment_edit_session_keys(key_prefix)
    mfr_keys = _manual_field_response_session_keys(key_prefix)
    reassign_keys = _reassign_session_keys(key_prefix)
    sel_key = _ticket_selection_session_key(key_prefix)

    slot_count = 2  # Select all, Clear
    if status_labels:
        slot_count += 2 if len(status_labels) > 1 else 1
    if allow_manual_field_response:
        slot_count += 1
    if allow_reassign:
        slot_count += 1
    if allow_edit_assignment:
        slot_count += 1
    if allow_mark_follow_up:
        slot_count += 1
    if allow_delete:
        slot_count += 1

    with st.container(key=f"{key_prefix}_tq_toolbar"):
        cols = st.columns(slot_count, gap="small", vertical_alignment="bottom")
        idx = 0

        with cols[idx]:
            if st.button(
                "Select all",
                key=f"{key_prefix}_sel_all",
                use_container_width=True,
            ):
                st.session_state[sel_key] = list(options)
                st.rerun()
        idx += 1

        with cols[idx]:
            if st.button(
                "Clear",
                key=f"{key_prefix}_sel_clear",
                use_container_width=True,
            ):
                st.session_state[sel_key] = []
                st.rerun()
        idx += 1

        if status_labels:
            if len(status_labels) == 1:
                label = status_labels[0]
                with cols[idx]:
                    if st.button(
                        label,
                        key=f"{key_prefix}_apply",
                        type="primary",
                        use_container_width=True,
                    ):
                        picked_list = _require_selected_tickets(
                            key_prefix=key_prefix, options=options
                        )
                        if picked_list:
                            ok = 0
                            for picked in picked_list:
                                if _apply_admin_ticket_action(
                                    picked=picked,
                                    choice=label,
                                    confirm_del=False,
                                    status_actions=status_actions,
                                    do_rerun=False,
                                ):
                                    ok += 1
                            if ok:
                                st.success(
                                    f"**{ok}** ticket(s) updated → **{label}**."
                                )
                                st.session_state[sel_key] = []
                                st.rerun()
                idx += 1
            else:
                with cols[idx]:
                    st.selectbox(
                        "Action",
                        options=status_labels,
                        key=f"{key_prefix}_action_sel",
                        label_visibility="collapsed",
                    )
                idx += 1
                with cols[idx]:
                    if st.button(
                        "Apply",
                        key=f"{key_prefix}_apply",
                        type="primary",
                        use_container_width=True,
                    ):
                        choice = str(
                            st.session_state.get(f"{key_prefix}_action_sel", "")
                        )
                        picked_list = _require_selected_tickets(
                            key_prefix=key_prefix, options=options
                        )
                        if picked_list and choice:
                            ok = 0
                            for picked in picked_list:
                                if _apply_admin_ticket_action(
                                    picked=picked,
                                    choice=choice,
                                    confirm_del=False,
                                    status_actions=status_actions,
                                    do_rerun=False,
                                ):
                                    ok += 1
                            if ok:
                                st.success(
                                    f"**{ok}** ticket(s) updated → **{choice}**."
                                )
                                st.session_state[sel_key] = []
                                st.rerun()
                idx += 1

        if allow_manual_field_response:
            with cols[idx]:
                if st.button(
                    "Record response",
                    key=f"{key_prefix}_mfr_btn",
                    use_container_width=True,
                ):
                    st.session_state[mfr_keys["show"]] = True
                    st.rerun()
            idx += 1

        if allow_reassign:
            with cols[idx]:
                if st.button(
                    "Reassign",
                    key=f"{key_prefix}_reassign_btn",
                    use_container_width=True,
                ):
                    _clear_reassign_panels_except(key_prefix)
                    st.session_state[reassign_keys["show"]] = True
                    st.rerun()
            idx += 1

        if allow_edit_assignment:
            with cols[idx]:
                if st.button(
                    "Edit assignment",
                    key=f"{key_prefix}_edit_btn",
                    use_container_width=True,
                ):
                    st.session_state[edit_keys["show"]] = True
                    st.rerun()
            idx += 1

        if allow_mark_follow_up:
            with cols[idx]:
                _render_mark_follow_up_popover(
                    key_prefix=key_prefix,
                    options=options,
                )
            idx += 1

        if allow_delete:
            with cols[idx]:
                _render_ticket_delete_popover(
                    key_prefix=key_prefix,
                    options=options,
                    status_actions=status_actions,
                    compact=True,
                )


def _fetch_attendance(
    *,
    ticket_number: str | None = None,
    member_query: str | None = None,
    since_utc: pd.Timestamp | None = None,
    until_utc: pd.Timestamp | None = None,
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
    if since_utc is not None:
        q = q.gte(
            "timestamp",
            since_utc.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    if until_utc is not None:
        q = q.lte(
            "timestamp",
            until_utc.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
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


def _perf_norm_member(raw: object) -> str:
    """Normalize ``assigned_to`` / log ``member_username`` for chart labels."""

    s = str(raw or "").strip()
    if not s or s.lower() in ("unknown", "none", "null"):
        return "(unknown)"
    low = s.lstrip("@").lower()
    return f"@{low}" if low else "(unknown)"


def _local_date_start(d: date) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(d, time.min), tz=LOCAL_TZ).tz_convert("UTC")


def _local_date_end(d: date) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(d, time.max), tz=LOCAL_TZ).tz_convert("UTC")


def _sync_search_date_widgets(start: pd.Timestamp, end: pd.Timestamp) -> None:
    st.session_state[_DASH_SEARCH_FROM_DATE_KEY] = start.tz_convert(LOCAL_TZ).date()
    st.session_state[_DASH_SEARCH_TO_DATE_KEY] = end.tz_convert(LOCAL_TZ).date()


def _preset_range_utc(preset: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    now = pd.Timestamp.now(tz="UTC")
    if preset == "Today":
        today = now.tz_convert(LOCAL_TZ).date()
        return _local_date_start(today), now
    if preset == "Last 30 days":
        return now - pd.Timedelta(days=30), now
    return now - pd.Timedelta(days=7), now


def _store_dash_range(start: pd.Timestamp, end: pd.Timestamp) -> None:
    st.session_state[_DASH_RANGE_FROM_KEY] = start.isoformat()
    st.session_state[_DASH_RANGE_TO_KEY] = end.isoformat()


def _get_dash_range() -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.to_datetime(st.session_state[_DASH_RANGE_FROM_KEY], utc=True)
    end = pd.to_datetime(st.session_state[_DASH_RANGE_TO_KEY], utc=True)
    return start, end


def _format_dash_range_caption() -> str:
    if _DASH_RANGE_FROM_KEY not in st.session_state:
        return ""
    start, end = _get_dash_range()
    lo = start.tz_convert(LOCAL_TZ).strftime("%d %b")
    hi = end.tz_convert(LOCAL_TZ).strftime("%d %b %Y")
    return f"{lo} – {hi} · {LOCAL_TZ_LABEL}"


def _ensure_dash_range_defaults() -> None:
    if _DASH_RANGE_FROM_KEY not in st.session_state:
        start, end = _preset_range_utc("Last 7 days")
        _store_dash_range(start, end)
        _sync_search_date_widgets(start, end)
    if _DASH_TIME_PRESET_KEY not in st.session_state:
        st.session_state[_DASH_TIME_PRESET_KEY] = "Last 7 days"


def _sidebar_date_range() -> tuple[int, pd.Timestamp, pd.Timestamp]:
    """Sidebar: presets or a simple From–To date pair (updates immediately)."""
    _ensure_dash_range_defaults()
    if _DASH_RANGE_FROM_KEY not in st.session_state:
        start, end = _preset_range_utc("Last 7 days")
        _store_dash_range(start, end)
        _sync_search_date_widgets(start, end)
    if _DASH_TIME_PRESET_KEY not in st.session_state:
        st.session_state[_DASH_TIME_PRESET_KEY] = "Last 7 days"
    cur = st.session_state.get(_DASH_TIME_PRESET_KEY)
    if cur in _LEGACY_TIME_PRESET_MAP:
        st.session_state[_DASH_TIME_PRESET_KEY] = _LEGACY_TIME_PRESET_MAP[cur]
    elif cur not in _DASH_TIME_PRESET_OPTIONS:
        st.session_state[_DASH_TIME_PRESET_KEY] = "Last 7 days"
    if _DASH_SEARCH_FROM_DATE_KEY not in st.session_state:
        _sync_search_date_widgets(*_get_dash_range())

    preset = st.selectbox(
        "Time Range",
        options=list(_DASH_TIME_PRESET_OPTIONS),
        key=_DASH_TIME_PRESET_KEY,
    )

    prev_preset = st.session_state.get(_DASH_PREV_PRESET_KEY)
    if preset != prev_preset:
        st.session_state[_DASH_PREV_PRESET_KEY] = preset
        if preset != "Pick dates":
            start, end = _preset_range_utc(preset)
            _store_dash_range(start, end)
            _sync_search_date_widgets(start, end)

    if preset == "Pick dates":
        c1, c2 = st.columns(2)
        with c1:
            from_d = st.date_input(
                "From",
                format="YYYY-MM-DD",
                key=_DASH_SEARCH_FROM_DATE_KEY,
            )
        with c2:
            to_d = st.date_input(
                "To",
                format="YYYY-MM-DD",
                key=_DASH_SEARCH_TO_DATE_KEY,
            )
        if from_d > to_d:
            to_d = from_d
        start = _local_date_start(from_d)
        end = _local_date_end(to_d)
        _store_dash_range(start, end)
    else:
        start, end = _preset_range_utc(preset)
        _store_dash_range(start, end)

    start, end = _get_dash_range()
    lookback_days = max(
        MIN_LOOKBACK_DAYS,
        min(MAX_LOOKBACK_DAYS, int((end - start).total_seconds() // 86400) + 1),
    )

    cap = _format_dash_range_caption()
    if cap:
        st.caption(cap)

    return lookback_days, start, end


def _perf_bucket_settings(
    start_utc: pd.Timestamp, end_utc: pd.Timestamp
) -> tuple[str, str, str]:
    """Return (bucket strftime, x-axis title, axis format) from range span."""
    span = end_utc - start_utc
    if span <= pd.Timedelta(hours=48):
        return "%Y-%m-%d %H:00", "Hour (local)", "%b %d %H:%M"
    return "%Y-%m-%d", "Day (local)", "%Y-%m-%d"


def _perf_status_timestamp(
    df: pd.DataFrame, *, prefer_follow_up: bool = False
) -> pd.Series:
    """Pick one UTC timestamp per row for performance time-window filtering."""
    u_col = (
        _parse_ts(df["updated_at"])
        if "updated_at" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )
    if prefer_follow_up and "follow_up_at" in df.columns:
        fu = _parse_ts(df["follow_up_at"])
        return fu.where(fu.notna(), u_col)
    r_col = (
        _parse_ts(df["responded_at"])
        if "responded_at" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )
    return u_col.where(u_col.notna(), r_col)


def _perf_filter_status_in_range(
    df_all: pd.DataFrame,
    status: str,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
    *,
    prefer_follow_up: bool = False,
) -> pd.DataFrame:
    """Rows in ``status`` whose activity timestamp falls in the sidebar range."""
    if df_all.empty or "status" not in df_all.columns:
        return pd.DataFrame()
    target = status.strip().casefold()
    slice_df = df_all[
        df_all["status"].astype(str).str.strip().str.casefold() == target
    ].copy()
    if slice_df.empty:
        return slice_df
    ts = _perf_status_timestamp(slice_df, prefer_follow_up=prefer_follow_up)
    slice_df = slice_df[ts.notna()].copy()
    slice_df["_ts"] = ts[ts.notna()]
    return slice_df[
        (slice_df["_ts"] >= range_start) & (slice_df["_ts"] <= range_end)
    ]


def _perf_reference_ts(df: pd.DataFrame) -> pd.Series:
    """Latest activity timestamp per ticket (same basis as Tickets time range)."""
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
        return pd.Series(pd.NaT, index=df.index)
    stacked = pd.concat([_parse_ts(df[c]) for c in cols], axis=1)
    return stacked.max(axis=1, skipna=True)


def _perf_prepare_slices(
    df_all: pd.DataFrame,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> dict[str, pd.DataFrame]:
    """Field tickets in view — same rules as the Tickets tab queue counts."""
    empty = pd.DataFrame()
    out: dict[str, pd.DataFrame] = {
        "in_view": empty,
        "pending": empty,
        "open": empty,
        "completed": empty,
        "investigation": empty,
        "on_hold": empty,
        "unattended": empty,
    }
    if df_all.empty or "status" not in df_all.columns:
        return out

    in_view, _in_range_n = _dashboard_tickets_in_view(
        df_all, range_start=range_start, range_end=range_end
    )
    out["in_view"] = in_view
    if in_view.empty:
        return out

    masks = _ticket_queue_count_masks(in_view)
    ref_ts = _perf_reference_ts(in_view)
    for key in (
        "pending",
        "open",
        "on_hold",
        "investigation",
        "unattended",
        "completed",
    ):
        part = in_view.loc[masks[key]].copy()
        if not part.empty:
            part["_ts"] = ref_ts.loc[masks[key]]
        out[key] = part
    return out


def _perf_filter_sales_in_range(
    df: pd.DataFrame,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> pd.DataFrame:
    """Sales Cases whose ``updated_at`` falls in the sidebar time range."""
    if df.empty:
        return df
    ts = (
        _parse_ts(df["updated_at"])
        if "updated_at" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )
    mask = ts.notna() & (ts >= range_start) & (ts <= range_end)
    part = df.loc[mask].copy()
    if not part.empty:
        part["_ts"] = ts.loc[mask]
    return part


def _perf_enrich_sales_cases(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``staff`` (sales_owner), ``category``, and local time from ``_ts``."""
    view = df.copy()
    if "_ts" not in view.columns and "updated_at" in view.columns:
        view["_ts"] = _parse_ts(view["updated_at"])
    view["_local"] = view["_ts"].dt.tz_convert(LOCAL_TZ)
    if "sales_owner" in view.columns:
        view["staff"] = view["sales_owner"].map(_perf_norm_member)
    elif "admin_owner" in view.columns:
        view["staff"] = view["admin_owner"].map(_perf_norm_member)
    else:
        view["staff"] = "(unknown)"
    if "sales_category" in view.columns:
        cat = view["sales_category"].fillna("").astype(str).str.strip()
        view["category"] = cat.mask(cat.eq(""), "(uncategorized)")
    else:
        view["category"] = "(uncategorized)"
    if "status" in view.columns:
        view["status_eff"] = view["status"].map(_sc_effective_status)
    return view


def _perf_build_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "status_eff" not in df.columns:
        return pd.DataFrame()
    view = _perf_enrich_sales_cases(df)
    rows: list[dict[str, object]] = []
    for person, grp in view.groupby("staff"):
        st = grp["status_eff"]
        rows.append(
            {
                "Person": person,
                "Sales Ticket": int(st.eq(SC_STATUS_SALES_TICKET).sum()),
                "Investigation": int(
                    st.isin(_SC_INVESTIGATION_QUEUE_STATUSES).sum()
                ),
                "Design": int(st.eq(SC_STATUS_DESIGN).sum()),
                "Resolved": int(st.eq(SC_STATUS_RESOLVED).sum()),
                "Total": len(grp),
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["Total", "Person"], ascending=[False, True])


def _perf_filter_by_person(df: pd.DataFrame, person: str) -> pd.DataFrame:
    if df.empty or person in ("", "All"):
        return df
    view = _perf_enrich_tickets(df) if "staff" not in df.columns else df.copy()
    if "staff" not in view.columns:
        return df
    return view[view["staff"] == person]


def _perf_sales_account_names(df: pd.DataFrame) -> list[str]:
    if df.empty or "account_name" not in df.columns:
        return []
    names = df["account_name"].fillna("").astype(str).str.strip()
    return sorted({n for n in names.tolist() if n}, key=str.lower)


def _perf_filter_sales_by_account(df: pd.DataFrame, account: str) -> pd.DataFrame:
    if df.empty or account in ("", "All"):
        return df
    if "account_name" not in df.columns:
        return df
    target = str(account).strip()
    return df[
        df["account_name"].fillna("").astype(str).str.strip() == target
    ].copy()


def _perf_enrich_tickets(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``staff``, ``category``, and local time from ``_ts``."""
    view = df.copy()
    view["_local"] = view["_ts"].dt.tz_convert(LOCAL_TZ)
    if "assigned_to" in view.columns:
        view["staff"] = view["assigned_to"].map(_perf_norm_member)
    else:
        view["staff"] = "(unknown)"
    if "task_category" in view.columns:
        cat_series = view["task_category"].fillna("").astype(str).str.strip()
        view["category"] = cat_series.mask(cat_series.eq(""), "(uncategorized)")
    else:
        view["category"] = "(uncategorized)"
    return view


def _perf_staff_counts(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=int)
    if "staff" not in df.columns:
        df = _perf_enrich_tickets(df)
    return df.groupby("staff").size()


def _perf_combine_work(
    completed: pd.DataFrame,
    investigation: pd.DataFrame,
) -> pd.DataFrame:
    """Resolved + Under Investigation = total active work in the window."""
    parts: list[pd.DataFrame] = []
    if not completed.empty:
        c = completed.copy()
        c["_outcome"] = STATUS_RESOLVED
        parts.append(c)
    if not investigation.empty:
        i = investigation.copy()
        i["_outcome"] = "Investigation"
        parts.append(i)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _perf_build_summary(
    pending: pd.DataFrame,
    open_df: pd.DataFrame,
    completed: pd.DataFrame,
    investigation: pd.DataFrame,
    on_hold: pd.DataFrame,
    unattended: pd.DataFrame,
) -> pd.DataFrame:
    p_counts = _perf_staff_counts(pending)
    o_counts = _perf_staff_counts(open_df)
    c_counts = _perf_staff_counts(completed)
    i_counts = _perf_staff_counts(investigation)
    h_counts = _perf_staff_counts(on_hold)
    u_counts = _perf_staff_counts(unattended)
    people = sorted(
        set(p_counts.index)
        | set(o_counts.index)
        | set(c_counts.index)
        | set(i_counts.index)
        | set(h_counts.index)
        | set(u_counts.index),
        key=str.lower,
    )
    if not people:
        return pd.DataFrame()
    rows = [
        {
            "Person": p,
            "Total": (
                int(p_counts.get(p, 0))
                + int(o_counts.get(p, 0))
                + int(c_counts.get(p, 0))
                + int(i_counts.get(p, 0))
                + int(h_counts.get(p, 0))
                + int(u_counts.get(p, 0))
            ),
            STATUS_DAILY_TASK: int(p_counts.get(p, 0)),
            "Needs Review": int(o_counts.get(p, 0)),
            STATUS_RESOLVED: int(c_counts.get(p, 0)),
            "Investigation": int(i_counts.get(p, 0)),
            "On Hold": int(h_counts.get(p, 0)),
            "Unattended": int(u_counts.get(p, 0)),
            "Handled": int(c_counts.get(p, 0)) + int(i_counts.get(p, 0)),
        }
        for p in people
    ]
    return pd.DataFrame(rows).sort_values(
        ["Total", "Handled"],
        ascending=[False, False],
    )


def _render_perf_individual_summary_table(summary: pd.DataFrame) -> None:
    if summary.empty:
        return
    st.dataframe(summary, use_container_width=True, hide_index=True)


def _render_perf_person_bar(
    view: pd.DataFrame,
    *,
    title: str,
    value_name: str = "Tickets",
) -> None:
    if view.empty:
        st.caption("No data for this filter.")
        return
    if "staff" not in view.columns:
        view = _perf_enrich_tickets(view)
    totals = (
        view.groupby("staff", as_index=False)
        .size()
        .rename(columns={"size": value_name})
        .sort_values(value_name, ascending=False)
    )
    height = min(420, max(160, 32 * len(totals)))
    chart = (
        alt.Chart(totals)
        .mark_bar()
        .encode(
            x=alt.X(f"{value_name}:Q", title=value_name),
            y=alt.Y("staff:N", sort="-x", title=""),
            tooltip=[
                alt.Tooltip("staff:N", title="assigned_to"),
                alt.Tooltip(f"{value_name}:Q", title="Count"),
            ],
            color=alt.value("#D7B491"),
        )
        .properties(height=height, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_perf_stacked_staff_chart(
    view: pd.DataFrame,
    *,
    y_title: str,
    bucket_fmt: str,
    x_title: str,
    axis_format: str,
    chart_height: int = 260,
) -> None:
    if view.empty:
        return
    if "staff" not in view.columns or "_local" not in view.columns:
        view = _perf_enrich_tickets(view)
    view = view.copy()
    view["bucket"] = view["_local"].dt.strftime(bucket_fmt)
    by_staff = (
        view.groupby(["bucket", "staff"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    by_staff["bucket_sort"] = pd.to_datetime(by_staff["bucket"], errors="coerce")
    by_staff = by_staff.sort_values("bucket_sort")
    chart = (
        alt.Chart(by_staff)
        .mark_bar()
        .encode(
            x=alt.X(
                "bucket_sort:T",
                title=x_title,
                axis=alt.Axis(labelAngle=-30, format=axis_format),
            ),
            y=alt.Y("count:Q", title=y_title),
            color=alt.Color("staff:N", legend=alt.Legend(title="assigned_to")),
            tooltip=[
                alt.Tooltip("bucket:N", title="Bucket"),
                alt.Tooltip("staff:N", title="assigned_to"),
                alt.Tooltip("count:Q", title="Count"),
            ],
        )
        .properties(height=chart_height)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_perf_outcome_trend(
    view: pd.DataFrame,
    *,
    bucket_fmt: str,
    x_title: str,
    axis_format: str,
) -> None:
    if view.empty or "_outcome" not in view.columns:
        return
    if "staff" not in view.columns or "_local" not in view.columns:
        view = _perf_enrich_tickets(view)
    view = view.copy()
    view["bucket"] = view["_local"].dt.strftime(bucket_fmt)
    by_out = (
        view.groupby(["bucket", "_outcome"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    by_out["bucket_sort"] = pd.to_datetime(by_out["bucket"], errors="coerce")
    by_out = by_out.sort_values("bucket_sort")
    chart = (
        alt.Chart(by_out)
        .mark_bar()
        .encode(
            x=alt.X(
                "bucket_sort:T",
                title=x_title,
                axis=alt.Axis(labelAngle=-30, format=axis_format),
            ),
            y=alt.Y("count:Q", title="Tickets"),
            color=alt.Color(
                "_outcome:N",
                legend=alt.Legend(title="Outcome"),
                scale=alt.Scale(range=["#D7B491", "#8fa89e"]),
            ),
            tooltip=[
                alt.Tooltip("bucket:N", title="Bucket"),
                alt.Tooltip("_outcome:N", title="Outcome"),
                alt.Tooltip("count:Q", title="Count"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_perf_ticket_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.caption("No tickets to list.")
        return
    detail = df.sort_values("_ts", ascending=False).head(200)
    if "_outcome" in detail.columns:
        detail = detail.rename(columns={"_outcome": "Outcome"})
    cols = [
        c
        for c in (
            "Outcome",
            "status",
            "ticket_number",
            "assigned_to",
            "task_category",
            "last_assigned_at",
            "updated_at",
            "responded_at",
            "follow_up_at",
            "follow_up_note",
            "unattended_nudge_sent_at",
        )
        if c in detail.columns
    ]
    st.dataframe(
        _format_local(detail[cols]),
        use_container_width=True,
        hide_index=True,
    )


def _render_perf_sales_case_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.caption("No Sales Cases to list.")
        return
    view = _perf_enrich_sales_cases(df)
    detail = view.sort_values("_ts", ascending=False).head(200)
    cols = [
        c
        for c in (
            "status_eff",
            "case_ref",
            "account_name",
            "sales_owner",
            "sales_category",
            "account_region",
            "admin_owner",
            "assigned_to",
            "updated_at",
        )
        if c in detail.columns
    ]
    show = detail[cols].rename(columns={"status_eff": "Status"})
    st.dataframe(
        _format_local(show),
        use_container_width=True,
        hide_index=True,
    )


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
            "New activity — check **Daily Task**, **Open**, **Log**, or **Performance**.",
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
            "Ticket Number must be exactly **9** or **16** digits "
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
    operator_id: str,
) -> None:
    now_iso = _cc_utc_now_iso()
    row: dict = {
        "ticket_number": ticket_number,
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": STATUS_DAILY_TASK,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "last_assigned_at": now_iso,
        "unattended_nudge_sent_at": None,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
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
        note=_cc_assignment_log_note(additional_info, operator_id),
    )


def _cc_insert_pending_unassigned(
    client,
    ticket_number: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    operator_id: str,
) -> None:
    """Queue a ticket in Daily Task with no engineer and no Telegram post."""
    row: dict = {
        "ticket_number": ticket_number,
        "assigned_to": None,
        "task_category": task_category,
        "status": STATUS_DAILY_TASK,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "last_assigned_at": None,
        "unattended_nudge_sent_at": None,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
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

    note = _cc_assignment_log_note(additional_info, operator_id) or (
        "Queued in Daily Task without engineer (no Telegram)."
    )
    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=f"@{operator_id.lstrip('@')}",
        action_type="TicketQueued",
        note=note,
    )


def _cc_queue_pending_unassigned(
    ticket_number: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    operator_id: str,
) -> str:
    """Create a Daily Task ticket with no assignee and no Telegram message."""
    client = _get_supabase_client()
    existing = _cc_fetch_ticket_minimal(client, ticket_number)
    if existing is not None:
        raise ValueError(
            f"Ticket **{ticket_number}** already exists "
            f"(status **{existing.get('status') or '—'}**)."
        )
    _cc_insert_pending_unassigned(
        client,
        ticket_number,
        task_category,
        additional_info=additional_info,
        operator_id=operator_id,
    )
    return (
        f"**{ticket_number}** added to **Daily Task** (no engineer, no Telegram). "
        "Assign or move to **Under Investigation** from the Daily Task queue."
    )


def _cc_reassign_ticket(
    client,
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    operator_id: str,
) -> None:
    now_iso = _cc_utc_now_iso()
    updates = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "status": STATUS_DAILY_TASK,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "responded_at": None,
        "updated_at": now_iso,
        "last_assigned_at": now_iso,
        "unattended_nudge_sent_at": None,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
    }
    _cc_execute_ticket_update(client, updates, ticket_number)
    _cc_ensure_reassign_cleared_response_fields(client, ticket_number)

    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=_cc_assignment_log_note(additional_info, operator_id),
    )


def _cc_ensure_reassign_cleared_response_fields(
    client, ticket_number: str
) -> None:
    """Retry NULL response columns when the first PATCH left stale metadata."""
    row = _cc_fetch_ticket_minimal(client, ticket_number)
    if not row:
        return
    if not (
        str(row.get("field_response") or "").strip()
        or row.get("photo_url")
        or row.get("field_responded_by")
        or row.get("responded_at")
    ):
        return
    _cc_execute_ticket_update(
        client,
        {
            "field_response": None,
            "field_responded_by": None,
            "photo_url": None,
            "responded_at": None,
            "updated_at": _cc_utc_now_iso(),
        },
        ticket_number,
    )


def _cc_upsert_assignment(
    assigned_to: str,
    ticket_number: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    operator_id: str,
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
            operator_id=operator_id,
        )
        return f"Created ticket **{ticket_number}** and logged assignment."
    _cc_reassign_ticket(
        client,
        ticket_number,
        assigned_to,
        task_category,
        additional_info=additional_info,
        operator_id=operator_id,
    )
    prev_assignee = existing.get("assigned_to") or "—"
    return (
        f"Re-assigned **{ticket_number}** from {prev_assignee} to {assigned_to}; "
        "status reset to Daily Task."
    )


def _cc_save_assignment_telegram_ref(
    client,
    ticket_number: str,
    ref: AssignmentTelegramRef,
) -> None:
    _cc_execute_ticket_update(
        client,
        {
            "assignment_telegram_chat_id": int(ref.chat_id),
            "assignment_telegram_message_id": int(ref.message_id),
            "updated_at": _cc_utc_now_iso(),
        },
        ticket_number,
    )
    verify = _fetch_ticket_row(ticket_number)
    if not verify or verify.get("assignment_telegram_message_id") is None:
        raise RuntimeError(
            "Could not save Telegram message link on the ticket. "
            "Apply migration `20260524_assignment_telegram_message.sql` in Supabase."
        )


def _cc_patch_assignment_fields(
    ticket_number: str,
    *,
    required_status: str,
    assigned_to: str,
    task_category: str,
    additional_info: str | None,
    operator_id: str,
) -> dict:
    """Update assignment fields for a ticket in the given status (dashboard edit)."""
    client = _get_supabase_client()
    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    status = str(row.get("status") or "").strip()
    if status != required_status:
        raise ValueError(f"Only **{required_status}** tickets can be edited here.")

    now_iso = _cc_utc_now_iso()
    prev_handle = str(row.get("assigned_to") or "").strip()
    updates: dict[str, object] = {
        "assigned_to": assigned_to,
        "task_category": task_category,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
        "updated_at": now_iso,
    }
    if assigned_to and not prev_handle:
        updates["last_assigned_at"] = now_iso
    _cc_execute_ticket_update(client, updates, ticket_number)
    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=f"@{operator_id.lstrip('@')}",
        action_type="AssignmentUpdated",
        note=_cc_assignment_log_note(additional_info, operator_id),
    )
    updated = _fetch_ticket_row(ticket_number)
    return updated or row


def _cc_dashboard_reassign_ticket(
    ticket_number: str,
    *,
    assigned_to: str,
    task_category: str,
    additional_info: str | None,
    operator_id: str,
    from_status: str,
) -> dict:
    """Reassign for next-day field work: reset to Daily Task and clear prior response."""
    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    status = _normalize_ticket_status_value(row.get("status"))
    if status not in _REASSIGNABLE_STATUSES:
        raise ValueError(
            f"Ticket **{ticket_number}** is **{status or '—'}** — cannot reassign from this status."
        )

    client = _get_supabase_client()
    _cc_reassign_ticket(
        client,
        ticket_number,
        assigned_to,
        task_category,
        additional_info=additional_info,
        operator_id=operator_id,
    )
    if from_status in ("Open", STATUS_DAILY_TASK, STATUS_ON_HOLD, STATUS_UNDER_INVESTIGATION):
        if from_status == "Open":
            action_type = "ReassignedFromOpen"
            note = "Moved back to Daily Task for next-day field work."
        elif from_status == STATUS_UNDER_INVESTIGATION:
            action_type = "ReassignedFromInvestigation"
            note = "Reassigned from Under Investigation; prior response cleared."
        elif from_status == STATUS_ON_HOLD:
            action_type = "ReassignedFromOnHold"
            note = "Reassigned from On Hold; prior response cleared for a fresh visit."
        else:
            action_type = "ReassignedFromPending"
            note = "Reassigned while Daily Task; prior response cleared for a fresh visit."
        try:
            client.table(ATTENDANCE_LOGS_TABLE).insert(
                {
                    "ticket_number": ticket_number,
                    "member_username": f"@{operator_id.lstrip('@')}",
                    "action_type": action_type,
                    "note": _cc_assignment_log_note(note, operator_id),
                    "timestamp": _cc_utc_now_iso(),
                }
            ).execute()
        except Exception:
            pass
    updated = _fetch_ticket_row(ticket_number)
    return updated or row


def _cc_resolve_telegram_credentials() -> tuple[str | None, int | str | None]:
    """Bot token + group chat id from env / secrets (Command Center)."""
    token = (
        _read_setting("TG_BOT_TOKEN").strip()
        or _read_setting("TELEGRAM_BOT_TOKEN").strip()
        or _read_setting("TELEGRAM_TOKEN").strip()
    )
    chat_raw = _read_telegram_group_chat_raw()
    chat_id: int | str | None = None
    if chat_raw:
        chat_id, _warn = _parse_telegram_group_chat_id(chat_raw)
    return token or None, chat_id


async def _cc_sync_assignment_to_telegram(
    *,
    row: dict,
    assigned_to: str,
    task_category: str,
    additional_info: str | None,
    operator_id: str,
    token: str,
    chat_id: int | str,
) -> str:
    """Edit linked Telegram message or post a new one. Returns user-facing status text."""
    ticket_number = str(row.get("ticket_number") or "")
    tg_chat = row.get("assignment_telegram_chat_id")
    tg_msg = row.get("assignment_telegram_message_id")
    assigned_by = f"{operator_id} (updated)"
    api_id = _read_setting("TG_API_ID") or _read_setting("TELEGRAM_API_ID") or None
    api_hash = _read_setting("TG_API_HASH") or _read_setting("TELEGRAM_API_HASH") or None
    client = _get_supabase_client()

    if tg_chat is None or tg_msg is None:
        found = await find_assignment_telegram_ref(
            ticket_number,
            group_id=chat_id,
            bot_token=token,
            api_id=api_id,
            api_hash=api_hash,
        )
        if found:
            tg_chat, tg_msg = found.chat_id, found.message_id
            _cc_save_assignment_telegram_ref(client, ticket_number, found)

    if tg_chat is not None and tg_msg is not None:
        try:
            await update_telegram_assignment_message(
                int(tg_chat),
                int(tg_msg),
                assigned_to,
                ticket_number,
                task_category,
                additional_info=additional_info,
                assigned_by=None,
                updated=False,
                api_id=api_id,
                api_hash=api_hash,
                bot_token=token,
            )
            return (
                "The **same** Telegram assignment message was updated in the group "
                "(no new post; notes refreshed in place)."
            )
        except Exception:
            pass

    ref = await notify_telegram_group(
        assigned_to,
        ticket_number,
        task_category,
        additional_info=additional_info,
        assigned_by=assigned_by,
        updated=True,
        api_id=api_id,
        api_hash=api_hash,
        bot_token=token,
        group_id=chat_id,
    )
    _cc_save_assignment_telegram_ref(client, ticket_number, ref)
    return (
        "Posted a **new** assignment message at the bottom of the group "
        "(starts with “Assignment updated”). Use that message for field replies."
    )


def _render_assignment_editor(
    *,
    required_status: str,
    key_prefix: str,
    edit_key_prefix: str,
    cat_names: list[str],
    fe_names: list[str],
    fe_missing: bool,
    ticket_options: list[str],
) -> None:
    """Edit assignment fields + optional Telegram sync (Daily Task or Open)."""
    if not ticket_options:
        return

    keys = _assignment_edit_session_keys(edit_key_prefix)
    picked = _picked_ticket_from_selection(
        key_prefix=key_prefix, ticket_options=ticket_options
    )
    if not picked:
        return

    row = _fetch_ticket_row(picked)
    if not row:
        st.warning("Ticket not found.")
        return
    if str(row.get("status") or "").strip() != required_status:
        st.info(f"Pick a **{required_status}** ticket to edit its assignment.")
        return

    linked = (
        row.get("assignment_telegram_message_id") is not None
        and row.get("assignment_telegram_chat_id") is not None
    )
    st.caption(
        f"Editing **{picked}** — saves to the dashboard. "
        + (
            "Telegram: will **edit the original** post in place (no “Assignment updated” banner)."
            if linked
            else "Telegram: no message link yet — will **post a new** message with "
            "“Assignment updated” at the bottom of the group."
        )
    )

    current_handle = str(row.get("assigned_to") or "").strip().lstrip("@")
    current_cat = str(row.get("task_category") or "").strip()
    current_notes = str(row.get("additional_info") or "")

    cats = cat_names if cat_names else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    if current_cat and current_cat not in cats:
        cats = [current_cat, *cats]

    _sync_assignment_edit_widgets(
        keys=keys,
        picked=picked,
        current_handle=current_handle,
        current_cat=current_cat,
        current_notes=current_notes,
        cats=cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
    )

    with st.form(f"{edit_key_prefix}_assignment_edit_form", clear_on_submit=False):
        if fe_names and not fe_missing:
            fe_opts = [f"@{n}" for n in fe_names]
            st.selectbox(
                "Engineer",
                options=fe_opts,
                key=keys["engineer"],
            )
        else:
            st.text_input(
                "Engineer",
                placeholder="username",
                key=keys["engineer"],
            )
        st.selectbox(
            "Category",
            options=cats,
            key=keys["category"],
        )
        st.text_area(
            "Notes (Additional Info)",
            height=80,
            key=keys["notes"],
        )
        st.checkbox(
            "Update Telegram Assignment Message",
            value=True,
            key=keys["sync_tg"],
        )
        submitted = st.form_submit_button("Save assignment changes", use_container_width=True)

    if not submitted:
        return

    try:
        if fe_names and not fe_missing:
            handle = _cc_normalize_handle(
                str(st.session_state.get(keys["engineer"], ""))
            )
        else:
            raw = str(st.session_state.get(keys["engineer"], "")).strip()
            handle = _cc_normalize_handle(raw) if raw else ""
            if not handle:
                raise ValueError("Enter an engineer username.")
        cat = str(st.session_state.get(keys["category"], "")).strip()
        if not cat:
            raise ValueError("Pick a category.")
        notes = str(st.session_state.get(keys["notes"], "")).strip() or None
    except ValueError as exc:
        st.error(str(exc))
        return

    op = _session_operator_id()
    if not op:
        st.error("Sign in again — operator session is missing.")
        return

    try:
        updated = _cc_patch_assignment_fields(
            picked,
            required_status=required_status,
            assigned_to=handle,
            task_category=cat,
            additional_info=notes,
            operator_id=op,
        )
    except Exception as exc:
        st.error(f"Could not save: {exc}")
        return

    tg_note = ""
    if st.session_state.get(keys["sync_tg"]):
        token, chat_id = _cc_resolve_telegram_credentials()
        if not token or chat_id is None:
            st.warning(
                "Saved in dashboard. Telegram not updated — set **TELEGRAM_TOKEN** and "
                "**TELEGRAM_GROUP_CHAT_ID** in `.env` / Secrets."
            )
        else:
            try:
                tg_note = asyncio.run(
                    _cc_sync_assignment_to_telegram(
                        row=updated,
                        assigned_to=handle,
                        task_category=cat,
                        additional_info=notes,
                        operator_id=op,
                        token=token,
                        chat_id=chat_id,
                    )
                )
            except Exception as exc:
                st.warning(f"Saved in dashboard. Telegram update failed: {exc}")
                tg_note = ""

    st.session_state[keys["show"]] = False
    st.session_state[_CC_FLASH_KEY] = (
        f"Updated **{required_status}** assignment **{picked}**."
        + (f" {tg_note}" if tg_note else "")
    )
    st.rerun()


def _render_reassign_editor(
    *,
    from_status: str,
    key_prefix: str,
    edit_key_prefix: str,
    cat_names: list[str],
    fe_names: list[str],
    fe_missing: bool,
    ticket_options: list[str],
) -> None:
    """Reassign a Daily Task or Open ticket: fresh Daily Task row, optional new Telegram post."""
    if not ticket_options:
        return

    keys = _reassign_session_keys(edit_key_prefix)
    picked = _picked_ticket_from_selection(
        key_prefix=key_prefix, ticket_options=ticket_options
    )
    if not picked:
        return

    row = _fetch_ticket_row(picked)
    if not row:
        st.warning("Ticket not found.")
        return
    if picked not in ticket_options:
        st.info("Select a ticket in this queue, then click **Reassign** again.")
        return

    actual_status = _normalize_ticket_status_value(row.get("status"))
    if actual_status not in _REASSIGNABLE_STATUSES:
        st.info(
            f"Cannot reassign — ticket **{picked}** is **{actual_status or '—'}**."
        )
        return

    st.caption(
        f"Reassign **{picked}** for next-day field work → **Daily Task**. "
        "Clears the previous field response and photo. "
        "Posts a **new** assignment line in Telegram (when enabled below)."
    )

    current_handle = str(row.get("assigned_to") or "").strip().lstrip("@")
    current_cat = str(row.get("task_category") or "").strip()
    current_notes = str(row.get("additional_info") or "")

    cats = cat_names if cat_names else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    if current_cat and current_cat not in cats:
        cats = [current_cat, *cats]

    _sync_assignment_edit_widgets(
        keys=keys,
        picked=picked,
        current_handle=current_handle,
        current_cat=current_cat,
        current_notes=current_notes,
        cats=cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
    )

    with st.form(f"{edit_key_prefix}_reassign_form", clear_on_submit=False):
        if fe_names and not fe_missing:
            st.selectbox("Engineer", options=[f"@{n}" for n in fe_names], key=keys["engineer"])
        else:
            st.text_input("Engineer", placeholder="username", key=keys["engineer"])
        st.selectbox("Category", options=cats, key=keys["category"])
        st.text_area("Notes (Additional Info)", height=80, key=keys["notes"])
        st.checkbox(
            "Post New Telegram Assignment",
            value=True,
            key=keys["sync_tg"],
        )
        submitted = st.form_submit_button(
            "Reassign → Daily Task", type="primary", use_container_width=True
        )

    if not submitted:
        return

    try:
        if fe_names and not fe_missing:
            handle = _cc_normalize_handle(str(st.session_state.get(keys["engineer"], "")))
        else:
            raw = str(st.session_state.get(keys["engineer"], "")).strip()
            handle = _cc_normalize_handle(raw) if raw else ""
            if not handle:
                raise ValueError("Enter an engineer username.")
        cat = str(st.session_state.get(keys["category"], "")).strip()
        if not cat:
            raise ValueError("Pick a category.")
        notes = str(st.session_state.get(keys["notes"], "")).strip() or None
    except ValueError as exc:
        st.error(str(exc))
        return

    op = _session_operator_id()
    if not op:
        st.error("Sign in again — operator session is missing.")
        return

    try:
        updated = _cc_dashboard_reassign_ticket(
            picked,
            assigned_to=handle,
            task_category=cat,
            additional_info=notes,
            operator_id=op,
            from_status=actual_status,
        )
    except Exception as exc:
        st.error(f"Could not reassign: {exc}")
        return

    tg_note = ""
    if st.session_state.get(keys["sync_tg"]):
        token, chat_id = _cc_resolve_telegram_credentials()
        if not token or chat_id is None:
            st.warning(
                "Reassigned in dashboard. Telegram not posted — set **TELEGRAM_TOKEN** and "
                "**TELEGRAM_GROUP_CHAT_ID**."
            )
        else:
            try:
                ref = asyncio.run(
                    notify_telegram_group(
                        handle,
                        picked,
                        cat,
                        additional_info=notes,
                        assigned_by=f"{op} (reassigned)",
                        api_id=_read_setting("TG_API_ID")
                        or _read_setting("TELEGRAM_API_ID")
                        or None,
                        api_hash=_read_setting("TG_API_HASH")
                        or _read_setting("TELEGRAM_API_HASH")
                        or None,
                        bot_token=token,
                        group_id=chat_id,
                    )
                )
                _cc_save_assignment_telegram_ref(_get_supabase_client(), picked, ref)
                tg_note = (
                    "Posted a **new** assignment message in the group — "
                    "field must swipe-reply to that line."
                )
            except Exception as exc:
                st.warning(f"Reassigned in dashboard. Telegram post failed: {exc}")

    st.session_state[keys["show"]] = False
    st.session_state[_CC_FLASH_KEY] = (
        f"**{picked}** reassigned → **Daily Task** ({handle}, {cat})."
        + (f" {tg_note}" if tg_note else "")
    )
    _get_supabase_client.clear()
    st.rerun()


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
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return [], False
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


def _ensure_task_categories_synced(client) -> None:
    """Backfill ``dashboard_task_categories`` from ticket rows (once per session)."""
    if st.session_state.get(_CATEGORIES_SYNCED_ONCE_KEY):
        return
    try:
        sync_ticket_categories_into_table(
            client,
            tickets_table=TICKETS_TABLE,
            categories_table=TASK_CATEGORIES_TABLE,
        )
    except Exception:
        pass
    st.session_state[_CATEGORIES_SYNCED_ONCE_KEY] = True


def _try_fetch_task_categories() -> tuple[list[str], bool]:
    """Return ``(category names, table_missing)`` from Supabase."""
    client = _get_supabase_client()
    _ensure_task_categories_synced(client)
    try:
        return fetch_task_category_names(client)
    except Exception as exc:
        if _looks_like_missing_table_error(exc):
            return [], True
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES), False
        raise


def _categories_manage_popover(categories: list[str], *, missing: bool) -> None:
    if missing:
        st.caption(f"Category table missing — apply `{TASK_CATEGORIES_TABLE}` migration.")
        return

    if not categories:
        st.caption("No categories yet.")
    for cat in categories:
        c_name, c_rm = st.columns([5, 1], gap="small", vertical_alignment="center")
        with c_name:
            st.markdown(f"**{cat}**")
        with c_rm:
            hkey = hashlib.sha256(cat.encode("utf-8")).hexdigest()[:16]
            if st.button(
                "×",
                key=f"cat_rm_{hkey}",
                help=f"Remove {cat}",
                type="secondary",
            ):
                try:
                    delete_task_category(_get_supabase_client(), cat)
                    st.session_state.pop(_CC_CATEGORY_SELECT_KEY, None)
                    st.session_state[_CATEGORIES_SYNCED_ONCE_KEY] = False
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    c_add, c_go = st.columns([5, 1], gap="small", vertical_alignment="bottom")
    with c_add:
        st.text_input(
            "Add Category",
            key="cc_new_category",
            placeholder="e.g. Site Survey",
            label_visibility="collapsed",
        )
    with c_go:
        if st.button("+", key="cc_cat_add_btn", help="Add Category", type="secondary"):
            raw = str(st.session_state.get("cc_new_category") or "").strip()
            if not raw:
                st.warning("Type a category name first.")
            else:
                try:
                    norm = normalize_task_category_name(raw)
                    existing, _ = _try_fetch_task_categories()
                    if any(c.lower() == norm.lower() for c in existing):
                        st.warning(f"**{norm}** is already listed.")
                    else:
                        upsert_task_category(_get_supabase_client(), norm)
                        st.session_state.pop("cc_new_category", None)
                        st.session_state[_CC_CATEGORY_SELECT_PENDING_KEY] = norm
                        st.session_state[_CATEGORIES_SYNCED_ONCE_KEY] = False
                        st.session_state[_CC_FLASH_KEY] = (
                            f"Category **{norm}** saved to Supabase — "
                            "assign picker and Telegram bot use it on the next assignment."
                        )
                        st.rerun()
                except ValueError as ve:
                    st.error(str(ve))
                except Exception as exc:
                    err = str(exc).lower()
                    if "duplicate" in err or "23505" in str(exc) or "unique" in err:
                        st.warning("Category already exists.")
                    else:
                        st.error(str(exc))


def _render_cc_category_row(categories: list[str], *, missing: bool) -> None:
    opts = categories if categories else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    pending = st.session_state.pop(_CC_CATEGORY_SELECT_PENDING_KEY, None)
    if pending is not None and pending in opts:
        st.session_state[_CC_CATEGORY_SELECT_KEY] = pending
    else:
        current = st.session_state.get(_CC_CATEGORY_SELECT_KEY)
        if current not in opts:
            st.session_state[_CC_CATEGORY_SELECT_KEY] = opts[0]
    st.selectbox("Category", options=opts, key=_CC_CATEGORY_SELECT_KEY)
    with st.popover("Edit categories", key="cc_categories_popover"):
        _categories_manage_popover(categories or opts, missing=missing)


def _field_team_manage_popover(names: list[str], *, missing: bool) -> None:
    """Compact add/remove handles list inside **Edit team** popover."""
    if missing:
        st.caption("Team table missing in Supabase.")
        return

    if not names:
        st.caption("No handles yet.")
    for u in names:
        c_name, c_rm = st.columns([5, 1], gap="small", vertical_alignment="center")
        with c_name:
            st.markdown(
                f'<p class="cc-team-handle">@{u}</p>',
                unsafe_allow_html=True,
            )
        with c_rm:
            hkey = hashlib.sha256(u.encode("utf-8")).hexdigest()[:16]
            if st.button(
                "×",
                key=f"fe_rm_{hkey}",
                help=f"Remove @{u}",
                type="secondary",
            ):
                try:
                    _delete_field_engineer(u)
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    c_add, c_go = st.columns([5, 1], gap="small", vertical_alignment="bottom")
    with c_add:
        st.text_input(
            "Add Handle",
            key="fe_new_handle",
            placeholder="name",
            label_visibility="collapsed",
        )
    with c_go:
        if st.button("+", key="fe_add_btn", help="Add Handle", type="secondary"):
            raw = str(st.session_state.get("fe_new_handle") or "").strip()
            if not raw:
                st.warning("Type a handle first.")
            else:
                try:
                    norm = _normalize_engineer_dir_handle(raw)
                    existing, _ = _try_fetch_field_engineer_usernames()
                    if any(e.lower() == norm.lower() for e in existing):
                        st.warning(f"**@{norm}** is already listed.")
                    else:
                        _insert_field_engineer(norm)
                        st.session_state.pop("fe_new_handle", None)
                        st.rerun()
                except ValueError as ve:
                    st.error(str(ve))
                except Exception as exc:
                    err = str(exc).lower()
                    if "duplicate" in err or "23505" in str(exc) or "unique" in err:
                        st.warning("Handle already exists.")
                    else:
                        st.error(str(exc))


def _render_cc_engineer_row(names: list[str], *, missing: bool) -> None:
    """Engineer picker + team list popover."""
    if missing:
        st.info(
            f"Directory table missing — type a username below, or add "
            f"`{FIELD_ENGINEERS_TABLE}` in Supabase."
        )
        st.text_input(
            "Engineer",
            placeholder="@ibeyx",
            key=_CC_FE_MANUAL_KEY,
        )
        return

    if names:
        st.selectbox(
            "Engineer",
            options=[f"@{n}" for n in names],
            key=_CC_FE_SELECT_KEY,
        )
    else:
        st.text_input(
            "Engineer",
            placeholder="@ibeyx",
            key=_CC_FE_MANUAL_KEY,
        )

    with st.popover("Edit team", key="cc_team_popover"):
        _field_team_manage_popover(names, missing=missing)


def _render_ticket_number_picker() -> None:
    """Assign tab: type ticket_number only (no list of other tickets)."""
    st.text_input(
        "Ticket",
        placeholder="9 or 16 digits",
        key=_CC_TICKET_INPUT_KEY,
    )


def _resolve_cc_ticket_number() -> str:
    return str(st.session_state.get(_CC_TICKET_INPUT_KEY, "")).strip()


def _reset_cc_assign_form(*, categories: list[str]) -> None:
    """Clear Assign sidebar fields — call only **before** those widgets render."""
    st.session_state[_CC_TICKET_INPUT_KEY] = ""
    st.session_state[_CC_ASSIGN_NOTES_KEY] = ""
    st.session_state[_CC_ADD_UNASSIGNED_KEY] = False
    opts = categories if categories else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    if opts:
        st.session_state[_CC_CATEGORY_SELECT_KEY] = opts[0]


def _cc_schedule_assign_form_clear() -> None:
    """Defer clear until next run (cannot mutate widget keys after they render)."""
    st.session_state[_CC_CLEAR_ASSIGN_KEY] = True


def _reset_sc_cc_sales_ticket_form() -> None:
    st.session_state[_SC_CC_ST_REF_KEY] = ""
    st.session_state[_SC_CC_ST_ACCOUNT_KEY] = ""
    st.session_state[_SC_CC_ST_OWNER_KEY] = ""
    st.session_state[_SC_CC_ST_DESC_KEY] = ""
    if _SC_CC_ST_PRIORITY_KEY in st.session_state:
        st.session_state[_SC_CC_ST_PRIORITY_KEY] = SALES_PRIORITY_OPTIONS[-1]


def _sc_insert_intake_case(
    *,
    case_ref: str,
    account_name: str,
    sales_owner: str,
    sales_priority: str,
    account_region: str,
    sales_category: str,
    description: str | None,
    status: str,
    queue_metric_label: str,
    clear_sales_ticket_form: bool = False,
) -> None:
    try:
        _sales_cases_insert_row(
            {
                "case_ref": case_ref,
                "account_name": account_name,
                "sales_owner": sales_owner,
                "sales_priority": sales_priority,
                "account_region": account_region,
                "sales_category": sales_category,
                "description": description,
                "status": status,
                "admin_owner": None,
            }
        )
    except Exception as exc:
        _sc_set_sales_flash(f"Could not create case: {exc}", level="error")
        st.rerun()
        return

    _get_supabase_client.clear()
    _sc_set_sales_flash(
        f"Case created — see **{status}** under **Sales Cases**."
    )
    st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Sales Cases"
    st.session_state[_DASH_PENDING_SALES_QUEUE_KEY] = _queue_segment_label(
        queue_metric_label, 1
    )
    if clear_sales_ticket_form:
        st.session_state[_SC_CC_CLEAR_ST_INTAKE_KEY] = True
    st.rerun()


def _sidebar_sales_intake() -> None:
    """SALES intake — new sales cases from the sidebar (no field Telegram)."""
    try:
        probe = _fetch_sales_cases_df()
    except Exception:
        return
    if probe is None:
        st.caption(
            "**SALES** unavailable — apply "
            "`supabase/migrations/20260620_dashboard_sales_cases.sql`."
        )
        return

    st.caption("Cases start in **Sales Ticket** — no Telegram post.")

    if st.session_state.pop(_SC_CC_CLEAR_ST_INTAKE_KEY, False):
        _reset_sc_cc_sales_ticket_form()

    sales_cats = list(DEFAULT_SALES_CASE_CATEGORIES)

    with st.container(border=True, key="sc_cc_intake_block"):
        st.markdown("##### SALES")
        st.caption("Ticket # and Resort/Company.")
        st.text_input(
            "Ticket Number",
            key=_SC_CC_ST_REF_KEY,
            placeholder="9 or 16 digits",
        )
        st.text_input(
            "Resort Name / Company Name",
            key=_SC_CC_ST_ACCOUNT_KEY,
            placeholder="Resort or company name",
        )
        r3, r4 = st.columns(2)
        with r3:
            st.selectbox(
                "Sales priority",
                options=list(SALES_PRIORITY_OPTIONS),
                key=_SC_CC_ST_PRIORITY_KEY,
            )
        with r4:
            st.selectbox(
                "Region Team",
                options=list(SALES_REGION_CODES),
                key=_SC_CC_ST_REGION_KEY,
            )
        st.selectbox(
            "Sales Category (Intent)",
            options=sales_cats,
            key=_SC_CC_ST_SCAT_KEY,
        )
        st.text_area(
            "Description (Optional)",
            key=_SC_CC_ST_DESC_KEY,
            height=64,
            placeholder="Short summary for the admin team",
        )
        submit_st = st.button(
            "Create Sales Case",
            type="primary",
            use_container_width=True,
            key="sc_cc_st_submit",
        )

    if submit_st:
        cr = str(st.session_state.get(_SC_CC_ST_REF_KEY, "")).strip()
        an = str(st.session_state.get(_SC_CC_ST_ACCOUNT_KEY, "")).strip()
        if not cr or not an:
            _sc_set_sales_flash(
                "Fill **Ticket Number** and **Resort Name / Company Name**.",
                level="warning",
            )
            st.rerun()
            return
        _sc_insert_intake_case(
            case_ref=cr,
            account_name=an,
            sales_owner="",
            sales_priority=str(
                st.session_state.get(_SC_CC_ST_PRIORITY_KEY, "Standard")
            ).strip(),
            account_region=str(st.session_state.get(_SC_CC_ST_REGION_KEY, "")).strip(),
            sales_category=str(st.session_state.get(_SC_CC_ST_SCAT_KEY, "")).strip(),
            description=str(st.session_state.get(_SC_CC_ST_DESC_KEY, "")).strip() or None,
            status=SC_STATUS_SALES_TICKET,
            queue_metric_label=SC_STATUS_SALES_TICKET,
            clear_sales_ticket_form=True,
        )


def _normalize_cc_sidebar_tab(raw: object) -> str:
    """Map legacy Assign / Sales intake session values to CSM / SALES."""
    s = str(raw or "").strip()
    if s in _CC_SIDEBAR_TAB_OPTIONS:
        return s
    low = s.casefold()
    if low in ("assign", "csm", "field", "field assign"):
        return CC_TAB_CSM
    if low in ("sales", "sales intake", "sales\nintake"):
        return CC_TAB_SALES
    return CC_TAB_CSM


def _render_cc_sidebar_nav() -> str:
    """TICKET hub — same expander + radio pattern as Team accounts / Filters."""
    st.session_state[_CC_SIDEBAR_TAB_KEY] = _normalize_cc_sidebar_tab(
        st.session_state.get(_CC_SIDEBAR_TAB_KEY)
    )
    with st.expander("TICKET", expanded=True, key="bon_box_ticket"):
        st.radio(
            "Branch",
            options=list(_CC_SIDEBAR_TAB_OPTIONS),
            key=_CC_SIDEBAR_TAB_KEY,
            label_visibility="collapsed",
            horizontal=False,
        )
    choice = _normalize_cc_sidebar_tab(st.session_state.get(_CC_SIDEBAR_TAB_KEY))
    return "sales" if choice == CC_TAB_SALES else "assign"


def _sidebar_field_assign() -> None:
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
                "Group ID from env / Streamlit Secrets is invalid. "
                + env_group_warn
                + " Fix **TELEGRAM_GROUP_CHAT_ID** (or **TG_GROUP_ID**) in Secrets / `.env` and restart."
            )
    env_group_ok = env_group_parsed is not None

    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
    cat_names, cat_missing = _try_fetch_task_categories()

    if st.session_state.pop(_CC_CLEAR_ASSIGN_KEY, False):
        _reset_cc_assign_form(categories=cat_names)

    submitted = False

    with st.container(border=True, key="cc_assign_block"):
        st.markdown("##### CSM")
        st.caption("Field Assignment → Telegram Group.")
        add_unassigned = st.checkbox(
            "Add to Daily Task Only (No Engineer, No Telegram)",
            key=_CC_ADD_UNASSIGNED_KEY,
            help=(
                "Creates the ticket in **Daily Task** without posting to the field group. "
                "Assign or send to **Under Investigation** from the Daily Task queue."
            ),
        )
        if not add_unassigned:
            _render_cc_engineer_row(fe_names, missing=fe_missing)
        else:
            st.caption(
                "No engineer yet — use **Daily Task** to assign or investigate later."
            )
        _render_ticket_number_picker()
        _render_cc_category_row(cat_names, missing=cat_missing)
        st.text_area(
            "Notes (Optional)",
            placeholder="Context for the field team",
            height=64,
            key=_CC_ASSIGN_NOTES_KEY,
        )
        if not add_unassigned:
            if not token_env:
                st.text_input(
                    "Bot Token (Session Only)",
                    type="password",
                    key=_CC_SESSION_TOKEN_KEY,
                    placeholder="If missing from Secrets",
                )
            if not env_group_ok:
                st.text_input(
                    "Group Chat ID",
                    key=_CC_SESSION_GROUP_KEY,
                    placeholder="-100… or @group",
                )
        submit_label = "Add to Daily Task" if add_unassigned else "Assign"
        submitted = st.button(
            submit_label,
            type="primary",
            use_container_width=True,
            key="cc_assign_submit_btn",
        )

    if not submitted:
        return

    add_unassigned = bool(st.session_state.get(_CC_ADD_UNASSIGNED_KEY))

    try:
        tid = _cc_validate_ticket_number(_resolve_cc_ticket_number())
    except ValueError as exc:
        _cc_set_flash(str(exc), level="error")
        st.rerun()
        return

    additional_info_val = (
        str(st.session_state.get(_CC_ASSIGN_NOTES_KEY, "")).strip() or None
    )
    cat = str(st.session_state.get(_CC_CATEGORY_SELECT_KEY, "")).strip()
    if not cat:
        _cc_set_flash("Pick a **Category**.", level="error")
        st.rerun()
        return

    op_assign = _session_operator_id()
    if not op_assign:
        _cc_set_flash(
            "Session is missing **Operator ID**. Use **Log Out** and sign in again.",
            level="error",
        )
        st.rerun()
        return

    if add_unassigned:
        try:
            summary = _cc_queue_pending_unassigned(
                tid,
                cat,
                additional_info=additional_info_val,
                operator_id=op_assign,
            )
        except ValueError as exc:
            _cc_set_flash(str(exc), level="error")
            st.rerun()
            return
        except Exception as exc:
            _cc_set_flash(f"Could not queue ticket: {exc}", level="error")
            st.rerun()
            return
        _get_supabase_client.clear()
        _cc_set_flash(summary, level="success")
        _cc_schedule_assign_form_clear()
        st.rerun()
        return

    try:
        if fe_names and not fe_missing:
            pick_choice = st.session_state.get(_CC_FE_SELECT_KEY)
            if not pick_choice or not str(pick_choice).strip():
                _cc_set_flash("Pick an engineer from the list.", level="error")
                st.rerun()
                return
            handle = _cc_normalize_handle(str(pick_choice))
        else:
            fe_handle_raw = str(st.session_state.get(_CC_FE_MANUAL_KEY, "")).strip()
            if not fe_handle_raw:
                _cc_set_flash(
                    "Enter an engineer Telegram username.", level="error"
                )
                st.rerun()
                return
            handle = _cc_normalize_handle(fe_handle_raw)
    except ValueError as exc:
        _cc_set_flash(str(exc), level="error")
        st.rerun()
        return

    # Form widgets with ``key=`` sometimes leave return values empty on submit;
    # merge ``st.session_state`` (updated when the form posts).
    token = token_env or str(st.session_state.get(_CC_SESSION_TOKEN_KEY, "")).strip()
    # Prefer env/Secrets; allow one session override when missing or invalid there.
    chat_raw = (env_chat_raw if env_group_ok else "") or str(
        st.session_state.get(_CC_SESSION_GROUP_KEY, "")
    ).strip()
    chat_id: int | str | None = None
    chat_parse_err: str | None = None
    if chat_raw:
        chat_id, chat_parse_err = _parse_telegram_group_chat_id(chat_raw)
    if chat_parse_err:
        _cc_set_flash(chat_parse_err, level="warning")

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
        _cc_set_flash(
            "Cannot post to Telegram yet. " + " · ".join(missing_bits) + ". "
            "If the bot token is missing, use the **session-only** token field in this form or set "
            "**TELEGRAM_TOKEN** in Secrets. "
            "For the group, use top-level Secrets keys or `[telegram]` / `group_chat_id` style keys; "
            "restart after editing Secrets. If the id still is not picked up, paste it in the "
            "**Group chat id (only if missing from Secrets)** field and Assign again.",
            level="error",
        )
        st.rerun()
        return

    op_assign = _session_operator_id()
    if not op_assign:
        _cc_set_flash(
            "Session is missing **Operator ID**. Use **Log Out** and sign in again — "
            "Operator ID is required before Command Center can assign.",
            level="error",
        )
        st.rerun()
        return

    try:
        summary = _cc_upsert_assignment(
            handle,
            tid,
            cat,
            additional_info=additional_info_val,
            operator_id=op_assign,
        )
    except Exception as exc:
        _cc_set_flash(f"Supabase upsert failed: {exc}", level="error")
        st.rerun()
        return

    try:
        tg_ref = asyncio.run(
            notify_telegram_group(
                handle,
                tid,
                cat,
                additional_info=additional_info_val,
                assigned_by=op_assign,
                api_id=_read_setting("TG_API_ID") or _read_setting("TELEGRAM_API_ID") or None,
                api_hash=_read_setting("TG_API_HASH") or _read_setting("TELEGRAM_API_HASH") or None,
                bot_token=token or None,
                group_id=chat_id,
            )
        )
        try:
            _cc_save_assignment_telegram_ref(_get_supabase_client(), tid, tg_ref)
        except Exception as link_exc:
            _cc_set_flash(
                f"{summary} Posted to Telegram but could not link message for edits: {link_exc}",
                level="warning",
            )
            _cc_schedule_assign_form_clear()
            st.rerun()
            return
    except Exception as exc:
        _cc_set_flash(
            f"{summary} Telegram post failed (saved in Supabase): {exc}",
            level="warning",
        )
        _cc_schedule_assign_form_clear()
        st.rerun()
        return

    _cc_set_flash(
        f"{summary} Posted to Telegram ({NOTIFY_BUILD_ID}, one message).",
        level="success",
    )
    _cc_schedule_assign_form_clear()
    st.rerun()


def _sidebar_command_center() -> None:
    _cc_show_flash()
    active = _render_cc_sidebar_nav()
    if active == "sales":
        _sidebar_sales_intake()
    else:
        _sidebar_field_assign()


DEFAULT_REFRESH_MINUTES = 1
MIN_REFRESH_MINUTES = 1
MAX_REFRESH_MINUTES = 60

DEFAULT_LOOKBACK_DAYS = 7
MIN_LOOKBACK_DAYS = 1
MAX_LOOKBACK_DAYS = 365


def _sidebar_controls() -> tuple[bool, int, int]:
    """Return (auto_enabled, interval_minutes, lookback_days)."""
    with st.sidebar:
        st.markdown("### NetOps  \nCoverage Eye")
        op = _session_operator_id()
        if op:
            st.caption(f"Signed in as **{op}**")

        if _dashboard_users_configured() and _is_dashboard_admin():
            _render_dashboard_team_accounts()

        _sidebar_command_center()

        st.markdown("**Time Range**")
        lookback_days, _range_start, _range_end = _sidebar_date_range()

        with st.expander("Filters", expanded=False, key="bon_box_filters"):
            auto = st.toggle("Auto-Refresh", value=True)
            if auto:
                interval_minutes = st.slider(
                    "Every (Minutes)",
                    min_value=MIN_REFRESH_MINUTES,
                    max_value=MAX_REFRESH_MINUTES,
                    value=DEFAULT_REFRESH_MINUTES,
                    step=1,
                )
            else:
                interval_minutes = DEFAULT_REFRESH_MINUTES
            if st.button("Refresh Now", use_container_width=True):
                _get_supabase_client.clear()
                st.session_state.pop(_DASH_LAST_ATTENDANCE_TS_KEY, None)
                st.rerun()
            lookup = st.text_input(
                "Look Up Ticket #",
                placeholder="9 or 16 digits",
                key="dash_ticket_lookup",
            )
            if lookup.strip():
                row = _fetch_ticket_row(lookup.strip())
                if row:
                    st.success(
                        f"**{row.get('ticket_number')}** — **{row.get('status')}**, "
                        f"→ {row.get('assigned_to') or '—'}"
                    )
                else:
                    st.warning("Not found.")

        st.markdown("---")
        if st.button("Log Out", use_container_width=True):
            _clear_auth_session()
            st.session_state.pop(_LOGIN_VIEW_KEY, None)
            st.rerun()
    return auto, int(interval_minutes), int(lookback_days)


def main() -> None:
    # Must be the first Streamlit command every run (login + dashboard).
    st.set_page_config(page_title="NetOps Coverage Eye", layout="wide")

    _check_password()

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
    _render_supabase_unreachable_banner()
    run_every = timedelta(minutes=interval_minutes) if auto else None

    @st.fragment(run_every=run_every)
    def _dashboard_fragment() -> None:
        _render_dashboard(lookback_days=lookback_days)

    _dashboard_fragment()


PHOTO_THUMB_WIDTH = 220  # px — tight enough that 3 fit per row on a laptop

# BONFamily-inspired UI: deep dark-black base, Light Oak (#D7B491) accents.
_BON_THEME_CSS = """
<style>
    :root {
        --bon-bg: #0B0B0B;
        --bon-panel: #0B0B0B;
        --bon-card: #141414;
        --bon-oak: #D7B491;
        --bon-text: #e8e6e3;
        --bon-muted: #a39e97;
        --bon-font: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        --bon-box-border: rgba(215, 180, 145, 0.45);
        --bon-box-radius: 8px;
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
    html, body {
        background-color: var(--bon-bg) !important;
    }
    .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewContainer"] > section,
    [data-testid="stAppViewContainer"] .main,
    [data-testid="stAppViewContainer"] .main > div,
    [data-testid="stMain"],
    [data-testid="stMain"] > div,
    section.main,
    [data-testid="stMain"] [data-testid="block-container"],
    [data-testid="stBottomBlockContainer"],
    [data-testid="stBottom"] {
        background-color: var(--bon-bg) !important;
    }
    [data-testid="stMain"] [data-testid="stVerticalBlock"],
    [data-testid="stMain"] [data-testid="element-container"] {
        background-color: transparent !important;
    }
    .stApp {
        color: var(--bon-text);
    }
    [data-testid="stHeader"] {
        background-color: var(--bon-bg) !important;
        border-bottom: 1px solid var(--bon-oak);
    }
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] > div,
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
        background-color: var(--bon-bg) !important;
    }
    [data-testid="stSidebar"] {
        border-right: 1px solid rgba(215, 180, 145, 0.22);
    }
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] span {
        color: var(--bon-text);
    }
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border: 1px solid var(--bon-box-border) !important;
        border-radius: var(--bon-box-radius) !important;
        background-color: var(--bon-card) !important;
    }
    /* Assign block: one oak outline; form has no extra gray box */
    [data-testid="stSidebar"] div.st-key-cc_assign_block [data-testid="stForm"] {
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        background: transparent !important;
    }
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div,
    [data-testid="stSidebar"] .stTextInput input,
    [data-testid="stSidebar"] .stTextArea textarea,
    [data-testid="stMain"] .stSelectbox [data-baseweb="select"] > div,
    [data-testid="stMain"] .stTextInput input,
    [data-testid="stMain"] .stTextArea textarea,
    [data-testid="stMain"] .stNumberInput input {
        border: 1px solid var(--bon-box-border) !important;
        border-radius: var(--bon-box-radius) !important;
    }
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div:focus-within,
    [data-testid="stSidebar"] .stTextInput input:focus,
    [data-testid="stSidebar"] .stTextArea textarea:focus,
    [data-testid="stMain"] .stSelectbox [data-baseweb="select"] > div:focus-within,
    [data-testid="stMain"] .stTextInput input:focus,
    [data-testid="stMain"] .stTextArea textarea:focus,
    [data-testid="stMain"] .stNumberInput input:focus {
        border-color: var(--bon-oak) !important;
        box-shadow: 0 0 0 1px var(--bon-oak) !important;
    }
    /* SALES intake: prominent ticket + account fields */
    [data-testid="stSidebar"] div.st-key-sc_cc_intake_block div[class*="st-key-sc_cc_st_case_ref"],
    [data-testid="stSidebar"] div.st-key-sc_cc_intake_block div[class*="st-key-sc_cc_st_account"] {
        margin-bottom: 0.65rem !important;
    }
    [data-testid="stSidebar"] div.st-key-sc_cc_intake_block div[class*="st-key-sc_cc_st_case_ref"] label,
    [data-testid="stSidebar"] div.st-key-sc_cc_intake_block div[class*="st-key-sc_cc_st_account"] label {
        font-size: 0.92rem !important;
        font-weight: 600 !important;
        color: var(--bon-text) !important;
    }
    [data-testid="stSidebar"] div.st-key-sc_cc_intake_block div[class*="st-key-sc_cc_st_case_ref"] input,
    [data-testid="stSidebar"] div.st-key-sc_cc_intake_block div[class*="st-key-sc_cc_st_account"] input {
        min-height: 3.1rem !important;
        font-size: 1.08rem !important;
        padding: 0.8rem 1rem !important;
        letter-spacing: 0.02em;
    }
    .stTabs [data-baseweb="tab-list"] {
        background-color: var(--bon-panel);
        border-radius: var(--bon-box-radius);
        padding: 4px;
        gap: 4px;
        border: 1px solid var(--bon-box-border);
    }
    .stTabs [data-baseweb="tab"] {
        color: var(--bon-muted);
        border-radius: 8px;
    }
    .stTabs [aria-selected="true"] {
        background-color: transparent !important;
        color: var(--bon-text) !important;
        box-shadow: inset 0 -2px 0 var(--bon-oak);
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
    /* All buttons (secondary + primary): oak outline like tabs — not Streamlit red */
    .stButton > button,
    .stButton > button[kind="primary"],
    .stButton > button[kind="secondary"],
    [data-testid="stFormSubmitButton"] button,
    [data-testid="stFormSubmitButton"] button[kind="primary"],
    [data-testid="stFormSubmitButton"] button[kind="secondary"],
    button[data-testid="stBaseButton-primary"],
    button[data-testid="stBaseButton-secondary"] {
        border-radius: var(--bon-box-radius) !important;
        border: 1px solid var(--bon-box-border) !important;
        background-color: var(--bon-card) !important;
        background-image: none !important;
        color: var(--bon-muted) !important;
        font-weight: 500;
        box-shadow: none !important;
    }
    .stButton > button[kind="primary"],
    button[data-testid="stBaseButton-primary"] {
        border-color: var(--bon-oak) !important;
        color: var(--bon-oak) !important;
        background-color: #34302c !important;
    }
    .stButton > button:hover,
    .stButton > button:focus,
    .stButton > button:active,
    .stButton > button[kind="primary"]:hover,
    .stButton > button[kind="primary"]:focus,
    .stButton > button[kind="primary"]:active,
    [data-testid="stFormSubmitButton"] button:hover,
    [data-testid="stFormSubmitButton"] button:focus,
    [data-testid="stFormSubmitButton"] button:active,
    button[data-testid="stBaseButton-primary"]:hover,
    button[data-testid="stBaseButton-primary"]:focus,
    button[data-testid="stBaseButton-primary"]:active {
        background-color: rgba(215, 180, 145, 0.12) !important;
        color: var(--bon-oak) !important;
        border-color: var(--bon-oak) !important;
    }
    div[data-baseweb="segmented-control"] button[aria-selected="true"] {
        background-color: #3a342f !important;
        color: var(--bon-oak) !important;
        border-color: var(--bon-oak) !important;
    }
    /* Expandable panels — same box as Log out (sidebar + main) */
    div[data-testid="stExpander"] details {
        border: 1px solid var(--bon-box-border) !important;
        border-radius: var(--bon-box-radius) !important;
        background-color: var(--bon-card) !important;
        overflow: hidden;
        margin-bottom: 0.5rem;
    }
    div[data-testid="stExpander"] summary {
        color: var(--bon-text) !important;
        font-weight: 500 !important;
        font-size: 0.95rem !important;
        padding: 0.65rem 1rem !important;
        min-height: 2.75rem !important;
        display: flex !important;
        align-items: center !important;
        list-style: none !important;
        cursor: pointer;
        box-shadow: none !important;
    }
    div[data-testid="stExpander"] summary:hover {
        background-color: rgba(215, 180, 145, 0.12) !important;
        color: var(--bon-oak) !important;
    }
    div[data-testid="stExpander"] details[open] > div {
        padding: 0.35rem 0.85rem 0.85rem !important;
        border-top: 1px solid rgba(215, 180, 145, 0.22);
    }
    /* Ticket / sales queue: one compact toolbar row (Select all … Apply) */
    div[class*="st-key-"][class*="_tq_toolbar"] .stButton > button,
    div[class*="st-key-"][class*="_tq_toolbar"] [data-testid="stPopover"] > button,
    div[class*="st-key-"][class*="_sc_toolbar"] .stButton > button {
        font-size: 0.72rem !important;
        padding: 0.2rem 0.35rem !important;
        min-height: 1.85rem !important;
        line-height: 1.2 !important;
        white-space: nowrap !important;
    }
    div[class*="st-key-"][class*="_tq_toolbar"] [data-testid="stSelectbox"] label,
    div[class*="st-key-"][class*="_sc_toolbar"] [data-testid="stSelectbox"] label {
        display: none !important;
    }
    div[class*="st-key-"][class*="_tq_toolbar"] [data-testid="stSelectbox"] > div,
    div[class*="st-key-"][class*="_sc_toolbar"] [data-testid="stSelectbox"] > div {
        min-height: 1.85rem !important;
    }
    div[class*="st-key-"][class*="_work_panel"] {
        margin-top: 0.75rem;
        padding: 0.25rem 0 0.5rem 0;
    }
    .sc-case-hero {
        border: 1px solid var(--bon-box-border);
        border-radius: var(--bon-box-radius);
        background: linear-gradient(
            135deg,
            rgba(215, 180, 145, 0.14) 0%,
            rgba(215, 180, 145, 0.04) 55%,
            transparent 100%
        );
        padding: 1rem 1.15rem 1.1rem;
        margin-bottom: 0.35rem;
    }
    .sc-case-hero__eyebrow {
        font-size: 0.68rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--bon-muted);
        margin-bottom: 0.35rem;
    }
    .sc-case-hero__ticket {
        font-size: 1.15rem;
        font-weight: 600;
        color: var(--bon-text);
        line-height: 1.3;
    }
    .sc-case-hero__account {
        font-size: 0.95rem;
        color: var(--bon-muted);
        margin-top: 0.2rem;
        margin-bottom: 0.75rem;
        line-height: 1.35;
    }
    .sc-case-hero__pills {
        display: flex;
        flex-wrap: wrap;
        gap: 0.4rem 0.5rem;
    }
    .sc-meta-pill {
        display: inline-block;
        font-size: 0.78rem;
        padding: 0.28rem 0.55rem;
        border-radius: 999px;
        border: 1px solid rgba(215, 180, 145, 0.35);
        background: rgba(0, 0, 0, 0.25);
        color: var(--bon-text);
        white-space: nowrap;
    }
    .sc-meta-pill--accent {
        border-color: var(--bon-oak);
        background: rgba(215, 180, 145, 0.18);
        color: var(--bon-oak);
        font-weight: 600;
    }
    div[class*="st-key-"][class*="_sc_details_box"] [data-testid="stForm"],
    div[class*="st-key-"][class*="_sc_next_box"] {
        border: none !important;
        background: transparent !important;
        padding: 0 !important;
    }
    div[class*="st-key-"][class*="_sc_details_box"],
    div[class*="st-key-"][class*="_sc_next_box"],
    div[class*="st-key-"][class*="_sc_site_box"],
    div[class*="st-key-"][class*="_work_panel"] > div[data-testid="stVerticalBlockBorderWrapper"] {
        border: 1px solid var(--bon-box-border) !important;
        border-radius: var(--bon-box-radius) !important;
        background: var(--bon-card) !important;
        padding: 0.85rem 1rem 1rem !important;
    }
    div[class*="st-key-"][class*="_work_panel"] .stTabs [data-baseweb="tab-list"] {
        margin-top: 0.25rem;
        margin-bottom: 0.65rem;
    }
    div[class*="st-key-"][class*="_work_panel"] [data-testid="stFormSubmitButton"] button {
        margin-top: 0.35rem;
    }
    /* Remove popover: muted trigger, not full-width primary styling */
    [data-testid="stPopover"] > button {
        font-size: 0.85rem !important;
        color: var(--bon-muted) !important;
        border-color: rgba(215, 180, 145, 0.22) !important;
        background-color: transparent !important;
    }
    [data-testid="stPopover"] > button:hover {
        color: var(--bon-text) !important;
        border-color: rgba(215, 180, 145, 0.4) !important;
        background-color: rgba(215, 180, 145, 0.06) !important;
    }
    [data-testid="stPopoverBody"] .stButton > button:disabled {
        opacity: 0.45;
        cursor: not-allowed;
    }
    [data-testid="stSidebar"] div[class*="st-key-bon_box_ticket"] details[open] div[data-testid="stRadio"] > div[role="radiogroup"] {
        gap: 0.65rem !important;
        padding: 0.15rem 0 0.15rem 0.15rem !important;
        margin: 0 !important;
    }
    /* Command Center: compact Team popover beside engineer dropdown */
    [data-testid="stSidebar"] div.st-key-cc_team_popover > button {
        font-size: 0.8rem !important;
        padding: 0.15rem 0 !important;
        min-height: unset !important;
        width: auto !important;
        color: var(--bon-muted) !important;
        border: none !important;
        background: transparent !important;
        box-shadow: inset 0 -1px 0 rgba(215, 180, 145, 0.35) !important;
    }
    [data-testid="stSidebar"] div.st-key-cc_team_popover > button:hover {
        color: var(--bon-oak) !important;
        background: transparent !important;
        box-shadow: inset 0 -1px 0 var(--bon-oak) !important;
    }
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] {
        max-width: 13.5rem !important;
        min-width: 11rem !important;
        padding: 0.45rem 0.55rem !important;
    }
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] {
        gap: 0.35rem !important;
    }
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] div[class*="st-key-fe_rm_"] .stButton > button,
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] div.st-key-fe_add_btn .stButton > button {
        font-size: 0.95rem !important;
        line-height: 1 !important;
        width: 1.65rem !important;
        min-width: 1.65rem !important;
        max-width: 1.65rem !important;
        height: 1.65rem !important;
        min-height: 1.65rem !important;
        padding: 0 !important;
        color: var(--bon-muted) !important;
        border-color: rgba(215, 180, 145, 0.25) !important;
        white-space: nowrap !important;
    }
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] p.cc-team-handle {
        font-family: var(--bon-font) !important;
        font-size: 0.875rem !important;
        font-weight: 400 !important;
        color: var(--bon-text) !important;
        margin: 0 !important;
        padding: 0.4rem 0 0.4rem 0.1rem !important;
        line-height: 1.25 !important;
    }
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] .stTextInput input {
        font-family: var(--bon-font) !important;
        font-size: 0.875rem !important;
        color: var(--bon-text) !important;
        background-color: var(--bon-card) !important;
        border: 1px solid rgba(215, 180, 145, 0.35) !important;
        border-radius: 8px !important;
    }
    [data-testid="stSidebar"] [data-testid="stPopoverBody"] .stTextInput input::placeholder {
        color: var(--bon-muted) !important;
        opacity: 1 !important;
    }
    [data-testid="stMetric"] {
        background: var(--bon-card);
        padding: 8px 12px;
        border-radius: var(--bon-box-radius);
        border: 1px solid var(--bon-box-border);
    }
    [data-testid="stMetric"]:has([data-testid="stMetricDelta"]) {
        border-color: var(--bon-oak);
    }
    [data-testid="stMetric"] label { color: var(--bon-muted) !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--bon-oak) !important;
    }
    .stMarkdown a { color: var(--bon-oak); }
    [data-testid="stDataFrame"] {
        border: 1px solid var(--bon-box-border);
        border-radius: var(--bon-box-radius);
        overflow: hidden;
        background: var(--bon-card);
    }
    /* Text-style nav radios (sidebar + main) */
    [data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"],
    [data-testid="stMain"] div[data-testid="stRadio"] > div[role="radiogroup"] {
        gap: 1.25rem !important;
        background: transparent !important;
        border: none !important;
        padding: 0 0 0.5rem 0 !important;
        margin-bottom: 0.25rem !important;
    }
    [data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label,
    [data-testid="stMain"] div[data-testid="stRadio"] > div[role="radiogroup"] > label {
        background: transparent !important;
        border: none !important;
        padding: 0.2rem 0 !important;
        margin: 0 !important;
        color: var(--bon-muted) !important;
        font-weight: 500 !important;
        min-height: unset !important;
    }
    [data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:hover,
    [data-testid="stMain"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:hover {
        color: var(--bon-text) !important;
    }
    [data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label[data-checked="true"],
    [data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked),
    [data-testid="stMain"] div[data-testid="stRadio"] > div[role="radiogroup"] > label[data-checked="true"],
    [data-testid="stMain"] div[data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked) {
        color: var(--bon-text) !important;
        font-weight: 600 !important;
        box-shadow: inset 0 -2px 0 var(--bon-oak) !important;
    }
    [data-testid="stSidebar"] div[data-testid="stRadio"] > div[role="radiogroup"] > label > div:first-child,
    [data-testid="stMain"] div[data-testid="stRadio"] > div[role="radiogroup"] > label > div:first-child {
        display: none !important;
    }
    /* Dashboard nav: one row (Tickets | Log | Performance) */
    [data-testid="stMain"] div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] {
        flex-direction: row !important;
        flex-wrap: wrap !important;
        align-items: flex-end !important;
        gap: 1.75rem !important;
        margin-bottom: 0.5rem !important;
    }
    /* Clickable queue metrics (replace second nav row) */
    [data-testid="stMain"] div[class*="st-key-dash_metric_nav_"] .stButton > button,
    [data-testid="stMain"] div[class*="st-key-dash_sc_metric_nav_"] .stButton > button {
        background: var(--bon-card) !important;
        border: 1px solid var(--bon-box-border) !important;
        border-radius: var(--bon-box-radius) !important;
        color: var(--bon-muted) !important;
        font-weight: 500 !important;
        font-size: 0.8rem !important;
        line-height: 1.35 !important;
        white-space: pre-line !important;
        min-height: 4.25rem !important;
        padding: 0.55rem 0.65rem !important;
        text-align: left !important;
    }
    [data-testid="stMain"] div[class*="st-key-dash_metric_nav_"] .stButton > button:not(:disabled):hover,
    [data-testid="stMain"] div[class*="st-key-dash_sc_metric_nav_"] .stButton > button:not(:disabled):hover {
        color: var(--bon-text) !important;
        border-color: rgba(215, 180, 145, 0.5) !important;
        background: rgba(215, 180, 145, 0.08) !important;
    }
    [data-testid="stMain"] div[class*="st-key-dash_metric_nav_"] .stButton > button:disabled,
    [data-testid="stMain"] div[class*="st-key-dash_sc_metric_nav_"] .stButton > button:disabled {
        opacity: 1 !important;
        color: var(--bon-oak) !important;
        border-color: var(--bon-oak) !important;
        font-weight: 600 !important;
        cursor: default !important;
        box-shadow: inset 0 -2px 0 var(--bon-oak) !important;
    }
    /* Desktop: wider sidebar, roomier main nav radios, taller tables */
    @media (min-width: 1100px) {
        [data-testid="stSidebar"] {
            min-width: 19rem !important;
            max-width: 22rem !important;
        }
        [data-testid="stMain"] [data-testid="block-container"] {
            padding-left: 2rem !important;
            padding-right: 2rem !important;
            max-width: 96rem !important;
        }
        [data-testid="stDataFrame"] {
            min-height: 280px;
        }
    }
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
    cfg: dict = {}
    try:
        if "photo_url" in df.columns:
            cfg["photo_url"] = st.column_config.LinkColumn(
                "photo_url",
                help="Click to open the field photo in a new tab.",
                display_text="Open photo",
            )
        if "field_response" in df.columns:
            cfg["field_response"] = st.column_config.TextColumn(
                "Field Response",
                help="Engineer’s reply text. ``@mentions`` here often tag the assigner/coordinator, not who sent the message.",
            )
        if "field_responded_by" in df.columns:
            cfg["field_responded_by"] = st.column_config.TextColumn(
                "Responded by",
                help="Telegram account that **sent** the reply (test phone), when different from assignee. Not ``@`` names inside the note.",
            )
    except Exception:
        return {}
    return cfg


def _normalize_ticket_status_value(raw: object) -> str:
    """Map legacy DB labels to current queue statuses for counts and filters."""
    s = str(raw or "").strip()
    if not s:
        return ""
    return _LEGACY_STATUS_ALIASES.get(s.casefold(), s)


def _normalized_status_series(df: pd.DataFrame) -> pd.Series:
    if df.empty or "status" not in df.columns:
        return pd.Series(dtype=str)
    return df["status"].map(_normalize_ticket_status_value)


def _dashboard_tickets_in_view(
    df_all: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    """Tickets in the sidebar range plus active queue rows (never hide open work)."""
    if df_all.empty:
        return df_all, 0
    in_range = _apply_dash_time_range(
        df_all, range_start=range_start, range_end=range_end
    )
    if "status" not in df_all.columns:
        return in_range, len(in_range)
    norm = _normalized_status_series(df_all)
    active = df_all[norm.isin(_ACTIVE_QUEUE_STATUSES)].copy()
    if in_range.empty:
        return active, 0
    if active.empty:
        return in_range, len(in_range)
    key = "ticket_number" if "ticket_number" in df_all.columns else None
    if key:
        merged = pd.concat([in_range, active], ignore_index=True).drop_duplicates(
            subset=[key], keep="first"
        )
    else:
        merged = pd.concat([in_range, active], ignore_index=True).drop_duplicates()
    return merged, len(in_range)


def _ticket_queue_count_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Boolean masks per queue tab (mutually exclusive on normalized status)."""
    empty = pd.Series(dtype=bool)
    if df.empty:
        return {
            "pending": empty,
            "on_hold": empty,
            "open": empty,
            "investigation": empty,
            "unattended": empty,
            "completed": empty,
            "other": empty,
        }
    status = _normalized_status_series(df)
    pending = status.eq(STATUS_DAILY_TASK)
    on_hold = status.eq(STATUS_ON_HOLD)
    open_m = status.eq("Open")
    investigation = status.eq(STATUS_UNDER_INVESTIGATION)
    unattended = status.eq(STATUS_UNATTENDED)
    completed = status.eq(STATUS_RESOLVED)
    known = pending | on_hold | open_m | investigation | unattended | completed
    other = ~known & status.ne("")
    return {
        "pending": pending,
        "on_hold": on_hold,
        "open": open_m,
        "investigation": investigation,
        "unattended": unattended,
        "completed": completed,
        "other": other,
    }


def _apply_dash_time_range(
    df: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> pd.DataFrame:
    """Keep rows whose latest activity falls in the sidebar **Time range**.

    Uses ``last_assigned_at``, ``responded_at``, ``updated_at``, and
    ``created_at`` (whichever is latest). Matches the From–To / preset
    range — not a rolling "last N days from now" window (that hid valid
    tickets when the preset span did not align with ``now()``).
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
    # Keep rows with no parseable timestamps so a bad/legacy cell does not wipe
    # the whole dashboard (everything became NaT → empty frame).
    mask = ref.isna() | ((ref >= range_start) & (ref <= range_end))
    return df[mask].copy()


def _queue_segment_label(name: str, count: int) -> str:
    return f"{name} ({count})" if count else name


def _queue_segment_base(label: str | None) -> str:
    """Map queue label (with optional count) back to queue name."""
    if not label:
        return STATUS_DAILY_TASK
    if label == "Unavailable" or label.startswith("Unavailable ("):
        return STATUS_ON_HOLD
    if label == "No Answer" or label.startswith("No Answer ("):
        return STATUS_ON_HOLD
    if label == STATUS_DAILY_TASK or label.startswith(f"{STATUS_DAILY_TASK} ("):
        return STATUS_DAILY_TASK
    if label == "Pending" or label.startswith("Pending ("):
        return STATUS_DAILY_TASK
    if label == "Completed" or label.startswith("Completed ("):
        return STATUS_RESOLVED
    for base in (
        STATUS_DAILY_TASK,
        "Open",
        STATUS_ON_HOLD,
        "Under Investigation",
        STATUS_RESOLVED,
        "Unattended",
        "Log",
        "Performance",
    ):
        if label == base or label.startswith(f"{base} ("):
            return base
    return STATUS_DAILY_TASK


def _migrate_legacy_queue_nav() -> None:
    """Map old single segmented control session key to two-level nav."""
    legacy = st.session_state.pop("dash_queue_segmented", None)
    if not legacy:
        return
    base = _queue_segment_base(legacy)
    if base in ("Log", "Performance"):
        st.session_state[_DASH_MAIN_NAV_KEY] = base
    else:
        st.session_state[_DASH_MAIN_NAV_KEY] = "Tickets"
        st.session_state[_DASH_TICKET_QUEUE_KEY] = legacy


def _render_dashboard_header(*, refreshed_at: str) -> None:
    """Desktop top bar: title and last refresh."""
    st.markdown("## NetOps  \nCoverage Eye")
    st.caption(f"Updated **{refreshed_at} {LOCAL_TZ_LABEL}** · change dates in sidebar **Time Range**")


def _apply_pending_dashboard_nav() -> None:
    """Apply metric-click navigation before nav widgets are drawn."""
    pending_main = st.session_state.pop(_DASH_PENDING_MAIN_NAV_KEY, None)
    pending_ticket_queue = st.session_state.pop(_DASH_PENDING_TICKET_QUEUE_KEY, None)
    pending_sales_queue = st.session_state.pop(_DASH_PENDING_SALES_QUEUE_KEY, None)
    if pending_main is not None:
        st.session_state[_DASH_MAIN_NAV_KEY] = pending_main
    if pending_ticket_queue is not None:
        st.session_state[_DASH_TICKET_QUEUE_KEY] = pending_ticket_queue
    if pending_sales_queue is not None:
        st.session_state[_DASH_SALES_QUEUE_KEY] = pending_sales_queue


def _render_clickable_queue_metric(
    col: object,
    *,
    title: str,
    value: int,
    queue_name: str,
    option_label: str,
) -> None:
    """Metric-style control — click to open that ticket queue."""
    main_nav = str(st.session_state.get(_DASH_MAIN_NAV_KEY, "Tickets"))
    q_base = _queue_segment_base(st.session_state.get(_DASH_TICKET_QUEUE_KEY))
    active = main_nav == "Tickets" and q_base == queue_name
    label = f"{title}\n{value:,}"
    with col:
        if st.button(
            label,
            key=f"dash_metric_nav_{queue_name.lower().replace(' ', '_')}",
            type="secondary",
            use_container_width=True,
            disabled=active,
        ):
            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Tickets"
            st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = option_label
            st.rerun()


def _render_assign_day_metrics(
    df_all: pd.DataFrame,
    *,
    df_in_view: pd.DataFrame | None = None,
) -> None:
    """Today's assignment funnel (ops timezone, UTC+5)."""
    if df_all.empty:
        return
    view = df_in_view if df_in_view is not None else df_all
    today = datetime.now(OPS_TZ).date()
    start = _local_date_start(today)
    end = _local_date_end(today)
    assigned_today = 0
    responded_today = 0
    unattended_in_view = 0
    pending_in_view = 0
    if "last_assigned_at" in df_all.columns:
        la = _parse_ts(df_all["last_assigned_at"])
        assigned_today = int(((la >= start) & (la <= end)).sum())
    if "responded_at" in df_all.columns:
        rp = _parse_ts(df_all["responded_at"])
        responded_today = int(((rp >= start) & (rp <= end)).sum())
    if not view.empty and "status" in view.columns:
        masks = _ticket_queue_count_masks(view)
        pending_in_view = int(masks["pending"].sum())
        unattended_in_view = int(masks["unattended"].sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Assigned today", assigned_today)
    c2.metric("Responded today", responded_today)
    c3.metric(f"{STATUS_DAILY_TASK} (in view)", pending_in_view)
    c4.metric("Unattended (in view)", unattended_in_view)
    nudge_h = int(UNATTENDED_NUDGE_HOURS) if UNATTENDED_NUDGE_HOURS == int(UNATTENDED_NUDGE_HOURS) else UNATTENDED_NUDGE_HOURS
    st.caption(
        f"Assign day ends {os.getenv('ASSIGN_DAY_CUTOFF_HOUR', '23')}:{os.getenv('ASSIGN_DAY_CUTOFF_MINUTE', '59').zfill(2)} "
        f"{LOCAL_TZ_LABEL}. Auto nudge after **{nudge_h}h** with no field reply; "
        "no same-day response → **Unattended**."
    )


def _render_queue_summary_metrics(
    *,
    total_pending: int,
    total_on_hold: int,
    total_open: int,
    total_investigation: int,
    total_unattended: int,
    total_completed: int,
    pending_label: str,
    on_hold_label: str,
    open_label: str,
    investigation_label: str,
    unattended_label: str,
    completed_label: str,
    total_in_view: int = 0,
    total_in_tabs: int = 0,
    total_other: int = 0,
) -> None:
    """Counts — click a queue to switch view."""
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    _render_clickable_queue_metric(
        c1,
        title=STATUS_DAILY_TASK,
        value=total_pending,
        queue_name=STATUS_DAILY_TASK,
        option_label=pending_label,
    )
    _render_clickable_queue_metric(
        c2,
        title="Needs Review",
        value=total_open,
        queue_name="Open",
        option_label=open_label,
    )
    _render_clickable_queue_metric(
        c3,
        title="On Hold",
        value=total_on_hold,
        queue_name=STATUS_ON_HOLD,
        option_label=on_hold_label,
    )
    _render_clickable_queue_metric(
        c4,
        title="Investigation",
        value=total_investigation,
        queue_name=STATUS_UNDER_INVESTIGATION,
        option_label=investigation_label,
    )
    _render_clickable_queue_metric(
        c5,
        title=STATUS_RESOLVED,
        value=total_completed,
        queue_name=STATUS_RESOLVED,
        option_label=completed_label,
    )
    _render_clickable_queue_metric(
        c6,
        title="Unattended",
        value=total_unattended,
        queue_name="Unattended",
        option_label=unattended_label,
    )
    if total_in_view > 0:
        st.caption(
            f"**{total_in_tabs}** ticket(s) across queue tabs "
            f"(**{total_in_view}** in view"
            + (
                f"; **{total_other}** with an unrecognized status — widen **Time range** "
                "or fix status in Supabase"
                if total_other
                else ""
            )
            + "). Active **Daily Task / Needs Review / On Hold / Investigation** rows stay "
            "visible even outside the date range."
        )


_TICKET_QUEUE_TABLE_COLS: tuple[str, ...] = (
    "ticket_number",
    "assigned_to",
    "field_responded_by",
    "task_category",
    "field_response",
    "photo_url",
    "responded_at",
    "last_assigned_at",
)


def _sync_dashboard_nav_state(
    *,
    total_pending: int,
    total_on_hold: int,
    total_open: int,
    total_investigation: int,
    total_unattended: int,
    total_completed: int,
) -> tuple[str, str, str, str, str, str]:
    """Keep queue session keys valid; return option labels for metrics."""
    _migrate_legacy_queue_nav()

    if st.session_state.get(_DASH_MAIN_NAV_KEY) not in _DASH_MAIN_NAV_OPTIONS:
        st.session_state[_DASH_MAIN_NAV_KEY] = "Tickets"

    pending_label = _queue_segment_label(STATUS_DAILY_TASK, total_pending)
    on_hold_label = _queue_segment_label(STATUS_ON_HOLD, total_on_hold)
    open_label = _queue_segment_label("Open", total_open)
    investigation_label = _queue_segment_label(
        STATUS_UNDER_INVESTIGATION, total_investigation
    )
    unattended_label = _queue_segment_label("Unattended", total_unattended)
    completed_label = _queue_segment_label(STATUS_RESOLVED, total_completed)
    ticket_options = (
        pending_label,
        open_label,
        on_hold_label,
        investigation_label,
        completed_label,
        unattended_label,
    )

    cur_q = st.session_state.get(_DASH_TICKET_QUEUE_KEY)
    if cur_q and _queue_segment_base(cur_q) == STATUS_RESOLVED:
        st.session_state[_DASH_TICKET_QUEUE_KEY] = completed_label

    prev_open = int(st.session_state.get("_dash_prev_open_count", 0))
    if total_open > prev_open:
        st.session_state[_DASH_MAIN_NAV_KEY] = "Tickets"
        st.session_state[_DASH_TICKET_QUEUE_KEY] = open_label
    elif st.session_state.get(_DASH_TICKET_QUEUE_KEY) not in ticket_options:
        st.session_state[_DASH_TICKET_QUEUE_KEY] = (
            open_label if total_open > 0 else pending_label
        )
    st.session_state["_dash_prev_open_count"] = total_open
    return (
        pending_label,
        open_label,
        on_hold_label,
        investigation_label,
        completed_label,
        unattended_label,
    )


def _render_main_navigation() -> str:
    """Top row: Tickets | Sales Cases | Log | Performance."""
    legacy_nav = st.session_state.get(_DASH_MAIN_NAV_KEY)
    if legacy_nav == "Sales cases":
        st.session_state[_DASH_MAIN_NAV_KEY] = "Sales Cases"
    return str(
        st.radio(
            "View",
            options=list(_DASH_MAIN_NAV_OPTIONS),
            horizontal=True,
            key=_DASH_MAIN_NAV_KEY,
            label_visibility="collapsed",
        )
    )


def _sort_tickets_newest_first(df: pd.DataFrame) -> pd.DataFrame:
    """Sort ticket rows for display (newest activity first)."""
    if df.empty:
        return df
    sort_col = next(
        (c for c in ("responded_at", "last_assigned_at", "updated_at", "created_at") if c in df.columns),
        None,
    )
    if not sort_col:
        return df
    out = df.copy()
    out["_sort"] = _parse_ts(out[sort_col])
    return out.sort_values("_sort", ascending=False, na_position="last").drop(
        columns=["_sort"], errors="ignore"
    )


def _ticket_queue_view(df: pd.DataFrame, cols: tuple[str, ...] = _TICKET_QUEUE_TABLE_COLS) -> pd.DataFrame:
    """Subset and format ticket columns for queue tables."""
    sorted_df = _sort_tickets_newest_first(df)
    show = [c for c in cols if c in sorted_df.columns]
    if not show:
        return _format_local(sorted_df)
    return _format_local(sorted_df[show].copy())


def _sc_set_sales_flash(message: str, *, level: str = "success") -> None:
    st.session_state[_SC_SALES_FLASH_KEY] = message
    st.session_state[_SC_SALES_FLASH_LEVEL_KEY] = level


def _sc_show_sales_flash() -> None:
    msg = st.session_state.pop(_SC_SALES_FLASH_KEY, None)
    if not msg:
        return
    lev = str(st.session_state.pop(_SC_SALES_FLASH_LEVEL_KEY, "success") or "success")
    if lev == "error":
        st.error(msg)
    elif lev == "warning":
        st.warning(msg)
    else:
        st.success(msg)


def _fetch_sales_cases_df() -> pd.DataFrame | None:
    """Return sales-case rows, empty DataFrame if none, or ``None`` if table missing."""
    client = _get_supabase_client()
    try:
        res = (
            client.table(SALES_CASES_TABLE)
            .select("*")
            .order("updated_at", desc=True)
            .execute()
        )
    except Exception as exc:
        err = str(exc).lower()
        if (
            "does not exist" in err
            or "schema cache" in err
            or "42p01" in err
            or "could not find the table" in err
        ):
            return None
        raise
    rows = res.data or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _sales_cases_update_row(row_id: str, payload: dict) -> None:
    client = _get_supabase_client()
    body = {**payload, "updated_at": _cc_utc_now_iso()}
    client.table(SALES_CASES_TABLE).update(body).eq("id", row_id).execute()


def _sales_cases_insert_row(payload: dict) -> None:
    client = _get_supabase_client()
    row = {**payload, "updated_at": _cc_utc_now_iso(), "created_at": _cc_utc_now_iso()}
    client.table(SALES_CASES_TABLE).insert(row).execute()


def _sc_filter_sales_df(df: pd.DataFrame, statuses: tuple[str, ...]) -> pd.DataFrame:
    if df.empty or "status" not in df.columns:
        return df.iloc[0:0].copy()
    effective = df["status"].astype(str).str.strip().map(_sc_effective_status)
    return df[effective.isin(statuses)].copy()


def _render_sc_selected_case_card(row: pd.Series) -> None:
    """Hero summary for the one selected case."""
    cref = html.escape(str(row.get("case_ref") or "—"))
    account = html.escape(str(row.get("account_name") or "—"))
    owner = html.escape(str(row.get("sales_owner") or "—"))
    status = html.escape(_sc_effective_status(row.get("status")))
    priority = html.escape(str(row.get("sales_priority") or "—"))
    region = html.escape(str(row.get("account_region") or "—"))
    category = html.escape(str(row.get("sales_category") or "—"))
    assignee = html.escape(str(row.get("assigned_to") or "").strip())
    engineer_pill = (
        f'<span class="sc-meta-pill">Engineer · {assignee}</span>' if assignee else ""
    )
    st.markdown(
        f"""
<div class="sc-case-hero">
  <div class="sc-case-hero__eyebrow">Selected case</div>
  <div class="sc-case-hero__ticket">{cref}</div>
  <div class="sc-case-hero__account">{account}</div>
  <div class="sc-case-hero__pills">
    <span class="sc-meta-pill sc-meta-pill--accent">{status}</span>
    <span class="sc-meta-pill">{owner}</span>
    <span class="sc-meta-pill">{priority}</span>
    <span class="sc-meta-pill">{region}</span>
    <span class="sc-meta-pill">{category}</span>
    {engineer_pill}
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


def _sc_sales_case_display_cols() -> tuple[str, ...]:
    return (
        "case_ref",
        "account_name",
        "sales_owner",
        "sales_priority",
        "account_region",
        "sales_category",
        "status",
        "admin_owner",
        "additional_info",
        "close_note",
        "dispatch_type",
        "dispatch_region",
        "assigned_to",
        "field_task_category",
        "updated_at",
    )


def _sc_rename_sales_case_columns_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Friendly column headers in tables (DB columns unchanged)."""
    mapping = {
        "case_ref": "Ticket Number",
        "account_name": "Resort Name / Company Name",
        "additional_info": "Case note",
        "close_note": "Closing note",
    }
    return df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})


def _sc_sync_case_edit_widgets(
    *,
    row_id: str,
    row: pd.Series,
    sales_cats: list[str],
) -> None:
    """Load selected case into edit widgets (Streamlit keeps stale keys otherwise)."""
    if st.session_state.get("sc_edit_synced_id") == row_id:
        return
    st.session_state["sc_edit_synced_id"] = row_id
    reg = str(row.get("account_region") or "").strip()
    st.session_state["sc_edit_region"] = (
        reg if reg in SALES_REGION_CODES else SALES_REGION_CODES[0]
    )
    scat = str(row.get("sales_category") or "").strip()
    cat_opts = list(sales_cats)
    if scat and scat not in cat_opts:
        cat_opts = [scat, *cat_opts]
    st.session_state["sc_edit_scat"] = scat if scat in cat_opts else cat_opts[0]
    st.session_state["sc_edit_admin_notes"] = str(row.get("additional_info") or "")


_SC_QUEUE_TABLE_COLS: tuple[str, ...] = (
    "case_ref",
    "account_name",
    "sales_owner",
    "sales_priority",
    "account_region",
    "sales_category",
    "additional_info",
    "assigned_to",
    "dispatch_region",
    "field_task_category",
    "updated_at",
)


def _sc_queue_segment_base(label: str | None) -> str:
    """Map sales queue label (with optional count) back to queue name."""
    if not label:
        return SC_STATUS_SALES_TICKET
    if label == "Residential" or label.startswith("Residential ("):
        return SC_STATUS_SALES_TICKET
    if label == "Regional" or label.startswith("Regional ("):
        return SC_STATUS_INVESTIGATION
    aliases: dict[str, str] = {}
    for base in (
        SC_STATUS_SALES_TICKET,
        SC_STATUS_INVESTIGATION,
        SC_STATUS_DESIGN,
        SC_STATUS_RESOLVED,
    ):
        if label == base or label.startswith(f"{base} ("):
            return base
    for short, base in aliases.items():
        if label == short or label.startswith(f"{short} ("):
            return base
    return SC_STATUS_SALES_TICKET


def _sc_case_selection_session_key(key_prefix: str) -> str:
    return f"{key_prefix}_selected_sales_cases"


def _sc_case_search_session_key(key_prefix: str) -> str:
    return f"{key_prefix}_case_search"


def _sc_case_options_for_admin(df: pd.DataFrame) -> list[str]:
    if df.empty or "case_ref" not in df.columns:
        return []
    refs = df["case_ref"].fillna("").astype(str).str.strip()
    return sorted({r for r in refs.tolist() if r})


def _get_selected_queue_sales_cases(key_prefix: str, options: list[str]) -> list[str]:
    allowed = set(options)
    raw = st.session_state.get(_sc_case_selection_session_key(key_prefix), [])
    if not isinstance(raw, list):
        return []
    return [str(t) for t in raw if str(t) in allowed]


def _require_selected_sales_cases(
    *,
    key_prefix: str,
    options: list[str],
    exactly_one: bool = False,
) -> list[str] | None:
    selected = _get_selected_queue_sales_cases(key_prefix, options)
    if not selected:
        st.error(
            "Tick **Select** on at least one case in the table above, "
            "then click the action."
        )
        return None
    if exactly_one and len(selected) != 1:
        st.error(
            f"Select **exactly one** case for this action "
            f"({len(selected)} selected now)."
        )
        return None
    return selected


def _picked_sales_case_from_selection(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    case_options: list[str],
) -> tuple[str | None, str | None, pd.Series | None]:
    selected = _require_selected_sales_cases(
        key_prefix=key_prefix, options=case_options, exactly_one=True
    )
    if not selected:
        return None, None, None
    cref = selected[0]
    sub = df[df["case_ref"].fillna("").astype(str) == cref]
    if sub.empty:
        st.warning("Case not found — refresh the page.")
        return None, None, None
    r0 = sub.iloc[0]
    return str(r0.get("id") or ""), cref, r0


def _sc_sales_queue_view(
    df: pd.DataFrame,
    *,
    cols: tuple[str, ...] = _SC_QUEUE_TABLE_COLS,
) -> pd.DataFrame:
    sorted_df = _sort_tickets_newest_first(df)
    show = [c for c in cols if c in sorted_df.columns]
    if not show:
        return _format_local(sorted_df)
    out = sorted_df[show].copy()
    if "status" in out.columns:
        out["status"] = out["status"].map(_sc_effective_status)
    return _format_local(_sc_rename_sales_case_columns_for_display(out))


def _render_selectable_sales_case_table(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    cols: tuple[str, ...] = _SC_QUEUE_TABLE_COLS,
) -> list[str]:
    """Table with a **Select** checkbox per row; returns chosen case refs."""
    options = _sc_case_options_for_admin(df)
    if not options:
        st.caption("No cases in this queue.")
        return []

    search_q = st.text_input(
        "Search Cases",
        placeholder="Ticket #, resort, sales owner, or category…",
        key=_sc_case_search_session_key(key_prefix),
    )
    filtered = _filter_sales_cases_search(df, search_q)
    if (search_q or "").strip() and len(filtered) < len(df):
        st.caption(f"Showing **{len(filtered)}** of **{len(df)}** cases.")
    if filtered.empty and (search_q or "").strip():
        st.info("No cases match that search.")
        return []

    options = _sc_case_options_for_admin(filtered)
    view = _sc_sales_queue_view(filtered, cols=cols)
    sel_key = _sc_case_selection_session_key(key_prefix)
    if sel_key not in st.session_state:
        st.session_state[sel_key] = []

    ticket_col = "Ticket Number" if "Ticket Number" in view.columns else "case_ref"
    if ticket_col not in view.columns:
        st.dataframe(view, use_container_width=True, hide_index=True)
        return []

    prev = set(_get_selected_queue_sales_cases(key_prefix, options))
    table = view.copy()
    table.insert(0, "Select", table[ticket_col].astype(str).isin(prev))

    disabled_cols = [c for c in table.columns if c != "Select"]
    col_cfg = {
        "Select": st.column_config.CheckboxColumn(
            "Select",
            help="Tick, then use the action buttons above",
            default=False,
        ),
        **_dataframe_column_config(view),
    }
    edited = st.data_editor(
        table,
        hide_index=True,
        use_container_width=True,
        key=f"{key_prefix}_case_select_editor",
        column_config=col_cfg,
        disabled=disabled_cols,
    )

    selected = [
        str(t)
        for t in edited.loc[edited["Select"] == True, ticket_col].astype(str).tolist()
        if str(t) in options
    ]
    st.session_state[sel_key] = selected
    if selected:
        shown = ", ".join(selected[:6])
        extra = f" (+{len(selected) - 6} more)" if len(selected) > 6 else ""
        st.caption(f"**{len(selected)}** selected: {shown}{extra}")
    else:
        st.caption("Select one row below to open the **work panel** (details & next step).")
    return selected


def _sc_clear_work_panel_tabs(key_prefix: str) -> None:
    """Reset per-queue selection helpers when clearing the table."""
    st.session_state.pop("sc_edit_synced_id", None)
    st.session_state.pop("sc_note_synced_id", None)


def _render_sales_case_toolbar(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    caption: str | None = None,
    status_actions: tuple[tuple[str, str], ...] = (),
    op: str = "",
) -> None:
    """Select all / Clear / bulk status actions for the queue table."""
    options = _sc_case_options_for_admin(df)
    if not options:
        return
    if caption:
        st.caption(caption)
    sel_key = _sc_case_selection_session_key(key_prefix)
    status_labels = [label for label, _ in status_actions] if op else []
    slot_count = 2
    if status_labels:
        slot_count += 2 if len(status_labels) > 1 else 1
    with st.container(key=f"{key_prefix}_sc_toolbar"):
        cols = st.columns(slot_count, gap="small", vertical_alignment="bottom")
        idx = 0

        with cols[idx]:
            if st.button(
                "Select all",
                key=f"{key_prefix}_sel_all",
                use_container_width=True,
            ):
                st.session_state[sel_key] = list(options)
                st.rerun()
        idx += 1

        with cols[idx]:
            if st.button(
                "Clear",
                key=f"{key_prefix}_sel_clear",
                use_container_width=True,
            ):
                st.session_state[sel_key] = []
                _sc_clear_work_panel_tabs(key_prefix)
                st.rerun()
        idx += 1

        if status_labels:
            label_to_target = {label: tgt for label, tgt in status_actions}
            if len(status_labels) == 1:
                only_label = status_labels[0]
                only_target = label_to_target[only_label]
                with cols[idx]:
                    if st.button(
                        only_label,
                        key=f"{key_prefix}_sc_apply",
                        type="primary",
                        use_container_width=True,
                    ):
                        _sc_advance_selected_case(
                            df,
                            key_prefix=key_prefix,
                            case_options=options,
                            target_status=only_target,
                            op=op,
                            success_flash=f"Case moved to **{only_target}**.",
                        )
                idx += 1
            else:
                with cols[idx]:
                    st.selectbox(
                        "Action",
                        options=status_labels,
                        key=f"{key_prefix}_sc_action_sel",
                        label_visibility="collapsed",
                    )
                idx += 1
                with cols[idx]:
                    if st.button(
                        "Apply",
                        key=f"{key_prefix}_sc_apply",
                        type="primary",
                        use_container_width=True,
                    ):
                        choice = str(
                            st.session_state.get(f"{key_prefix}_sc_action_sel", "")
                        )
                        target = label_to_target.get(choice)
                        if target:
                            _sc_advance_selected_case(
                                df,
                                key_prefix=key_prefix,
                                case_options=options,
                                target_status=target,
                                op=op,
                                success_flash=f"Case moved to **{target}**.",
                            )
                idx += 1


def _render_clickable_sales_queue_metric(
    col: object,
    *,
    title: str,
    value: int,
    queue_name: str,
    option_label: str,
) -> None:
    main_nav = str(st.session_state.get(_DASH_MAIN_NAV_KEY, "Tickets"))
    q_base = _sc_queue_segment_base(st.session_state.get(_DASH_SALES_QUEUE_KEY))
    active = main_nav == "Sales Cases" and q_base == queue_name
    label = f"{title}\n{value:,}"
    with col:
        if st.button(
            label,
            key=f"dash_sc_metric_nav_{queue_name.lower().replace(' ', '_')}",
            type="secondary",
            use_container_width=True,
            disabled=active,
        ):
            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Sales Cases"
            st.session_state[_DASH_PENDING_SALES_QUEUE_KEY] = option_label
            st.rerun()


def _render_sales_queue_summary_metrics(
    *,
    total_sales_ticket: int,
    total_investigation: int,
    total_design: int,
    total_resolved: int,
    sales_ticket_label: str,
    investigation_label: str,
    design_label: str,
    resolved_label: str,
) -> None:
    c1, c2, c3, c4 = st.columns(4)
    _render_clickable_sales_queue_metric(
        c1,
        title="Sales Ticket",
        value=total_sales_ticket,
        queue_name=SC_STATUS_SALES_TICKET,
        option_label=sales_ticket_label,
    )
    _render_clickable_sales_queue_metric(
        c2,
        title="Investigation",
        value=total_investigation,
        queue_name=SC_STATUS_INVESTIGATION,
        option_label=investigation_label,
    )
    _render_clickable_sales_queue_metric(
        c3,
        title="Design",
        value=total_design,
        queue_name=SC_STATUS_DESIGN,
        option_label=design_label,
    )
    _render_clickable_sales_queue_metric(
        c4,
        title="Resolved",
        value=total_resolved,
        queue_name=SC_STATUS_RESOLVED,
        option_label=resolved_label,
    )


def _sync_sales_dashboard_nav_state(
    *,
    total_sales_ticket: int,
    total_investigation: int,
    total_design: int,
    total_resolved: int,
) -> tuple[str, str, str, str]:
    sales_ticket_label = _queue_segment_label(SC_STATUS_SALES_TICKET, total_sales_ticket)
    investigation_label = _queue_segment_label(SC_STATUS_INVESTIGATION, total_investigation)
    design_label = _queue_segment_label(SC_STATUS_DESIGN, total_design)
    resolved_label = _queue_segment_label(SC_STATUS_RESOLVED, total_resolved)
    sales_options = (
        sales_ticket_label,
        investigation_label,
        design_label,
        resolved_label,
    )
    if st.session_state.get(_DASH_MAIN_NAV_KEY) == "Sales Cases":
        cur = st.session_state.get(_DASH_SALES_QUEUE_KEY)
        if cur not in sales_options:
            st.session_state[_DASH_SALES_QUEUE_KEY] = (
                sales_ticket_label if total_sales_ticket > 0 else investigation_label
            )
    return (
        sales_ticket_label,
        investigation_label,
        design_label,
        resolved_label,
    )


def _render_sales_case_work_panel(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    case_options: list[str],
    op: str,
    sales_cats: list[str],
    field_cats: list[str],
    fe_names: list[str],
    fe_missing: bool,
) -> None:
    """Actions for exactly one selected case (edit, dispatch, resolve, assign)."""
    row_id, _cref, r0 = _picked_sales_case_from_selection(
        df, key_prefix=key_prefix, case_options=case_options
    )
    if not row_id or r0 is None:
        return

    cur_status = _sc_effective_status(r0.get("status"))
    _sc_sync_case_edit_widgets(row_id=row_id, row=r0, sales_cats=sales_cats)
    _sc_sync_case_note_widgets(row_id=row_id, row=r0)
    edit_cat_opts = list(sales_cats)
    cur_scat = str(r0.get("sales_category") or "").strip()
    if cur_scat and cur_scat not in edit_cat_opts:
        edit_cat_opts = [cur_scat, *edit_cat_opts]

    status_actions = _sc_status_actions_for_case(cur_status)
    show_site_visit = cur_status == SC_STATUS_REGIONAL
    tab_names = ["Case Details"]
    if status_actions and op:
        tab_names.append("Next Step")
    if show_site_visit:
        tab_names.append("Site Visit")

    _render_sc_selected_case_card(r0)
    tabs = st.tabs(tab_names)

    tab_idx = 0
    with tabs[tab_idx]:
        tab_idx += 1
        with st.container(border=True, key=f"{key_prefix}_sc_details_box"):
            st.markdown("**Case Details**")
            st.caption("Region, category, and notes — does not change queue status.")
            with st.form(f"sc_edit_case_form_{key_prefix}", clear_on_submit=False):
                e1, e2 = st.columns(2)
                with e1:
                    st.selectbox(
                        "Region Team",
                        options=list(SALES_REGION_CODES),
                        key="sc_edit_region",
                    )
                with e2:
                    st.selectbox(
                        "Sales Category (Intent)",
                        options=edit_cat_opts,
                        key="sc_edit_scat",
                    )
                st.text_area(
                    "Case Note (Internal)",
                    key="sc_edit_admin_notes",
                    height=140,
                    placeholder="What the team needs to know while working this case…",
                )
                save_edit = st.form_submit_button(
                    "Save Details",
                    type="primary",
                    use_container_width=True,
                )
        if save_edit:
            notes = str(st.session_state.get("sc_edit_admin_notes", "")).strip() or None
            try:
                _sales_cases_update_row(
                    row_id,
                    {
                        "account_region": str(
                            st.session_state.get("sc_edit_region", "")
                        ).strip(),
                        "sales_category": str(
                            st.session_state.get("sc_edit_scat", "")
                        ).strip(),
                        "additional_info": notes,
                    },
                )
                _get_supabase_client.clear()
                _sc_set_sales_flash("Case details saved.")
                st.rerun()
            except Exception as exc:
                st.error(f"Could not save: {exc}")

    if status_actions and op:
        with tabs[tab_idx]:
            tab_idx += 1
            with st.container(border=True, key=f"{key_prefix}_sc_next_box"):
                st.markdown("**Next Step**")
                st.caption("Move this case to another queue.")
                status_labels = [label for label, _ in status_actions]
                label_to_target = {label: tgt for label, tgt in status_actions}
                st.selectbox(
                    "Move To",
                    options=status_labels,
                    key=f"{key_prefix}_panel_sc_action_sel",
                )
                st.text_area(
                    "Comment (Optional)",
                    key="sc_action_comment",
                    height=88,
                    placeholder="Optional note saved with the status change",
                )
                if st.button(
                    "Apply Move",
                    key=f"{key_prefix}_panel_sc_apply",
                    type="primary",
                    use_container_width=True,
                ):
                    choice = str(
                        st.session_state.get(f"{key_prefix}_panel_sc_action_sel", "")
                    )
                    target = label_to_target.get(choice)
                    if not target:
                        return
                    err = _sc_apply_status_advance(
                        row_id,
                        r0=r0,
                        target_status=target,
                        op=op,
                        action_comment=str(
                            st.session_state.get("sc_action_comment", "")
                        ).strip()
                        or None,
                    )
                    if err:
                        st.warning(err)
                    else:
                        _get_supabase_client.clear()
                        _sc_set_sales_flash(f"Case moved to **{target}**.")
                        st.rerun()

    if show_site_visit:
        with tabs[tab_idx]:
            region_label = str(r0.get("dispatch_region") or "—")
            cur_assignee = str(r0.get("assigned_to") or "").strip()
            with st.container(border=True, key=f"{key_prefix}_sc_site_box"):
                st.markdown("**Site Visit**")
                st.caption(f"**{region_label}** — assign the visiting engineer.")
                if cur_assignee:
                    st.markdown(f"Current: **{cur_assignee}**")
                with st.form(
                    f"sc_region_assign_engineer_form_{key_prefix}",
                    clear_on_submit=False,
                ):
                    if fe_names and not fe_missing:
                        st.selectbox(
                            "Engineer",
                            options=[f"@{n}" for n in fe_names],
                            key="sc_region_assign_fe",
                        )
                    else:
                        st.text_input(
                            "Engineer @username",
                            key="sc_region_assign_fe_manual",
                            placeholder="username",
                        )
                    st.checkbox(
                        "Post Assignment to Field Telegram",
                        value=False,
                        key="sc_region_assign_post_tg",
                    )
                    assign_submitted = st.form_submit_button(
                        "Save Engineer Assignment",
                        type="primary",
                        use_container_width=True,
                    )
                if assign_submitted:
                    raw_h = (
                        str(st.session_state.get("sc_region_assign_fe", "")).strip()
                        if fe_names and not fe_missing
                        else str(
                            st.session_state.get("sc_region_assign_fe_manual", "")
                        ).strip()
                    )
                    try:
                        handle = _cc_normalize_handle(raw_h)
                    except ValueError as ve:
                        st.error(str(ve))
                    else:
                        patch = {"assigned_to": handle}
                        post_tg = bool(st.session_state.get("sc_region_assign_post_tg"))
                        tg_ok = False
                        if post_tg:
                            token = (
                                _read_setting("TG_BOT_TOKEN").strip()
                                or _read_setting("TELEGRAM_BOT_TOKEN").strip()
                                or _read_setting("TELEGRAM_TOKEN").strip()
                            )
                            chat_raw = _read_telegram_group_chat_raw()
                            chat_id, _w = _parse_telegram_group_chat_id(chat_raw)
                            fcat = str(r0.get("field_task_category") or "").strip()
                            if not fcat:
                                st.warning(
                                    "Set **Field task category** on dispatch first."
                                )
                            elif not token or chat_id is None:
                                st.warning(
                                    "Engineer saved. Telegram skipped — set token + group id."
                                )
                            else:
                                cref = str(r0.get("case_ref") or "").strip()
                                try:
                                    asyncio.run(
                                        notify_telegram_group(
                                            handle.lstrip("@"),
                                            cref or row_id[:8],
                                            fcat,
                                            additional_info=str(
                                                r0.get("description") or ""
                                            )
                                            or None,
                                            assigned_by=op,
                                            api_id=_read_setting("TG_API_ID")
                                            or _read_setting("TELEGRAM_API_ID")
                                            or None,
                                            api_hash=_read_setting("TG_API_HASH")
                                            or _read_setting("TELEGRAM_API_HASH")
                                            or None,
                                            bot_token=token or None,
                                            group_id=chat_id,
                                        )
                                    )
                                    tg_ok = True
                                except Exception as tg_exc:
                                    st.warning(
                                        f"Engineer saved; Telegram post failed: {tg_exc}"
                                    )
                        try:
                            _sales_cases_update_row(row_id, patch)
                            _get_supabase_client.clear()
                            msg = f"Engineer **{handle}** assigned for **{region_label}**."
                            if post_tg and tg_ok:
                                msg += " Telegram posted."
                            _sc_set_sales_flash(msg)
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Could not assign engineer: {exc}")

def _render_sales_queue_segment(
    df: pd.DataFrame,
    *,
    queue_status: str,
    queue_statuses: tuple[str, ...] | None = None,
    key_prefix: str,
    title: str,
    empty_msg: str,
    caption: str,
    table_cols: tuple[str, ...] = _SC_QUEUE_TABLE_COLS,
    toolbar_caption: str | None = None,
    show_work_panel: bool = True,
    op: str = "",
    sales_cats: list[str] | None = None,
    field_cats: list[str] | None = None,
    fe_names: list[str] | None = None,
    fe_missing: bool = False,
) -> None:
    st.markdown(f"##### {title}")
    statuses = queue_statuses if queue_statuses else (queue_status,)
    sub = _sc_filter_sales_df(df, statuses)
    if sub.empty:
        st.info(empty_msg)
        return
    if caption:
        st.caption(caption)
    options = _sc_case_options_for_admin(sub)
    status_actions = _sc_status_actions_for_queue(queue_status)
    _render_sales_case_toolbar(
        sub,
        key_prefix=key_prefix,
        caption=toolbar_caption,
        status_actions=status_actions if op else (),
        op=op,
    )
    _render_selectable_sales_case_table(sub, key_prefix=key_prefix, cols=table_cols)
    if show_work_panel and queue_status != SC_STATUS_RESOLVED:
        selected = _get_selected_queue_sales_cases(key_prefix, options)
        if len(selected) == 1:
            with st.container(border=True, key=f"{key_prefix}_work_panel"):
                _render_sales_case_work_panel(
                    sub,
                    key_prefix=key_prefix,
                    case_options=options,
                    op=op,
                    sales_cats=sales_cats or list(DEFAULT_SALES_CASE_CATEGORIES),
                    field_cats=field_cats or list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES),
                    fe_names=fe_names or [],
                    fe_missing=fe_missing,
                )
        elif len(selected) > 1:
            st.warning("Select **exactly one** case to open the work panel.")


def _render_sales_cases_dashboard() -> None:
    """Separate UI for sales-priority cases (not field ``tickets_active``)."""
    _sc_show_sales_flash()
    st.markdown("##### Sales Cases")
    st.caption(
        "New cases: sidebar **SALES**. Open a queue with the metrics above."
    )

    try:
        df = _fetch_sales_cases_df()
    except Exception as exc:
        st.error(f"Could not load sales cases: {exc}")
        return

    if df is None:
        st.warning(
            f"The `{SALES_CASES_TABLE}` table is missing. Apply "
            f"``supabase/migrations/20260620_dashboard_sales_cases.sql`` in the Supabase SQL "
            "editor (includes RLS for the anon key), then refresh this app."
        )
        return

    op = _session_operator_id()
    if not op:
        st.error("Sign in again — **Operator ID** is required.")
        return

    cat_names, _cat_miss = _try_fetch_task_categories()
    field_cats = (
        cat_names if cat_names else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    )
    sales_cats = list(DEFAULT_SALES_CASE_CATEGORIES)
    fe_names, fe_missing = _try_fetch_field_engineer_usernames()

    total_sales_ticket = len(_sc_filter_sales_df(df, (SC_STATUS_SALES_TICKET,)))
    total_investigation = len(
        _sc_filter_sales_df(df, _SC_INVESTIGATION_QUEUE_STATUSES)
    )
    total_design = len(_sc_filter_sales_df(df, (SC_STATUS_DESIGN,)))
    total_resolved = len(_sc_filter_sales_df(df, (SC_STATUS_RESOLVED,)))
    (
        sales_ticket_label,
        investigation_label,
        design_label,
        resolved_label,
    ) = _sync_sales_dashboard_nav_state(
        total_sales_ticket=total_sales_ticket,
        total_investigation=total_investigation,
        total_design=total_design,
        total_resolved=total_resolved,
    )
    _render_sales_queue_summary_metrics(
        total_sales_ticket=total_sales_ticket,
        total_investigation=total_investigation,
        total_design=total_design,
        total_resolved=total_resolved,
        sales_ticket_label=sales_ticket_label,
        investigation_label=investigation_label,
        design_label=design_label,
        resolved_label=resolved_label,
    )

    queue_view = _sc_queue_segment_base(st.session_state.get(_DASH_SALES_QUEUE_KEY))
    work_kw = dict(
        op=op,
        sales_cats=sales_cats,
        field_cats=field_cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
    )

    if queue_view == SC_STATUS_SALES_TICKET:
        _render_sales_queue_segment(
            df,
            queue_status=SC_STATUS_SALES_TICKET,
            key_prefix="sc_sales_ticket",
            title="Sales Ticket — New Intake",
            empty_msg="No cases in **Sales Ticket**. Create one in sidebar **SALES**.",
            caption=None,
            toolbar_caption=None,
            **work_kw,
        )
    elif queue_view == SC_STATUS_INVESTIGATION:
        _render_sales_queue_segment(
            df,
            queue_status=SC_STATUS_INVESTIGATION,
            queue_statuses=_SC_INVESTIGATION_QUEUE_STATUSES,
            key_prefix="sc_investigation",
            title="Investigation — Admin Review & Site Visit",
            empty_msg="No cases in **Investigation**.",
            caption=None,
            table_cols=_SC_QUEUE_TABLE_COLS + ("dispatch_type",),
            **work_kw,
        )
    elif queue_view == SC_STATUS_DESIGN:
        _render_sales_queue_segment(
            df,
            queue_status=SC_STATUS_DESIGN,
            key_prefix="sc_design",
            title="Design — Post-Visit Solution",
            empty_msg="No cases in **Design**.",
            caption=None,
            **work_kw,
        )
    elif queue_view == SC_STATUS_RESOLVED:
        _render_sales_queue_segment(
            df,
            queue_status=SC_STATUS_RESOLVED,
            key_prefix="sc_resolved",
            title="Resolved",
            empty_msg="No **Resolved** Sales Cases yet.",
            caption=None,
            table_cols=_SC_QUEUE_TABLE_COLS + ("close_note", "status"),
            show_work_panel=False,
            **work_kw,
        )


def _render_dashboard(
    *,
    lookback_days: int,
) -> None:
    day_word = "day" if lookback_days == 1 else "days"
    refreshed_at = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

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
        st.warning(
            "No ticket rows returned (empty ``tickets_active`` or connection issue). "
            "Queue counts are zero — **Performance** and **Log** still use attendance history."
        )
        df = pd.DataFrame({"status": pd.Series(dtype=str)})
    elif "status" not in df_all.columns:
        st.error(f"The `{TICKETS_TABLE}` table has no `status` column.")
        return
    else:
        range_start, range_end = _get_dash_range()
        df, in_range_count = _dashboard_tickets_in_view(
            df_all, range_start=range_start, range_end=range_end
        )
        range_hint = _format_dash_range_caption() or f"the last {lookback_days} day(s)"
        active_extra = max(0, len(df) - in_range_count)
        if len(df_all) > in_range_count or active_extra:
            parts = [
                f"**{in_range_count}** ticket(s) with activity in {range_hint}",
            ]
            if active_extra:
                parts.append(
                    f"**{active_extra}** active queue ticket(s) kept visible outside that range"
                )
            if len(df_all) > len(df):
                parts.append(f"**{len(df_all) - len(df)}** older row(s) hidden — widen **Time range**")
            st.caption(". ".join(parts) + ".")

    if not df_all.empty and "status" in df_all.columns:
        mismatches = _fetch_pending_with_response_mismatch()
        if mismatches:
            shown = ", ".join(mismatches[:5])
            st.error(
                f"**{len(mismatches)}** ticket(s) look stuck in **Daily Task** after a field reply "
                f"(e.g. {shown}). Use **Record response** on Daily Task, or check Railway bot logs "
                "and `supabase/migrations/20260516_tickets_active_anon_policies.sql`. "
                "Tickets **reassigned** for another visit are not listed here."
            )

    masks = _ticket_queue_count_masks(df)
    pending_mask = masks["pending"]
    on_hold_mask = masks["on_hold"]
    open_mask = masks["open"]
    investigation_mask = masks["investigation"]
    unattended_mask = masks["unattended"]
    completed_mask = masks["completed"]
    other_mask = masks["other"]

    total_pending = int(pending_mask.sum())
    total_on_hold = int(on_hold_mask.sum())
    total_open = int(open_mask.sum())
    total_investigation = int(investigation_mask.sum())
    total_unattended = int(unattended_mask.sum())
    total_completed = int(completed_mask.sum())
    total_other = int(other_mask.sum())
    total_in_tabs = (
        total_pending
        + total_on_hold
        + total_open
        + total_investigation
        + total_unattended
        + total_completed
    )
    total_in_view = len(df) if not df.empty else 0
    if total_other:
        raw_other = (
            df.loc[other_mask, "status"].astype(str).str.strip().value_counts().head(5)
            if not df.empty and "status" in df.columns
            else pd.Series(dtype=int)
        )
        detail = ", ".join(f"**{k}** ({v})" for k, v in raw_other.items())
        st.warning(
            f"**{total_other}** ticket(s) use a status not shown in the queue tabs: {detail}. "
            "Run migration `20260625_rename_no_answer_to_on_hold.sql` or move them manually."
        )
    _apply_pending_dashboard_nav()
    (
        pending_label,
        open_label,
        on_hold_label,
        investigation_label,
        completed_label,
        unattended_label,
    ) = _sync_dashboard_nav_state(
        total_pending=total_pending,
        total_on_hold=total_on_hold,
        total_open=total_open,
        total_investigation=total_investigation,
        total_unattended=total_unattended,
        total_completed=total_completed,
    )

    _render_dashboard_header(refreshed_at=refreshed_at)
    main_nav = _render_main_navigation()
    if main_nav == "Tickets":
        _render_assign_day_metrics(df_all, df_in_view=df)
        _render_queue_summary_metrics(
            total_pending=total_pending,
            total_on_hold=total_on_hold,
            total_open=total_open,
            total_investigation=total_investigation,
            total_unattended=total_unattended,
            total_completed=total_completed,
            pending_label=pending_label,
            open_label=open_label,
            on_hold_label=on_hold_label,
            investigation_label=investigation_label,
            completed_label=completed_label,
            unattended_label=unattended_label,
            total_in_view=total_in_view,
            total_in_tabs=total_in_tabs,
            total_other=total_other,
        )
    queue_view = _queue_segment_base(st.session_state.get(_DASH_TICKET_QUEUE_KEY))

    if main_nav == "Log":
        _render_attendance_tab(lookback_days=lookback_days)
        return

    if main_nav == "Performance":
        _render_field_performance_tab(lookback_days=lookback_days)
        return

    if main_nav == "Sales Cases":
        _render_sales_cases_dashboard()
        return

    if queue_view == STATUS_DAILY_TASK:
        st.markdown(f"##### {STATUS_DAILY_TASK} — Waiting on Field")
        st.caption(
            "Rows show **assignment** details (`additional_info` = site notes from the Telegram "
            "assignment lines). A **field response** is a separate swipe-reply (or "
            "``ticket_id + notes`` message) — that moves the ticket to **Open** and fills "
            "``field_response``."
        )
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            pend = df[pending_mask].copy()
            if pend.empty:
                st.info(f"No pending tickets in the last {lookback_days} {day_word}.")
            else:
                st.caption(
                    "Unassigned rows (no engineer) were added without Telegram — use "
                    "**Edit assignment** to assign, or **Under Investigation** to park. "
                    "**Reassign** / **Record response** need exactly one ticket selected."
                )
                pend_show = tuple(
                    c
                    for c in (
                        "ticket_number",
                        "assigned_to",
                        "task_category",
                        "additional_info",
                        "created_at",
                        "last_assigned_at",
                    )
                    if c in pend.columns
                )
                _render_admin_ticket_toolbar(
                    pend,
                    key_prefix="assigned",
                    caption=(
                        "**Edit assignment** assigns an engineer (optional Telegram). "
                        "**Under Investigation** parks without field assign. "
                        "**Reassign** clears prior response for a new visit."
                    ),
                    status_actions=(
                        (
                            "Under Investigation",
                            STATUS_UNDER_INVESTIGATION,
                            "MovedToInvestigation",
                        ),
                        ("On Hold", STATUS_ON_HOLD, "OnHold"),
                    ),
                    allow_delete=True,
                    allow_edit_assignment=True,
                    allow_manual_field_response=_is_dashboard_admin(),
                    allow_reassign=_is_dashboard_admin(),
                )
                _render_selectable_ticket_table(
                    pend,
                    key_prefix="assigned",
                    cols=pend_show,
                )

                if _is_dashboard_admin():
                    mismatch = _fetch_pending_with_response_mismatch()
                    if mismatch:
                        shown = ", ".join(mismatch[:8])
                        extra = f" (+{len(mismatch) - 8} more)" if len(mismatch) > 8 else ""
                        st.warning(
                            "Daily Task tickets with a **current** field reply not reflected in "
                            f"status (bot may have failed): {shown}{extra}. Use **Record response**, "
                            "**Reassign** for a fresh visit, or check **Open**."
                        )

                if _is_dashboard_admin() and st.session_state.get(
                    _reassign_session_keys("assigned")["show"]
                ):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_reassign_editor(
                        from_status=STATUS_DAILY_TASK,
                        key_prefix="assigned",
                        edit_key_prefix="assigned",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(pend),
                    )

                if st.session_state.get(
                    _assignment_edit_session_keys("assigned")["show"]
                ):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_assignment_editor(
                        required_status=STATUS_DAILY_TASK,
                        key_prefix="assigned",
                        edit_key_prefix="assigned",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(pend),
                    )

                if _is_dashboard_admin() and st.session_state.get(
                    _manual_field_response_session_keys("assigned")["show"]
                ):
                    _render_manual_field_response_editor(
                        key_prefix="assigned",
                        edit_key_prefix="assigned",
                        ticket_options=_ticket_options_for_admin(pend),
                        allowed_statuses=(STATUS_DAILY_TASK,),
                        save_label="Save → Open",
                    )

                stale_col = (
                    "last_assigned_at"
                    if "last_assigned_at" in pend.columns
                    else ("created_at" if "created_at" in pend.columns else None)
                )
                if stale_col:
                    pend["_stale_ts"] = _parse_ts(pend[stale_col])
                    now_utc = pd.Timestamp.now(tz=LOCAL_TZ).tz_convert("UTC")
                    if pend["_stale_ts"].notna().any():
                        stale_ids = pend.loc[
                            pend["_stale_ts"].notna()
                            & ((now_utc - pend["_stale_ts"]) > pd.Timedelta(hours=24)),
                            "ticket_number",
                        ].astype(str).tolist()
                        if stale_ids:
                            st.caption(
                                "Stale (>24h since assign, no response): "
                                + ", ".join(stale_ids[:8])
                                + (
                                    f" (+{len(stale_ids) - 8} more)"
                                    if len(stale_ids) > 8
                                    else ""
                                )
                            )

    elif queue_view == STATUS_ON_HOLD:
        st.markdown("##### On Hold — Admin Chase Queue")
        st.caption(
            "Tickets land here only when an admin uses **Action → On Hold** "
            "(from **Daily Task**, **Needs Review**, or **Investigation**). "
            "**Record response** when the engineer replies → **Needs Review**. "
            "**Reassign** clears the visit for a fresh assignment."
        )
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            na_df = df[on_hold_mask].copy()
            if na_df.empty:
                st.info(f"No **On Hold** tickets in the last {lookback_days} {day_word}.")
            else:
                na_show = tuple(
                    c
                    for c in (
                        "ticket_number",
                        "assigned_to",
                        "task_category",
                        "additional_info",
                        "last_assigned_at",
                        "created_at",
                    )
                    if c in na_df.columns
                )
                _render_admin_ticket_toolbar(
                    na_df,
                    key_prefix="on_hold",
                    caption=(
                        "**Action**: **Under Investigation** to park, or **Reassign** / "
                        "**Record response** (one ticket selected)."
                    ),
                    status_actions=(
                        (
                            "Under Investigation",
                            STATUS_UNDER_INVESTIGATION,
                            "MovedToInvestigation",
                        ),
                    ),
                    allow_delete=True,
                    allow_edit_assignment=True,
                    allow_manual_field_response=_is_dashboard_admin(),
                    allow_reassign=_is_dashboard_admin(),
                )
                _render_selectable_ticket_table(
                    na_df,
                    key_prefix="on_hold",
                    cols=na_show,
                )
                if _is_dashboard_admin() and st.session_state.get(
                    _reassign_session_keys("on_hold")["show"]
                ):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_reassign_editor(
                        from_status=STATUS_ON_HOLD,
                        key_prefix="on_hold",
                        edit_key_prefix="on_hold",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(na_df),
                    )
                if st.session_state.get(
                    _assignment_edit_session_keys("on_hold")["show"]
                ):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_assignment_editor(
                        required_status=STATUS_ON_HOLD,
                        key_prefix="on_hold",
                        edit_key_prefix="on_hold",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(na_df),
                    )
                if _is_dashboard_admin() and st.session_state.get(
                    _manual_field_response_session_keys("on_hold")["show"]
                ):
                    _render_manual_field_response_editor(
                        key_prefix="on_hold",
                        edit_key_prefix="on_hold",
                        ticket_options=_ticket_options_for_admin(na_df),
                        allowed_statuses=(STATUS_ON_HOLD,),
                        save_label="Save → Open",
                    )

    elif queue_view == "Unattended":
        st.markdown("##### Unattended — No Same-Day Field Response")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            unat = df[unattended_mask].copy()
            if unat.empty:
                st.info(
                    "No **Unattended** tickets in this time window. "
                    "Tickets move here automatically after assign-day cutoff "
                    "if the engineer never replied."
                )
            else:
                _render_admin_ticket_toolbar(
                    unat,
                    key_prefix="unattended",
                    caption="Reopen to **Daily Task** to reassign or chase again.",
                    status_actions=(
                        ("Reopen to Daily Task", STATUS_DAILY_TASK, "ReopenedFromUnattended"),
                    ),
                    allow_delete=True,
                )
                _render_selectable_ticket_table(
                    unat,
                    key_prefix="unattended",
                    cols=_TICKET_QUEUE_TABLE_COLS
                    + ("additional_info", "last_assigned_at", "unattended_nudge_sent_at"),
                )

    elif queue_view == "Open":
        st.markdown("##### Open — Needs Your Review")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            open_df = df[open_mask].copy()
            if open_df.empty:
                st.info(f"No tickets awaiting admin review in the last {lookback_days} {day_word}.")
            else:
                st.caption(
                    "Select ticket(s) with **Select**, then: **Mark Resolved** (bulk OK) · "
                    "**Under Investigation** (park, no ●) · **Follow-up** (one ticket + note, ● tracked)."
                )
                _render_admin_ticket_toolbar(
                    open_df,
                    key_prefix="open",
                    caption=(
                        "**Action** menu: **Mark Resolved** or **Under Investigation** (no follow-up marker). "
                        "**Follow-up** popover = individual tracked case. **Reassign** → **Daily Task**."
                    ),
                    status_actions=(
                        ("Mark Resolved", STATUS_RESOLVED, "Resolved"),
                        (
                            "Under Investigation",
                            STATUS_UNDER_INVESTIGATION,
                            "MovedToInvestigation",
                        ),
                        ("On Hold", STATUS_ON_HOLD, "OnHold"),
                    ),
                    allow_delete=True,
                    allow_edit_assignment=True,
                    allow_manual_field_response=_is_dashboard_admin(),
                    allow_reassign=True,
                    allow_mark_follow_up=True,
                )
                _render_selectable_ticket_table(
                    open_df,
                    key_prefix="open",
                    cols=_TICKET_QUEUE_TABLE_COLS + ("additional_info", "created_at"),
                )

                if _is_dashboard_admin() and st.session_state.get(
                    _manual_field_response_session_keys("open")["show"]
                ):
                    _render_manual_field_response_editor(
                        key_prefix="open",
                        edit_key_prefix="open",
                        ticket_options=_ticket_options_for_admin(open_df),
                        allowed_statuses=("Open",),
                        save_label="Save response",
                    )

                if st.session_state.get(
                    _assignment_edit_session_keys("open")["show"]
                ):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_assignment_editor(
                        required_status="Open",
                        key_prefix="open",
                        edit_key_prefix="open",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(open_df),
                    )

                if st.session_state.get(_reassign_session_keys("open")["show"]):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_reassign_editor(
                        from_status="Open",
                        key_prefix="open",
                        edit_key_prefix="open",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(open_df),
                    )

                with st.expander("Photo gallery", expanded=total_open <= 3):
                    _render_field_photos_section(open_df)

    elif queue_view == STATUS_UNDER_INVESTIGATION:
        st.markdown("##### Under Investigation")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            inv_df = df[investigation_mask].copy()
            if inv_df.empty:
                st.info(
                    f"No tickets under investigation in the last {lookback_days} {day_word}. "
                    "Move tickets here from **Needs Review** via **Under Investigation** or **Follow-up**."
                )
            else:
                st.caption(
                    "**Follow-up** cases (● in the Follow-up column) stay pinned on top. "
                    "Other **Under Investigation** tickets have no ●. Search by ticket # if needed."
                )
                _render_admin_ticket_toolbar(
                    inv_df,
                    key_prefix="investigation",
                    caption=None,
                    status_actions=(
                        ("Back to Open", "Open", "BackToOpenFromInvestigation"),
                        ("Mark Resolved", STATUS_RESOLVED, "Resolved"),
                        ("On Hold", STATUS_ON_HOLD, "OnHold"),
                    ),
                    allow_delete=True,
                    allow_edit_assignment=True,
                    allow_reassign=True,
                )
                inv_cols = list(
                    _TICKET_QUEUE_TABLE_COLS + ("additional_info", "created_at")
                )
                if "follow_up_note" in inv_df.columns:
                    inv_cols.extend(["follow_up_at", "follow_up_note"])
                _render_selectable_ticket_table(
                    inv_df,
                    key_prefix="investigation",
                    cols=tuple(dict.fromkeys(c for c in inv_cols if c in inv_df.columns)),
                    highlight_follow_up=True,
                )

                if st.session_state.get(
                    _assignment_edit_session_keys("investigation")["show"]
                ):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_assignment_editor(
                        required_status=STATUS_UNDER_INVESTIGATION,
                        key_prefix="investigation",
                        edit_key_prefix="investigation",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(inv_df),
                    )

                if st.session_state.get(_reassign_session_keys("investigation")["show"]):
                    cat_names, _cat_missing = _try_fetch_task_categories()
                    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
                    _render_reassign_editor(
                        from_status=STATUS_UNDER_INVESTIGATION,
                        key_prefix="investigation",
                        edit_key_prefix="investigation",
                        cat_names=cat_names,
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        ticket_options=_ticket_options_for_admin(inv_df),
                    )

                with st.expander("Photo gallery", expanded=total_investigation <= 3):
                    _render_field_photos_section(inv_df)

    elif queue_view == STATUS_RESOLVED:
        st.markdown(f"##### {STATUS_RESOLVED}")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            done = df[completed_mask].copy()
            if done.empty:
                st.info(f"No resolved tickets in the last {lookback_days} {day_word}.")
            else:
                _render_admin_ticket_toolbar(
                    done,
                    key_prefix="completed",
                    caption="Send back to **Open** for more field work.",
                    status_actions=(
                        ("Send back to Open", "Open", "Reopened"),
                    ),
                    allow_delete=True,
                )
                _render_selectable_ticket_table(
                    done,
                    key_prefix="completed",
                    cols=_TICKET_QUEUE_TABLE_COLS + ("additional_info", "created_at"),
                )

                with st.expander("Photo gallery", expanded=False):
                    _render_field_photos_section(done)


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
        st.caption("No resolved tickets to show photos for.")
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


def _render_field_performance_tab(*, lookback_days: int) -> None:
    """Field tickets + sales cases — outcomes per person in the sidebar window."""
    range_caption = _format_dash_range_caption() or "sidebar time range"
    st.caption(f"{range_caption} · {LOCAL_TZ_LABEL}")

    range_start, range_end = _get_dash_range()
    bucket_fmt, x_title, axis_format = _perf_bucket_settings(range_start, range_end)

    st.markdown("##### Field tickets")
    try:
        df_all = _fetch_tickets()
    except Exception as exc:
        st.error(f"Could not load tickets: {exc}")
        df_all = pd.DataFrame()

    field_has_data = (
        not df_all.empty and "status" in df_all.columns
    )
    if not field_has_data:
        st.info("No field ticket data to analyze.")
        in_view = pending = open_df = completed = investigation = on_hold = unattended = (
            pd.DataFrame()
        )
    else:
        slices = _perf_prepare_slices(df_all, range_start, range_end)
        in_view = slices["in_view"]
        pending = slices["pending"]
        open_df = slices["open"]
        completed = slices["completed"]
        investigation = slices["investigation"]
        on_hold = slices["on_hold"]
        unattended = slices["unattended"]

    n_in_view = len(in_view)
    n_pending = len(pending)
    n_open = len(open_df)
    n_done = len(completed)
    n_inv = len(investigation)
    n_hold = len(on_hold)
    n_unatt = len(unattended)
    n_tab_sum = n_pending + n_open + n_hold + n_done + n_inv + n_unatt

    if field_has_data and n_in_view == 0:
        st.info(
            "No field tickets in this time window. Try **Last 30 days** in the sidebar."
        )

    if field_has_data and n_in_view > 0:
        summary = _perf_build_summary(
            pending, open_df, completed, investigation, on_hold, unattended
        )
        people = ["All"] + (
            summary["Person"].tolist() if not summary.empty else []
        )
        focus = st.selectbox(
            "Focus Assignee (Field)",
            options=people,
            key="perf_focus_person",
            help="Filter field ticket tabs to one engineer, or **All**.",
        )
        pending_f = _perf_filter_by_person(pending, focus)
        open_f = _perf_filter_by_person(open_df, focus)
        completed_f = _perf_filter_by_person(completed, focus)
        investigation_f = _perf_filter_by_person(investigation, focus)
        on_hold_f = _perf_filter_by_person(on_hold, focus)
        unattended_f = _perf_filter_by_person(unattended, focus)
        work_f = _perf_combine_work(completed_f, investigation_f)
        n_work = len(work_f)
        n_filtered = (
            len(pending_f)
            + len(open_f)
            + len(on_hold_f)
            + len(completed_f)
            + len(investigation_f)
            + len(unattended_f)
        )

        m0, m1, m2, m3, m4, m5, m6 = st.columns(7)
        m0.metric("In view", n_in_view if focus == "All" else n_filtered)
        m1.metric(STATUS_DAILY_TASK, len(pending_f))
        m2.metric("Needs Review", len(open_f))
        m3.metric("On Hold", len(on_hold_f))
        m4.metric(STATUS_RESOLVED, len(completed_f))
        m5.metric("Investigation", len(investigation_f))
        m6.metric("Unattended", len(unattended_f))
        st.caption(
            "**In view** matches the **Tickets** tab (time range + active queues). "
            "Queue counts above sum to **In view** when **All** assignees are selected. "
            f"**Handled** ({STATUS_RESOLVED} + Investigation) is in the overview table — not the same as **In view**."
        )
        if focus == "All" and n_tab_sum != n_in_view:
            st.warning(
                f"Queue sum (**{n_tab_sum}**) ≠ in view (**{n_in_view}**) — "
                "some tickets have an unrecognized status."
            )

        tab_overview, tab_work, tab_hold, tab_unatt = st.tabs(
            [
                "Overview",
                f"Handled ({n_work})",
                f"On Hold ({len(on_hold_f)})",
                f"Unattended ({len(unattended_f)})",
            ]
        )

        with tab_overview:
            if focus != "All":
                sub = (
                    summary[summary["Person"] == focus]
                    if not summary.empty
                    else summary
                )
                _render_perf_individual_summary_table(sub)
            else:
                _render_perf_individual_summary_table(summary)
            c1, c2, c3 = st.columns(3)
            with c1:
                if not work_f.empty:
                    st.markdown("**Handled by assignee**")
                    _render_perf_person_bar(
                        work_f, title="", value_name="Handled"
                    )
            with c2:
                if not on_hold_f.empty:
                    st.markdown("**On Hold by assignee**")
                    _render_perf_person_bar(
                        on_hold_f, title="", value_name="On Hold"
                    )
            with c3:
                if not unattended_f.empty:
                    st.markdown("**Unattended by assignee**")
                    _render_perf_person_bar(
                        unattended_f, title="", value_name="Unattended"
                    )

        with tab_work:
            st.caption(
                f"**Handled** = {STATUS_RESOLVED} + Under Investigation (subset of **In view**)."
            )
            if work_f.empty:
                st.info("No resolved or investigation tickets for this filter.")
            else:
                view = _perf_enrich_tickets(work_f)
                c_chart, c_table = st.columns([3, 2])
                with c_chart:
                    _render_perf_person_bar(
                        view,
                        title="Handled by assignee",
                        value_name="Handled",
                    )
                with c_table:
                    st.markdown("**Split**")
                    split = (
                        view.groupby(["_outcome", "category"], as_index=False)
                        .size()
                        .rename(columns={"size": "Tickets"})
                        .sort_values(
                            ["Tickets", "_outcome", "category"],
                            ascending=[False, True, True],
                        )
                        .rename(columns={"_outcome": "Outcome", "category": "Category"})
                    )
                    st.dataframe(split, use_container_width=True, hide_index=True)
                with st.expander("Trend & ticket list", expanded=False):
                    _render_perf_outcome_trend(
                        view,
                        bucket_fmt=bucket_fmt,
                        x_title=x_title,
                        axis_format=axis_format,
                    )
                    _render_perf_ticket_table(view)

        with tab_hold:
            st.caption(
                "Tickets moved to **On Hold** by an admin in this window — chase queue per assignee."
            )
            if on_hold_f.empty:
                st.info("No on-hold tickets for this filter.")
            else:
                hold_view = _perf_enrich_tickets(on_hold_f)
                _render_perf_person_bar(
                    hold_view,
                    title="On Hold by assignee",
                    value_name="On Hold",
                )
                with st.expander("Trend over time", expanded=False):
                    _render_perf_stacked_staff_chart(
                        hold_view,
                        y_title="On Hold",
                        bucket_fmt=bucket_fmt,
                        x_title=x_title,
                        axis_format=axis_format,
                    )
                with st.expander("Ticket list", expanded=len(on_hold_f) <= 8):
                    _render_perf_ticket_table(hold_view)

        with tab_unatt:
            st.caption(
                "No same-day field response before assign-day cutoff — per assignee."
            )
            if unattended_f.empty:
                st.info("No unattended tickets for this filter.")
            else:
                _render_perf_person_bar(
                    unattended_f,
                    title="Unattended by assignee",
                    value_name="Unattended",
                )
                with st.expander("Trend over time", expanded=False):
                    _render_perf_stacked_staff_chart(
                        unattended_f,
                        y_title="Unattended",
                        bucket_fmt=bucket_fmt,
                        x_title=x_title,
                        axis_format=axis_format,
                    )
                with st.expander("Ticket list", expanded=len(unattended_f) <= 8):
                    _render_perf_ticket_table(unattended_f)

    st.divider()
    st.markdown("##### Sales Cases")
    st.caption(
        "Cases with activity in this window (by **updated_at**). "
        "Grouped by **sales_owner**."
    )
    try:
        sc_all = _fetch_sales_cases_df()
    except Exception as exc:
        st.error(f"Could not load sales cases: {exc}")
        return

    if sc_all is None:
        st.warning(
            f"The `{SALES_CASES_TABLE}` table is missing. Apply "
            f"``supabase/migrations/20260620_dashboard_sales_cases.sql`` in Supabase."
        )
        return

    sc_df = _perf_filter_sales_in_range(sc_all, range_start, range_end)
    if sc_df.empty:
        st.info(
            "No Sales Cases updated in this time window. "
            "Try **Last 30 days** or widen **Time range**."
        )
        return

    sc_summary = _perf_build_sales_summary(sc_df)
    sc_people = ["All"] + (
        sc_summary["Person"].tolist() if not sc_summary.empty else []
    )
    sc_accounts = ["All"] + _perf_sales_account_names(sc_df)
    f_owner, f_resort = st.columns(2)
    with f_owner:
        sc_focus = st.selectbox(
            "Focus Sales Owner",
            options=sc_people,
            key="perf_sc_focus_person",
            help="Filter to one **sales_owner**, or **All**.",
        )
    with f_resort:
        sc_account = st.selectbox(
            "Focus Resort / Company",
            options=sc_accounts,
            key="perf_sc_focus_account",
            help="Filter to one **Resort name / Company name** (`account_name`), or **All**.",
        )
    sc_f = _perf_filter_by_person(sc_df, sc_focus)
    sc_f = _perf_filter_sales_by_account(sc_f, sc_account)
    if sc_focus != "All" or sc_account != "All":
        bits = []
        if sc_focus != "All":
            bits.append(f"sales owner **{sc_focus}**")
        if sc_account != "All":
            bits.append(f"resort/company **{sc_account}**")
        st.caption(f"Filtered by {' and '.join(bits)} · **{len(sc_f)}** case(s).")
    sc_view = _perf_enrich_sales_cases(sc_f)

    n_sales = len(_sc_filter_sales_df(sc_f, (SC_STATUS_SALES_TICKET,)))
    n_sc_inv = len(_sc_filter_sales_df(sc_f, _SC_INVESTIGATION_QUEUE_STATUSES))
    n_design = len(_sc_filter_sales_df(sc_f, (SC_STATUS_DESIGN,)))
    n_resolved = len(_sc_filter_sales_df(sc_f, (SC_STATUS_RESOLVED,)))

    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("In window", len(sc_f))
    s2.metric("Sales Ticket", n_sales)
    s3.metric("Investigation", n_sc_inv)
    s4.metric("Design", n_design)
    s5.metric("Resolved", n_resolved)

    tab_sc_over, tab_sc_list = st.tabs(
        ["Overview", f"Cases ({len(sc_f)})"],
    )
    with tab_sc_over:
        summary_view = sc_summary
        if sc_focus != "All" and not sc_summary.empty:
            summary_view = sc_summary[sc_summary["Person"] == sc_focus]
        if sc_account != "All" and not sc_f.empty:
            summary_view = _perf_build_sales_summary(sc_f)
        _render_perf_individual_summary_table(summary_view)
        if not sc_view.empty:
            by_status = (
                sc_view.groupby("status_eff", as_index=False)
                .size()
                .rename(columns={"size": "Cases"})
                .sort_values("Cases", ascending=False)
            )
            c_bar, c_pie = st.columns([2, 1])
            with c_bar:
                _render_perf_person_bar(
                    sc_view,
                    title="Cases by sales owner",
                    value_name="Cases",
                )
            with c_pie:
                st.markdown("**By status**")
                st.dataframe(by_status, use_container_width=True, hide_index=True)

    with tab_sc_list:
        if sc_view.empty:
            st.info("No Sales Cases for this filter.")
        else:
            with st.expander("Trend over time", expanded=False):
                _render_perf_stacked_staff_chart(
                    sc_view,
                    y_title="Cases",
                    bucket_fmt=bucket_fmt,
                    x_title=x_title,
                    axis_format=axis_format,
                )
            _render_perf_sales_case_table(sc_view)


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


def _render_attendance_tab(*, lookback_days: int) -> None:
    """Attendance log table; optional filters; timeline in expander."""
    range_start, range_end = _get_dash_range()
    st.caption(
        f"Attendance history · {_format_dash_range_caption() or 'sidebar time range'}"
    )

    f1, f2 = st.columns(2)
    ticket_clean = f1.text_input(
        "Ticket #",
        placeholder="optional",
        key="att_ticket_q",
    ).strip()
    member_clean = f2.text_input(
        "Member",
        placeholder="@username",
        key="att_member_q",
    ).strip()

    try:
        logs = _fetch_attendance(
            ticket_number=ticket_clean if ticket_clean else None,
            member_query=member_clean if member_clean else None,
            since_utc=range_start,
            until_utc=range_end,
            limit=2000,
        )
    except _TableMissingError as missing:
        _render_missing_table_help(missing.table)
        return

    if logs.empty:
        st.info("No log entries for this time window. Widen **Time range** or clear filters.")
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

    with st.expander("Timeline (detail cards)", expanded=False):
        for _, row in logs.iterrows():
            member = row.get("member_username") or "unknown"
            action = row.get("action_type") or "?"
            tid = row.get("ticket_number") or "—"
            when_local = ""
            ts_raw = row.get("timestamp")
            if pd.notna(ts_raw):
                try:
                    when_local = pd.Timestamp(ts_raw).tz_convert(LOCAL_TZ).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
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
