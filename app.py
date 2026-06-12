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
import logging
import math
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from cryptography.fernet import Fernet
import altair as alt

_STAFF_MATRIX_BUILD = Path(__file__).resolve().parent / "components" / "staff_matrix" / "build"
_HAS_STAFF_MATRIX = (_STAFF_MATRIX_BUILD / "index.html").is_file()
try:
    from components.staff_matrix import staff_matrix as _staff_matrix_component
except ImportError:
    _staff_matrix_component = None
    _HAS_STAFF_MATRIX = False

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
    canonical_task_category,
    delete_task_category,
    dedupe_canonical_categories,
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
    run_unattended_close,
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
_OUTCOME_SAME_LABEL = "Same as assigned"

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
SALES_REGION_CENTRAL = "CENTRAL"


def _sc_region_is_central(region: object) -> bool:
    return str(region or "").strip().upper() == SALES_REGION_CENTRAL


def _sc_sync_region_engineer_widgets(
    *,
    region_key: str,
    region_watch_key: str,
    engineer_select_key: str,
    engineer_manual_key: str,
    fe_names: list[str],
    fe_missing: bool,
    restore_handle: str = "",
    engineer_select2_key: str | None = None,
    engineer_manual2_key: str | None = None,
    restore_handle_2: str = "",
) -> bool:
    """When Region Team is not CENTRAL, clear engineer; restore when switching back."""
    region = str(st.session_state.get(region_key, "")).strip()
    central = _sc_region_is_central(region)
    prev = st.session_state.get(region_watch_key)
    sel2 = engineer_select2_key or f"{engineer_select_key}_2"
    man2 = engineer_manual2_key or f"{engineer_manual_key}_2"
    if prev != region:
        st.session_state[region_watch_key] = region
        if not central:
            st.session_state.pop(engineer_select_key, None)
            st.session_state.pop(engineer_manual_key, None)
            st.session_state.pop(sel2, None)
            st.session_state.pop(man2, None)
        else:
            if restore_handle:
                handle = restore_handle.lstrip("@").lower()
                _sync_engineer_widget_value(
                    key=engineer_select_key,
                    current_handle=handle,
                    fe_names=fe_names,
                    fe_missing=fe_missing,
                )
                if engineer_manual_key != engineer_select_key:
                    st.session_state[engineer_manual_key] = handle
            if restore_handle_2:
                handle2 = restore_handle_2.lstrip("@").lower()
                _sync_engineer_widget_value(
                    key=sel2,
                    current_handle=handle2,
                    fe_names=fe_names,
                    fe_missing=fe_missing,
                )
                if man2 != sel2:
                    st.session_state[man2] = handle2
    return central


def _render_sc_engineer_field(
    *,
    central: bool,
    fe_names: list[str],
    fe_missing: bool,
    select_key: str,
    manual_key: str,
    blank_key: str,
    select2_key: str | None = None,
    manual2_key: str | None = None,
) -> None:
    """Engineer picker(s) for CENTRAL; blank disabled field for regional teams."""
    if central:
        eng2_key = select2_key or f"{select_key}_2"
        man2_key = manual2_key or f"{manual_key}_2"
        if select2_key is None and manual2_key is None:
            _render_engineer_pair_fields(
                fe_names=fe_names,
                fe_missing=fe_missing,
                engineer_key=select_key if fe_names and not fe_missing else manual_key,
                engineer2_key=eng2_key if fe_names and not fe_missing else man2_key,
            )
        else:
            _render_engineer_pair_fields(
                fe_names=fe_names,
                fe_missing=fe_missing,
                engineer_key=select_key if fe_names and not fe_missing else manual_key,
                engineer2_key=eng2_key if fe_names and not fe_missing else man2_key,
            )
    else:
        st.caption(
            "Regional team — leave engineer blank; assign from **Site Visit** "
            "when the case is **Regional for site visit**."
        )
        st.text_input("Engineer", value="", disabled=True, key=blank_key)


def _sc_resolve_sales_engineer_handle(
    *,
    central: bool,
    fe_names: list[str],
    fe_missing: bool,
    select_key: str,
    manual_key: str,
    select2_key: str | None = None,
    manual2_key: str | None = None,
) -> str | None:
    if not central:
        return None
    eng2 = select2_key or f"{select_key}_2"
    man2 = manual2_key or f"{manual_key}_2"
    h1, _h2 = _resolve_engineer_pair(
        fe_names=fe_names,
        fe_missing=fe_missing,
        engineer_key=select_key if fe_names and not fe_missing else manual_key,
        engineer2_key=eng2 if fe_names and not fe_missing else man2,
        required_primary=True,
    )
    return h1


def _sc_resolve_sales_engineer_pair(
    *,
    central: bool,
    fe_names: list[str],
    fe_missing: bool,
    select_key: str,
    manual_key: str,
    select2_key: str | None = None,
    manual2_key: str | None = None,
) -> tuple[str | None, str | None]:
    if not central:
        return None, None
    eng2 = select2_key or f"{select_key}_2"
    man2 = manual2_key or f"{manual_key}_2"
    return _resolve_engineer_pair(
        fe_names=fe_names,
        fe_missing=fe_missing,
        engineer_key=select_key if fe_names and not fe_missing else manual_key,
        engineer2_key=eng2 if fe_names and not fe_missing else man2,
        required_primary=True,
    )


SALES_PRIORITY_OPTIONS: tuple[str, ...] = ("Strategic", "High", "Urgent", "Standard")
DEFAULT_SALES_CASE_CATEGORIES: tuple[str, ...] = (
    "QOS Issue",
    "Call Drop Issues",
    "Coverage Check",
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
_SC_ACTIVE_QUEUE_STATUSES: frozenset[str] = frozenset(
    {
        SC_STATUS_SALES_TICKET,
        SC_STATUS_INVESTIGATION,
        SC_STATUS_REGIONAL,
        SC_STATUS_DESIGN,
    }
)
# Weekly attended report — Sales cases that count as "attended" (no On Hold track).
_SC_ATTENDED_STATUSES: frozenset[str] = frozenset(
    {
        SC_STATUS_INVESTIGATION,
        SC_STATUS_REGIONAL,
        SC_STATUS_RESOLVED,
    }
)
_PERF_WEEKLY_DATE_KEY = "perf_weekly_pick_date"
_PERF_WEEKLY_WEEK_KEY = _PERF_WEEKLY_DATE_KEY  # legacy alias (old selectbox key name)
# CSM rows counted in Weekly attended (excludes Daily Task / Open / Unattended).
_CSM_WEEKLY_ATTENDED_KEYS: tuple[str, ...] = (
    "on_hold",
    "completed",
    "investigation",
)
_CSM_WEEKLY_ATTENDED_LABELS: dict[str, str] = {
    "on_hold": STATUS_ON_HOLD,
    "completed": STATUS_RESOLVED,
    "investigation": STATUS_UNDER_INVESTIGATION,
}

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


def _sc_row_text(val: object) -> str:
    s = str(val or "").strip()
    return "" if not s or s.lower() == "nan" else s


def _sc_status_actions_for_queue(queue_status: str) -> tuple[tuple[str, str], ...]:
    """(Action menu label, target status) for the active sales queue toolbar."""
    if queue_status == SC_STATUS_SALES_TICKET:
        return (
            ("Investigation", SC_STATUS_INVESTIGATION),
            ("Design", SC_STATUS_DESIGN),
            ("Mark Resolved", SC_STATUS_RESOLVED),
        )
    if queue_status == SC_STATUS_INVESTIGATION:
        return (
            ("Design", SC_STATUS_DESIGN),
            ("Mark Resolved", SC_STATUS_RESOLVED),
        )
    if queue_status == SC_STATUS_DESIGN:
        return (
            ("Back to Investigation", SC_STATUS_INVESTIGATION),
            ("Mark Resolved", SC_STATUS_RESOLVED),
        )
    if queue_status == SC_STATUS_RESOLVED:
        return (
            ("Back to Design", SC_STATUS_DESIGN),
            ("Back to Investigation", SC_STATUS_INVESTIGATION),
        )
    return ()


def _sc_toolbar_action_label(label: str, target_status: str) -> str:
    if target_status == SC_STATUS_RESOLVED:
        return "Mark Resolved"
    return label


def _sc_toolbar_label_to_target(
    status_actions: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    return {
        _sc_toolbar_action_label(label, tgt): tgt for label, tgt in status_actions
    }


def _sc_toolbar_panel_keys(key_prefix: str) -> dict[str, str]:
    return {
        "details": f"{key_prefix}_sc_show_details",
    }


def _sc_clear_toolbar_panels_except(key_prefix: str) -> None:
    for prefix in (
        "sc_sales_ticket",
        "sc_investigation",
        "sc_design",
        "sc_resolved",
    ):
        if prefix == key_prefix:
            continue
        keys = _sc_toolbar_panel_keys(prefix)
        for k in keys.values():
            st.session_state.pop(k, None)


def _sc_status_actions_for_case(cur_status: str) -> tuple[tuple[str, str], ...]:
    """Action menu options for one selected case (row status may differ from queue view)."""
    if cur_status == SC_STATUS_SALES_TICKET:
        return (
            ("Investigation", SC_STATUS_INVESTIGATION),
            ("Design", SC_STATUS_DESIGN),
            ("Mark Resolved", SC_STATUS_RESOLVED),
        )
    if cur_status in (SC_STATUS_INVESTIGATION, SC_STATUS_REGIONAL):
        return (
            ("Design", SC_STATUS_DESIGN),
            ("Mark Resolved", SC_STATUS_RESOLVED),
        )
    if cur_status == SC_STATUS_DESIGN:
        return (
            ("Back to Investigation", SC_STATUS_INVESTIGATION),
            ("Mark Resolved", SC_STATUS_RESOLVED),
        )
    if cur_status == SC_STATUS_RESOLVED:
        return (
            ("Back to Design", SC_STATUS_DESIGN),
            ("Back to Investigation", SC_STATUS_INVESTIGATION),
        )
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
        if cur == SC_STATUS_SALES_TICKET:
            payload = _sc_patch_with_action_comment(
                {"status": SC_STATUS_INVESTIGATION, "admin_owner": op},
                action_comment,
            )
        elif cur in (
            SC_STATUS_REGIONAL,
            SC_STATUS_DESIGN,
            SC_STATUS_RESOLVED,
        ):
            body: dict[str, object] = {"status": SC_STATUS_INVESTIGATION}
            if cur == SC_STATUS_RESOLVED:
                body["close_note"] = None
            payload = _sc_patch_with_action_comment(body, action_comment)
        else:
            return f"Case is **{cur}** — cannot move to **Investigation**."
        _sales_cases_update_row(row_id, payload)
        return None
    if target_status == SC_STATUS_DESIGN:
        if cur in (
            SC_STATUS_SALES_TICKET,
            SC_STATUS_INVESTIGATION,
            SC_STATUS_REGIONAL,
        ):
            payload = _sc_patch_with_action_comment(
                {"status": SC_STATUS_DESIGN},
                action_comment,
            )
        elif cur == SC_STATUS_RESOLVED:
            payload = _sc_patch_with_action_comment(
                {"status": SC_STATUS_DESIGN, "close_note": None},
                action_comment,
            )
        else:
            return f"Case is **{cur}** — cannot move to **Design**."
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


def _sc_apply_status_to_selected_cases(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    case_options: list[str],
    target_status: str,
    op: str,
) -> int:
    """Bulk status move for all ticked cases in the queue table."""
    picked = _require_selected_sales_cases(
        key_prefix=key_prefix, options=case_options, exactly_one=False
    )
    if not picked:
        return 0
    action_comment = str(st.session_state.get("sc_action_comment", "")).strip() or None
    ok = 0
    for cref in picked:
        sub = df[df["case_ref"].fillna("").astype(str) == cref]
        if sub.empty:
            continue
        r0 = sub.iloc[0]
        row_id = str(r0.get("id") or "").strip()
        if not row_id:
            continue
        err = _sc_apply_status_advance(
            row_id,
            r0=r0,
            target_status=target_status,
            op=op,
            action_comment=action_comment,
        )
        if err:
            st.warning(f"**{cref}**: {err}")
        else:
            ok += 1
    if ok:
        _invalidate_dashboard_data_cache()
        st.session_state[_sc_case_selection_session_key(key_prefix)] = []
        _sc_set_sales_flash(f"**{ok}** case(s) moved to **{target_status}**.")
        st.rerun()
    return ok


_SC_SALES_FLASH_KEY = "_dash_sales_cases_flash"
_SC_SALES_FLASH_LEVEL_KEY = "_dash_sales_cases_flash_level"

from dotenv import load_dotenv
from supabase_client import (
    get_cached_supabase_client,
    is_transient_supabase_error,
    resolve_supabase_config,
    test_supabase_connection,
)

log = logging.getLogger(__name__)

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
TICKET_VISITS_TABLE = (
    _read_setting("TICKET_VISITS_TABLE", "ticket_visits")
    or "ticket_visits"
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
_QUEUE_ACTIONS_POPOVER_WIDTH_PX = 220


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
    "This week",
    "Last 30 days",
    "Pick dates",
)
_DASH_SEARCH_FROM_DATE_KEY = "_dash_search_from_date"
_DASH_SEARCH_TO_DATE_KEY = "_dash_search_to_date"
_DASH_PREV_PRESET_KEY = "_dash_prev_preset"
_DASH_RANGE_CUSTOM_OPEN_KEY = "_dash_range_custom_open"
_DASH_TIME_PRESET_MENU: tuple[str, ...] = ("Today", "This week", "Last 30 days")
_DASH_TIME_PRESET_PILL_KEYS: dict[str, str] = {
    "Today": "bon_dash_range_pill_today",
    "This week": "bon_dash_range_pill_week",
    "Last 30 days": "bon_dash_range_pill_30d",
}
_LEGACY_TIME_PRESET_MAP: dict[str, str] = {
    "Last 24 hours": "Today",
    "Last 7 days": "This week",
    "Single day": "Pick dates",
    "Custom range": "Pick dates",
}
_DASH_MAIN_NAV_KEY = "_dash_main_nav"
_DASH_TICKET_QUEUE_KEY = "_dash_ticket_queue"
_DASH_SALES_QUEUE_KEY = "_dash_sales_queue"
_DASH_PENDING_MAIN_NAV_KEY = "_dash_pending_main_nav"
_DASH_PENDING_TICKET_QUEUE_KEY = "_dash_pending_ticket_queue"
_DASH_PENDING_SALES_QUEUE_KEY = "_dash_pending_sales_queue"
_DASH_NAV_CSM = "CSM Cases"
_DASH_MAIN_NAV_OPTIONS: tuple[str, ...] = (_DASH_NAV_CSM, "Sales Cases", "Log", "Performance")
_CC_SESSION_TOKEN_KEY = "_ticket_dashboard_cc_bot_token_session"
_CC_SESSION_GROUP_KEY = "cc_cmd_center_telegram_group_id"
_CC_FE_SELECT_KEY = "cc_fe_select"
_CC_FE_MANUAL_KEY = "cc_fe_manual"
_CC_FE_SELECT_2_KEY = "cc_fe_select_2"
_CC_FE_MANUAL_2_KEY = "cc_fe_manual_2"
_CC_TICKET_INPUT_KEY = "cc_ticket_number"
_CC_ASSIGN_NOTES_KEY = "cc_assign_notes"
_CC_CLEAR_ASSIGN_KEY = "_cc_clear_assign_form"
_CC_ADD_UNASSIGNED_KEY = "cc_add_unassigned"
_CC_CATEGORY_SELECT_KEY = "cc_category_select"
_CC_CATEGORY_SELECT_PENDING_KEY = "_cc_category_select_pending"
_SC_CC_ST_SCAT_PENDING_KEY = "_sc_cc_st_scat_pending"
_SC_EDIT_SCAT_PENDING_KEY = "_sc_edit_scat_pending"
_SC_CC_ST_REF_KEY = "sc_cc_st_case_ref"
_SC_CC_ST_ACCOUNT_KEY = "sc_cc_st_account"
_SC_CC_ST_PRIORITY_KEY = "sc_cc_st_priority"
_SC_CC_ST_REGION_KEY = "sc_cc_st_region"
_SC_CC_ST_SCAT_KEY = "sc_cc_st_scat"
_SC_CC_ST_DESC_KEY = "sc_cc_st_desc"
_SC_CC_SKIP_ASSIGN_KEY = "sc_cc_skip_field_assign"
_SC_CC_FE_SELECT_KEY = "sc_cc_fe_select"
_SC_CC_FE_MANUAL_KEY = "sc_cc_fe_manual"
_SC_CC_FE_SELECT_2_KEY = "sc_cc_fe_select_2"
_SC_CC_FE_MANUAL_2_KEY = "sc_cc_fe_manual_2"
_SC_CC_CLEAR_ST_INTAKE_KEY = "_sc_cc_clear_st_intake"
_CC_SIDEBAR_TAB_KEY = "_cc_sidebar_tab"
CC_TAB_CSM = "CSM"
CC_TAB_SALES = "SALES"
_CC_SIDEBAR_TAB_OPTIONS: tuple[str, ...] = (CC_TAB_CSM, CC_TAB_SALES)
_BON_SIDEBAR_OPEN_KEY = "bon_sidebar_open"
_BON_MENU_OPEN_KEY = "bon_app_menu_open"


def _bon_sidebar_is_open() -> bool:
    if _BON_SIDEBAR_OPEN_KEY in st.session_state:
        return bool(st.session_state[_BON_SIDEBAR_OPEN_KEY])
    legacy_state = st.session_state.pop("bon_sidebar_state", None)
    if legacy_state is not None:
        open_ = str(legacy_state) != "collapsed"
        st.session_state[_BON_SIDEBAR_OPEN_KEY] = open_
        return open_
    return True


def _normalize_dash_main_nav(value: object) -> str:
    """Map legacy nav labels (e.g. Tickets, CSM) to current options."""
    nav = str(value or _DASH_NAV_CSM).strip()
    if nav in ("Tickets", "CSM"):
        return _DASH_NAV_CSM
    if nav == "Sales cases":
        return "Sales Cases"
    return nav


def _assignment_edit_session_keys(prefix: str) -> dict[str, str]:
    return {
        "engineer": f"{prefix}_edit_engineer",
        "engineer_2": f"{prefix}_edit_engineer_2",
        "category": f"{prefix}_edit_category",
        "notes": f"{prefix}_edit_notes",
        "sync_tg": f"{prefix}_edit_sync_tg",
        "show": f"{prefix}_show_assignment_edit",
        "synced_ticket": f"{prefix}_edit_synced_ticket",
    }


def _reassign_session_keys(prefix: str) -> dict[str, str]:
    return {
        "engineer": f"{prefix}_reassign_engineer",
        "engineer_2": f"{prefix}_reassign_engineer_2",
        "category": f"{prefix}_reassign_category",
        "notes": f"{prefix}_reassign_notes",
        "sync_tg": f"{prefix}_reassign_sync_tg",
        "show": f"{prefix}_show_reassign",
        "synced_ticket": f"{prefix}_reassign_synced_ticket",
    }


def _engineer_pick_optional(raw: object) -> str | None:
    s = str(raw or "").strip()
    if not s:
        return None
    return _cc_normalize_handle(s)


def _resolve_engineer_pair(
    *,
    fe_names: list[str],
    fe_missing: bool,
    engineer_key: str,
    engineer2_key: str,
    required_primary: bool = True,
) -> tuple[str | None, str | None]:
    """Return ``(primary, secondary)`` handles from form session keys."""
    h1 = _engineer_pick_optional(st.session_state.get(engineer_key))
    h2 = _engineer_pick_optional(st.session_state.get(engineer2_key))
    if required_primary and not h1:
        raise ValueError("Select or enter an engineer.")
    if h1 and h2 and h1.lower() == h2.lower():
        raise ValueError("Engineer 2 must be different from Engineer 1.")
    return h1, h2


def _coassignee_telegram_note(handle2: str | None, note: str | None) -> str | None:
    if not handle2:
        return note
    extra = f"Co-assignee: {handle2}"
    base = (note or "").strip()
    return f"{base}\n{extra}".strip() if base else extra


def _render_engineer_pair_fields(
    *,
    fe_names: list[str],
    fe_missing: bool,
    engineer_key: str,
    engineer2_key: str,
    engineer_label: str = "Engineer",
    engineer2_label: str = "Engineer 2 (optional)",
) -> None:
    """Engineer pickers — empty on first open (no auto-selected first name)."""
    if fe_missing or not fe_names:
        st.text_input(engineer_label, placeholder="username", key=engineer_key)
        st.text_input(
            engineer2_label,
            placeholder="username (optional)",
            key=engineer2_key,
        )
        return
    opts = [f"@{n}" for n in fe_names]
    st.selectbox(
        engineer_label,
        options=opts,
        index=None,
        placeholder="Select engineer",
        key=engineer_key,
    )
    st.selectbox(
        engineer2_label,
        options=opts,
        index=None,
        placeholder="Optional second engineer",
        key=engineer2_key,
    )


def _render_category_selectbox(
    label: str,
    options: list[str],
    *,
    key: str,
    help: str | None = None,
) -> None:
    """Category picker — empty on first open (no auto-selected first option)."""
    if not options:
        st.selectbox(label, options=["—"], key=key, disabled=True, help=help)
        return
    st.selectbox(
        label,
        options=options,
        index=None,
        placeholder="Select category",
        key=key,
        help=help,
    )


def _sync_category_widget_value(
    *,
    key: str,
    current_cat: str,
    options: list[str],
) -> None:
    """Set category widget only when the ticket already has a category."""
    cat = (current_cat or "").strip()
    if not cat:
        st.session_state.pop(key, None)
        return
    canon = canonical_task_category(cat) or cat
    if canon in options:
        st.session_state[key] = canon
    elif cat in options:
        st.session_state[key] = cat
    else:
        st.session_state.pop(key, None)


def _sync_engineer_widget_value(
    *,
    key: str,
    current_handle: str,
    fe_names: list[str],
    fe_missing: bool,
) -> None:
    """Set engineer widget only when the ticket already has an assignee."""
    handle = (current_handle or "").strip().lstrip("@")
    if not handle:
        st.session_state.pop(key, None)
        return
    if fe_names and not fe_missing:
        target = f"@{handle}"
        fe_opts = [f"@{n}" for n in fe_names]
        st.session_state[key] = target if target in fe_opts else None
    else:
        st.session_state[key] = handle


def _clear_reassign_panels_except(active_prefix: str) -> None:
    """Close reassign forms from other queues so status checks stay aligned."""
    for prefix in (
        "assigned",
        "open",
        "on_hold",
        "investigation",
        "sc_sales_ticket",
        "sc_investigation",
        "sc_design",
    ):
        if prefix != active_prefix:
            st.session_state.pop(_reassign_session_keys(prefix)["show"], None)


def _clear_sc_assignment_edit_panels_except(active_prefix: str) -> None:
    for prefix in ("sc_sales_ticket", "sc_investigation", "sc_design"):
        if prefix != active_prefix:
            st.session_state.pop(_assignment_edit_session_keys(prefix)["show"], None)


def _manual_field_response_session_keys(prefix: str) -> dict[str, str]:
    return {
        "show": f"{prefix}_show_manual_field_response",
        "text": f"{prefix}_mfr_text",
        "responded_by": f"{prefix}_mfr_responded_by",
        "synced_ticket": f"{prefix}_mfr_synced_ticket",
    }


def _canonical_username_stem(stem: str) -> str:
    """Map retired handles (e.g. ibbe → ibeyx)."""
    s = stem.strip().lstrip("@").lower()
    if s.startswith("ibbe"):
        return "ibeyx" + s[4:]
    return s


def _dash_normalize_handle(handle: str) -> str:
    return _canonical_username_stem(handle)


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
    current_handle_2: str = "",
    current_cat: str,
    current_notes: str,
    cats: list[str],
    fe_names: list[str],
    fe_missing: bool,
    skip_engineer: bool = False,
) -> None:
    """Push the selected ticket's values into edit widgets (Streamlit keeps stale keys)."""
    if st.session_state.get(keys["synced_ticket"]) == picked:
        return
    st.session_state[keys["synced_ticket"]] = picked
    if skip_engineer:
        st.session_state.pop(keys["engineer"], None)
        st.session_state.pop(keys.get("engineer_2", ""), None)
    else:
        _sync_engineer_widget_value(
            key=keys["engineer"],
            current_handle=current_handle,
            fe_names=fe_names,
            fe_missing=fe_missing,
        )
        eng2_key = keys.get("engineer_2")
        if eng2_key:
            _sync_engineer_widget_value(
                key=eng2_key,
                current_handle=current_handle_2,
                fe_names=fe_names,
                fe_missing=fe_missing,
            )
    _sync_category_widget_value(
        key=keys["category"],
        current_cat=current_cat,
        options=cats,
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


def _render_dashboard_team_accounts_body() -> None:
    admin_user = _session_dashboard_username()
    if not admin_user:
        return

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
        .bon-login-line-netops {
            font-size: 125% !important;
            line-height: 1.1 !important;
        }
        .bon-login-word-netops {
            color: #FF5A1F !important;
            -webkit-text-fill-color: #FF5A1F !important;
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
        div.st-key-login_shell .stTextInput input,
        [data-testid="stAppViewContainer"] section.main .stTextInput input {
            background-color: #141414 !important;
            color: #e8e6e3 !important;
            border: 1px solid rgba(215, 180, 145, 0.45) !important;
            border-radius: 8px !important;
            -webkit-text-fill-color: #e8e6e3 !important;
            caret-color: #e8e6e3 !important;
        }
        div.st-key-login_shell .stTextInput input:focus,
        [data-testid="stAppViewContainer"] section.main .stTextInput input:focus {
            border-color: #D7B491 !important;
            box-shadow: 0 0 0 1px #D7B491 !important;
        }
        div.st-key-login_shell .stTextInput input:-webkit-autofill,
        div.st-key-login_shell .stTextInput input:-webkit-autofill:hover,
        div.st-key-login_shell .stTextInput input:-webkit-autofill:focus,
        div.st-key-login_shell .stTextInput input:-webkit-autofill:active,
        [data-testid="stAppViewContainer"] section.main .stTextInput input:-webkit-autofill,
        [data-testid="stAppViewContainer"] section.main .stTextInput input:-webkit-autofill:hover,
        [data-testid="stAppViewContainer"] section.main .stTextInput input:-webkit-autofill:focus,
        [data-testid="stAppViewContainer"] section.main .stTextInput input:-webkit-autofill:active {
            -webkit-box-shadow: 0 0 0 1000px #141414 inset !important;
            box-shadow: 0 0 0 1000px #141414 inset !important;
            -webkit-text-fill-color: #e8e6e3 !important;
            caret-color: #e8e6e3 !important;
            border: 1px solid rgba(215, 180, 145, 0.45) !important;
            transition: background-color 99999s ease-out 0s;
        }
        [data-testid="stAppViewContainer"] section.main button[kind="primary"] {
            background: linear-gradient(90deg, #F15A29, #F7931E) !important;
            border: 1px solid rgba(255,255,255,0.1) !important;
        }
        /* Login: hidden default submit (Enter) + Forgot beside Save Password */
        div.st-key-login_enter_submit {
            height: 0 !important;
            max-height: 0 !important;
            overflow: hidden !important;
            margin: 0 !important;
            padding: 0 !important;
            opacity: 0 !important;
            pointer-events: none !important;
            border: none !important;
        }
        div.st-key-login_shell [data-testid="stForm"] [data-testid="column"]:last-child button {
            font-size: 0.72rem !important;
            white-space: nowrap !important;
            padding: 0.28rem 0.45rem !important;
            min-height: 1.75rem !important;
            line-height: 1.15 !important;
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


_SC_SALES_OVERVIEW_ADMIN_LABEL = "Admin"
_EMPTY_ASSIGNEE_TOKENS = frozenset({"", "nan", "none", "null", "nat"})


def _sc_sales_has_field_assignee(raw: object) -> bool:
    """True when ``assigned_to`` is a real field engineer (not null/NaN/empty)."""
    if raw is None:
        return False
    try:
        if pd.isna(raw):
            return False
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    return bool(s) and s.lower() not in _EMPTY_ASSIGNEE_TOKENS


def _sc_sales_attended_by_for_case(*, assigned_to: str | None = None) -> str:
    """Stored ``attended_by`` on the sales row (admin queue)."""
    return _SC_SALES_OVERVIEW_ADMIN_LABEL


def _sc_perf_sales_credit_staff(row: object) -> str:
    """Performance credit: field engineer when dispatched, otherwise **Admin**."""
    if isinstance(row, pd.Series):
        assigned = row.get("assigned_to")
    else:
        d = row if isinstance(row, dict) else {}
        assigned = d.get("assigned_to")
    if _sc_sales_has_field_assignee(assigned):
        return _perf_norm_member(assigned)
    return _SC_SALES_OVERVIEW_ADMIN_LABEL


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
    forgot_clicked = False
    submitted = False

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
                with st.container(key="login_enter_submit"):
                    submitted_enter = st.form_submit_button(
                        "Sign In",
                        use_container_width=True,
                        key="login_form_submit_enter",
                    )
                save_col, forgot_col = st.columns(
                    [1.35, 1.05], vertical_alignment="center"
                )
                with save_col:
                    st.checkbox(
                        "Save Password",
                        key=_LOGIN_SAVE_PW_KEY,
                        help="Stores an encrypted login token in this browser only.",
                    )
                with forgot_col:
                    forgot_clicked = st.form_submit_button(
                        "Forgot Password",
                        use_container_width=True,
                        key="login_form_submit_forgot",
                    )
                submitted_main = st.form_submit_button(
                    "Sign In",
                    use_container_width=True,
                    key="login_form_submit_main",
                )
                submitted = submitted_enter or submitted_main
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
                forgot_clicked = False
                submitted = st.form_submit_button(
                    "Sign In",
                    use_container_width=True,
                    key="login_form_submit_legacy",
                )

    if per_user and forgot_clicked and not submitted:
        st.session_state[_LOGIN_VIEW_KEY] = "forgot_request"
        st.rerun()
        return

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
            nav_col, _ = st.columns([1, 2])
            with nav_col:
                back_clicked = st.form_submit_button("Back to sign in", use_container_width=True)
            submitted = st.form_submit_button("Get reset code", use_container_width=True)

    if back_clicked:
        st.session_state[_LOGIN_VIEW_KEY] = "sign_in"
        st.rerun()
        return

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
            nav_col, _ = st.columns([1, 2])
            with nav_col:
                back_clicked = st.form_submit_button("Back to sign in", use_container_width=True)
            submitted = st.form_submit_button("Set new password", use_container_width=True)

    if back_clicked:
        st.session_state[_LOGIN_VIEW_KEY] = "sign_in"
        st.session_state.pop("_dash_reset_username", None)
        st.session_state.pop("_dash_reset_code_display", None)
        st.rerun()
        return

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
            '<span class="bon-login-line bon-login-line-netops">'
            '<span class="bon-login-word-netops">NetOps</span></span>'
            '<span class="bon-login-line">'
            '<span class="bon-login-word-coverage">Coverage</span> '
            '<span class="bon-login-word-eye">Eye</span></span></h2>',
            unsafe_allow_html=True,
        )

        if per_user_state is None and legacy_pw:
            st.warning(
                "Supabase is unreachable from this PC — using **shared password** login. "
                "Ticket data will not load until the connection works."
            )

        _render_login_supabase_status()

        if view == "sign_in":
            _render_login_sign_in(per_user=per_user, legacy_password=legacy_pw)
        elif view == "forgot_reset":
            _render_login_forgot_reset()
        else:
            _render_login_forgot_request()

    st.stop()


_SUPABASE_HTTP_TIMEOUT_SEC = float(os.getenv("SUPABASE_HTTP_TIMEOUT_SEC", "25"))
_DASH_SUPABASE_DOWN_KEY = "_dash_supabase_unreachable"
_DASH_UNATTENDED_TICK_KEY = "_dash_unattended_last_tick"
_DASH_MISMATCH_CACHE_KEY = "_dash_pending_mismatch_cache"
_DASH_DATA_CACHE_TTL_SEC = max(
    15, int(float(os.getenv("DASH_DATA_CACHE_TTL_SEC", "20") or "20"))
)


def _invalidate_dashboard_data_cache() -> None:
    """Drop cached reads after writes; keep the Supabase client connection."""
    for clearable in (
        _fetch_tickets_cached,
        _fetch_sales_cases_cached,
        _fetch_latest_attendance_ts_cached,
        _cached_field_engineer_usernames,
        _cached_task_categories,
    ):
        clearable.clear()
    st.session_state.pop(_DASH_MISMATCH_CACHE_KEY, None)


def _maybe_run_unattended_close() -> None:
    """Backup path when bot cron/background worker is not running."""
    if st.session_state.get(_DASH_SUPABASE_DOWN_KEY):
        return
    last = st.session_state.get(_DASH_UNATTENDED_TICK_KEY)
    now = datetime.now(timezone.utc)
    if isinstance(last, datetime) and (now - last).total_seconds() < 300:
        return
    try:
        stats = run_unattended_close(
            _get_supabase_client(),
            tickets_table=TICKETS_TABLE,
            attendance_table=ATTENDANCE_LOGS_TABLE,
        )
        st.session_state[_DASH_UNATTENDED_TICK_KEY] = now
        closed = int(stats.get("closed") or 0)
        if closed:
            _invalidate_dashboard_data_cache()
            st.toast(
                f"Marked {closed} ticket(s) unattended and sent to **Needs Review** "
                "(permanent count)."
            )
    except Exception:
        log.exception("dashboard unattended auto-close failed")


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
    raw_status = str(row.get("status") or "").strip()
    status = _normalize_ticket_status_value(row.get("status"))
    if status not in (STATUS_DAILY_TASK, "Open", STATUS_ON_HOLD):
        raise ValueError(
            f"Ticket is {raw_status or status}; manual response is only allowed while "
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
    # Phase 2: close the open visit as 'responded' for this assignee.
    visit_assignee = field_responded_by or assignee_raw or assignee
    _visits_close_responded(
        client,
        ticket_number,
        assignee=visit_assignee,
        response_note=text,
        closed_by="dashboard",
        visit_end=now_iso,
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
    # Phase 2: close visit for the current assignee with outcome 'on_hold'
    _visits_close_open(
        client,
        ticket_number,
        outcome="on_hold",
        closed_by="dashboard",
        visit_end=now_iso,
    )


def _navigate_to_on_hold_queue() -> None:
    st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
    st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_ON_HOLD


def _navigate_to_resolved_queue() -> None:
    st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
    st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_RESOLVED


def _outcome_category_options() -> list[str]:
    cats, _missing = _try_fetch_task_categories()
    if cats:
        return dedupe_canonical_categories(cats)
    return list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)


def _assigned_categories_for_tickets(
    df: pd.DataFrame,
    ticket_ids: list[str],
) -> list[str]:
    if df.empty or "ticket_number" not in df.columns or "task_category" not in df.columns:
        return []
    ids = {str(t).strip() for t in ticket_ids if str(t).strip()}
    if not ids:
        return []
    sub = df[df["ticket_number"].astype(str).isin(ids)]
    return sorted(
        {
            str(c).strip()
            for c in sub["task_category"].dropna().tolist()
            if str(c).strip()
        },
        key=str.lower,
    )


def _resolve_outcome_category(*, assigned: str, chosen: str) -> str:
    picked = canonical_task_category((chosen or "").strip())
    assign = canonical_task_category((assigned or "").strip())
    if not picked or picked == _OUTCOME_SAME_LABEL:
        if not assign:
            raise ValueError(
                "Pick an **outcome category** — this ticket has no assigned category."
            )
        return assign
    return picked


def _build_outcome_log_note(
    *,
    assigned: str,
    outcome: str,
    comment: str | None = None,
    prefix: str | None = None,
) -> str:
    parts: list[str] = []
    if prefix:
        parts.append(prefix.strip())
    assign = (assigned or "").strip()
    out = (outcome or "").strip()
    if assign and out and assign != out:
        parts.append(f"Assigned: {assign} → Outcome: {out}")
    elif out:
        parts.append(f"Outcome: {out}")
    note = (comment or "").strip()
    if note:
        parts.append(note)
    return " — ".join(parts)


def _persist_ticket_resolved_with_outcome(
    ticket_number: str,
    *,
    outcome_category_input: str,
    operator_id: str,
    comment: str | None = None,
    log_action: str = "Resolved",
    log_prefix: str | None = None,
) -> str:
    """Set **Resolved** + ``outcome_category``; keep ``task_category`` unchanged."""
    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    assigned = str(row.get("task_category") or "").strip()
    outcome = _resolve_outcome_category(assigned=assigned, chosen=outcome_category_input)
    op_at = f"@{operator_id.lstrip('@')}"
    note = _build_outcome_log_note(
        assigned=assigned,
        outcome=outcome,
        comment=comment,
        prefix=log_prefix,
    )
    client = _get_supabase_client()
    now_iso = _cc_utc_now_iso()
    payload: dict[str, object] = {
        "status": STATUS_RESOLVED,
        "outcome_category": outcome,
        "updated_at": now_iso,
        "follow_up_at": None,
        "follow_up_note": None,
    }
    _cc_execute_ticket_update(client, payload, ticket_number)
    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=op_at,
        action_type=log_action,
        note=note or None,
    )
    _invalidate_dashboard_data_cache()
    return outcome


def _render_outcome_category_picker(
    *,
    key: str,
    cat_options: list[str],
    assigned_categories: list[str],
) -> str:
    opts = [_OUTCOME_SAME_LABEL, *cat_options]
    for ac in assigned_categories:
        if ac and ac not in opts:
            opts.insert(1, ac)
    if len(assigned_categories) == 1:
        st.caption(f"Assigned category: **{assigned_categories[0]}**")
    elif len(assigned_categories) > 1:
        st.caption(
            "Multiple assigned categories — **Same as assigned** uses each ticket's own "
            "category."
        )
    return st.selectbox(
        "Outcome category",
        options=opts,
        key=key,
        help="What was actually done. Performance reports use outcome, not assignment category.",
    )


def _apply_admin_close_ticket(
    ticket_number: str,
    *,
    comment: str,
    operator_id: str,
    outcome_category_input: str,
) -> None:
    """Admin-only: close without field completion → **Resolved** with required note."""
    text = (comment or "").strip()
    if not text:
        raise ValueError("A comment is required to close this ticket.")

    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    status = _normalize_ticket_status_value(row.get("status"))
    allowed = (STATUS_DAILY_TASK, "Open", STATUS_ON_HOLD, STATUS_UNDER_INVESTIGATION)
    if status not in allowed:
        raw = str(row.get("status") or "").strip()
        raise ValueError(
            f"Ticket **{ticket_number}** is **{raw or '—'}** — **Closed** is only allowed from "
            "**Daily Task**, **Needs Review**, **On Hold**, or **Under Investigation**."
        )

    client = _get_supabase_client()
    now_iso = _cc_utc_now_iso()
    from_label = {
        STATUS_DAILY_TASK: STATUS_DAILY_TASK,
        "Open": "Needs Review",
        STATUS_ON_HOLD: STATUS_ON_HOLD,
        STATUS_UNDER_INVESTIGATION: STATUS_UNDER_INVESTIGATION,
    }.get(status, status)
    assigned = str(row.get("task_category") or "").strip()
    outcome = _resolve_outcome_category(
        assigned=assigned,
        chosen=outcome_category_input,
    )
    log_note = _build_outcome_log_note(
        assigned=assigned,
        outcome=outcome,
        comment=text,
        prefix=f"Admin closed from {from_label}",
    )
    _persist_ticket_resolved_with_outcome(
        ticket_number,
        outcome_category_input=outcome_category_input,
        operator_id=operator_id,
        comment=text,
        log_action="AdminClosed",
        log_prefix=f"Admin closed from {from_label}",
    )
    _visits_close_open(
        client,
        ticket_number,
        outcome="unattended",
        response_note=log_note,
        closed_by="dashboard",
        visit_end=now_iso,
    )


def _fetch_pending_with_response_mismatch_uncached() -> list[str]:
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


@st.cache_data(ttl=_DASH_DATA_CACHE_TTL_SEC, show_spinner=False)
def _fetch_pending_mismatch_cached() -> tuple[str, ...]:
    return tuple(_fetch_pending_with_response_mismatch_uncached())


def _fetch_pending_with_response_mismatch() -> list[str]:
    """Throttled in session; backed by ``cache_data`` to avoid duplicate HTTP per rerun."""
    now = datetime.now(timezone.utc).timestamp()
    cached = st.session_state.get(_DASH_MISMATCH_CACHE_KEY)
    if cached and (now - float(cached[0])) < 90:
        return list(cached[1])
    mismatches = list(_fetch_pending_mismatch_cached())
    st.session_state[_DASH_MISMATCH_CACHE_KEY] = (now, tuple(mismatches))
    return mismatches


@st.cache_data(ttl=_DASH_DATA_CACHE_TTL_SEC, show_spinner=False)
def _fetch_tickets_cached() -> pd.DataFrame:
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


def _fetch_tickets() -> pd.DataFrame:
    return _fetch_tickets_cached()


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
    row_cap = min(limit_per_ticket * max(len(ticket_numbers or [1]), 1), 500)
    try:
        res = q.order("timestamp", desc=True).limit(row_cap).execute()
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


def _ticket_select_editor_key(key_prefix: str) -> str:
    return f"{key_prefix}_ticket_select_editor"


def _sc_case_select_editor_key(key_prefix: str) -> str:
    return f"{key_prefix}_case_select_editor"


def _data_editor_snapshot_key(editor_key: str) -> str:
    return f"{editor_key}_snapshot"


def _ticket_clear_select_flag_key(key_prefix: str) -> str:
    return f"{key_prefix}_clear_ticket_select"


def _sc_clear_select_flag_key(key_prefix: str) -> str:
    return f"{key_prefix}_clear_case_select"


def _reset_data_editor_queue_selection(*, sel_key: str, editor_key: str) -> None:
    """Drop list selection and reset the data_editor widget so checkboxes clear."""
    st.session_state[sel_key] = []
    st.session_state.pop(editor_key, None)
    snap_key = _data_editor_snapshot_key(editor_key)
    snap = st.session_state.get(snap_key)
    if isinstance(snap, pd.DataFrame) and not snap.empty and "Select" in snap.columns:
        cleared = snap.copy()
        cleared["Select"] = False
        st.session_state[snap_key] = cleared


def _clear_ticket_queue_selection(key_prefix: str) -> None:
    _reset_data_editor_queue_selection(
        sel_key=_ticket_selection_session_key(key_prefix),
        editor_key=_ticket_select_editor_key(key_prefix),
    )


def _clear_sales_case_queue_selection(key_prefix: str) -> None:
    _reset_data_editor_queue_selection(
        sel_key=_sc_case_selection_session_key(key_prefix),
        editor_key=_sc_case_select_editor_key(key_prefix),
    )
    _sc_clear_work_panel_tabs(key_prefix)


def _maybe_apply_pending_ticket_selection_clear(key_prefix: str) -> None:
    if st.session_state.pop(_ticket_clear_select_flag_key(key_prefix), False):
        _clear_ticket_queue_selection(key_prefix)


def _maybe_apply_pending_sales_case_selection_clear(key_prefix: str) -> None:
    if st.session_state.pop(_sc_clear_select_flag_key(key_prefix), False):
        _clear_sales_case_queue_selection(key_prefix)


def _apply_data_editor_editing_state(
    df: pd.DataFrame, state: dict[str, object]
) -> pd.DataFrame:
    """Apply Streamlit ``EditingState`` (edited_rows / deleted_rows) to a table snapshot."""
    out = df.copy()
    edited_rows = state.get("edited_rows")
    if isinstance(edited_rows, dict):
        for row_id, row_changes in edited_rows.items():
            try:
                row_pos = int(row_id)
            except (TypeError, ValueError):
                continue
            if row_pos < 0 or row_pos >= len(out) or not isinstance(row_changes, dict):
                continue
            for col_name, value in row_changes.items():
                if col_name in out.columns:
                    out.iloc[row_pos, out.columns.get_loc(col_name)] = value
    deleted = state.get("deleted_rows")
    if isinstance(deleted, list) and deleted:
        drop_idx = sorted(
            {int(i) for i in deleted if 0 <= int(i) < len(out)},
            reverse=True,
        )
        for i in drop_idx:
            out = out.drop(out.index[i])
        out = out.reset_index(drop=True)
    return out


def _selection_from_data_editor_state(
    editor_key: str,
    *,
    id_column_candidates: tuple[str, ...],
    options: list[str],
) -> list[str] | None:
    """Read checkbox column from the data_editor widget state (fresher than deferred sync)."""
    raw = st.session_state.get(editor_key)
    if raw is None:
        return None
    edited: pd.DataFrame | None = None
    if isinstance(raw, pd.DataFrame):
        edited = raw
    elif isinstance(raw, dict):
        if "edited_rows" in raw or "added_rows" in raw or "deleted_rows" in raw:
            snap = st.session_state.get(_data_editor_snapshot_key(editor_key))
            if isinstance(snap, pd.DataFrame) and not snap.empty:
                edited = _apply_data_editor_editing_state(snap, raw)
        else:
            data = raw.get("data")
            if data is not None:
                cols = raw.get("columns")
                if isinstance(cols, list) and cols and isinstance(cols[0], dict):
                    names = [str(c.get("name") or c.get("field") or "") for c in cols]
                    edited = pd.DataFrame(data, columns=names)
                else:
                    edited = pd.DataFrame(data)
    if edited is None or edited.empty or "Select" not in edited.columns:
        return None
    id_col = next((c for c in id_column_candidates if c in edited.columns), None)
    if not id_col:
        return None
    allowed = set(options)
    select_on = edited["Select"].fillna(False).astype(bool)
    return [
        str(t)
        for t in edited.loc[select_on, id_col].astype(str).tolist()
        if str(t) in allowed
    ]


def _get_selected_queue_tickets(key_prefix: str, options: list[str]) -> list[str]:
    sel_key = _ticket_selection_session_key(key_prefix)
    synced = _selection_from_data_editor_state(
        _ticket_select_editor_key(key_prefix),
        id_column_candidates=("ticket_number", "Ticket Number"),
        options=options,
    )
    if synced is not None:
        st.session_state[sel_key] = synced
    allowed = set(options)
    raw = st.session_state.get(sel_key, [])
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
    """Search ticket #, resort/company, attended by, or category."""
    raw = (query or "").strip()
    if not raw or df.empty:
        return df
    lower = raw.lower()
    mask = pd.Series(False, index=df.index)
    for col in ("case_ref", "account_name", "attended_by", "sales_category"):
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


def _follow_up_labels_by_ticket(df: pd.DataFrame) -> dict[str, str]:
    """Map ticket_number → follow-up display label (avoids row-order mismatches)."""
    if df.empty or "ticket_number" not in df.columns:
        return {}
    labels: dict[str, str] = {}
    for _, row in df.iterrows():
        tn = str(row.get("ticket_number") or "").strip()
        if not tn:
            continue
        labels[tn] = _follow_up_display_label(row)
    return labels


def _render_selectable_ticket_table(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    cols: tuple[str, ...],
    highlight_follow_up: bool = False,
    show_selection_caption: bool = True,
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
    # Keep follow-up pin order; do not re-sort newest-first on top of it.
    view = _ticket_queue_view(filtered, cols=cols, preserve_order=highlight_follow_up)
    if highlight_follow_up and not view.empty and "follow_up_at" in filtered.columns:
        fu_labels = _follow_up_labels_by_ticket(filtered)
        view.insert(
            0,
            "Follow-up",
            view["ticket_number"]
            .astype(str)
            .map(lambda tn: fu_labels.get(str(tn).strip(), ""))
            .tolist(),
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
    editor_key = _ticket_select_editor_key(key_prefix)
    st.session_state[_data_editor_snapshot_key(editor_key)] = table.copy()
    edited = st.data_editor(
        table,
        hide_index=True,
        use_container_width=True,
        key=editor_key,
        column_config=col_cfg,
        disabled=disabled_cols,
    )

    select_on = edited["Select"].fillna(False).astype(bool)
    selected = [
        str(t)
        for t in edited.loc[select_on, "ticket_number"].astype(str).tolist()
        if str(t) in options
    ]
    st.session_state[sel_key] = selected
    if show_selection_caption:
        if selected:
            shown = ", ".join(selected[:6])
            extra = f" (+{len(selected) - 6} more)" if len(selected) > 6 else ""
            st.caption(f"**{len(selected)}** selected: {shown}{extra}")
        else:
            st.caption("Tick **Select** on ticket(s) to show actions.")
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
    raw_status = str(row.get("status") or "").strip()
    status = _normalize_ticket_status_value(row.get("status"))
    if status not in ("Open", STATUS_DAILY_TASK, STATUS_ON_HOLD):
        raise ValueError(
            f"Ticket **{ticket_number}** is **{raw_status or status or '—'}** — "
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


def _render_ticket_transfer_to_sales_popover(
    *,
    key_prefix: str,
    options: list[str],
) -> None:
    with st.popover("Move to Sales", use_container_width=True):
        picked_list = _get_selected_queue_tickets(key_prefix, options)
        if not picked_list:
            st.caption("Select ticket(s), then open **Move to Sales** again.")
            return
        st.markdown("**" + "**, **".join(picked_list[:12]) + "**")
        if len(picked_list) > 12:
            st.caption(f"+ {len(picked_list) - 12} more")
        st.caption(
            "Creates a **Sales case** and removes the CSM ticket. "
            "**Log** keeps attendance history."
        )
        op = _session_operator_id()
        if not op:
            st.warning("Sign in with an **Operator ID** first.")
            return
        account_name = ""
        if len(picked_list) == 1:
            account_name = st.text_input(
                "Resort / company name (optional)",
                placeholder=f"Defaults to Ticket {picked_list[0]}",
                key=f"{key_prefix}_xfer_sales_account",
            )
        region = st.selectbox(
            "Region Team",
            options=list(SALES_REGION_CODES),
            key=f"{key_prefix}_xfer_sales_region",
        )
        confirm = st.checkbox(
            "Yes, move to Sales Cases",
            value=False,
            key=f"{key_prefix}_xfer_sales_confirm",
        )
        if st.button(
            "Move",
            key=f"{key_prefix}_xfer_sales_btn",
            type="secondary",
            use_container_width=True,
            disabled=not confirm,
        ):
            ok = 0
            for picked in picked_list:
                try:
                    an = account_name.strip() if len(picked_list) == 1 else None
                    _cc_transfer_ticket_to_sales_case(
                        picked,
                        operator_id=op,
                        account_name=an,
                        account_region=region,
                    )
                    ok += 1
                except Exception as exc:
                    st.error(f"**{picked}**: {exc}")
            if ok:
                _invalidate_dashboard_data_cache()
                st.session_state[_ticket_selection_session_key(key_prefix)] = []
                st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Sales Cases"
                st.session_state[_DASH_PENDING_SALES_QUEUE_KEY] = _queue_segment_label(
                    SC_STATUS_SALES_TICKET, 1
                )
                st.success(f"Moved **{ok}** ticket(s) to **Sales Cases**.")
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
    status = _normalize_ticket_status_value(row.get("status"))
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
        _invalidate_dashboard_data_cache()
        st.rerun()


def _render_ticket_status_action_popover(
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
    status_labels: list[str],
) -> None:
    """Action picker: trigger shows choice; menu lists options; Apply commits."""
    sel_key = f"{key_prefix}_action_sel"
    if sel_key not in st.session_state:
        st.session_state[sel_key] = ""
    elif st.session_state[sel_key] not in status_labels:
        st.session_state[sel_key] = ""
    current = str(st.session_state.get(sel_key, "") or "")
    trigger = current if current in status_labels else "Action"

    with st.popover(trigger, use_container_width=True):
        st.caption("Choose action — click again to clear")
        for label in status_labels:
            picked = label == current
            if st.button(
                label,
                key=f"{key_prefix}_pick_{label.replace(' ', '_')}",
                type="primary" if picked else "secondary",
                use_container_width=True,
            ):
                st.session_state[sel_key] = "" if picked else label
                st.rerun()
        st.divider()
        if st.button(
            "Apply",
            key=f"{key_prefix}_apply",
            type="primary",
            use_container_width=True,
            disabled=current not in status_labels,
        ):
            choice = str(st.session_state.get(sel_key, ""))
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
                    st.success(f"**{ok}** ticket(s) updated → **{choice}**.")
                    st.session_state[_ticket_selection_session_key(key_prefix)] = []
                    st.rerun()


def _render_mark_follow_up_popover(*, key_prefix: str, options: list[str]) -> None:
    """Tracked individual follow-up (●) — one Open ticket, optional note."""
    with st.popover("Follow-up", use_container_width=True):
        picked = _get_selected_queue_tickets(key_prefix, options)
        if not picked:
            st.caption(
                "Tick **Select** on a ticket in the table below, then open **Follow-up**."
            )
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
            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
            st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_UNDER_INVESTIGATION
            st.session_state[_CC_FLASH_KEY] = (
                f"**{ticket}** → **Under Investigation** (follow-up tracked ●)."
            )
            st.session_state[_CC_FLASH_LEVEL_KEY] = "success"
            st.rerun()


def _split_ticket_status_actions(
    status_actions: tuple[tuple[str, str, str], ...],
) -> tuple[tuple[str, str, str] | None, list[tuple[str, str, str]]]:
    """Prefer **Mark Resolved** as the primary status action; others go to overflow."""
    resolved: tuple[str, str, str] | None = None
    other: list[tuple[str, str, str]] = []
    for item in status_actions:
        label, status, _ = item
        if status == STATUS_RESOLVED or "resolved" in label.strip().lower():
            if resolved is None:
                resolved = item
            else:
                other.append(item)
        else:
            other.append(item)
    return resolved, other


def _apply_ticket_status_batch(
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
    choice_label: str,
) -> None:
    picked_list = _require_selected_tickets(key_prefix=key_prefix, options=options)
    if not picked_list:
        return
    ok = 0
    for picked in picked_list:
        if _apply_admin_ticket_action(
            picked=picked,
            choice=choice_label,
            confirm_del=False,
            status_actions=status_actions,
            do_rerun=False,
        ):
            ok += 1
    if ok:
        st.success(f"**{ok}** ticket(s) updated → **{choice_label}**.")
        st.session_state[_ticket_selection_session_key(key_prefix)] = []
        st.rerun()


def _render_admin_close_form_inline(
    *,
    key_prefix: str,
    options: list[str],
    df: pd.DataFrame,
) -> None:
    """Admin close — required comment + outcome category → **Resolved**."""
    picked_list = _get_selected_queue_tickets(key_prefix, options)
    if not picked_list:
        st.caption("Select ticket(s) in the table first.")
        return
    shown = ", ".join(picked_list[:4])
    extra = f" (+{len(picked_list) - 4} more)" if len(picked_list) > 4 else ""
    st.caption(
        f"**{len(picked_list)}** selected · {shown}{extra} · "
        "for customer unavailable / no field visit — moves to **Resolved**."
    )
    assigned_cats = _assigned_categories_for_tickets(df, picked_list)
    outcome_pick = _render_outcome_category_picker(
        key=f"{key_prefix}_admin_close_outcome",
        cat_options=_outcome_category_options(),
        assigned_categories=assigned_cats,
    )
    comment = st.text_area(
        "Close comment",
        placeholder="e.g. Customer unavailable — admin closed without field visit",
        key=f"{key_prefix}_admin_close_comment",
        height=72,
        label_visibility="collapsed",
    )
    if st.button(
        "Confirm close",
        key=f"{key_prefix}_admin_close_confirm",
        type="primary",
        use_container_width=True,
    ):
        op = _session_operator_id()
        if not op:
            st.error("Sign in again — operator session is missing.")
            return
        ok = 0
        for ticket in picked_list:
            try:
                _apply_admin_close_ticket(
                    ticket,
                    comment=comment,
                    operator_id=op,
                    outcome_category_input=outcome_pick,
                )
                ok += 1
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"**{ticket}**: {exc}")
        if ok:
            st.session_state[_ticket_selection_session_key(key_prefix)] = []
            _navigate_to_resolved_queue()
            st.session_state[_CC_FLASH_KEY] = (
                f"Closed **{ok}** ticket(s) → **Resolved**."
            )
            st.session_state[_CC_FLASH_LEVEL_KEY] = "success"
            st.rerun()


def _render_mark_resolved_form_inline(
    *,
    key_prefix: str,
    options: list[str],
    df: pd.DataFrame,
    log_action: str = "Resolved",
) -> None:
    """Mark selected ticket(s) **Resolved** with an outcome category."""
    picked_list = _get_selected_queue_tickets(key_prefix, options)
    if not picked_list:
        st.caption("Select ticket(s) in the table first.")
        return
    shown = ", ".join(picked_list[:4])
    extra = f" (+{len(picked_list) - 4} more)" if len(picked_list) > 4 else ""
    st.caption(f"**{len(picked_list)}** selected · {shown}{extra}")
    assigned_cats = _assigned_categories_for_tickets(df, picked_list)
    outcome_pick = _render_outcome_category_picker(
        key=f"{key_prefix}_mark_resolved_outcome",
        cat_options=_outcome_category_options(),
        assigned_categories=assigned_cats,
    )
    comment = st.text_area(
        "Note (optional)",
        placeholder="e.g. Coverage OK on site — no installation required",
        key=f"{key_prefix}_mark_resolved_note",
        height=60,
        label_visibility="collapsed",
    )
    if st.button(
        "Confirm resolved",
        key=f"{key_prefix}_mark_resolved_confirm",
        type="primary",
        use_container_width=True,
    ):
        op = _session_operator_id()
        if not op:
            st.error("Sign in again — operator session is missing.")
            return
        ok = 0
        for ticket in picked_list:
            try:
                _persist_ticket_resolved_with_outcome(
                    ticket,
                    outcome_category_input=outcome_pick,
                    operator_id=op,
                    comment=comment or None,
                    log_action=log_action,
                )
                ok += 1
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"**{ticket}**: {exc}")
        if ok:
            st.session_state[_ticket_selection_session_key(key_prefix)] = []
            _navigate_to_resolved_queue()
            st.session_state[_CC_FLASH_KEY] = (
                f"Marked **{ok}** ticket(s) → **Resolved**."
            )
            st.session_state[_CC_FLASH_LEVEL_KEY] = "success"
            st.rerun()


def _render_follow_up_form_inline(*, key_prefix: str, options: list[str]) -> None:
    """Follow-up form (used inside overflow menu)."""
    picked = _get_selected_queue_tickets(key_prefix, options)
    if len(picked) != 1:
        st.caption("Select **exactly one** ticket for follow-up.")
        return
    ticket = picked[0]
    st.markdown(f"**{ticket}**")
    note = st.text_area(
        "Follow-up Note (Optional)",
        placeholder="e.g. Revisit Tuesday — waiting for site access",
        key=f"{key_prefix}_follow_up_note",
        height=72,
        label_visibility="collapsed",
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
        st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
        st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_UNDER_INVESTIGATION
        st.session_state[_CC_FLASH_KEY] = (
            f"**{ticket}** → **Under Investigation** (follow-up tracked ●)."
        )
        st.session_state[_CC_FLASH_LEVEL_KEY] = "success"
        st.rerun()


def _render_ticket_overflow_menu(
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
    overflow_status: list[tuple[str, str, str]],
    allow_reassign: bool,
    allow_edit_assignment: bool,
    allow_mark_follow_up: bool,
    allow_transfer_to_sales: bool,
    allow_delete: bool,
) -> None:
    """Secondary actions + destructive remove."""
    edit_keys = _assignment_edit_session_keys(key_prefix)
    reassign_keys = _reassign_session_keys(key_prefix)
    picked = _get_selected_queue_tickets(key_prefix, options)

    with st.popover("⋯", use_container_width=False):
        if allow_reassign:
            if st.button(
                "Reassign",
                key=f"{key_prefix}_ctx_reassign",
                use_container_width=True,
            ):
                if st.session_state.get(reassign_keys["show"]):
                    st.session_state.pop(reassign_keys["show"], None)
                else:
                    _clear_reassign_panels_except(key_prefix)
                    st.session_state.pop(edit_keys["show"], None)
                    st.session_state[reassign_keys["show"]] = True
                st.rerun()
        if allow_edit_assignment:
            if st.button(
                "Edit assignment",
                key=f"{key_prefix}_ctx_edit",
                use_container_width=True,
            ):
                if st.session_state.get(edit_keys["show"]):
                    st.session_state.pop(edit_keys["show"], None)
                else:
                    st.session_state[edit_keys["show"]] = True
                st.rerun()
        if allow_mark_follow_up:
            with st.expander("Follow-up", expanded=False):
                _render_follow_up_form_inline(key_prefix=key_prefix, options=options)
        if allow_transfer_to_sales and _session_operator_id():
            with st.expander("Move to Sales", expanded=False):
                picked_list = picked
                if not picked_list:
                    st.caption("No tickets selected.")
                else:
                    st.markdown("**" + "**, **".join(picked_list[:8]) + "**")
                    region = st.selectbox(
                        "Region Team",
                        options=list(SALES_REGION_CODES),
                        key=f"{key_prefix}_ctx_xfer_region",
                        label_visibility="collapsed",
                    )
                    confirm = st.checkbox(
                        "Yes, move to Sales Cases",
                        key=f"{key_prefix}_ctx_xfer_confirm",
                    )
                    if st.button(
                        "Move",
                        key=f"{key_prefix}_ctx_xfer_btn",
                        disabled=not confirm,
                        use_container_width=True,
                    ):
                        op = _session_operator_id() or ""
                        ok = 0
                        for tid in picked_list:
                            try:
                                _cc_transfer_ticket_to_sales_case(
                                    tid, operator_id=op, account_region=region
                                )
                                ok += 1
                            except Exception as exc:
                                st.error(f"**{tid}**: {exc}")
                        if ok:
                            _invalidate_dashboard_data_cache()
                            st.session_state[
                                _ticket_selection_session_key(key_prefix)
                            ] = []
                            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = (
                                "Sales Cases"
                            )
                            _cc_set_flash(
                                f"Moved **{ok}** ticket(s) to **Sales Cases**."
                            )
                            st.rerun()
        for label, _status, _log in overflow_status:
            if st.button(
                label,
                key=f"{key_prefix}_ctx_status_{label.replace(' ', '_')}",
                use_container_width=True,
            ):
                _apply_ticket_status_batch(
                    key_prefix=key_prefix,
                    options=options,
                    status_actions=status_actions,
                    choice_label=label,
                )
        if allow_delete:
            st.divider()
            with st.container(key=f"{key_prefix}_ctx_remove"):
                picked_list = picked
                if not picked_list:
                    st.caption("No tickets selected.")
                else:
                    st.caption("**" + "**, **".join(picked_list[:6]) + "**")
                    confirm_del = st.checkbox(
                        "Yes, remove permanently",
                        key=f"{key_prefix}_ctx_del_confirm",
                    )
                    if st.button(
                        "Remove",
                        key=f"{key_prefix}_ctx_del_btn",
                        use_container_width=True,
                        disabled=not confirm_del,
                    ):
                        ok = 0
                        for tid in picked_list:
                            try:
                                _delete_ticket(tid, delete_telegram=False)
                                ok += 1
                            except Exception as exc:
                                _delete_ticket_error_ui(tid, exc)
                        if ok:
                            st.success(f"Removed **{ok}** ticket(s).")
                            st.session_state[
                                _ticket_selection_session_key(key_prefix)
                            ] = []
                            st.rerun()


def _render_ticket_table_selection_hint(key_prefix: str, options: list[str]) -> None:
    """Deprecated — use :func:`_render_ticket_queue_actions_row`."""
    _render_ticket_queue_actions_row(
        pd.DataFrame(),
        key_prefix=key_prefix,
        options_override=options,
    )


def _render_ticket_actions_popover(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
    allow_delete: bool = True,
    allow_transfer_to_sales: bool = True,
    allow_edit_assignment: bool = False,
    allow_manual_field_response: bool = False,
    allow_reassign: bool = False,
    allow_mark_follow_up: bool = False,
    allow_admin_close: bool = False,
) -> None:
    """Actions menu — only enabled when at least one ticket is selected."""
    picked = _get_selected_queue_tickets(key_prefix, options)
    with st.popover("Actions", width=_QUEUE_ACTIONS_POPOVER_WIDTH_PX):
        with st.container(key=f"{key_prefix}_queue_actions_pop"):
            if not picked:
                st.caption(
                    "Tick **Select** on at least one ticket in the table below. "
                    "No action runs until you do."
                )
                return
            shown = ", ".join(picked[:6])
            extra = f" (+{len(picked) - 6} more)" if len(picked) > 6 else ""
            st.caption(f"**{len(picked)}** selected · {shown}{extra}")
            st.divider()
            _render_ticket_actions_popover_body(
                df,
                key_prefix=key_prefix,
                options=options,
                status_actions=status_actions,
                allow_delete=allow_delete,
                allow_transfer_to_sales=allow_transfer_to_sales,
                allow_edit_assignment=allow_edit_assignment,
                allow_manual_field_response=allow_manual_field_response,
                allow_reassign=allow_reassign,
                allow_mark_follow_up=allow_mark_follow_up,
                allow_admin_close=allow_admin_close,
            )


def _render_ticket_actions_popover_body(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
    allow_delete: bool,
    allow_transfer_to_sales: bool,
    allow_edit_assignment: bool,
    allow_manual_field_response: bool,
    allow_reassign: bool,
    allow_mark_follow_up: bool,
    allow_admin_close: bool,
) -> None:
    """Action buttons inside the Actions popover (selection already verified)."""
    if not _is_dashboard_admin():
        status_actions = tuple(a for a in status_actions if a[2] != "OnHold")
    resolved_action, overflow_status = _split_ticket_status_actions(status_actions)
    status_buttons: list[tuple[str, str, str]] = list(overflow_status)
    mfr_keys = _manual_field_response_session_keys(key_prefix)
    edit_keys = _assignment_edit_session_keys(key_prefix)
    reassign_keys = _reassign_session_keys(key_prefix)
    btn_kw = {"use_container_width": True}
    has_workflow = allow_reassign or allow_edit_assignment

    if allow_manual_field_response:
        if st.button(
            "Record response",
            key=f"{key_prefix}_pop_mfr",
            type="secondary",
            **btn_kw,
        ):
            if st.session_state.get(mfr_keys["show"]):
                st.session_state.pop(mfr_keys["show"], None)
            else:
                st.session_state[mfr_keys["show"]] = True
            st.rerun()
    if resolved_action:
        with st.expander(resolved_action[0], expanded=False):
            _render_mark_resolved_form_inline(
                key_prefix=key_prefix,
                options=options,
                df=df,
                log_action=resolved_action[2],
            )
    for label, _status, _log in status_buttons:
        if st.button(
            label,
            key=f"{key_prefix}_pop_status_{label.replace(' ', '_')}",
            type="secondary",
            **btn_kw,
        ):
            _apply_ticket_status_batch(
                key_prefix=key_prefix,
                options=options,
                status_actions=status_actions,
                choice_label=label,
            )

    has_primary = (
        allow_manual_field_response
        or bool(status_buttons)
        or bool(resolved_action)
    )
    has_below_primary = (
        has_workflow
        or allow_mark_follow_up
        or (allow_admin_close and _is_dashboard_admin())
        or (allow_transfer_to_sales and _session_operator_id())
        or allow_delete
    )
    if has_primary and has_below_primary:
        st.divider()

    if allow_reassign:
        if st.button(
            "Reassign",
            key=f"{key_prefix}_pop_reassign",
            type="secondary",
            **btn_kw,
        ):
            if st.session_state.get(reassign_keys["show"]):
                st.session_state.pop(reassign_keys["show"], None)
            else:
                _clear_reassign_panels_except(key_prefix)
                st.session_state.pop(edit_keys["show"], None)
                st.session_state[reassign_keys["show"]] = True
            st.rerun()
    if allow_edit_assignment:
        if st.button(
            "Edit assignment",
            key=f"{key_prefix}_pop_edit",
            type="secondary",
            **btn_kw,
        ):
            if st.session_state.get(edit_keys["show"]):
                st.session_state.pop(edit_keys["show"], None)
            else:
                st.session_state[edit_keys["show"]] = True
            st.rerun()

    if allow_mark_follow_up:
        with st.expander("Follow-up", expanded=False):
            _render_follow_up_form_inline(key_prefix=key_prefix, options=options)
    if allow_admin_close and _is_dashboard_admin():
        with st.expander("Closed", expanded=False):
            _render_admin_close_form_inline(
                key_prefix=key_prefix,
                options=options,
                df=df,
            )
    if allow_transfer_to_sales and _session_operator_id():
        with st.expander("Move to Sales", expanded=False):
            picked_list = _get_selected_queue_tickets(key_prefix, options)
            if picked_list:
                region = st.selectbox(
                    "Region",
                    options=list(SALES_REGION_CODES),
                    key=f"{key_prefix}_pop_xfer_region",
                )
                confirm = st.checkbox(
                    "Confirm move to Sales",
                    key=f"{key_prefix}_pop_xfer_confirm",
                )
                if st.button(
                    "Move",
                    key=f"{key_prefix}_pop_xfer_btn",
                    type="secondary",
                    disabled=not confirm,
                    **btn_kw,
                ):
                    op = _session_operator_id() or ""
                    ok = 0
                    for tid in picked_list:
                        try:
                            _cc_transfer_ticket_to_sales_case(
                                tid, operator_id=op, account_region=region
                            )
                            ok += 1
                        except Exception as exc:
                            st.error(f"**{tid}**: {exc}")
                    if ok:
                        _invalidate_dashboard_data_cache()
                        st.session_state[_ticket_selection_session_key(key_prefix)] = []
                        st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Sales Cases"
                        _cc_set_flash(f"Moved **{ok}** ticket(s) to **Sales Cases**.")
                        st.rerun()

    if allow_delete:
        st.divider()
        with st.container(key=f"{key_prefix}_ctx_remove"):
            confirm_del = st.checkbox(
                "Confirm permanent remove",
                key=f"{key_prefix}_pop_del_confirm",
            )
            if st.button(
                "Remove",
                key=f"{key_prefix}_pop_del_btn",
                type="secondary",
                disabled=not confirm_del,
                **btn_kw,
            ):
                picked_list = _get_selected_queue_tickets(key_prefix, options)
                ok = 0
                for tid in picked_list:
                    try:
                        _delete_ticket(tid, delete_telegram=False)
                        ok += 1
                    except Exception as exc:
                        _delete_ticket_error_ui(tid, exc)
                if ok:
                    st.success(f"Removed **{ok}** ticket(s).")
                    st.session_state[_ticket_selection_session_key(key_prefix)] = []
                    st.rerun()


def _render_ticket_queue_actions_row(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    options_override: list[str] | None = None,
    **toolbar_kwargs: object,
) -> None:
    """Selection summary + Actions popover above the table."""
    options = options_override or _ticket_options_for_admin(df)
    if not options:
        return
    picked = _get_selected_queue_tickets(key_prefix, options)
    sel_key = _ticket_selection_session_key(key_prefix)

    with st.container(key=f"{key_prefix}_ctx_toolbar"):
        left, right = st.columns([4.1, 1.05], vertical_alignment="center", gap="small")
        with left:
            if picked:
                lc1, lc2 = st.columns([1.2, 1.3], vertical_alignment="center")
                with lc1:
                    word = "ticket" if len(picked) == 1 else "tickets"
                    st.markdown(f"**{len(picked):,}** {word} selected")
                with lc2:
                    if st.button(
                        "Clear selection",
                        key=f"{key_prefix}_ctx_clear",
                        type="secondary",
                    ):
                        st.session_state[_ticket_clear_select_flag_key(key_prefix)] = True
                        st.rerun()
            else:
                st.caption(
                    "Tick **Select** on ticket(s) in the table below, then open **Actions**."
                )
        with right:
            _render_ticket_actions_popover(df, key_prefix=key_prefix, options=options, **toolbar_kwargs)


def _render_admin_ticket_toolbar(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    caption: str | None = None,
    status_actions: tuple[tuple[str, str, str], ...] = (),
    allow_delete: bool = True,
    allow_transfer_to_sales: bool = True,
    allow_edit_assignment: bool = False,
    allow_manual_field_response: bool = False,
    allow_reassign: bool = False,
    allow_mark_follow_up: bool = False,
    allow_admin_close: bool = False,
    selected: list[str] | None = None,
) -> None:
    """Legacy entry point — delegates to the queue actions row + Actions popover."""
    if caption:
        st.caption(caption)
    _render_ticket_queue_actions_row(
        df,
        key_prefix=key_prefix,
        status_actions=status_actions,
        allow_delete=allow_delete,
        allow_transfer_to_sales=allow_transfer_to_sales,
        allow_edit_assignment=allow_edit_assignment,
        allow_manual_field_response=allow_manual_field_response,
        allow_reassign=allow_reassign,
        allow_mark_follow_up=allow_mark_follow_up,
        allow_admin_close=allow_admin_close,
    )


def _render_ticket_toolbar_then_table(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    cols: tuple[str, ...],
    highlight_follow_up: bool = False,
    **toolbar_kwargs: object,
) -> None:
    """Actions row above table; selection is synced from the editor widget before actions run."""
    caption = toolbar_kwargs.pop("caption", None)
    if caption:
        st.caption(caption)

    _maybe_apply_pending_ticket_selection_clear(key_prefix)
    options = _ticket_options_for_admin(df)
    with st.container(key=f"{key_prefix}_queue_block"):
        _render_selectable_ticket_table(
            df,
            key_prefix=key_prefix,
            cols=cols,
            highlight_follow_up=highlight_follow_up,
            show_selection_caption=True,
        )
        if options:
            _render_ticket_queue_actions_row(
                df, key_prefix=key_prefix, **toolbar_kwargs
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


def _visits_deactivate_ticket(client, ticket_number: str) -> None:
    """Mark all active visits for a ticket inactive before opening a new cycle."""
    try:
        client.table(TICKET_VISITS_TABLE).update({"is_active": False}).eq(
            "ticket_number", str(ticket_number).strip()
        ).eq("is_active", True).execute()
    except Exception:
        pass


def _normalize_visit_assignee(raw: object) -> str:
    """Canonical @username for ticket_visits.assignee."""
    s = str(raw or "").strip().lstrip("@")
    return f"@{s.lower()}" if s else ""


def _visits_current_assignee(client, ticket_number: str) -> str | None:
    """Current field engineer from the active visit row (source of truth)."""
    row = _visits_open_visit(client, ticket_number)
    if not row:
        return None
    assignee = str(row.get("assignee") or "").strip()
    return assignee or None


def _visits_open_visit(client, ticket_number: str) -> dict | None:
    """Return the current active visit row for a ticket, or None."""
    tn = str(ticket_number).strip()
    try:
        res = (
            client.table(TICKET_VISITS_TABLE)
            .select("*")
            .eq("ticket_number", tn)
            .eq("is_active", True)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if rows:
            return rows[0]
    except Exception:
        pass
    # Legacy rows before is_active migration.
    try:
        res = (
            client.table(TICKET_VISITS_TABLE)
            .select("*")
            .eq("ticket_number", tn)
            .is_("visit_end", "null")
            .limit(1)
            .execute()
        )
        rows = res.data or []
        return rows[0] if rows else None
    except Exception:
        return None


def _visits_open_new(
    client,
    ticket_number: str,
    assignee: str,
    *,
    visit_start: str | None = None,
) -> None:
    """Insert a new open visit row (outcome = 'assigned', is_active = true)."""
    tn = str(ticket_number).strip()
    try:
        _visits_deactivate_ticket(client, tn)
        client.table(TICKET_VISITS_TABLE).insert(
            {
                "ticket_number": tn,
                "assignee": _normalize_visit_assignee(assignee),
                "visit_start": visit_start or _cc_utc_now_iso(),
                "visit_end": None,
                "outcome": "assigned",
                "closed_by": "dashboard",
                "is_active": True,
            }
        ).execute()
    except Exception:
        pass


def _visits_close_open(
    client,
    ticket_number: str,
    *,
    outcome: str,
    response_note: str | None = None,
    photo_url: str | None = None,
    closed_by: str = "dashboard",
    visit_end: str | None = None,
    assignee: str | None = None,
) -> None:
    """Close the active visit for a ticket (set visit_end + outcome)."""
    tn = str(ticket_number).strip()
    end_ts = visit_end or _cc_utc_now_iso()
    payload = {
        "visit_end": end_ts,
        "outcome": outcome,
        "response_note": response_note,
        "photo_url": photo_url,
        "closed_by": closed_by,
        "is_active": False,
    }

    def _apply(update_q):
        try:
            update_q.execute()
        except Exception:
            pass

    try:
        q = (
            client.table(TICKET_VISITS_TABLE)
            .update(payload)
            .eq("ticket_number", tn)
            .eq("is_active", True)
        )
        if assignee:
            q = q.eq("assignee", _normalize_visit_assignee(assignee))
        _apply(q)
    except Exception:
        pass

    if assignee:
        # Fallback: close by ticket only if assignee filter matched nothing (legacy rows).
        try:
            active = (
                client.table(TICKET_VISITS_TABLE)
                .select("id")
                .eq("ticket_number", tn)
                .eq("is_active", True)
                .limit(1)
                .execute()
            )
            if active.data:
                _apply(
                    client.table(TICKET_VISITS_TABLE)
                    .update(payload)
                    .eq("ticket_number", tn)
                    .eq("is_active", True)
                )
        except Exception:
            pass

    # Legacy rows before is_active migration.
    try:
        q = (
            client.table(TICKET_VISITS_TABLE)
            .update(payload)
            .eq("ticket_number", tn)
            .is_("visit_end", "null")
        )
        if assignee:
            q = q.eq("assignee", _normalize_visit_assignee(assignee))
        _apply(q)
    except Exception:
        pass


def _visits_close_responded(
    client,
    ticket_number: str,
    *,
    assignee: str,
    response_note: str | None = None,
    photo_url: str | None = None,
    closed_by: str = "dashboard",
    visit_end: str | None = None,
) -> None:
    """Close the active visit for this ticket + engineer as responded."""
    _visits_close_open(
        client,
        ticket_number,
        outcome="responded",
        response_note=response_note,
        photo_url=photo_url,
        closed_by=closed_by,
        visit_end=visit_end,
        assignee=assignee,
    )


def _visits_reassign(
    client,
    ticket_number: str,
    new_assignee: str,
    *,
    now_iso: str | None = None,
) -> None:
    """Close current open visit as 'reassigned', open new visit for new_assignee."""
    ts = now_iso or _cc_utc_now_iso()
    _visits_close_open(
        client,
        ticket_number,
        outcome="reassigned",
        closed_by="dashboard",
        visit_end=ts,
    )
    _visits_open_new(client, ticket_number, new_assignee, visit_start=ts)


def _fetch_visits_for_tickets(
    ticket_numbers: list[str],
    *,
    since_utc: pd.Timestamp | None = None,
    until_utc: pd.Timestamp | None = None,
    limit: int = 8000,
) -> pd.DataFrame:
    if not ticket_numbers or not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()
    client = _get_supabase_client()
    nums = sorted({str(t).strip() for t in ticket_numbers if str(t).strip()})
    parts: list[pd.DataFrame] = []
    for i in range(0, len(nums), 80):
        chunk = nums[i : i + 80]
        try:
            q = (
                client.table(TICKET_VISITS_TABLE)
                .select("*")
                .in_("ticket_number", chunk)
            )
            if since_utc is not None:
                q = q.gte("visit_start", since_utc.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"))
            if until_utc is not None:
                q = q.lte("visit_start", until_utc.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"))
            res = q.order("visit_start", desc=False).limit(limit).execute()
        except Exception:
            continue
        if res.data:
            parts.append(pd.DataFrame(res.data))
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _fetch_visits_in_range(
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
    *,
    limit: int = 8000,
) -> pd.DataFrame:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()
    client = _get_supabase_client()
    try:
        res = (
            client.table(TICKET_VISITS_TABLE)
            .select("*")
            .gte("visit_start", range_start.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"))
            .lte("visit_start", range_end.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"))
            .order("visit_start", desc=False)
            .limit(limit)
            .execute()
        )
    except Exception:
        return pd.DataFrame()
    rows = res.data or []
    if not rows:
        return pd.DataFrame()
    return _perf_prepare_visits_df(pd.DataFrame(rows))


def _perf_normalize_matrix_lookup(raw: object) -> str:
    """Digits-only ticket id for matrix lookup (9 or 16 digits)."""
    digits = re.sub(r"\D", "", str(raw or "").strip())
    if len(digits) in (9, 16):
        return digits
    return ""


def _perf_matrix_sync_lookup_from_component() -> str:
    """Read Ticket ID search from matrix component state (single search box)."""
    ret = st.session_state.get(_PERF_MATRIX_COMPONENT_KEY)
    if isinstance(ret, dict) and "lookup" in ret:
        raw = str(ret.get("lookup") or "").strip()
        if not raw:
            st.session_state.pop(_PERF_MATRIX_LOOKUP_KEY, None)
            return ""
        normalized = _perf_normalize_matrix_lookup(raw)
        if normalized:
            st.session_state[_PERF_MATRIX_LOOKUP_KEY] = normalized
            return normalized
    elif isinstance(ret, str) and ret.strip():
        normalized = _perf_normalize_matrix_lookup(ret.strip())
        if normalized:
            st.session_state[_PERF_MATRIX_LOOKUP_KEY] = normalized
            return normalized
    return str(st.session_state.get(_PERF_MATRIX_LOOKUP_KEY, "") or "").strip()


def _perf_matrix_merge_ticket_lookup(
    visits_all: pd.DataFrame,
    lookup: str,
) -> pd.DataFrame:
    """Merge visit rows for a ticket id — ignores sidebar date range."""
    tid = _perf_normalize_matrix_lookup(lookup)
    if not tid:
        return visits_all
    extra = _fetch_visits_for_tickets([tid])
    if extra.empty:
        return visits_all
    parts = [df for df in (visits_all, extra) if not df.empty]
    if not parts:
        return extra
    merged = pd.concat(parts, ignore_index=True)
    dedupe_cols = [c for c in ("id", "ticket_number", "assignee", "visit_start") if c in merged.columns]
    if "id" in dedupe_cols:
        merged = merged.drop_duplicates(subset=["id"], keep="last")
    elif {"ticket_number", "assignee", "visit_start"}.issubset(merged.columns):
        merged = merged.drop_duplicates(
            subset=["ticket_number", "assignee", "visit_start"],
            keep="last",
        )
    return _perf_prepare_visits_df(merged)


def _perf_prepare_visits_df(visits: pd.DataFrame) -> pd.DataFrame:
    """Normalize assignee labels and boolean flags for Performance UI."""
    if visits.empty:
        return visits
    out = visits.copy()
    if "assignee" in out.columns:
        out["assignee"] = out["assignee"].map(_normalize_visit_assignee)
        out.loc[out["assignee"].eq(""), "assignee"] = "(unknown)"
    if "is_active" in out.columns:
        out["is_active"] = out["is_active"].fillna(False).astype(bool)
    return out


def _perf_handled_visit_credit_counts(visits: pd.DataFrame) -> pd.DataFrame:
    """Fair credit for Handled: distinct tickets per assignee (+ visit-cycle total)."""
    if visits.empty or "assignee" not in visits.columns:
        return pd.DataFrame(columns=["assignee", "Tickets", "Visit cycles"])
    data = _perf_prepare_visits_df(visits)
    if "ticket_number" not in data.columns:
        grouped = (
            data.groupby("assignee", as_index=False)
            .size()
            .rename(columns={"size": "Tickets"})
        )
        grouped["Visit cycles"] = grouped["Tickets"]
        return grouped.sort_values("Tickets", ascending=False)
    rows: list[dict[str, object]] = []
    for assignee, grp in data.groupby("assignee", sort=False):
        rows.append(
            {
                "assignee": assignee,
                "Tickets": int(grp["ticket_number"].astype(str).nunique()),
                "Visit cycles": int(len(grp)),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["assignee", "Tickets", "Visit cycles"])
    return pd.DataFrame(rows).sort_values(["Tickets", "Visit cycles"], ascending=False)


def _perf_build_visit_summary(visits: pd.DataFrame) -> pd.DataFrame:
    """Per-person visit counts broken down by outcome (visit-cycle accountability)."""
    if visits.empty or "assignee" not in visits.columns:
        return pd.DataFrame()
    visits = _perf_prepare_visits_df(visits)
    g = visits.groupby(["assignee", "outcome"], as_index=False).size().rename(columns={"size": "count"})
    outcomes = ["assigned", "responded", "reassigned", "unattended", "on_hold"]
    people = sorted(visits["assignee"].dropna().unique().tolist(), key=str.lower)
    rows = []
    for person in people:
        pdata = g[g["assignee"] == person]
        person_visits = visits[visits["assignee"] == person]
        row: dict = {"Person": person}
        for o in outcomes:
            o_rows = person_visits[person_visits["outcome"] == o] if "outcome" in person_visits.columns else person_visits.iloc[0:0]
            if o == "responded" and "ticket_number" in o_rows.columns and not o_rows.empty:
                row[o.capitalize()] = int(o_rows["ticket_number"].astype(str).nunique())
            else:
                row[o.capitalize()] = int(pdata.loc[pdata["outcome"] == o, "count"].sum())
        row["Total visits"] = int(pdata["count"].sum())
        if "ticket_number" in person_visits.columns:
            row["Tickets touched"] = int(person_visits["ticket_number"].nunique())
        if "is_active" in person_visits.columns:
            row["Active now"] = int(person_visits["is_active"].sum())
        rows.append(row)
    df = pd.DataFrame(rows)
    return df.sort_values(["Responded", "Total visits"], ascending=[False, False])


def _perf_merge_field_and_visit_summaries(
    field_summary: pd.DataFrame,
    visit_summary: pd.DataFrame,
) -> pd.DataFrame:
    """One overview table: ticket-queue snapshot + visit-cycle responded counts."""
    if field_summary.empty and visit_summary.empty:
        return pd.DataFrame()
    if field_summary.empty:
        return visit_summary
    if visit_summary.empty:
        return field_summary
    left = field_summary.copy()
    right = visit_summary[["Person", "Responded", "Reassigned", "Tickets touched"]].rename(
        columns={
            "Responded": "Visit responded",
            "Reassigned": "Visit reassigned",
            "Tickets touched": "Visit tickets",
        }
    )
    merged = left.merge(right, on="Person", how="outer")
    for col in ("Total", "Handled", "Visit responded"):
        if col in merged.columns:
            merged[col] = merged[col].fillna(0).astype(int)
    sort_cols = [c for c in ("Visit responded", "Handled", "Total") if c in merged.columns]
    if sort_cols:
        return merged.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    return merged


def _perf_focus_people(
    field_summary: pd.DataFrame,
    visits: pd.DataFrame,
    sales_summary: pd.DataFrame | None = None,
) -> list[str]:
    names: set[str] = set()
    if not field_summary.empty and "Person" in field_summary.columns:
        names |= {str(p) for p in field_summary["Person"].tolist() if str(p).strip()}
    if not visits.empty and "assignee" in visits.columns:
        names |= {str(p) for p in visits["assignee"].dropna().unique().tolist() if str(p).strip()}
    if sales_summary is not None and not sales_summary.empty and "Person" in sales_summary.columns:
        names |= {str(p) for p in sales_summary["Person"].tolist() if str(p).strip()}
    return ["All"] + sorted(names, key=str.lower)


def _perf_filter_visits_by_person(visits: pd.DataFrame, person: str) -> pd.DataFrame:
    if visits.empty or person in ("", "All"):
        return visits
    key = _perf_norm_member(person)
    prepared = _perf_prepare_visits_df(visits)
    return prepared.loc[prepared["assignee"] == key].copy()


def _perf_ticket_detail_rows(
    visits: pd.DataFrame,
    *,
    person: str | None = None,
) -> pd.DataFrame:
    """Per ticket: solo vs shared for tickets in the visit window (optionally one engineer)."""
    if visits.empty or "ticket_number" not in visits.columns:
        return pd.DataFrame()
    prepared = _perf_prepare_visits_df(visits)
    if person and person not in ("", "All"):
        key = _perf_norm_member(person)
        touched = prepared.loc[prepared["assignee"] == key, "ticket_number"].astype(str).unique()
    else:
        touched = prepared["ticket_number"].astype(str).unique()
    rows: list[dict[str, object]] = []
    for tn in sorted({str(t).strip() for t in touched if str(t).strip()}):
        tvisits = prepared[prepared["ticket_number"].astype(str) == tn]
        engineers = sorted(tvisits["assignee"].dropna().unique().tolist(), key=str.lower)
        solo = len(engineers) == 1
        responded = False
        if "outcome" in tvisits.columns:
            if person and person not in ("", "All"):
                responded = bool(
                    ((tvisits["assignee"] == key) & (tvisits["outcome"] == "responded")).any()
                )
            else:
                responded = bool((tvisits["outcome"] == "responded").any())
        rows.append(
            {
                "Ticket": tn,
                "Type": "Solo" if solo else "Shared",
                "Engineers": len(engineers),
                "Who was involved": ", ".join(engineers),
                "Engineer responded": "Yes" if responded else "No",
            }
        )
    return pd.DataFrame(rows)


def _perf_solo_shared_ticket_rows(visits: pd.DataFrame, person: str) -> pd.DataFrame:
    """Per ticket: solo vs shared for tickets this engineer touched in the window."""
    if person in ("", "All"):
        return pd.DataFrame()
    return _perf_ticket_detail_rows(visits, person=person)


def _perf_solo_shared_summary_all(visits: pd.DataFrame) -> pd.DataFrame:
    """Solo vs shared ticket counts for every engineer in the visit window."""
    if visits.empty or "assignee" not in visits.columns:
        return pd.DataFrame()
    prepared = _perf_prepare_visits_df(visits)
    people = sorted(prepared["assignee"].dropna().unique().tolist(), key=str.lower)
    rows: list[dict[str, object]] = []
    for person in people:
        detail = _perf_solo_shared_ticket_rows(prepared, person)
        if detail.empty:
            continue
        solo = int((detail["Type"] == "Solo").sum())
        shared = int((detail["Type"] == "Shared").sum())
        rows.append(
            {
                "Person": person,
                "Solo tickets": solo,
                "Shared tickets": shared,
                "Tickets touched": solo + shared,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["Tickets touched", "Solo tickets"],
        ascending=[False, False],
    )


def _perf_solo_shared_board_summary(
    visits: pd.DataFrame,
    sales_summary: pd.DataFrame | None,
) -> pd.DataFrame:
    """Visit solo/shared counts plus Sales Cases staff (e.g. sales-only in window)."""
    visit_summary = (
        _perf_solo_shared_summary_all(visits)
        if not visits.empty
        else pd.DataFrame()
    )
    sales = sales_summary if sales_summary is not None else pd.DataFrame()
    if visit_summary.empty and (sales.empty or "Person" not in sales.columns):
        return pd.DataFrame()
    if sales.empty or "Person" not in sales.columns or "Total" not in sales.columns:
        out = visit_summary.copy()
        if not out.empty:
            out["Sales Cases"] = 0
        return out
    sales_cols = sales[["Person", "Total"]].rename(columns={"Total": "Sales Cases"})
    if visit_summary.empty:
        out = sales_cols.copy()
        out["Solo tickets"] = 0
        out["Shared tickets"] = 0
        out["Tickets touched"] = 0
        return out.sort_values(["Sales Cases", "Person"], ascending=[False, True])
    merged = visit_summary.merge(sales_cols, on="Person", how="outer")
    for col in ("Solo tickets", "Shared tickets", "Tickets touched", "Sales Cases"):
        if col in merged.columns:
            merged[col] = merged[col].fillna(0).astype(int)
    return merged.sort_values(
        ["Tickets touched", "Sales Cases", "Solo tickets"],
        ascending=[False, False, False],
    )


def _perf_count_column_total(
    table: pd.DataFrame,
    column: str,
    *,
    focus: str,
) -> int:
    if table.empty or column not in table.columns:
        return 0
    if focus not in ("", "All"):
        key = _perf_norm_member(focus)
        row = table.loc[table["Person"] == key]
        return int(row[column].sum()) if not row.empty else 0
    return int(table[column].sum())


def _perf_attach_sales_to_overview(
    overview_table: pd.DataFrame,
    sales_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Add per-person Sales Cases counts (field ``Total`` stays field-only)."""
    if sales_summary.empty or "Total" not in sales_summary.columns:
        if overview_table.empty:
            return overview_table
        out = overview_table.copy()
        out["Sales Cases"] = 0
        return out
    sales_col = sales_summary[["Person", "Total"]].rename(
        columns={"Total": "Sales Cases"},
    )
    if overview_table.empty:
        out = sales_col.copy()
        out["Total"] = 0
        out["Handled"] = 0
        return out
    merged = overview_table.merge(sales_col, on="Person", how="outer")
    for col in ("Total", "Sales Cases", "Handled"):
        if col in merged.columns:
            merged[col] = merged[col].fillna(0).astype(int)
    return merged


def _perf_grand_total_for_board(
    overview_table: pd.DataFrame,
    solo_shared_summary: pd.DataFrame,
    *,
    sales_summary: pd.DataFrame | None = None,
    focus: str = "All",
) -> int:
    """Ring total: field queue snapshot + Sales Cases in the sidebar window."""
    field_n = _perf_count_column_total(overview_table, "Total", focus=focus)
    sales_n = _perf_count_column_total(
        sales_summary if sales_summary is not None else pd.DataFrame(),
        "Total",
        focus=focus,
    )
    if field_n or sales_n:
        return field_n + sales_n
    if not solo_shared_summary.empty and "Tickets touched" in solo_shared_summary.columns:
        return _perf_count_column_total(solo_shared_summary, "Tickets touched", focus=focus)
    return 0


def _perf_overview_total_breakdown(
    overview_table: pd.DataFrame,
    *,
    sales_summary: pd.DataFrame | None = None,
    focus: str = "All",
) -> tuple[int, int, int]:
    field_n = _perf_count_column_total(overview_table, "Total", focus=focus)
    sales_n = _perf_count_column_total(
        sales_summary if sales_summary is not None else pd.DataFrame(),
        "Total",
        focus=focus,
    )
    return field_n + sales_n, field_n, sales_n


def _perf_overview_ring_subhtml(field_n: int, sales_n: int) -> str:
    if field_n and sales_n:
        return (
            f'<div class="perf-ss-total-sub">Field {field_n} · Sales {sales_n}</div>'
        )
    if sales_n:
        return f'<div class="perf-ss-total-sub">Sales {sales_n}</div>'
    if field_n:
        return f'<div class="perf-ss-total-sub">Field {field_n}</div>'
    return ""


def _render_perf_queue_strip(summary: pd.DataFrame, *, focus: str) -> None:
    """Thin queue breakdown chips for one engineer or team totals."""
    if summary.empty:
        return
    if focus != "All":
        key = _perf_norm_member(focus)
        row = summary.loc[summary["Person"] == key]
        if row.empty:
            return
        r = row.iloc[0]
    else:
        r = summary.sum(numeric_only=True)
    labels = (
        ("Total", "Field queues"),
        ("Sales Cases", "Sales Cases"),
        (STATUS_DAILY_TASK, STATUS_DAILY_TASK),
        ("Needs Review", "Needs Review"),
        ("Investigation", "Investigation"),
        (STATUS_RESOLVED, STATUS_RESOLVED),
        ("On Hold", "On Hold"),
        ("Unattended", "Unattended"),
        ("Handled", "Handled"),
        ("Visit responded", "Visit responded"),
    )
    chips: list[str] = []
    for col, label in labels:
        if col not in summary.columns:
            continue
        val = int(r.get(col, 0))
        if val == 0 and col not in ("Total", "Sales Cases", "Handled"):
            continue
        chips.append(
            f'<span class="perf-queue-chip">{html.escape(label)}'
            f"<strong>{val}</strong></span>"
        )
    if not chips:
        return
    st.markdown(
        f'<div class="perf-queue-strip">{"".join(chips)}</div>',
        unsafe_allow_html=True,
    )


def _render_perf_solo_shared_board(
    visits_all: pd.DataFrame,
    *,
    focus: str,
    overview_table: pd.DataFrame | None = None,
    sales_summary: pd.DataFrame | None = None,
) -> None:
    """Engineer rows (solo | shared pills) + total ring."""
    overview_table = overview_table if overview_table is not None else pd.DataFrame()
    sales_summary = sales_summary if sales_summary is not None else pd.DataFrame()
    total_n, field_n, sales_n = _perf_overview_total_breakdown(
        overview_table,
        sales_summary=sales_summary,
        focus=focus,
    )
    sub_html = _perf_overview_ring_subhtml(field_n, sales_n)

    summary = _perf_solo_shared_board_summary(visits_all, sales_summary)
    if summary.empty:
        if visits_all.empty and total_n == 0:
            st.markdown(
                '<p class="perf-ss-hint">No field visits or Sales Cases in this window.</p>',
                unsafe_allow_html=True,
            )
        elif visits_all.empty and total_n > 0:
            st.markdown(
                '<p class="perf-ss-hint">No visit data — field counts come from queue snapshot; '
                "Sales Cases: **Admin** if unassigned, else field engineer.</p>",
                unsafe_allow_html=True,
            )
        if total_n > 0:
            st.markdown(
                f'<div class="perf-ss-board"><div class="perf-ss-list"></div>'
                f'<div class="perf-ss-total"><div class="perf-ss-circle">{total_n}</div>'
                f'<div class="perf-ss-total-lbl">in queues</div>{sub_html}</div></div>',
                unsafe_allow_html=True,
            )
        elif summary.empty:
            st.caption("No engineers in visit or Sales Cases data for this window.")
        return

    focus_key = _perf_norm_member(focus) if focus not in ("", "All") else ""
    if total_n == 0:
        total_n = _perf_grand_total_for_board(
            overview_table,
            summary,
            sales_summary=sales_summary,
            focus=focus,
        )
        _, field_n, sales_n = _perf_overview_total_breakdown(
            overview_table,
            sales_summary=sales_summary,
            focus=focus,
        )
        sub_html = _perf_overview_ring_subhtml(field_n, sales_n)
    rows_html: list[str] = []
    for _, row in summary.iterrows():
        person = str(row["Person"])
        solo = int(row.get("Solo tickets", 0))
        shared = int(row.get("Shared tickets", 0))
        sales_n_person = int(row.get("Sales Cases", 0))
        selected = focus_key and person == focus_key
        row_cls = "perf-ss-row is-selected" if selected else "perf-ss-row"
        sales_seg = ""
        if sales_n_person > 0:
            sales_seg = (
                f'<span class="perf-ss-seg sales">sales'
                f'<span class="num">{sales_n_person}</span></span>'
            )
        rows_html.append(
            f'<div class="{row_cls}">'
            f'<span class="perf-ss-name">{html.escape(person)}</span>'
            f'<div class="perf-ss-pill{" has-sales" if sales_n_person else ""}">'
            f'<span class="perf-ss-seg solo">solo<span class="num">{solo}</span></span>'
            f'<span class="perf-ss-seg shared">shared<span class="num">{shared}</span></span>'
            f"{sales_seg}"
            f"</div>"
            f"</div>"
        )

    st.markdown(
        f'<div class="perf-ss-board">'
        f'<div class="perf-ss-list">{"".join(rows_html)}</div>'
        f'<div class="perf-ss-total">'
        f'<div class="perf-ss-circle">{total_n}</div>'
        f'<div class="perf-ss-total-lbl">in queues</div>'
        f"{sub_html}"
        f"</div></div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="perf-ss-hint">Solo / shared = field visit history · '
        "<strong>Sales</strong> = Sales Cases in window · "
        "<strong>Admin</strong> if no field engineer · else <code>assigned_to</code> · "
        "Ring = field queues + Sales Cases (excludes **Unattended** from credit). "
        "Use <strong>Focus Assignee</strong> to highlight a row.</p>",
        unsafe_allow_html=True,
    )


def _render_perf_solo_shared_detail(
    visits_all: pd.DataFrame,
    *,
    focus: str,
    sales_cases: pd.DataFrame | None = None,
) -> None:
    """Field visit ticket list + Sales Cases for Overview (always visible)."""
    all_focus = focus in ("", "All")
    detail = (
        _perf_ticket_detail_rows(
            visits_all,
            person=None if all_focus else focus,
        )
        if not visits_all.empty
        else pd.DataFrame()
    )
    sales_view = pd.DataFrame()
    if sales_cases is not None and not sales_cases.empty:
        sales_view = _perf_enrich_sales_cases(sales_cases)
        if not all_focus:
            sales_view = _perf_filter_by_person(sales_view, focus)
    if detail.empty and sales_view.empty:
        st.caption("No field visit or Sales Case ticket detail in this window.")
        return

    label = "All engineers" if all_focus else focus
    if not detail.empty:
        st.markdown(f"**Field tickets — {label} ({len(detail)})**")
        shared_only = detail[detail["Type"] == "Shared"]
        if not shared_only.empty:
            st.caption(f"**{len(shared_only)}** shared (multiple engineers on the same ticket).")
            st.dataframe(shared_only, use_container_width=True, hide_index=True)
        st.dataframe(detail, use_container_width=True, hide_index=True)

    if not sales_view.empty:
        cols = [
            c
            for c in (
                "ticket_number",
                "account_name",
                "sales_category",
                "status_eff",
                "_local",
            )
            if c in sales_view.columns
        ]
        show = sales_view[cols].copy() if cols else sales_view.copy()
        rename = {
            "ticket_number": "Ticket",
            "account_name": "Account",
            "sales_category": "Category",
            "status_eff": "Status",
            "_local": "Updated",
        }
        show = show.rename(columns={k: v for k, v in rename.items() if k in show.columns})
        if "Updated" in show.columns:
            show["Updated"] = show["Updated"].dt.strftime("%Y-%m-%d %H:%M")
        st.markdown(f"**Sales Cases — {label} ({len(show)})**")
        st.dataframe(show, use_container_width=True, hide_index=True)


_PERF_ENG_LINE_COLORS: tuple[str, ...] = (
    "#7eb8da",
    "#D7B491",
    "#8fd99a",
    "#c9a0dc",
    "#f0a080",
    "#e8d06a",
    "#6ecfcf",
    "#f08098",
)


def _perf_engineer_color_map(engineers: list[str]) -> dict[str, str]:
    return {
        eng: _PERF_ENG_LINE_COLORS[i % len(_PERF_ENG_LINE_COLORS)]
        for i, eng in enumerate(engineers)
    }


_PERF_MATRIX_LOOKUP_KEY = "perf_matrix_ticket_lookup"
_PERF_MATRIX_COMPONENT_KEY = "perf_staff_matrix"
_PERF_MATRIX_MAX_TICKETS = 40
_PERF_MATRIX_OUTCOME_STYLE: dict[str, tuple[str, str]] = {
    "assigned": ("A", "#9ec5e8"),
    "responded": ("✓", "#b8d4a8"),
    "reassigned": ("↪", "#D7B491"),
    "unattended": ("U", "#c97a7a"),
    "on_hold": ("H", "#a39e97"),
}
_PERF_MATRIX_OUTCOME_LABELS: dict[str, str] = {
    "active": "Active assignment",
    "assigned": "Assigned",
    "responded": "Responded",
    "reassigned": "Reassigned",
    "unattended": "Unattended",
    "on_hold": "On hold",
}
_PERF_MATRIX_STATUS_RANK: tuple[str, ...] = (
    "active",
    "responded",
    "reassigned",
    "assigned",
    "on_hold",
    "unattended",
)


def _perf_visit_bipartite_data(
    visits: pd.DataFrame,
    *,
    focus: str,
) -> dict[str, object] | None:
    """Engineers, tickets, and visit links for the Case Info tab matrix."""
    if visits.empty or "ticket_number" not in visits.columns or "assignee" not in visits.columns:
        return None
    prepared = _perf_prepare_visits_df(visits)
    pairs = prepared[["assignee", "ticket_number"]].copy()
    pairs["ticket_number"] = pairs["ticket_number"].astype(str).str.strip()
    pairs = pairs.loc[pairs["ticket_number"].ne("")].drop_duplicates()
    if pairs.empty:
        return None

    ticket_engineers: dict[str, set[str]] = {}
    for tn, grp in pairs.groupby("ticket_number"):
        ticket_engineers[str(tn)] = set(grp["assignee"].dropna().astype(str).tolist())

    focus_key = _perf_norm_member(focus) if focus not in ("", "All") else ""
    all_engineers = sorted(pairs["assignee"].dropna().unique().tolist(), key=str.lower)
    all_tickets = sorted(
        ticket_engineers.keys(),
        key=lambda t: (-len(ticket_engineers[t]), str(t).lower()),
    )
    return {
        "all_engineers": all_engineers,
        "all_tickets": all_tickets,
        "ticket_engineers": ticket_engineers,
        "focus_key": focus_key,
        "total_tickets": len(all_tickets),
    }


def _perf_visit_map_ticket_pool(
    all_tickets: list[str],
    ticket_engineers: dict[str, set[str]],
    eng_index: dict[str, int],
    *,
    focus_key: str,
    search: str,
) -> list[str]:
    """Filter + sort tickets for the map (engineer focus, search, barycenter order)."""
    pool = list(all_tickets)
    if focus_key:
        pool = [t for t in pool if focus_key in ticket_engineers.get(t, set())]
    q = str(search or "").strip().lower()
    if q:
        pool = [t for t in pool if q in str(t).lower()]
    return _perf_bipartite_ticket_order(pool, ticket_engineers, eng_index)


def _perf_bipartite_ticket_order(
    tickets: list[str],
    ticket_engineers: dict[str, set[str]],
    eng_index: dict[str, int],
) -> list[str]:
    """Order tickets near their engineers' rows — fewer crossing lines."""

    def _key(tn: str) -> tuple[float, str]:
        engs = ticket_engineers.get(tn, set())
        if not engs:
            return (0.0, tn.lower())
        avg = sum(eng_index.get(e, 0) for e in engs) / len(engs)
        return (avg, tn.lower())

    return sorted(tickets, key=_key)


def _perf_visit_ticket_pool(
    data: dict[str, object],
    *,
    search: str,
    lookup_ticket: str = "",
) -> tuple[list[str], list[str], dict[str, set[str]], str, int]:
    all_engineers: list[str] = data["all_engineers"]  # type: ignore[assignment]
    all_tickets: list[str] = data["all_tickets"]  # type: ignore[assignment]
    ticket_engineers: dict[str, set[str]] = data["ticket_engineers"]  # type: ignore[assignment]
    focus_key: str = data["focus_key"]  # type: ignore[assignment]
    total_all: int = data["total_tickets"]  # type: ignore[assignment]
    eng_index = {eng: i for i, eng in enumerate(all_engineers)}
    tid = _perf_normalize_matrix_lookup(lookup_ticket)
    pool_focus = "" if tid and tid in all_tickets else focus_key
    pool = _perf_visit_map_ticket_pool(
        all_tickets,
        ticket_engineers,
        eng_index,
        focus_key=pool_focus,
        search=search or (tid if tid else ""),
    )
    return all_engineers, pool, ticket_engineers, focus_key, total_all


def _perf_visit_staff_outcome(
    visits: pd.DataFrame,
    *,
    assignee: str,
    ticket: str,
) -> tuple[str, str]:
    """Return (outcome_code, label) for one engineer×ticket cell."""
    if visits.empty:
        return "", ""
    mask = (
        visits["assignee"].astype(str) == assignee
    ) & (visits["ticket_number"].astype(str) == ticket)
    sub = visits.loc[mask]
    if sub.empty:
        return "", ""
    if "is_active" in sub.columns and sub["is_active"].any():
        return "active", _PERF_MATRIX_OUTCOME_LABELS["active"]
    outcome = "assigned"
    if "outcome" in sub.columns:
        ordered = sub.copy()
        if "visit_start" in ordered.columns:
            ordered = ordered.sort_values("visit_start")
        outcome = str(ordered["outcome"].iloc[-1] or "assigned")
    label = _PERF_MATRIX_OUTCOME_LABELS.get(outcome, outcome.replace("_", " ").title())
    return outcome, label


def _perf_ticket_matrix_status(staff_assignments: dict[str, dict[str, str]], assigned: list[str]) -> str:
    if any(staff_assignments.get(s, {}).get("outcome") == "active" for s in assigned):
        return "Active"
    best: str | None = None
    best_rank = len(_PERF_MATRIX_STATUS_RANK)
    for staff in assigned:
        outcome = staff_assignments.get(staff, {}).get("outcome", "")
        if not outcome:
            continue
        try:
            rank = _PERF_MATRIX_STATUS_RANK.index(outcome)
        except ValueError:
            rank = len(_PERF_MATRIX_STATUS_RANK)
        if rank < best_rank:
            best_rank = rank
            best = outcome
    if not best:
        return "Unknown"
    if best == "on_hold":
        return "On Hold"
    return best.replace("_", " ").title()


def _perf_ticket_matrix_priority(*, assigned_count: int) -> str:
    if assigned_count >= 4:
        return "Critical"
    if assigned_count >= 3:
        return "High"
    if assigned_count >= 2:
        return "Normal"
    return "Low"


def _perf_matrix_display_status(status: str) -> str:
    """Workflow labels for the matrix Status column."""
    return {
        "Active": "In Progress",
        "Responded": "Resolved",
        "Assigned": "Open",
        "Reassigned": "In Progress",
        "On Hold": "On Hold",
        "Unattended": "Open",
        "Unknown": "Open",
    }.get(status, status)


def _perf_matrix_staff_role(outcome: str) -> str:
    return {
        "active": "Active Dev",
        "responded": "Peer Reviewer",
        "reassigned": "Handoff",
        "unattended": "Observer",
        "on_hold": "On Hold",
        "assigned": "Contributor",
    }.get(outcome, "Contributor")


def _perf_matrix_case_label(ticket_id: str, priority: str, *, seq: int) -> str:
    code = {"Critical": "C", "High": "H", "Normal": "N", "Low": "L"}.get(priority, "N")
    return f"Case #{ticket_id} ({code}-{seq:02d})"


_PERF_MATRIX_COMMENTS_MAX = 40
_PERF_MATRIX_PHOTOS_MAX = 20


def _perf_matrix_case_info_ts(raw: object) -> tuple[str, float]:
    """Local display time + unix sort key for matrix Case Info."""
    dt = _parse_ts_value(raw)
    if not dt:
        return "", 0.0
    try:
        ts = pd.Timestamp(dt)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        local = ts.tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
        return local, float(ts.timestamp())
    except Exception:
        return str(raw), 0.0


_PERF_MATRIX_COMMENT_PREFIX_RE = re.compile(
    r"^(?:responded by\s+@\S+:\s*|manual dashboard\s+.+?\s+by\s+@\S+:\s*)",
    re.IGNORECASE,
)

# Attendance ``action_type`` → Case Info comment ``kind`` (admin resolve/close notes included).
_PERF_MATRIX_LOG_ACTION_KIND: dict[str, str] = {
    "Response": "response",
    "Assignment": "assignment",
    "Resolved": "admin",
    "AdminClosed": "admin",
    "OnHold": "admin",
    "MovedToInvestigation": "admin",
    "Reopened": "admin",
    "ReopenedFromUnattended": "admin",
}


def _perf_matrix_comment_key(author: str, text: str) -> tuple[str, str]:
    a = str(author or "").strip().lower().lstrip("@")
    t = " ".join(str(text or "").strip().split())
    t = _PERF_MATRIX_COMMENT_PREFIX_RE.sub("", t).strip().lower()
    return (a, t[:220])


class _PerfMatrixCaseInfoBucket:
    """Per-ticket comments (text) and photos (URLs), deduped across sources."""

    def __init__(self) -> None:
        self.comments: dict[str, list[dict[str, str]]] = {}
        self.photos: dict[str, list[dict[str, str]]] = {}
        self._comment_seen: dict[str, set[tuple[str, str]]] = {}
        self._photo_seen: dict[str, set[str]] = {}

    def add(
        self,
        ticket_number: str,
        *,
        at_raw: object,
        author: str,
        text: str | None,
        photo_url: str | None,
        kind: str,
    ) -> None:
        tn = str(ticket_number or "").strip()
        if not tn:
            return
        text_s = str(text or "").strip()
        photo_s = str(photo_url or "").strip()
        at_label, sort_key = _perf_matrix_case_info_ts(at_raw)
        author_s = str(author or "—").strip() or "—"
        sort_s = str(sort_key)
        if text_s:
            ckey = _perf_matrix_comment_key(author_s, text_s)
            cseen = self._comment_seen.setdefault(tn, set())
            if ckey not in cseen:
                cseen.add(ckey)
                self.comments.setdefault(tn, []).append(
                    {
                        "at": at_label,
                        "author": author_s,
                        "text": text_s,
                        "kind": kind,
                        "_sort": sort_s,
                    }
                )
        if photo_s.startswith("http"):
            pseen = self._photo_seen.setdefault(tn, set())
            if photo_s not in pseen:
                pseen.add(photo_s)
                self.photos.setdefault(tn, []).append(
                    {
                        "at": at_label,
                        "author": author_s,
                        "url": photo_s,
                        "_sort": sort_s,
                    }
                )


def _perf_matrix_case_info_from_visits(
    visits: pd.DataFrame,
    ticket_numbers: set[str],
    bucket: _PerfMatrixCaseInfoBucket,
) -> None:
    if visits.empty or "ticket_number" not in visits.columns:
        return
    prepared = _perf_prepare_visits_df(visits)
    for _, row in prepared.iterrows():
        tn = str(row.get("ticket_number") or "").strip()
        if tn not in ticket_numbers:
            continue
        note = str(row.get("response_note") or "").strip()
        photo = str(row.get("photo_url") or "").strip()
        if not note and not photo.startswith("http"):
            continue
        bucket.add(
            tn,
            at_raw=row.get("visit_end") or row.get("visit_start"),
            author=str(row.get("assignee") or "—"),
            text=note or None,
            photo_url=photo or None,
            kind="visit",
        )


def _fetch_attendance_for_matrix_tickets(
    ticket_numbers: list[str],
    *,
    limit: int = 4000,
) -> pd.DataFrame:
    """Attendance log rows for matrix Case Info (responses + noted assignments)."""
    if not ticket_numbers or not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()
    client = _get_supabase_client()
    ids = [str(t).strip() for t in dict.fromkeys(ticket_numbers) if str(t).strip()]
    parts: list[pd.DataFrame] = []
    per_chunk = 80
    for i in range(0, len(ids), per_chunk):
        chunk = ids[i : i + per_chunk]
        try:
            res = (
                client.table(ATTENDANCE_LOGS_TABLE)
                .select(
                    "ticket_number, member_username, action_type, note, photo_url, timestamp"
                )
                .in_("ticket_number", chunk)
                .order("timestamp", desc=True)
                .limit(limit)
                .execute()
            )
        except Exception:
            continue
        if res.data:
            parts.append(pd.DataFrame(res.data))
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _perf_matrix_case_info_from_attendance(
    logs: pd.DataFrame,
    ticket_numbers: set[str],
    bucket: _PerfMatrixCaseInfoBucket,
) -> None:
    if logs.empty:
        return
    for _, row in logs.iterrows():
        tn = str(row.get("ticket_number") or "").strip()
        if tn not in ticket_numbers:
            continue
        action = str(row.get("action_type") or "").strip()
        note = str(row.get("note") or "").strip()
        photo = str(row.get("photo_url") or "").strip()
        kind = _PERF_MATRIX_LOG_ACTION_KIND.get(action)
        if not kind:
            continue
        if action == "Assignment" and not note:
            continue
        if action == "Response" and not note and not photo.startswith("http"):
            continue
        if kind == "admin" and not note:
            continue
        bucket.add(
            tn,
            at_raw=row.get("timestamp"),
            author=str(row.get("member_username") or "—"),
            text=note or None,
            photo_url=photo or None,
            kind=kind,
        )


def _perf_matrix_case_info_from_tickets_snapshot(
    tickets_df: pd.DataFrame,
    ticket_numbers: set[str],
    bucket: _PerfMatrixCaseInfoBucket,
) -> None:
    if tickets_df.empty or "ticket_number" not in tickets_df.columns:
        return
    for _, row in tickets_df.iterrows():
        tn = str(row.get("ticket_number") or "").strip()
        if tn not in ticket_numbers:
            continue
        text = str(row.get("field_response") or "").strip()
        photo = str(row.get("photo_url") or "").strip()
        if not text and not photo.startswith("http"):
            continue
        author = str(
            row.get("field_responded_by") or row.get("assigned_to") or "—"
        )
        bucket.add(
            tn,
            at_raw=row.get("responded_at") or row.get("updated_at"),
            author=author,
            text=text or None,
            photo_url=photo or None,
            kind="field",
        )


def _perf_matrix_case_info_finalize(
    bucket: _PerfMatrixCaseInfoBucket,
    ticket_numbers: set[str],
) -> dict[str, dict[str, list[dict[str, object]]]]:
    out: dict[str, dict[str, list[dict[str, object]]]] = {}
    for tn in ticket_numbers:
        comments = bucket.comments.get(tn, [])
        photos = bucket.photos.get(tn, [])
        comments_sorted = sorted(
            comments,
            key=lambda e: float(e.pop("_sort", 0) or 0),
            reverse=True,
        )[:_PERF_MATRIX_COMMENTS_MAX]
        photos_sorted = sorted(
            photos,
            key=lambda e: float(e.pop("_sort", 0) or 0),
            reverse=True,
        )[:_PERF_MATRIX_PHOTOS_MAX]
        out[tn] = {
            "comments": [
                {
                    "at": e["at"],
                    "author": e["author"],
                    "text": e["text"],
                    "kind": e["kind"],
                }
                for e in comments_sorted
            ],
            "photos": [
                {"at": e["at"], "author": e["author"], "url": e["url"]}
                for e in photos_sorted
            ],
        }
    return out


def _perf_matrix_case_info_by_ticket(
    visits_all: pd.DataFrame,
    ticket_numbers: list[str],
) -> dict[str, dict[str, list[dict[str, object]]]]:
    """Comments and photos for Case Info — text and images kept separate, deduped."""
    ids = {str(t).strip() for t in ticket_numbers if str(t).strip()}
    if not ids:
        return {}
    bucket = _PerfMatrixCaseInfoBucket()
    _perf_matrix_case_info_from_visits(visits_all, ids, bucket)
    try:
        logs = _fetch_attendance_for_matrix_tickets(sorted(ids))
    except Exception:
        logs = pd.DataFrame()
    _perf_matrix_case_info_from_attendance(logs, ids, bucket)
    try:
        tickets_df = _fetch_tickets_cached()
    except Exception:
        tickets_df = pd.DataFrame()
    if not tickets_df.empty:
        subset = tickets_df[
            tickets_df["ticket_number"].astype(str).str.strip().isin(ids)
        ]
        _perf_matrix_case_info_from_tickets_snapshot(subset, ids, bucket)
    # Only add gallery URLs not already captured (older pinned photos).
    photo_hist = _fetch_ticket_photos(sorted(ids), limit_per_ticket=15)
    for tn, items in photo_hist.items():
        if tn not in ids:
            continue
        for p in items:
            bucket.add(
                tn,
                at_raw=p.get("when"),
                author=str(p.get("member") or "—"),
                text=None,
                photo_url=str(p.get("url") or ""),
                kind="response",
            )
    return _perf_matrix_case_info_finalize(bucket, ids)


def _perf_build_staff_matrix_payload(
    visits_all: pd.DataFrame,
    *,
    data: dict[str, object],
    search: str,
    lookup_ticket: str = "",
) -> dict[str, object]:
    """JSON payload for the React Multi-Staff Case Management Matrix."""
    all_engineers, pool, ticket_engineers, _focus_key, _total_all = _perf_visit_ticket_pool(
        data,
        search=search,
        lookup_ticket=lookup_ticket,
    )
    prepared = _perf_prepare_visits_df(visits_all)
    eng_colors = _perf_engineer_color_map(all_engineers)
    case_info_by_ticket = _perf_matrix_case_info_by_ticket(visits_all, pool)
    tickets: list[dict[str, object]] = []
    for seq, tn in enumerate(pool, start=1):
        assigned = sorted(ticket_engineers.get(tn, set()), key=str.lower)
        staff_assignments: dict[str, dict[str, str]] = {}
        for eng in assigned:
            outcome, label = _perf_visit_staff_outcome(
                prepared,
                assignee=eng,
                ticket=tn,
            )
            if outcome:
                staff_assignments[eng] = {
                    "outcome": outcome,
                    "label": label,
                    "role": _perf_matrix_staff_role(outcome),
                }
        is_shared = len(assigned) > 1
        status = _perf_ticket_matrix_status(staff_assignments, assigned)
        priority = _perf_ticket_matrix_priority(assigned_count=len(assigned))
        tickets.append(
            {
                "id": tn,
                "caseLabel": _perf_matrix_case_label(tn, priority, seq=seq),
                "status": status,
                "displayStatus": _perf_matrix_display_status(status),
                "priority": priority,
                "assignedStaff": assigned,
                "staffAssignments": staff_assignments,
                "isShared": is_shared,
                "comments": case_info_by_ticket.get(tn, {}).get("comments", []),
                "photos": case_info_by_ticket.get(tn, {}).get("photos", []),
            }
        )
    staff_counts = [len(t.get("assignedStaff") or []) for t in tickets]
    total = len(tickets)
    avg_staff = (sum(staff_counts) / total) if total else 0.0
    top_idx = max(range(total), key=lambda i: staff_counts[i]) if total else 0
    top_ticket = tickets[top_idx] if total else {}
    return {
        "tickets": tickets,
        "staffMembers": all_engineers,
        "staffColors": eng_colors,
        "lookupTicket": lookup_ticket or "",
        "summary": {
            "totalCases": total,
            "avgStaffPerCase": round(avg_staff, 1),
            "topCollaborativeCaseId": str(top_ticket.get("id") or ""),
            "topCollaborativeStaffCount": int(staff_counts[top_idx]) if total else 0,
        },
    }


def _perf_visit_matrix_cell(
    visits: pd.DataFrame,
    *,
    assignee: str,
    ticket: str,
    eng_colors: dict[str, str],
) -> tuple[str, str, str]:
    """Return (symbol, color, title) for one engineer×ticket cell."""
    outcome, label = _perf_visit_staff_outcome(visits, assignee=assignee, ticket=ticket)
    if not outcome:
        return "", "", ""
    if outcome == "active":
        return "●", eng_colors.get(assignee, "#9ec5e8"), label
    sym, col = _PERF_MATRIX_OUTCOME_STYLE.get(outcome, ("·", "#a39e97"))
    return sym, col, label


def _render_perf_visit_staff_matrix(
    visits_all: pd.DataFrame,
    *,
    data: dict[str, object],
    search: str,
    lookup_ticket: str = "",
) -> None:
    """Tickets × staff grid — virtualized React matrix when build is present."""
    payload = _perf_build_staff_matrix_payload(
        visits_all,
        data=data,
        search=search,
        lookup_ticket=lookup_ticket,
    )
    tickets = payload.get("tickets") or []
    if not tickets:
        st.caption("No tickets to show in the matrix.")
        return

    if _staff_matrix_component is not None and _HAS_STAFF_MATRIX:
        _staff_matrix_component(payload, height=720, key=_PERF_MATRIX_COMPONENT_KEY)
        return

    all_engineers, pool, ticket_engineers, _focus_key, _total_all = _perf_visit_ticket_pool(
        data,
        search=search,
    )
    tickets_pool = pool[:_PERF_MATRIX_MAX_TICKETS]
    if len(pool) > _PERF_MATRIX_MAX_TICKETS:
        st.caption(
            f"Showing first **{_PERF_MATRIX_MAX_TICKETS}** of **{len(pool)}** tickets — "
            "build the React component for full virtualization (`npm run build` in "
            "`components/staff_matrix/frontend`)."
        )

    prepared = _perf_prepare_visits_df(visits_all)
    eng_colors = _perf_engineer_color_map(all_engineers)

    header_cells = [
        '<th class="perf-matrix-sticky-col">Ticket</th>',
    ]
    for eng in all_engineers:
        eng_short = eng if len(eng) <= 14 else eng[:11] + "…"
        col = eng_colors.get(eng, "#9ec5e8")
        header_cells.append(
            f'<th class="perf-matrix-ticket" title="{html.escape(eng)}" '
            f'style="color:{col}">{html.escape(eng_short)}</th>'
        )

    body_rows: list[str] = []
    for tn in tickets_pool:
        engs = ticket_engineers.get(tn, set())
        shared_cls = " shared-col" if len(engs) > 1 else ""
        short = tn if len(tn) <= 14 else tn[:11] + "…"
        cells = [
            f'<td class="perf-matrix-sticky-col{shared_cls}" title="{html.escape(tn)}">'
            f"{html.escape(short)}</td>"
        ]
        for eng in all_engineers:
            if eng not in engs:
                cells.append('<td class="perf-matrix-empty">·</td>')
                continue
            sym, sym_col, title = _perf_visit_matrix_cell(
                prepared,
                assignee=eng,
                ticket=tn,
                eng_colors=eng_colors,
            )
            cells.append(
                f'<td class="perf-matrix-cell" style="color:{sym_col}" '
                f'title="{html.escape(title)}">{html.escape(sym)}</td>'
            )
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    legend_bits = [
        '<span class="perf-matrix-legend-item"><i style="color:#9ec5e8">●</i> Active</span>',
        '<span class="perf-matrix-legend-item"><i style="color:#b8d4a8">✓</i> Responded</span>',
        '<span class="perf-matrix-legend-item"><i style="color:#D7B491">↪</i> Reassigned</span>',
        '<span class="perf-matrix-legend-item"><i style="color:#9ec5e8">A</i> Assigned</span>',
        '<span class="perf-matrix-legend-item"><i style="color:#c97a7a">U</i> Unattended</span>',
        '<span class="perf-matrix-legend-item"><i style="color:#D7B491">■</i> Shared ticket</span>',
    ]

    matrix_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/>
<style>
html, body {{
  margin: 0; padding: 0; background: #141414; color: #e8e6e3;
  font-family: system-ui, sans-serif; font-size: 11px;
}}
.perf-matrix-shell {{ padding: 6px 8px 10px; }}
.perf-matrix-legend {{
  display: flex; flex-wrap: wrap; gap: 0.5rem 1rem;
  margin-bottom: 8px; color: #a39e97; font-size: 10px;
}}
.perf-matrix-legend-item {{ display: inline-flex; align-items: center; gap: 0.25rem; }}
.perf-matrix-wrap {{
  overflow: auto; max-height: 520px;
  border: 1px solid rgba(215,180,145,0.22); border-radius: 6px;
}}
table.perf-matrix {{
  border-collapse: collapse; min-width: 100%;
}}
.perf-matrix th, .perf-matrix td {{
  border: 1px solid rgba(215,180,145,0.12);
  padding: 4px 6px; text-align: center; white-space: nowrap;
}}
.perf-matrix th {{
  background: #1a1a1a; color: #D7B491; font-weight: 600;
  position: sticky; top: 0; z-index: 2;
}}
.perf-matrix-sticky-col {{
  position: sticky; left: 0; z-index: 3;
  background: #141414; text-align: left !important;
  font-weight: 500; min-width: 108px;
  box-shadow: 2px 0 4px rgba(0,0,0,0.35);
}}
th.perf-matrix-sticky-col {{ z-index: 4; background: #1a1a1a; }}
.perf-matrix-ticket {{ min-width: 52px; font-size: 9px; }}
th.perf-matrix-ticket.shared-col {{ color: #e8c9a0; }}
.perf-matrix-cell {{ font-weight: 700; font-size: 12px; }}
.perf-matrix-empty {{ color: #3a3a3a; }}
</style>
</head><body>
<div class="perf-matrix-shell">
<div class="perf-matrix-legend">{"".join(legend_bits)}</div>
<div class="perf-matrix-wrap">
<table class="perf-matrix">
<thead><tr>{"".join(header_cells)}</tr></thead>
<tbody>{"".join(body_rows)}</tbody>
</table>
</div>
</div>
</body></html>"""
    components.html(matrix_html, height=min(560, 80 + 28 * max(len(tickets_pool), 1)), scrolling=True)


def _render_perf_case_info_tab(
    visits_all: pd.DataFrame,
    *,
    focus: str,
) -> None:
    """Case Info tab — multi-staff case management matrix."""
    lookup_raw = _perf_matrix_sync_lookup_from_component()
    lookup_tid = _perf_normalize_matrix_lookup(lookup_raw)
    visits_matrix = _perf_matrix_merge_ticket_lookup(visits_all, lookup_raw)
    data_matrix = _perf_visit_bipartite_data(visits_matrix, focus=focus)
    st.caption(
        "Rows = tickets · columns = staff · colored dots show involvement. "
        "Use **Ticket ID** in the matrix filter bar to search or look up history "
        "outside the sidebar date range (9 or 16 digits). Case Info on the right."
    )
    if lookup_tid and not data_matrix:
        st.warning(
            f"No visit cycles found for ticket **{lookup_tid}**. "
            "Check the ticket number or confirm it was assigned from the dashboard."
        )
    elif not data_matrix:
        st.info(
            "No visit data in this window. Enter a full **Ticket ID** in the matrix "
            "filter bar to look up a specific ticket, or widen the sidebar date range."
        )
    else:
        if lookup_tid and lookup_tid not in (data_matrix.get("all_tickets") or []):
            st.warning(
                f"Ticket **{lookup_tid}** has no `ticket_visits` rows — "
                "matrix Case Info may be empty."
            )
        _render_perf_visit_staff_matrix(
            visits_matrix,
            data=data_matrix,
            search=lookup_tid,
            lookup_ticket=lookup_tid,
        )


def _render_visit_summary_table(visits: pd.DataFrame) -> None:
    summary = _perf_build_visit_summary(visits)
    if summary.empty:
        st.caption("No visit data for this filter.")
        return
    st.dataframe(summary, use_container_width=True, hide_index=True)


def _render_visit_detail_table(visits: pd.DataFrame) -> None:
    if visits.empty:
        st.caption("No visits to list.")
        return
    view = visits.copy()
    for col in ("visit_start", "visit_end"):
        if col in view.columns:
            ts = _parse_ts(view[col])
            view[col] = ts.dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d %H:%M")
    cols = [c for c in (
        "ticket_number", "assignee", "is_active", "outcome", "visit_start", "visit_end",
        "response_note", "photo_url", "closed_by",
    ) if c in view.columns]
    st.dataframe(view[cols].sort_values("visit_start", ascending=False).head(300),
                 use_container_width=True, hide_index=True)


def _render_handled_visit_credit_bar(visits: pd.DataFrame) -> None:
    """Distinct tickets handled per engineer (fair credit); tooltip shows visit cycles."""
    prepared = _perf_prepare_visits_df(visits)
    counts = _perf_handled_visit_credit_counts(prepared)
    if counts.empty:
        st.caption("No responded visits in this time range.")
        return
    n_tickets = (
        int(prepared["ticket_number"].astype(str).nunique())
        if "ticket_number" in prepared.columns
        else int(len(prepared))
    )
    n_cycles = int(len(prepared))
    st.caption(
        f"**{n_tickets}** unique tickets with a responded visit · "
        f"**{n_cycles}** visit cycles in range "
        "(multiple cycles on one ticket count once per engineer)."
    )
    height = min(520, max(180, 42 * len(counts)))
    chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x=alt.X(
                "Tickets:Q",
                title="Tickets handled",
                axis=alt.Axis(format=".0f", tickMinStep=1),
            ),
            y=alt.Y(
                "assignee:N",
                sort="-x",
                title="",
                axis=alt.Axis(
                    labelOverlap=False,
                    labelFontSize=13,
                    labelPadding=8,
                ),
            ),
            tooltip=[
                alt.Tooltip("assignee:N", title="Engineer"),
                alt.Tooltip("Tickets:Q", title="Unique tickets"),
                alt.Tooltip("Visit cycles:Q", title="Visit cycles"),
            ],
            color=alt.value("#7eb8da"),
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_visit_bar(visits: pd.DataFrame, *, outcome: str | None = None) -> None:
    if visits.empty:
        return
    data = _perf_prepare_visits_df(visits)
    if outcome:
        data = data[data["outcome"] == outcome].copy()
    if data.empty:
        st.caption("No data.")
        return
    counts = data.groupby("assignee").size().rename("Count").reset_index()
    height = min(520, max(180, 42 * len(counts)))
    color = "#7eb8da" if outcome == "responded" else "#D7B491"
    label = outcome.capitalize() if outcome else "Visits"
    chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x=alt.X(
                "Count:Q",
                title=label,
                axis=alt.Axis(format=".0f", tickMinStep=1),
            ),
            y=alt.Y(
                "assignee:N",
                sort="-x",
                title="",
                axis=alt.Axis(
                    labelOverlap=False,
                    labelFontSize=13,
                    labelPadding=8,
                ),
            ),
            tooltip=[
                alt.Tooltip("assignee:N", title="Engineer"),
                alt.Tooltip("Count:Q", title=label),
            ],
            color=alt.value(color),
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def _perf_norm_member(raw: object) -> str:
    """Normalize ``assigned_to`` / log ``member_username`` for chart labels."""

    s = str(raw or "").strip()
    if not s or s.lower() in ("unknown", "none", "null", "nan", "nat"):
        return "(unknown)"
    if s.lower().lstrip("@") == "admin":
        return _SC_SALES_OVERVIEW_ADMIN_LABEL
    low = _canonical_username_stem(s)
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
    if preset == "This week":
        start, end, _, _ = _perf_calendar_week_range_utc(week_offset=0)
        return start, end
    if preset == "Last 30 days":
        return now - pd.Timedelta(days=30), now
    start, end, _, _ = _perf_calendar_week_range_utc(week_offset=0)
    return start, end


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
    return f"{lo} – {hi} • {LOCAL_TZ_LABEL}"


def _dash_time_preset_display(preset: str) -> str:
    labels = {
        "Today": "Today",
        "This week": "This week",
        "Last 30 days": "Last 30 Days",
        "Pick dates": "Custom Date Range",
    }
    return labels.get(preset, preset)


def _dash_time_preset_trigger_label(preset: str) -> str:
    """Compact single-line label for the header trigger button."""
    labels = {
        "Today": "Today",
        "This week": "This week",
        "Last 30 days": "Last 30 Days",
        "Pick dates": "Custom",
    }
    return labels.get(preset, preset)


def _ensure_dash_range_defaults() -> None:
    if _DASH_RANGE_FROM_KEY not in st.session_state:
        start, end = _preset_range_utc("This week")
        _store_dash_range(start, end)
        _sync_search_date_widgets(start, end)
    if _DASH_TIME_PRESET_KEY not in st.session_state:
        st.session_state[_DASH_TIME_PRESET_KEY] = "This week"


def _init_dash_date_range_state() -> None:
    _ensure_dash_range_defaults()
    if _DASH_RANGE_FROM_KEY not in st.session_state:
        start, end = _preset_range_utc("This week")
        _store_dash_range(start, end)
        _sync_search_date_widgets(start, end)
    if _DASH_TIME_PRESET_KEY not in st.session_state:
        st.session_state[_DASH_TIME_PRESET_KEY] = "This week"
    cur = st.session_state.get(_DASH_TIME_PRESET_KEY)
    if cur in _LEGACY_TIME_PRESET_MAP:
        st.session_state[_DASH_TIME_PRESET_KEY] = _LEGACY_TIME_PRESET_MAP[cur]
    elif cur not in _DASH_TIME_PRESET_OPTIONS:
        st.session_state[_DASH_TIME_PRESET_KEY] = "This week"
    if _DASH_SEARCH_FROM_DATE_KEY not in st.session_state:
        _sync_search_date_widgets(*_get_dash_range())


def _sync_dash_range_from_ui(preset: str) -> None:
    """Apply preset or custom From–To dates to session range keys."""
    if preset == "Pick dates":
        from_d = st.session_state.get(_DASH_SEARCH_FROM_DATE_KEY)
        to_d = st.session_state.get(_DASH_SEARCH_TO_DATE_KEY)
        if from_d is None or to_d is None:
            return
        if from_d > to_d:
            to_d = from_d
            st.session_state[_DASH_SEARCH_TO_DATE_KEY] = to_d
        _store_dash_range(_local_date_start(from_d), _local_date_end(to_d))
        return
    start, end = _preset_range_utc(preset)
    _store_dash_range(start, end)
    _sync_search_date_widgets(start, end)


def _dash_date_range_lookback() -> tuple[int, pd.Timestamp, pd.Timestamp]:
    start, end = _get_dash_range()
    lookback_days = max(
        MIN_LOOKBACK_DAYS,
        min(MAX_LOOKBACK_DAYS, int((end - start).total_seconds() // 86400) + 1),
    )
    return lookback_days, start, end


def _dash_refresh_settings() -> tuple[bool, int]:
    return (
        bool(st.session_state.get("bon_toolbar_auto_refresh", True)),
        int(
            st.session_state.get("bon_toolbar_refresh_interval", DEFAULT_REFRESH_MINUTES)
        ),
    )


def _render_dash_time_preset_select(*, show_label: bool = False) -> str:
    """Time-range preset for the filters menu."""
    _init_dash_date_range_state()
    _sync_dash_range_from_ui(str(st.session_state.get(_DASH_TIME_PRESET_KEY, "This week")))
    range_help = _format_dash_range_caption() or "Time range for tickets and performance"
    preset = st.selectbox(
        "Time Range",
        options=list(_DASH_TIME_PRESET_OPTIONS),
        key=_DASH_TIME_PRESET_KEY,
        label_visibility="visible" if show_label else "collapsed",
        help=range_help,
    )
    prev_preset = st.session_state.get(_DASH_PREV_PRESET_KEY)
    if preset != prev_preset:
        st.session_state[_DASH_PREV_PRESET_KEY] = preset
    _sync_dash_range_from_ui(preset)
    return preset


def _render_dash_time_range_pills(*, preset: str, custom_open: bool) -> None:
    """Preset pill buttons + optional custom date inputs."""
    selected_pill_key = (
        _DASH_TIME_PRESET_PILL_KEYS.get(preset)
        if preset in _DASH_TIME_PRESET_MENU
        else "bon_dash_range_custom_btn"
        if preset == "Pick dates"
        else None
    )
    if selected_pill_key:
        st.markdown(
            f"""
            <style>
            div.st-key-{selected_pill_key} button {{
                border-color: var(--bon-oak) !important;
                color: var(--bon-oak) !important;
                background: rgba(215, 180, 145, 0.12) !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
    with st.container(key="bon_dash_range_menu"):
        for opt in _DASH_TIME_PRESET_MENU:
            pill_label = _dash_time_preset_display(opt)
            if preset == opt:
                pill_label = f"✓ {pill_label}"
            if st.button(
                pill_label,
                key=_DASH_TIME_PRESET_PILL_KEYS[opt],
                use_container_width=True,
            ):
                st.session_state[_DASH_TIME_PRESET_KEY] = opt
                st.session_state[_DASH_RANGE_CUSTOM_OPEN_KEY] = False
                st.session_state[_DASH_PREV_PRESET_KEY] = opt
                _sync_dash_range_from_ui(opt)
                st.rerun()

        st.markdown(
            '<div class="bon-dash-range-divider" aria-hidden="true"></div>',
            unsafe_allow_html=True,
        )

        custom_label = "⚙ Custom Date Range"
        if preset == "Pick dates":
            custom_label = "✓ ⚙ Custom Date Range"
        if st.button(
            custom_label,
            key="bon_dash_range_custom_btn",
            use_container_width=True,
        ):
            st.session_state[_DASH_TIME_PRESET_KEY] = "Pick dates"
            st.session_state[_DASH_RANGE_CUSTOM_OPEN_KEY] = True
            st.session_state[_DASH_PREV_PRESET_KEY] = "Pick dates"
            st.rerun()

        if preset == "Pick dates" or custom_open:
            _render_dash_custom_date_inputs()


def _render_dash_menu_time_range() -> str:
    """Time-range presets inside the ☰ menu."""
    _init_dash_date_range_state()
    preset = str(st.session_state.get(_DASH_TIME_PRESET_KEY, "This week"))
    _sync_dash_range_from_ui(preset)
    custom_open = bool(
        st.session_state.get(_DASH_RANGE_CUSTOM_OPEN_KEY, preset == "Pick dates")
    )
    range_cap = _format_dash_range_caption()
    expander_label = f"Time range · {_dash_time_preset_trigger_label(preset)}"

    with st.container(key="bon_dash_range_picker"):
        with st.expander(expander_label, expanded=True):
            if range_cap:
                st.markdown(
                    f'<p class="bon-menu-range-cap">{html.escape(range_cap)}</p>',
                    unsafe_allow_html=True,
                )
            _render_dash_time_range_pills(preset=preset, custom_open=custom_open)

    return preset


def _render_dash_custom_date_inputs() -> None:
    """From/To pickers when preset is Pick dates."""
    c1, c2 = st.columns(2)
    with c1:
        st.date_input(
            "From",
            format="YYYY-MM-DD",
            key=_DASH_SEARCH_FROM_DATE_KEY,
        )
    with c2:
        st.date_input(
            "To",
            format="YYYY-MM-DD",
            key=_DASH_SEARCH_TO_DATE_KEY,
        )
    _sync_dash_range_from_ui("Pick dates")


def _render_dash_filters_panel() -> None:
    """Auto-refresh, ticket lookup — Filters menu."""
    st.toggle("Auto-Refresh", value=True, key="bon_toolbar_auto_refresh")
    if st.session_state.get("bon_toolbar_auto_refresh", True):
        st.slider(
            "Every (Minutes)",
            min_value=MIN_REFRESH_MINUTES,
            max_value=MAX_REFRESH_MINUTES,
            value=DEFAULT_REFRESH_MINUTES,
            step=1,
            key="bon_toolbar_refresh_interval",
        )
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
    if st.button("↻ Refresh now", key="bon_menu_refresh_btn", use_container_width=True):
        _invalidate_dashboard_data_cache()
        st.session_state.pop(_DASH_LAST_ATTENDANCE_TS_KEY, None)
        st.rerun()


def _render_app_topbar() -> None:
    """Fixed header shell — brand · date range · user · menu."""
    _init_dash_date_range_state()
    op = _session_operator_id() or _session_dashboard_username() or ""
    op_display = html.escape(op.lstrip("@") if op else "—")
    menu_open = bool(st.session_state.get(_BON_MENU_OPEN_KEY, False))
    with st.container(key="bon_app_header_shell"):
        with st.container(key="bon_app_topbar"):
            range_cap = _format_dash_range_caption()
            c_brand, c_right = st.columns(
                [1.15, 5],
                vertical_alignment="center",
                gap="small",
            )
            with c_brand:
                st.markdown(
                    """
                    <div class="bon-app-topbar-title">
                      <span class="bon-app-topbar-netops">NetOps</span>
                      <span class="bon-app-topbar-sep">|</span>
                      <span class="bon-app-topbar-sub">Coverage Eye</span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with c_right:
                c_spacer, c_tail = st.columns(
                    [1, 2.5],
                    vertical_alignment="center",
                    gap="small",
                )
                with c_spacer:
                    st.empty()
                with c_tail:
                    c_cap, c_account = st.columns(
                        [1.6, 1],
                        vertical_alignment="center",
                        gap="small",
                    )
                    with c_cap:
                        if range_cap:
                            st.markdown(
                                f'<span class="bon-app-topbar-range-cap">{html.escape(range_cap)}</span>',
                                unsafe_allow_html=True,
                            )
                    with c_account:
                        c_user, c_menu = st.columns(
                            [1, 1],
                            vertical_alignment="center",
                            gap="small",
                        )
                        with c_user:
                            st.markdown(
                                f"""
                                <div class="bon-app-topbar-userchip" title="Signed in as {op_display}">
                                  <span class="bon-app-topbar-mark" aria-hidden="true">
                                    <span class="bon-app-topbar-mark-bar"></span>
                                  </span>
                                  <span class="bon-app-topbar-username">{op_display}</span>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )
                        with c_menu:
                            if menu_open:
                                st.markdown(
                                    """
                                    <style>
                                    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn button::before {
                                        display: none !important;
                                    }
                                    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn button {
                                        font-size: 0.9rem !important;
                                        line-height: 1 !important;
                                        color: var(--bon-muted) !important;
                                    }
                                    </style>
                                    """,
                                    unsafe_allow_html=True,
                                )
                            if st.button(
                                "✕" if menu_open else "☰",
                                key="bon_menu_toggle_btn",
                                help="Open menu" if not menu_open else "Close menu",
                            ):
                                st.session_state[_BON_MENU_OPEN_KEY] = not menu_open
                                st.rerun()


def _render_app_menu_controls() -> None:
    """Dropdown menu below the fixed header."""
    _render_app_menu_panel()


def _render_app_menu_panel() -> None:
    """Dropdown menu below header — Assign, Admin, Filters, Log out."""
    if not st.session_state.get(_BON_MENU_OPEN_KEY, False):
        return

    with st.container(key="bon_app_menu"):
        _render_dash_menu_time_range()
        sidebar_open = _bon_sidebar_is_open()
        if st.button(
            "▸ Assign" if not sidebar_open else "▾ Assign",
            key="bon_menu_assign_btn",
            use_container_width=True,
        ):
            st.session_state[_BON_SIDEBAR_OPEN_KEY] = not sidebar_open
            st.rerun()

        if _dashboard_users_configured() and _is_dashboard_admin():
            with st.expander("Admin", expanded=False):
                _render_dashboard_team_accounts_body()

        with st.expander("Filters", expanded=False):
            _render_dash_filters_panel()

        if st.button("Log out", key="bon_menu_logout_btn", use_container_width=True):
            _clear_auth_session()
            st.session_state.pop(_LOGIN_VIEW_KEY, None)
            st.rerun()


def _render_app_chrome() -> None:
    _render_app_topbar()
    _render_app_menu_controls()
    _inject_bon_header_pin()


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
    """Add ``staff`` (Performance credit), ``category``, and local time from ``_ts``."""
    view = df.copy()
    if "_ts" not in view.columns and "updated_at" in view.columns:
        view["_ts"] = _parse_ts(view["updated_at"])
    view["_local"] = view["_ts"].dt.tz_convert(LOCAL_TZ)
    view["staff"] = view.apply(_sc_perf_sales_credit_staff, axis=1)
    if "sales_category" in view.columns:
        cat = view["sales_category"].fillna("").astype(str).str.strip()
        view["category"] = cat.mask(cat.eq(""), "(uncategorized)")
    else:
        view["category"] = "(uncategorized)"
    if "status" in view.columns:
        view["status_eff"] = view["status"].map(_sc_effective_status)
    return view


def _perf_build_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    view = _perf_enrich_sales_cases(df)
    if view.empty or "status_eff" not in view.columns:
        return pd.DataFrame()
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
        assigned = view["task_category"].map(canonical_task_category)
        assigned = assigned.fillna("").astype(str).str.strip()
        view["assigned_category"] = assigned.mask(assigned.eq(""), "(uncategorized)")
    else:
        view["assigned_category"] = "(uncategorized)"
    if "outcome_category" in view.columns:
        outcome = view["outcome_category"].map(canonical_task_category)
        outcome = outcome.fillna("").astype(str).str.strip()
    else:
        outcome = pd.Series("", index=view.index, dtype=str)
    view["outcome_category"] = outcome
    view["category"] = outcome.where(outcome.ne(""), view["assigned_category"])
    view["category"] = view["category"].mask(view["category"].eq(""), "(uncategorized)")
    return view


def _perf_staff_counts(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=int)
    if "staff" not in df.columns:
        df = _perf_enrich_tickets(df)
    return df.groupby("staff").size()


def _perf_calendar_week_range_utc(
    *,
    week_offset: int = 0,
) -> tuple[pd.Timestamp, pd.Timestamp, date, date]:
    """Sun–Sat calendar week in ``LOCAL_TZ`` (UTC+5). ``week_offset`` 0 = this week."""
    today = pd.Timestamp.now(tz=LOCAL_TZ).date()
    days_since_sun = (today.weekday() + 1) % 7
    week_start = today - timedelta(days=days_since_sun) + timedelta(weeks=week_offset)
    week_end = week_start + timedelta(days=6)
    return (
        _local_date_start(week_start),
        _local_date_end(week_end),
        week_start,
        week_end,
    )


def _perf_calendar_week_for_date(any_date: date) -> tuple[pd.Timestamp, pd.Timestamp, date, date]:
    """Sun–Sat calendar week containing ``any_date`` in ``LOCAL_TZ``."""
    days_since_sun = (any_date.weekday() + 1) % 7
    week_start = any_date - timedelta(days=days_since_sun)
    week_end = week_start + timedelta(days=6)
    return (
        _local_date_start(week_start),
        _local_date_end(week_end),
        week_start,
        week_end,
    )


def _perf_week_offset_for_date(any_date: date) -> int:
    """Calendar-week offset from the current Sun–Sat week (0 = this week)."""
    _, _, this_start, _ = _perf_calendar_week_range_utc(week_offset=0)
    _, _, pick_start, _ = _perf_calendar_week_for_date(any_date)
    return (pick_start - this_start).days // 7


def _apply_weekly_time_range(
    df: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> pd.DataFrame:
    """Weekly window — latest activity timestamp (same basis as Overview in-view)."""
    if df.empty:
        return df
    ref = _perf_reference_ts(df)
    mask = ref.notna() & (ref >= range_start) & (ref <= range_end)
    return df.loc[mask].copy()


def _perf_csm_attended_in_week(
    df_all: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> pd.DataFrame:
    """CSM tickets in On Hold / Resolved / Investigation with activity in the week."""
    if df_all.empty or "status" not in df_all.columns:
        return pd.DataFrame()
    blocked = _perf_unattended_ticket_numbers(df_all)
    in_range = _apply_weekly_time_range(
        df_all, range_start=range_start, range_end=range_end
    )
    if in_range.empty:
        return pd.DataFrame()
    in_range = _perf_drop_unattended_tickets(in_range, blocked)
    if in_range.empty:
        return pd.DataFrame()
    masks = _ticket_queue_count_masks(in_range)
    attended_mask = pd.Series(False, index=in_range.index)
    for key in _CSM_WEEKLY_ATTENDED_KEYS:
        attended_mask |= masks[key]
    part = in_range.loc[attended_mask].copy()
    if part.empty:
        return part
    norm = _normalized_status_series(part)
    label_map = {
        STATUS_ON_HOLD.casefold(): STATUS_ON_HOLD,
        STATUS_RESOLVED.casefold(): STATUS_RESOLVED,
        STATUS_UNDER_INVESTIGATION.casefold(): STATUS_UNDER_INVESTIGATION,
    }
    part["_attended_status"] = norm.str.casefold().map(label_map).fillna(norm)
    part["track"] = "CSM"
    part["_ts"] = _perf_reference_ts(part)
    return part


def _perf_unattended_ticket_numbers(df_all: pd.DataFrame) -> frozenset[str]:
    """Ticket IDs ever auto-unattended — excluded from attended / credit totals."""
    if df_all.empty or "ticket_number" not in df_all.columns:
        return frozenset()
    mask = _ticket_marked_unattended_mask(df_all)
    if not mask.any():
        return frozenset()
    ids = df_all.loc[mask, "ticket_number"].astype(str).str.strip()
    return frozenset(t for t in ids.tolist() if t)


def _perf_drop_unattended_tickets(
    df: pd.DataFrame,
    blocked: frozenset[str],
) -> pd.DataFrame:
    if df.empty or not blocked or "ticket_number" not in df.columns:
        return df
    keep = ~df["ticket_number"].astype(str).str.strip().isin(blocked)
    return df.loc[keep].copy()


def _perf_sales_attended_in_week(
    df_all: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> pd.DataFrame:
    """Sales cases in Investigation / Regional / Resolved with activity in the week."""
    if df_all.empty or "status" not in df_all.columns:
        return pd.DataFrame()
    in_range = _apply_weekly_time_range(
        df_all, range_start=range_start, range_end=range_end
    )
    if in_range.empty:
        return pd.DataFrame()
    effective = in_range["status"].astype(str).str.strip().map(_sc_effective_status)
    mask = effective.isin(_SC_ATTENDED_STATUSES)
    part = in_range.loc[mask].copy()
    if part.empty:
        return part
    part["_attended_status"] = effective.loc[mask].values
    part["track"] = "Sales"
    part["_ts"] = _perf_reference_ts(part)
    return part


def _perf_weekly_attended_bundle(
    df_all: pd.DataFrame,
    sales_all: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> dict[str, object]:
    """Compute weekly CSM + Sales attended once (summary, detail, counts)."""
    csm_attended = _perf_csm_attended_in_week(
        df_all, range_start=range_start, range_end=range_end
    )
    sales_attended = _perf_sales_attended_in_week(
        sales_all if sales_all is not None else pd.DataFrame(),
        range_start=range_start,
        range_end=range_end,
    )
    summary, detail = _perf_build_weekly_attended_tables(csm_attended, sales_attended)
    return {
        "csm": csm_attended,
        "sales": sales_attended,
        "summary": summary,
        "detail": detail,
        "n_csm": len(csm_attended),
        "n_sales": len(sales_attended),
        "total": len(csm_attended) + len(sales_attended),
    }


def _perf_resolve_display_category(row: pd.Series, *, track: str) -> str:
    """Outcome category when set; otherwise assignment / sales category."""
    if track == "CSM":
        outcome = canonical_task_category(row.get("outcome_category", ""))
        assigned = canonical_task_category(row.get("task_category", ""))
        cat = outcome if outcome else assigned
    else:
        cat = canonical_task_category(row.get("sales_category", ""))
        if not cat:
            cat = canonical_task_category(row.get("field_task_category", ""))
    return cat if cat else "(uncategorized)"


def _perf_build_weekly_attended_tables(
    csm_raw: pd.DataFrame,
    sales_raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(summary_by_person, detail_rows)`` for the weekly attended report."""
    detail_cols = [
        "Track",
        "ID",
        "Status",
        "Attended by",
        "Category",
        "Activity (local)",
    ]
    detail_parts: list[pd.DataFrame] = []

    if not csm_raw.empty:
        csm = _perf_enrich_tickets(csm_raw)
        detail_parts.append(
            pd.DataFrame(
                {
                    "Track": "CSM",
                    "ID": csm.get("ticket_number", pd.Series("", index=csm.index)),
                    "Status": csm.get("_attended_status", pd.Series("", index=csm.index)),
                    "Attended by": csm.get("staff", pd.Series("(unknown)", index=csm.index)),
                    "Category": csm.get("category", pd.Series("(uncategorized)", index=csm.index)),
                    "Activity (local)": csm["_local"].dt.strftime("%Y-%m-%d %H:%M"),
                }
            )
        )

    if not sales_raw.empty:
        sales = _perf_enrich_sales_cases(sales_raw)
        sales_cat = sales.get("sales_category", pd.Series("", index=sales.index)).map(
            canonical_task_category
        )
        field_cat = sales.get(
            "field_task_category", pd.Series("", index=sales.index)
        ).map(canonical_task_category)
        category = sales_cat.where(sales_cat.notna(), field_cat).fillna("(uncategorized)")
        detail_parts.append(
            pd.DataFrame(
                {
                    "Track": "Sales",
                    "ID": sales.get("case_ref", pd.Series("", index=sales.index)),
                    "Status": sales.get(
                        "_attended_status", pd.Series("", index=sales.index)
                    ),
                    "Attended by": sales.get(
                        "staff", pd.Series("(unknown)", index=sales.index)
                    ),
                    "Category": category,
                    "Activity (local)": sales["_local"].dt.strftime("%Y-%m-%d %H:%M"),
                }
            )
        )

    if detail_parts:
        detail = pd.concat(detail_parts, ignore_index=True)
    else:
        detail = pd.DataFrame(columns=detail_cols)

    empty_summary = pd.DataFrame(
        columns=[
            "Attended by",
            "CSM total",
            f"CSM {STATUS_ON_HOLD}",
            f"CSM {STATUS_RESOLVED}",
            "CSM Investigation",
            "Sales total",
            "Sales Investigation",
            "Sales Regional",
            f"Sales {STATUS_RESOLVED}",
            "Grand total",
        ]
    )
    if detail.empty:
        return empty_summary, detail

    attended = detail["Attended by"].astype(str).str.strip()
    detail = detail.loc[attended.ne("")].copy()
    if detail.empty:
        return empty_summary, detail

    csm = detail[detail["Track"] == "CSM"]
    sales = detail[detail["Track"] == "Sales"]
    people = sorted(
        detail["Attended by"].astype(str).str.strip().unique(), key=str.lower
    )
    idx = pd.Index(people, name="Attended by")

    def _count(sub: pd.DataFrame, status: str) -> pd.Series:
        if sub.empty:
            return pd.Series(0, index=idx, dtype=int)
        mask = sub["Status"].astype(str).eq(status)
        return (
            sub.loc[mask]
            .groupby("Attended by")
            .size()
            .reindex(idx, fill_value=0)
            .astype(int)
        )

    summary = pd.DataFrame({"Attended by": people})
    summary["CSM total"] = (
        csm.groupby("Attended by").size().reindex(idx, fill_value=0).astype(int).values
    )
    summary[f"CSM {STATUS_ON_HOLD}"] = _count(csm, STATUS_ON_HOLD).values
    summary[f"CSM {STATUS_RESOLVED}"] = _count(csm, STATUS_RESOLVED).values
    summary["CSM Investigation"] = _count(csm, STATUS_UNDER_INVESTIGATION).values
    summary["Sales total"] = (
        sales.groupby("Attended by").size().reindex(idx, fill_value=0).astype(int).values
    )
    summary["Sales Investigation"] = _count(sales, SC_STATUS_INVESTIGATION).values
    summary["Sales Regional"] = _count(sales, SC_STATUS_REGIONAL).values
    summary[f"Sales {STATUS_RESOLVED}"] = _count(sales, SC_STATUS_RESOLVED).values
    summary["Grand total"] = (
        detail.groupby("Attended by").size().reindex(idx, fill_value=0).astype(int).values
    )
    summary = summary.sort_values(
        ["Grand total", "Attended by"], ascending=[False, True]
    )
    totals = summary.drop(columns=["Attended by"]).sum(numeric_only=True)
    totals["Attended by"] = "TOTAL"
    summary = pd.concat([summary, pd.DataFrame([totals])], ignore_index=True)
    return summary, detail


_WEEKLY_INV_COLOR = "#E8A838"
_WEEKLY_RESOLVED_COLOR = "#9A6B2F"
_WEEKLY_CHART_THEME = {
    "background": "transparent",
}


def _weekly_altair_theme(chart: alt.Chart) -> alt.Chart:
    return (
        chart.configure(**_WEEKLY_CHART_THEME)
        .configure_view(strokeWidth=0, stroke="transparent")
        .configure_axis(labelColor="#a39e97", titleColor="#e8e6e3", gridColor="#333333")
        .configure_legend(labelColor="#e8e6e3", titleColor="#e8e6e3")
    )


def _perf_weekly_outcome_group(status: str) -> str:
    """Roll weekly statuses into Investigation vs Resolved for executive charts."""
    s = str(status or "").strip()
    if s in (STATUS_RESOLVED, SC_STATUS_RESOLVED):
        return "Resolved"
    return "Investigation"


def _perf_weekly_executive_metrics(detail_df: pd.DataFrame) -> dict[str, object]:
    """KPI + chart/table frames for the weekly executive summary."""
    empty_cat = pd.DataFrame(columns=["category", "outcome", "count"])
    empty_trend = pd.DataFrame(columns=["week", "rate", "total"])
    base: dict[str, object] = {
        "total": 0,
        "resolved": 0,
        "investigation": 0,
        "resolution_rate": 0,
        "top_inv_category": "—",
        "top_resolved_category": "—",
        "outcome_df": pd.DataFrame(columns=["outcome", "count"]),
        "category_df": empty_cat,
        "priority_df": pd.DataFrame(
            columns=["Outcome", "Assigned category", "Tickets", "Action Required"]
        ),
        "trend_df": empty_trend,
    }
    if detail_df.empty:
        return base

    view = detail_df.copy()
    view["Category"] = (
        view["Category"].astype(str).str.strip().replace("", "(uncategorized)")
    )
    view["Outcome"] = view["Status"].astype(str).str.strip().map(_perf_weekly_outcome_group)

    total = len(view)
    resolved = int(view["Outcome"].eq("Resolved").sum())
    investigation = total - resolved
    rate = int(round(100 * resolved / total)) if total else 0

    inv_by_cat = (
        view.loc[view["Outcome"].eq("Investigation")]
        .groupby("Category")
        .size()
        .sort_values(ascending=False)
    )
    res_by_cat = (
        view.loc[view["Outcome"].eq("Resolved")]
        .groupby("Category")
        .size()
        .sort_values(ascending=False)
    )

    category_df = (
        view.groupby(["Category", "Outcome"], as_index=False)
        .size()
        .rename(columns={"size": "count", "Category": "category", "Outcome": "outcome"})
        .sort_values(["count", "category"], ascending=[False, True])
    )

    priority = (
        view.groupby(["Outcome", "Category"], as_index=False)
        .size()
        .rename(
            columns={
                "size": "Tickets",
                "Outcome": "Outcome",
                "Category": "Assigned category",
            }
        )
        .sort_values(["Tickets", "Assigned category"], ascending=[False, True])
    )
    priority["Action Required"] = priority["Outcome"].map(
        lambda o: "High" if o == "Investigation" else "Review"
    )

    return {
        "total": total,
        "resolved": resolved,
        "investigation": investigation,
        "resolution_rate": rate,
        "top_inv_category": str(inv_by_cat.index[0]) if len(inv_by_cat) else "—",
        "top_resolved_category": str(res_by_cat.index[0]) if len(res_by_cat) else "—",
        "outcome_df": pd.DataFrame(
            {"outcome": ["Investigation", "Resolved"], "count": [investigation, resolved]}
        ),
        "category_df": category_df,
        "priority_df": priority,
        "trend_df": empty_trend,
    }


def _perf_weekly_resolution_trend(
    df_all: pd.DataFrame,
    sales_all: pd.DataFrame,
    *,
    end_week_offset: int = 0,
    weeks: int = 4,
) -> pd.DataFrame:
    """Resolution rate for the last ``weeks`` Sun–Sat windows ending at ``end_week_offset``."""
    rows: list[dict[str, object]] = []
    for offset in range(end_week_offset - weeks + 1, end_week_offset + 1):
        rs, re, d0, _d1 = _perf_calendar_week_range_utc(week_offset=offset)
        bundle = _perf_weekly_attended_bundle(
            df_all,
            sales_all if sales_all is not None else pd.DataFrame(),
            range_start=rs,
            range_end=re,
        )
        detail = bundle.get("detail")
        detail_df = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()
        total = int(bundle.get("total") or 0)
        if total == 0 or detail_df.empty:
            rate = 0
        else:
            resolved = int(
                detail_df["Status"]
                .astype(str)
                .str.strip()
                .isin([STATUS_RESOLVED, SC_STATUS_RESOLVED])
                .sum()
            )
            rate = int(round(100 * resolved / total))
        rows.append(
            {
                "week": d0.strftime("%d %b"),
                "rate": rate,
                "total": total,
                "sort": offset,
            }
        )
    return pd.DataFrame(rows).sort_values("sort")


def _render_weekly_kpi_cards(metrics: dict[str, object]) -> None:
    st.markdown(
        """
<style>
.weekly-exec-header {
  display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
  gap: 12px; margin-bottom: 1rem; padding-bottom: 0.75rem;
  border-bottom: 1px solid rgba(215, 180, 145, 0.35);
}
.weekly-exec-title { font-size: 1.15rem; font-weight: 600; color: #e8e6e3; margin: 0; }
.weekly-exec-sub { font-size: 0.85rem; color: #a39e97; margin: 0.15rem 0 0; }
.weekly-exec-badge {
  font-size: 0.85rem; color: #a39e97; padding: 0.45rem 0.85rem;
  border: 1px solid #374151; border-radius: 8px; background: #141414;
}
.weekly-date-wrap [data-testid="stDateInput"] label {
  font-size: 0.85rem !important; color: #a39e97 !important;
}
.weekly-date-wrap [data-testid="stDateInput"] > div {
  background: #141414 !important;
  border: 1px solid #374151 !important;
  border-radius: 8px !important;
}
.weekly-date-range {
  font-size: 0.78rem; color: #D7B491; margin: 0.25rem 0 0; text-align: right;
}
.weekly-kpi-card {
  background: #141414; border: 1px solid rgba(215, 180, 145, 0.28);
  border-radius: 10px; padding: 1rem 1.1rem; min-height: 88px;
}
.weekly-kpi-label { font-size: 0.78rem; color: #a39e97; margin: 0 0 0.35rem; }
.weekly-kpi-value { font-size: 1.65rem; font-weight: 600; color: #e8e6e3; margin: 0; line-height: 1.2; }
.weekly-kpi-sub { font-size: 0.75rem; color: #D7B491; margin: 0.35rem 0 0; }
.weekly-panel {
  background: #141414; border: 1px solid rgba(215, 180, 145, 0.28);
  border-radius: 10px; padding: 0.85rem 1rem 0.5rem; margin-bottom: 0.5rem;
}
.weekly-panel h4 {
  font-size: 0.95rem; font-weight: 600; color: #e8e6e3; margin: 0 0 0.65rem;
}
</style>
        """,
        unsafe_allow_html=True,
    )
    k1, k2, k3, k4 = st.columns(4)
    cards = [
        (k1, "Total Tickets", str(metrics["total"]), ""),
        (k2, "Resolution Rate", f"{metrics['resolution_rate']}%", ""),
        (k3, "Top 'Investigation' Category", str(metrics["top_inv_category"]), ""),
        (k4, "Top 'Resolved' Category", str(metrics["top_resolved_category"]), ""),
    ]
    for col, label, value, sub in cards:
        with col:
            sub_html = f'<p class="weekly-kpi-sub">{sub}</p>' if sub else ""
            st.markdown(
                f'<div class="weekly-kpi-card">'
                f'<p class="weekly-kpi-label">{label}</p>'
                f'<p class="weekly-kpi-value">{value}</p>{sub_html}</div>',
                unsafe_allow_html=True,
            )


def _render_perf_weekly_executive_dashboard(
    df_all: pd.DataFrame,
    sales_all: pd.DataFrame,
    bundle: dict[str, object],
    *,
    week_start: date,
    week_end: date,
    week_offset: int,
) -> None:
    """Executive summary layout — Streamlit + Altair (no React)."""
    detail = bundle.get("detail")
    detail_df = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()
    metrics = _perf_weekly_executive_metrics(detail_df)
    metrics["trend_df"] = _perf_weekly_resolution_trend(
        df_all, sales_all, end_week_offset=week_offset, weeks=4
    )

    st.markdown("#### KPI Overview & Outcome Breakdown")
    left, right = st.columns([1.35, 1])
    with left:
        _render_weekly_kpi_cards(metrics)
    with right:
        st.markdown('<div class="weekly-panel"><h4>Outcome Breakdown</h4></div>', unsafe_allow_html=True)
        outcome_df = metrics["outcome_df"]
        if isinstance(outcome_df, pd.DataFrame) and not outcome_df.empty:
            donut = _weekly_altair_theme(
                alt.Chart(outcome_df)
                .mark_arc(innerRadius=58, outerRadius=92)
                .encode(
                    theta=alt.Theta("count:Q", stack=True),
                    color=alt.Color(
                        "outcome:N",
                        scale=alt.Scale(
                            domain=["Investigation", "Resolved"],
                            range=[_WEEKLY_INV_COLOR, _WEEKLY_RESOLVED_COLOR],
                        ),
                        legend=alt.Legend(title=None, orient="right"),
                    ),
                    tooltip=[
                        alt.Tooltip("outcome:N", title="Outcome"),
                        alt.Tooltip("count:Q", title="Tickets"),
                    ],
                )
                .properties(height=220)
            )
            st.altair_chart(donut, use_container_width=True)
            st.caption(f"Total Outcome: **{metrics['total']}**")
        else:
            st.caption("No outcome data for this week.")

    st.markdown("#### Tickets by Category & Outcome")
    category_df = metrics["category_df"]
    if isinstance(category_df, pd.DataFrame) and not category_df.empty:
        bar = _weekly_altair_theme(
            alt.Chart(category_df)
            .mark_bar()
            .encode(
                x=alt.X(
                    "category:N",
                    title="Assigned Category",
                    sort=alt.EncodingSortField(field="count", op="sum", order="descending"),
                    axis=alt.Axis(labelAngle=-28),
                ),
                y=alt.Y("count:Q", title="Tickets", axis=alt.Axis(tickMinStep=1)),
                color=alt.Color(
                    "outcome:N",
                    scale=alt.Scale(
                        domain=["Investigation", "Resolved"],
                        range=[_WEEKLY_INV_COLOR, _WEEKLY_RESOLVED_COLOR],
                    ),
                    legend=alt.Legend(title="Outcome"),
                ),
                order=alt.Order("outcome:N", sort="ascending"),
                tooltip=[
                    alt.Tooltip("category:N", title="Category"),
                    alt.Tooltip("outcome:N", title="Outcome"),
                    alt.Tooltip("count:Q", title="Tickets"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.caption("No category breakdown for this week.")

    st.markdown("#### Priority Cases & Efficiency Trends")
    pri_col, trend_col = st.columns(2)
    with pri_col:
        st.markdown('<div class="weekly-panel"><h4>Priority Cases (Investigation)</h4></div>', unsafe_allow_html=True)
        priority_df = metrics["priority_df"]
        if isinstance(priority_df, pd.DataFrame) and not priority_df.empty:
            show = priority_df.copy()
            show["Action Required"] = show["Action Required"].map(
                {"High": "🚩 High", "Review": "🟧 Review"}
            )
            st.dataframe(
                show,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Tickets": st.column_config.NumberColumn(format="%d"),
                },
            )
        else:
            st.caption("No priority cases this week.")
    with trend_col:
        st.markdown('<div class="weekly-panel"><h4>Efficiency Trends</h4></div>', unsafe_allow_html=True)
        trend_df = metrics["trend_df"]
        if isinstance(trend_df, pd.DataFrame) and not trend_df.empty:
            trend = _weekly_altair_theme(
                alt.Chart(trend_df)
                .mark_area(color=_WEEKLY_INV_COLOR, opacity=0.35, line={"color": _WEEKLY_INV_COLOR})
                .encode(
                    x=alt.X("week:N", title="Week", sort=alt.EncodingSortField(field="sort", order="ascending")),
                    y=alt.Y("rate:Q", title="Resolution Rate (%)", scale=alt.Scale(domain=[0, 100])),
                    tooltip=[
                        alt.Tooltip("week:N", title="Week"),
                        alt.Tooltip("rate:Q", title="Resolution %"),
                        alt.Tooltip("total:Q", title="Tickets"),
                    ],
                )
                .properties(height=260)
            )
            st.altair_chart(trend, use_container_width=True)
        else:
            st.caption("Not enough history for trends.")


def _render_perf_weekly_attended_report(
    df_all: pd.DataFrame,
    sales_all: pd.DataFrame,
    *,
    this_week_bundle: dict[str, object] | None = None,
) -> None:
    """Sun–Sat weekly attended counts — CSM (assignee) + Sales (attended_by)."""
    _, _, default_week_start, _ = _perf_calendar_week_range_utc(week_offset=0)

    st.markdown(
        """
<style>
.weekly-exec-header {
  display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
  gap: 12px; margin-bottom: 1rem; padding-bottom: 0.75rem;
  border-bottom: 1px solid rgba(215, 180, 145, 0.35);
}
.weekly-exec-title { font-size: 1.15rem; font-weight: 600; color: #e8e6e3; margin: 0; }
.weekly-exec-sub { font-size: 0.85rem; color: #a39e97; margin: 0.15rem 0 0; }
.weekly-date-wrap [data-testid="stDateInput"] label {
  font-size: 0.85rem !important; color: #a39e97 !important;
}
.weekly-date-wrap [data-testid="stDateInput"] > div {
  background: #141414 !important;
  border: 1px solid #374151 !important;
  border-radius: 8px !important;
}
.weekly-date-range {
  font-size: 0.78rem; color: #D7B491; margin: 0.25rem 0 0; text-align: right;
}
</style>
        """,
        unsafe_allow_html=True,
    )

    h_left, h_right = st.columns([1.35, 1])
    with h_left:
        st.markdown(
            f'<div class="weekly-exec-header" style="border:none;padding:0;margin:0;">'
            f'<div><p class="weekly-exec-title">NetOps | Coverage Eye</p>'
            f'<p class="weekly-exec-sub">Executive Summary · {LOCAL_TZ_LABEL}</p></div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with h_right:
        st.markdown('<div class="weekly-date-wrap">', unsafe_allow_html=True)
        picked = st.date_input(
            "Weekly Operational Report",
            value=default_week_start,
            key=_PERF_WEEKLY_DATE_KEY,
            help=f"Pick any date in the week — report uses Sun–Sat ({LOCAL_TZ_LABEL}).",
        )
        range_start, range_end, d0, d1 = _perf_calendar_week_for_date(picked)
        week_label = f"{d0.strftime('%d %b')} – {d1.strftime('%d %b %Y')}"
        st.markdown(
            f'<p class="weekly-date-range">{week_label}</p></div>',
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div style="margin-bottom:1rem;padding-bottom:0.75rem;'
        'border-bottom:1px solid rgba(215,180,145,0.35);"></div>',
        unsafe_allow_html=True,
    )

    week_offset = _perf_week_offset_for_date(picked)
    _, _, this_start, _ = _perf_calendar_week_range_utc(week_offset=0)
    if d0 == this_start and this_week_bundle is not None:
        bundle = this_week_bundle
    else:
        bundle = _perf_weekly_attended_bundle(
            df_all,
            sales_all,
            range_start=range_start,
            range_end=range_end,
        )
    summary = bundle["summary"]
    detail = bundle["detail"]

    _render_perf_weekly_executive_dashboard(
        df_all,
        sales_all,
        bundle,
        week_start=d0,
        week_end=d1,
        week_offset=week_offset,
    )

    if summary.empty:
        st.info("No attended CSM tickets or Sales cases in this week.")
        return

    st.subheader("Staff breakdown")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    file_stamp = f"{d0.isoformat()}_{d1.isoformat()}"
    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button(
            "Download summary CSV",
            data=summary.to_csv(index=False).encode("utf-8"),
            file_name=f"weekly_attended_{file_stamp}_summary.csv",
            mime="text/csv",
            key=f"perf_weekly_summary_csv_{file_stamp}",
        )
    with dl2:
        st.download_button(
            "Download detail CSV",
            data=detail.to_csv(index=False).encode("utf-8"),
            file_name=f"weekly_attended_{file_stamp}_detail.csv",
            mime="text/csv",
            key=f"perf_weekly_detail_csv_{file_stamp}",
        )

    with st.expander(f"Case list ({len(detail)})", expanded=False):
        st.dataframe(detail, use_container_width=True, hide_index=True)


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
            # Credit total — Unattended is shown separately and does not count.
            "Total": (
                int(p_counts.get(p, 0))
                + int(o_counts.get(p, 0))
                + int(c_counts.get(p, 0))
                + int(i_counts.get(p, 0))
                + int(h_counts.get(p, 0))
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
    else:
        view = view.copy()
        view["staff"] = view["staff"].map(_perf_norm_member)
    totals = (
        view.groupby("staff", as_index=False)
        .size()
        .rename(columns={"size": value_name})
        .sort_values(value_name, ascending=False)
    )
    height = min(520, max(200, 42 * len(totals)))
    chart = (
        alt.Chart(totals)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{value_name}:Q",
                title=value_name,
                axis=alt.Axis(format=".0f", tickMinStep=1),
            ),
            y=alt.Y(
                "staff:N",
                sort="-x",
                title="",
                axis=alt.Axis(
                    labelOverlap=False,
                    labelFontSize=13,
                    labelPadding=8,
                ),
            ),
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
            "outcome_category",
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


def _perf_attach_field_responses_to_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``field_response`` from linked CSM tickets when missing on the sales row."""
    if df.empty:
        return df
    out = df.copy()
    if "field_response" not in out.columns:
        out["field_response"] = ""
    if "case_ref" not in out.columns:
        return out
    missing = out["field_response"].fillna("").astype(str).str.strip().eq("")
    if not missing.any():
        return out
    try:
        tickets = _fetch_tickets()
    except Exception:
        return out
    if tickets.empty or "ticket_number" not in tickets.columns or "field_response" not in tickets.columns:
        return out
    lookup = (
        tickets[["ticket_number", "field_response"]]
        .dropna(subset=["ticket_number"])
        .drop_duplicates(subset=["ticket_number"], keep="last")
    )
    lookup = lookup.set_index("ticket_number")["field_response"]
    refs = out.loc[missing, "case_ref"].astype(str).str.strip()
    out.loc[missing, "field_response"] = refs.map(
        lambda r: str(lookup.get(r, "") or "").strip()
    )
    return out


def _perf_sales_case_list_view(df: pd.DataFrame) -> pd.DataFrame:
    """Compact case list columns for Performance → Sales Cases."""
    if df.empty:
        return pd.DataFrame()
    view = _perf_enrich_sales_cases(df)
    view = _perf_attach_field_responses_to_sales(view)
    view = view.sort_values("_ts", ascending=False).head(200).copy()

    region = (
        view["account_region"].fillna("").astype(str).str.strip()
        if "account_region" in view.columns
        else pd.Series("", index=view.index)
    )
    if "dispatch_region" in view.columns:
        dispatch = view["dispatch_region"].fillna("").astype(str).str.strip()
        region = region.mask(region.eq(""), dispatch)

    case_note = (
        view["description"].fillna("").astype(str).str.strip()
        if "description" in view.columns
        else pd.Series("", index=view.index)
    )
    if "additional_info" in view.columns:
        extra = view["additional_info"].fillna("").astype(str).str.strip()
        case_note = case_note.mask(case_note.eq(""), extra)

    comment = (
        view["close_note"].fillna("").astype(str).str.strip()
        if "close_note" in view.columns
        else pd.Series("", index=view.index)
    )
    field_resp = (
        view["field_response"].fillna("").astype(str).str.strip()
        if "field_response" in view.columns
        else pd.Series("", index=view.index)
    )

    status = (
        view["status_eff"].fillna("").astype(str).str.strip()
        if "status_eff" in view.columns
        else pd.Series("", index=view.index)
    )

    return pd.DataFrame(
        {
            "Ticket Number": view["case_ref"].astype(str)
            if "case_ref" in view.columns
            else "",
            "Status": status,
            "Resort Name / Company Name": view["account_name"].astype(str)
            if "account_name" in view.columns
            else "",
            "Region": region,
            "Field category": view["field_task_category"].fillna("").astype(str)
            if "field_task_category" in view.columns
            else "",
            "Case note": case_note,
            "Comment (Optional)": comment,
            "Field response": field_resp,
        },
        index=view.index,
    )


def _render_perf_sales_case_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.caption("No Sales Cases to list.")
        return
    show = _perf_sales_case_list_view(df)
    if show.empty:
        st.caption("No Sales Cases to list.")
        return
    col_cfg = {
        "Ticket Number": st.column_config.TextColumn("Ticket Number", width="small"),
        "Status": st.column_config.TextColumn("Status", width="small"),
        "Resort Name / Company Name": st.column_config.TextColumn(
            "Resort Name / Company Name",
            width="medium",
        ),
        "Region": st.column_config.TextColumn("Region", width="small"),
        "Field category": st.column_config.TextColumn("Field category", width="medium"),
        "Case note": st.column_config.TextColumn("Case note", width="large"),
        "Comment (Optional)": st.column_config.TextColumn(
            "Comment (Optional)",
            width="medium",
        ),
        "Field response": st.column_config.TextColumn("Field response", width="large"),
    }
    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
        column_config=col_cfg,
    )


def _perf_sales_status_counts(view: pd.DataFrame) -> dict[str, int]:
    if view.empty or "status_eff" not in view.columns:
        return {
            "Sales Ticket": 0,
            "Investigation": 0,
            "Design": 0,
            "Resolved": 0,
        }
    st = view["status_eff"]
    return {
        "Sales Ticket": int(st.eq(SC_STATUS_SALES_TICKET).sum()),
        "Investigation": int(st.isin(_SC_INVESTIGATION_QUEUE_STATUSES).sum()),
        "Design": int(st.eq(SC_STATUS_DESIGN).sum()),
        "Resolved": int(st.eq(SC_STATUS_RESOLVED).sum()),
    }


def _render_perf_sales_status_strip(view: pd.DataFrame) -> None:
    counts = _perf_sales_status_counts(view)
    chips: list[str] = []
    for label, val in counts.items():
        if val == 0 and label != "Sales Ticket":
            continue
        chips.append(
            f'<span class="perf-queue-chip">{html.escape(label)}'
            f"<strong>{val}</strong></span>"
        )
    if not chips:
        return
    st.markdown(
        f'<div class="perf-queue-strip">{"".join(chips)}</div>',
        unsafe_allow_html=True,
    )


def _render_perf_sales_staff_bar(view: pd.DataFrame, *, title: str) -> None:
    if view.empty:
        st.caption("No data for this filter.")
        return
    if "staff" not in view.columns:
        view = _perf_enrich_sales_cases(view)
    totals = (
        view.groupby("staff", as_index=False)
        .size()
        .rename(columns={"size": "Cases"})
        .sort_values("Cases", ascending=False)
    )
    height = min(420, max(180, 38 * len(totals)))
    chart = (
        alt.Chart(totals)
        .mark_bar(color="#D7B491")
        .encode(
            x=alt.X("Cases:Q", title="Cases"),
            y=alt.Y("staff:N", sort="-x", title="Sales credit"),
            tooltip=[
                alt.Tooltip("staff:N", title="Sales credit"),
                alt.Tooltip("Cases:Q", title="Cases"),
            ],
        )
        .properties(height=height, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_perf_sales_status_bar(view: pd.DataFrame, *, title: str) -> None:
    counts = _perf_sales_status_counts(view)
    rows = [{"Status": k, "Cases": v} for k, v in counts.items() if v > 0]
    if not rows:
        st.caption("No cases by status.")
        return
    data = pd.DataFrame(rows)
    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("Cases:Q", title="Cases"),
            y=alt.Y("Status:N", sort="-x", title=""),
            color=alt.Color(
                "Status:N",
                legend=None,
                scale=alt.Scale(
                    domain=list(counts.keys()),
                    range=["#9ec5e8", "#c4a882", "#D7B491", "#8fa89e"],
                ),
            ),
            tooltip=[
                alt.Tooltip("Status:N", title="Queue"),
                alt.Tooltip("Cases:Q", title="Cases"),
            ],
        )
        .properties(height=220, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_perf_sales_cases_tab(
    sales_df: pd.DataFrame,
    *,
    focus: str,
    bucket_fmt: str,
    x_title: str,
    axis_format: str,
) -> None:
    """Sales Cases in the Performance window — queues, staff, and case list."""
    st.caption(
        "Cases in the sidebar **Time range**, plus any still in an active sales queue. "
        "**Performance credit** = field engineer when `assigned_to` is set, otherwise **Admin**."
    )
    if sales_df.empty:
        st.info("No Sales Cases for this filter — try **All** in Focus Assignee or widen the time range.")
        return

    view = _perf_enrich_sales_cases(sales_df)
    counts = _perf_sales_status_counts(view)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total", len(view))
    c2.metric("Sales Ticket", counts["Sales Ticket"])
    c3.metric("Investigation", counts["Investigation"])
    c4.metric("Design", counts["Design"])
    c5.metric("Resolved", counts["Resolved"])

    _render_perf_sales_status_strip(view)

    chart_l, chart_r = st.columns(2)
    with chart_l:
        _render_perf_sales_status_bar(view, title="By queue")
    with chart_r:
        staff_title = (
            f"By sales credit ({focus})"
            if focus not in ("", "All")
            else "By sales credit (field or Admin)"
        )
        _render_perf_sales_staff_bar(view, title=staff_title)

    split = (
        view.groupby(["status_eff", "category"], as_index=False)
        .size()
        .rename(columns={"size": "Cases", "status_eff": "Status", "category": "Category"})
        .sort_values(["Cases", "Status", "Category"], ascending=[False, True, True])
    )
    if not split.empty:
        st.markdown("**Split (status × category)**")
        st.dataframe(split, use_container_width=True, hide_index=True)

    with st.expander("Trend over time", expanded=len(view) <= 12):
        if "staff" in view.columns and "_local" in view.columns:
            _render_perf_stacked_staff_chart(
                view,
                y_title="Sales Cases",
                bucket_fmt=bucket_fmt,
                x_title=x_title,
                axis_format=axis_format,
            )
        else:
            st.caption("No timeline data for this range.")

    st.markdown("**Case list**")
    _render_perf_sales_case_table(sales_df)


@st.cache_data(ttl=30, show_spinner=False)
def _fetch_latest_attendance_ts_cached() -> str | None:
    """ISO timestamp string for cache serialization, or None."""
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
    return ts.to_pydatetime().isoformat()


def _fetch_latest_attendance_timestamp() -> datetime | None:
    """Return newest log row timestamp, or None if table empty / unreadable."""
    iso = _fetch_latest_attendance_ts_cached()
    if not iso:
        return None
    ts = pd.to_datetime(iso, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def _maybe_toast_new_telegram_activity() -> None:
    """Detect new bot/field log rows; refresh ticket cache so queues update."""
    _fetch_latest_attendance_ts_cached.clear()
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
        st.session_state[_DASH_LAST_ATTENDANCE_TS_KEY] = latest_iso
        _invalidate_dashboard_data_cache()
        st.toast(
            "New field activity — refreshing **Open** / **Daily Task** queues.",
            icon="📥",
        )
        st.rerun()


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _to_local(series: pd.Series) -> pd.Series:
    return _parse_ts(series).dt.tz_convert(LOCAL_TZ)


def _format_local(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()]
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
    cleaned = _canonical_username_stem(raw)
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
    if not _cc_execute_ticket_update_if(
        client, payload, ticket_number, match=None, require_match=False
    ):
        raise RuntimeError(f"Update failed for ticket {ticket_number}")


def _cc_execute_ticket_update_if(
    client,
    payload: dict,
    ticket_number: str,
    *,
    match: dict[str, object] | None,
    require_match: bool = True,
) -> bool:
    """Update ticket; optional optimistic match (e.g. last_assigned_at unchanged)."""
    attempt = _cc_strip_missing_ticket_columns(dict(payload))
    last_err: Exception | None = None
    for _ in range(4):
        try:
            q = client.table(TICKETS_TABLE).update(attempt).eq(
                "ticket_number", ticket_number
            )
            if match:
                for col, expected in match.items():
                    if expected is None:
                        q = q.is_(col, "null")
                    else:
                        q = q.eq(col, expected)
            res = q.execute()
            rows = res.data or []
            if require_match and match and not rows:
                return False
            return True
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
    return False


def _cc_insert_assignment(
    client,
    ticket_number: str,
    assigned_to: str,
    task_category: str,
    *,
    additional_info: str | None = None,
    operator_id: str,
    assigned_to_2: str | None = None,
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
    if assigned_to_2:
        row["assigned_to_2"] = assigned_to_2
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
    # Phase 2: open a new visit cycle for this assignment
    _visits_open_new(client, ticket_number, assigned_to, visit_start=now_iso)


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


def _cc_map_sales_status_to_csm(sales_status: str) -> str:
    """Pick a CSM queue when moving a sales case to field tickets."""
    if str(sales_status or "").strip() == SC_STATUS_RESOLVED:
        return STATUS_RESOLVED
    return STATUS_DAILY_TASK


def _cc_insert_transferred_ticket(
    client,
    ticket_number: str,
    *,
    task_category: str,
    status: str,
    assigned_to: str | None,
    additional_info: str | None,
    operator_id: str,
    last_assigned_at: object | None = None,
) -> None:
    """Insert a CSM ticket row when moving from Sales Cases."""
    now_iso = _cc_utc_now_iso()
    handle: str | None = None
    if assigned_to:
        handle = _cc_normalize_handle(assigned_to)
    row: dict = {
        "ticket_number": ticket_number,
        "assigned_to": handle,
        "task_category": task_category,
        "status": status,
        "field_response": None,
        "field_responded_by": None,
        "photo_url": None,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
        "unattended_nudge_sent_at": None,
    }
    if handle:
        la = last_assigned_at if last_assigned_at else None
        row["last_assigned_at"] = la if la else now_iso
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
        member_username=f"@{operator_id.lstrip('@')}",
        action_type="TransferredFromSales",
        note=_cc_assignment_log_note(
            f"Moved from Sales Cases → **{status}**.",
            operator_id,
        ),
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
    expected_last_assigned_at: object | None = None,
    assigned_to_2: str | None = None,
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
        "assigned_to_2": assigned_to_2,
    }
    if not _cc_execute_ticket_update_if(
        client,
        updates,
        ticket_number,
        match={"last_assigned_at": expected_last_assigned_at},
        require_match=True,
    ):
        raise ValueError(
            f"Ticket **{ticket_number}** was changed by someone else "
            "(assignment timestamp mismatch). Refresh the queue and try again."
        )
    _cc_ensure_reassign_cleared_response_fields(client, ticket_number)

    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=_cc_assignment_log_note(additional_info, operator_id),
    )
    # Phase 2: close previous visit as 'reassigned', open new visit for new assignee
    _visits_reassign(client, ticket_number, assigned_to, now_iso=now_iso)


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
    assigned_to_2: str | None = None,
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
            assigned_to_2=assigned_to_2,
        )
        return f"Created ticket **{ticket_number}** and logged assignment."
    _cc_reassign_ticket(
        client,
        ticket_number,
        assigned_to,
        task_category,
        additional_info=additional_info,
        operator_id=operator_id,
        expected_last_assigned_at=existing.get("last_assigned_at"),
        assigned_to_2=assigned_to_2,
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
    assigned_to_2: str | None = None,
) -> dict:
    """Update assignment fields for a ticket in the given status (dashboard edit)."""
    client = _get_supabase_client()
    row = _fetch_ticket_row(ticket_number)
    if not row:
        raise ValueError(f"Ticket **{ticket_number}** not found.")
    status = _normalize_ticket_status_value(row.get("status"))
    if status != required_status:
        raise ValueError(f"Only **{required_status}** tickets can be edited here.")

    now_iso = _cc_utc_now_iso()
    prev_handle = str(row.get("assigned_to") or "").strip()
    updates: dict[str, object] = {
        "assigned_to": assigned_to,
        "assigned_to_2": assigned_to_2,
        "task_category": task_category,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
        "updated_at": now_iso,
    }
    first_assignee = bool(assigned_to and not prev_handle)
    if first_assignee:
        updates["last_assigned_at"] = now_iso
    _cc_execute_ticket_update(client, updates, ticket_number)
    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=f"@{operator_id.lstrip('@')}",
        action_type="AssignmentUpdated",
        note=_cc_assignment_log_note(additional_info, operator_id),
    )
    if first_assignee:
        _visits_open_new(client, ticket_number, assigned_to, visit_start=now_iso)
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
    assigned_to_2: str | None = None,
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
        expected_last_assigned_at=row.get("last_assigned_at"),
        assigned_to_2=assigned_to_2,
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
    if _normalize_ticket_status_value(row.get("status")) != required_status:
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
    current_handle_2 = str(row.get("assigned_to_2") or "").strip().lstrip("@")
    current_cat = str(row.get("task_category") or "").strip()
    current_notes = str(row.get("additional_info") or "")

    cats = cat_names if cat_names else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    if current_cat and current_cat not in cats:
        cats = [current_cat, *cats]

    _sync_assignment_edit_widgets(
        keys=keys,
        picked=picked,
        current_handle=current_handle,
        current_handle_2=current_handle_2,
        current_cat=current_cat,
        current_notes=current_notes,
        cats=cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
    )

    with st.form(f"{edit_key_prefix}_assignment_edit_form", clear_on_submit=False):
        _render_engineer_pair_fields(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=keys["engineer"],
            engineer2_key=keys["engineer_2"],
        )
        _render_category_selectbox("Category", cats, key=keys["category"])
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
        handle, handle2 = _resolve_engineer_pair(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=keys["engineer"],
            engineer2_key=keys["engineer_2"],
            required_primary=True,
        )
        cat = str(st.session_state.get(keys["category"], "")).strip()
        if not cat:
            raise ValueError("Pick a category.")
        notes = str(st.session_state.get(keys["notes"], "")).strip() or None
        notes = _coassignee_telegram_note(handle2, notes)
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
            assigned_to=handle or "",
            assigned_to_2=handle2,
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
    current_handle_2 = str(row.get("assigned_to_2") or "").strip().lstrip("@")
    current_cat = str(row.get("task_category") or "").strip()
    current_notes = str(row.get("additional_info") or "")

    cats = cat_names if cat_names else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    if current_cat and current_cat not in cats:
        cats = [current_cat, *cats]

    _sync_assignment_edit_widgets(
        keys=keys,
        picked=picked,
        current_handle=current_handle,
        current_handle_2=current_handle_2,
        current_cat=current_cat,
        current_notes=current_notes,
        cats=cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
    )

    with st.form(f"{edit_key_prefix}_reassign_form", clear_on_submit=False):
        _render_engineer_pair_fields(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=keys["engineer"],
            engineer2_key=keys["engineer_2"],
        )
        _render_category_selectbox("Category", cats, key=keys["category"])
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
        handle, handle2 = _resolve_engineer_pair(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=keys["engineer"],
            engineer2_key=keys["engineer_2"],
            required_primary=True,
        )
        cat = str(st.session_state.get(keys["category"], "")).strip()
        if not cat:
            raise ValueError("Pick a category.")
        notes = str(st.session_state.get(keys["notes"], "")).strip() or None
        notes = _coassignee_telegram_note(handle2, notes)
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
            assigned_to=handle or "",
            assigned_to_2=handle2,
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
    _invalidate_dashboard_data_cache()
    st.rerun()


def _sc_patch_assignment_fields(
    row_id: str,
    *,
    assigned_to: str | None,
    field_task_category: str,
    additional_info: str | None,
    operator_id: str,
    account_region: str | None = None,
    account_name: str | None = None,
    assigned_to_2: str | None = None,
) -> dict:
    """Update sales case field assignment (same fields as CSM edit assignment)."""
    row = _fetch_sales_case_row_by_id(row_id)
    if not row:
        raise ValueError("Sales case not found.")
    if _sc_effective_status(row.get("status")) == SC_STATUS_RESOLVED:
        raise ValueError("Cannot edit assignment on a **Resolved** sales case.")

    prev_handle = str(row.get("assigned_to") or "").strip()
    patch: dict[str, object] = {
        "assigned_to": assigned_to,
        "assigned_to_2": assigned_to_2,
        "field_task_category": field_task_category,
        "additional_info": additional_info,
        "admin_owner": operator_id,
    }
    an = (account_name or "").strip()
    if an:
        patch["account_name"] = an
    region = (account_region or "").strip()
    if region:
        if region not in SALES_REGION_CODES:
            raise ValueError("Pick a valid **Region Team**.")
        patch["account_region"] = region
        patch["dispatch_region"] = region
    _sc_stamp_last_assigned_at_if_first(patch, prev_assigned_to=prev_handle)
    _sales_cases_update_row(row_id, patch)
    updated = _fetch_sales_case_row_by_id(row_id)
    return updated or row


async def _sc_sync_assignment_to_telegram(
    *,
    case_ref: str,
    assigned_to: str,
    task_category: str,
    additional_info: str | None,
    operator_id: str,
    token: str,
    chat_id: int | str,
) -> str:
    """Edit linked Telegram message or post a new one for a sales case."""
    assigned_by = f"{operator_id} (updated)"
    api_id = _read_setting("TG_API_ID") or _read_setting("TELEGRAM_API_ID") or None
    api_hash = _read_setting("TG_API_HASH") or _read_setting("TELEGRAM_API_HASH") or None

    found = await find_assignment_telegram_ref(
        case_ref,
        group_id=chat_id,
        bot_token=token,
        api_id=api_id,
        api_hash=api_hash,
    )
    if found:
        try:
            await update_telegram_assignment_message(
                int(found.chat_id),
                int(found.message_id),
                assigned_to,
                case_ref,
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

    await notify_telegram_group(
        assigned_to,
        case_ref,
        task_category,
        additional_info=additional_info,
        assigned_by=assigned_by,
        updated=True,
        api_id=api_id,
        api_hash=api_hash,
        bot_token=token,
        group_id=chat_id,
    )
    return (
        "Posted a **new** assignment message at the bottom of the group "
        "(starts with “Assignment updated”). Use that message for field replies."
    )


def _render_sales_assignment_editor(
    *,
    key_prefix: str,
    edit_key_prefix: str,
    field_cats: list[str],
    fe_names: list[str],
    fe_missing: bool,
    case_options: list[str],
    df: pd.DataFrame,
) -> None:
    """Edit sales case field assignment — same form template as CSM."""
    if not case_options:
        return

    keys = _assignment_edit_session_keys(edit_key_prefix)
    row_id, cref, r0 = _picked_sales_case_from_selection(
        df, key_prefix=key_prefix, case_options=case_options
    )
    if not row_id or r0 is None or not cref:
        return

    if _sc_effective_status(r0.get("status")) == SC_STATUS_RESOLVED:
        st.info(f"Pick an active sales case — **{cref}** is **Resolved**.")
        return

    st.caption(
        f"Editing **{cref}** — saves to the dashboard. "
        "Telegram: tries to **edit the original** post when found; otherwise posts "
        "“Assignment updated” at the bottom of the group."
    )

    current_handle = _sc_row_text(r0.get("assigned_to")).lstrip("@")
    current_handle_2 = _sc_row_text(r0.get("assigned_to_2")).lstrip("@")
    current_cat = _sc_row_text(r0.get("field_task_category")) or _sc_row_text(
        r0.get("sales_category")
    )
    current_notes = _sc_row_text(r0.get("additional_info")) or _sc_row_text(
        r0.get("description")
    )

    cats = _sales_category_options(field_cats)
    if current_cat:
        canon = canonical_task_category(current_cat) or current_cat
        if canon not in cats:
            cats = [canon, *cats]
    account_key = f"{edit_key_prefix}_sc_edit_assign_account"
    synced_key = f"{edit_key_prefix}_sc_edit_assign_synced"
    region_watch_key = f"{edit_key_prefix}_sc_edit_assign_region_watch"
    engineer_blank_key = f"{edit_key_prefix}_sc_edit_assign_engineer_blank"
    if st.session_state.get(synced_key) != cref:
        st.session_state[synced_key] = cref
        cur_region = _sc_row_text(r0.get("account_region"))
        st.session_state[region_key] = (
            cur_region if cur_region in SALES_REGION_CODES else SALES_REGION_CODES[0]
        )
        st.session_state[account_key] = _sc_row_text(r0.get("account_name"))
        st.session_state.pop(region_watch_key, None)

    central_on_load = _sc_region_is_central(st.session_state.get(region_key))
    _sync_assignment_edit_widgets(
        keys=keys,
        picked=cref,
        current_handle=current_handle,
        current_handle_2=current_handle_2,
        current_cat=current_cat,
        current_notes=current_notes,
        cats=cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
        skip_engineer=not central_on_load,
    )

    with st.form(f"{edit_key_prefix}_sc_assignment_edit_form", clear_on_submit=False):
        st.text_input(
            "Resort Name / Company Name",
            key=account_key,
            placeholder="Resort or company name",
        )
        st.selectbox(
            "Region Team",
            options=list(SALES_REGION_CODES),
            key=region_key,
        )
        central = _sc_sync_region_engineer_widgets(
            region_key=region_key,
            region_watch_key=region_watch_key,
            engineer_select_key=keys["engineer"],
            engineer_manual_key=keys["engineer"],
            fe_names=fe_names,
            fe_missing=fe_missing,
            restore_handle=current_handle,
            engineer_select2_key=keys["engineer_2"],
            engineer_manual2_key=keys["engineer_2"],
            restore_handle_2=current_handle_2,
        )
        _render_sc_engineer_field(
            central=central,
            fe_names=fe_names,
            fe_missing=fe_missing,
            select_key=keys["engineer"],
            manual_key=keys["engineer"],
            blank_key=engineer_blank_key,
            select2_key=keys["engineer_2"],
            manual2_key=keys["engineer_2"],
        )
        _render_category_selectbox("Category", cats, key=keys["category"])
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
        submitted = st.form_submit_button(
            "Save assignment changes",
            use_container_width=True,
        )

    if not submitted:
        return

    try:
        region = str(st.session_state.get(region_key, "")).strip()
        central = _sc_region_is_central(region)
        handle, handle2 = _sc_resolve_sales_engineer_pair(
            central=central,
            fe_names=fe_names,
            fe_missing=fe_missing,
            select_key=keys["engineer"],
            manual_key=keys["engineer"],
            select2_key=keys["engineer_2"],
            manual2_key=keys["engineer_2"],
        )
        cat = str(st.session_state.get(keys["category"], "")).strip()
        if not cat:
            raise ValueError("Pick a category.")
        notes = str(st.session_state.get(keys["notes"], "")).strip() or None
        notes = _coassignee_telegram_note(handle2, notes)
        account_name = str(st.session_state.get(account_key, "")).strip()
        if not account_name:
            raise ValueError("Fill **Resort Name / Company Name**.")
    except ValueError as exc:
        st.error(str(exc))
        return

    op = _session_operator_id()
    if not op:
        st.error("Sign in again — operator session is missing.")
        return

    try:
        _sc_patch_assignment_fields(
            row_id,
            assigned_to=handle,
            assigned_to_2=handle2,
            field_task_category=cat,
            additional_info=notes,
            operator_id=op,
            account_region=region,
            account_name=account_name,
        )
    except Exception as exc:
        st.error(f"Could not save: {exc}")
        return

    tg_note = ""
    if handle and st.session_state.get(keys["sync_tg"]):
        token, chat_id = _cc_resolve_telegram_credentials()
        if not token or chat_id is None:
            st.warning(
                "Saved in dashboard. Telegram not updated — set **TELEGRAM_TOKEN** and "
                "**TELEGRAM_GROUP_CHAT_ID** in `.env` / Secrets."
            )
        else:
            try:
                tg_note = asyncio.run(
                    _sc_sync_assignment_to_telegram(
                        case_ref=cref,
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
    flash = f"Updated assignment for **{cref}**."
    if tg_note:
        flash += f" {tg_note}"
    _sc_set_sales_flash(flash)
    _invalidate_dashboard_data_cache()
    st.rerun()


def _sc_dashboard_reassign_case(
    row_id: str,
    *,
    assigned_to: str,
    field_task_category: str,
    additional_info: str | None,
    operator_id: str,
    assigned_to_2: str | None = None,
) -> dict:
    """Reassign field engineer on a sales case (same intent as CSM reassign)."""
    row = _fetch_sales_case_row_by_id(row_id)
    if not row:
        raise ValueError("Sales case not found.")
    status = _sc_effective_status(row.get("status"))
    if status == SC_STATUS_RESOLVED:
        raise ValueError("Cannot reassign a **Resolved** sales case.")

    patch: dict[str, object] = {
        "assigned_to": assigned_to,
        "assigned_to_2": assigned_to_2,
        "field_task_category": field_task_category,
        "admin_owner": operator_id,
    }
    if additional_info is not None:
        patch["additional_info"] = additional_info
    _sc_stamp_last_assigned_at(patch)
    _sales_cases_update_row(row_id, patch)
    updated = _fetch_sales_case_row_by_id(row_id)
    return updated or row


def _render_sales_reassign_editor(
    *,
    key_prefix: str,
    edit_key_prefix: str,
    field_cats: list[str],
    fe_names: list[str],
    fe_missing: bool,
    case_options: list[str],
    df: pd.DataFrame,
) -> None:
    """Reassign field engineer on a sales case; optional new Telegram post."""
    if not case_options:
        return

    keys = _reassign_session_keys(edit_key_prefix)
    row_id, cref, r0 = _picked_sales_case_from_selection(
        df, key_prefix=key_prefix, case_options=case_options
    )
    if not row_id or r0 is None or not cref:
        st.info("Select a case in this queue, then click **Reassign** again.")
        return
    if cref not in case_options:
        st.info("Select a case in this queue, then click **Reassign** again.")
        return

    status = _sc_effective_status(r0.get("status"))
    if status == SC_STATUS_RESOLVED:
        st.info(f"Cannot reassign — case **{cref}** is **Resolved**.")
        return

    st.caption(
        f"Reassign **{cref}** to a different field engineer. "
        "Posts a **new** assignment line in Telegram when enabled below."
    )

    current_handle = _sc_row_text(r0.get("assigned_to")).lstrip("@")
    current_handle_2 = _sc_row_text(r0.get("assigned_to_2")).lstrip("@")
    current_cat = _sc_row_text(r0.get("field_task_category")) or _sc_row_text(
        r0.get("sales_category")
    )
    current_notes = _sc_row_text(r0.get("additional_info")) or _sc_row_text(
        r0.get("description")
    )

    cats = _sales_category_options(field_cats)
    if current_cat:
        canon = canonical_task_category(current_cat) or current_cat
        if canon not in cats:
            cats = [canon, *cats]

    _sync_assignment_edit_widgets(
        keys=keys,
        picked=cref,
        current_handle=current_handle,
        current_handle_2=current_handle_2,
        current_cat=current_cat,
        current_notes=current_notes,
        cats=cats,
        fe_names=fe_names,
        fe_missing=fe_missing,
    )

    with st.form(f"{edit_key_prefix}_sc_reassign_form", clear_on_submit=False):
        _render_engineer_pair_fields(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=keys["engineer"],
            engineer2_key=keys["engineer_2"],
        )
        _render_category_selectbox("Category", cats, key=keys["category"])
        st.text_area("Notes (Additional Info)", height=80, key=keys["notes"])
        st.checkbox(
            "Post New Telegram Assignment",
            value=True,
            key=keys["sync_tg"],
        )
        submitted = st.form_submit_button(
            "Reassign engineer",
            type="primary",
            use_container_width=True,
        )

    if not submitted:
        return

    try:
        handle, handle2 = _resolve_engineer_pair(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=keys["engineer"],
            engineer2_key=keys["engineer_2"],
            required_primary=True,
        )
        cat = str(st.session_state.get(keys["category"], "")).strip()
        if not cat:
            raise ValueError("Pick a category.")
        notes = str(st.session_state.get(keys["notes"], "")).strip() or None
        notes = _coassignee_telegram_note(handle2, notes)
    except ValueError as exc:
        st.error(str(exc))
        return

    op = _session_operator_id()
    if not op:
        st.error("Sign in again — operator session is missing.")
        return

    try:
        _sc_dashboard_reassign_case(
            row_id,
            assigned_to=handle or "",
            assigned_to_2=handle2,
            field_task_category=cat,
            additional_info=notes,
            operator_id=op,
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
                asyncio.run(
                    notify_telegram_group(
                        handle.lstrip("@"),
                        cref,
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
                tg_note = (
                    "Posted a **new** assignment message in the group — "
                    "field must swipe-reply to that line."
                )
            except Exception as exc:
                st.warning(f"Reassigned in dashboard. Telegram post failed: {exc}")

    st.session_state[keys["show"]] = False
    flash = f"**{cref}** reassigned to **{handle}** ({cat})."
    if tg_note:
        flash += f" {tg_note}"
    _sc_set_sales_flash(flash)
    _invalidate_dashboard_data_cache()
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


@st.cache_data(ttl=300, show_spinner=False)
def _cached_field_engineer_usernames() -> tuple[tuple[str, ...], bool]:
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
            return (), True
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return (), False
        raise
    rows = res.data or []
    names = [str(r["username"]) for r in rows if r.get("username")]
    return tuple(sorted(set(names), key=str.lower)), False


def _try_fetch_field_engineer_usernames() -> tuple[list[str], bool]:
    names, missing = _cached_field_engineer_usernames()
    return list(names), missing


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


@st.cache_data(ttl=300, show_spinner=False)
def _cached_task_categories() -> tuple[tuple[str, ...], bool]:
    """Return ``(category names, table_missing)`` from Supabase."""
    client = _get_supabase_client()
    _ensure_task_categories_synced(client)
    try:
        names, missing = fetch_task_category_names(client)
        return tuple(names), missing
    except Exception as exc:
        if _looks_like_missing_table_error(exc):
            return (), True
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return tuple(DEFAULT_ASSIGNMENT_TASK_CATEGORIES), False
        raise


def _try_fetch_task_categories() -> tuple[list[str], bool]:
    names, missing = _cached_task_categories()
    if missing:
        return list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES), True
    return _merge_category_option_lists(DEFAULT_ASSIGNMENT_TASK_CATEGORIES, names), False


def _try_fetch_task_categories_db_only() -> tuple[list[str], bool]:
    """Categories stored in Supabase (no built-in defaults) — for add/remove UI."""
    client = _get_supabase_client()
    try:
        names, missing = fetch_task_category_names(
            client, include_defaults_if_empty=False
        )
        return list(names), missing
    except Exception as exc:
        if _looks_like_missing_table_error(exc):
            return [], True
        if is_transient_supabase_error(exc):
            _note_supabase_unreachable(exc)
            return [], False
        raise


def _merge_category_option_lists(*lists: list[str] | tuple[str, ...]) -> list[str]:
    """Dedupe category labels case-insensitively; preserve first-seen order."""
    out: list[str] = []
    seen: set[str] = set()
    for lst in lists:
        for raw in lst:
            name = str(raw).strip()
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(name)
    return out


def _sales_category_options(
    task_categories: list[str] | None = None,
) -> list[str]:
    """Sales Category (Intent): sales defaults plus all CSM field categories."""
    if task_categories is None:
        task_categories, _ = _try_fetch_task_categories()
    field_cats = (
        task_categories
        if task_categories
        else list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES)
    )
    return _merge_category_option_lists(DEFAULT_SALES_CASE_CATEGORIES, field_cats)


def _categories_manage_popover(
    *,
    missing: bool,
    select_key: str = _CC_CATEGORY_SELECT_KEY,
    pending_key: str = _CC_CATEGORY_SELECT_PENDING_KEY,
    new_category_key: str = "cc_new_category",
    add_button_key: str = "cc_cat_add_btn",
    remove_button_prefix: str = "cat_rm",
) -> None:
    db_cats, db_missing = _try_fetch_task_categories_db_only()
    if db_missing or missing:
        st.caption(f"Category table missing — apply `{TASK_CATEGORIES_TABLE}` migration.")
        return

    if not db_cats:
        st.caption("No categories in Supabase yet — add one below.")
    for cat in db_cats:
        c_name, c_rm = st.columns([5, 1], gap="small", vertical_alignment="center")
        with c_name:
            st.markdown(f"**{cat}**")
        with c_rm:
            hkey = hashlib.sha256(cat.encode("utf-8")).hexdigest()[:16]
            if st.button(
                "×",
                key=f"{remove_button_prefix}_{hkey}",
                help=f"Remove {cat}",
                type="secondary",
            ):
                try:
                    delete_task_category(_get_supabase_client(), cat)
                    _cached_task_categories.clear()
                    st.session_state.pop(select_key, None)
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    c_add, c_go = st.columns([5, 1], gap="small", vertical_alignment="bottom")
    with c_add:
        st.text_input(
            "Add Category",
            key=new_category_key,
            placeholder="e.g. Site Survey",
            label_visibility="collapsed",
        )
    with c_go:
        if st.button("+", key=add_button_key, help="Add Category", type="secondary"):
            raw = str(st.session_state.get(new_category_key) or "").strip()
            if not raw:
                st.warning("Type a category name first.")
            else:
                try:
                    norm = normalize_task_category_name(raw)
                    if any(c.lower() == norm.lower() for c in db_cats):
                        st.warning(f"**{norm}** is already listed.")
                    else:
                        upsert_task_category(_get_supabase_client(), norm)
                        _cached_task_categories.clear()
                        st.session_state.pop(new_category_key, None)
                        st.session_state[pending_key] = norm
                        if pending_key == _CC_CATEGORY_SELECT_PENDING_KEY:
                            st.session_state[_CC_FLASH_KEY] = (
                                f"Category **{norm}** saved to Supabase — "
                                "assign picker and Telegram bot use it on the next assignment."
                            )
                        elif pending_key in (
                            _SC_CC_ST_SCAT_PENDING_KEY,
                            _SC_EDIT_SCAT_PENDING_KEY,
                        ):
                            st.session_state[_SC_SALES_FLASH_KEY] = (
                                f"Category **{norm}** saved — available in Sales and CSM pickers."
                            )
                            st.session_state[_SC_SALES_FLASH_LEVEL_KEY] = "success"
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
    elif st.session_state.get(_CC_CATEGORY_SELECT_KEY) not in opts:
        st.session_state.pop(_CC_CATEGORY_SELECT_KEY, None)
    _render_category_selectbox("Category", opts, key=_CC_CATEGORY_SELECT_KEY)
    with st.popover("Edit categories", key="cc_categories_popover"):
        _categories_manage_popover(missing=missing)


def _render_sales_category_row(
    task_categories: list[str],
    *,
    missing: bool,
    select_key: str = _SC_CC_ST_SCAT_KEY,
    pending_key: str = _SC_CC_ST_SCAT_PENDING_KEY,
    popover_key: str = "sc_categories_popover",
    key_prefix: str = "sc_cat",
    label: str = "Sales Category (Intent)",
    help: str | None = (
        "Used for the sales case and for Telegram field assignment when assigning."
    ),
    extra_options: list[str] | None = None,
) -> None:
    opts = _sales_category_options(task_categories)
    if extra_options:
        opts = _merge_category_option_lists(extra_options, opts)
    pending = st.session_state.pop(pending_key, None)
    if pending is not None and pending in opts:
        st.session_state[select_key] = pending
    elif st.session_state.get(select_key) not in opts:
        st.session_state.pop(select_key, None)
    _render_category_selectbox(label, opts, key=select_key, help=help)
    with st.popover("Edit categories", key=popover_key):
        _categories_manage_popover(
            missing=missing,
            select_key=select_key,
            pending_key=pending_key,
            new_category_key=f"{key_prefix}_new_category",
            add_button_key=f"{key_prefix}_add_btn",
            remove_button_prefix=f"{key_prefix}_rm",
        )


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
                    _cached_field_engineer_usernames.clear()
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
                        _cached_field_engineer_usernames.clear()
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


def _render_cc_engineer_row(
    names: list[str],
    *,
    missing: bool,
    select_key: str = _CC_FE_SELECT_KEY,
    manual_key: str = _CC_FE_MANUAL_KEY,
    select2_key: str = _CC_FE_SELECT_2_KEY,
    manual2_key: str = _CC_FE_MANUAL_2_KEY,
    team_popover_key: str = "cc_team_popover",
) -> None:
    """Engineer pickers + team list popover."""
    if missing:
        st.info(
            f"Directory table missing — type a username below, or add "
            f"`{FIELD_ENGINEERS_TABLE}` in Supabase."
        )
        st.text_input("Engineer", placeholder="@ibeyx", key=manual_key)
        st.text_input(
            "Engineer 2 (optional)",
            placeholder="@username (optional)",
            key=manual2_key,
        )
        return

    _render_engineer_pair_fields(
        fe_names=names,
        fe_missing=missing,
        engineer_key=select_key if names else manual_key,
        engineer2_key=select2_key if names else manual2_key,
    )

    with st.popover("Edit team", key=team_popover_key):
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
    for k in (
        _CC_FE_SELECT_KEY,
        _CC_FE_SELECT_2_KEY,
        _CC_FE_MANUAL_KEY,
        _CC_FE_MANUAL_2_KEY,
    ):
        st.session_state.pop(k, None)
    st.session_state.pop(_CC_CATEGORY_SELECT_KEY, None)


def _cc_schedule_assign_form_clear() -> None:
    """Defer clear until next run (cannot mutate widget keys after they render)."""
    st.session_state[_CC_CLEAR_ASSIGN_KEY] = True


def _cc_resolve_intake_engineer_handle(
    fe_names: list[str],
    *,
    fe_missing: bool,
    select_key: str,
    manual_key: str,
) -> str:
    if fe_names and not fe_missing:
        pick = st.session_state.get(select_key)
        if not pick or not str(pick).strip():
            raise ValueError("Pick an engineer from the list.")
        return _cc_normalize_handle(str(pick))
    raw = str(st.session_state.get(manual_key, "")).strip()
    if not raw:
        raise ValueError("Enter an engineer Telegram username.")
    return _cc_normalize_handle(raw)


def _cc_resolve_telegram_credentials_for_form() -> tuple[str | None, int | str | None, str | None]:
    """Token + group id from env/secrets, with session-only form fallbacks."""
    token = (
        _read_setting("TG_BOT_TOKEN").strip()
        or _read_setting("TELEGRAM_BOT_TOKEN").strip()
        or _read_setting("TELEGRAM_TOKEN").strip()
        or str(st.session_state.get(_CC_SESSION_TOKEN_KEY, "")).strip()
    )
    env_chat_raw = _read_telegram_group_chat_raw()
    chat_raw = env_chat_raw or str(st.session_state.get(_CC_SESSION_GROUP_KEY, "")).strip()
    chat_id: int | str | None = None
    warn: str | None = None
    if chat_raw:
        chat_id, warn = _parse_telegram_group_chat_id(chat_raw)
    return token or None, chat_id, warn


def _reset_sc_cc_sales_ticket_form() -> None:
    st.session_state[_SC_CC_ST_REF_KEY] = ""
    st.session_state[_SC_CC_ST_ACCOUNT_KEY] = ""
    st.session_state[_SC_CC_ST_DESC_KEY] = ""
    st.session_state[_SC_CC_SKIP_ASSIGN_KEY] = False
    for k in (
        _SC_CC_FE_SELECT_KEY,
        _SC_CC_FE_SELECT_2_KEY,
        _SC_CC_FE_MANUAL_KEY,
        _SC_CC_FE_MANUAL_2_KEY,
        _SC_CC_ST_SCAT_KEY,
    ):
        st.session_state.pop(k, None)
    if _SC_CC_ST_PRIORITY_KEY in st.session_state:
        st.session_state[_SC_CC_ST_PRIORITY_KEY] = SALES_PRIORITY_OPTIONS[-1]


def _sc_insert_intake_case(
    *,
    case_ref: str,
    account_name: str,
    attended_by: str,
    sales_priority: str,
    account_region: str,
    sales_category: str,
    description: str | None,
    status: str,
    queue_metric_label: str,
    clear_sales_ticket_form: bool = False,
    assigned_to: str | None = None,
    assigned_to_2: str | None = None,
    field_task_category: str | None = None,
    post_telegram: bool = False,
    operator_id: str = "",
) -> None:
    row: dict[str, object] = {
        "case_ref": case_ref,
        "account_name": account_name,
        "attended_by": attended_by,
        "sales_priority": sales_priority,
        "account_region": account_region,
        "sales_category": sales_category,
        "description": description,
        "status": status,
        "admin_owner": operator_id or attended_by,
    }
    if description:
        row["additional_info"] = description
    if assigned_to:
        row["assigned_to"] = assigned_to
        if assigned_to_2:
            row["assigned_to_2"] = assigned_to_2
        row["field_task_category"] = field_task_category
        row["dispatch_region"] = account_region
        row["last_assigned_at"] = _cc_utc_now_iso()
    try:
        _sales_cases_insert_row(row)
    except Exception as exc:
        _sc_set_sales_flash(f"Could not create case: {exc}", level="error")
        st.rerun()
        return

    flash = f"Case created — see **{status}** under **Sales Cases**."
    level = "success"

    if post_telegram and assigned_to and field_task_category:
        token, chat_id, chat_warn = _cc_resolve_telegram_credentials_for_form()
        if chat_warn:
            flash += f" Telegram skipped: {chat_warn}"
            level = "warning"
        elif not token or chat_id is None:
            flash += " Engineer saved on case; Telegram skipped (missing bot token or group id)."
            level = "warning"
        else:
            try:
                asyncio.run(
                    notify_telegram_group(
                        assigned_to.lstrip("@"),
                        case_ref,
                        field_task_category,
                        additional_info=description,
                        assigned_by=operator_id or attended_by,
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
                flash += " Telegram assignment posted."
            except Exception as exc:
                flash += f" Engineer saved; Telegram post failed: {exc}"
                level = "warning"

    _invalidate_dashboard_data_cache()
    _sc_set_sales_flash(flash, level=level)
    st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Sales Cases"
    st.session_state[_DASH_PENDING_SALES_QUEUE_KEY] = _queue_segment_label(
        queue_metric_label, 1
    )
    if clear_sales_ticket_form:
        st.session_state[_SC_CC_CLEAR_ST_INTAKE_KEY] = True
    st.rerun()


def _sidebar_sales_intake() -> None:
    """SALES intake — same assign / skip pattern as CSM sidebar."""
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

    op = _session_operator_id()
    if not op:
        st.caption("Sign in with an **Operator ID** to create sales cases.")
        return

    fe_names, fe_missing = _try_fetch_field_engineer_usernames()
    cat_names, cat_missing = _try_fetch_task_categories()
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

    if st.session_state.pop(_SC_CC_CLEAR_ST_INTAKE_KEY, False):
        _reset_sc_cc_sales_ticket_form()

    with st.container(border=True, key="sc_cc_intake_block"):
        st.markdown("##### SALES")
        st.caption("Field Assignment → Telegram Group.")
        skip_assign = st.checkbox(
            "Add to Daily Task Only (No Engineer, No Telegram)",
            key=_SC_CC_SKIP_ASSIGN_KEY,
            help=(
                "Creates the **Sales case** without posting to the field group. "
                "Assign an engineer later from the sales work panel."
            ),
        )
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
        intake_central = True
        if not skip_assign:
            intake_central = _sc_sync_region_engineer_widgets(
                region_key=_SC_CC_ST_REGION_KEY,
                region_watch_key="_sc_cc_intake_region_watch",
                engineer_select_key=_SC_CC_FE_SELECT_KEY,
                engineer_manual_key=_SC_CC_FE_MANUAL_KEY,
                engineer_select2_key=_SC_CC_FE_SELECT_2_KEY,
                engineer_manual2_key=_SC_CC_FE_MANUAL_2_KEY,
                fe_names=fe_names,
                fe_missing=fe_missing,
            )
            _render_sc_engineer_field(
                central=intake_central,
                fe_names=fe_names,
                fe_missing=fe_missing,
                select_key=_SC_CC_FE_SELECT_KEY,
                manual_key=_SC_CC_FE_MANUAL_KEY,
                blank_key="sc_cc_intake_engineer_blank",
                select2_key=_SC_CC_FE_SELECT_2_KEY,
                manual2_key=_SC_CC_FE_MANUAL_2_KEY,
            )
            with st.popover("Edit team", key="sc_cc_team_popover"):
                _field_team_manage_popover(fe_names, missing=fe_missing)
        else:
            st.caption(
                "No engineer yet — assign from the sales case work panel later."
            )
        _render_sales_category_row(cat_names, missing=cat_missing)
        st.text_area(
            "Notes (Optional)",
            key=_SC_CC_ST_DESC_KEY,
            height=64,
            placeholder="Context for the field team",
        )
        if not skip_assign and intake_central:
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
        submit_label = "Create Sales Case" if skip_assign else "Assign"
        submit_st = st.button(
            submit_label,
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

        sales_cat = str(st.session_state.get(_SC_CC_ST_SCAT_KEY, "")).strip()
        if not sales_cat:
            _sc_set_sales_flash("Pick **Sales Category (Intent)**.", level="error")
            st.rerun()
            return

        skip_assign = bool(st.session_state.get(_SC_CC_SKIP_ASSIGN_KEY))
        region = str(st.session_state.get(_SC_CC_ST_REGION_KEY, "")).strip()
        central = _sc_region_is_central(region)
        assigned_to: str | None = None
        assigned_to_2: str | None = None
        field_cat: str | None = None
        post_telegram = not skip_assign and central

        if not skip_assign and central:
            field_cat = sales_cat
            try:
                assigned_to, assigned_to_2 = _sc_resolve_sales_engineer_pair(
                    central=True,
                    fe_names=fe_names,
                    fe_missing=fe_missing,
                    select_key=_SC_CC_FE_SELECT_KEY,
                    manual_key=_SC_CC_FE_MANUAL_KEY,
                    select2_key=_SC_CC_FE_SELECT_2_KEY,
                    manual2_key=_SC_CC_FE_MANUAL_2_KEY,
                )
            except ValueError as exc:
                _sc_set_sales_flash(str(exc), level="error")
                st.rerun()
                return
        if post_telegram:
            token = token_env or str(
                st.session_state.get(_CC_SESSION_TOKEN_KEY, "")
            ).strip()
            chat_raw = (env_chat_raw if env_group_ok else "") or str(
                st.session_state.get(_CC_SESSION_GROUP_KEY, "")
            ).strip()
            chat_id: int | str | None = None
            chat_parse_err: str | None = None
            if chat_raw:
                chat_id, chat_parse_err = _parse_telegram_group_chat_id(chat_raw)
            if chat_parse_err:
                _sc_set_sales_flash(chat_parse_err, level="warning")
                st.rerun()
                return
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
                _sc_set_sales_flash(
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

        desc_raw = str(st.session_state.get(_SC_CC_ST_DESC_KEY, "")).strip() or None
        desc_note = _coassignee_telegram_note(assigned_to_2, desc_raw)
        _sc_insert_intake_case(
            case_ref=cr,
            account_name=an,
            attended_by=_sc_sales_attended_by_for_case(assigned_to=assigned_to),
            sales_priority=str(
                st.session_state.get(_SC_CC_ST_PRIORITY_KEY, "Standard")
            ).strip(),
            account_region=str(st.session_state.get(_SC_CC_ST_REGION_KEY, "")).strip(),
            sales_category=sales_cat,
            description=desc_note,
            status=SC_STATUS_SALES_TICKET,
            queue_metric_label=SC_STATUS_SALES_TICKET,
            clear_sales_ticket_form=True,
            assigned_to=assigned_to,
            assigned_to_2=assigned_to_2,
            field_task_category=field_cat,
            post_telegram=post_telegram,
            operator_id=op,
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
        _invalidate_dashboard_data_cache()
        _cc_set_flash(summary, level="success")
        _cc_schedule_assign_form_clear()
        st.rerun()
        return

    try:
        handle, handle2 = _resolve_engineer_pair(
            fe_names=fe_names,
            fe_missing=fe_missing,
            engineer_key=_CC_FE_SELECT_KEY if fe_names and not fe_missing else _CC_FE_MANUAL_KEY,
            engineer2_key=_CC_FE_SELECT_2_KEY if fe_names and not fe_missing else _CC_FE_MANUAL_2_KEY,
            required_primary=True,
        )
        additional_info_val = _coassignee_telegram_note(
            handle2,
            str(st.session_state.get(_CC_ASSIGN_NOTES_KEY, "")).strip() or None,
        )
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
            assigned_to_2=handle2,
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
    active = _render_cc_sidebar_nav()
    _cc_show_flash()
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


def _sidebar_controls() -> None:
    """Sidebar: Command Center assign only."""
    with st.sidebar:
        _sidebar_command_center()


def main() -> None:
    # Must be the first Streamlit command every run (login + dashboard).
    st.set_page_config(
        page_title="NetOps Coverage Eye",
        layout="wide",
        initial_sidebar_state="expanded",
    )

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
    _init_dash_date_range_state()
    _inject_bon_sidebar_visibility_css()
    _render_app_chrome()

    _sidebar_controls()

    auto, interval_minutes = _dash_refresh_settings()
    run_every = timedelta(minutes=interval_minutes) if auto else None

    @st.fragment(run_every=run_every)
    def _metrics_fragment() -> None:
        _render_global_assign_day_metrics()
        _inject_bon_dash_row_align()

    _metrics_fragment()
    _render_supabase_unreachable_banner()

    @st.fragment(run_every=run_every)
    def _dashboard_fragment() -> None:
        _sync_dash_range_from_ui(
            str(st.session_state.get(_DASH_TIME_PRESET_KEY, "This week"))
        )
        lookback_days, _, _ = _dash_date_range_lookback()
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
        --bon-topbar-h: 3.5rem;
        --bon-topbar-icon: 2.125rem;
        --bon-dash-top-pad: 0.5rem;
        --bon-metrics-nudge: 0px;
        --bon-sidebar-ticket-nudge: 0px;
        --bon-dash-band-top: calc(var(--bon-topbar-h) + var(--bon-dash-top-pad));
        --bon-chrome-h: var(--bon-topbar-h);
        --bon-netops: #FF5A1F;
        --bon-metric-label-size: 0.875rem;
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
        display: none !important;
        height: 0 !important;
        min-height: 0 !important;
    }
    [data-testid="stAppViewContainer"] {
        padding-top: 0 !important;
        margin-top: 0 !important;
        overflow-y: auto !important;
        overflow-x: hidden !important;
        height: 100vh !important;
        max-height: 100vh !important;
    }
    [data-testid="stAppViewContainer"] section.main {
        padding-top: 0 !important;
        overflow: visible !important;
        min-height: auto !important;
    }
    .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewContainer"] > section,
    [data-testid="stAppViewContainer"] section.main,
    [data-testid="stAppViewContainer"] .main,
    [data-testid="stAppViewContainer"] .main > div,
    [data-testid="stMain"],
    [data-testid="stMain"] > div,
    [data-testid="stMain"] [data-testid="block-container"] {
        transform: none !important;
        filter: none !important;
        perspective: none !important;
        contain: none !important;
    }
    [data-testid="stMain"] [data-testid="block-container"] {
        padding-top: var(--bon-dash-band-top) !important;
        padding-bottom: 1rem !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }
    [data-testid="stMain"] [data-testid="block-container"] > div:has(div.st-key-bon_app_menu),
    [data-testid="stMain"] [data-testid="block-container"] > [data-testid="stVerticalBlockBorderWrapper"]:has(div.st-key-bon_app_menu) {
        height: 0 !important;
        min-height: 0 !important;
        max-height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: visible !important;
        border: none !important;
        pointer-events: none !important;
    }
    div.st-key-bon_app_menu {
        pointer-events: auto !important;
    }
    [data-testid="stMain"] [data-testid="block-container"] > div > [data-testid="stVerticalBlock"],
    [data-testid="stMain"] [data-testid="block-container"] [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="block-container"],
    [data-testid="stSidebar"] [data-testid="stSidebarUserContent"],
    [data-testid="stSidebar"] > div:first-child {
        padding-top: var(--bon-dash-top-pad) !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        padding-bottom: 0.5rem !important;
    }
    [data-testid="stSidebar"] [data-testid="block-container"] > div > [data-testid="stVerticalBlock"],
    [data-testid="stSidebar"] [data-testid="block-container"] [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    div.st-key-bon_dash_metrics_row {
        position: sticky !important;
        top: var(--bon-topbar-h) !important;
        z-index: 999 !important;
        background: var(--bon-bg) !important;
        margin-top: var(--bon-metrics-nudge, 0px) !important;
        margin-bottom: 0 !important;
        padding: 0 0 0.15rem 0 !important;
        box-sizing: border-box !important;
    }
    div.st-key-bon_dash_metrics_row,
    div.st-key-bon_dash_metrics_row [data-testid="element-container"],
    [data-testid="stMain"] [data-testid="element-container"]:has(div.st-key-bon_dash_metrics_row) {
        margin-left: 0 !important;
        margin-right: 0 !important;
        padding-top: 0 !important;
        padding-left: 0 !important;
        padding-right: 0 !important;
    }
    div.st-key-bon_dash_metrics_row [data-testid="stHorizontalBlock"] {
        align-items: stretch !important;
        gap: 0.65rem !important;
        margin: 0 !important;
    }
    div.st-key-bon_dash_metrics_row + div [data-testid="stCaptionContainer"],
    div.st-key-bon_dash_metrics_row + [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stCaptionContainer"] {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-bon_box_ticket"] {
        position: sticky !important;
        top: 0 !important;
        z-index: 50 !important;
        background: var(--bon-bg) !important;
        margin-top: var(--bon-sidebar-ticket-nudge, 0px) !important;
        padding-top: 0 !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-bon_box_ticket"] [data-testid="stExpander"],
    [data-testid="stSidebar"] div[class*="st-key-bon_box_ticket"] details {
        margin: 0 !important;
    }
    div.st-key-bon_dash_metrics_row [data-testid="stMetric"],
    [data-testid="stSidebar"] div[class*="st-key-bon_box_ticket"] summary {
        min-height: 4.35rem !important;
        box-sizing: border-box !important;
        background: var(--bon-card) !important;
        border: 1px solid var(--bon-box-border) !important;
        border-radius: var(--bon-box-radius) !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-bon_box_ticket"] summary {
        display: flex !important;
        align-items: center !important;
        padding: 0.5rem 0.75rem !important;
    }
    div.st-key-bon_app_header_shell {
        position: fixed !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        width: 100% !important;
        max-width: 100vw !important;
        z-index: 1001 !important;
        pointer-events: auto !important;
        background: var(--bon-bg) !important;
        border-bottom: 1px solid rgba(215, 180, 145, 0.18) !important;
        padding: 0.45rem 1.15rem !important;
        margin: 0 !important;
        height: var(--bon-topbar-h) !important;
        min-height: var(--bon-topbar-h) !important;
        max-height: var(--bon-topbar-h) !important;
        box-sizing: border-box !important;
        overflow: visible !important;
        transform: none !important;
    }
    div.st-key-bon_app_topbar {
        position: static !important;
        width: 100% !important;
        height: 100% !important;
        min-height: 0 !important;
        max-height: none !important;
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        box-shadow: none !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_header_shell [data-testid="stSelectbox"],
    div.st-key-bon_app_header_shell .stPopover,
    div.st-key-bon_app_header_shell .stPopover > button,
    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn button {
        pointer-events: auto !important;
    }
    div.st-key-bon_app_header_shell > div[data-testid="stVerticalBlock"] {
        margin: 0 !important;
        padding: 0 !important;
        height: 100% !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] {
        margin: 0 !important;
        padding: 0 !important;
        height: 100% !important;
        justify-content: center !important;
    }
    [data-testid="stMain"] [data-testid="element-container"]:has(div.st-key-bon_app_header_shell),
    [data-testid="stMain"] [data-testid="block-container"] > div:has(div.st-key-bon_app_header_shell),
    [data-testid="stMain"] [data-testid="block-container"] > [data-testid="stVerticalBlockBorderWrapper"]:has(div.st-key-bon_app_header_shell),
    [data-testid="stMain"] [data-testid="element-container"]:has(div.st-key-bon_app_topbar),
    [data-testid="stMain"] [data-testid="block-container"] > div:has(div.st-key-bon_app_topbar),
    [data-testid="stMain"] [data-testid="block-container"] > [data-testid="stVerticalBlockBorderWrapper"]:has(div.st-key-bon_app_topbar) {
        height: 0 !important;
        min-height: 0 !important;
        max-height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: visible !important;
        border: none !important;
        pointer-events: none !important;
    }
    div.st-key-bon_app_header_shell > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"],
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] {
        align-items: center !important;
        height: 100% !important;
        gap: 0.75rem !important;
        width: 100% !important;
        flex-wrap: nowrap !important;
        justify-content: space-between !important;
    }
    div.st-key-bon_app_header_shell [data-testid="column"],
    div.st-key-bon_app_topbar [data-testid="column"] {
        display: flex !important;
        flex-direction: column !important;
        justify-content: center !important;
        min-height: 0 !important;
        padding: 0 !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(1) {
        flex: 0 0 auto !important;
        width: auto !important;
        min-width: 0 !important;
        max-width: none !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) {
        flex: 1 1 auto !important;
        width: auto !important;
        min-width: 0 !important;
        max-width: none !important;
        align-items: stretch !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] {
        width: 100% !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] {
        display: flex !important;
        flex-flow: row nowrap !important;
        align-items: center !important;
        justify-content: flex-start !important;
        gap: 0.45rem !important;
        width: 100% !important;
        min-height: var(--bon-topbar-icon) !important;
        height: var(--bon-topbar-icon) !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(1) {
        flex: 1 1 auto !important;
        width: auto !important;
        min-width: 0 !important;
        max-width: none !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) {
        flex: 0 0 auto !important;
        width: auto !important;
        min-width: max-content !important;
        max-width: none !important;
        margin-left: auto !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] {
        display: flex !important;
        flex-flow: row nowrap !important;
        align-items: center !important;
        justify-content: flex-end !important;
        gap: 0.25rem !important;
        width: auto !important;
        min-height: var(--bon-topbar-icon) !important;
        height: var(--bon-topbar-icon) !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"] {
        flex: 0 0 auto !important;
        width: auto !important;
        min-width: 0 !important;
        max-width: none !important;
        flex-shrink: 0 !important;
        padding: 0 !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"] [data-testid="stVerticalBlock"] {
        justify-content: center !important;
        min-height: var(--bon-topbar-icon) !important;
        width: auto !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(1) {
        flex: 0 0 auto !important;
        width: auto !important;
        min-width: 0 !important;
        max-width: none !important;
        margin-right: 0.45rem !important;
    }
    div.st-key-bon_app_topbar > div[data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) [data-testid="column"] [data-testid="stVerticalBlock"] {
        justify-content: center !important;
        min-height: var(--bon-topbar-icon) !important;
        overflow: visible !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_picker,
    div.st-key-bon_app_menu div.st-key-bon_dash_range_picker > [data-testid="stVerticalBlock"] {
        width: 100% !important;
        max-width: none !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_picker [data-testid="stExpander"] {
        border: none !important;
        background: transparent !important;
        margin: 0 0 0.12rem 0 !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_picker [data-testid="stExpander"] summary {
        min-height: 1.75rem !important;
        max-height: none !important;
        font-size: 0.76rem !important;
        font-weight: 600 !important;
        text-align: left !important;
        padding: 0.22rem 0.45rem !important;
        border: 1px solid rgba(215, 180, 145, 0.35) !important;
        border-radius: 5px !important;
        background: rgba(20, 20, 20, 0.85) !important;
        color: var(--bon-oak) !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_picker [data-testid="stExpander"] [data-testid="stExpanderDetails"] {
        padding: 0.35rem 0.15rem 0.15rem !important;
    }
    .bon-menu-range-cap {
        margin: 0 0 0.35rem 0 !important;
        padding: 0 0.15rem !important;
        font-size: 0.72rem !important;
        font-weight: 400 !important;
        color: var(--bon-muted) !important;
        line-height: 1.2 !important;
        white-space: nowrap !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu {
        min-width: 0 !important;
        padding: 0 !important;
    }
    div.st-key-bon_dash_range_menu > [data-testid="stVerticalBlock"] {
        gap: 0.35rem !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu button {
        min-height: 1.85rem !important;
        max-height: 1.85rem !important;
        font-size: 0.76rem !important;
        font-weight: 500 !important;
        text-align: center !important;
        padding: 0.2rem 0.65rem !important;
        border: 1px solid rgba(215, 180, 145, 0.32) !important;
        border-radius: 999px !important;
        background: rgba(20, 20, 20, 0.92) !important;
        color: var(--bon-text) !important;
        box-shadow: none !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu button:hover {
        border-color: rgba(215, 180, 145, 0.55) !important;
        color: var(--bon-oak) !important;
        background: rgba(215, 180, 145, 0.08) !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu div.st-key-bon_dash_range_custom_btn button {
        border-radius: 6px !important;
        text-align: left !important;
        padding-left: 0.55rem !important;
        margin-top: 0.1rem !important;
        background: transparent !important;
    }
    .bon-dash-range-divider {
        height: 1px;
        margin: 0.35rem 0 0.2rem;
        background: rgba(215, 180, 145, 0.28);
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu .stDateInput {
        margin-top: 0.15rem !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu .stDateInput label {
        font-size: 0.72rem !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_dash_range_menu .stDateInput input {
        min-height: 1.85rem !important;
        font-size: 0.74rem !important;
    }
    div.st-key-bon_app_topbar .bon-app-topbar-userchip {
        margin-left: 0 !important;
        gap: 0.45rem !important;
        padding-right: 0 !important;
    }
    div.st-key-bon_app_topbar div.st-key-bon_menu_toggle_btn {
        margin-left: 0 !important;
        padding-left: 0 !important;
    }
    div.st-key-bon_app_header_shell .stSelectbox > div > div {
        min-height: 1.85rem !important;
        max-height: 1.85rem !important;
        font-size: 0.76rem !important;
    }
    div.st-key-bon_app_header_shell .stSelectbox label {
        display: none !important;
    }
    div.st-key-bon_app_topbar .bon-app-topbar-range-cap {
        display: inline-block;
        white-space: nowrap !important;
        margin: 0 !important;
        padding: 0 !important;
        line-height: 1.15 !important;
        font-size: var(--bon-metric-label-size) !important;
        font-weight: 400 !important;
        color: var(--bon-muted) !important;
    }
    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn {
        min-width: var(--bon-topbar-icon) !important;
        max-width: var(--bon-topbar-icon) !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn button {
        min-height: var(--bon-topbar-icon);
        max-height: var(--bon-topbar-icon);
        width: var(--bon-topbar-icon);
        min-width: var(--bon-topbar-icon);
        font-size: 0;
        line-height: 0;
        padding: 0;
        border: 1px solid rgba(163, 158, 151, 0.45);
        border-radius: 6px;
        background: #141414;
        box-sizing: border-box;
        color: transparent;
        box-shadow: none;
        position: relative;
    }
    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn button::before {
        content: "";
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        width: 0.8rem;
        height: 2px;
        border-radius: 1px;
        background: #a39e97;
        box-shadow: 0 -5px 0 #a39e97, 0 5px 0 #a39e97;
    }
    div.st-key-bon_app_header_shell div.st-key-bon_menu_toggle_btn button:hover {
        border-color: rgba(215, 180, 145, 0.55);
        background: #1a1a1a;
    }
    div.st-key-bon_app_header_shell [data-testid="stMarkdownContainer"] {
        display: flex !important;
        align-items: center !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    div.st-key-bon_app_menu {
        position: fixed !important;
        top: var(--bon-topbar-h) !important;
        right: 1rem !important;
        left: auto !important;
        width: min(14rem, calc(100vw - 2.5rem)) !important;
        z-index: 1005 !important;
        pointer-events: auto !important;
        background: linear-gradient(165deg, #1a1612 0%, #121010 100%) !important;
        border: 1px solid rgba(215, 180, 145, 0.38) !important;
        border-radius: 8px !important;
        padding: 0.28rem !important;
        margin: 0.2rem 0 0 0 !important;
        box-sizing: border-box !important;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.5) !important;
        overflow: visible !important;
        max-height: calc(100vh - var(--bon-topbar-h) - 1rem);
        overflow-y: auto !important;
    }
    [data-testid="stMain"] [data-testid="block-container"] > div:has(div.st-key-bon_app_menu),
    [data-testid="stMain"] [data-testid="block-container"] > [data-testid="stVerticalBlockBorderWrapper"]:has(div.st-key-bon_app_menu) {
        height: 0 !important;
        min-height: 0 !important;
        overflow: visible !important;
        margin: 0 !important;
        padding: 0 !important;
        border: none !important;
        pointer-events: none !important;
    }
    div.st-key-bon_app_menu > div[data-testid="stVerticalBlock"] {
        gap: 0.18rem !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_menu_assign_btn button,
    div.st-key-bon_app_menu [data-testid="stExpander"] summary {
        min-height: 1.75rem !important;
        max-height: 1.75rem !important;
        font-size: 0.76rem !important;
        font-weight: 500 !important;
        text-align: left !important;
        padding: 0.22rem 0.45rem !important;
        border: 1px solid rgba(215, 180, 145, 0.28) !important;
        border-radius: 5px !important;
        background: rgba(20, 20, 20, 0.85) !important;
        color: var(--bon-text) !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_menu_assign_btn button {
        background: linear-gradient(145deg, rgba(42, 36, 28, 0.9), rgba(20, 20, 20, 0.95)) !important;
        border-color: rgba(215, 180, 145, 0.42) !important;
        color: var(--bon-oak) !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_menu_assign_btn button:hover {
        border-color: var(--bon-oak) !important;
        background: rgba(215, 180, 145, 0.12) !important;
    }
    div.st-key-bon_app_menu [data-testid="stExpander"] {
        border: none !important;
        background: transparent !important;
    }
    div.st-key-bon_app_menu [data-testid="stExpander"] summary:hover {
        border-color: var(--bon-oak) !important;
        color: var(--bon-oak) !important;
    }
    div.st-key-bon_app_menu [data-testid="stExpander"] details {
        font-size: 0.76rem !important;
    }
    div.st-key-bon_app_menu [data-testid="stExpander"] [data-testid="stExpanderDetails"] {
        padding: 0.35rem 0.15rem 0.25rem !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_menu_logout_btn button {
        min-height: 1.75rem !important;
        max-height: 1.75rem !important;
        font-size: 0.74rem !important;
        padding: 0.22rem 0.45rem !important;
        border: 1px solid rgba(215, 180, 145, 0.32) !important;
        border-radius: 5px !important;
        background: transparent !important;
        color: var(--bon-muted) !important;
    }
    div.st-key-bon_app_menu div.st-key-bon_menu_logout_btn button:hover {
        border-color: rgba(215, 180, 145, 0.55) !important;
        color: var(--bon-text) !important;
    }
    .bon-app-topbar-range-cap {
        display: inline-block;
        font-size: var(--bon-metric-label-size);
        font-weight: 400;
        color: var(--bon-muted);
        line-height: 1.15;
        white-space: nowrap;
        flex-shrink: 0;
        font-family: var(--bon-font);
    }
    .bon-app-topbar-userchip {
        display: inline-flex;
        align-items: center;
        justify-content: flex-start;
        gap: 0.45rem;
        min-width: max-content;
        flex-shrink: 0;
        margin-left: 0;
        padding-right: 0;
        box-sizing: border-box;
        height: var(--bon-topbar-icon);
        min-height: var(--bon-topbar-icon);
        white-space: nowrap;
    }
    .bon-app-topbar-title {
        display: flex;
        align-items: baseline;
        gap: 0.4rem;
        flex-shrink: 0;
        min-width: 0;
    }
    .bon-app-topbar-netops {
        font-size: 0.95rem;
        font-weight: 700;
        color: var(--bon-netops);
        -webkit-text-fill-color: var(--bon-netops);
        letter-spacing: -0.02em;
        line-height: 1;
        white-space: nowrap;
    }
    .bon-app-topbar-sep {
        color: var(--bon-muted);
        font-size: 0.88rem;
        line-height: 1;
    }
    .bon-app-topbar-sub {
        font-size: 0.88rem;
        font-weight: 400;
        color: #ffffff;
        -webkit-text-fill-color: #ffffff;
        line-height: 1;
        white-space: nowrap;
    }
    .bon-app-topbar-mark {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: var(--bon-topbar-icon);
        height: var(--bon-topbar-icon);
        border-radius: 6px;
        background: #141414;
        border: 1px solid rgba(163, 158, 151, 0.45);
        box-sizing: border-box;
        flex-shrink: 0;
    }
    .bon-app-topbar-mark-bar {
        display: block;
        width: 2px;
        height: 0.72rem;
        background: var(--bon-oak);
        border-radius: 1px;
    }
    .bon-app-topbar-username {
        font-size: 0.92rem;
        font-weight: 700;
        color: #ffffff;
        -webkit-text-fill-color: #ffffff;
        letter-spacing: -0.01em;
        line-height: 1;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 10rem;
    }
    [data-testid="stSidebar"] {
        top: var(--bon-topbar-h) !important;
        height: calc(100vh - var(--bon-topbar-h)) !important;
        max-height: calc(100vh - var(--bon-topbar-h)) !important;
        z-index: 998 !important;
        overflow-y: auto !important;
        overflow-x: hidden !important;
    }
    [data-testid="stSidebarCollapsedControl"],
    [data-testid="collapsedControl"],
    [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] {
        display: none !important;
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
    [data-testid="stSidebar"] .stTextInput input,
    [data-testid="stMain"] .stTextInput input {
        background-color: var(--bon-card) !important;
        color: var(--bon-text) !important;
        -webkit-text-fill-color: var(--bon-text) !important;
        caret-color: var(--bon-text) !important;
    }
    [data-testid="stSidebar"] .stTextInput input:-webkit-autofill,
    [data-testid="stSidebar"] .stTextInput input:-webkit-autofill:hover,
    [data-testid="stSidebar"] .stTextInput input:-webkit-autofill:focus,
    [data-testid="stSidebar"] .stTextInput input:-webkit-autofill:active,
    [data-testid="stMain"] .stTextInput input:-webkit-autofill,
    [data-testid="stMain"] .stTextInput input:-webkit-autofill:hover,
    [data-testid="stMain"] .stTextInput input:-webkit-autofill:focus,
    [data-testid="stMain"] .stTextInput input:-webkit-autofill:active {
        -webkit-box-shadow: 0 0 0 1000px var(--bon-card) inset !important;
        box-shadow: 0 0 0 1000px var(--bon-card) inset !important;
        -webkit-text-fill-color: var(--bon-text) !important;
        caret-color: var(--bon-text) !important;
        transition: background-color 99999s ease-out 0s;
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
    /* Contextual queue toolbar (selection + primary actions) */
    div[class*="st-key-"][class*="_ctx_toolbar"] {
        margin: 0 0 0.5rem 0 !important;
        padding: 0.5rem 0.65rem !important;
        border: 1px solid rgba(215, 180, 145, 0.28) !important;
        border-radius: 8px !important;
        background: linear-gradient(
            180deg,
            rgba(215, 180, 145, 0.08) 0%,
            rgba(0, 0, 0, 0.15) 100%
        ) !important;
    }
    div[class*="st-key-"][class*="_ctx_toolbar"] .stMarkdown p {
        margin: 0 !important;
        font-size: 0.88rem !important;
    }
    div[class*="st-key-"][class*="_ctx_toolbar"] .stButton > button {
        font-size: 0.78rem !important;
        min-height: 2rem !important;
        padding: 0.25rem 0.65rem !important;
    }
    /* Queue Actions popover — clean readable menu */
    [data-testid="stPopoverBody"] {
        min-width: 13.25rem !important;
        max-width: 14.75rem !important;
        padding: 0.45rem 0.5rem !important;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] [data-testid="stVerticalBlock"],
    [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] {
        gap: 0.28rem !important;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] .stButton > button,
    [data-testid="stPopoverBody"] .stButton > button {
        font-size: 0.8125rem !important;
        min-height: 2rem !important;
        padding: 0.28rem 0.55rem !important;
        line-height: 1.25 !important;
        white-space: nowrap !important;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] [data-testid="stExpander"],
    [data-testid="stPopoverBody"] [data-testid="stExpander"] {
        margin: 0 !important;
        border: 1px solid rgba(215, 180, 145, 0.22) !important;
        border-radius: 6px !important;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] [data-testid="stExpander"] summary,
    [data-testid="stPopoverBody"] [data-testid="stExpander"] summary {
        min-height: 2rem !important;
        padding: 0.28rem 0.55rem !important;
        font-size: 0.8125rem !important;
        font-weight: 500 !important;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] [data-testid="stExpander"] details[open] > div,
    [data-testid="stPopoverBody"] [data-testid="stExpander"] details[open] > div {
        padding: 0.35rem 0.5rem 0.45rem !important;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] [data-testid="stCaptionContainer"],
    div[class*="st-key-"][class*="_queue_actions_pop"] .stMarkdown p,
    [data-testid="stPopoverBody"] [data-testid="stCaptionContainer"],
    [data-testid="stPopoverBody"] .stMarkdown p {
        font-size: 0.75rem !important;
        line-height: 1.3 !important;
        margin: 0 0 0.15rem 0 !important;
        opacity: 0.92;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] hr,
    [data-testid="stPopoverBody"] hr {
        margin: 0.35rem 0 !important;
        opacity: 0.35;
    }
    div[class*="st-key-"][class*="_queue_actions_pop"] [data-testid="stCheckbox"] label,
    [data-testid="stPopoverBody"] [data-testid="stCheckbox"] label {
        font-size: 0.75rem !important;
    }
    div[class*="st-key-"][class*="_ctx_toolbar"] [data-testid="stPopover"] > button,
    div[class*="st-key-"][class*="_sc_toolbar"] [data-testid="stPopover"] > button {
        min-height: 1.85rem !important;
        font-size: 0.8125rem !important;
        line-height: 1.2 !important;
        padding: 0.22rem 0.55rem !important;
    }
    /* Queue block: render table before toolbar in code; show toolbar on top */
    div[class*="st-key-"][class*="_queue_block"] {
        display: flex !important;
        flex-direction: column-reverse !important;
        gap: 0.65rem !important;
    }
    div[class*="st-key-"][class*="_ctx_remove"] .stButton > button {
        color: #e57373 !important;
        border-color: rgba(229, 115, 115, 0.45) !important;
    }
    div[class*="st-key-"][class*="_ctx_remove"] .stButton > button:hover {
        color: #ff8a80 !important;
        background: rgba(229, 115, 115, 0.12) !important;
    }
    /* Legacy ticket toolbar (unused) */
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
    [data-testid="stMetric"] [data-testid="stMetricLabel"],
    [data-testid="stMetric"] label,
    div.st-key-bon_dash_metrics_row [data-testid="stMetricLabel"] {
        font-size: var(--bon-metric-label-size) !important;
        font-weight: 400 !important;
        line-height: 1.25 !important;
    }
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
    /* Dashboard nav: one row (CSM Cases | Sales Cases | Log | Performance) */
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
    /* Performance — solo vs shared board (engineer rows + total ring) */
    .perf-ss-board {
        display: flex;
        align-items: center;
        gap: 2rem;
        margin: 0.35rem 0 0.75rem;
        font-family: var(--bon-font);
    }
    .perf-ss-list {
        flex: 1 1 auto;
        display: flex;
        flex-direction: column;
        gap: 0.45rem;
        min-width: 0;
    }
    .perf-ss-row {
        display: flex;
        align-items: center;
        gap: 0.65rem;
    }
    .perf-ss-row.is-selected .perf-ss-name {
        color: var(--bon-oak);
    }
    .perf-ss-name {
        flex: 0 0 9.5rem;
        font-size: 0.82rem;
        color: var(--bon-text);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .perf-ss-pill {
        flex: 1 1 auto;
        display: flex;
        max-width: 14rem;
        border: 1px solid rgba(215, 180, 145, 0.35);
        border-radius: 999px;
        overflow: hidden;
        background: var(--bon-card);
    }
    .perf-ss-pill.has-sales {
        max-width: 20rem;
    }
    .perf-ss-seg {
        flex: 1 1 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.35rem;
        padding: 0.28rem 0.55rem;
        font-size: 0.72rem;
        line-height: 1.1;
        text-transform: lowercase;
        letter-spacing: 0.02em;
    }
    .perf-ss-seg.solo {
        border-right: 1px solid rgba(215, 180, 145, 0.22);
        color: #9ec5e8;
    }
    .perf-ss-seg.shared {
        color: var(--bon-oak);
    }
    .perf-ss-pill.has-sales .perf-ss-seg.shared {
        border-right: 1px solid rgba(215, 180, 145, 0.22);
    }
    .perf-ss-seg.sales {
        color: #b8d4a8;
    }
    .perf-ss-seg .num {
        font-size: 0.88rem;
        font-weight: 600;
        color: var(--bon-text);
    }
    .perf-ss-total {
        flex: 0 0 auto;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 0 0.25rem;
    }
    .perf-ss-circle {
        width: 4.6rem;
        height: 4.6rem;
        border-radius: 50%;
        border: 2px solid rgba(215, 180, 145, 0.55);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.55rem;
        font-weight: 600;
        color: var(--bon-text);
        background: radial-gradient(circle at 30% 30%, #1a1a1a, var(--bon-bg));
    }
    .perf-ss-total-lbl {
        margin-top: 0.25rem;
        font-size: 0.68rem;
        color: var(--bon-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .perf-ss-total-sub {
        margin-top: 0.15rem;
        font-size: 0.62rem;
        color: var(--bon-oak);
        letter-spacing: 0.02em;
        text-align: center;
        line-height: 1.25;
    }
    .perf-ss-hint {
        font-size: 0.72rem;
        color: var(--bon-muted);
        margin: 0 0 0.5rem;
    }
    .perf-queue-strip {
        display: flex;
        flex-wrap: wrap;
        gap: 0.4rem 0.65rem;
        margin: 0.25rem 0 0.65rem;
        font-size: 0.78rem;
    }
    .perf-queue-chip {
        padding: 0.18rem 0.55rem;
        border-radius: 999px;
        border: 1px solid rgba(215, 180, 145, 0.28);
        color: var(--bon-muted);
        background: var(--bon-card);
    }
    .perf-queue-chip strong {
        color: var(--bon-text);
        font-weight: 600;
        margin-left: 0.2rem;
    }
</style>
"""


def _inject_bon_sidebar_visibility_css() -> None:
    """Show/hide Assign sidebar via CSS — avoids Streamlit collapse localStorage."""
    if _bon_sidebar_is_open():
        st.markdown(
            """
            <style>
            [data-testid="stSidebar"] {
                display: block !important;
                visibility: visible !important;
                transform: none !important;
                opacity: 1 !important;
                pointer-events: auto !important;
                min-width: 19rem !important;
                width: 21rem !important;
                max-width: 22rem !important;
                overflow-x: hidden !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        components.html(
            """
            <script>
            (function () {
              const doc = window.parent.document;
              const sb = doc.querySelector('[data-testid="stSidebar"]');
              if (sb) {
                sb.setAttribute("aria-expanded", "true");
                sb.style.removeProperty("display");
                sb.style.removeProperty("visibility");
              }
            })();
            </script>
            """,
            height=0,
        )
        return
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] {
            display: none !important;
            visibility: hidden !important;
            width: 0 !important;
            min-width: 0 !important;
            max-width: 0 !important;
            overflow: hidden !important;
            opacity: 0 !important;
            pointer-events: none !important;
            border: none !important;
        }
        [data-testid="stAppViewContainer"] > section.main,
        section.main {
            margin-left: 0 !important;
            width: 100% !important;
            max-width: 100vw !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _inject_bon_header_pin() -> None:
    """Keep the full dashboard header fixed while the main pane scrolls."""
    components.html(
        """
        <script>
        (function () {
          const win = window.parent;
          const doc = win.document;
          let pinTimer = null;

          function pinBonHeader() {
            const app = doc.querySelector(".stApp");
            const header = doc.querySelector("div.st-key-bon_app_header_shell");
            if (!app || !header) return;

            if (header.parentElement !== app) {
              app.insertBefore(header, app.firstChild);
            }

            const height = header.getBoundingClientRect().height;
            if (height > 0) {
              doc.documentElement.style.setProperty("--bon-topbar-h", height + "px");
            }

            header.style.setProperty("position", "fixed", "important");
            header.style.setProperty("top", "0px", "important");
            header.style.setProperty("left", "0px", "important");
            header.style.setProperty("right", "0px", "important");
            header.style.setProperty("width", "100%", "important");
            header.style.setProperty("max-width", "100vw", "important");
            header.style.setProperty("z-index", "1001", "important");
            header.style.setProperty("box-sizing", "border-box", "important");
          }

          function schedulePin() {
            if (pinTimer) win.clearTimeout(pinTimer);
            pinTimer = win.setTimeout(function () {
              win.requestAnimationFrame(pinBonHeader);
            }, 16);
          }

          pinBonHeader();
          win.requestAnimationFrame(pinBonHeader);
          win.setTimeout(pinBonHeader, 80);
          win.setTimeout(pinBonHeader, 350);
          win.addEventListener("resize", schedulePin);
          doc
            .querySelector('[data-testid="stAppViewContainer"]')
            ?.addEventListener("scroll", schedulePin, { passive: true });

          if (!win.__bonHeaderPinObs) {
            win.__bonHeaderPinObs = new MutationObserver(schedulePin);
            win.__bonHeaderPinObs.observe(doc.body, {
              childList: true,
              subtree: true,
              attributes: true,
            });
          } else {
            schedulePin();
          }
        })();
        </script>
        """,
        height=0,
    )


def _inject_bon_dash_row_align() -> None:
    """Align TICKET box (sidebar) with metrics row (main) when scrolled to top."""
    components.html(
        """
        <script>
        (function () {
          const doc = window.parent.document;
          const root = doc.documentElement;
          let alignTimer = null;
          function scrollRoot(el) {
            if (!el) return null;
            const oy = getComputedStyle(el).overflowY;
            if (oy === "auto" || oy === "scroll") return el;
            return el.querySelector('[data-testid="block-container"]') || el;
          }
          function atScrollTop() {
            const main = doc.querySelector('[data-testid="stAppViewContainer"]');
            const sidebar = doc.querySelector('[data-testid="stSidebar"]');
            const mainTop = main ? main.scrollTop < 2 : true;
            const sbRoot = scrollRoot(sidebar);
            const sbTop = sbRoot ? sbRoot.scrollTop < 2 : true;
            return mainTop && sbTop;
          }
          function alignBonDashRow() {
            root.style.setProperty("--bon-metrics-nudge", "0px");
            root.style.setProperty("--bon-sidebar-ticket-nudge", "0px");
            if (!atScrollTop()) return;
            const ticket =
              doc.querySelector('[data-testid="stSidebar"] [class*="st-key-bon_box_ticket"] summary') ||
              doc.querySelector('[data-testid="stSidebar"] [class*="st-key-bon_box_ticket"]');
            const metrics =
              doc.querySelector('[data-testid="stMain"] div[class*="st-key-bon_dash_metrics_row"]');
            if (!ticket || !metrics) return;
            const delta =
              ticket.getBoundingClientRect().top - metrics.getBoundingClientRect().top;
            if (Math.abs(delta) < 0.5) return;
            if (delta > 0) {
              root.style.setProperty("--bon-metrics-nudge", delta + "px");
            } else {
              root.style.setProperty("--bon-sidebar-ticket-nudge", (-delta) + "px");
            }
          }
          function scheduleAlign() {
            if (alignTimer) window.clearTimeout(alignTimer);
            alignTimer = window.setTimeout(alignBonDashRow, 16);
          }
          alignBonDashRow();
          requestAnimationFrame(alignBonDashRow);
          window.setTimeout(alignBonDashRow, 80);
          window.setTimeout(alignBonDashRow, 350);
          window.parent.addEventListener("resize", scheduleAlign);
          const main = doc.querySelector('[data-testid="stAppViewContainer"]');
          const sidebar = doc.querySelector('[data-testid="stSidebar"]');
          main?.addEventListener("scroll", scheduleAlign, { passive: true });
          sidebar?.addEventListener("scroll", scheduleAlign, { passive: true });
          scrollRoot(sidebar)?.addEventListener("scroll", scheduleAlign, { passive: true });
          if (!window.parent.__bonDashRowAlignObs) {
            window.parent.__bonDashRowAlignObs = new MutationObserver(scheduleAlign);
            window.parent.__bonDashRowAlignObs.observe(doc.body, {
              childList: true,
              subtree: true,
              attributes: true,
            });
          }
        })();
        </script>
        """,
        height=0,
    )


def _inject_bon_theme() -> None:
    st.markdown(_BON_THEME_CSS, unsafe_allow_html=True)
    _inject_bon_sidebar_visibility_css()
    _inject_bon_dash_row_align()


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
        if "task_category" in df.columns:
            cfg["task_category"] = st.column_config.TextColumn(
                "Assigned category",
                help="Dispatch intent when the ticket was assigned.",
            )
        if "outcome_category" in df.columns:
            cfg["outcome_category"] = st.column_config.TextColumn(
                "Outcome category",
                help="Actual work performed when resolved (used in Performance reports).",
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
    marked = df_all.loc[_ticket_marked_unattended_mask(df_all)].copy()
    if not marked.empty:
        active = pd.concat([active, marked], ignore_index=True)
        if "ticket_number" in active.columns:
            active = active.drop_duplicates(subset=["ticket_number"], keep="first")
        else:
            active = active.drop_duplicates()
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


def _dashboard_sales_cases_in_view(
    df_all: pd.DataFrame,
    *,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    """Sales cases in the sidebar range plus active queue rows (never hide open work)."""
    if df_all.empty:
        return df_all, 0
    in_range = _apply_dash_time_range(
        df_all, range_start=range_start, range_end=range_end
    )
    if "status" not in df_all.columns:
        return in_range, len(in_range)
    effective = df_all["status"].astype(str).str.strip().map(_sc_effective_status)
    active = df_all[effective.isin(_SC_ACTIVE_QUEUE_STATUSES)].copy()
    if in_range.empty:
        return active, 0
    if active.empty:
        return in_range, len(in_range)
    key = "case_ref" if "case_ref" in df_all.columns else None
    if key:
        merged = pd.concat([in_range, active], ignore_index=True).drop_duplicates(
            subset=[key], keep="first"
        )
    else:
        merged = pd.concat([in_range, active], ignore_index=True).drop_duplicates()
    return merged, len(in_range)


def _ticket_marked_unattended_mask(df: pd.DataFrame) -> pd.Series:
    """Permanent unattended flag — set once on auto-unattended; never cleared."""
    if df.empty:
        return pd.Series(dtype=bool)
    if "marked_unattended_at" in df.columns:
        return _parse_ts(df["marked_unattended_at"]).notna()
    if "status" in df.columns:
        return _normalized_status_series(df).eq(STATUS_UNATTENDED)
    return pd.Series(False, index=df.index)


def _ticket_queue_count_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Boolean masks per queue tab. Unattended = permanent ``marked_unattended_at`` tag."""
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
    unattended = _ticket_marked_unattended_mask(df)
    completed = status.eq(STATUS_RESOLVED)
    known = pending | on_hold | open_m | investigation | completed | unattended
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
        st.session_state[_DASH_MAIN_NAV_KEY] = _DASH_NAV_CSM
        st.session_state[_DASH_TICKET_QUEUE_KEY] = legacy


def _render_dashboard_header(*, refreshed_at: str) -> None:
    """Last refresh line below the metric cards."""
    st.caption(
        f"Updated **{refreshed_at} {LOCAL_TZ_LABEL}** · change dates in the header **Time Range**"
    )


def _apply_pending_dashboard_nav() -> None:
    """Apply metric-click navigation before nav widgets are drawn."""
    pending_main = st.session_state.pop(_DASH_PENDING_MAIN_NAV_KEY, None)
    pending_ticket_queue = st.session_state.pop(_DASH_PENDING_TICKET_QUEUE_KEY, None)
    pending_sales_queue = st.session_state.pop(_DASH_PENDING_SALES_QUEUE_KEY, None)
    if pending_main is not None:
        st.session_state[_DASH_MAIN_NAV_KEY] = _normalize_dash_main_nav(pending_main)
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
    main_nav = _normalize_dash_main_nav(st.session_state.get(_DASH_MAIN_NAV_KEY, _DASH_NAV_CSM))
    q_base = _queue_segment_base(st.session_state.get(_DASH_TICKET_QUEUE_KEY))
    active = main_nav == _DASH_NAV_CSM and q_base == queue_name
    label = f"{title}\n{value:,}"
    with col:
        if st.button(
            label,
            key=f"dash_metric_nav_{queue_name.lower().replace(' ', '_')}",
            type="secondary",
            use_container_width=True,
            disabled=active,
        ):
            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
            st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = option_label
            st.rerun()


def _render_assign_day_metrics(
    df_all: pd.DataFrame,
    *,
    df_in_view: pd.DataFrame | None = None,
    sc_all: pd.DataFrame | None = None,
    sc_in_view: pd.DataFrame | None = None,
) -> None:
    """Today's assignment funnel (ops timezone, UTC+5)."""
    today = datetime.now(OPS_TZ).date()
    start = _local_date_start(today)
    end = _local_date_end(today)
    assigned_today = 0
    responded_today = 0
    unattended_in_view = 0
    pending_in_view = 0
    if not df_all.empty:
        view = df_in_view if df_in_view is not None else df_all
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
    if sc_all is not None and not sc_all.empty:
        if "last_assigned_at" in sc_all.columns:
            sc_la = _parse_ts(sc_all["last_assigned_at"])
            sc_mask = (sc_la >= start) & (sc_la <= end)
            if "assigned_to" in sc_all.columns:
                sc_mask &= (
                    sc_all["assigned_to"].fillna("").astype(str).str.strip().ne("")
                )
            if "status" in sc_all.columns:
                effective = sc_all["status"].astype(str).str.strip().map(
                    _sc_effective_status
                )
                sc_mask &= effective.ne(SC_STATUS_RESOLVED)
            assigned_today += int(sc_mask.sum())
        if "created_at" in sc_all.columns:
            created = _parse_ts(sc_all["created_at"])
            attended_today = (created >= start) & (created <= end)
            if "attended_by" in sc_all.columns:
                ab = sc_all["attended_by"].fillna("").astype(str).str.strip()
                attended_today = attended_today & ab.ne("")
            responded_today += int(attended_today.sum())
        sc_view = sc_in_view if sc_in_view is not None else sc_all
        if not sc_view.empty and "status" in sc_view.columns:
            effective = sc_view["status"].astype(str).str.strip().map(_sc_effective_status)
            pending_in_view += int(effective.eq(SC_STATUS_SALES_TICKET).sum())
    if (
        assigned_today == 0
        and responded_today == 0
        and pending_in_view == 0
        and unattended_in_view == 0
    ):
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Assigned today", assigned_today)
    c2.metric("Responded today", responded_today)
    c3.metric(f"{STATUS_DAILY_TASK} (in view)", pending_in_view)
    c4.metric("Unattended (in view)", unattended_in_view)


def _render_global_assign_day_metrics() -> None:
    """Assign-day funnel under the page title — shown on every main nav tab."""
    try:
        df_all = _fetch_tickets()
    except _TableMissingError:
        df_all = pd.DataFrame()
    except Exception:
        return

    sc_all: pd.DataFrame | None
    try:
        sc_all = _fetch_sales_cases_df()
    except Exception:
        sc_all = None
    if sc_all is None:
        sc_all = pd.DataFrame()

    if df_all.empty and sc_all.empty:
        return

    range_start, range_end = _get_dash_range()
    df_in_view: pd.DataFrame | None = None
    if not df_all.empty and "status" in df_all.columns:
        df_in_view, _ = _dashboard_tickets_in_view(
            df_all, range_start=range_start, range_end=range_end
        )
    sc_in_view: pd.DataFrame | None = None
    if not sc_all.empty and "status" in sc_all.columns:
        sc_in_view, _ = _dashboard_sales_cases_in_view(
            sc_all, range_start=range_start, range_end=range_end
        )
    with st.container(key="bon_dash_metrics_row"):
        _render_assign_day_metrics(
            df_all,
            df_in_view=df_in_view,
            sc_all=sc_all if not sc_all.empty else None,
            sc_in_view=sc_in_view,
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
    """Keep queue session keys valid; return option labels for metrics.

    Do not assign ``_DASH_MAIN_NAV_KEY`` here — main nav radio is already drawn.
    Use ``_DASH_PENDING_*`` keys and ``st.rerun()`` when auto-switching tabs.
    """
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
    st.session_state["_dash_prev_open_count"] = total_open
    if total_open > prev_open:
        st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
        st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = open_label
        st.rerun()
    elif st.session_state.get(_DASH_TICKET_QUEUE_KEY) not in ticket_options:
        st.session_state[_DASH_TICKET_QUEUE_KEY] = (
            open_label if total_open > 0 else pending_label
        )
    return (
        pending_label,
        open_label,
        on_hold_label,
        investigation_label,
        completed_label,
        unattended_label,
    )


def _render_main_navigation() -> str:
    """Top row: CSM Cases | Sales Cases | Log | Performance."""
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


def _ticket_queue_view(
    df: pd.DataFrame,
    cols: tuple[str, ...] = _TICKET_QUEUE_TABLE_COLS,
    *,
    preserve_order: bool = False,
) -> pd.DataFrame:
    """Subset and format ticket columns for queue tables."""
    ordered = df if preserve_order else _sort_tickets_newest_first(df)
    show: list[str] = []
    seen: set[str] = set()
    for c in cols:
        if c in ordered.columns and c not in seen:
            seen.add(c)
            show.append(c)
    if not show:
        return _format_local(ordered)
    view = ordered[show].copy()
    for col in ("task_category", "outcome_category"):
        if col in view.columns:
            view[col] = view[col].map(canonical_task_category)
    return _format_local(view)


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


@st.cache_data(ttl=_DASH_DATA_CACHE_TTL_SEC, show_spinner=False)
def _fetch_sales_cases_cached() -> pd.DataFrame | None:
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


def _fetch_sales_cases_df() -> pd.DataFrame | None:
    return _fetch_sales_cases_cached()


def _sc_stamp_last_assigned_at(patch: dict[str, object]) -> None:
    """Set ``last_assigned_at`` when ``assigned_to`` is present (reassign / new assign)."""
    if str(patch.get("assigned_to") or "").strip():
        patch["last_assigned_at"] = _cc_utc_now_iso()


def _sc_stamp_last_assigned_at_if_first(
    patch: dict[str, object], *, prev_assigned_to: object
) -> None:
    """Set ``last_assigned_at`` on first engineer assignment (edit-assignment form)."""
    assignee = str(patch.get("assigned_to") or "").strip()
    if not assignee:
        return
    prev = str(prev_assigned_to or "").strip().lstrip("@").casefold()
    if not prev:
        patch["last_assigned_at"] = _cc_utc_now_iso()


def _sales_cases_update_row(row_id: str, payload: dict) -> None:
    client = _get_supabase_client()
    body = {**payload, "updated_at": _cc_utc_now_iso()}
    client.table(SALES_CASES_TABLE).update(body).eq("id", row_id).execute()


def _sales_cases_insert_row(payload: dict) -> None:
    client = _get_supabase_client()
    row = {**payload, "updated_at": _cc_utc_now_iso(), "created_at": _cc_utc_now_iso()}
    client.table(SALES_CASES_TABLE).insert(row).execute()


def _sales_cases_delete_row(row_id: str) -> None:
    client = _get_supabase_client()
    client.table(SALES_CASES_TABLE).delete().eq("id", row_id).execute()


def _sc_filter_sales_df(df: pd.DataFrame, statuses: tuple[str, ...]) -> pd.DataFrame:
    if df.empty or "status" not in df.columns:
        return df.iloc[0:0].copy()
    effective = df["status"].astype(str).str.strip().map(_sc_effective_status)
    return df[effective.isin(statuses)].copy()


def _sc_sales_case_display_cols() -> tuple[str, ...]:
    return (
        "case_ref",
        "account_name",
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


_SC_QUEUE_TABLE_COLS: tuple[str, ...] = (
    "case_ref",
    "account_name",
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
    sel_key = _sc_case_selection_session_key(key_prefix)
    synced = _selection_from_data_editor_state(
        _sc_case_select_editor_key(key_prefix),
        id_column_candidates=("Ticket Number", "case_ref"),
        options=options,
    )
    if synced is not None:
        st.session_state[sel_key] = synced
    allowed = set(options)
    raw = st.session_state.get(sel_key, [])
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
    editor_key = _sc_case_select_editor_key(key_prefix)
    st.session_state[_data_editor_snapshot_key(editor_key)] = table.copy()
    edited = st.data_editor(
        table,
        hide_index=True,
        use_container_width=True,
        key=editor_key,
        column_config=col_cfg,
        disabled=disabled_cols,
    )

    select_on = edited["Select"].fillna(False).astype(bool)
    selected = [
        str(t)
        for t in edited.loc[select_on, ticket_col].astype(str).tolist()
        if str(t) in options
    ]
    st.session_state[sel_key] = selected
    if selected:
        shown = ", ".join(selected[:6])
        extra = f" (+{len(selected) - 6} more)" if len(selected) > 6 else ""
        st.caption(f"**{len(selected)}** selected: {shown}{extra}")
    else:
        st.caption(
            "Tick **Select**, then use the action buttons below "
            "(or open the **work panel** for one case)."
        )
    return selected


def _sc_clear_work_panel_tabs(key_prefix: str) -> None:
    """Reset per-queue selection helpers when clearing the table."""
    st.session_state.pop("sc_action_synced_id", None)
    panel = _sc_toolbar_panel_keys(key_prefix)
    for k in panel.values():
        st.session_state.pop(k, None)


def _render_sales_case_action_popover(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    case_options: list[str],
    status_actions: tuple[tuple[str, str], ...],
    op: str,
) -> None:
    """Action picker: trigger shows choice; menu lists options; Apply commits."""
    status_labels = [_sc_toolbar_action_label(lbl, tgt) for lbl, tgt in status_actions]
    label_to_target = _sc_toolbar_label_to_target(status_actions)
    sel_key = f"{key_prefix}_sc_action_sel"
    if sel_key not in st.session_state:
        st.session_state[sel_key] = ""
    elif st.session_state[sel_key] not in status_labels:
        st.session_state[sel_key] = ""
    current = str(st.session_state.get(sel_key, "") or "")
    trigger = current if current in status_labels else "Action"

    with st.popover(trigger, use_container_width=True):
        st.caption("Choose action — click again to clear")
        for label in status_labels:
            picked = label == current
            if st.button(
                label,
                key=f"{key_prefix}_sc_pick_{label.replace(' ', '_')}",
                type="primary" if picked else "secondary",
                use_container_width=True,
            ):
                st.session_state[sel_key] = "" if picked else label
                st.rerun()
        st.divider()
        if st.button(
            "Apply",
            key=f"{key_prefix}_sc_apply",
            type="primary",
            use_container_width=True,
            disabled=current not in status_labels,
        ):
            choice = str(st.session_state.get(sel_key, ""))
            target = label_to_target.get(choice)
            if target:
                _sc_apply_status_to_selected_cases(
                    df,
                    key_prefix=key_prefix,
                    case_options=case_options,
                    target_status=target,
                    op=op,
                )


def _render_sales_case_delete_popover(
    *,
    key_prefix: str,
    options: list[str],
) -> None:
    with st.popover("Remove", use_container_width=True):
        picked_list = _get_selected_queue_sales_cases(key_prefix, options)
        if not picked_list:
            st.caption("Tick **Select** on case(s) in the table below, then open **Remove**.")
            return
        st.markdown("**" + "**, **".join(picked_list[:12]) + "**")
        if len(picked_list) > 12:
            st.caption(f"+ {len(picked_list) - 12} more")
        st.caption("Permanently deletes the sales case row.")
        confirm_del = st.checkbox(
            "Yes, Remove Permanently",
            value=False,
            key=f"{key_prefix}_sc_del_confirm",
        )
        if st.button(
            "Delete",
            key=f"{key_prefix}_sc_del_btn",
            type="secondary",
            use_container_width=True,
            disabled=not confirm_del,
        ):
            ok = 0
            for cref in picked_list:
                row = _fetch_sales_case_row_by_ref(cref)
                if not row:
                    st.warning(f"**{cref}** not found.")
                    continue
                row_id = str(row.get("id") or "").strip()
                if not row_id:
                    continue
                try:
                    _sales_cases_delete_row(row_id)
                    ok += 1
                except Exception as exc:
                    st.error(f"**{cref}**: {exc}")
            if ok:
                _invalidate_dashboard_data_cache()
                st.session_state[_sc_case_selection_session_key(key_prefix)] = []
                _sc_set_sales_flash(f"Removed **{ok}** sales case(s).")
                st.rerun()


def _fetch_sales_case_row_by_ref(case_ref: str) -> dict | None:
    cref = str(case_ref or "").strip()
    if not cref:
        return None
    try:
        res = (
            _get_supabase_client()
            .table(SALES_CASES_TABLE)
            .select("*")
            .eq("case_ref", cref)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        return rows[0] if rows else None
    except Exception:
        return None


def _fetch_sales_case_row_by_id(row_id: str) -> dict | None:
    rid = str(row_id or "").strip()
    if not rid:
        return None
    try:
        res = (
            _get_supabase_client()
            .table(SALES_CASES_TABLE)
            .select("*")
            .eq("id", rid)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        return rows[0] if rows else None
    except Exception:
        return None


def _cc_transfer_ticket_to_sales_case(
    ticket_number: str,
    *,
    operator_id: str,
    account_name: str | None = None,
    account_region: str = "CENTRAL",
) -> None:
    """Move a CSM field ticket into Sales Cases (removes the ticket row)."""
    tid = str(ticket_number or "").strip()
    row = _fetch_ticket_row(tid)
    if not row:
        raise ValueError(f"Ticket **{tid}** not found.")
    if _fetch_sales_case_row_by_ref(tid):
        raise ValueError(
            f"Sales case **{tid}** already exists — remove or merge the duplicate first."
        )

    task_cat = str(row.get("task_category") or "").strip() or "Coverage Check"
    notes = str(row.get("additional_info") or "").strip() or None
    assigned = str(row.get("assigned_to") or "").strip() or None
    attended = _sc_sales_attended_by_for_case(assigned_to=assigned)
    region = (
        account_region
        if account_region in SALES_REGION_CODES
        else SALES_REGION_CODES[0]
    )
    an = (account_name or "").strip() or f"Ticket {tid}"

    sales_row: dict[str, object] = {
        "case_ref": tid,
        "account_name": an,
        "attended_by": attended,
        "sales_priority": "Standard",
        "account_region": region,
        "sales_category": task_cat,
        "description": notes,
        "status": SC_STATUS_SALES_TICKET,
        "admin_owner": operator_id,
    }
    if notes:
        sales_row["additional_info"] = notes
    if assigned:
        sales_row["assigned_to"] = assigned
        sales_row["field_task_category"] = task_cat
        sales_row["dispatch_region"] = region
        la = row.get("last_assigned_at")
        sales_row["last_assigned_at"] = la if la else _cc_utc_now_iso()

    _sales_cases_insert_row(sales_row)

    client = _get_supabase_client()
    _cc_insert_attendance_log(
        client,
        ticket_number=tid,
        member_username=f"@{operator_id.lstrip('@')}",
        action_type="TransferredToSales",
        note=_cc_assignment_log_note(
            "Moved to Sales Cases (ticket row removed).",
            operator_id,
        ),
    )
    _delete_ticket(tid, actor=f"@{operator_id.lstrip('@')}")


def _cc_transfer_sales_case_to_ticket(
    row_id: str,
    *,
    operator_id: str,
) -> str:
    """Move a sales case into CSM tickets (removes the sales case row). Returns ticket number."""
    sales_row = _fetch_sales_case_row_by_id(row_id)
    if not sales_row:
        raise ValueError("Sales case not found.")

    case_ref = str(sales_row.get("case_ref") or "").strip()
    if not case_ref:
        raise ValueError("Sales case has no ticket / case reference.")
    try:
        tid = _cc_validate_ticket_number(case_ref)
    except ValueError as exc:
        raise ValueError(
            f"Case ref **{case_ref}** must be **9** or **16** digits to move to CSM."
        ) from exc

    if _fetch_ticket_row(tid):
        raise ValueError(
            f"CSM ticket **{tid}** already exists — resolve the duplicate first."
        )

    task_cat = (
        str(sales_row.get("field_task_category") or "").strip()
        or str(sales_row.get("sales_category") or "").strip()
        or "Coverage Check"
    )
    notes = (
        str(sales_row.get("additional_info") or "").strip()
        or str(sales_row.get("description") or "").strip()
        or None
    )
    assigned = str(sales_row.get("assigned_to") or "").strip() or None
    csm_status = _cc_map_sales_status_to_csm(str(sales_row.get("status") or ""))

    client = _get_supabase_client()
    _cc_insert_transferred_ticket(
        client,
        tid,
        task_category=task_cat,
        status=csm_status,
        assigned_to=assigned,
        additional_info=notes,
        operator_id=operator_id,
        last_assigned_at=sales_row.get("last_assigned_at"),
    )
    _sales_cases_delete_row(row_id)
    return tid


def _render_sales_case_transfer_to_csm_popover(
    *,
    key_prefix: str,
    options: list[str],
    op: str,
) -> None:
    with st.popover("Move to CSM", use_container_width=True):
        picked_list = _get_selected_queue_sales_cases(key_prefix, options)
        if not picked_list:
            st.caption("Select case(s), then open **Move to CSM** again.")
            return
        st.markdown("**" + "**, **".join(picked_list[:12]) + "**")
        if len(picked_list) > 12:
            st.caption(f"+ {len(picked_list) - 12} more")
        st.caption(
            "Creates a CSM field ticket (**Daily Task** or **Resolved**) and "
            "removes the sales case. Case ref must be **9** or **16** digits."
        )
        confirm = st.checkbox(
            "Yes, move to CSM tickets",
            value=False,
            key=f"{key_prefix}_xfer_csm_confirm",
        )
        if st.button(
            "Move",
            key=f"{key_prefix}_xfer_csm_btn",
            type="secondary",
            use_container_width=True,
            disabled=not confirm,
        ):
            ok = 0
            moved: list[str] = []
            for cref in picked_list:
                row = _fetch_sales_case_row_by_ref(cref)
                if not row:
                    st.warning(f"**{cref}** not found.")
                    continue
                row_id = str(row.get("id") or "").strip()
                if not row_id:
                    continue
                try:
                    tid = _cc_transfer_sales_case_to_ticket(
                        row_id, operator_id=op
                    )
                    ok += 1
                    moved.append(tid)
                except Exception as exc:
                    st.error(f"**{cref}**: {exc}")
            if ok:
                _invalidate_dashboard_data_cache()
                st.session_state[_sc_case_selection_session_key(key_prefix)] = []
                st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
                st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_DAILY_TASK
                _sc_set_sales_flash(
                    f"Moved **{ok}** case(s) to CSM (**Daily Task** / **Resolved**)."
                )
                st.rerun()


def _render_sales_case_actions_popover(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str], ...],
    op: str,
    allow_delete: bool,
    allow_transfer_to_csm: bool,
    allow_reassign: bool,
    allow_edit_assignment: bool,
) -> None:
    """Actions menu — only enabled when at least one sales case is selected."""
    picked = _get_selected_queue_sales_cases(key_prefix, options)
    with st.popover("Actions", width=_QUEUE_ACTIONS_POPOVER_WIDTH_PX):
        with st.container(key=f"{key_prefix}_queue_actions_pop"):
            if not picked:
                st.caption(
                    "Tick **Select** on at least one case in the table below. "
                    "No action runs until you do."
                )
                return
            shown = ", ".join(picked[:6])
            extra = f" (+{len(picked) - 6} more)" if len(picked) > 6 else ""
            st.caption(f"**{len(picked)}** selected · {shown}{extra}")
            st.divider()
            _render_sales_case_actions_popover_body(
                df,
                key_prefix=key_prefix,
                options=options,
                status_actions=status_actions,
                op=op,
                allow_delete=allow_delete,
                allow_transfer_to_csm=allow_transfer_to_csm,
                allow_reassign=allow_reassign,
                allow_edit_assignment=allow_edit_assignment,
            )


def _render_sales_case_actions_popover_body(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str], ...],
    op: str,
    allow_delete: bool,
    allow_transfer_to_csm: bool,
    allow_reassign: bool,
    allow_edit_assignment: bool,
) -> None:
    """Action buttons inside the Actions popover (selection already verified)."""
    panel_keys = _sc_toolbar_panel_keys(key_prefix)
    edit_keys = _assignment_edit_session_keys(key_prefix)
    reassign_keys = _reassign_session_keys(key_prefix)
    btn_kw = {"use_container_width": True}

    if op and status_actions:
        for lbl, tgt in status_actions:
            label = _sc_toolbar_action_label(lbl, tgt)
            if st.button(
                label,
                key=f"{key_prefix}_pop_sc_{tgt}",
                type="secondary",
                **btn_kw,
            ):
                _sc_apply_status_to_selected_cases(
                    df,
                    key_prefix=key_prefix,
                    case_options=options,
                    target_status=tgt,
                    op=op,
                )

    has_primary = bool(op and status_actions)
    has_below = (
        allow_edit_assignment
        or (allow_reassign and op)
        or (allow_transfer_to_csm and op)
        or allow_delete
    )
    if has_primary and has_below:
        st.divider()

    if allow_edit_assignment:
        if st.button(
            "Edit assignment",
            key=f"{key_prefix}_pop_sc_edit",
            type="secondary",
            **btn_kw,
        ):
            if st.session_state.get(edit_keys["show"]):
                st.session_state.pop(edit_keys["show"], None)
                st.session_state.pop(reassign_keys["show"], None)
                st.session_state.pop(panel_keys["details"], None)
                st.rerun()
            elif _require_selected_sales_cases(
                key_prefix=key_prefix, options=options, exactly_one=True
            ):
                _clear_sc_assignment_edit_panels_except(key_prefix)
                st.session_state.pop(reassign_keys["show"], None)
                st.session_state.pop(panel_keys["details"], None)
                st.session_state[edit_keys["show"]] = True
                st.rerun()

    if allow_reassign and op:
        if st.button(
            "Reassign",
            key=f"{key_prefix}_pop_sc_reassign",
            type="secondary",
            **btn_kw,
        ):
            if st.session_state.get(reassign_keys["show"]):
                st.session_state.pop(reassign_keys["show"], None)
                st.session_state.pop(edit_keys["show"], None)
                st.session_state.pop(panel_keys["details"], None)
                st.rerun()
            elif _require_selected_sales_cases(
                key_prefix=key_prefix, options=options, exactly_one=True
            ):
                _clear_reassign_panels_except(key_prefix)
                st.session_state.pop(edit_keys["show"], None)
                st.session_state.pop(panel_keys["details"], None)
                st.session_state[reassign_keys["show"]] = True
                st.rerun()

    if allow_transfer_to_csm and op:
        with st.expander("Move to CSM", expanded=False):
            confirm = st.checkbox(
                "Confirm move to CSM",
                key=f"{key_prefix}_pop_xfer_csm_confirm",
            )
            if st.button(
                "Move",
                key=f"{key_prefix}_pop_xfer_csm_btn",
                type="secondary",
                disabled=not confirm,
                **btn_kw,
            ):
                picked_list = _get_selected_queue_sales_cases(key_prefix, options)
                ok = 0
                for cref in picked_list:
                    row = _fetch_sales_case_row_by_ref(cref)
                    if not row:
                        st.warning(f"**{cref}** not found.")
                        continue
                    row_id = str(row.get("id") or "").strip()
                    if not row_id:
                        continue
                    try:
                        _cc_transfer_sales_case_to_ticket(
                            row_id, operator_id=op
                        )
                        ok += 1
                    except Exception as exc:
                        st.error(f"**{cref}**: {exc}")
                if ok:
                    _invalidate_dashboard_data_cache()
                    st.session_state[_sc_case_selection_session_key(key_prefix)] = []
                    st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = _DASH_NAV_CSM
                    st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = STATUS_DAILY_TASK
                    _sc_set_sales_flash(
                        f"Moved **{ok}** case(s) to CSM (**Daily Task** / **Resolved**)."
                    )
                    st.rerun()

    if allow_delete:
        st.divider()
        with st.container(key=f"{key_prefix}_ctx_remove"):
            confirm_del = st.checkbox(
                "Confirm permanent remove",
                key=f"{key_prefix}_pop_sc_del_confirm",
            )
            if st.button(
                "Remove",
                key=f"{key_prefix}_pop_sc_del_btn",
                type="secondary",
                disabled=not confirm_del,
                **btn_kw,
            ):
                picked_list = _get_selected_queue_sales_cases(key_prefix, options)
                ok = 0
                for cref in picked_list:
                    row = _fetch_sales_case_row_by_ref(cref)
                    if not row:
                        st.warning(f"**{cref}** not found.")
                        continue
                    row_id = str(row.get("id") or "").strip()
                    if not row_id:
                        continue
                    try:
                        _sales_cases_delete_row(row_id)
                        ok += 1
                    except Exception as exc:
                        st.error(f"**{cref}**: {exc}")
                if ok:
                    _invalidate_dashboard_data_cache()
                    st.session_state[_sc_case_selection_session_key(key_prefix)] = []
                    _sc_set_sales_flash(f"Removed **{ok}** sales case(s).")
                    st.rerun()


def _render_sales_case_queue_actions_row(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    caption: str | None = None,
    **toolbar_kwargs: object,
) -> None:
    """Selection summary + Actions popover above the sales case table."""
    options = _sc_case_options_for_admin(df)
    if not options:
        return
    if caption:
        st.caption(caption)
    picked = _get_selected_queue_sales_cases(key_prefix, options)
    sel_key = _sc_case_selection_session_key(key_prefix)

    with st.container(key=f"{key_prefix}_sc_toolbar"):
        left, right = st.columns([4.1, 1.05], vertical_alignment="center", gap="small")
        with left:
            if picked:
                lc1, lc2 = st.columns([1.2, 1.3], vertical_alignment="center")
                with lc1:
                    word = "case" if len(picked) == 1 else "cases"
                    st.markdown(f"**{len(picked):,}** {word} selected")
                with lc2:
                    if st.button(
                        "Clear selection",
                        key=f"{key_prefix}_sc_ctx_clear",
                        type="secondary",
                    ):
                        st.session_state[_sc_clear_select_flag_key(key_prefix)] = True
                        st.rerun()
            else:
                st.caption(
                    "Tick **Select** on case(s) in the table below, then open **Actions**."
                )
        with right:
            _render_sales_case_actions_popover(
                df, key_prefix=key_prefix, options=options, **toolbar_kwargs
            )


def _render_sales_case_toolbar(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    caption: str | None = None,
    status_actions: tuple[tuple[str, str], ...] = (),
    op: str = "",
    allow_delete: bool = True,
    allow_transfer_to_csm: bool = True,
    allow_reassign: bool = False,
    allow_edit_assignment: bool = False,
) -> None:
    """Legacy entry point — delegates to queue actions row + Actions popover."""
    _render_sales_case_queue_actions_row(
        df,
        key_prefix=key_prefix,
        caption=caption,
        status_actions=status_actions,
        op=op,
        allow_delete=allow_delete,
        allow_transfer_to_csm=allow_transfer_to_csm,
        allow_reassign=allow_reassign,
        allow_edit_assignment=allow_edit_assignment,
    )


def _render_clickable_sales_queue_metric(
    col: object,
    *,
    title: str,
    value: int,
    queue_name: str,
    option_label: str,
) -> None:
    main_nav = _normalize_dash_main_nav(st.session_state.get(_DASH_MAIN_NAV_KEY, _DASH_NAV_CSM))
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
    open_tab: str = "next_step",
) -> None:
    """Actions for exactly one selected case (queue moves, site visit assign)."""
    row_id, _cref, r0 = _picked_sales_case_from_selection(
        df, key_prefix=key_prefix, case_options=case_options
    )
    if not row_id or r0 is None:
        return

    cur_status = _sc_effective_status(r0.get("status"))
    if st.session_state.get("sc_action_synced_id") != row_id:
        st.session_state["sc_action_synced_id"] = row_id
        st.session_state["sc_action_comment"] = ""

    status_actions = _sc_status_actions_for_case(cur_status)
    show_site_visit = cur_status == SC_STATUS_REGIONAL
    show_next_step = bool(status_actions and op)
    if not show_next_step and not show_site_visit:
        return

    tab_names: list[str] = []
    if show_next_step:
        tab_names.append("Next Step")
    if show_site_visit:
        tab_names.append("Site Visit")

    use_tabs = len(tab_names) > 1
    if use_tabs:
        tabs = st.tabs(tab_names)
        tab_by_name = {name: i for i, name in enumerate(tab_names)}
        start_idx = tab_by_name.get(
            "Site Visit" if open_tab == "site_visit" else "Next Step",
            0,
        )
        if start_idx > 0:
            st.caption(f"Opened **{tab_names[start_idx]}** — use the tabs to switch.")
    else:
        tabs = [st.container()]

    tab_idx = 0
    if show_next_step:
        with tabs[tab_idx]:
            tab_idx += 1
            with st.container(border=True, key=f"{key_prefix}_sc_next_box"):
                st.markdown("**Next Step**")
                st.caption("Move this case to another queue.")
                status_labels = [
                    _sc_toolbar_action_label(label, tgt) for label, tgt in status_actions
                ]
                label_to_target = _sc_toolbar_label_to_target(status_actions)
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
                        _invalidate_dashboard_data_cache()
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
                    fe_key = f"sc_region_assign_fe_{key_prefix}"
                    fe2_key = f"sc_region_assign_fe2_{key_prefix}"
                    fe_man_key = f"sc_region_assign_fe_manual_{key_prefix}"
                    fe2_man_key = f"sc_region_assign_fe2_manual_{key_prefix}"
                    _render_engineer_pair_fields(
                        fe_names=fe_names,
                        fe_missing=fe_missing,
                        engineer_key=fe_key if fe_names and not fe_missing else fe_man_key,
                        engineer2_key=fe2_key if fe_names and not fe_missing else fe2_man_key,
                    )
                    st.checkbox(
                        "Post Assignment to Field Telegram",
                        value=False,
                        key=f"sc_region_assign_post_tg_{key_prefix}",
                    )
                    assign_submitted = st.form_submit_button(
                        "Save Engineer Assignment",
                        type="primary",
                        use_container_width=True,
                    )
                if assign_submitted:
                    try:
                        handle, handle2 = _resolve_engineer_pair(
                            fe_names=fe_names,
                            fe_missing=fe_missing,
                            engineer_key=fe_key if fe_names and not fe_missing else fe_man_key,
                            engineer2_key=fe2_key if fe_names and not fe_missing else fe2_man_key,
                            required_primary=True,
                        )
                    except ValueError as ve:
                        st.error(str(ve))
                    else:
                        patch: dict[str, object] = {
                            "assigned_to": handle,
                            "assigned_to_2": handle2,
                        }
                        _sc_stamp_last_assigned_at(patch)
                        post_tg = bool(
                            st.session_state.get(f"sc_region_assign_post_tg_{key_prefix}")
                        )
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
                                tg_info = _coassignee_telegram_note(
                                    handle2,
                                    str(r0.get("description") or "").strip() or None,
                                )
                                try:
                                    asyncio.run(
                                        notify_telegram_group(
                                            handle.lstrip("@"),
                                            cref or row_id[:8],
                                            fcat,
                                            additional_info=tg_info,
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
                            _invalidate_dashboard_data_cache()
                            msg = f"Engineer **{handle}** assigned for **{region_label}**."
                            if handle2:
                                msg += f" Co-assignee: **{handle2}**."
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
    allow_delete: bool = True,
    allow_edit_assignment: bool = False,
    allow_reassign: bool = False,
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
    _maybe_apply_pending_sales_case_selection_clear(key_prefix)
    with st.container(key=f"{key_prefix}_queue_block"):
        _render_selectable_sales_case_table(sub, key_prefix=key_prefix, cols=table_cols)
        if options:
            _get_selected_queue_sales_cases(key_prefix, options)
            _render_sales_case_toolbar(
                sub,
                key_prefix=key_prefix,
                caption=toolbar_caption,
                status_actions=status_actions if op else (),
                op=op,
                allow_delete=allow_delete,
                allow_edit_assignment=allow_edit_assignment,
                allow_reassign=allow_reassign,
            )

    reassign_open = bool(
        op
        and allow_reassign
        and st.session_state.get(_reassign_session_keys(key_prefix)["show"])
    )
    edit_open = bool(
        op
        and allow_edit_assignment
        and st.session_state.get(_assignment_edit_session_keys(key_prefix)["show"])
    )

    if edit_open:
        with st.container(border=True, key=f"{key_prefix}_edit_panel"):
            _render_sales_assignment_editor(
                key_prefix=key_prefix,
                edit_key_prefix=key_prefix,
                field_cats=field_cats or list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES),
                fe_names=fe_names or [],
                fe_missing=fe_missing,
                case_options=options,
                df=sub,
            )

    if reassign_open:
        with st.container(border=True, key=f"{key_prefix}_reassign_panel"):
            _render_sales_reassign_editor(
                key_prefix=key_prefix,
                edit_key_prefix=key_prefix,
                field_cats=field_cats or list(DEFAULT_ASSIGNMENT_TASK_CATEGORIES),
                fe_names=fe_names or [],
                fe_missing=fe_missing,
                case_options=options,
                df=sub,
            )

    if (
        show_work_panel
        and queue_status != SC_STATUS_RESOLVED
        and not reassign_open
        and not edit_open
    ):
        selected = _get_selected_queue_sales_cases(key_prefix, options)
        if len(selected) == 1:
            with st.container(border=True, key=f"{key_prefix}_work_panel"):
                _render_sales_case_work_panel(
                    sub,
                    key_prefix=key_prefix,
                    case_options=options,
                    op=op,
                    sales_cats=sales_cats or _sales_category_options(),
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
    sales_cats = _sales_category_options(cat_names)
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
            allow_edit_assignment=True,
            allow_reassign=True,
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
            allow_edit_assignment=True,
            allow_reassign=True,
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
            allow_edit_assignment=True,
            allow_reassign=True,
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
            allow_delete=True,
            allow_edit_assignment=False,
            **work_kw,
        )


def _render_dashboard(
    *,
    lookback_days: int,
) -> None:
    day_word = "day" if lookback_days == 1 else "days"
    refreshed_at = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    _apply_pending_dashboard_nav()
    _migrate_legacy_queue_nav()
    nav_intent = _normalize_dash_main_nav(st.session_state.get(_DASH_MAIN_NAV_KEY, _DASH_NAV_CSM))
    if nav_intent not in _DASH_MAIN_NAV_OPTIONS:
        st.session_state[_DASH_MAIN_NAV_KEY] = _DASH_NAV_CSM
        nav_intent = _DASH_NAV_CSM
    elif nav_intent != st.session_state.get(_DASH_MAIN_NAV_KEY):
        st.session_state[_DASH_MAIN_NAV_KEY] = nav_intent

    _render_dashboard_header(refreshed_at=refreshed_at)
    main_nav = _render_main_navigation()

    if main_nav == "Log":
        _render_attendance_tab(lookback_days=lookback_days)
        return
    if main_nav == "Performance":
        _render_field_performance_tab(lookback_days=lookback_days)
        return
    if main_nav == "Sales Cases":
        _render_sales_cases_dashboard()
        return

    _maybe_run_unattended_close()
    _maybe_toast_new_telegram_activity()

    try:
        df_all = _fetch_tickets()
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
    total_in_tabs = int(
        (
            pending_mask
            | on_hold_mask
            | open_mask
            | investigation_mask
            | completed_mask
            | unattended_mask
            | other_mask
        ).sum()
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

    if queue_view == STATUS_DAILY_TASK:
        st.markdown(f"##### {STATUS_DAILY_TASK} — Waiting on Field")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            pend = df[pending_mask].copy()
            if pend.empty:
                st.info(f"No pending tickets in the last {lookback_days} {day_word}.")
            else:
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
                _render_ticket_toolbar_then_table(
                    pend,
                    key_prefix="assigned",
                    cols=pend_show,
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
                    allow_admin_close=_is_dashboard_admin(),
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
                _render_ticket_toolbar_then_table(
                    na_df,
                    key_prefix="on_hold",
                    cols=na_show,
                    status_actions=(
                        ("Send to Needs Review", "Open", "BackToOpenFromOnHold"),
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
                    allow_admin_close=_is_dashboard_admin(),
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
        st.markdown("##### Unattended — Permanent Record (No Same-Day Field Response)")
        if df.empty:
            st.info(f"No tickets in the last {lookback_days} {day_word}.")
        else:
            unat = df[unattended_mask].copy()
            if unat.empty:
                st.info(
                    "No **Unattended** tickets in this time window. "
                    "When an engineer does not reply before assign-day cutoff, the ticket is "
                    "marked unattended permanently and moved to **Needs Review** for admin."
                )
            else:
                _render_ticket_toolbar_then_table(
                    unat,
                    key_prefix="unattended",
                    cols=_TICKET_QUEUE_TABLE_COLS
                    + ("status", "marked_unattended_at", "additional_info", "unattended_nudge_sent_at"),
                    caption=(
                        "Permanent unattended count — stays even if status changes. "
                        "New auto-unattended tickets appear in **Needs Review** for admin action."
                    ),
                    status_actions=(
                        ("Send to Daily Task", STATUS_DAILY_TASK, "ReopenedFromUnattended"),
                    ),
                    allow_delete=True,
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
                _render_ticket_toolbar_then_table(
                    open_df,
                    key_prefix="open",
                    cols=_TICKET_QUEUE_TABLE_COLS
                    + ("outcome_category", "additional_info", "created_at"),
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
                    allow_admin_close=_is_dashboard_admin(),
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
                inv_cols = list(
                    _TICKET_QUEUE_TABLE_COLS + ("additional_info", "created_at")
                )
                if "follow_up_note" in inv_df.columns:
                    inv_cols.extend(["follow_up_at", "follow_up_note"])
                _render_ticket_toolbar_then_table(
                    inv_df,
                    key_prefix="investigation",
                    cols=tuple(dict.fromkeys(c for c in inv_cols if c in inv_df.columns)),
                    highlight_follow_up=True,
                    caption=None,
                    status_actions=(
                        ("Back to Open", "Open", "BackToOpenFromInvestigation"),
                        ("Mark Resolved", STATUS_RESOLVED, "Resolved"),
                        ("On Hold", STATUS_ON_HOLD, "OnHold"),
                    ),
                    allow_delete=True,
                    allow_edit_assignment=True,
                    allow_reassign=True,
                    allow_admin_close=_is_dashboard_admin(),
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
                _render_ticket_toolbar_then_table(
                    done,
                    key_prefix="completed",
                    cols=_TICKET_QUEUE_TABLE_COLS
                    + ("outcome_category", "additional_info", "created_at"),
                    caption="Send back to **Open** for more field work.",
                    status_actions=(
                        ("Send back to Open", "Open", "Reopened"),
                    ),
                    allow_delete=True,
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
    """Field ticket outcomes per person in the sidebar window."""
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
    if not in_view.empty:
        _qm = _ticket_queue_count_masks(in_view)
        n_tab_union = int(
            (
                _qm["pending"]
                | _qm["on_hold"]
                | _qm["open"]
                | _qm["investigation"]
                | _qm["completed"]
                | _qm["unattended"]
                | _qm["other"]
            ).sum()
        )
        n_other_status = int(_qm["other"].sum())
    else:
        n_tab_union = 0
        n_other_status = 0

    if field_has_data and n_in_view == 0:
        st.info(
            "No field tickets in this time window. Try **Last 30 days** in the sidebar."
        )

    sales_all = pd.DataFrame()
    try:
        raw_sales = _fetch_sales_cases_cached()
        sales_all = raw_sales if raw_sales is not None else pd.DataFrame()
    except Exception as exc:
        st.warning(f"Could not load Sales Cases for Performance: {exc}")
    sales_in_view, _n_sales_range = _dashboard_sales_cases_in_view(
        sales_all,
        range_start=range_start,
        range_end=range_end,
    )
    sales_summary = _perf_build_sales_summary(sales_in_view)
    n_sales = len(sales_in_view)

    # Performance tabs — Weekly attended is always in the tab row.
    if field_has_data or not sales_all.empty or not df_all.empty:
        visits_all = pd.DataFrame()
        try:
            visits_all = _fetch_visits_in_range(range_start, range_end)
        except Exception:
            pass

        summary = _perf_build_summary(
            pending, open_df, completed, investigation, on_hold, unattended
        )
        visit_summary = _perf_build_visit_summary(visits_all)
        overview_table = _perf_merge_field_and_visit_summaries(summary, visit_summary)
        overview_table = _perf_attach_sales_to_overview(overview_table, sales_summary)
        people = _perf_focus_people(summary, visits_all, sales_summary)
        focus = st.selectbox(
            "Focus Assignee (Field)",
            options=people,
            key="perf_focus_person",
            help=(
                "Filter Performance tabs to one engineer, or **All**. "
                "Includes field queues, visit cycles, and Sales Cases staff."
            ),
        )
        pending_f = _perf_filter_by_person(pending, focus)
        open_f = _perf_filter_by_person(open_df, focus)
        completed_f = _perf_filter_by_person(completed, focus)
        investigation_f = _perf_filter_by_person(investigation, focus)
        on_hold_f = _perf_filter_by_person(on_hold, focus)
        unattended_f = _perf_filter_by_person(unattended, focus)
        sales_f = _perf_filter_by_person(
            _perf_enrich_sales_cases(sales_in_view), focus,
        )
        work_f = _perf_combine_work(completed_f, investigation_f)
        n_work = len(work_f)
        responded_in_view = pd.DataFrame()
        if not visits_all.empty and "outcome" in visits_all.columns:
            responded_in_view = visits_all[
                visits_all["outcome"].astype(str).eq("responded")
            ].copy()
        n_handled_visit_tickets = (
            int(responded_in_view["ticket_number"].astype(str).nunique())
            if not responded_in_view.empty and "ticket_number" in responded_in_view.columns
            else 0
        )
        n_filtered = (
            len(pending_f)
            + len(open_f)
            + len(on_hold_f)
            + len(completed_f)
            + len(investigation_f)
            + len(unattended_f)
        )

        if n_sales > 0:
            m0, m1, m2, m3, m4, m5, m6, m7 = st.columns(8)
        else:
            m0, m1, m2, m3, m4, m5, m6 = st.columns(7)
            m7 = None
        m0.metric(
            "In view",
            n_in_view if focus == "All" else n_filtered,
            help="Field tickets in the sidebar window (snapshot queues).",
        )
        m1.metric(STATUS_DAILY_TASK, len(pending_f))
        m2.metric("Needs Review", len(open_f))
        m3.metric("On Hold", len(on_hold_f))
        m4.metric(STATUS_RESOLVED, len(completed_f))
        m5.metric("Investigation", len(investigation_f))
        m6.metric("Unattended", len(unattended_f))
        if m7 is not None:
            m7.metric(
                "Sales Cases",
                n_sales if focus == "All" else len(sales_f),
                help="In sidebar range plus any still in an active sales queue.",
            )
        visits_f = _perf_filter_visits_by_person(visits_all, focus)
        n_case_info = (
            int(visits_f["ticket_number"].astype(str).nunique())
            if not visits_f.empty and "ticket_number" in visits_f.columns
            else 0
        )

        st.caption(
            "**Ticket queues** = current snapshot (`tickets_active`). "
            "**Visit cycles** = per-assignment history (`ticket_visits`, fair credit when A→B→C on one ticket). "
            f"**Visit responded** counts closed cycles; **Handled** is {STATUS_RESOLVED} + Investigation in the window."
        )
        if focus == "All" and n_tab_union != n_in_view:
            if n_other_status:
                raw_other = (
                    in_view.loc[_qm["other"], "status"]
                    .astype(str)
                    .str.strip()
                    .value_counts()
                    .head(5)
                    if not in_view.empty and "status" in in_view.columns
                    else pd.Series(dtype=int)
                )
                detail = ", ".join(f"**{k}** ({v})" for k, v in raw_other.items())
                st.warning(
                    f"**{n_other_status}** in-view ticket(s) use a status not mapped to a "
                    f"queue tab ({detail}). **{n_tab_union}** tabbed vs **{n_in_view}** in view."
                )
            else:
                st.warning(
                    f"**{n_tab_union}** ticket(s) in queue tabs vs **{n_in_view}** in view — "
                    "some rows may have a blank or unmappable status."
                )

        n_sales_tab = len(sales_f) if focus not in ("", "All") else n_sales

        _wk_start, _wk_end, _, _ = _perf_calendar_week_range_utc(week_offset=0)
        weekly_this_week = _perf_weekly_attended_bundle(
            df_all,
            sales_all,
            range_start=_wk_start,
            range_end=_wk_end,
        )
        n_weekly = int(weekly_this_week["total"])

        tab_overview, tab_weekly, tab_sales, tab_case_info, tab_work, tab_hold, tab_unatt = st.tabs(
            [
                "Overview",
                f"Weekly ({n_weekly})",
                f"Sales Cases ({n_sales_tab})",
                f"Case Info ({n_case_info})",
                f"Handled ({n_work})",
                f"On Hold ({len(on_hold_f)})",
                f"Unattended ({len(unattended_f)})",
            ]
        )

        with tab_weekly:
            _render_perf_weekly_attended_report(
                df_all, sales_all, this_week_bundle=weekly_this_week
            )

        with tab_overview:
            _render_perf_solo_shared_board(
                visits_all,
                focus=focus,
                overview_table=overview_table,
                sales_summary=sales_summary,
            )
            if not overview_table.empty:
                _render_perf_queue_strip(overview_table, focus=focus)
            _render_perf_solo_shared_detail(
                visits_all, focus=focus, sales_cases=sales_in_view,
            )

        with tab_sales:
            _render_perf_sales_cases_tab(
                sales_f,
                focus=focus,
                bucket_fmt=bucket_fmt,
                x_title=x_title,
                axis_format=axis_format,
            )

        with tab_case_info:
            _render_perf_case_info_tab(visits_all, focus=focus)

        with tab_work:
            st.caption(
                f"**Handled tab ({n_work})** = {STATUS_RESOLVED} + Investigation ticket snapshot. "
                f"**Visit fair credit** = distinct tickets per engineer from responded visits "
                f"({n_handled_visit_tickets} unique tickets in range)."
            )
            if work_f.empty and visits_f.empty:
                st.info("No resolved/investigation tickets or visits for this filter.")
            else:
                view = _perf_enrich_tickets(work_f) if not work_f.empty else work_f
                c_chart, c_table = st.columns([3, 2])
                with c_chart:
                    if not visits_f.empty:
                        responded_visits = visits_f[
                            visits_f["outcome"].astype(str).eq("responded")
                        ]
                        if responded_visits.empty:
                            st.caption("No responded visits in this time range.")
                        else:
                            st.markdown("**Handled by visit assignee (fair credit)**")
                            _render_handled_visit_credit_bar(responded_visits)
                    elif not view.empty:
                        _render_perf_person_bar(
                            view,
                            title="Handled by assignee (ticket snapshot)",
                            value_name="Handled",
                        )
                with c_table:
                    if work_f.empty:
                        st.caption("No ticket snapshot rows for split table.")
                    else:
                        st.markdown("**Split (ticket snapshot)**")
                        split = (
                            view.groupby(["_outcome", "category"], as_index=False)
                            .size()
                            .rename(columns={"size": "Tickets"})
                            .sort_values(
                                ["Tickets", "_outcome", "category"],
                                ascending=[False, True, True],
                            )
                            .rename(columns={"_outcome": "Outcome", "category": "Outcome category"})
                        )
                        st.dataframe(split, use_container_width=True, hide_index=True)
                        if "assigned_category" in view.columns:
                            assign_split = (
                                view.groupby(["_outcome", "assigned_category"], as_index=False)
                                .size()
                                .rename(
                                    columns={
                                        "size": "Tickets",
                                        "_outcome": "Outcome",
                                        "assigned_category": "Assigned category",
                                    }
                                )
                                .sort_values(
                                    ["Tickets", "Outcome", "Assigned category"],
                                    ascending=[False, True, True],
                                )
                            )
                            st.markdown("**Assigned vs outcome (mismatch check)**")
                            st.dataframe(
                                assign_split,
                                use_container_width=True,
                                hide_index=True,
                            )
                if not view.empty:
                    with st.expander("Trend & ticket list", expanded=False):
                        _render_perf_outcome_trend(
                            view,
                            bucket_fmt=bucket_fmt,
                            x_title=x_title,
                            axis_format=axis_format,
                        )
                        _render_perf_ticket_table(view)
                elif not visits_f.empty:
                    with st.expander("Visit detail list", expanded=False):
                        _render_visit_detail_table(visits_f)

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
                "No same-day field response before assign-day cutoff — listed for "
                "accountability only; **does not count** toward Overview credit, "
                "Handled, or Weekly attended."
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
