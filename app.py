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
    # Apply migration 20260520_dashboard_users.sql — per-user login (recommended).
    # Default seed: username admin / password ChangeMeNow! (change after first login).
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
import hashlib
import hmac
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt
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
_DASH_PENDING_MAIN_NAV_KEY = "_dash_pending_main_nav"
_DASH_PENDING_TICKET_QUEUE_KEY = "_dash_pending_ticket_queue"
_DASH_MAIN_NAV_OPTIONS: tuple[str, ...] = ("Tickets", "Log", "Performance")
_CC_SESSION_TOKEN_KEY = "_ticket_dashboard_cc_bot_token_session"
_CC_SESSION_GROUP_KEY = "cc_cmd_center_telegram_group_id"
_CC_NEW_TICKET_LABEL = "New ticket number…"
_CC_FE_SELECT_KEY = "cc_fe_select"
_CC_FE_MANUAL_KEY = "cc_fe_manual"
_CC_TICKET_MODE_KEY = "_cc_ticket_input_mode"
_CC_TICKET_PICK_KEY = "cc_ticket_pick"
_CC_TICKET_NEW_VAL_KEY = "cc_ticket_new_val"
_CC_TICKET_EXTRAS_KEY = "_cc_ticket_extras"

# Session keys — namespaced so we never collide with other widgets / demos,
# and so a stale boolean from an older app version cannot bypass the gate.
_AUTH_OK_KEY = "_ticket_dashboard_auth_ok"
_AUTH_PWD_VER_KEY = "_ticket_dashboard_auth_pwd_ver"
_AUTH_USERNAME_KEY = "_ticket_dashboard_auth_username"
_OPERATOR_ID_KEY = "_ticket_dashboard_operator_id"
_LOGIN_VIEW_KEY = "_ticket_dashboard_login_view"
_MIN_DASHBOARD_PASSWORD_LEN = 8
_MAX_OPERATOR_ID_LEN = 64
_MAX_DASHBOARD_USERNAME_LEN = 48


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


def _dashboard_users_configured() -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        res = _get_supabase_client().rpc("dashboard_users_configured").execute()
        return bool(res.data)
    except Exception:
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


def _dashboard_admin_usernames() -> frozenset[str]:
    raw = _read_setting("DASHBOARD_ADMIN_USERNAMES", "admin")
    if not raw:
        return frozenset({"admin"})
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return frozenset(parts) if parts else frozenset({"admin"})


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

    with st.expander("Team accounts (admin)", expanded=False):
        st.caption(
            "Create dashboard logins for your team. "
            "Re-enter **your** password to confirm each action."
        )
        admin_pw = st.text_input(
            "Your password (confirm)",
            type="password",
            key="dash_team_admin_pw",
            autocomplete="current-password",
        )

        view_tab, add_tab = st.tabs(["Accounts", "Add user"])

        with view_tab:
            if st.button("Refresh list", key="dash_team_refresh", use_container_width=True):
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
                st.caption("Click **Refresh list** to load accounts.")
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
                new_pw = st.text_input("Temporary password", type="password")
                confirm_pw = st.text_input("Confirm password", type="password")
                submitted = st.form_submit_button(
                    "Create account", use_container_width=True
                )

            if not submitted:
                return
            if not admin_pw:
                st.error("Enter your password under **Your password (confirm)** first.")
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
        /* Login page: same deep dark-black as dashboard */
        .stApp,
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"],
        [data-testid="stMain"],
        [data-testid="stAppViewContainer"] section.main {
            background-color: var(--bon-bg) !important;
        }
        [data-testid="stHeader"] {
            border-bottom: none !important;
            box-shadow: none !important;
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
        }
        [data-testid="stAppViewContainer"] section.main .block-container {
            width: min(30rem, 92vw) !important;
            max-width: 30rem !important;
            margin: 0 auto !important;
            padding: 0.5rem 0.75rem 1.5rem !important;
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
            background-color: var(--bon-card) !important;
        }
        [data-testid="stAppViewContainer"] section.main .block-container [data-testid="stMarkdownContainer"] {
            width: 100% !important;
            text-align: center !important;
        }
        h2.bon-login-title {
            font-size: 2.55rem !important;
            font-weight: 700 !important;
            color: #e8e6e3 !important;
            text-align: center !important;
            width: 100% !important;
            margin: 0 auto 0.35rem auto !important;
            padding: 0 !important;
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
    with st.container(border=True, key="login_shell"):
        with st.form("login_form", clear_on_submit=False):
            if per_user:
                user = st.text_input(
                    "Username",
                    placeholder="your login name",
                    autocomplete="username",
                )
                pwd = st.text_input(
                    "Password",
                    type="password",
                    autocomplete="current-password",
                )
            else:
                st.caption("Legacy mode: shared dashboard password.")
                user = ""
                pwd = st.text_input(
                    "Password",
                    type="password",
                    autocomplete="current-password",
                )
                oid = st.text_input(
                    "Operator ID",
                    placeholder="e.g. ali.ops",
                )
            submitted = st.form_submit_button("Sign in", use_container_width=True)

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
            code = st.text_input("Reset code", placeholder="8-character code")
            new_pw = st.text_input("New password", type="password")
            confirm_pw = st.text_input("Confirm new password", type="password")
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
    per_user = _dashboard_users_configured()
    legacy_pw = _read_dashboard_password() if not per_user else ""

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
            '<h2 class="bon-login-title">Field Ticket Ops</h2>'
            '<p class="bon-login-sub">Sign in to continue.</p>',
            unsafe_allow_html=True,
        )

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
                    "Forgot password",
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


def _cc_assign_ticket_options(*, limit: int = 80) -> list[str]:
    """Recent ticket numbers for the Command Center assign dropdown."""
    try:
        df = _fetch_tickets()
    except Exception:
        return []
    opts = _ticket_options_for_admin(df)
    return opts[:limit] if limit else opts


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
) -> None:
    if choice == "Delete row":
        if not confirm_del:
            st.warning("Check **Yes, remove permanently** first.")
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


def _render_ticket_delete_popover(
    *,
    key_prefix: str,
    options: list[str],
    status_actions: tuple[tuple[str, str, str], ...],
) -> None:
    """Secondary remove flow — popover, confirm checkbox, disabled until checked."""
    picked = str(st.session_state.get(f"{key_prefix}_sb_ticket", options[0]))
    with st.popover("Remove…", use_container_width=True):
        st.markdown(f"**{picked}**")
        st.caption("Removes from queue · **Log** keeps history.")
        confirm_del = st.checkbox(
            "Yes, remove permanently",
            value=False,
            key=f"{key_prefix}_del_confirm",
        )
        if st.button(
            "Delete",
            key=f"{key_prefix}_del_btn",
            type="secondary",
            use_container_width=True,
            disabled=not confirm_del,
        ):
            _apply_admin_ticket_action(
                picked=picked,
                choice="Delete row",
                confirm_del=confirm_del,
                status_actions=status_actions,
            )


def _render_admin_ticket_toolbar(
    df: pd.DataFrame,
    *,
    key_prefix: str,
    caption: str | None = None,
    status_actions: tuple[tuple[str, str, str], ...] = (),
    allow_delete: bool = True,
) -> None:
    """Ticket picker + primary action; remove lives in a small side popover."""
    options = _ticket_options_for_admin(df)
    if not options:
        return

    if caption:
        st.caption(caption)

    status_labels = [a[0] for a in status_actions]
    del_col = 1 if allow_delete else 0

    with st.container(border=True):
        if status_labels:
            widths = [2, 1, del_col] if del_col else [2, 1]
            cols = st.columns(widths, vertical_alignment="bottom")
            c_ticket, c_action = cols[0], cols[1]
            c_del = cols[2] if del_col else None
            with c_ticket:
                picked = st.selectbox(
                    "Ticket",
                    options=options,
                    key=f"{key_prefix}_sb_ticket",
                )
            with c_action:
                if len(status_labels) == 1:
                    label = status_labels[0]
                    if st.button(
                        label,
                        key=f"{key_prefix}_apply",
                        type="primary",
                        use_container_width=True,
                    ):
                        _apply_admin_ticket_action(
                            picked=picked,
                            choice=label,
                            confirm_del=False,
                            status_actions=status_actions,
                        )
                else:
                    choice = st.selectbox(
                        "Action",
                        options=status_labels,
                        key=f"{key_prefix}_action_sel",
                    )
                    if st.button(
                        "Apply",
                        key=f"{key_prefix}_apply",
                        type="primary",
                        use_container_width=True,
                    ):
                        _apply_admin_ticket_action(
                            picked=picked,
                            choice=choice,
                            confirm_del=False,
                            status_actions=status_actions,
                        )
            if c_del is not None:
                with c_del:
                    _render_ticket_delete_popover(
                        key_prefix=key_prefix,
                        options=options,
                        status_actions=status_actions,
                    )
        else:
            if del_col:
                c_ticket, c_del = st.columns([3, 1], vertical_alignment="bottom")
            else:
                c_ticket, c_del = st.columns([1]), None
            with c_ticket:
                st.selectbox(
                    "Ticket",
                    options=options,
                    key=f"{key_prefix}_sb_ticket",
                )
            if c_del is not None:
                with c_del:
                    _render_ticket_delete_popover(
                        key_prefix=key_prefix,
                        options=options,
                        status_actions=status_actions,
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
        "Time range",
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
            "New activity — check **Pending**, **Open**, **Log**, or **Performance**.",
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
    operator_id: str,
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
        "status": "Pending",
        "field_response": None,
        "photo_url": None,
        "updated_at": now_iso,
        "last_assigned_at": now_iso,
        "additional_info": additional_info,
        "dashboard_assigned_by": operator_id,
    }
    _cc_execute_ticket_update(client, updates, ticket_number)

    _cc_insert_attendance_log(
        client,
        ticket_number=ticket_number,
        member_username=assigned_to,
        action_type="Assignment",
        note=_cc_assignment_log_note(additional_info, operator_id),
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
            "Add handle",
            key="fe_new_handle",
            placeholder="name",
            label_visibility="collapsed",
        )
    with c_go:
        if st.button("+", key="fe_add_btn", help="Add handle", type="secondary"):
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


def _on_cc_ticket_pick_change() -> None:
    if st.session_state.get(_CC_TICKET_PICK_KEY) == _CC_NEW_TICKET_LABEL:
        st.session_state[_CC_TICKET_MODE_KEY] = "new"
    else:
        st.session_state.pop(_CC_TICKET_MODE_KEY, None)


def _render_ticket_number_picker() -> None:
    """Single ticket control: dropdown for existing, or type when **New ticket number…** is chosen."""
    recent = _cc_assign_ticket_options()
    extras: list[str] = st.session_state.setdefault(_CC_TICKET_EXTRAS_KEY, [])
    all_tickets = list(dict.fromkeys([*extras, *recent]))
    use_new = st.session_state.get(_CC_TICKET_MODE_KEY) == "new" or not all_tickets

    if use_new:
        st.text_input(
            "Ticket",
            placeholder="9 or 16 digits",
            key=_CC_TICKET_NEW_VAL_KEY,
        )
        if all_tickets and st.button(
            "Pick existing",
            key="cc_ticket_use_list",
            type="secondary",
            use_container_width=True,
        ):
            st.session_state.pop(_CC_TICKET_MODE_KEY, None)
            st.rerun()
        return

    st.selectbox(
        "Ticket",
        options=[_CC_NEW_TICKET_LABEL, *all_tickets],
        key=_CC_TICKET_PICK_KEY,
        on_change=_on_cc_ticket_pick_change,
    )


def _resolve_cc_ticket_number() -> str:
    if st.session_state.get(_CC_TICKET_MODE_KEY) == "new":
        return str(st.session_state.get(_CC_TICKET_NEW_VAL_KEY, "")).strip()
    pick = st.session_state.get(_CC_TICKET_PICK_KEY, "")
    if pick == _CC_NEW_TICKET_LABEL:
        return str(st.session_state.get(_CC_TICKET_NEW_VAL_KEY, "")).strip()
    return str(pick or "").strip()


def _remember_cc_ticket_number(ticket_number: str) -> None:
    tid = ticket_number.strip()
    if not tid:
        return
    extras: list[str] = st.session_state.setdefault(_CC_TICKET_EXTRAS_KEY, [])
    if tid not in extras:
        st.session_state[_CC_TICKET_EXTRAS_KEY] = [tid, *extras][:80]
    st.session_state.pop(_CC_TICKET_MODE_KEY, None)


def _sidebar_command_center() -> None:
    flash = st.session_state.pop(_CC_FLASH_KEY, None)
    if flash:
        st.success(flash)

    st.markdown("##### Assign")
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

    fe_names, fe_missing = _try_fetch_field_engineer_usernames()

    token_session = ""
    chat_session = ""
    additional_info_raw = ""
    submitted = False

    with st.container(border=True, key="cc_assign_block"):
        _render_cc_engineer_row(fe_names, missing=fe_missing)
        _render_ticket_number_picker()
        with st.form("cc_assign_form"):
            cat = st.selectbox(
                "Category",
                options=list(ASSIGNMENT_TASK_CATEGORIES),
            )
            additional_info_raw = st.text_area(
                "Notes (optional)",
                placeholder="Context for the field team",
                height=64,
            )
            if not token_env:
                token_session = st.text_input(
                    "Bot token (session only)",
                    type="password",
                    key=_CC_SESSION_TOKEN_KEY,
                    placeholder="If missing from Secrets",
                )
            if not env_group_ok:
                chat_session = st.text_input(
                    "Group chat id",
                    key=_CC_SESSION_GROUP_KEY,
                    placeholder="-100… or @group",
                )
            submitted = st.form_submit_button(
                "Assign",
                type="primary",
                use_container_width=True,
            )

    if not submitted:
        return

    try:
        if fe_names and not fe_missing:
            pick_choice = st.session_state.get(_CC_FE_SELECT_KEY)
            if not pick_choice or not str(pick_choice).strip():
                st.error("Pick an engineer from the list.")
                return
            handle = _cc_normalize_handle(str(pick_choice))
        else:
            fe_handle_raw = str(st.session_state.get(_CC_FE_MANUAL_KEY, "")).strip()
            if not fe_handle_raw:
                st.error("Enter an engineer Telegram username.")
                return
            handle = _cc_normalize_handle(fe_handle_raw)
        tid = _cc_validate_ticket_number(_resolve_cc_ticket_number())
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

    op_assign = _session_operator_id()
    if not op_assign:
        st.error(
            "Session is missing **Operator ID**. Use **Log out** and sign in again — "
            "Operator ID is required before Command Center can assign."
        )
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
        st.error(f"Supabase upsert failed: {exc}")
        return

    try:
        asyncio.run(
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
    except Exception as exc:
        st.warning(f"{summary} Telegram post failed (saved in Supabase): {exc}")
        return

    _remember_cc_ticket_number(tid)
    st.session_state[_CC_FLASH_KEY] = (
        f"{summary} Posted to Telegram ({NOTIFY_BUILD_ID}, one message)."
    )
    st.rerun()


DEFAULT_REFRESH_MINUTES = 1
MIN_REFRESH_MINUTES = 1
MAX_REFRESH_MINUTES = 60

DEFAULT_LOOKBACK_DAYS = 7
MIN_LOOKBACK_DAYS = 1
MAX_LOOKBACK_DAYS = 365


def _sidebar_controls() -> tuple[bool, int, int]:
    """Return (auto_enabled, interval_minutes, lookback_days)."""
    with st.sidebar:
        st.markdown("### Field Ticket Ops")
        op = _session_operator_id()
        if op:
            st.caption(f"Signed in as **{op}**")

        if _dashboard_users_configured() and _is_dashboard_admin():
            _render_dashboard_team_accounts()

        _sidebar_command_center()

        st.markdown("**Time range**")
        lookback_days, _range_start, _range_end = _sidebar_date_range()

        with st.expander("More filters", expanded=False):
            auto = st.toggle("Auto-refresh", value=True)
            if auto:
                interval_minutes = st.slider(
                    "Every (minutes)",
                    min_value=MIN_REFRESH_MINUTES,
                    max_value=MAX_REFRESH_MINUTES,
                    value=DEFAULT_REFRESH_MINUTES,
                    step=1,
                )
            else:
                interval_minutes = DEFAULT_REFRESH_MINUTES
            if st.button("Refresh now", use_container_width=True):
                _get_supabase_client.clear()
                st.session_state.pop(_DASH_LAST_ATTENDANCE_TS_KEY, None)
                st.rerun()
            lookup = st.text_input(
                "Look up ticket #",
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
        if st.button("Log out", use_container_width=True):
            _clear_auth_session()
            st.session_state.pop(_LOGIN_VIEW_KEY, None)
            st.rerun()
    return auto, int(interval_minutes), int(lookback_days)


def main() -> None:
    # Must be the first Streamlit command every run (login + dashboard).
    st.set_page_config(page_title="Field Ticket Ops", layout="wide")

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
        border: 1px solid var(--bon-oak) !important;
        border-radius: 14px !important;
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
    [data-testid="stSidebar"] .stTextArea textarea {
        border: 1px solid rgba(215, 180, 145, 0.45) !important;
        border-radius: 8px !important;
    }
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div:focus-within,
    [data-testid="stSidebar"] .stTextInput input:focus,
    [data-testid="stSidebar"] .stTextArea textarea:focus {
        border-color: var(--bon-oak) !important;
        box-shadow: 0 0 0 1px var(--bon-oak) !important;
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
        border-radius: 8px !important;
        border: 1px solid rgba(215, 180, 145, 0.45) !important;
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
    div[data-testid="stExpander"] details {
        border: 1px solid var(--bon-oak);
        border-radius: 14px;
        background-color: var(--bon-card);
    }
    div[data-testid="stExpander"] summary {
        color: var(--bon-oak);
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
        border-radius: 10px;
        border: 1px solid rgba(215, 180, 145, 0.28);
    }
    [data-testid="stMetric"]:has([data-testid="stMetricDelta"]) {
        border-color: rgba(215, 180, 145, 0.55);
    }
    [data-testid="stMetric"] label { color: var(--bon-muted) !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--bon-oak) !important;
    }
    .stMarkdown a { color: var(--bon-oak); }
    [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
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
    [data-testid="stMain"] div[class*="st-key-dash_metric_nav_"] .stButton > button {
        background: var(--bon-card) !important;
        border: 1px solid rgba(215, 180, 145, 0.28) !important;
        border-radius: 10px !important;
        color: var(--bon-muted) !important;
        font-weight: 500 !important;
        font-size: 0.8rem !important;
        line-height: 1.35 !important;
        white-space: pre-line !important;
        min-height: 4.25rem !important;
        padding: 0.55rem 0.65rem !important;
        text-align: left !important;
    }
    [data-testid="stMain"] div[class*="st-key-dash_metric_nav_"] .stButton > button:not(:disabled):hover {
        color: var(--bon-text) !important;
        border-color: rgba(215, 180, 145, 0.5) !important;
        background: rgba(215, 180, 145, 0.08) !important;
    }
    [data-testid="stMain"] div[class*="st-key-dash_metric_nav_"] .stButton > button:disabled {
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
    """Map queue label (with optional count) back to queue name."""
    if not label:
        return "Pending"
    for base in ("Pending", "Open", "Completed", "Log", "Performance"):
        if label == base or label.startswith(f"{base} ("):
            return base
    return "Pending"


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
    st.markdown("## Dashboard")
    st.caption(f"Updated **{refreshed_at} {LOCAL_TZ_LABEL}** · change dates in sidebar **Time range**")


def _apply_pending_dashboard_nav() -> None:
    """Apply metric-click navigation before nav widgets are drawn."""
    pending_main = st.session_state.pop(_DASH_PENDING_MAIN_NAV_KEY, None)
    pending_queue = st.session_state.pop(_DASH_PENDING_TICKET_QUEUE_KEY, None)
    if pending_main is not None:
        st.session_state[_DASH_MAIN_NAV_KEY] = pending_main
    if pending_queue is not None:
        st.session_state[_DASH_TICKET_QUEUE_KEY] = pending_queue


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
            key=f"dash_metric_nav_{queue_name.lower()}",
            type="secondary",
            use_container_width=True,
            disabled=active,
        ):
            st.session_state[_DASH_PENDING_MAIN_NAV_KEY] = "Tickets"
            st.session_state[_DASH_PENDING_TICKET_QUEUE_KEY] = option_label
            st.rerun()


def _render_queue_summary_metrics(
    *,
    total_pending: int,
    total_open: int,
    total_completed: int,
    avg_seconds: float | None,
    pending_label: str,
    open_label: str,
    completed_label: str,
) -> None:
    """Counts — click Pending / Needs review / Completed to switch queue."""
    c1, c2, c3, c4 = st.columns(4)
    _render_clickable_queue_metric(
        c1,
        title="Pending",
        value=total_pending,
        queue_name="Pending",
        option_label=pending_label,
    )
    _render_clickable_queue_metric(
        c2,
        title="Needs review",
        value=total_open,
        queue_name="Open",
        option_label=open_label,
    )
    _render_clickable_queue_metric(
        c3,
        title="Completed",
        value=total_completed,
        queue_name="Completed",
        option_label=completed_label,
    )
    with c4:
        st.metric("Avg completion", _format_duration(avg_seconds))


_TICKET_QUEUE_TABLE_COLS: tuple[str, ...] = (
    "ticket_number",
    "assigned_to",
    "task_category",
    "field_response",
    "photo_url",
    "responded_at",
    "last_assigned_at",
)


def _sync_dashboard_nav_state(
    *,
    total_pending: int,
    total_open: int,
    total_completed: int,
) -> tuple[str, str, str]:
    """Keep queue session keys valid; return option labels for metrics."""
    _migrate_legacy_queue_nav()

    if st.session_state.get(_DASH_MAIN_NAV_KEY) not in _DASH_MAIN_NAV_OPTIONS:
        st.session_state[_DASH_MAIN_NAV_KEY] = "Tickets"

    pending_label = _queue_segment_label("Pending", total_pending)
    open_label = _queue_segment_label("Open", total_open)
    completed_label = _queue_segment_label("Completed", total_completed)
    ticket_options = (pending_label, open_label, completed_label)

    prev_open = int(st.session_state.get("_dash_prev_open_count", 0))
    if total_open > prev_open:
        st.session_state[_DASH_MAIN_NAV_KEY] = "Tickets"
        st.session_state[_DASH_TICKET_QUEUE_KEY] = open_label
    elif st.session_state.get(_DASH_TICKET_QUEUE_KEY) not in ticket_options:
        st.session_state[_DASH_TICKET_QUEUE_KEY] = (
            open_label if total_open > 0 else pending_label
        )
    st.session_state["_dash_prev_open_count"] = total_open
    return pending_label, open_label, completed_label


def _render_main_navigation() -> str:
    """Single row: Tickets | Log | Performance."""
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
        df = _apply_lookback(df_all, lookback_days)
        if len(df_all) > len(df):
            st.caption(
                f"Showing **{len(df)}** of **{len(df_all)}** tickets in the last "
                f"{lookback_days} day(s). Widen **Time range** in the sidebar "
                "if a Telegram assignment is missing."
            )

    if not df_all.empty and "status" in df_all.columns:
        mismatches = _fetch_pending_with_response_mismatch()
        if mismatches:
            st.error(
                f"**{len(mismatches)}** ticket(s) have a Response in the log but are still **Pending** "
                f"in `{TICKETS_TABLE}` (e.g. {', '.join(mismatches[:5])}). "
                "The Railway bot could not UPDATE the row — check bot logs and apply "
                "`supabase/migrations/20260516_tickets_active_anon_policies.sql`."
            )

    status = df["status"].astype(str).str.strip() if not df.empty and "status" in df.columns else pd.Series(dtype=str)
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

    total_completed = int(completed_mask.sum()) if not df.empty else 0
    _apply_pending_dashboard_nav()
    pending_label, open_label, completed_label = _sync_dashboard_nav_state(
        total_pending=total_pending,
        total_open=total_open,
        total_completed=total_completed,
    )

    _render_dashboard_header(refreshed_at=refreshed_at)
    main_nav = _render_main_navigation()
    if main_nav == "Tickets":
        _render_queue_summary_metrics(
            total_pending=total_pending,
            total_open=total_open,
            total_completed=total_completed,
            avg_seconds=avg_seconds,
            pending_label=pending_label,
            open_label=open_label,
            completed_label=completed_label,
        )
    queue_view = _queue_segment_base(st.session_state.get(_DASH_TICKET_QUEUE_KEY))

    if main_nav == "Log":
        _render_attendance_tab(lookback_days=lookback_days)
        return

    if main_nav == "Performance":
        _render_field_performance_tab(lookback_days=lookback_days)
        return

    if queue_view == "Pending":
        st.markdown("##### Pending — waiting on field")
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

                show = [
                    c
                    for c in (
                        "ticket_number",
                        "assigned_to",
                        "task_category",
                        "created_at",
                        "last_assigned_at",
                    )
                    if c in pend.columns
                ]
                view = _format_local(pend[show])

                def _row_red(_row: pd.Series) -> list[str]:
                    stale = bool(pend.loc[_row.name, "_stale"]) if "_stale" in pend.columns else False
                    color = "background-color: rgba(215, 180, 145, 0.12); color: #f0e6dc" if stale else ""
                    return [color] * len(_row)

                try:
                    styled = view.style.apply(_row_red, axis=1)
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(view, use_container_width=True, hide_index=True)
                if pend["_stale"].any():
                    st.caption(
                        "Oak-tinted rows have been pending **more than 24 hours**."
                    )

    elif queue_view == "Open":
        st.markdown("##### Open — needs your review")
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
                    caption="Mark **Completed** when verified.",
                    status_actions=(
                        ("Mark Completed", "Completed", "Completed"),
                    ),
                    allow_delete=True,
                )

                open_view = _ticket_queue_view(
                    open_df,
                    cols=_TICKET_QUEUE_TABLE_COLS + ("additional_info", "created_at"),
                )
                st.dataframe(
                    open_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config=_dataframe_column_config(open_view),
                )

                with st.expander("Photo gallery", expanded=total_open <= 3):
                    _render_field_photos_section(open_df)

    elif queue_view == "Completed":
        st.markdown("##### Completed")
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
                    caption="Send back to **Open** for more field work.",
                    status_actions=(
                        ("Send back to Open", "Open", "Reopened"),
                    ),
                    allow_delete=True,
                )

                done_view = _ticket_queue_view(
                    done,
                    cols=_TICKET_QUEUE_TABLE_COLS + ("additional_info", "created_at"),
                )
                st.dataframe(
                    done_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config=_dataframe_column_config(done_view),
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


def _render_field_performance_tab(*, lookback_days: int) -> None:
    """Completed-task counts from ``tickets_active`` (admin **Completed** status)."""
    st.markdown("##### Field team performance")
    st.caption(
        f"**Completed** tickets · {_format_dash_range_caption() or 'sidebar time range'} · {LOCAL_TZ_LABEL}"
    )

    range_start, range_end = _get_dash_range()

    try:
        df_all = _fetch_tickets()
    except Exception as exc:
        st.error(f"Could not load tickets: {exc}")
        return

    if df_all.empty or "status" not in df_all.columns:
        st.info("No ticket data to analyze.")
        return

    done = df_all[
        df_all["status"].astype(str).str.strip().str.casefold() == "completed"
    ].copy()
    if done.empty:
        st.info("No **Completed** tickets in the database yet.")
        return

    u_col = _parse_ts(done["updated_at"]) if "updated_at" in done.columns else pd.Series(pd.NaT, index=done.index)
    r_col = _parse_ts(done["responded_at"]) if "responded_at" in done.columns else pd.Series(pd.NaT, index=done.index)
    done["_ts"] = u_col.where(u_col.notna(), r_col)
    done = done[done["_ts"].notna()]
    if done.empty:
        st.info("Completed tickets have no usable **updated_at** / **responded_at** timestamps.")
        return

    done = done[(done["_ts"] >= range_start) & (done["_ts"] <= range_end)]
    if done.empty:
        st.info(
            "No **Completed** tickets in this time window. "
            "Try **Last 30 days** or **Pick dates** in the sidebar."
        )
        return

    done["_local"] = done["_ts"].dt.tz_convert(LOCAL_TZ)
    if "assigned_to" in done.columns:
        done["staff"] = done["assigned_to"].map(_perf_norm_member)
    else:
        done["staff"] = "(unknown)"

    if "task_category" in done.columns:
        cat_series = done["task_category"].fillna("").astype(str).str.strip()
        done["category"] = cat_series.mask(cat_series.eq(""), "(uncategorized)")
    else:
        done["category"] = "(uncategorized)"

    bucket_fmt, x_title, axis_format = _perf_bucket_settings(range_start, range_end)
    view = done.copy()
    view["bucket"] = view["_local"].dt.strftime(bucket_fmt)

    # --- By assignee ---
    by_staff = (
        view.groupby(["bucket", "staff"], as_index=False)
        .size()
        .rename(columns={"size": "completions"})
    )
    by_staff["bucket_sort"] = pd.to_datetime(by_staff["bucket"], errors="coerce")
    by_staff = by_staff.sort_values("bucket_sort")

    chart_staff = (
        alt.Chart(by_staff)
        .mark_bar()
        .encode(
            x=alt.X(
                "bucket_sort:T",
                title=x_title,
                axis=alt.Axis(labelAngle=-30, format=axis_format),
            ),
            y=alt.Y("completions:Q", title="Completed tasks"),
            color=alt.Color("staff:N", legend=alt.Legend(title="assigned_to")),
            tooltip=[
                alt.Tooltip("bucket:N", title="Bucket"),
                alt.Tooltip("staff:N", title="assigned_to"),
                alt.Tooltip("completions:Q", title="Count"),
            ],
        )
        .properties(height=300)
    )
    st.markdown("##### Completions by **assigned_to** (stacked)")
    st.altair_chart(chart_staff, use_container_width=True)

    # --- By task category ---
    by_cat = (
        view.groupby(["bucket", "category"], as_index=False)
        .size()
        .rename(columns={"size": "completions"})
    )
    by_cat["bucket_sort"] = pd.to_datetime(by_cat["bucket"], errors="coerce")
    by_cat = by_cat.sort_values("bucket_sort")

    chart_cat = (
        alt.Chart(by_cat)
        .mark_bar()
        .encode(
            x=alt.X(
                "bucket_sort:T",
                title=x_title,
                axis=alt.Axis(labelAngle=-30, format=axis_format),
            ),
            y=alt.Y("completions:Q", title="Completed tasks"),
            color=alt.Color("category:N", legend=alt.Legend(title="task_category")),
            tooltip=[
                alt.Tooltip("bucket:N", title="Bucket"),
                alt.Tooltip("category:N", title="task_category"),
                alt.Tooltip("completions:Q", title="Count"),
            ],
        )
        .properties(height=300)
    )
    st.markdown("##### Completions by **task_category** (stacked)")
    st.altair_chart(chart_cat, use_container_width=True)

    with st.expander("Drill down by person", expanded=False):
        staff_opts = sorted(view["staff"].unique().tolist())
        pick = st.selectbox("Person", options=staff_opts, index=0)
        one = view[view["staff"] == pick].copy()
        solo = (
            one.groupby(["bucket", "category"], as_index=False)
            .size()
            .rename(columns={"size": "completions"})
        )
        solo["bucket_sort"] = pd.to_datetime(solo["bucket"], errors="coerce")
        solo = solo.sort_values("bucket_sort")

        solo_chart = (
            alt.Chart(solo)
            .mark_bar()
            .encode(
                x=alt.X(
                    "bucket_sort:T",
                    title=x_title,
                    axis=alt.Axis(labelAngle=-30, format=axis_format),
                ),
                y=alt.Y("completions:Q", title="Completed tasks"),
                color=alt.Color("category:N", legend=alt.Legend(title="task_category")),
                tooltip=[
                    alt.Tooltip("bucket:N", title="Bucket"),
                    alt.Tooltip("category:N", title="task_category"),
                    alt.Tooltip("completions:Q", title="Count"),
                ],
            )
            .properties(height=300, title=f"{pick}")
        )
        st.altair_chart(solo_chart, use_container_width=True)

        show_cols = [
            c
            for c in (
                "ticket_number",
                "assigned_to",
                "task_category",
                "updated_at",
                "responded_at",
                "last_assigned_at",
            )
            if c in one.columns
        ]
        detail = one.sort_values("_ts", ascending=False)[show_cols].head(400)
        st.dataframe(
            _format_local(detail),
            use_container_width=True,
            hide_index=True,
        )


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
