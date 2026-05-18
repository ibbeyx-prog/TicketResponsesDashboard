"""Shared Supabase URL/key resolution and HTTP client setup (bot + dashboard)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv
from supabase import ClientOptions, create_client

_DEFAULT_TIMEOUT_SEC = 25.0
_PROBE_RPC = "dashboard_users_configured"


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    key: str
    key_source: str  # env var name that supplied the working key


def _truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in ("1", "true", "yes", "on")


def _strip(s: str | None) -> str:
    return (s or "").strip()


def _candidate_keys_from_env() -> list[tuple[str, str]]:
    """Return (env_var_name, key) pairs in probe order."""
    primary = _strip(os.getenv("SUPABASE_KEY"))
    anon = _strip(os.getenv("SUPABASE_ANON_KEY"))
    prefer_legacy = _truthy(os.getenv("SUPABASE_USE_LEGACY_ANON_KEY"))

    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(name: str, value: str) -> None:
        if not value or value in seen:
            return
        seen.add(value)
        out.append((name, value))

    if prefer_legacy:
        add("SUPABASE_ANON_KEY", anon)
        add("SUPABASE_KEY", primary)
    else:
        add("SUPABASE_KEY", primary)
        add("SUPABASE_ANON_KEY", anon)
    return out


def is_transient_supabase_error(exc: BaseException) -> bool:
    """Network / timeout failures — callers should degrade, not crash."""
    try:
        import httpx

        if isinstance(exc, httpx.HTTPError):
            return True
    except ImportError:
        pass
    if isinstance(exc, (TimeoutError, OSError)):
        return True
    text = str(exc).lower()
    return any(
        needle in text
        for needle in (
            "connecttimeout",
            "readtimeout",
            "connection",
            "10060",
            "timed out",
            "network",
            "failed to respond",
            "name or service not known",
            "connection refused",
            "ssl",
        )
    )


def supabase_http_timeout_sec() -> float:
    raw = _strip(os.getenv("SUPABASE_HTTP_TIMEOUT_SEC"))
    if not raw:
        return _DEFAULT_TIMEOUT_SEC
    try:
        return max(5.0, float(raw))
    except ValueError:
        return _DEFAULT_TIMEOUT_SEC


def _build_httpx_client(timeout_sec: float):
    import httpx

    return httpx.Client(
        timeout=httpx.Timeout(timeout_sec, connect=min(timeout_sec, 20.0)),
        trust_env=True,
    )


def create_supabase_client(
    url: str,
    key: str,
    *,
    timeout_sec: float | None = None,
):
    """Create a Supabase client with proxy-aware httpx and generous timeouts."""
    t = timeout_sec if timeout_sec is not None else supabase_http_timeout_sec()
    http = _build_httpx_client(t)
    opts = ClientOptions(
        postgrest_client_timeout=t,
        storage_client_timeout=t,
        httpx_client=http,
    )
    return create_client(url.rstrip("/"), key, options=opts)


def probe_supabase_key(url: str, key: str, *, timeout_sec: float = 18.0) -> bool:
    """Return True if PostgREST accepts this key (light RPC)."""
    if not url or not key:
        return False
    try:
        client = create_supabase_client(url, key, timeout_sec=timeout_sec)
        client.rpc(_PROBE_RPC).execute()
        return True
    except Exception:
        return False


def resolve_supabase_config(
    *,
    env_path: Path | None = None,
    read_env: Callable[[str, str], str] | None = None,
    probe: bool = True,
) -> SupabaseConfig | None:
    """
    Load URL + best API key from environment.

    ``read_env(key, default)`` can be supplied (e.g. Streamlit ``_read_setting``).
    When ``probe`` is True, tries each candidate key until one responds.
    """
    if env_path and env_path.exists():
        load_dotenv(env_path, encoding="utf-8-sig", override=False)

    def getenv(key: str, default: str = "") -> str:
        if read_env is not None:
            return read_env(key, default)
        return _strip(os.getenv(key)) or default

    url = getenv("SUPABASE_URL", "").rstrip("/")
    if not url:
        return None

    candidates = _candidate_keys_from_env()
    if read_env is not None:
        # Streamlit secrets may override process env for keys not yet in os.environ.
        pk = read_env("SUPABASE_KEY", "")
        ak = read_env("SUPABASE_ANON_KEY", "")
        prefer_legacy = _truthy(read_env("SUPABASE_USE_LEGACY_ANON_KEY", ""))
        candidates = []
        seen: set[str] = set()

        def add(name: str, value: str) -> None:
            v = _strip(value)
            if not v or v in seen:
                return
            seen.add(v)
            candidates.append((name, v))

        if prefer_legacy:
            add("SUPABASE_ANON_KEY", ak)
            add("SUPABASE_KEY", pk)
        else:
            add("SUPABASE_KEY", pk)
            add("SUPABASE_ANON_KEY", ak)

    if not candidates:
        return None

    if not probe:
        name, key = candidates[0]
        return SupabaseConfig(url=url, key=key, key_source=name)

    timeout = min(supabase_http_timeout_sec(), 20.0)
    for name, key in candidates:
        if probe_supabase_key(url, key, timeout_sec=timeout):
            return SupabaseConfig(url=url, key=key, key_source=name)
    # Last resort: first key without probe (caller may still hit errors).
    name, key = candidates[0]
    return SupabaseConfig(url=url, key=key, key_source=name)


@lru_cache(maxsize=4)
def _cached_client(url: str, key: str, timeout_sec: float):
    return create_supabase_client(url, key, timeout_sec=timeout_sec)


def get_cached_supabase_client(url: str, key: str, *, timeout_sec: float | None = None):
    t = timeout_sec if timeout_sec is not None else supabase_http_timeout_sec()
    return _cached_client(url, key, t)


def test_supabase_connection(url: str, key: str) -> dict:
    """Diagnostics dict for scripts and the login page."""
    if not url or not key:
        return {
            "ok": False,
            "error": "missing_url_or_key",
            "detail": "Set SUPABASE_URL and SUPABASE_KEY (or SUPABASE_ANON_KEY).",
        }
    try:
        client = create_supabase_client(url, key)
        res = client.rpc(_PROBE_RPC).execute()
        return {
            "ok": True,
            "users_configured": bool(res.data),
            "key_prefix": key[:12] + "…" if len(key) > 12 else key,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": "transient" if is_transient_supabase_error(exc) else "api_error",
            "detail": str(exc)[:280],
        }


def clear_supabase_client_cache() -> None:
    _cached_client.cache_clear()
