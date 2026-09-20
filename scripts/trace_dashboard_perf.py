"""One-shot backend timing trace (Supabase + perf helpers). No secrets printed."""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
import warnings
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

_NOISE_MARKERS = (
    "ScriptRunContext",
    "MemoryCacheStorageManager",
    "Session state does not function",
    "to view this Streamlit app",
    "streamlit run",
    "No runtime found",
)


def _configure_quiet_streamlit() -> None:
    os.environ.setdefault("DASH_PERF_LOG", "0")
    os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")
    os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
    warnings.filterwarnings("ignore", module=r"streamlit(\.|$)")
    for name in (
        "streamlit",
        "streamlit.runtime",
        "streamlit.runtime.scriptrunner_utils",
        "streamlit.runtime.scriptrunner_utils.script_run_context",
        "streamlit.runtime.state",
        "streamlit.runtime.state.session_state_proxy",
        "streamlit.runtime.caching",
        "streamlit.runtime.caching.cache_data_api",
        "streamlit.runtime.caching.storage",
    ):
        logging.getLogger(name).setLevel(logging.CRITICAL)


class _FilteredStream(io.TextIOBase):
    """Drop Streamlit bare-mode chatter; pass through real errors."""

    def __init__(self, real: io.TextIOBase) -> None:
        self._real = real

    def write(self, s: str) -> int:  # type: ignore[override]
        if s and any(m in s for m in _NOISE_MARKERS):
            return len(s)
        return self._real.write(s)

    def flush(self) -> None:
        self._real.flush()

    def __getattr__(self, name: str):
        return getattr(self._real, name)


@contextmanager
def _quiet_streamlit_io(*, verbose: bool):
    if verbose:
        yield
        return
    old_err = sys.stderr
    sys.stderr = _FilteredStream(old_err)  # type: ignore[assignment]
    try:
        yield
    finally:
        sys.stderr = old_err


def _ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backend dashboard timing trace")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show Streamlit bare-mode warnings (default: hidden)",
    )
    args = parser.parse_args()

    _configure_quiet_streamlit()

    with _quiet_streamlit_io(verbose=args.verbose):
        t_import = time.perf_counter()
        import app as dash  # noqa: WPS433

    print("Coverage Eye backend trace (plain `python`, not `streamlit run`)")
    print(f"import app.py: {_ms(t_import)}ms")

    if not dash.SUPABASE_URL:
        print("SKIP: no SUPABASE_URL")
        return

    with _quiet_streamlit_io(verbose=args.verbose):
        t0 = time.perf_counter()
        configured = dash._dashboard_users_configured()
        print(f"rpc dashboard_users_configured: {_ms(t0)}ms -> {configured}")

        uname = (os.getenv("TRACE_LOGIN_USER") or "ibeyx").strip()
        pwd = os.getenv("TRACE_LOGIN_PASSWORD") or os.getenv("DASHBOARD_PASSWORD") or ""
        if configured and pwd:
            t0 = time.perf_counter()
            try:
                payload = dash._rpc_dashboard_verify_login(
                    dash._normalize_dashboard_username(uname), pwd
                )
                ok = bool(payload.get("ok"))
                print(f"rpc dashboard_verify_login ({uname}): {_ms(t0)}ms -> ok={ok}")
            except Exception as exc:
                print(f"rpc dashboard_verify_login: {_ms(t0)}ms -> error={exc!r}")
        elif pwd:
            print(
                "legacy password set but per-user mode; "
                "set TRACE_LOGIN_PASSWORD for RPC test"
            )

        t0 = time.perf_counter()
        df = dash._fetch_tickets_cached()
        print(f"_fetch_tickets_cached: {_ms(t0)}ms rows={len(df)}")

        t0 = time.perf_counter()
        sales = dash._fetch_sales_cases_cached()
        n_sales = len(sales) if sales is not None else 0
        print(f"_fetch_sales_cases_cached: {_ms(t0)}ms rows={n_sales}")

        dash._init_dash_date_range_state()
        dash._sync_dash_range_from_ui("This week")
        rs, re = dash._get_dash_range()

        t0 = time.perf_counter()
        visits_hist = dash._perf_load_overview_visits_history(df)
        print(f"_perf_load_overview_visits_history: {_ms(t0)}ms rows={len(visits_hist)}")

        t0 = time.perf_counter()
        dash._perf_assignment_task_day_counts_memo(
            df, sales, range_start=rs, range_end=re
        )
        print(f"_perf_assignment_task_day_counts_memo: {_ms(t0)}ms")

        t0 = time.perf_counter()
        dash._perf_team_assignment_summary_df(df, sales, range_start=rs, range_end=re)
        print(f"_perf_team_assignment_summary_df: {_ms(t0)}ms")

        t0 = time.perf_counter()
        vr = dash._fetch_visits_in_range(rs, re)
        print(f"_fetch_visits_in_range: {_ms(t0)}ms rows={len(vr)}")

    print("done")


if __name__ == "__main__":
    main()
