"""Browser trace: login + Performance nav; prints Navigation Timing only."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

URL = os.getenv("TRACE_URL", "http://localhost:8501/")
USER = (os.getenv("TRACE_LOGIN_USER") or "ibeyx").strip()
PWD = os.getenv("TRACE_LOGIN_PASSWORD") or os.getenv("DASHBOARD_PASSWORD") or ""


def main() -> None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright not installed; skip browser trace")
        return
    if not PWD:
        print("no TRACE_LOGIN_PASSWORD / DASHBOARD_PASSWORD; skip login")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        t0 = time.perf_counter()
        page.goto(URL, wait_until="domcontentloaded")
        print(f"goto login dom: {round((time.perf_counter()-t0)*1000,1)}ms")

        page.get_by_placeholder("your username").fill(USER)
        page.get_by_placeholder("your password").fill(PWD)
        t1 = time.perf_counter()
        page.get_by_role("button", name="Sign in").click()
        page.wait_for_timeout(500)
        # Wait for header nav (post-login)
        try:
            page.get_by_text("Performance", exact=True).first.wait_for(timeout=120_000)
        except Exception as exc:
            print(f"login wait failed: {exc!r}")
            browser.close()
            return
        print(f"login -> dashboard nav visible: {round((time.perf_counter()-t1)*1000,1)}ms")

        t2 = time.perf_counter()
        page.get_by_text("Performance", exact=True).first.click()
        page.wait_for_timeout(8000)
        print(f"click Performance + 8s settle: {round((time.perf_counter()-t2)*1000,1)}ms")

        nav = page.evaluate(
            """() => {
              const n = performance.getEntriesByType('navigation')[0];
              return n ? {duration: n.duration, domContentLoaded: n.domContentLoadedEventEnd} : null;
            }"""
        )
        print(f"navigation timing: {nav}")
        browser.close()
        print("see logs/dashboard-perf.log for server spans (>=50ms)")


if __name__ == "__main__":
    main()
