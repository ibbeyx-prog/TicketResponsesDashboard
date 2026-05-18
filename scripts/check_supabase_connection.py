#!/usr/bin/env python3
"""Verify Supabase URL + API key from .env (tries publishable and legacy anon)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from supabase_client import (  # noqa: E402
    _candidate_keys_from_env,
    probe_supabase_key,
    resolve_supabase_config,
    test_supabase_connection,
)

_ENV = ROOT / ".env"


def main() -> int:
    load_dotenv(_ENV, encoding="utf-8-sig")
    cfg = resolve_supabase_config(env_path=_ENV, probe=True)
    if not cfg:
        print("ERROR: SUPABASE_URL and at least one API key required.", file=sys.stderr)
        print(f"Checked {_ENV} (exists={_ENV.exists()})", file=sys.stderr)
        return 1

    print(f"SUPABASE_URL={cfg.url}")
    print(f"Using key from {cfg.key_source} (prefix {cfg.key[:16]}…)")

    status = test_supabase_connection(cfg.url, cfg.key)
    if status.get("ok"):
        print("OK — connected.")
        print(f"dashboard_users_configured={status.get('users_configured')}")
        return 0

    print(f"FAIL — {status.get('error')}: {status.get('detail')}", file=sys.stderr)
    url = cfg.url
    print("\nTrying each key in .env:", file=sys.stderr)
    for name, key in _candidate_keys_from_env():
        ok = probe_supabase_key(url, key)
        print(f"  {name}: {'OK' if ok else 'failed'}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
