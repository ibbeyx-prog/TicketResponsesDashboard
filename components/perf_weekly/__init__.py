"""Streamlit custom component: Performance Weekly Operational Report."""

from __future__ import annotations

import os

import streamlit.components.v1 as components

_BUILD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build")

_perf_weekly = components.declare_component("perf_weekly", path=_BUILD_DIR)


def perf_weekly(
    data: dict | None,
    *,
    height: int = 720,
    key: str | None = None,
) -> None:
    """Render the weekly operational dashboard."""
    _perf_weekly(data=data or {}, height=height, key=key, default=None)
