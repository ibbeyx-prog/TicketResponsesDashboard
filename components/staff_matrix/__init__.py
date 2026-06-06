"""Streamlit custom component: Multi-Staff Case Management Matrix."""

from __future__ import annotations

import os

import streamlit.components.v1 as components

_BUILD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build")

_staff_matrix = components.declare_component("staff_matrix", path=_BUILD_DIR)


def staff_matrix(
    data: dict | None,
    *,
    height: int = 620,
    key: str | None = None,
) -> str | None:
    """Render the React matrix. Returns selected ticket id when a row is confirmed."""
    return _staff_matrix(data=data or {}, height=height, key=key, default=None)
