"""NetOps Coverage Eye — dispatch console UI (dark ops theme + layout helpers)."""

from __future__ import annotations

import html
from datetime import datetime, timedelta, timezone
from collections.abc import Callable
from typing import Any

import streamlit as st

_UI_TZ_UTC5 = timezone(timedelta(hours=5))
_DISP_INSET = "10px"
_DISP_BODY_TOP = "6px"
_DISP_HEADER_H = "56px"
UI_MIN_FONT_PX = 11

# Shared design tokens (CSS variables for embedded HTML components).
_DISPATCH_VARS = f"""
:root {{
  --disp-min-font: {UI_MIN_FONT_PX}px;
  --disp-bg: #0a0f1a;
  --disp-panel: #0f1629;
  --disp-card: #141e32;
  --disp-card-hover: #1a2740;
  --disp-row-alt: #121b2e;
  --disp-row-sel: #1a2240;
  --disp-border: #243047;
  --disp-border-strong: #334766;
  --disp-text: #f0f4fc;
  --disp-muted: #9aa8c4;
  --disp-dim: #6b7a99;
  --disp-accent: #818cf8;
  --disp-accent-strong: #6366f1;
  --disp-accent-bg: #1e1b4b;
  --disp-green: #34d399;
  --disp-amber: #fbbf24;
  --disp-red: #f87171;
  --disp-purple: #a78bfa;
  --disp-header-h: {_DISP_HEADER_H};
  --disp-radius: 10px;
  --disp-radius-sm: 8px;
  --disp-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
  --disp-shadow-lg: 0 8px 24px rgba(0, 0, 0, 0.28);
}}
"""

DISPATCH_LAYOUT_RULES = f"""
{_DISPATCH_VARS}
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > section,
[data-testid="stAppViewContainer"] .main,
[data-testid="stAppViewContainer"] section.main,
[data-testid="stMain"],
[data-testid="stMain"] > div,
[data-testid="stMainBlockContainer"],
[data-testid="stBottomBlockContainer"],
[data-testid="stBottom"],
section.main,
.main .block-container {{
  background: var(--disp-bg) !important;
  color: var(--disp-text) !important;
  max-width: 100% !important;
  width: 100% !important;
}}
[data-testid="block-container"],
[data-testid="stMain"] [data-testid="block-container"],
[data-testid="stAppViewContainer"] [data-testid="block-container"],
[data-testid="stMainBlockContainer"],
.main .block-container {{
  padding-top: 0 !important;
  padding-left: 0 !important;
  padding-right: 0 !important;
  padding-bottom: 0 !important;
  max-width: 100% !important;
  width: 100% !important;
  margin-left: 0 !important;
  margin-right: 0 !important;
}}
[data-testid="stHeader"] {{ display: none !important; height: 0 !important; }}
[data-testid="stAppViewContainer"] {{ padding-top: 0 !important; }}
[data-testid="stSidebar"],
[data-testid="collapsedControl"],
button[kind="header"] {{ display: none !important; }}
html, body {{ background: var(--disp-bg) !important; overflow-x: hidden !important; }}

[data-testid="stMain"] [data-testid="element-container"] {{
  margin-bottom: 0.2rem !important;
}}
[data-testid="stMain"] [data-testid="stVerticalBlock"] {{
  gap: 0.15rem !important;
}}

[data-testid="stHorizontalBlock"] {{
  gap: 0.2rem !important;
  width: 100% !important;
}}
[data-testid="stHorizontalBlock"] > [data-testid="column"],
[data-testid="stHorizontalBlock"] > div[data-testid="column"] {{
  padding-left: 0 !important;
  padding-right: 0 !important;
  min-width: 0 !important;
}}

.disp-full-bleed {{
  width: 100% !important;
  max-width: 100% !important;
  margin-left: 0 !important;
  margin-right: 0 !important;
  box-sizing: border-box !important;
}}

/* ── Header shell ── */
div.st-key-disp_header_shell {{
  background: rgba(15, 22, 41, 0.92) !important;
  backdrop-filter: blur(10px) !important;
  -webkit-backdrop-filter: blur(10px) !important;
  border-bottom: 1px solid var(--disp-border) !important;
  box-shadow: var(--disp-shadow) !important;
  padding: 0 20px 0 {_DISP_INSET} !important;
  margin: 0 !important;
  width: 100% !important;
  max-width: 100% !important;
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
  max-height: var(--disp-header-h) !important;
  box-sizing: border-box !important;
  position: sticky !important;
  top: 0 !important;
  z-index: 999 !important;
  overflow: visible !important;
}}
div.st-key-disp_header_shell [data-testid="stVerticalBlockBorderWrapper"] {{
  position: relative !important;
  overflow: visible !important;
}}
div.st-key-disp_header_shell [data-testid="stVerticalBlockBorderWrapper"],
div.st-key-disp_header_shell [data-testid="stVerticalBlock"],
div.st-key-disp_header_shell [data-testid="stHorizontalBlock"],
div.st-key-disp_header_shell [data-testid="element-container"],
div.st-key-disp_header_shell [data-testid="column"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
  gap: 0 !important;
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
  max-height: var(--disp-header-h) !important;
  align-items: center !important;
  overflow: visible !important;
}}
div.st-key-disp_header_shell [data-testid="stMarkdownContainer"],
div.st-key-disp_header_shell [data-testid="stMarkdownContainer"] p {{
  display: flex !important;
  align-items: center !important;
  margin: 0 !important;
  padding: 0 !important;
  line-height: 1 !important;
}}
div.st-key-disp_header_shell p.disp-brand {{
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
}}
div.st-key-disp_header_shell [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-wrap: nowrap !important;
  width: 100% !important;
  align-items: center !important;
}}
div.st-key-disp_header_shell > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"],
div.st-key-disp_header_shell > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > div[data-testid="column"] {{
  flex-wrap: nowrap !important;
}}
div.st-key-disp_header_shell > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child,
div.st-key-disp_header_shell [data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child {{
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  flex: 1 1 auto !important;
  min-width: 0 !important;
  overflow: visible !important;
}}
div.st-key-disp_header_left,
div.st-key-disp_header_left [data-testid="stVerticalBlockBorderWrapper"],
div.st-key-disp_header_left [data-testid="stVerticalBlock"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
  width: 100% !important;
  overflow: visible !important;
  display: flex !important;
  align-items: center !important;
}}
div.st-key-disp_header_left [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  width: 100% !important;
  overflow: visible !important;
  gap: 0 !important;
}}
div.st-key-disp_header_left [data-testid="column"]:first-child {{
  flex: 0 0 auto !important;
  width: auto !important;
  overflow: visible !important;
  display: flex !important;
  align-items: center !important;
  align-self: stretch !important;
  height: var(--disp-header-h) !important;
}}
div.st-key-disp_header_left [data-testid="column"]:first-child [data-testid="element-container"],
div.st-key-disp_header_left [data-testid="column"]:first-child [data-testid="stMarkdownContainer"] {{
  display: flex !important;
  align-items: center !important;
  height: 100% !important;
  min-height: var(--disp-header-h) !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_header_left [data-testid="column"]:last-child {{
  flex: 1 1 auto !important;
  width: auto !important;
  min-width: 0 !important;
  overflow: visible !important;
}}
div.st-key-disp_header_shell [data-testid="element-container"] {{
  display: flex !important;
  align-items: center !important;
  margin-bottom: 0 !important;
  min-height: var(--disp-header-h) !important;
}}
div.st-key-disp_main_nav_tabs,
div.st-key-disp_main_nav_tabs [data-testid="stVerticalBlockBorderWrapper"],
div.st-key-disp_main_nav_tabs [data-testid="stVerticalBlock"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
  width: 100% !important;
  overflow: visible !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
}}
div.st-key-disp_main_nav_tabs [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-direction: row !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  justify-content: center !important;
  width: 100% !important;
  gap: 0 !important;
  overflow: visible !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button,
div.st-key-disp_main_nav_tabs .stButton > button[kind="primary"],
div.st-key-disp_main_nav_tabs .stButton > button[kind="secondary"],
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-primary"],
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-secondary"] {{
  background: transparent !important;
  background-color: transparent !important;
  background-image: none !important;
  border: none !important;
  border-bottom: 2px solid transparent !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  color: var(--disp-dim) !important;
  font-size: 13px !important;
  font-weight: 500 !important;
  letter-spacing: 0.01em !important;
  padding: 0 16px !important;
  min-height: var(--disp-header-h) !important;
  height: var(--disp-header-h) !important;
  line-height: 1.1 !important;
  white-space: nowrap !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button[kind="primary"],
div.st-key-disp_main_nav_tabs .stButton > button[data-testid="baseButton-primary"],
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-primary"] {{
  color: var(--disp-text) !important;
  font-weight: 600 !important;
  border-bottom-color: var(--disp-accent-strong) !important;
  background: transparent !important;
  background-color: transparent !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button:hover,
div.st-key-disp_main_nav_tabs .stButton > button[kind="secondary"]:hover,
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-secondary"]:hover {{
  color: var(--disp-muted) !important;
  background: transparent !important;
  background-color: transparent !important;
  border-color: transparent !important;
  border-bottom-color: transparent !important;
  box-shadow: none !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button[kind="primary"]:hover,
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-primary"]:hover {{
  color: var(--disp-text) !important;
  border-bottom-color: var(--disp-accent) !important;
  background: transparent !important;
  background-color: transparent !important;
}}
div.st-key-disp_main_nav_tabs [data-testid="column"] {{
  flex: 0 0 auto !important;
  width: auto !important;
  min-width: 0 !important;
  overflow: visible !important;
  padding: 0 !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
}}
div.st-key-disp_main_nav_tabs [data-testid="element-container"] {{
  margin: 0 !important;
  padding: 0 !important;
  width: auto !important;
  display: flex !important;
  justify-content: flex-start !important;
  align-items: center !important;
}}
div.st-key-disp_main_nav_tabs .stButton {{
  margin: 0 !important;
  width: auto !important;
  display: flex !important;
  justify-content: flex-start !important;
}}
div.st-key-disp_main_nav_tabs [class*="st-key-_dash_nav_tab_csm"] .stButton > button {{
  padding-left: 0 !important;
}}
div.st-key-disp_header_shell > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child,
div.st-key-disp_header_shell [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {{
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-end !important;
  flex: 1 1 auto !important;
  min-width: 0 !important;
  overflow: visible !important;
}}
/* Middle band: nav + clock + operator + actions on one 56px midline */
div.st-key-disp_header_mid,
div.st-key-disp_header_mid [data-testid="stVerticalBlockBorderWrapper"],
div.st-key-disp_header_mid [data-testid="stVerticalBlock"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
  width: 100% !important;
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
  max-height: var(--disp-header-h) !important;
  overflow: visible !important;
  display: flex !important;
  align-items: center !important;
}}
div.st-key-disp_header_mid [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  width: 100% !important;
  height: var(--disp-header-h) !important;
  gap: 0 !important;
}}
div.st-key-disp_header_mid > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > div[data-testid="column"] {{
  display: flex !important;
  align-items: center !important;
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
  max-height: var(--disp-header-h) !important;
}}
div.st-key-disp_header_mid > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child {{
  flex: 0 0 auto !important;
  justify-content: flex-start !important;
}}
div.st-key-disp_header_mid > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {{
  flex: 1 1 auto !important;
  justify-content: flex-end !important;
}}
div.st-key-disp_header_right,
div.st-key-disp_header_right [data-testid="stVerticalBlockBorderWrapper"],
div.st-key-disp_header_right [data-testid="stVerticalBlock"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
  width: 100% !important;
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
  display: flex !important;
  align-items: center !important;
  justify-content: flex-end !important;
}}
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  justify-content: flex-end !important;
  width: 100% !important;
  height: var(--disp-header-h) !important;
  gap: 10px !important;
}}
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] > div[data-testid="column"] {{
  flex: 0 0 auto !important;
  width: auto !important;
  min-width: 0 !important;
  max-width: none !important;
  height: var(--disp-header-h) !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  padding: 0 !important;
  overflow: visible !important;
}}
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {{
  padding-right: 4px !important;
  flex: 0 0 auto !important;
  min-width: 52px !important;
}}
div.st-key-disp_header_settings [data-testid="stPopover"] > button {{
  min-width: 48px !important;
  width: auto !important;
  padding: 0 10px !important;
  box-sizing: border-box !important;
}}
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-child(2),
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-child(3),
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-child(4) {{
  border-left: 1px solid var(--disp-border) !important;
  padding-left: 12px !important;
}}
div.disp-header-mid-item {{
  display: flex !important;
  align-items: center !important;
  height: var(--disp-header-h) !important;
  gap: 10px;
  min-width: 0;
}}
.disp-brand-stack {{
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 2px;
  padding-right: 18px;
  margin-right: 4px;
  border-right: 1px solid var(--disp-border);
  height: var(--disp-header-h);
  min-width: 0;
}}
.disp-brand-mark {{
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}}
.disp-brand-kicker {{
  font-size: 10px;
  font-weight: 700;
  color: var(--disp-accent);
  letter-spacing: 0.14em;
  text-transform: uppercase;
  line-height: 1;
  white-space: nowrap;
}}
.disp-brand-sep {{
  width: 1px;
  height: 12px;
  background: var(--disp-border-strong);
  flex-shrink: 0;
}}
.disp-brand-title {{
  font-size: 15px;
  font-weight: 600;
  color: var(--disp-text);
  letter-spacing: -0.02em;
  line-height: 1.1;
  white-space: nowrap;
}}
.disp-brand-sub {{
  font-size: 10px;
  font-weight: 500;
  color: var(--disp-dim);
  letter-spacing: 0.04em;
  line-height: 1;
  white-space: nowrap;
}}
.disp-header-clock-pill {{
  display: inline-flex;
  align-items: center;
  gap: 8px;
  height: 32px;
  padding: 0 12px;
  border-radius: 999px;
  background: var(--disp-card);
  border: 1px solid var(--disp-border);
  box-sizing: border-box;
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}}
.disp-header-clock-date {{
  font-size: 11px;
  font-weight: 500;
  color: var(--disp-muted);
  letter-spacing: 0.01em;
}}
.disp-header-clock-sep {{
  width: 1px;
  height: 14px;
  background: var(--disp-border-strong);
  flex-shrink: 0;
}}
.disp-header-clock-time {{
  font-size: 12px;
  font-weight: 600;
  color: var(--disp-text);
  letter-spacing: 0.02em;
}}
.disp-header-clock-tz {{
  font-size: 9px;
  font-weight: 600;
  color: var(--disp-dim);
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding-left: 2px;
}}
.disp-header-operator {{
  gap: 10px !important;
  min-width: 0;
  max-width: 100%;
}}
.disp-header-avatar {{
  width: 30px;
  height: 30px;
  border-radius: 50%;
  border: 1px solid;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.02em;
  flex-shrink: 0;
}}
.disp-header-operator-meta {{
  display: flex;
  flex-direction: column;
  gap: 3px;
  min-width: 0;
}}
.disp-header-operator-name {{
  font-size: 13px;
  font-weight: 500;
  color: var(--disp-text);
  line-height: 1.1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.disp-header-role-badge {{
  display: inline-flex;
  align-items: center;
  width: fit-content;
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  line-height: 1;
  padding: 3px 6px;
  border-radius: 999px;
  border: 1px solid transparent;
}}
.disp-header-role-admin {{
  color: #86efac;
  background: rgba(20, 83, 45, 0.35);
  border-color: #166534;
}}
.disp-header-role-operator {{
  color: #a5b4fc;
  background: rgba(49, 46, 129, 0.35);
  border-color: #4338ca;
}}
.disp-header-role-unverified {{
  color: #fcd34d;
  background: rgba(120, 53, 15, 0.35);
  border-color: #92400e;
}}
div.st-key-disp_header_lookup .stButton > button,
div.st-key-disp_header_settings [data-testid="stPopover"] > button {{
  background: var(--disp-card) !important;
  border: 1px solid var(--disp-border) !important;
  color: var(--disp-muted) !important;
  font-size: 12px !important;
  font-weight: 500 !important;
  height: 32px !important;
  min-height: 32px !important;
  max-height: 32px !important;
  padding: 0 12px !important;
  border-radius: var(--disp-radius-sm) !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  margin: 0 !important;
  letter-spacing: 0.01em !important;
}}
div.st-key-disp_header_lookup .stButton > button:hover,
div.st-key-disp_header_settings [data-testid="stPopover"] > button:hover {{
  border-color: var(--disp-border-strong) !important;
  color: var(--disp-text) !important;
  background: var(--disp-card-hover) !important;
}}
div.st-key-disp_header_lookup,
div.st-key-disp_header_settings,
div.st-key-disp_header_lookup [data-testid="stVerticalBlock"],
div.st-key-disp_header_settings [data-testid="stVerticalBlock"],
div.st-key-disp_header_lookup [data-testid="element-container"],
div.st-key-disp_header_settings [data-testid="element-container"] {{
  display: flex !important;
  align-items: center !important;
  height: var(--disp-header-h) !important;
  min-height: var(--disp-header-h) !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_header_shell [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child [data-testid="element-container"] {{
  width: auto !important;
  flex: 0 0 auto !important;
}}
div.st-key-disp_header_right [data-testid="stPopover"] {{
  min-width: max-content !important;
  flex-shrink: 0 !important;
  margin: 0 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopover"] {{
  min-width: max-content !important;
  flex-shrink: 0 !important;
  margin: 0 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopover"] > button,
div.st-key-disp_header_right [data-testid="stPopover"] > button {{
  font-size:13px !important;
  font-weight: 400 !important;
  letter-spacing: .01em !important;
  color: #4a5a7a !important;
  white-space: nowrap !important;
  padding: 0 12px !important;
  min-height: 34px !important;
  height: 34px !important;
  min-width: max-content !important;
  border: 0.5px solid #1a2035 !important;
  border-radius: 6px !important;
  background: transparent !important;
  line-height: 1.2 !important;
  box-shadow: none !important;
}}
div.st-key-disp_header_shell [data-testid="stPopover"] > button:hover,
div.st-key-disp_header_right [data-testid="stPopover"] > button:hover {{
  border-color: #2a3a5a !important;
  color: #8a9ac0 !important;
  background: #0d1220 !important;
}}
div.st-key-disp_header_lookup,
div.st-key-disp_header_settings {{
  width: auto !important;
  max-width: fit-content !important;
  flex: 0 0 auto !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_header_lookup [data-testid="element-container"],
div.st-key-disp_header_settings [data-testid="element-container"] {{
  width: auto !important;
  max-width: fit-content !important;
  flex: 0 0 auto !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_header_lookup [data-testid="stPopover"],
div.st-key-disp_header_settings [data-testid="stPopover"] {{
  width: auto !important;
  max-width: fit-content !important;
  display: inline-flex !important;
  margin: 0 !important;
  align-items: center !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"],
div.st-key-disp_header_right [data-testid="stPopoverBody"] {{
  min-width: 280px !important;
  max-width: min(420px, 92vw) !important;
  padding: 10px 12px !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"],
div.st-key-disp_header_right [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] {{
  gap: 0.35rem !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] .settings-section-label,
div.st-key-disp_header_right [data-testid="stPopoverBody"] .settings-section-label {{
  font-size: 11px !important;
  font-weight: 600 !important;
  color: #8a9ac0 !important;
  text-transform: uppercase !important;
  letter-spacing: 0.06em !important;
  margin: 0.65rem 0 0.35rem !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] .settings-range-cap,
div.st-key-disp_header_right [data-testid="stPopoverBody"] .settings-range-cap {{
  font-size: 11px !important;
  color: #4a5a7a !important;
  margin: 0 0 0.45rem !important;
  line-height: 1.35 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] div[data-testid="stRadio"] > div[role="radiogroup"],
div.st-key-disp_header_right [data-testid="stPopoverBody"] div[data-testid="stRadio"] > div[role="radiogroup"] {{
  flex-direction: column !important;
  gap: 2px !important;
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] div[data-testid="stRadio"] label,
div.st-key-disp_header_right [data-testid="stPopoverBody"] div[data-testid="stRadio"] label {{
  padding: 5px 8px !important;
  margin: 0 !important;
  font-size: 13px !important;
  color: #8a9ac0 !important;
  border-radius: 4px !important;
  min-height: unset !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] div[data-testid="stRadio"] label[data-checked="true"],
div.st-key-disp_header_right [data-testid="stPopoverBody"] div[data-testid="stRadio"] label[data-checked="true"] {{
  color: #e2e8f8 !important;
  background: #121a2a !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] hr,
div.st-key-disp_header_right [data-testid="stPopoverBody"] hr {{
  margin: 0.5rem 0 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] [data-testid="stExpander"],
div.st-key-disp_header_right [data-testid="stPopoverBody"] [data-testid="stExpander"] {{
  border: 0.5px solid #1a2035 !important;
  border-radius: 6px !important;
  background: #0d1220 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"] [data-testid="stExpander"] summary,
div.st-key-disp_header_right [data-testid="stPopoverBody"] [data-testid="stExpander"] summary {{
  font-size: 13px !important;
  color: #8a9ac0 !important;
}}

/* ── Body — content inset below header ── */
div.st-key-disp_csm_body {{
  padding: {_DISP_BODY_TOP} {_DISP_INSET} 10px {_DISP_INSET} !important;
  width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
}}
div.st-key-disp_csm_body [data-testid="stVerticalBlockBorderWrapper"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
}}

div.st-key-disp_csm_body [data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child {{
  border-right: 0.5px solid var(--disp-border) !important;
  padding-right: 10px !important;
  min-width: 196px !important;
  max-width: 220px !important;
  flex: 0 0 200px !important;
}}
div.st-key-disp_csm_body [data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-child(2) {{
  padding-left: 8px !important;
  padding-right: 8px !important;
  flex: 1 1 auto !important;
  min-width: 0 !important;
}}
div.st-key-disp_csm_body [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {{
  border-left: 0.5px solid var(--disp-border) !important;
  padding-left: 10px !important;
  min-width: 280px !important;
  max-width: 320px !important;
  flex: 0 0 300px !important;
}}
div.st-key-disp_right_rail {{
  background: var(--disp-panel) !important;
  border: 1px solid var(--disp-border) !important;
  border-radius: var(--disp-radius) !important;
  box-shadow: var(--disp-shadow) !important;
  overflow-x: hidden !important;
  overflow-y: auto !important;
  min-height: calc(100vh - var(--disp-header-h) - 20px) !important;
}}
div.st-key-disp_right_rail [data-testid="stVerticalBlockBorderWrapper"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
}}
div.st-key-disp_right_rail [data-testid="stTabs"] {{
  margin: 0 !important;
}}
div.st-key-disp_right_rail .stTabs [data-baseweb="tab-list"] {{
  gap: 0 !important;
  padding: 0 10px !important;
  border-bottom: 0.5px solid var(--disp-border) !important;
  background: #080b14 !important;
}}
div.st-key-disp_right_rail .stTabs [data-baseweb="tab"] {{
  font-size: 13px !important;
  font-weight: 400 !important;
  color: #4a5a7a !important;
  padding: 10px 14px 8px !important;
  background: transparent !important;
  border-bottom: 2px solid transparent !important;
}}
div.st-key-disp_right_rail .stTabs [aria-selected="true"] {{
  color: #e2e8f8 !important;
  font-weight: 500 !important;
  border-bottom-color: var(--disp-accent) !important;
}}
div.st-key-disp_right_rail [data-testid="stTabContent"] {{
  padding: 0 !important;
}}
div.st-key-disp_right_rail div.st-key-disp_assign_panel,
div.st-key-disp_right_rail div.st-key-disp_sales_assign_panel {{
  margin-bottom: 0 !important;
  border: none !important;
  border-radius: 0 !important;
  background: transparent !important;
  padding: 10px 12px 14px !important;
}}
div.st-key-disp_right_rail div.st-key-disp_detail_panel {{
  padding: 10px 12px 14px !important;
  min-height: 280px !important;
  max-height: calc(100vh - var(--disp-header-h) - 52px) !important;
  overflow-y: auto !important;
  overflow-x: hidden !important;
  overscroll-behavior: contain !important;
}}
.disp-case-activity-scroll {{
  overflow-y: auto !important;
  overflow-x: hidden !important;
  overscroll-behavior: contain !important;
  padding-right: 4px;
  margin-bottom: 4px;
}}
.disp-case-activity-scroll--comments {{
  max-height: 220px !important;
}}
.disp-case-activity-scroll--photos {{
  max-height: 160px !important;
}}
.disp-case-activity-more {{
  font-size: 11px !important;
  color: #2a3a5a !important;
  margin: 6px 0 0 !important;
}}
.disp-case-photo-link {{
  display: flex !important;
  align-items: center !important;
  gap: 8px !important;
  background: #0d1220 !important;
  border: 0.5px solid #1a2035 !important;
  border-radius: 4px !important;
  padding: 7px 8px !important;
  text-decoration: none !important;
}}
.disp-case-photo-link:hover {{
  border-color: #2a3a5a !important;
}}
.disp-case-photo-icon {{
  flex-shrink: 0;
  width: 28px;
  height: 28px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  background: #121a2a;
  font-size: 14px;
}}
.disp-case-photo-body {{
  min-width: 0;
  flex: 1;
}}
.disp-case-photo-title {{
  display: block;
  font-size: 13px;
  font-weight: 500;
  color: #3b82f6 !important;
}}
.disp-case-photo-meta {{
  display: block;
  font-size: 11px;
  color: #2a3a5a !important;
  margin-top: 2px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
div.st-key-disp_right_rail div.st-key-disp_assign_panel .disp-mode-caption {{
  font-size: 11px !important;
  color: #4a5a7a !important;
  margin: 0 0 10px 0 !important;
  line-height: 1.4 !important;
}}
div.st-key-disp_sidebar_inner {{
  background: var(--disp-panel) !important;
  border: 1px solid var(--disp-border) !important;
  border-radius: var(--disp-radius) !important;
  padding: 12px 10px 10px !important;
  box-sizing: border-box !important;
  box-shadow: var(--disp-shadow) !important;
}}
div.st-key-disp_sidebar_queues {{
  margin-top: 8px !important;
  margin-bottom: 4px !important;
  border-top: 1px solid var(--disp-border) !important;
  padding-top: 10px !important;
}}
div.st-key-disp_sidebar_queues [data-testid="stVerticalBlock"] {{
  gap: 5px !important;
}}
div.st-key-disp_sidebar_queues .stButton {{
  margin: 0 !important;
  width: 100% !important;
}}
.disp-today-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin: 0 0 2px 0;
  padding: 0;
}}
.disp-today-strip {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  margin: 0 0 2px 0;
  padding: 0;
}}
.disp-today-cell {{
  min-width: 0;
  background: var(--disp-card);
  border: 1px solid var(--disp-border);
  border-radius: var(--disp-radius-sm);
  padding: 10px 10px 8px;
  box-sizing: border-box;
  text-align: left;
}}
.disp-today-label {{
  font-size: 10px;
  font-weight: 600;
  color: var(--disp-dim);
  line-height: 1.2;
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}}
.disp-today-value {{
  font-size: 20px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  line-height: 1.1;
}}

div.st-key-disp_perf_body {{
  padding: {_DISP_BODY_TOP} {_DISP_INSET} 10px {_DISP_INSET} !important;
  width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
}}
div.st-key-disp_perf_body [data-testid="stVerticalBlockBorderWrapper"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
}}
div.st-key-disp_perf_body [data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child {{
  border-right: 0.5px solid var(--disp-border) !important;
  padding-right: 10px !important;
  min-width: 212px !important;
}}
div.st-key-disp_perf_body [data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-child(2) {{
  padding-left: 4px !important;
  padding-right: 4px !important;
}}
div.st-key-disp_perf_body [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {{
  border-left: 0.5px solid var(--disp-border) !important;
  padding-left: 10px !important;
}}
div.st-key-disp_perf_body [data-testid="stExpander"] {{
  margin: 8px 0 0 !important;
}}
div.st-key-disp_perf_body [data-testid="stExpander"] details {{
  border: 0.5px solid #1a2035 !important;
  border-radius: 4px !important;
  background: #0d1220 !important;
  overflow: hidden;
  margin-bottom: 6px !important;
}}
div.st-key-disp_perf_body [data-testid="stExpander"] summary {{
  color: #8a9ac0 !important;
  font-size: 13px !important;
  font-weight: 500 !important;
  padding: 7px 8px !important;
  min-height: 0 !important;
  display: flex !important;
  align-items: center !important;
  list-style: none !important;
  cursor: pointer;
  box-shadow: none !important;
}}
div.st-key-disp_perf_body [data-testid="stExpander"] summary:hover {{
  color: #e2e8f8 !important;
  background: #121a2a !important;
}}
div.st-key-disp_perf_body [data-testid="stExpander"] details[open] > div {{
  padding: 4px 6px 8px !important;
  border-top: 0.5px solid #1a2035 !important;
}}

div.st-key-disp_sidebar {{
  border-right: none !important;
  padding-right: 0 !important;
  min-height: calc(100vh - var(--disp-header-h) - 8px) !important;
}}
div.st-key-disp_detail_panel {{
  border-left: none !important;
  padding-left: 0 !important;
}}
div.st-key-disp_right_rail div.st-key-disp_detail_panel [data-testid="stVerticalBlock"] {{
  gap: 0.75rem !important;
}}
div.st-key-disp_detail_panel [data-testid="element-container"] {{
  overflow: visible !important;
  margin-bottom: 0.35rem !important;
}}
div.st-key-disp_detail_panel [data-testid="stMarkdownContainer"] {{
  overflow: visible !important;
}}
div.st-key-disp_detail_panel [data-testid="stMarkdownContainer"] p {{
  margin: 0 0 6px 0 !important;
  line-height: 1.35 !important;
}}
div.st-key-disp_detail_panel [data-testid="stImage"] {{
  margin: 4px 0 10px !important;
}}
div.st-key-disp_detail_panel [data-testid="stExpander"] {{
  margin: 10px 0 6px !important;
}}
div.st-key-disp_detail_panel [data-testid="stExpander"] details {{
  border: 0.5px solid #1a2035 !important;
  border-radius: 4px !important;
  background: #0d1220 !important;
  overflow: hidden;
  margin-bottom: 6px !important;
}}
div.st-key-disp_detail_panel [data-testid="stExpander"] summary {{
  color: #8a9ac0 !important;
  font-size: 13px !important;
  font-weight: 500 !important;
  padding: 7px 8px !important;
  min-height: 0 !important;
  display: flex !important;
  align-items: center !important;
  list-style: none !important;
  cursor: pointer;
  box-shadow: none !important;
}}
div.st-key-disp_detail_panel [data-testid="stExpander"] summary:hover {{
  color: #e2e8f8 !important;
  background: #121a2a !important;
}}
div.st-key-disp_detail_panel [data-testid="stExpander"] details[open] > div {{
  padding: 4px 6px 8px !important;
  border-top: 0.5px solid #1a2035 !important;
}}

div.st-key-disp_assign_bar,
div.st-key-disp_assign_panel,
div.st-key-disp_sales_assign_panel {{
  background: #080b14 !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  padding: 12px 14px 14px !important;
  margin-bottom: 0.75rem !important;
}}
div.st-key-disp_assign_panel [data-testid="stHorizontalBlock"],
div.st-key-disp_sales_assign_panel [data-testid="stHorizontalBlock"] {{
  gap: 0.55rem !important;
  align-items: center !important;
  flex-wrap: nowrap !important;
}}
div.st-key-disp_assign_panel [data-testid="column"],
div.st-key-disp_sales_assign_panel [data-testid="column"] {{
  display: flex !important;
  align-items: center !important;
  align-self: center !important;
  min-height: 0 !important;
}}
div.st-key-disp_assign_panel [data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"],
div.st-key-disp_sales_assign_panel [data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] {{
  gap: 0.35rem !important;
}}
div.st-key-disp_assign_panel .disp-assign-header-spacer,
div.st-key-disp_sales_assign_panel .disp-assign-header-spacer {{
  height: 10px !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_assign_panel .disp-assign-fields-spacer,
div.st-key-disp_sales_assign_panel .disp-assign-fields-spacer {{
  height: 6px !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_assign_panel [data-testid="stVerticalBlockBorderWrapper"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
}}
.disp-brand {{
  font-size: 17px;
  font-weight: 700;
  color: var(--disp-text);
  letter-spacing: -0.01em;
  text-transform: none;
  line-height: 1;
  white-space: nowrap;
  margin: 0;
  padding: 0;
  display: inline-flex;
  align-items: center;
  height: var(--disp-header-h);
  gap: 10px;
  font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
}}
.disp-brand::before {{
  content: "";
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--disp-accent-strong), var(--disp-accent));
  box-shadow: 0 0 10px rgba(99, 102, 241, 0.55);
  flex-shrink: 0;
}}
.disp-header-clock {{
  display: inline-block;
  font-size: 12px;
  font-weight: 500;
  color: var(--disp-accent);
  letter-spacing: .02em;
  background: var(--disp-accent-bg);
  border: 1px solid #312e81;
  padding: 5px 11px;
  border-radius: 999px;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
  line-height: 1.2;
}}
.disp-header-op {{
  display: inline-flex;
  align-items: center;
  gap: 7px;
  font-size: 13px;
  font-weight: 400;
  color: var(--disp-muted);
  background: transparent !important;
  white-space: nowrap;
  line-height: 1.2;
  user-select: none;
}}
.disp-header-op-dot {{
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--disp-green);
  display: inline-block;
  flex-shrink: 0;
  box-shadow: 0 0 6px rgba(52, 211, 153, 0.5);
}}
.disp-header-divider {{
  color: #2a3a5a;
  font-weight: 400;
  font-size:22px;
  margin: 0 12px 0 14px;
  text-transform: none;
  letter-spacing: 0;
  line-height: 1;
  align-self: center;
}}
.disp-header-right-cluster {{
  display: inline-flex;
  align-items: center;
  gap: 8px;
  white-space: nowrap;
  line-height: 1.2;
}}
div.st-key-disp_header_right,
div.st-key-disp_header_right [data-testid="stVerticalBlockBorderWrapper"],
div.st-key-disp_header_right [data-testid="stVerticalBlock"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
  width: 100% !important;
  display: flex !important;
  flex-direction: row !important;
  flex-wrap: nowrap !important;
  justify-content: flex-end !important;
  align-items: center !important;
  gap: 10px !important;
  overflow: visible !important;
}}
div.st-key-disp_header_right [data-testid="element-container"] {{
  margin: 0 !important;
  width: auto !important;
  padding: 0 !important;
  flex: 0 0 auto !important;
  display: flex !important;
  align-items: center !important;
}}
div.st-key-disp_header_right [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-wrap: nowrap !important;
  justify-content: flex-end !important;
  align-items: center !important;
  gap: 10px !important;
  width: fit-content !important;
  max-width: 100% !important;
  margin-left: auto !important;
}}
div.st-key-disp_header_right [data-testid="column"] {{
  flex: 0 0 auto !important;
  width: auto !important;
  min-width: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_header_right [data-testid="stMarkdownContainer"],
div.st-key-disp_header_right [data-testid="stMarkdownContainer"] p,
div.st-key-disp_header_right [data-testid="stMarkdownContainer"] span {{
  background: transparent !important;
  margin: 0 !important;
  padding: 0 !important;
  line-height: 1.2 !important;
}}
.disp-section-label {{
  font-size: var(--disp-min-font);
  font-weight: 600;
  color: #2a3a5a;
  text-transform: uppercase;
  letter-spacing: .08em;
  margin: 0 0 7px;
}}
.disp-queue-count {{
  font-size: var(--disp-min-font);
  font-weight: 400;
  color: #8a9ac0;
  font-variant-numeric: tabular-nums;
  text-align: right;
}}
[class*="st-key-disp_queue_"] .stButton > button,
[class*="st-key-sales_queue_"] .stButton > button,
[class*="st-key-uqueue_"] .stButton > button {{
  font-size: 12px !important;
  font-weight: 500 !important;
  text-align: left !important;
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  padding: 7px 10px !important;
  min-height: 32px !important;
  height: 32px !important;
  white-space: nowrap !important;
  overflow: hidden !important;
  border-radius: var(--disp-radius-sm) !important;
  width: 100% !important;
  border: 1px solid transparent !important;
  background: transparent !important;
  color: var(--disp-muted) !important;
  transition: background 0.12s, color 0.12s, border-color 0.12s, box-shadow 0.12s !important;
}}
[class*="st-key-disp_queue_"] .stButton > button:hover,
[class*="st-key-sales_queue_"] .stButton > button:hover,
[class*="st-key-uqueue_"] .stButton > button:hover {{
  background: var(--disp-card) !important;
  border-color: var(--disp-border) !important;
  color: var(--disp-text) !important;
}}
[class*="st-key-disp_queue_"] .stButton > button::before,
[class*="st-key-sales_queue_"] .stButton > button::before,
[class*="st-key-uqueue_"] .stButton > button::before {{
  content: "●" !important;
  font-size: 8px !important;
  line-height: 1 !important;
  flex: 0 0 auto !important;
  margin-right: 8px !important;
}}
[class*="st-key-disp_queue_"] .stButton > button::after,
[class*="st-key-sales_queue_"] .stButton > button::after {{
  font-size: var(--disp-min-font) !important;
  font-weight: 500 !important;
  color: var(--disp-muted) !important;
  font-variant-numeric: tabular-nums !important;
  flex: 0 0 auto !important;
  margin-left: auto !important;
  padding-left: 8px !important;
  line-height: 1 !important;
}}
[class*="st-key-uqueue_"] .stButton > button::after {{
  content: "" !important;
}}
[class*="st-key-disp_queue_"].disp-queue-active .stButton > button,
[class*="st-key-sales_queue_"].disp-queue-active .stButton > button {{
  background: var(--disp-accent-bg) !important;
  border-color: #4338ca !important;
  color: var(--disp-text) !important;
  font-weight: 500 !important;
  box-shadow: inset 3px 0 0 var(--disp-accent-strong) !important;
}}
[class*="st-key-disp_queue_"] .stButton > button[kind="primary"],
[class*="st-key-disp_queue_"] .stButton > button[data-testid="stBaseButton-primary"],
[class*="st-key-sales_queue_"] .stButton > button[kind="primary"],
[class*="st-key-sales_queue_"] .stButton > button[data-testid="stBaseButton-primary"],
[class*="st-key-uqueue_"] .stButton > button[kind="primary"],
[class*="st-key-uqueue_"] .stButton > button[data-testid="stBaseButton-primary"] {{
  background: var(--disp-accent-bg) !important;
  border-color: #4338ca !important;
  color: var(--disp-text) !important;
  font-weight: 500 !important;
  box-shadow: inset 3px 0 0 var(--disp-accent-strong) !important;
}}

[data-testid="stMain"] div[class*="st-key-_dash_main_nav"] div[role="radiogroup"],
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] {{
  background: transparent !important;
  border-bottom: none !important;
  gap: 0 !important;
  padding: 0 !important;
  margin: 0 !important;
  justify-content: flex-start !important;
  height: var(--disp-header-h) !important;
  align-items: center !important;
  flex-wrap: nowrap !important;
  width: max-content !important;
  max-width: none !important;
  overflow: visible !important;
}}
[data-testid="stMain"] div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label,
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label {{
  background: transparent !important;
  border: none !important;
  border-bottom: 2px solid transparent !important;
  border-radius: 0 !important;
  padding: 0 12px !important;
  margin: 0 !important;
  min-height: var(--disp-header-h) !important;
  height: var(--disp-header-h) !important;
  display: flex !important;
  align-items: center !important;
  cursor: pointer !important;
  box-shadow: none !important;
  flex: 0 0 auto !important;
  white-space: nowrap !important;
}}
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label:first-of-type {{
  padding-left: 0 !important;
}}
[data-testid="stMain"] div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label span,
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label span,
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label p {{
  color: #4a5a7a !important;
  font-size:14px !important;
  font-weight: 400 !important;
  margin: 0 !important;
}}
[data-testid="stMain"] div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label:has(input:checked),
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label:has(input:checked) {{
  border-bottom-color: var(--disp-accent) !important;
  box-shadow: none !important;
}}
[data-testid="stMain"] div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label:has(input:checked) span,
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label:has(input:checked) span,
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label:has(input:checked) p {{
  color: #e2e8f8 !important;
  font-weight: 500 !important;
}}
div.st-key-disp_header_shell .stButton > button {{
  font-size:13px !important;
  padding: 3px 9px !important;
  white-space: nowrap !important;
}}
div.st-key-disp_assign_panel .disp-manage-icon .stButton > button,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"] .stButton > button,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"] .stButton > button,
div.st-key-disp_sales_assign_panel .disp-manage-icon .stButton > button,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_eng"] .stButton > button,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_cat"] .stButton > button {{
  font-size: 16px !important;
  font-weight: 500 !important;
  color: #6b7280 !important;
  line-height: 1 !important;
  width: 30px !important;
  height: 30px !important;
  min-height: 30px !important;
  min-width: 30px !important;
  max-width: 30px !important;
  padding: 0 !important;
  margin: 0 !important;
  border: 0.5px solid #2a3548 !important;
  border-radius: 5px !important;
  background: #0d1220 !important;
  box-shadow: none !important;
  white-space: nowrap !important;
  letter-spacing: 0 !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  overflow: hidden !important;
}}
div.st-key-disp_assign_panel .disp-manage-icon .stButton > button:hover,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"] .stButton > button:hover,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"] .stButton > button:hover,
div.st-key-disp_sales_assign_panel .disp-manage-icon .stButton > button:hover,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_eng"] .stButton > button:hover,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_cat"] .stButton > button:hover {{
  color: #8a9ac0 !important;
  border-color: #3b465c !important;
  background: #121a2a !important;
}}
div.st-key-disp_assign_panel .disp-manage-icon,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"],
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"],
div.st-key-disp_sales_assign_panel .disp-manage-icon,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_eng"],
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_cat"] {{
  flex: 0 0 auto !important;
  width: 30px !important;
  min-width: 30px !important;
  max-width: 30px !important;
  padding: 0 !important;
  margin: 0 !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
}}
div.st-key-disp_assign_panel .disp-manage-icon .stButton,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"] .stButton,
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"] .stButton,
div.st-key-disp_sales_assign_panel .disp-manage-icon .stButton,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_eng"] .stButton,
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_cat"] .stButton {{
  width: 30px !important;
  min-width: 30px !important;
  margin: 0 !important;
}}
div.st-key-disp_assign_panel .disp-manage-icon [data-testid="stVerticalBlock"],
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"] [data-testid="stVerticalBlock"],
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"] [data-testid="stVerticalBlock"],
div.st-key-disp_sales_assign_panel .disp-manage-icon [data-testid="stVerticalBlock"],
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_eng"] [data-testid="stVerticalBlock"],
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_cat"] [data-testid="stVerticalBlock"] {{
  align-items: center !important;
  justify-content: center !important;
  min-height: 0 !important;
  width: 30px !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"] [data-testid="column"],
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"] [data-testid="column"],
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_eng"] [data-testid="column"],
div.st-key-disp_sales_assign_panel [class*="st-key-btn_manage_cat"] [data-testid="column"] {{
  flex: 0 0 auto !important;
  width: auto !important;
  min-width: 30px !important;
  max-width: 42px !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
}}
.disp-field-label-row {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 4px;
  margin: 0 0 2px;
  min-height: 14px;
  height: 14px;
}}
.disp-field-label {{
  font-size:11px;
  font-weight: 500;
  color: #4a5a7a;
  margin: 0 0 6px 0 !important;
  line-height: 1.25;
  white-space: nowrap;
  display: block !important;
  position: relative !important;
  z-index: 1 !important;
}}
div.st-key-disp_assign_panel [data-testid="stMarkdownContainer"]:has(.disp-field-label),
div.st-key-disp_sales_assign_panel [data-testid="stMarkdownContainer"]:has(.disp-field-label) {{
  margin-bottom: 0 !important;
  padding-bottom: 0 !important;
}}
div.st-key-disp_assign_panel [data-testid="stSelectbox"],
div.st-key-disp_assign_panel [data-testid="stTextInput"],
div.st-key-disp_sales_assign_panel [data-testid="stSelectbox"],
div.st-key-disp_sales_assign_panel [data-testid="stTextInput"] {{
  margin-bottom: 0 !important;
  margin-top: 0 !important;
}}
div.st-key-disp_assign_panel [data-testid="stSelectbox"] > label,
div.st-key-disp_assign_panel [data-testid="stTextInput"] > label,
div.st-key-disp_sales_assign_panel [data-testid="stSelectbox"] > label,
div.st-key-disp_sales_assign_panel [data-testid="stTextInput"] > label {{
  display: none !important;
  min-height: 0 !important;
  height: 0 !important;
  margin: 0 !important;
  padding: 0 !important;
}}
div.st-key-disp_assign_panel [data-testid="stSelectbox"] [data-baseweb="select"],
div.st-key-disp_assign_panel [data-testid="stTextInput"] input,
div.st-key-disp_sales_assign_panel [data-testid="stSelectbox"] [data-baseweb="select"],
div.st-key-disp_sales_assign_panel [data-testid="stTextInput"] input {{
  min-height: 32px !important;
  font-size:13px !important;
}}
div.st-key-disp_assign_panel div.sales-mode-toggle,
div.st-key-disp_sales_assign_panel div.sales-mode-toggle {{
  margin-bottom: 2px !important;
}}
div.st-key-disp_assign_panel div.primary-btn [data-testid="stVerticalBlock"] {{
  justify-content: flex-end !important;
  padding-top: 16px !important;
}}
div.st-key-disp_assign_panel [class*="st-key-btn_manage_eng"] [data-testid="column"],
div.st-key-disp_assign_panel [class*="st-key-btn_manage_cat"] [data-testid="column"] {{
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
}}
div.st-key-disp_assign_panel div.primary-btn .stButton > button {{
  min-height: 28px !important;
  height: 28px !important;
  padding: 0 10px !important;
  font-size:13px !important;
}}
div.st-key-disp_assign_panel div.sales-mode-toggle .stButton > button,
div.st-key-disp_sales_assign_panel div.sales-mode-toggle .stButton > button {{
  font-size:11px !important;
  min-height: 26px !important;
  height: 26px !important;
  padding: 0 8px !important;
  white-space: nowrap !important;
}}
div.st-key-disp_assign_panel div.sales-mode-toggle .stButton > button[kind="primary"],
div.st-key-disp_assign_panel div.sales-mode-toggle .stButton > button[data-testid="stBaseButton-primary"],
div.st-key-disp_sales_assign_panel div.sales-mode-toggle .stButton > button[kind="primary"],
div.st-key-disp_sales_assign_panel div.sales-mode-toggle .stButton > button[data-testid="stBaseButton-primary"] {{
  background: #1a1030 !important;
  border-color: #2a1a52 !important;
  color: #a78bfa !important;
  font-weight: 500 !important;
}}
div.st-key-disp_assign_panel div.sales-mode-toggle .stButton > button[kind="secondary"],
div.st-key-disp_assign_panel div.sales-mode-toggle .stButton > button[data-testid="stBaseButton-secondary"],
div.st-key-disp_sales_assign_panel div.sales-mode-toggle .stButton > button[kind="secondary"],
div.st-key-disp_sales_assign_panel div.sales-mode-toggle .stButton > button[data-testid="stBaseButton-secondary"] {{
  background: transparent !important;
  border-color: #1a2035 !important;
  color: #4a5a7a !important;
}}
div.st-key-disp_assign_panel div.sales-btn .stButton > button,
div.st-key-disp_sales_assign_panel div.sales-btn .stButton > button {{
  background: #1a1030 !important;
  border-color: #2a1a52 !important;
  color: #a78bfa !important;
  font-weight: 500 !important;
  min-height: 28px !important;
  height: 28px !important;
  font-size:13px !important;
}}
div.st-key-disp_assign_panel div.sales-btn .stButton > button:hover,
div.st-key-disp_sales_assign_panel div.sales-btn .stButton > button:hover {{
  background: #221440 !important;
}}
div.st-key-disp_assign_panel div.sales-btn [data-testid="stVerticalBlock"],
div.st-key-disp_sales_assign_panel div.sales-btn [data-testid="stVerticalBlock"] {{
  padding-top: 22px !important;
}}
div.st-key-disp_assign_panel .assign-id-status {{
  font-size: 11px !important;
  margin: 2px 0 8px !important;
  line-height: 1.35 !important;
}}
div.st-key-disp_assign_panel .assign-id-muted {{ color: #4a5a7a !important; }}
div.st-key-disp_assign_panel .assign-id-warn {{ color: #b8954f !important; }}
div.st-key-disp_assign_panel .assign-id-danger {{ color: #c06868 !important; }}
div.st-key-disp_assign_panel .assign-id-ok {{ color: #5a9c7a !important; }}
div.st-key-disp_assign_panel .assign-impact {{
  font-size: 12px !important;
  line-height: 1.45 !important;
  margin: 8px 0 10px !important;
  padding: 9px 11px !important;
  border-radius: 6px !important;
  border: 0.5px solid #1a2035 !important;
  background: #0d1220 !important;
  color: #8a9ac0 !important;
}}
div.st-key-disp_assign_panel .assign-impact-warn {{
  border-color: #3a2020 !important;
  background: #140d0d !important;
  color: #c06868 !important;
}}
div.st-key-disp_assign_panel .assign-impact-info {{
  border-color: #1a2035 !important;
}}
div.st-key-disp_assign_panel .assign-eng-picker .stButton > button {{
  font-size: 12px !important;
  font-weight: 400 !important;
  justify-content: flex-start !important;
  text-align: left !important;
  min-height: 30px !important;
  height: auto !important;
  padding: 6px 10px !important;
  white-space: normal !important;
}}
div.st-key-disp_assign_panel .assign-eng-picker .stButton > button[kind="primary"],
div.st-key-disp_assign_panel .assign-eng-picker .stButton > button[data-testid="stBaseButton-primary"] {{
  background: #0d1e3a !important;
  border-color: #1a3460 !important;
  color: #5b7fb5 !important;
}}
div.st-key-disp_assign_panel .assign-eng-selected {{
  font-size: 11px !important;
  color: #5b7fb5 !important;
  margin: 4px 0 8px !important;
}}
div.st-key-disp_assign_panel div.disp-manage-btn .stButton > button {{
  font-size:11px !important;
  font-weight: 400 !important;
  color: #4a5a7a !important;
  min-height: 28px !important;
  height: 28px !important;
  padding: 0 6px !important;
  border: 0.5px solid #1a2035 !important;
  border-radius: 4px !important;
  background: transparent !important;
  white-space: nowrap !important;
}}
div.st-key-disp_assign_panel div.disp-manage-btn .stButton > button:hover {{
  color: #8a9ac0 !important;
  border-color: #2a3a5a !important;
  background: #0d1220 !important;
}}
div.st-key-disp_assign_panel div.disp-manage-btn .stButton,
div.st-key-disp_assign_panel div.disp-manage-btn [data-testid="stVerticalBlock"] {{
  width: 100% !important;
  min-width: 0 !important;
}}

div.st-key-disp_sales_assign_panel {{
  background: #080b14 !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  padding: 8px 10px !important;
  margin-bottom: 0.5rem !important;
}}
div.st-key-disp_sales_assign_panel [data-testid="stVerticalBlockBorderWrapper"] {{
  background: transparent !important;
  border: none !important;
  padding: 0 !important;
  margin: 0 !important;
}}
div.st-key-disp_sales_assign_panel [data-testid="stSelectbox"] > label,
div.st-key-disp_sales_assign_panel [data-testid="stTextInput"] > label {{
  display: none !important;
}}
div.st-key-disp_sales_assign_panel div.disp-manage-btn .stButton > button {{
  font-size:11px !important;
  font-weight: 400 !important;
  color: #4a5a7a !important;
  min-height: 28px !important;
  height: 28px !important;
  padding: 0 6px !important;
  border: 0.5px solid #1a2035 !important;
  border-radius: 4px !important;
  background: transparent !important;
  white-space: nowrap !important;
}}
div.st-key-disp_sales_assign_panel div.disp-manage-btn .stButton > button:hover {{
  color: #8a9ac0 !important;
  border-color: #2a3a5a !important;
  background: #0d1220 !important;
}}
div.st-key-disp_sales_assign_panel div.disp-manage-btn .stButton,
div.st-key-disp_sales_assign_panel div.disp-manage-btn [data-testid="stVerticalBlock"] {{
  width: 100% !important;
  min-width: 0 !important;
}}

/* Ticket / sales table — spaced card rows + action buttons */
div.st-key-disp_ticket_table {{
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  padding: 0 !important;
  margin-top: 8px !important;
  box-sizing: border-box !important;
  overflow: visible !important;
}}
div.st-key-disp_ticket_table > [data-testid="stVerticalBlock"] {{
  gap: 6px !important;
}}
div[class*="st-key-disp_ticket_row_"] {{
  background: var(--disp-card) !important;
  border: 1px solid var(--disp-border) !important;
  border-radius: var(--disp-radius-sm) !important;
  padding: 0 8px !important;
  margin: 0 !important;
  box-shadow: var(--disp-shadow) !important;
  transition: background 0.12s, border-color 0.12s, box-shadow 0.12s !important;
}}
div[class*="st-key-disp_ticket_row_"]:hover {{
  background: var(--disp-card-hover) !important;
  border-color: var(--disp-border-strong) !important;
}}
div[class*="st-key-disp_ticket_row_"]:has(.disp-row-selected) {{
  background: var(--disp-row-sel) !important;
  border-color: #4338ca !important;
  box-shadow: inset 3px 0 0 var(--disp-accent-strong), var(--disp-shadow) !important;
}}
.disp-row-selected {{
  display: none !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="element-container"] {{
  margin: 0 !important;
  padding: 0 !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="stHorizontalBlock"] {{
  align-items: center !important;
  flex-wrap: nowrap !important;
  gap: 4px !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="column"] {{
  display: flex !important;
  align-items: center !important;
  align-self: center !important;
  min-height: 36px !important;
  max-height: 36px !important;
  padding: 0 !important;
  overflow: hidden !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="stMarkdownContainer"] {{
  width: 100% !important;
  margin: 0 !important;
}}
div[class*="st-key-disp_row_actions_"] {{
  display: flex !important;
  flex-direction: row !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  justify-content: center !important;
  gap: 0 !important;
  width: 100% !important;
  min-height: 36px !important;
  max-height: 36px !important;
  padding: 0 !important;
  margin: 0 !important;
  box-sizing: border-box !important;
  border: none !important;
  border-radius: 0 !important;
  background: transparent !important;
  overflow: hidden !important;
}}
div[class*="st-key-disp_row_actions_"] [data-testid="stVerticalBlock"] {{
  flex-direction: row !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  gap: 0 !important;
  width: auto !important;
  max-width: 100% !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] {{
  flex: 0 0 32px !important;
  width: 32px !important;
  min-width: 32px !important;
  max-width: 32px !important;
  margin: 0 !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton > button,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button {{
  font-size: 14px !important;
  font-weight: 600 !important;
  color: var(--disp-dim) !important;
  line-height: 1 !important;
  width: 32px !important;
  height: 32px !important;
  min-height: 32px !important;
  min-width: 32px !important;
  max-width: 32px !important;
  padding: 0 !important;
  margin: 0 !important;
  border: 1px solid var(--disp-border) !important;
  border-radius: var(--disp-radius-sm) !important;
  background: var(--disp-panel) !important;
  box-shadow: none !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  overflow: hidden !important;
  white-space: nowrap !important;
}}
div[class*="st-key-disp_ticket_row_"]:has(.disp-row-selected) div[class*="st-key-disp_row_actions_"] .stButton > button,
div[class*="st-key-disp_ticket_row_"]:has(.disp-row-selected) div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button {{
  color: var(--disp-accent) !important;
  border-color: #4338ca !important;
  background: var(--disp-accent-bg) !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton > button:hover,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button:hover {{
  color: var(--disp-text) !important;
  border-color: var(--disp-border-strong) !important;
  background: var(--disp-card-hover) !important;
}}
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button svg,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button [data-testid*="Icon"] {{
  display: none !important;
}}
div[class*="st-key-disp_row_actions_"] [data-testid="stVerticalBlock"] {{
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-end !important;
  gap: 10px !important;
  min-height: 0 !important;
  padding: 0 !important;
  margin: 0 !important;
}}

[data-testid="stVerticalBlockBorderWrapper"],
div[data-testid="stExpander"],
[data-testid="stPopoverBody"] {{
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
}}
[data-testid="stDataFrame"],
[data-testid="stAlert"] {{
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
}}
.disp-refresh-caption {{
  font-size:13px;
  font-weight: 400;
  color: var(--disp-dim);
  margin: 0 0 0.75rem;
}}

[data-testid="element-container"]:has(div.st-key-disp_header_shell),
[data-testid="element-container"]:has(div.st-key-disp_csm_body) {{
  margin-top: 0 !important;
  margin-bottom: 0 !important;
  padding: 0 !important;
  max-width: 100% !important;
}}

@media (min-width: 768px) {{
  [data-testid="block-container"],
  [data-testid="stMain"] [data-testid="block-container"],
  [data-testid="stMainBlockContainer"] {{
    max-width: 100% !important;
    padding-left: 0 !important;
    padding-right: 0 !important;
  }}
}}

/* Header nav — hide Streamlit radio circles only (keep tab text visible) */
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] [data-baseweb="radio"],
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] [data-testid="stRadio"] [data-baseweb="radio"],
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] input[type="radio"] {{
  display: none !important;
  width: 0 !important;
  height: 0 !important;
  overflow: hidden !important;
  margin: 0 !important;
  padding: 0 !important;
  opacity: 0 !important;
  pointer-events: none !important;
  position: absolute !important;
}}
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label [data-testid="stMarkdownContainer"],
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label span,
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label p {{
  display: inline !important;
  visibility: visible !important;
  width: auto !important;
  height: auto !important;
  overflow: visible !important;
  opacity: 1 !important;
}}
div.st-key-disp_header_shell div[class*="st-key-_dash_main_nav"] div[role="radiogroup"] label {{
  gap: 0 !important;
}}

/* Ticket table typography + pager + case-type chips */
.disp-col-head {{
  font-size: 11px !important;
  font-weight: 600 !important;
  color: var(--disp-dim) !important;
  text-transform: uppercase !important;
  letter-spacing: 0.06em !important;
  margin: 0 !important;
}}
.disp-ticket-num {{
  font-size: 13px !important;
  font-weight: 600 !important;
  color: var(--disp-text) !important;
  font-variant-numeric: tabular-nums !important;
}}
.disp-table-empty {{
  padding: 44px 24px !important;
  text-align: center !important;
  color: var(--disp-dim) !important;
  font-size: 14px !important;
  font-weight: 400 !important;
  background: var(--disp-panel) !important;
  border: 1px dashed var(--disp-border) !important;
  border-radius: var(--disp-radius) !important;
  margin-top: 8px !important;
}}
div.st-key-disp_table_header {{
  margin-top: 8px !important;
  padding: 0 4px 8px !important;
  background: transparent !important;
  border: none !important;
  border-bottom: 1px solid var(--disp-border) !important;
  border-radius: 0 !important;
}}
table.disp-html-table {{
  width: 100% !important;
  border-collapse: collapse !important;
  table-layout: fixed !important;
  margin-top: 8px !important;
  font-size: 13px !important;
}}
table.disp-html-table thead th {{
  font-size: 11px !important;
  font-weight: 600 !important;
  color: var(--disp-dim) !important;
  text-transform: uppercase !important;
  letter-spacing: 0.06em !important;
  text-align: left !important;
  padding: 8px 8px 10px !important;
  border-bottom: 1px solid var(--disp-border) !important;
  white-space: nowrap !important;
  overflow: hidden !important;
}}
table.disp-html-table tbody td.disp-html-notes {{
  max-width: 0 !important;
}}
table.disp-html-table tbody td.disp-html-notes span {{
  display: block !important;
  overflow: hidden !important;
  text-overflow: ellipsis !important;
  white-space: nowrap !important;
  min-width: 0 !important;
}}
table.disp-html-table tbody td {{
  padding: 5px 8px !important;
  height: 36px !important;
  max-height: 36px !important;
  vertical-align: middle !important;
  border-bottom: 0.5px solid var(--disp-border) !important;
  overflow: hidden !important;
  text-overflow: ellipsis !important;
  white-space: nowrap !important;
}}
table.disp-html-table tbody tr:nth-child(even) td {{
  background: var(--disp-row-alt) !important;
}}
table.disp-html-table tbody tr.disp-html-row-sel td {{
  background: var(--disp-row-sel) !important;
}}
table.disp-html-table.disp-html-head-table {{
  margin-top: 0 !important;
}}
table.disp-html-table.disp-html-row-table {{
  margin: 0 !important;
}}
div.st-key-disp_ticket_table_fast {{
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  padding: 0 !important;
  margin-top: 0 !important;
  box-sizing: border-box !important;
}}
div.st-key-disp_ticket_table_fast [data-testid="stVerticalBlock"] {{
  gap: 0 !important;
}}
div.st-key-disp_ticket_table_fast [data-testid="stHorizontalBlock"] {{
  margin: 0 !important;
  border-bottom: 0.5px solid var(--disp-border) !important;
}}
div.st-key-disp_ticket_table_fast [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {{
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  min-height: 36px !important;
}}
div.st-key-disp_ticket_table_fast [data-testid="stMarkdownContainer"] {{
  margin: 0 !important;
}}
div.st-key-disp_ticket_table_fast [data-testid="column"]:last-child .stButton > button {{
  min-height: 28px !important;
  height: 28px !important;
  width: 100% !important;
  padding: 0 4px !important;
  font-size: 14px !important;
  line-height: 1 !important;
  margin: 0 !important;
  border-radius: var(--disp-radius-sm) !important;
}}
div.st-key-disp_ticket_pager {{
  margin-top: 12px !important;
  padding: 8px 12px !important;
  background: var(--disp-panel) !important;
  border: 1px solid var(--disp-border) !important;
  border-radius: var(--disp-radius) !important;
  box-shadow: var(--disp-shadow) !important;
}}
div.st-key-disp_ticket_pager .stButton > button {{
  min-height: 32px !important;
  height: 32px !important;
  font-size: 12px !important;
  border-radius: var(--disp-radius-sm) !important;
  border: 1px solid var(--disp-border) !important;
  background: var(--disp-card) !important;
}}
.disp-pager-label {{
  text-align: center !important;
  font-size: 12px !important;
  color: var(--disp-muted) !important;
  margin: 6px 0 0 !important;
  font-variant-numeric: tabular-nums !important;
}}
.disp-pager-label strong {{
  color: var(--disp-text) !important;
  font-weight: 600 !important;
}}
[class*="st-key-disp_case_type_"] .stButton > button {{
  font-size: 12px !important;
  min-height: 32px !important;
  height: 32px !important;
  border-radius: 999px !important;
  border: 1px solid var(--disp-border) !important;
  background: var(--disp-panel) !important;
  color: var(--disp-muted) !important;
  margin: 0 !important;
}}
[class*="st-key-disp_case_type_"] .stButton > button[kind="primary"],
[class*="st-key-disp_case_type_"] .stButton > button[data-testid="stBaseButton-primary"] {{
  background: var(--disp-accent-bg) !important;
  border-color: #4338ca !important;
  color: var(--disp-text) !important;
  font-weight: 500 !important;
}}
"""

# Global dark styling for Streamlit widgets outside the dispatch 3-column shell.
DISPATCH_FULL_DARK_CSS = """
/* ── Typography & links ── */
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span,
.stMarkdown p,
.stMarkdown li {
  color: var(--disp-muted) !important;
}
[data-testid="stMarkdownContainer"] strong,
.stMarkdown strong {
  color: var(--disp-text) !important;
}
.stMarkdown a,
[data-testid="stMarkdownContainer"] a {
  color: var(--disp-accent) !important;
}
.stMarkdown code,
[data-testid="stMarkdownContainer"] code {
  background: var(--disp-card) !important;
  color: var(--disp-text) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 3px !important;
  padding: 0 4px !important;
}

/* ── Alerts ── */
[data-testid="stAlert"],
div[data-testid="stNotification"] {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  color: var(--disp-muted) !important;
}
[data-testid="stAlert"] [data-testid="stMarkdownContainer"] p,
div[data-testid="stNotification"] p {
  color: var(--disp-muted) !important;
}
[data-testid="stAlertIcon"] {
  color: var(--disp-muted) !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
  margin: 6px 0 !important;
  border: none !important;
  background: transparent !important;
}
[data-testid="stExpander"] details {
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  background: var(--disp-card) !important;
  overflow: hidden;
}
[data-testid="stExpander"] summary {
  color: var(--disp-muted) !important;
  font-size: 13px !important;
  font-weight: 500 !important;
  padding: 8px 10px !important;
  background: transparent !important;
}
[data-testid="stExpander"] summary:hover {
  color: var(--disp-text) !important;
  background: #121a2a !important;
}
[data-testid="stExpander"] details[open] > div,
[data-testid="stExpander"] [data-testid="stExpanderDetails"] {
  padding: 8px 10px 10px !important;
  border-top: 0.5px solid var(--disp-border) !important;
  background: var(--disp-panel) !important;
}

/* ── Bordered containers ── */
[data-testid="stVerticalBlockBorderWrapper"] {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
}

/* ── Checkbox & radio (outside header) ── */
[data-testid="stCheckbox"] label,
[data-testid="stCheckbox"] label span,
[data-testid="stCheckbox"] label p {
  color: var(--disp-muted) !important;
  font-size: 13px !important;
}
[data-testid="stRadio"] label span,
[data-testid="stRadio"] label p {
  color: var(--disp-muted) !important;
}

/* ── Date input ── */
.stDateInput > div > div,
.stDateInput input {
  background: var(--disp-card) !important;
  color: var(--disp-text) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 4px !important;
}
.stDateInput label {
  color: var(--disp-dim) !important;
  font-size: 11px !important;
}

/* ── Multi-select ── */
.stMultiSelect [data-baseweb="select"] > div {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  color: var(--disp-muted) !important;
}

/* ── Select / dropdown menus (Baseweb portal) ── */
div[data-baseweb="popover"],
div[data-baseweb="menu"],
ul[role="listbox"] {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  color: var(--disp-muted) !important;
}
ul[role="listbox"] li,
div[data-baseweb="menu"] li {
  background: var(--disp-card) !important;
  color: var(--disp-muted) !important;
}
ul[role="listbox"] li:hover,
div[data-baseweb="menu"] li:hover {
  background: #121a2a !important;
  color: var(--disp-text) !important;
}

/* ── Popover panels ── */
[data-testid="stPopoverBody"] {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  color: var(--disp-muted) !important;
  min-width: 13rem !important;
  max-width: 16rem !important;
  padding: 8px 10px !important;
}
[data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] {
  gap: 4px !important;
}
[data-testid="stPopoverBody"] .stButton > button {
  font-size: 13px !important;
  min-height: 32px !important;
  text-align: left !important;
}
[data-testid="stPopoverBody"] [data-testid="stExpander"] details {
  border-color: var(--disp-border) !important;
}
[data-testid="stPopoverBody"] hr {
  border-color: var(--disp-border) !important;
  margin: 6px 0 !important;
}

/* ── Data frames & editors ── */
[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  overflow: hidden !important;
  background: var(--disp-card) !important;
  min-height: 6rem;
}
[data-testid="stDataFrame"] .stDataFrameGlideDataEditor,
[data-testid="stDataEditor"] .stDataEditorGlideDataEditor,
[data-testid="stDataFrame"] [data-testid="glideDataEditor"],
[data-testid="stDataEditor"] [data-testid="glideDataEditor"] {
  --gdg-bg-cell: #0d1220 !important;
  --gdg-bg-header: #080b14 !important;
  --gdg-bg-header-has-focus: #121a2a !important;
  --gdg-bg-header-hovered: #121a2a !important;
  --gdg-text-dark: #e2e8f8 !important;
  --gdg-text-medium: #8a9ac0 !important;
  --gdg-text-light: #4a5a7a !important;
  --gdg-text-header: #8a9ac0 !important;
  --gdg-border-color: #1a2035 !important;
  --gdg-accent-color: #3b82f6 !important;
  --gdg-accent-light: rgba(59, 130, 246, 0.15) !important;
  --gdg-accent-fg: #e2e8f8 !important;
  --gdg-bg-cell-medium: #0d1220 !important;
  --gdg-bg-search-cell: #121a2a !important;
  min-height: 6rem !important;
}
[data-testid="stDataFrame"] .dvn-scroller,
[data-testid="stDataEditor"] .dvn-scroller {
  min-height: 4rem !important;
}

/* ── Code blocks ── */
.stCode,
pre,
code block {
  background: var(--disp-panel) !important;
  color: var(--disp-muted) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 4px !important;
}

/* ── Primary / secondary buttons (Streamlit defaults) ── */
.stButton > button[kind="primary"],
button[data-testid="stBaseButton-primary"],
[data-testid="stFormSubmitButton"] button[kind="primary"] {
  background: #0d2a50 !important;
  border-color: #1a4a80 !important;
  color: #3b82f6 !important;
}
.stButton > button[kind="primary"]:hover,
button[data-testid="stBaseButton-primary"]:hover {
  background: #102f5a !important;
  border-color: #2563eb !important;
  color: #60a5fa !important;
}
.stButton > button[kind="secondary"],
button[data-testid="stBaseButton-secondary"] {
  background: transparent !important;
  border-color: var(--disp-border) !important;
  color: var(--disp-muted) !important;
}
.stButton > button[kind="secondary"]:hover,
button[data-testid="stBaseButton-secondary"]:hover {
  background: var(--disp-card) !important;
  border-color: #2a3a5a !important;
  color: var(--disp-text) !important;
}

/* ── Download button ── */
.stDownloadButton > button {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  color: var(--disp-muted) !important;
}

/* ── Spinner ── */
[data-testid="stSpinner"] {
  color: var(--disp-accent) !important;
}

/* ── Segmented control ── */
div[data-baseweb="segmented-control"] {
  background: var(--disp-panel) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  gap: 4px !important;
}
div[data-baseweb="segmented-control"] button {
  color: var(--disp-dim) !important;
  background: transparent !important;
}
div[data-baseweb="segmented-control"] button[aria-selected="true"] {
  background: var(--disp-accent-bg) !important;
  color: var(--disp-text) !important;
}

/* ── Toolbar rows (queue actions) ── */
div[class*="st-key-"][class*="_ctx_toolbar"],
div[class*="st-key-"][class*="_sc_toolbar"] {
  padding: 4px 0 !important;
}
div[class*="st-key-"][class*="_ctx_toolbar"] .stMarkdown p,
div[class*="st-key-"][class*="_sc_toolbar"] .stMarkdown p {
  color: var(--disp-muted) !important;
  margin: 0 !important;
}

/* ── Log / attendance tab body ── */
div.st-key-disp_log_body,
div.st-key-disp_log_body [data-testid="stVerticalBlockBorderWrapper"] {
  background: transparent !important;
  border: none !important;
}

/* ── Performance weekly panels (dispatch palette) ── */
.weekly-exec-title,
.weekly-kpi-value,
.weekly-panel h4 {
  color: var(--disp-text) !important;
}
.weekly-exec-sub,
.weekly-kpi-label,
.weekly-exec-badge {
  color: var(--disp-dim) !important;
}
.weekly-exec-badge,
.weekly-kpi-card,
.weekly-panel {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
}
.weekly-exec-header {
  border-bottom-color: var(--disp-border) !important;
}
.weekly-date-range,
.weekly-kpi-sub {
  color: var(--disp-accent) !important;
}
.weekly-date-wrap [data-testid="stDateInput"] > div {
  background: var(--disp-card) !important;
  border: 0.5px solid var(--disp-border) !important;
}
"""

# Legacy alias — layout is merged into app.apply_theme()
DISPATCH_THEME_CSS = f"<style>{DISPATCH_LAYOUT_RULES}</style>"

DISPATCH_LOGIN_CSS = """
<style>
.stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"],
[data-testid="stAppViewContainer"] section.main {
  background: #0b0f18 !important;
}
[data-testid="stAppViewContainer"] section.main::before {
  content: "";
  position: fixed; inset: 0; z-index: 0; pointer-events: none;
  background:
    radial-gradient(ellipse 80% 50% at 20% 40%, rgba(59, 130, 246, 0.06), transparent 55%),
    radial-gradient(ellipse 70% 45% at 80% 60%, rgba(34, 197, 94, 0.04), transparent 50%);
}
</style>
"""

STATUS_COLORS: dict[str, dict[str, str]] = {
    "Daily Task": {"bg": "#0d1e3a", "fg": "#3b82f6"},
    "Open": {"bg": "#0d2218", "fg": "#22c55e"},
    "Needs Review": {"bg": "#0d2218", "fg": "#22c55e"},
    "On Hold": {"bg": "#231a06", "fg": "#f59e0b"},
    "Under Investigation": {"bg": "#1a1030", "fg": "#a78bfa"},
    "Resolved": {"bg": "#0d1a10", "fg": "#34d399"},
    "Unattended": {"bg": "#2d1515", "fg": "#ef4444"},
    "Sales ticket": {"bg": "#0d1e3a", "fg": "#3b82f6"},
    "Investigation": {"bg": "#1a1030", "fg": "#a78bfa"},
    "Design": {"bg": "#1a1a30", "fg": "#818cf8"},
    "Regional for site visit": {"bg": "#0d1e3a", "fg": "#60a5fa"},
}

QUEUE_DOTS: dict[str, str] = {
    "Daily Task": "#3b82f6",
    "Needs Review": "#22c55e",
    "On Hold": "#f59e0b",
    "Under Investigation": "#a78bfa",
    "Follow up": "#f472b6",
    "Unattended": "#ef4444",
    "Resolved": "#4a5a7a",
}

QUEUE_ORDER: tuple[str, ...] = tuple(QUEUE_DOTS.keys())

SALES_QUEUE_DOTS: dict[str, str] = {
    "Sales ticket": "#a78bfa",
    "Investigation": "#8a9ac0",
    "Design": "#8a9ac0",
    "Resolved": "#4a5a7a",
}

SALES_QUEUE_ORDER: tuple[str, ...] = tuple(SALES_QUEUE_DOTS.keys())


def _build_queue_dot_css(prefix: str, dots: dict[str, str]) -> str:
    """Static queue-dot colours (avoids per-rerun inline style blocks)."""
    rules: list[str] = []
    for q, color in dots.items():
        key = q.replace(" ", "_")
        rules.append(
            f"div.st-key-{prefix}_{key} .stButton > button::before {{"
            f"color: {color} !important;}}"
        )
    return "\n".join(rules)


DISPATCH_LAYOUT_RULES = (
    DISPATCH_LAYOUT_RULES
    + _build_queue_dot_css("uqueue", QUEUE_DOTS)
    + _build_queue_dot_css("disp_queue", QUEUE_DOTS)
    + _build_queue_dot_css("sales_queue", SALES_QUEUE_DOTS)
)

TIMELINE_DOT: dict[str, str] = {
    "Assignment": "#3b82f6",
    "Response": "#22c55e",
    "Nudge": "#f59e0b",
    "AutoUnattended": "#ef4444",
    "OnHold": "#f59e0b",
    "AdminClosed": "#f59e0b",
    "Resolved": "#34d399",
    "MovedToInvestigation": "#a78bfa",
    "ReopenedFromUnattended": "#3b82f6",
    "ReopenedFromResolved": "#3b82f6",
    "LegacyLogin": "#4a5a7a",
}

_DISPATCH_ACTIVE_QUEUE_KEY = "active_queue"
_DISP_MENU_OPEN_KEY = "disp_menu_open"


def inject_dispatch_theme(*, login: bool = False) -> None:
    """Backward-compatible shim — dashboard theme is app.apply_theme()."""
    if login:
        st.markdown(DISPATCH_LOGIN_CSS, unsafe_allow_html=True)


def status_pill(status: str) -> str:
    label = display_status(status)
    palette = {
        "Unattended": ("#3f1515", "#f87171"),
        "Needs Review": ("#14291a", "#4ade80"),
        "Daily Task": ("#0f2438", "#60a5fa"),
        "On Hold": ("#2a2008", "#fbbf24"),
        "Under Investigation": ("#1f1530", "#c084fc"),
        "Follow up": ("#2a1030", "#f472b6"),
        "Resolved": ("#142018", "#86efac"),
    }
    bg, fg = palette.get(label, ("#1a1a1f", "#a1a1aa"))
    safe = html.escape(label)
    return (
        f'<span style="font-size:10px;font-weight:500;padding:2px 5px;'
        f'border-radius:2px;background:{bg};color:{fg};'
        f'white-space:nowrap">{safe}</span>'
    )


def display_status(status: str) -> str:
    if status == "Open":
        return "Needs Review"
    return status


def elapsed_color(assigned_at_utc: datetime) -> str:
    hours = (datetime.now(timezone.utc) - assigned_at_utc).total_seconds() / 3600
    if hours >= 5.5:
        return "#ef4444"
    if hours >= 3:
        return "#f59e0b"
    return "#22c55e"


def elapsed_label(assigned_at_utc: datetime) -> str:
    delta = datetime.now(timezone.utc) - assigned_at_utc
    h, m = divmod(int(delta.total_seconds()) // 60, 60)
    suffix = " ⚠" if delta.total_seconds() / 3600 >= 5.5 else ""
    return f"{h}h {m:02d}m{suffix}"


def format_utc5(dt: object, *, tz: timezone) -> str:
    if dt is None:
        return "—"
    try:
        import pandas as pd

        ts = pd.Timestamp(dt)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert(tz).strftime("%Y-%m-%d %H:%M UTC+5")
    except Exception:
        return str(dt)


def render_topbar(
    *,
    operator_id: str,
    now_label: str | None = None,
    section: str = "Ticket",
    is_admin: bool = False,
    is_legacy: bool = False,
) -> None:
    """Legacy single-row header (prefer unified shell in app.py)."""
    op = html.escape(operator_id or "—")
    if now_label is None:
        now_label = datetime.now(_UI_TZ_UTC5).strftime("%a %d %b · %H:%M UTC+5")
    role = "identity unverified" if is_legacy else ("admin" if is_admin else "operator")
    st.markdown(
        f"""
    <div class="disp-full-bleed" style="
      background:#0b0f18;border-bottom:0.5px solid #1a2035;
      height:var(--disp-header-h);display:flex;align-items:center;padding:0 {_DISP_INSET};
      position:sticky;top:0;z-index:999;gap:0;margin-bottom:0.5rem
    ">
      <div style="display:flex;flex-direction:column;padding-right:18px;border-right:0.5px solid #1a2035">
        <span style="font-size:9px;font-weight:600;color:#5b7fb5;letter-spacing:.12em;text-transform:uppercase">NetOps</span>
        <span style="font-size:13px;font-weight:500;color:#e2e8f8">Coverage Eye</span>
      </div>
      <span style="font-size:15px;color:#2a3a5a;padding:0 16px">{html.escape(section)}</span>
      <div style="margin-left:auto;display:flex;align-items:center;gap:10px">
        <span class="disp-header-clock-pill">{html.escape(now_label)}</span>
        <span class="disp-header-op">{op} · {html.escape(role)}</span>
      </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_sidebar_today_grid(
    items: tuple[tuple[str, int | str, str], ...],
) -> None:
    """Compact 2x2 today stats — aligned with queue list below."""
    cells: list[str] = []
    for label, value, colour in items:
        cells.append(
            f'<div class="disp-today-cell">'
            f'<div class="disp-today-label">{html.escape(str(label))}</div>'
            f'<div class="disp-today-value" style="color:{html.escape(colour)}">'
            f"{html.escape(str(value))}</div></div>"
        )
    st.markdown(
        f'<div class="disp-today-grid">{"".join(cells)}</div>',
        unsafe_allow_html=True,
    )


def render_queue_list(
    *,
    selected: str,
    counts: dict[str, int],
    session_key: str,
    queue_order: tuple[str, ...] | None = None,
    queue_dots: dict[str, str] | None = None,
    button_key_prefix: str = "disp_queue",
) -> str:
    """Sidebar queue picker — dot inside button, count on the right."""
    order = queue_order or QUEUE_ORDER
    dots = queue_dots or QUEUE_DOTS
    picked = selected
    with st.container(key="disp_sidebar_queues"):
        for q in order:
            cnt = counts.get(q, 0)
            is_active = q == selected
            row_key = q.replace(" ", "_")
            btn_key = f"{button_key_prefix}_{row_key}"
            st.markdown(
                f"<style>div.st-key-{btn_key} .stButton > button::after {{"
                f'content: "{cnt}";}}</style>',
                unsafe_allow_html=True,
            )
            if st.button(
                q,
                key=btn_key,
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                st.session_state[session_key] = q
                st.rerun()
    return str(st.session_state.get(session_key, picked))


def render_refresh_caption(text: str) -> None:
    st.markdown(
        f'<p class="disp-refresh-caption">{html.escape(text)}</p>',
        unsafe_allow_html=True,
    )


def render_quick_assign_bar(
    *,
    engineers: list[str],
    categories: list[str],
    on_submit: Callable[[str, str, str, str], None],
) -> None:
    """Pinned single-line assign bar. Call before the queue header."""
    with st.container(key="disp_assign_bar"):
        col_lbl, col_num, col_eng, col_eng2, col_cat, col_btn = st.columns(
            [0.7, 1.4, 1.2, 1.2, 1.2, 1.3], gap="small"
        )

        with col_lbl:
            st.markdown(
                '<p style="font-size:11px;font-weight:600;color:#2a3a5a;'
                'text-transform:uppercase;letter-spacing:.06em;'
                'margin:0;padding-top:6px">Quick assign</p>',
                unsafe_allow_html=True,
            )

        with col_num:
            ticket_num = st.text_input(
                "Ticket #",
                placeholder="9 or 16-digit ID",
                label_visibility="collapsed",
                key="qa_ticket_num",
            )

        with col_eng:
            engineer = st.selectbox(
                "Engineer",
                engineers or ["—"],
                label_visibility="collapsed",
                key="qa_engineer",
            )

        with col_eng2:
            engineer2 = st.selectbox(
                "Eng 2 (optional)",
                ["—"] + list(engineers),
                label_visibility="collapsed",
                key="qa_engineer2",
            )

        with col_cat:
            category = st.selectbox(
                "Category",
                categories or ["—"],
                label_visibility="collapsed",
                key="qa_category",
            )

        with col_btn:
            st.markdown('<div class="primary-btn">', unsafe_allow_html=True)
            if st.button("Assign + Telegram ↗", key="qa_submit", use_container_width=True):
                on_submit(ticket_num, engineer, engineer2, category)
            st.markdown("</div>", unsafe_allow_html=True)


def render_settings_popover(
    *,
    time_preset_options: list[str],
    time_preset_key: str,
    on_refresh: Callable[[], None] | None = None,
    on_signout: Callable[[], None] | None = None,
    render_custom_dates: Callable[[], None] | None = None,
    render_admin: Callable[[], None] | None = None,
    range_caption: str = "",
) -> None:
    """Compact settings popover for the top bar."""
    with st.popover("Settings", use_container_width=False):
        st.markdown(
            '<p class="settings-section-label" style="margin-top:0">Refresh</p>',
            unsafe_allow_html=True,
        )
        auto = st.toggle(
            "Auto-refresh",
            value=st.session_state.get("bon_toolbar_auto_refresh", True),
            key="settings_auto_refresh",
        )
        st.session_state["bon_toolbar_auto_refresh"] = auto

        if auto:
            interval = st.select_slider(
                "Interval",
                options=[1, 2, 5, 10, 15, 30, 60],
                value=int(st.session_state.get("bon_toolbar_refresh_interval", 5)),
                key="settings_interval",
                format_func=lambda x: f"{x} min",
            )
            st.session_state["bon_toolbar_refresh_interval"] = interval

        if st.button("↻ Refresh now", key="settings_refresh_now", use_container_width=True):
            if on_refresh:
                on_refresh()
            else:
                st.rerun()

        st.divider()

        st.markdown(
            '<p class="settings-section-label">Date range</p>',
            unsafe_allow_html=True,
        )
        if range_caption:
            st.markdown(
                f'<p class="settings-range-cap">{html.escape(range_caption)}</p>',
                unsafe_allow_html=True,
            )
        menu_labels = [o for o in time_preset_options if o != "Pick dates"]
        display_opts = menu_labels + ["Custom"]
        cur = str(st.session_state.get(time_preset_key, "This week"))
        if cur == "Pick dates":
            cur = "Custom"
        if cur not in display_opts:
            cur = "This week"
            st.session_state[time_preset_key] = "This week"
        # Drop legacy searchable selectbox state (could show stale filter text).
        st.session_state.pop("settings_range", None)
        radio_key = f"settings_time_range_{time_preset_key}"
        if st.session_state.get(radio_key) not in display_opts:
            st.session_state[radio_key] = cur
        range_opt = st.radio(
            "Range",
            display_opts,
            label_visibility="collapsed",
            key=radio_key,
        )
        if range_opt == "Custom":
            st.session_state[time_preset_key] = "Pick dates"
            if render_custom_dates:
                render_custom_dates()
        else:
            st.session_state[time_preset_key] = range_opt

        if render_admin:
            st.divider()
            st.markdown(
                '<p class="settings-section-label">Admin</p>',
                unsafe_allow_html=True,
            )
            with st.expander("Team accounts", expanded=False):
                render_admin()

        st.divider()

        if st.button("Sign out", key="settings_signout", use_container_width=True):
            if on_signout:
                on_signout()
            else:
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()


def menu_open_key() -> str:
    return _DISP_MENU_OPEN_KEY


def _engineer_initials(handle: str) -> str:
    h = str(handle or "").replace("@", "")
    parts = h.split("_")
    return "".join(p[0].upper() for p in parts[:2] if p) or "?"


def _avatar_html(handle: str, *, margin_left: str = "0") -> str:
    init = html.escape(_engineer_initials(handle))
    ml = f"margin-left:{margin_left};" if margin_left else ""
    return (
        f'<div style="width:24px;height:24px;border-radius:50%;'
        f'background:#0d1e3a;display:inline-flex;align-items:center;'
        f'justify-content:center;font-size:10px;font-weight:600;'
        f'color:#3b82f6;flex-shrink:0;{ml}">{init}</div>'
    )


def _row_elapsed(t: dict[str, Any]) -> tuple[str, str]:
    """Return (color, label) for the elapsed column."""
    status = str(t.get("status") or "")
    last_at = t.get("last_assigned_at")
    if last_at is not None and status in (
        "Daily Task",
        "On Hold",
        "Under Investigation",
    ):
        try:
            import pandas as pd

            ts = pd.Timestamp(last_at)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            dt = ts.to_pydatetime()
            return elapsed_color(dt), elapsed_label(dt)
        except Exception:
            pass
    if status in ("Open", "Needs Review"):
        return "#22c55e", "Responded"
    return "#4a5a7a", "—"


_TICKET_TABLE_COLS: tuple[float, ...] = (1.45, 1.15, 1.5, 2.35, 1.25, 1.15, 1.0)
_TICKET_TABLE_COLS_WITH_TYPE: tuple[float, ...] = (
    1.35, 0.95, 1.05, 1.35, 2.0, 1.15, 1.05, 1.0
)
_CASE_TYPE_PILL_COLORS: dict[str, str] = {
    "Residential": "#3b82f6",
    "Resort": "#a78bfa",
}


def _table_cell_style(*, is_sel: bool) -> str:
    """Shared row cell chrome for ticket/sales tables (selection via row CSS)."""
    del is_sel
    return (
        "padding:5px 8px;min-height:36px;display:flex;align-items:center;"
        "box-sizing:border-box;width:100%;white-space:nowrap;overflow:hidden;"
    )


def _fast_row_cell_style(*, is_sel: bool, is_alt: bool) -> str:
    style = _table_cell_style(is_sel=False)
    if is_sel:
        return f"{style}background:var(--disp-row-sel);"
    if is_alt:
        return f"{style}background:var(--disp-row-alt);"
    return style


def _render_table_row_actions(
    *,
    row_key: str,
    row_data: dict[str, Any],
    row_id: str,
    row_actions_fn: Callable[[dict[str, Any], str], None] | None,
    **_: object,
) -> None:
    """Single ⋮ control — selects row when unselected, opens menu when selected."""
    with st.container(
        horizontal=True,
        vertical_alignment="center",
        key=f"disp_row_actions_{row_key}",
    ):
        if row_actions_fn:
            row_actions_fn(row_data, row_id)


def _case_type_pill_html(case_type: str) -> str:
    raw = str(case_type or "—").strip() or "—"
    label = "Res" if raw == "Residential" else "Rsr" if raw == "Resort" else raw
    label = html.escape(label)
    color = _CASE_TYPE_PILL_COLORS.get(raw, "#4a5a7a")
    return (
        f'<span style="font-size:11px;font-weight:500;color:{color};'
        f'white-space:nowrap">{label}</span>'
    )


def _ticket_row_notes_html(t: dict[str, Any]) -> str:
    fu = html.escape(str(t.get("follow_up_label") or "").strip())
    notes = html.escape(str(t.get("additional_info") or ""))
    if fu:
        inner = (
            f'<span style="color:#a78bfa;font-weight:500">{fu}</span>'
            + (f' <span style="color:#4a5a7a">{notes}</span>' if notes else "")
        )
    else:
        inner = f'<span style="color:#4a5a7a">{notes}</span>' if notes else "—"
    return f"<span>{inner}</span>"


def _ticket_row_elapsed_html(t: dict[str, Any]) -> str:
    if str(t.get("case_type") or "") == "Resort":
        pri = str(t.get("sales_priority") or "Standard").strip() or "Standard"
        style = {
            "Strategic": ("#2d1515", "#ef4444"),
            "Urgent": ("#231a06", "#f59e0b"),
            "High": ("#231a06", "#b8954f"),
            "Standard": ("#1a2035", "#4a5a7a"),
        }
        bg, fg = style.get(pri, ("#1a2035", "#4a5a7a"))
        return (
            f'<span style="font-size:11px;font-weight:500;padding:3px 7px;'
            f'border-radius:3px;background:{bg};color:{fg}">'
            f"{html.escape(pri)}</span>"
        )
    el_color, el_label = _row_elapsed(t)
    return (
        f'<span style="font-size:13px;font-weight:400;color:{el_color};'
        f'font-variant-numeric:tabular-nums">{html.escape(el_label)}</span>'
    )


def _ticket_row_engineer_html(t: dict[str, Any]) -> str:
    eng = str(t.get("assigned_to") or "—")
    eng2 = str(t.get("assigned_to_2") or "").strip()
    av = _avatar_html(eng)
    if eng2:
        av += _avatar_html(eng2, margin_left="-5px")
        tag = (
            ' <span style="font-size:11px;font-weight:400;color:#3b82f6">shared</span>'
        )
    else:
        tag = (
            f' <span style="font-size:13px;font-weight:400;color:#8a9ac0">'
            f"{html.escape(eng)}</span>"
        )
    return f'<span style="display:inline-flex;align-items:center;gap:2px">{av}{tag}</span>'


def _ticket_table_layout(show_case_type: bool) -> tuple[list[str], list[str]]:
    headers = (
        ["Ticket", "Type", "Category", "Engineer", "Notes", "Elapsed", "Status"]
        if show_case_type
        else ["Ticket", "Category", "Engineer", "Notes", "Elapsed", "Status"]
    )
    col_widths = (
        ["14%", "5%", "12%", "10%", "34%", "10%", "9%"]
        if show_case_type
        else ["15%", "13%", "11%", "33%", "11%", "10%"]
    )
    return headers, col_widths


def _build_ticket_row_cells_html(
    t: dict[str, Any],
    *,
    selected: str | None,
    show_case_type: bool,
) -> str:
    tnum = str(t.get("ticket_number") or "")
    fu_dot = ""
    if (
        str(t.get("case_type") or "") == "Residential"
        and str(t.get("follow_up_at") or "").strip()
    ):
        fu_dot = '<span style="font-size:12px;color:#a78bfa;margin-left:4px">●</span>'
    cells = [
        f'<td><span class="disp-ticket-num">{html.escape(tnum)}{fu_dot}</span></td>',
    ]
    if show_case_type:
        cells.append(
            f"<td>{_case_type_pill_html(str(t.get('case_type') or ''))}</td>"
        )
    cells.extend(
        [
            f'<td><span style="color:#4a5a7a">'
            f'{html.escape(str(t.get("task_category") or "—"))}</span></td>',
            f"<td>{_ticket_row_engineer_html(t)}</td>",
            f'<td class="disp-html-notes">{_ticket_row_notes_html(t)}</td>',
            f"<td>{_ticket_row_elapsed_html(t)}</td>",
            f"<td>{status_pill(str(t.get('status') or ''))}</td>",
        ]
    )
    return "".join(cells)


def _build_ticket_table_head_html(*, show_case_type: bool) -> str:
    headers, col_widths = _ticket_table_layout(show_case_type)
    head = "".join(
        f'<th style="width:{w}">{html.escape(label)}</th>'
        for label, w in zip(headers, col_widths, strict=True)
    )
    return (
        f'<table class="disp-html-table disp-html-head-table">'
        f"<thead><tr>{head}</tr></thead></table>"
    )


def _build_ticket_table_row_html(
    t: dict[str, Any],
    *,
    selected: str | None,
    show_case_type: bool,
) -> str:
    tnum = str(t.get("ticket_number") or "")
    is_sel = selected == tnum
    row_cls = "disp-html-row-sel" if is_sel else ""
    cells = _build_ticket_row_cells_html(
        t, selected=selected, show_case_type=show_case_type
    )
    return (
        f'<table class="disp-html-table disp-html-row-table">'
        f'<tbody><tr class="{row_cls}">{cells}</tr></tbody></table>'
    )


def _build_ticket_table_html(
    tickets: list[dict[str, Any]],
    *,
    selected: str | None,
    show_case_type: bool,
) -> str:
    """Full table HTML (header + rows) — used for exports or previews."""
    head = _build_ticket_table_head_html(show_case_type=show_case_type)
    merged_rows: list[str] = []
    for t in tickets:
        row_html = _build_ticket_table_row_html(
            t, selected=selected, show_case_type=show_case_type
        )
        start = row_html.find("<tr")
        end = row_html.find("</tr>") + len("</tr>")
        if start >= 0 and end > start:
            merged_rows.append(row_html[start:end])
    thead_start = head.find("<thead>")
    thead_end = head.find("</thead>") + len("</thead>")
    thead = head[thead_start:thead_end] if thead_start >= 0 else ""
    return (
        f'<table class="disp-html-table">{thead}'
        f'<tbody>{"".join(merged_rows)}</tbody></table>'
    )


def render_ticket_table(
    tickets: list[dict[str, Any]],
    *,
    selected: str | None,
    selected_key: str,
    row_actions_fn: Callable[[dict[str, Any], str], None] | None = None,
    show_case_type: bool = False,
    case_type_session_key: str | None = None,
) -> None:
    """Render ticket rows as Streamlit columns (supports per-row popovers)."""
    if not tickets:
        st.markdown(
            '<div class="disp-table-empty">No tickets in this queue</div>',
            unsafe_allow_html=True,
        )
        return

    col_ratios = (
        list(_TICKET_TABLE_COLS_WITH_TYPE)
        if show_case_type
        else list(_TICKET_TABLE_COLS)
    )
    header_labels = (
        ["Ticket", "Type", "Category", "Engineer", "Notes", "Elapsed", "Status"]
        if show_case_type
        else ["Ticket", "Category", "Engineer", "Notes", "Elapsed", "Status"]
    )
    with st.container(key="disp_table_header"):
        header_cols = st.columns(col_ratios, gap="small")
        for col, label in zip(header_cols[:-1], header_labels):
            with col:
                st.markdown(
                    f'<p class="disp-col-head">{html.escape(label)}</p>',
                    unsafe_allow_html=True,
                )

    with st.container(key="disp_ticket_table"):
        for t in tickets:
            tnum = str(t.get("ticket_number") or "")
            is_sel = selected == tnum
            cell = _table_cell_style(is_sel=is_sel)

            with st.container(key=f"disp_ticket_row_{tnum}"):
                if is_sel:
                    st.markdown(
                        '<span class="disp-row-selected" aria-hidden="true"></span>',
                        unsafe_allow_html=True,
                    )
                row_cols = st.columns(
                    col_ratios,
                    gap="small",
                    vertical_alignment="center",
                )
                col_idx = 0

                with row_cols[col_idx]:
                    fu_dot = ""
                    if (
                        str(t.get("case_type") or "") == "Residential"
                        and str(t.get("follow_up_at") or "").strip()
                    ):
                        fu_dot = '<span style="font-size:12px;color:#a78bfa;margin-left:4px">●</span>'
                    st.markdown(
                        f'<div style="{cell}"><span class="disp-ticket-num">'
                        f"{html.escape(tnum)}{fu_dot}</span></div>",
                        unsafe_allow_html=True,
                    )
                col_idx += 1
                if show_case_type:
                    with row_cols[col_idx]:
                        st.markdown(
                            f'<div style="{cell}">{_case_type_pill_html(str(t.get("case_type") or ""))}</div>',
                            unsafe_allow_html=True,
                        )
                    col_idx += 1
                c_cat, c_eng, c_notes, c_elapsed, c_status, c_actions = row_cols[
                    col_idx : col_idx + 6
                ]
                with c_cat:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                        f'color:#4a5a7a">'
                        f'{html.escape(str(t.get("task_category") or "—"))}</span></div>',
                        unsafe_allow_html=True,
                    )
                with c_eng:
                    eng = str(t.get("assigned_to") or "—")
                    eng2 = str(t.get("assigned_to_2") or "").strip()
                    av = _avatar_html(eng)
                    if eng2:
                        av += _avatar_html(eng2, margin_left="-5px")
                    if eng2:
                        tag = (
                            ' <span style="font-size:11px;font-weight:400;color:#3b82f6">'
                            "shared</span>"
                        )
                    else:
                        tag = (
                            f' <span style="font-size:13px;font-weight:400;color:#8a9ac0">'
                            f"{html.escape(eng)}</span>"
                        )
                    st.markdown(
                        f'<div style="{cell};gap:2px;min-width:0">'
                        f"{av}{tag}</div>",
                        unsafe_allow_html=True,
                    )
                with c_notes:
                    fu = html.escape(str(t.get("follow_up_label") or "").strip())
                    notes = html.escape(str(t.get("additional_info") or ""))
                    if fu:
                        note_html = (
                            f'<span style="color:#a78bfa;font-weight:500">{fu}</span>'
                            + (f' <span style="color:#4a5a7a">{notes}</span>' if notes else "")
                        )
                    else:
                        note_html = notes
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                        f'white-space:nowrap;overflow:hidden;'
                        f'text-overflow:ellipsis;display:block;min-width:0">'
                        f"{note_html}</span></div>",
                        unsafe_allow_html=True,
                    )
                with c_elapsed:
                    if str(t.get("case_type") or "") == "Resort":
                        pri = str(t.get("sales_priority") or "Standard").strip() or "Standard"
                        style = {
                            "Strategic": ("#2d1515", "#ef4444"),
                            "Urgent": ("#231a06", "#f59e0b"),
                            "High": ("#231a06", "#b8954f"),
                            "Standard": ("#1a2035", "#4a5a7a"),
                        }
                        bg, fg = style.get(pri, ("#1a2035", "#4a5a7a"))
                        st.markdown(
                            f'<div style="{cell}"><span style="font-size:11px;font-weight:500;'
                            f'padding:3px 7px;border-radius:3px;background:{bg};color:{fg}">'
                            f"{html.escape(pri)}</span></div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        el_color, el_label = _row_elapsed(t)
                        st.markdown(
                            f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                            f'color:{el_color};font-variant-numeric:tabular-nums">'
                            f"{html.escape(el_label)}</span></div>",
                            unsafe_allow_html=True,
                        )
                with c_status:
                    st.markdown(
                        f'<div style="{cell}">{status_pill(str(t.get("status") or ""))}</div>',
                        unsafe_allow_html=True,
                    )
                with c_actions:
                    _render_table_row_actions(
                        row_key=tnum,
                        is_sel=is_sel,
                        selected_key=selected_key,
                        select_value=tnum,
                        select_button_key=f"sel_{tnum}",
                        row_data=t,
                        row_id=tnum,
                        row_actions_fn=row_actions_fn,
                        case_type_session_key=case_type_session_key,
                    )


def render_ticket_table_fast(
    tickets: list[dict[str, Any]],
    *,
    selected: str | None,
    selected_key: str,
    row_actions_fn: Callable[[dict[str, Any], str], None] | None = None,
    show_case_type: bool = False,
    case_type_session_key: str | None = None,
) -> None:
    """Ticket grid — same column ratios as the full table, without per-row card containers."""
    del selected_key, case_type_session_key
    if not tickets:
        st.markdown(
            '<div class="disp-table-empty">No tickets in this queue</div>',
            unsafe_allow_html=True,
        )
        return

    col_ratios = (
        list(_TICKET_TABLE_COLS_WITH_TYPE)
        if show_case_type
        else list(_TICKET_TABLE_COLS)
    )
    header_labels = (
        ["Ticket", "Type", "Category", "Engineer", "Notes", "Elapsed", "Status"]
        if show_case_type
        else ["Ticket", "Category", "Engineer", "Notes", "Elapsed", "Status"]
    )
    with st.container(key="disp_table_header"):
        header_cols = st.columns(col_ratios, gap="small")
        for col, label in zip(header_cols[:-1], header_labels, strict=True):
            with col:
                st.markdown(
                    f'<p class="disp-col-head">{html.escape(label)}</p>',
                    unsafe_allow_html=True,
                )

    with st.container(key="disp_ticket_table_fast"):
        for idx, t in enumerate(tickets):
            tnum = str(t.get("ticket_number") or "")
            is_sel = selected == tnum
            cell = _fast_row_cell_style(is_sel=is_sel, is_alt=idx % 2 == 1)
            row_cols = st.columns(
                col_ratios, gap="small", vertical_alignment="center"
            )
            col_idx = 0

            with row_cols[col_idx]:
                fu_dot = ""
                if (
                    str(t.get("case_type") or "") == "Residential"
                    and str(t.get("follow_up_at") or "").strip()
                ):
                    fu_dot = (
                        '<span style="font-size:12px;color:#a78bfa;margin-left:4px">●</span>'
                    )
                st.markdown(
                    f'<div style="{cell}"><span class="disp-ticket-num">'
                    f"{html.escape(tnum)}{fu_dot}</span></div>",
                    unsafe_allow_html=True,
                )
            col_idx += 1

            if show_case_type:
                with row_cols[col_idx]:
                    st.markdown(
                        f'<div style="{cell}">'
                        f"{_case_type_pill_html(str(t.get('case_type') or ''))}</div>",
                        unsafe_allow_html=True,
                    )
                col_idx += 1

            c_cat, c_eng, c_notes, c_elapsed, c_status, c_actions = row_cols[
                col_idx : col_idx + 6
            ]
            with c_cat:
                st.markdown(
                    f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                    f'color:#4a5a7a">'
                    f'{html.escape(str(t.get("task_category") or "—"))}</span></div>',
                    unsafe_allow_html=True,
                )
            with c_eng:
                st.markdown(
                    f'<div style="{cell}">{_ticket_row_engineer_html(t)}</div>',
                    unsafe_allow_html=True,
                )
            with c_notes:
                st.markdown(
                    f'<div style="{cell};min-width:0"><span style="font-size:13px;'
                    f"font-weight:400;white-space:nowrap;overflow:hidden;"
                    f'text-overflow:ellipsis;display:block;min-width:0">'
                    f"{_ticket_row_notes_html(t)}</span></div>",
                    unsafe_allow_html=True,
                )
            with c_elapsed:
                st.markdown(
                    f'<div style="{cell}">{_ticket_row_elapsed_html(t)}</div>',
                    unsafe_allow_html=True,
                )
            with c_status:
                st.markdown(
                    f'<div style="{cell}">{status_pill(str(t.get("status") or ""))}</div>',
                    unsafe_allow_html=True,
                )
            with c_actions:
                if row_actions_fn:
                    row_actions_fn(t, tnum)


DISPATCH_TICKET_PAGE_SIZE = 20
_DISP_TICKET_PAGE_KEY = "disp_ticket_page"
_DISP_TICKET_PAGE_SIG_KEY = "disp_ticket_page_sig"


def paginate_ticket_rows(
    rows: list[dict[str, Any]],
    *,
    page: int,
    page_size: int = DISPATCH_TICKET_PAGE_SIZE,
) -> tuple[list[dict[str, Any]], int, int, int, int, int]:
    """Return (page_rows, page, total_pages, total, range_start, range_end) — 1-based page."""
    total = len(rows)
    if total == 0:
        return [], 1, 1, 0, 0, 0
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(int(page), total_pages))
    start = (page - 1) * page_size
    end = min(start + page_size, total)
    return rows[start:end], page, total_pages, total, start + 1, end


def ticket_page_for_index(index: int, *, page_size: int = DISPATCH_TICKET_PAGE_SIZE) -> int:
    """1-based page number for a zero-based row index."""
    if index < 0:
        return 1
    return index // page_size + 1


def render_ticket_table_pager(
    *,
    page: int,
    total_pages: int,
    total: int,
    range_start: int,
    range_end: int,
    page_size: int = DISPATCH_TICKET_PAGE_SIZE,
    key_prefix: str = "disp_ticket",
) -> None:
    """Prev / Next controls below the ticket table."""
    if total <= page_size:
        return
    with st.container(key="disp_ticket_pager"):
        c_prev, c_label, c_next = st.columns([1, 2, 1], gap="small")
        with c_prev:
            if st.button(
                "◀ Prev",
                key=f"{key_prefix}_page_prev",
                disabled=page <= 1,
                use_container_width=True,
            ):
                st.session_state[_DISP_TICKET_PAGE_KEY] = page - 1
        with c_label:
            st.markdown(
                f'<p class="disp-pager-label">'
                f"Page <strong>{page}</strong> of <strong>{total_pages}</strong> · "
                f"<strong>{range_start}–{range_end}</strong> of <strong>{total}</strong></p>",
                unsafe_allow_html=True,
            )
        with c_next:
            if st.button(
                "Next ▶",
                key=f"{key_prefix}_page_next",
                disabled=page >= total_pages,
                use_container_width=True,
            ):
                st.session_state[_DISP_TICKET_PAGE_KEY] = page + 1


def prepare_dispatch_ticket_page(
    rows: list[dict[str, Any]],
    *,
    context_sig: str,
    selected: str | None = None,
    jump_to_selection: bool = False,
    page_size: int = DISPATCH_TICKET_PAGE_SIZE,
) -> tuple[list[dict[str, Any]], int, int, int, int, int]:
    """Reset or jump page on filter change / lookup; return paginated slice."""
    if st.session_state.get(_DISP_TICKET_PAGE_SIG_KEY) != context_sig:
        st.session_state[_DISP_TICKET_PAGE_SIG_KEY] = context_sig
        st.session_state[_DISP_TICKET_PAGE_KEY] = 1

    sel = str(selected or "").strip()
    if sel and jump_to_selection:
        ids = [str(r.get("ticket_number") or "") for r in rows]
        if sel in ids:
            st.session_state[_DISP_TICKET_PAGE_KEY] = ticket_page_for_index(
                ids.index(sel),
                page_size=page_size,
            )

    page = int(st.session_state.get(_DISP_TICKET_PAGE_KEY, 1))
    page_rows, page, total_pages, total, range_start, range_end = paginate_ticket_rows(
        rows,
        page=page,
        page_size=page_size,
    )
    st.session_state[_DISP_TICKET_PAGE_KEY] = page

    if sel and sel not in {str(r.get("ticket_number") or "") for r in page_rows}:
        ids = [str(r.get("ticket_number") or "") for r in rows]
        if sel in ids:
            page = ticket_page_for_index(ids.index(sel), page_size=page_size)
            st.session_state[_DISP_TICKET_PAGE_KEY] = page
            page_rows, page, total_pages, total, range_start, range_end = paginate_ticket_rows(
                rows,
                page=page,
                page_size=page_size,
            )

    return page_rows, page, total_pages, total, range_start, range_end


def render_nudge_banner(tickets: list[dict[str, Any]]) -> None:
    from unattended import should_show_dashboard_cutoff_warning

    near: list[str] = []
    for t in tickets:
        if should_show_dashboard_cutoff_warning(t):
            near.append(str(t.get("ticket_number") or ""))
    if not near:
        return
    nums = html.escape(", ".join(n for n in near if n))
    st.markdown(
        f"""
    <div style="background:#1f1506;border:0.5px solid #3d2a0a;border-radius:5px;
      padding:8px 12px;display:flex;align-items:flex-start;gap:8px;margin-bottom:8px">
      <span style="font-size:15px;color:#f59e0b;flex-shrink:0;margin-top:1px">⚠</span>
      <span style="font-size:14px;font-weight:400;color:#d97706;line-height:1.5">
        Ticket{"s" if len(near) > 1 else ""}
        <strong style="font-weight:500;color:#f59e0b">{nums}</strong>
        approaching end-of-day cutoff — no field response yet.
      </span>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_engineer_row(eng: dict[str, Any]) -> None:
    username = html.escape(str(eng.get("username") or ""))
    initials = html.escape(_engineer_initials(username))
    active = int(eng.get("active_tickets") or 0)
    online = bool(eng.get("online"))
    status_dot = "#22c55e" if online else "#2a3a5a"
    sub = f"{active} active" if active else "Off shift"
    st.markdown(
        f"""
    <div style="display:flex;align-items:center;gap:8px;padding:5px 6px;
      border-radius:4px;margin-bottom:2px">
      <div style="width:24px;height:24px;border-radius:50%;background:#0d1e3a;
        display:flex;align-items:center;justify-content:center;
        font-size:10px;font-weight:600;color:#3b82f6;flex-shrink:0">{initials}</div>
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;font-weight:400;color:#8a9ac0">{username}</div>
        <div style="font-size:11px;font-weight:400;color:#2a3a5a">{html.escape(sub)}</div>
      </div>
      <div style="width:5px;height:5px;border-radius:50%;
        background:{status_dot};flex-shrink:0"></div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_timeline_entry(log: dict[str, Any], *, is_last: bool, tz: timezone) -> None:
    action = str(log.get("action_type") or "")
    dot_color = TIMELINE_DOT.get(action, "#1a2035")
    member = html.escape(str(log.get("member_username") or "—"))
    when = html.escape(format_utc5(log.get("timestamp"), tz=tz))
    note_raw = str(log.get("note") or "").strip()
    note_html = ""
    if note_raw:
        note_html = (
            f'<div style="font-size:12px;font-weight:400;color:#8a9ac0;line-height:1.45;'
            f"margin-top:4px;white-space:pre-wrap;word-break:break-word\">"
            f"{html.escape(note_raw)}</div>"
        )
    st.markdown(
        f"""
    <div style="display:flex;gap:8px;padding-bottom:{'0' if is_last else '10px'};position:relative">
      <div style="position:relative;flex-shrink:0">
        <div style="width:7px;height:7px;border-radius:50%;background:{dot_color};margin-top:3px"></div>
        {'<div style="position:absolute;left:3px;top:10px;bottom:0;width:0.5px;background:#1a2035"></div>' if not is_last else ''}
      </div>
      <div>
        <div style="font-size:13px;font-weight:400;color:#4a5a7a;line-height:1.4">{html.escape(action)} · {member}</div>
        <div style="font-size:11px;font-weight:400;color:#2a3a5a;margin-top:1px">{when}</div>
        {note_html}
      </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def active_queue_key() -> str:
    return _DISPATCH_ACTIVE_QUEUE_KEY


def render_sales_case_table(
    cases: list[dict[str, Any]],
    *,
    selected: str | None,
    selected_key: str,
    row_actions_fn: Callable[[dict[str, Any], str], None] | None = None,
) -> None:
    """Sales case rows — same column-header pattern as render_ticket_table()."""
    if not cases:
        st.markdown(
            '<div style="padding:40px;text-align:center;color:#2a3a5a;font-size:13px">'
            "No cases in this queue</div>",
            unsafe_allow_html=True,
        )
        return

    h1, h2, h3, h4, h5, h6, h7 = st.columns(
        [1.25, 2.1, 1.0, 1.3, 1.2, 1.2, 0.72], gap="small"
    )
    for col, label in zip(
        [h1, h2, h3, h4, h5, h6],
        ["Case ref", "Account", "Region", "Engineer", "Priority", "Status"],
    ):
        with col:
            st.markdown(
                f'<p style="font-size:11px;font-weight:600;color:#2a3a5a;'
                f'text-transform:uppercase;letter-spacing:.05em;margin:0 0 4px">'
                f"{html.escape(label)}</p>",
                unsafe_allow_html=True,
            )
    st.markdown(
        '<hr style="border-color:#1a2035;margin:0 0 4px">',
        unsafe_allow_html=True,
    )

    with st.container(key="disp_ticket_table"):
        for c in cases:
            cref = str(c.get("case_ref") or "")
            row_id = str(c.get("id") or cref)
            is_sel = selected == cref
            cell = _table_cell_style(is_sel=is_sel)

            with st.container(key=f"disp_ticket_row_{row_id}"):
                if is_sel:
                    st.markdown(
                        '<span class="disp-row-selected" aria-hidden="true"></span>',
                        unsafe_allow_html=True,
                    )
                c1, c2, c3, c4, c5, c6, c7 = st.columns(
                    [1.25, 2.1, 1.0, 1.3, 1.2, 1.2, 0.72],
                    gap="small",
                    vertical_alignment="center",
                )

                with c1:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:500;'
                        f'color:#8a9ac0;font-variant-numeric:tabular-nums">'
                        f"{html.escape(cref)}</span></div>",
                        unsafe_allow_html=True,
                    )
                with c2:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;color:#8a9ac0;'
                        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
                        f'{html.escape(str(c.get("account_name") or ""))}</span></div>',
                        unsafe_allow_html=True,
                    )
                with c3:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:11px;color:#4a5a7a">'
                        f'{html.escape(str(c.get("account_region") or "—"))}</span></div>',
                        unsafe_allow_html=True,
                    )
                with c4:
                    eng = str(c.get("assigned_to") or "").strip()
                    if eng:
                        eng_html = (
                            f'<span style="font-size:13px;color:#8a9ac0">'
                            f"{html.escape(eng)}</span>"
                        )
                    else:
                        eng_html = (
                            '<span style="font-size:13px;color:#4a5a7a;font-style:italic">'
                            "unassigned</span>"
                        )
                    st.markdown(f'<div style="{cell}">{eng_html}</div>', unsafe_allow_html=True)
                with c5:
                    pri = str(c.get("sales_priority") or "Standard")
                    color = "#ef4444" if pri == "High" else "#4a5a7a"
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;color:{color}">'
                        f"{html.escape(pri)}</span></div>",
                        unsafe_allow_html=True,
                    )
                with c6:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:11px;font-weight:500;'
                        f'padding:2px 6px;border-radius:3px;background:#1a2035;color:#8a9ac0">'
                        f'{html.escape(str(c.get("status") or ""))}</span></div>',
                        unsafe_allow_html=True,
                    )
                with c7:
                    _render_table_row_actions(
                        row_key=row_id,
                        is_sel=is_sel,
                        selected_key=selected_key,
                        select_value=cref,
                        select_button_key=f"sel_sc_{row_id}",
                        row_data=c,
                        row_id=row_id,
                        row_actions_fn=row_actions_fn,
                    )
