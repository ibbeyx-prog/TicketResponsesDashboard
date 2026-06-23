"""NetOps Coverage Eye — dispatch console UI (dark ops theme + layout helpers)."""

from __future__ import annotations

import html
from datetime import datetime, timezone
from collections.abc import Callable
from typing import Any

import streamlit as st

# Horizontal inset for header/body content (block-container itself is full-bleed).
_DISP_INSET = "10px"
_DISP_BODY_TOP = "6px"
_DISP_HEADER_H = "56px"
UI_MIN_FONT_PX = 11

# Shared design tokens (CSS variables for embedded HTML components).
_DISPATCH_VARS = f"""
:root {{
  --disp-min-font: {UI_MIN_FONT_PX}px;
  --disp-bg: #0b0f18;
  --disp-panel: #080b14;
  --disp-card: #0d1220;
  --disp-border: #1a2035;
  --disp-text: #e2e8f8;
  --disp-muted: #8a9ac0;
  --disp-dim: #2a3a5a;
  --disp-accent: #3b82f6;
  --disp-accent-bg: #0d1e3a;
  --disp-green: #22c55e;
  --disp-amber: #f59e0b;
  --disp-red: #ef4444;
  --disp-purple: #a78bfa;
  --disp-header-h: {_DISP_HEADER_H};
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
  background: #0b0f18 !important;
  border-bottom: 0.5px solid var(--disp-border) !important;
  padding: 0 {_DISP_INSET} !important;
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
  justify-content: flex-start !important;
}}
div.st-key-disp_main_nav_tabs [data-testid="stHorizontalBlock"] {{
  display: flex !important;
  flex-direction: row !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  justify-content: flex-start !important;
  width: auto !important;
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
  color: #6b7f9e !important;
  font-size:14px !important;
  font-weight: 400 !important;
  letter-spacing: 0 !important;
  padding: 0 14px !important;
  min-height: var(--disp-header-h) !important;
  height: var(--disp-header-h) !important;
  line-height: 1.1 !important;
  white-space: nowrap !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button[kind="primary"],
div.st-key-disp_main_nav_tabs .stButton > button[data-testid="baseButton-primary"],
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-primary"] {{
  color: #e2e8f8 !important;
  font-weight: 500 !important;
  border-bottom-color: #3b82f6 !important;
  background: transparent !important;
  background-color: transparent !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button:hover,
div.st-key-disp_main_nav_tabs .stButton > button[kind="secondary"]:hover,
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-secondary"]:hover {{
  color: #8a9ac0 !important;
  background: transparent !important;
  background-color: transparent !important;
  border-color: transparent !important;
  border-bottom-color: transparent !important;
  box-shadow: none !important;
}}
div.st-key-disp_main_nav_tabs .stButton > button[kind="primary"]:hover,
div.st-key-disp_main_nav_tabs button[data-testid="stBaseButton-primary"]:hover {{
  color: #e2e8f8 !important;
  border-bottom-color: #3b82f6 !important;
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
  justify-content: flex-start !important;
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
  flex: 0 0 auto !important;
  min-width: 0 !important;
  overflow: visible !important;
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
  font-size:14px !important;
  font-weight: 400 !important;
  letter-spacing: .01em !important;
  color: #8a9ac0 !important;
  white-space: nowrap !important;
  padding: 6px 12px !important;
  min-height: 32px !important;
  height: 32px !important;
  min-width: max-content !important;
  border: 0.5px solid #2a3a5a !important;
  border-radius: 999px !important;
  background: transparent !important;
  line-height: 1.2 !important;
  box-shadow: none !important;
}}
div.st-key-disp_header_shell [data-testid="stPopover"] > button:hover,
div.st-key-disp_header_right [data-testid="stPopover"] > button:hover {{
  border-color: #4a5a7a !important;
  color: #8a9ac0 !important;
  background: #0d1220 !important;
}}
div.st-key-disp_header_shell [data-testid="stPopoverBody"],
div.st-key-disp_header_right [data-testid="stPopoverBody"] {{
  min-width: 280px !important;
  max-width: min(420px, 92vw) !important;
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
  background: #080b14 !important;
  border: 0.5px solid var(--disp-border) !important;
  border-radius: 6px !important;
  overflow: hidden !important;
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
  border-bottom-color: #3b82f6 !important;
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
}}
div.st-key-disp_right_rail div.st-key-disp_assign_panel .disp-mode-caption {{
  font-size: 11px !important;
  color: #4a5a7a !important;
  margin: 0 0 10px 0 !important;
  line-height: 1.4 !important;
}}
div.st-key-disp_sidebar_inner {{
  padding: 0 2px 0 0 !important;
}}
div.st-key-disp_sidebar_queues {{
  margin-top: 2px !important;
  margin-bottom: 4px !important;
}}
div.st-key-disp_sidebar_queues [data-testid="stVerticalBlock"] {{
  gap: 3px !important;
}}
div.st-key-disp_sidebar_queues .stButton {{
  margin: 0 !important;
  width: 100% !important;
}}
.disp-today-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px 8px;
  margin: 0 0 2px 0;
  padding: 0;
}}
.disp-today-cell {{
  min-width: 0;
}}
.disp-today-label {{
  font-size: 11px;
  font-weight: 400;
  color: #2a3a5a;
  line-height: 1.2;
  margin-bottom: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.disp-today-value {{
  font-size: 20px;
  font-weight: 500;
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
div.st-key-disp_detail_panel [data-testid="stExpander"] {{
  margin: 8px 0 0 !important;
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
  font-size:22px;
  font-weight: 600;
  color: #3b82f6;
  letter-spacing: .06em;
  text-transform: uppercase;
  line-height: 1;
  white-space: nowrap;
  margin: 0;
  padding: 0;
  display: inline-flex;
  align-items: center;
  height: var(--disp-header-h);
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
.disp-header-clock {{
  display: inline-block;
  font-size:14px;
  font-weight: 400;
  color: #3b82f6;
  letter-spacing: .01em;
  background: #0d1e3a;
  border: 0.5px solid #1a3460;
  padding: 6px 12px;
  border-radius: 999px;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
  line-height: 1.2;
}}
.disp-header-op {{
  display: inline-flex;
  align-items: center;
  gap: 7px;
  font-size:14px;
  font-weight: 400;
  color: #8a9ac0;
  background: transparent !important;
  white-space: nowrap;
  line-height: 1.2;
  user-select: none;
}}
.disp-header-op-dot {{
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: #22c55e;
  display: inline-block;
  flex-shrink: 0;
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
[class*="st-key-sales_queue_"] .stButton > button {{
  font-size: 12px !important;
  font-weight: 400 !important;
  text-align: left !important;
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  padding: 5px 8px !important;
  min-height: 30px !important;
  height: 30px !important;
  white-space: nowrap !important;
  overflow: hidden !important;
  border-radius: 5px !important;
  width: 100% !important;
}}
[class*="st-key-disp_queue_"] .stButton > button::before,
[class*="st-key-sales_queue_"] .stButton > button::before {{
  content: "●" !important;
  font-size: 7px !important;
  line-height: 1 !important;
  flex: 0 0 auto !important;
  margin-right: 7px !important;
}}
[class*="st-key-disp_queue_"] .stButton > button::after,
[class*="st-key-sales_queue_"] .stButton > button::after {{
  font-size: var(--disp-min-font) !important;
  font-weight: 400 !important;
  color: #8a9ac0 !important;
  font-variant-numeric: tabular-nums !important;
  flex: 0 0 auto !important;
  margin-left: auto !important;
  padding-left: 8px !important;
  line-height: 1 !important;
}}
[class*="st-key-disp_queue_"].disp-queue-active .stButton > button,
[class*="st-key-sales_queue_"].disp-queue-active .stButton > button {{
  background: #0d1e3a !important;
  border-color: #1a3460 !important;
  color: #e2e8f8 !important;
}}
[class*="st-key-disp_queue_"] .stButton > button[kind="primary"],
[class*="st-key-disp_queue_"] .stButton > button[data-testid="stBaseButton-primary"],
[class*="st-key-sales_queue_"] .stButton > button[kind="primary"],
[class*="st-key-sales_queue_"] .stButton > button[data-testid="stBaseButton-primary"] {{
  background: #0d1e3a !important;
  border-color: #1a3460 !important;
  color: #e2e8f8 !important;
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
  border-bottom-color: #3b82f6 !important;
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

/* Ticket / sales table — rows + action buttons (● select + ⋮ menu) */
div.st-key-disp_ticket_table > [data-testid="stVerticalBlock"] {{
  gap: 8px !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="element-container"] {{
  margin: 0 !important;
  padding: 0 !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="stHorizontalBlock"] {{
  align-items: center !important;
  flex-wrap: nowrap !important;
  gap: 6px !important;
}}
div[class*="st-key-disp_ticket_row_"] [data-testid="column"] {{
  display: flex !important;
  align-items: center !important;
  align-self: center !important;
  min-height: 34px !important;
  max-height: 34px !important;
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
  justify-content: flex-end !important;
  gap: 10px !important;
  width: 100% !important;
  min-height: 34px !important;
  max-height: 34px !important;
  padding: 5px 4px !important;
  margin: 0 !important;
  box-sizing: border-box !important;
  border: 0.5px solid transparent !important;
  border-radius: 4px !important;
}}
div[class*="st-key-disp_ticket_row_"]:has([data-testid="stBaseButton-primary"]) div[class*="st-key-disp_row_actions_"] {{
  background: #0d1e3a !important;
  border-color: #1a3460 !important;
}}
div[class*="st-key-disp_row_actions_"] [data-testid="element-container"] {{
  margin: 0 !important;
  padding: 0 !important;
  flex: 0 0 auto !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] {{
  flex: 0 0 auto !important;
  width: 30px !important;
  min-width: 30px !important;
  max-width: 30px !important;
  margin: 0 !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton > button,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button {{
  font-size: 12px !important;
  font-weight: 600 !important;
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
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  overflow: hidden !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton > button[kind="primary"],
div[class*="st-key-disp_row_actions_"] .stButton > button[data-testid="stBaseButton-primary"] {{
  color: #3b82f6 !important;
  border-color: #1a3460 !important;
  background: #0d1e3a !important;
}}
div[class*="st-key-disp_row_actions_"] .stButton > button:hover,
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button:hover {{
  color: #8a9ac0 !important;
  border-color: #3b465c !important;
  background: #121a2a !important;
}}
div[class*="st-key-disp_row_actions_"] [data-testid="stPopover"] > button {{
  font-size: 16px !important;
  font-weight: 500 !important;
  letter-spacing: 0 !important;
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
}
[data-testid="stDataFrame"] div,
[data-testid="stDataEditor"] div {
  background: var(--disp-card) !important;
}
[data-testid="stDataFrame"] [data-testid="glideDataEditor"],
[data-testid="stDataEditor"] [data-testid="glideDataEditor"] {
  --gdg-bg-cell: #0d1220;
  --gdg-bg-header: #080b14;
  --gdg-bg-header-has-focus: #121a2a;
  --gdg-bg-header-hovered: #121a2a;
  --gdg-text-dark: #e2e8f8;
  --gdg-text-medium: #8a9ac0;
  --gdg-text-light: #4a5a7a;
  --gdg-text-header: #8a9ac0;
  --gdg-border-color: #1a2035;
  --gdg-accent-color: #3b82f6;
  --gdg-accent-light: rgba(59, 130, 246, 0.15);
  --gdg-accent-fg: #e2e8f8;
  --gdg-bg-cell-medium: #0d1220;
  --gdg-bg-search-cell: #121a2a;
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

TIMELINE_DOT: dict[str, str] = {
    "Assignment": "#3b82f6",
    "Response": "#22c55e",
    "Nudge": "#f59e0b",
    "AutoUnattended": "#ef4444",
    "OnHold": "#f59e0b",
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
    if label == "Unattended":
        return (
            f'<span style="font-size:11px;font-weight:500;padding:2px 6px;'
            f'border-radius:3px;background:#2d1515;color:#ef4444;'
            f'white-space:nowrap">{html.escape(label)}</span>'
        )
    safe = html.escape(label)
    return (
        f'<span style="font-size:11px;font-weight:500;padding:2px 6px;'
        f'border-radius:3px;background:#1a2035;color:#8a9ac0;'
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


def render_topbar(*, operator_id: str, now_label: str, section: str = "CSM Cases") -> None:
    """Legacy single-row header (prefer unified shell in app.py)."""
    op = html.escape(operator_id or "—")
    st.markdown(
        f"""
    <div class="disp-full-bleed" style="
      background:#0b0f18;border-bottom:0.5px solid #1a2035;
      height:var(--disp-header-h);display:flex;align-items:center;padding:0 {_DISP_INSET};
      position:sticky;top:0;z-index:999;gap:0;margin-bottom:0.5rem
    ">
      <span class="disp-brand" style="padding-right:18px;border-right:0.5px solid #1a2035">
        NetOps · Coverage Eye
      </span>
      <span style="font-size:15px;color:#2a3a5a;padding:0 16px">{html.escape(section)}</span>
      <div style="margin-left:auto;display:flex;align-items:center;gap:10px">
        <span class="disp-header-clock">{html.escape(now_label)}</span>
        <span class="disp-header-op">
          <span class="disp-header-op-dot"></span>
          {op}
        </span>
      </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_sidebar_today_grid(
    items: tuple[tuple[str, int | str, str], ...],
) -> None:
    """Compact 2×2 today stats — aligned with queue buttons below."""
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
            dot = dots.get(q, "#4a5a7a")
            cnt = counts.get(q, 0)
            is_active = q == selected
            row_key = q.replace(" ", "_")
            btn_key = f"{button_key_prefix}_{row_key}"
            st.markdown(
                f"<style>"
                f"div.st-key-{btn_key} .stButton > button::before {{"
                f"color: {dot} !important;"
                f"}}"
                f"div.st-key-{btn_key} .stButton > button::after {{"
                f'content: "{cnt}";'
                f"}}"
                f"</style>",
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
) -> None:
    """Compact settings popover for the top bar."""
    with st.popover("⚙ Settings", use_container_width=False):
        st.markdown(
            '<p style="font-size:11px;font-weight:600;color:#2a3a5a;'
            'text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Refresh</p>',
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
            '<p style="font-size:11px;font-weight:600;color:#2a3a5a;'
            'text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Date range</p>',
            unsafe_allow_html=True,
        )
        menu_labels = [o for o in time_preset_options if o != "Pick dates"]
        display_opts = menu_labels + ["Custom"]
        cur = str(st.session_state.get(time_preset_key, "This week"))
        if cur == "Pick dates":
            cur = "Custom"
        if cur not in display_opts:
            cur = display_opts[0] if display_opts else "Today"
        range_opt = st.selectbox(
            "Range",
            display_opts,
            index=display_opts.index(cur),
            label_visibility="collapsed",
            key="settings_range",
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
                '<p style="font-size:11px;font-weight:600;color:#2a3a5a;'
                'text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">'
                "Admin</p>",
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


def _table_cell_style(*, is_sel: bool) -> str:
    """Shared row cell chrome for ticket/sales tables."""
    row_bg = "#0d1e3a" if is_sel else "transparent"
    row_pad = "5px 8px" if is_sel else "5px 6px"
    row_radius = "4px" if is_sel else "0"
    border = "0.5px solid #1a3460" if is_sel else "0.5px solid transparent"
    return (
        f"padding:{row_pad};background:{row_bg};border-radius:{row_radius};"
        f"min-height:34px;display:flex;align-items:center;box-sizing:border-box;"
        f"width:100%;border:{border};white-space:nowrap;overflow:hidden;"
    )


def _render_table_row_actions(
    *,
    row_key: str,
    is_sel: bool,
    selected_key: str,
    select_value: str,
    select_button_key: str,
    row_data: dict[str, Any],
    row_id: str,
    row_actions_fn: Callable[[dict[str, Any], str], None] | None,
) -> None:
    """Select (●) + menu (⋮) in one horizontal action strip."""
    with st.container(
        horizontal=True,
        vertical_alignment="center",
        key=f"disp_row_actions_{row_key}",
    ):
        if st.button(
            "●",
            key=select_button_key,
            help="Select row",
            type="primary" if is_sel else "secondary",
        ):
            st.session_state[selected_key] = select_value
            st.rerun()
        if row_actions_fn:
            row_actions_fn(row_data, row_id)


def render_ticket_table(
    tickets: list[dict[str, Any]],
    *,
    selected: str | None,
    selected_key: str,
    row_actions_fn: Callable[[dict[str, Any], str], None] | None = None,
) -> None:
    """Render ticket rows as Streamlit columns (supports per-row popovers)."""
    if not tickets:
        st.markdown(
            '<div style="padding:40px;text-align:center;color:#2a3a5a;'
            'font-size:13px;font-weight:400">'
            "No tickets in this queue</div>",
            unsafe_allow_html=True,
        )
        return

    h1, h2, h3, h4, h5, h6, h7 = st.columns(
        list(_TICKET_TABLE_COLS), gap="small"
    )
    for col, label in zip(
        [h1, h2, h3, h4, h5, h6],
        ["Ticket", "Category", "Engineer", "Notes", "Elapsed", "Status"],
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
        for t in tickets:
            tnum = str(t.get("ticket_number") or "")
            is_sel = selected == tnum
            cell = _table_cell_style(is_sel=is_sel)

            with st.container(key=f"disp_ticket_row_{tnum}"):
                c1, c2, c3, c4, c5, c6, c7 = st.columns(
                    list(_TICKET_TABLE_COLS),
                    gap="small",
                    vertical_alignment="center",
                )

                with c1:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:500;'
                        f'color:#8a9ac0;font-variant-numeric:tabular-nums">'
                        f"{html.escape(tnum)}</span></div>",
                        unsafe_allow_html=True,
                    )
                with c2:
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                        f'color:#4a5a7a">'
                        f'{html.escape(str(t.get("task_category") or "—"))}</span></div>',
                        unsafe_allow_html=True,
                    )
                with c3:
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
                with c4:
                    notes = html.escape(str(t.get("additional_info") or ""))
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                        f'color:#4a5a7a;white-space:nowrap;overflow:hidden;'
                        f'text-overflow:ellipsis;display:block;min-width:0">'
                        f"{notes}</span></div>",
                        unsafe_allow_html=True,
                    )
                with c5:
                    el_color, el_label = _row_elapsed(t)
                    st.markdown(
                        f'<div style="{cell}"><span style="font-size:13px;font-weight:400;'
                        f'color:{el_color};font-variant-numeric:tabular-nums">'
                        f"{html.escape(el_label)}</span></div>",
                        unsafe_allow_html=True,
                    )
                with c6:
                    st.markdown(
                        f'<div style="{cell}">{status_pill(str(t.get("status") or ""))}</div>',
                        unsafe_allow_html=True,
                    )
                with c7:
                    _render_table_row_actions(
                        row_key=tnum,
                        is_sel=is_sel,
                        selected_key=selected_key,
                        select_value=tnum,
                        select_button_key=f"sel_{tnum}",
                        row_data=t,
                        row_id=tnum,
                        row_actions_fn=row_actions_fn,
                    )


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
