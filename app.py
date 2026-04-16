"""Module Yield Dashboard - Main Streamlit Application."""

import logging
import importlib
import os
import re
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from urllib.parse import urlencode, parse_qs
import base64
import io

from src.frpt_runner import FrptRunner, FrptCommand

# Force reload frpt_parser to pick up latest bin parsing changes
from src import frpt_parser
importlib.reload(frpt_parser)
from src.frpt_parser import FrptParser

# Force reload data_processor to pick up latest changes
from src import data_processor
importlib.reload(data_processor)
from src.data_processor import DataProcessor
from src.cache import FrptCache
from src.pdf_report import create_dashboard_pdf, export_chart_to_png
from src.html_export import create_smt6_html_report

# Force reload fiscal_calendar to pick up latest changes
from src import fiscal_calendar
importlib.reload(fiscal_calendar)
from src.fiscal_calendar import get_fiscal_month, get_workweek_labels_with_months, get_calendar_year_month

# Force reload failcrawler module to pick up latest changes
from src import failcrawler
importlib.reload(failcrawler)
from src.failcrawler import (
    fetch_failcrawler_data,
    fetch_msn_status_correlation_data,
    fetch_msn_status_fid_counts,
    fetch_total_uin_by_step,
    process_failcrawler_data,
    create_failcrawler_chart,
    create_failcrawler_summary_table,
    create_pareto_summary_html,
    create_weekly_cdpm_table_html,
    process_msn_status_correlation,
    create_msn_status_correlation_chart,
    create_msn_status_ranked_table_html,
    # New DPM metric functions
    fetch_cdpm_data,
    fetch_mdpm_data,
    fetch_fcdpm_data,
    fetch_all_dpm_metrics,
    create_dpm_metrics_summary_html,
    create_dpm_comparison_table_html,
    # Decode quality functions
    fetch_fcfm_decode_quality,
    calculate_decode_quality,
    create_decode_quality_html,
    # Trend detection and alerts
    detect_excursions,
    create_alert_summary_html,
    # Top Movers
    calculate_failcrawler_wow_changes,
    create_top_movers_html,
    # Drill-down
    fetch_failcrawler_msn_drilldown,
    create_failcrawler_drilldown_html,
    get_failcrawler_list_for_step,
    get_msn_status_list_for_step,
)

# SMT6 yield module
from src import smt6_yield
importlib.reload(smt6_yield)
from src.smt6_yield import (
    fetch_smt6_yield_data,
    fetch_smt6_site_data,
    create_smt6_yield_chart,
    create_smt6_summary_table,
    create_machine_yield_cards,
    create_site_yield_heatmap,
    create_machine_socket_heatmap,
    create_socket_drilldown_heatmap,
    create_site_summary_table,
    create_site_grid_html,
    create_multi_machine_socket_grid,
    create_slice_channel_map_html,
    get_slice_list,
    get_smt6_cache_stats,
    analyze_site_trends,
    create_site_trend_heatmap,
    create_site_trend_summary_html,
    create_site_channel_summary_html,
)

# GRACE Motherboard monitoring module
from src import grace_motherboard
importlib.reload(grace_motherboard)
from src.grace_motherboard import (
    fetch_grace_health_data,
    aggregate_weekly_health,
    calculate_rolling_metrics,
    aggregate_by_machine,
    get_health_status,
    # New FM-based analysis functions
    fetch_grace_fm_data,
    get_hang_machines,
    compare_weeks,
    analyze_hang_failures,
    analyze_machines_100pct_fails,
    get_previous_workweek,
)
from config.settings import Settings
from config.yield_targets import (
    HMFN_TARGETS, SLT_TARGETS, ELC_TARGETS,
    get_target, get_available_configs, normalize_speed, normalize_density
)

# ELC Curve History for target comparison
from config import curve_history
importlib.reload(curve_history)
from config.curve_history import (
    CURVE_ORDER, ACTIVE_CURVE, CURVE_INFO, CURVE_DELTAS,
    ELC_CURVES, get_curve_target, get_curve_history_for_config,
    get_available_configs_for_curve
)

# Fail Viewer module
from fail_viewer import (
    create_fail_viewer,
    create_fail_heatmap,
    create_dq_distribution,
    create_bank_distribution,
    load_fail_csv,
    process_fail_data,
    load_geometry,
    add_repair_overlay,
    create_mock_repair_data,
    apply_did_equations,
    get_repair_summary,
    # Real repair loading
    load_repair_data,
    get_available_repair_sources,
    get_repair_info_from_mtsums,
)


# Configure logging to file for debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/asegaran/MODULE_YIELD_DASHBOARD/dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Suppress noisy loggers
logging.getLogger('watchdog').setLevel(logging.WARNING)

# Constants
MAX_WEEKS_PER_YEAR = 52
MIN_YEAR = 2020
MAX_YEAR = 2030


def get_current_workweek() -> str:
    """Get current work week in YYYYWW format (Micron Fri-Thu weeks).

    Micron fiscal weeks run Friday to Thursday:
    - If today is Mon-Thu, we're in the previous ISO week's Micron week
    - If today is Fri-Sun, we're in the current ISO week's Micron week
    """
    now = datetime.now()
    iso_year, iso_week, iso_day = now.isocalendar()

    # Micron week adjustment: Mon-Thu (days 1-4) belong to previous week
    if iso_day <= 4:  # Monday=1 through Thursday=4
        micron_week = iso_week - 1
        # Handle year boundary (week 0 means week 52 of previous year)
        if micron_week <= 0:
            micron_week = 52
            iso_year -= 1
    else:  # Friday=5 through Sunday=7
        micron_week = iso_week

    return f"{iso_year}{micron_week:02d}"


def get_4week_rolled_yields(df: pd.DataFrame) -> dict:
    """Calculate 4-week rolled yields by DID and step."""
    if df.empty:
        return {}

    # Find workweek column
    ww_col = None
    for col in ['workweek', 'MFG_WORKWEEK']:
        if col in df.columns:
            ww_col = col
            break

    if not ww_col:
        return {}

    # Find DID column
    did_col = None
    for col in ['DBASE', 'design_id', 'DESIGN_ID']:
        if col in df.columns:
            did_col = col
            break

    if not did_col:
        return {}

    # Find step column
    step_col = None
    for col in ['STEP', 'step']:
        if col in df.columns:
            step_col = col
            break

    # Get last 4 workweeks
    workweeks = sorted(df[ww_col].unique())
    last_4_weeks = workweeks[-4:] if len(workweeks) >= 4 else workweeks
    df_4week = df[df[ww_col].isin(last_4_weeks)].copy()

    # Aggregate by DID and step
    group_cols = [did_col]
    if step_col:
        group_cols.append(step_col)

    rolled = df_4week.groupby(group_cols).agg({
        'UIN': 'sum',
        'UPASS': 'sum'
    }).reset_index()

    rolled['yield_pct'] = (rolled['UPASS'] / rolled['UIN'] * 100).round(2)

    # Build result dict: {(did, step): {'yield': x, 'uin': y}}
    result = {}
    for _, row in rolled.iterrows():
        did = row[did_col]
        step = row[step_col].lower() if step_col else 'all'
        result[(did, step)] = {
            'yield': row['yield_pct'],
            'uin': int(row['UIN'])
        }

    return result


def get_did_breakdown_local(df: pd.DataFrame, by_step: bool = True) -> pd.DataFrame:
    """Local function to get DID breakdown - workaround for import issues."""
    if df.empty:
        return pd.DataFrame()

    # Determine DID column name
    did_col = None
    for col in ['DBASE', 'design_id', 'DESIGN_ID']:
        if col in df.columns:
            did_col = col
            break

    if did_col is None:
        return pd.DataFrame()

    # Determine step column name
    step_col = None
    for col in ['STEP', 'step']:
        if col in df.columns:
            step_col = col
            break

    # Get latest workweek
    ww_col = None
    for col in ['workweek', 'MFG_WORKWEEK']:
        if col in df.columns:
            ww_col = col
            break

    if ww_col:
        latest_ww = df[ww_col].max()
        df = df[df[ww_col] == latest_ww].copy()

    # Build group columns
    group_cols = [did_col]
    if by_step and step_col:
        group_cols.append(step_col)

    # Aggregate
    did_summary = df.groupby(group_cols).agg({
        'UIN': 'sum',
        'UPASS': 'sum'
    }).reset_index()

    # Rename columns
    if by_step and step_col:
        did_summary.columns = ['design_id', 'step', 'uin', 'upass']
    else:
        did_summary.columns = ['design_id', 'uin', 'upass']

    did_summary['yield_pct'] = (did_summary['upass'] / did_summary['uin'] * 100).round(2)
    did_summary['ufail'] = did_summary['uin'] - did_summary['upass']

    # Sort
    if by_step and 'step' in did_summary.columns:
        step_order = {'hmfn': 0, 'slt': 1, 'elc': 2}
        did_summary['step_order'] = did_summary['step'].str.lower().map(step_order).fillna(99)
        did_summary = did_summary.sort_values(['design_id', 'step_order'])
        did_summary = did_summary.drop(columns=['step_order'])
    else:
        did_summary = did_summary.sort_values('uin', ascending=False)

    # Add workweek info
    if ww_col:
        did_summary['workweek'] = latest_ww

    return did_summary


def init_session_state() -> None:
    """Initialize session state variables."""
    if "data" not in st.session_state:
        st.session_state.data = pd.DataFrame()
    if "last_error" not in st.session_state:
        st.session_state.last_error = None
    if "fetch_in_progress" not in st.session_state:
        st.session_state.fetch_in_progress = False
    if "last_fetch_time" not in st.session_state:
        st.session_state.last_fetch_time = None
    if "last_fetch_filters" not in st.session_state:
        st.session_state.last_fetch_filters = None
    # ELC tab session state
    if "elc_data" not in st.session_state:
        st.session_state.elc_data = pd.DataFrame()
    if "elc_last_fetch_time" not in st.session_state:
        st.session_state.elc_last_fetch_time = None
    if "elc_annotations" not in st.session_state:
        st.session_state.elc_annotations = []
    # Pareto tab session state
    if "pareto_data" not in st.session_state:
        st.session_state.pareto_data = pd.DataFrame()
    if "pareto_last_fetch_time" not in st.session_state:
        st.session_state.pareto_last_fetch_time = None
    # FAILCRAWLER DPM session state
    if "failcrawler_data" not in st.session_state:
        st.session_state.failcrawler_data = pd.DataFrame()
    if "failcrawler_msn_corr_data" not in st.session_state:
        st.session_state.failcrawler_msn_corr_data = pd.DataFrame()
    if "failcrawler_fid_counts" not in st.session_state:
        st.session_state.failcrawler_fid_counts = pd.DataFrame()
    if "failcrawler_total_uin" not in st.session_state:
        st.session_state.failcrawler_total_uin = pd.DataFrame()
    if "failcrawler_cdpm_data" not in st.session_state:
        st.session_state.failcrawler_cdpm_data = pd.DataFrame()
    if "failcrawler_mdpm_data" not in st.session_state:
        st.session_state.failcrawler_mdpm_data = pd.DataFrame()
    if "failcrawler_fcfm_data" not in st.session_state:
        st.session_state.failcrawler_fcfm_data = pd.DataFrame()
    if "failcrawler_last_fetch_time" not in st.session_state:
        st.session_state.failcrawler_last_fetch_time = None
    if "failcrawler_filters" not in st.session_state:
        st.session_state.failcrawler_filters = {}
    # SMT6 yield session state
    if "smt6_data" not in st.session_state:
        st.session_state.smt6_data = pd.DataFrame()
    if "smt6_site_data" not in st.session_state:
        st.session_state.smt6_site_data = pd.DataFrame()
    if "smt6_last_fetch_time" not in st.session_state:
        st.session_state.smt6_last_fetch_time = None
    if "smt6_available_machines" not in st.session_state:
        st.session_state.smt6_available_machines = []


def inject_custom_css() -> None:
    """Inject glassmorphism theme CSS with light/dark mode support."""
    st.markdown("""
    <style>
    /* ============================================
       GLASSMORPHISM THEME - Module Yield Dashboard
       Works in both Light & Dark Modes
       ============================================ */

    /* ============================================
       LIGHT MODE STYLES (Default for Streamlit)
       ============================================ */

    /* Main title - gradient text */
    .stApp h1, .main h1, [data-testid="stHeader"] h1 {
        background: linear-gradient(90deg, #0891b2 0%, #059669 50%, #7c3aed 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
        font-weight: 800 !important;
        letter-spacing: -0.5px !important;
    }

    /* Sidebar - frosted glass light */
    section[data-testid="stSidebar"] {
        background: rgba(248, 250, 252, 0.85) !important;
        backdrop-filter: blur(20px) !important;
        -webkit-backdrop-filter: blur(20px) !important;
        border-right: 1px solid rgba(0, 0, 0, 0.08) !important;
    }

    section[data-testid="stSidebar"] > div:first-child {
        background: transparent !important;
    }

    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] .stMarkdown h2 {
        background: linear-gradient(90deg, #0891b2, #059669) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
        font-weight: 700 !important;
    }

    /* Main container */
    .main .block-container {
        background: transparent !important;
    }

    /* Metrics cards - glass effect */
    [data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.7) !important;
        backdrop-filter: blur(12px) !important;
        -webkit-backdrop-filter: blur(12px) !important;
        border: 1px solid rgba(0, 0, 0, 0.06) !important;
        border-radius: 12px !important;
        padding: 16px !important;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.06) !important;
        transition: all 0.3s ease !important;
    }

    [data-testid="stMetric"]:hover {
        border-color: #0891b2 !important;
        box-shadow: 0 8px 30px rgba(8, 145, 178, 0.15) !important;
        transform: translateY(-2px) !important;
    }

    [data-testid="stMetric"] label {
        color: #64748b !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        font-size: 0.7rem !important;
        letter-spacing: 0.5px !important;
    }

    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #1e293b !important;
        font-weight: 700 !important;
    }

    /* Tabs - glass styling */
    [data-baseweb="tab-list"] {
        gap: 4px !important;
        background: transparent !important;
    }

    button[data-baseweb="tab"] {
        background: rgba(255, 255, 255, 0.6) !important;
        backdrop-filter: blur(8px) !important;
        -webkit-backdrop-filter: blur(8px) !important;
        border: 1px solid rgba(0, 0, 0, 0.06) !important;
        border-radius: 8px 8px 0 0 !important;
        color: #64748b !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
        transition: all 0.2s ease !important;
    }

    button[data-baseweb="tab"]:hover {
        background: rgba(8, 145, 178, 0.1) !important;
        color: #0891b2 !important;
        border-color: #0891b2 !important;
    }

    button[data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, rgba(8, 145, 178, 0.15) 0%, rgba(5, 150, 105, 0.1) 100%) !important;
        color: #0891b2 !important;
        border-color: #0891b2 !important;
        border-bottom-color: transparent !important;
    }

    /* Tab content */
    [data-testid="stTabContent"] {
        background: rgba(255, 255, 255, 0.5) !important;
        backdrop-filter: blur(12px) !important;
        -webkit-backdrop-filter: blur(12px) !important;
        border: 1px solid rgba(0, 0, 0, 0.06) !important;
        border-top: none !important;
        border-radius: 0 0 12px 12px !important;
        padding: 20px !important;
    }

    /* Expanders - glass accordion */
    [data-testid="stExpander"] {
        background: rgba(255, 255, 255, 0.6) !important;
        backdrop-filter: blur(12px) !important;
        -webkit-backdrop-filter: blur(12px) !important;
        border: 1px solid rgba(0, 0, 0, 0.06) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
        margin-bottom: 12px !important;
    }

    [data-testid="stExpander"] summary {
        background: linear-gradient(90deg, rgba(8, 145, 178, 0.08) 0%, transparent 100%) !important;
        font-weight: 600 !important;
        color: #1e293b !important;
    }

    [data-testid="stExpander"] summary:hover {
        background: linear-gradient(90deg, rgba(8, 145, 178, 0.15) 0%, rgba(5, 150, 105, 0.08) 100%) !important;
    }

    /* Buttons - gradient */
    .stButton > button {
        background: linear-gradient(135deg, #0891b2 0%, #059669 100%) !important;
        color: white !important;
        font-weight: 600 !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 10px 24px !important;
        box-shadow: 0 4px 15px rgba(8, 145, 178, 0.3) !important;
        transition: all 0.3s ease !important;
    }

    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 25px rgba(8, 145, 178, 0.4) !important;
    }

    /* Multiselect tags */
    [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
        background: linear-gradient(135deg, #0891b2 0%, #059669 100%) !important;
        color: white !important;
        border-radius: 6px !important;
        font-weight: 500 !important;
    }

    /* Select boxes */
    [data-testid="stSelectbox"] > div > div,
    [data-testid="stMultiSelect"] > div > div {
        background: rgba(255, 255, 255, 0.8) !important;
        border-color: rgba(0, 0, 0, 0.1) !important;
        border-radius: 8px !important;
    }

    [data-testid="stSelectbox"] > div > div:hover,
    [data-testid="stMultiSelect"] > div > div:hover {
        border-color: #0891b2 !important;
    }

    /* Alert boxes */
    [data-testid="stAlert"] {
        backdrop-filter: blur(8px) !important;
        -webkit-backdrop-filter: blur(8px) !important;
        border-radius: 8px !important;
    }

    /* Success alert (green) */
    .stAlert[data-baseweb="notification"][kind="positive"],
    div[data-testid="stAlert"]:has(svg[data-testid="stIconSuccess"]) {
        background: rgba(5, 150, 105, 0.1) !important;
        border-left: 4px solid #059669 !important;
    }

    /* Warning alert (yellow) */
    .stAlert[data-baseweb="notification"][kind="warning"],
    div[data-testid="stAlert"]:has(svg[data-testid="stIconWarning"]) {
        background: rgba(245, 158, 11, 0.1) !important;
        border-left: 4px solid #f59e0b !important;
    }

    /* Info alert */
    .stAlert[data-baseweb="notification"][kind="info"],
    div[data-testid="stAlert"]:has(svg[data-testid="stIconInfo"]) {
        background: rgba(8, 145, 178, 0.1) !important;
        border-left: 4px solid #0891b2 !important;
    }

    /* DataFrames */
    [data-testid="stDataFrame"] {
        background: rgba(255, 255, 255, 0.7) !important;
        backdrop-filter: blur(12px) !important;
        -webkit-backdrop-filter: blur(12px) !important;
        border: 1px solid rgba(0, 0, 0, 0.06) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }

    /* Headers h2, h3 */
    .stApp h2, .stApp h3, .main h2, .main h3 {
        color: #1e293b !important;
        border-bottom: 2px solid rgba(0, 0, 0, 0.06) !important;
        padding-bottom: 8px !important;
    }

    /* Dividers */
    .stApp hr {
        border: none !important;
        height: 1px !important;
        background: linear-gradient(90deg, transparent, rgba(0, 0, 0, 0.1), transparent) !important;
    }

    /* Charts container */
    .js-plotly-plot {
        border-radius: 12px !important;
        overflow: hidden !important;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.06) !important;
    }

    /* Scrollbar - light mode */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }

    ::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.05);
        border-radius: 4px;
    }

    ::-webkit-scrollbar-thumb {
        background: linear-gradient(180deg, #0891b2 0%, #7c3aed 100%);
        border-radius: 4px;
    }

    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(180deg, #059669 0%, #0891b2 100%);
    }

    /* ============================================
       DARK MODE STYLES
       ============================================ */
    @media (prefers-color-scheme: dark) {
        /* Title gradient - brighter for dark mode */
        .stApp h1, .main h1 {
            background: linear-gradient(90deg, #00d4ff 0%, #00ff88 50%, #a855f7 100%) !important;
            -webkit-background-clip: text !important;
            -webkit-text-fill-color: transparent !important;
            background-clip: text !important;
        }

        /* Sidebar */
        section[data-testid="stSidebar"] {
            background: rgba(17, 25, 40, 0.85) !important;
            border-right: 1px solid rgba(255, 255, 255, 0.1) !important;
        }

        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2 {
            background: linear-gradient(90deg, #00d4ff, #00ff88) !important;
            -webkit-background-clip: text !important;
            -webkit-text-fill-color: transparent !important;
        }

        /* Metrics */
        [data-testid="stMetric"] {
            background: rgba(17, 25, 40, 0.8) !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3) !important;
        }

        [data-testid="stMetric"]:hover {
            border-color: #00d4ff !important;
            box-shadow: 0 8px 30px rgba(0, 212, 255, 0.2) !important;
        }

        [data-testid="stMetric"] label {
            color: #8b949e !important;
        }

        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #f0f6fc !important;
        }

        /* Tabs */
        button[data-baseweb="tab"] {
            background: rgba(17, 25, 40, 0.7) !important;
            border-color: rgba(255, 255, 255, 0.1) !important;
            color: #8b949e !important;
        }

        button[data-baseweb="tab"]:hover {
            background: rgba(0, 212, 255, 0.15) !important;
            color: #00d4ff !important;
            border-color: #00d4ff !important;
        }

        button[data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.2) 0%, rgba(0, 255, 136, 0.1) 100%) !important;
            color: #00d4ff !important;
            border-color: #00d4ff !important;
        }

        /* Tab content */
        [data-testid="stTabContent"] {
            background: rgba(17, 25, 40, 0.6) !important;
            border-color: rgba(255, 255, 255, 0.1) !important;
        }

        /* Expanders */
        [data-testid="stExpander"] {
            background: rgba(17, 25, 40, 0.7) !important;
            border-color: rgba(255, 255, 255, 0.1) !important;
        }

        [data-testid="stExpander"] summary {
            background: linear-gradient(90deg, rgba(0, 212, 255, 0.1) 0%, transparent 100%) !important;
            color: #f0f6fc !important;
        }

        /* Buttons */
        .stButton > button {
            background: linear-gradient(135deg, #00d4ff 0%, #00ff88 100%) !important;
            color: #000 !important;
            box-shadow: 0 4px 15px rgba(0, 212, 255, 0.3) !important;
        }

        /* Multiselect tags */
        [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
            background: linear-gradient(135deg, #00d4ff 0%, #00ff88 100%) !important;
            color: #000 !important;
        }

        /* Select boxes */
        [data-testid="stSelectbox"] > div > div,
        [data-testid="stMultiSelect"] > div > div {
            background: rgba(17, 25, 40, 0.8) !important;
            border-color: rgba(255, 255, 255, 0.1) !important;
        }

        [data-testid="stSelectbox"] > div > div:hover,
        [data-testid="stMultiSelect"] > div > div:hover {
            border-color: #00d4ff !important;
        }

        /* Alerts - dark mode */
        div[data-testid="stAlert"]:has(svg[data-testid="stIconSuccess"]) {
            background: rgba(0, 255, 136, 0.1) !important;
            border-left-color: #00ff88 !important;
        }

        div[data-testid="stAlert"]:has(svg[data-testid="stIconWarning"]) {
            background: rgba(251, 191, 36, 0.1) !important;
            border-left-color: #fbbf24 !important;
        }

        div[data-testid="stAlert"]:has(svg[data-testid="stIconInfo"]) {
            background: rgba(0, 212, 255, 0.1) !important;
            border-left-color: #00d4ff !important;
        }

        /* DataFrames */
        [data-testid="stDataFrame"] {
            background: rgba(17, 25, 40, 0.8) !important;
            border-color: rgba(255, 255, 255, 0.1) !important;
        }

        /* Headers */
        .stApp h2, .stApp h3 {
            color: #f0f6fc !important;
            border-bottom-color: rgba(255, 255, 255, 0.1) !important;
        }

        /* Dividers */
        .stApp hr {
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.1), transparent) !important;
        }

        /* Scrollbar - dark mode */
        ::-webkit-scrollbar-track {
            background: rgba(17, 25, 40, 0.5);
        }
    }

    /* ============================================
       ANIMATIONS
       ============================================ */
    @keyframes shimmer {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }

    .shimmer {
        background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.1) 50%, transparent 100%);
        background-size: 200% 100%;
        animation: shimmer 2s infinite;
    }

    /* ============================================
       UTILITY CLASSES
       ============================================ */
    .yield-good { color: #059669 !important; }
    .yield-warning { color: #d97706 !important; }
    .yield-critical { color: #dc2626 !important; }

    @media (prefers-color-scheme: dark) {
        .yield-good { color: #00ff88 !important; }
        .yield-warning { color: #fbbf24 !important; }
        .yield-critical { color: #f87171 !important; }
    }

    </style>
    """, unsafe_allow_html=True)


def get_ai_robot_image_base64() -> str:
    """Load the AI robot assistant image as base64 string."""
    image_path = os.path.join(os.path.dirname(__file__), "Designer (1).png")
    try:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except FileNotFoundError:
        return ""


def setup_page() -> None:
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="SOCAMM 1-Stop",
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Inject custom CSS
    inject_custom_css()

    # Modern Header with gradient and glow
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #064e3b 0%, #047857 50%, #059669 100%);
        border-radius: 16px;
        padding: 20px 30px;
        margin-bottom: 20px;
        border: 1px solid #10b981;
        box-shadow: 0 8px 32px rgba(16, 185, 129, 0.25), inset 0 1px 0 rgba(255,255,255,0.1);
        display: flex;
        justify-content: space-between;
        align-items: center;
    ">
        <div style="display: flex; align-items: center; gap: 15px;">
            <div style="
                font-size: 42px;
                filter: drop-shadow(0 0 15px rgba(110, 231, 183, 0.6));
                animation: pulse 2s ease-in-out infinite;
            ">🎯</div>
            <div>
                <h1 style="
                    color: #ecfdf5;
                    margin: 0;
                    font-size: 32px;
                    font-weight: 700;
                    letter-spacing: -0.5px;
                    text-shadow: 0 2px 10px rgba(0,0,0,0.2);
                ">SOCAMM 1-Stop</h1>
                <p style="
                    color: #a7f3d0;
                    margin: 4px 0 0 0;
                    font-size: 14px;
                    font-weight: 400;
                    letter-spacing: 0.3px;
                ">Your One-Stop Dashboard for SOCAMM / SOCAMM2 Manufacturing Yield Analytics</p>
            </div>
        </div>
        <div style="text-align: right;">
            <div style="
                background: rgba(0,0,0,0.2);
                border-radius: 10px;
                padding: 10px 16px;
                border: 1px solid rgba(110, 231, 183, 0.3);
            ">
                <span style="color: #a7f3d0; font-size: 11px; text-transform: uppercase; letter-spacing: 1px;">Last refreshed</span><br>
                <span style="color: #6ee7b7; font-size: 18px; font-weight: 600;">{datetime.now().strftime('%H:%M:%S')}</span>
            </div>
        </div>
    </div>
    <style>
        @keyframes pulse {{
            0%, 100% {{ transform: scale(1); }}
            50% {{ transform: scale(1.05); }}
        }}
    </style>
    """, unsafe_allow_html=True)


def render_primary_filters() -> dict[str, Any]:
    """Render primary filter widgets (form factor, step, design_id, facility)."""
    form_factors = st.sidebar.multiselect(
        "Module Form Factor",
        options=Settings.FORM_FACTORS,
        default=Settings.FORM_FACTORS,
        help="Select one or more module form factors",
    )

    test_steps = st.sidebar.multiselect(
        "Test Step",
        options=Settings.TEST_STEPS,
        default=Settings.TEST_STEPS,
        help="Select one or more test steps",
    )

    design_ids = st.sidebar.multiselect(
        "Design ID",
        options=Settings.DESIGN_IDS,
        default=[Settings.DESIGN_IDS[0]],
        help="Select one or more design IDs (DBASE parameter)",
    )

    facility = st.sidebar.selectbox(
        "Test Facility",
        options=Settings.FACILITIES,
        index=0,
        help="Select test facility. For GRACE Motherboard analysis, select PENANG or BOISE (not 'all').",
    )

    return {
        "form_factors": form_factors,
        "test_steps": test_steps,
        "design_ids": design_ids,
        "facility": facility,
    }


def get_previous_workweeks(end_ww: str, count: int = 10) -> tuple[str, str]:
    """Calculate start workweek that is 'count' weeks before end_ww.

    Args:
        end_ww: End workweek in YYYYWW format
        count: Number of weeks to include (default 10)

    Returns:
        Tuple of (start_ww, end_ww) in YYYYWW format
    """
    year = int(end_ww[:4])
    week = int(end_ww[4:])

    # Go back (count - 1) weeks to get start
    weeks_back = count - 1
    while weeks_back > 0:
        week -= 1
        if week < 1:
            year -= 1
            week = 52
        weeks_back -= 1

    return f"{year}{week:02d}", end_ww


def render_workweek_filters() -> dict[str, str]:
    """Render work week filter widget (single workweek input)."""
    st.sidebar.divider()
    st.sidebar.subheader("Work Week")

    current_ww = get_current_workweek()
    current_year = int(current_ww[:4])
    current_week = min(int(current_ww[4:]), MAX_WEEKS_PER_YEAR)

    st.sidebar.caption("Select target workweek (will show 10 weeks of data)")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        selected_year = st.number_input(
            "Year",
            min_value=MIN_YEAR,
            max_value=MAX_YEAR,
            value=current_year,
        )
    with col2:
        selected_week = st.number_input(
            "Week",
            min_value=1,
            max_value=MAX_WEEKS_PER_YEAR,
            value=current_week,
        )

    selected_ww = f"{selected_year}{selected_week:02d}"
    start_ww, end_ww = get_previous_workweeks(selected_ww, count=10)

    st.sidebar.caption(f"Range: WW{start_ww} to WW{end_ww}")

    return {
        "start_ww": start_ww,
        "end_ww": end_ww,
    }


def render_optional_filters() -> dict[str, Optional[list[str]]]:
    """Render optional filter widgets (density, speed)."""
    st.sidebar.divider()
    st.sidebar.subheader("Optional Filters")

    # Hardcoded speed options with MTPS suffix
    SPEED_OPTIONS = ["6400MTPS", "7500MTPS", "8533MTPS", "9600MTPS"]

    densities = st.sidebar.multiselect(
        "Density",
        options=Settings.DENSITIES,
        default=[],
        help="Filter by module density (optional)",
    )

    speeds = st.sidebar.multiselect(
        "Speed",
        options=SPEED_OPTIONS,  # Use hardcoded values
        default=[],
        help="Filter by module speed (optional)",
        key="speed_filter_v5",  # Force widget refresh
    )

    return {
        "densities": densities if densities else None,
        "speeds": speeds if speeds else None,
    }


def validate_filters(filters: dict[str, Any]) -> Optional[str]:
    """Validate filter selections.

    Args:
        filters: Filter dictionary

    Returns:
        Error message if validation fails, None if valid
    """
    if not filters["form_factors"]:
        return "Please select at least one Form Factor"
    if not filters["test_steps"]:
        return "Please select at least one Test Step"
    if not filters["design_ids"]:
        return "Please select at least one Design ID"

    return None


def render_sidebar() -> dict[str, Any]:
    """Render sidebar filters and return selected values."""
    st.sidebar.header("Filters")

    primary = render_primary_filters()
    workweek = render_workweek_filters()
    optional = render_optional_filters()

    return {**primary, **workweek, **optional}


def fetch_data(filters: dict[str, Any], use_cache: bool = True) -> pd.DataFrame:
    """Fetch data from frpt commands based on filters using parallel execution.

    Args:
        filters: Filter parameters
        use_cache: Whether to use cached results

    Returns:
        DataFrame with yield data

    Raises:
        RuntimeError: If data fetching fails
    """
    logger.info("="*60)
    logger.info(f"FETCH DATA STARTED (PARALLEL, cache={'ON' if use_cache else 'OFF'})")
    logger.info(f"Filters: {filters}")

    runner = FrptRunner(max_workers=8, use_cache=use_cache)  # Run up to 8 queries in parallel
    parser = FrptParser()

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        logger.info(f"Workweeks to fetch: {workweeks}")
    except Exception as e:
        logger.error("Failed to generate workweek range: %s", e)
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    # Build all commands first (iterate over all combinations including design_ids)
    commands = []
    for design_id in filters["design_ids"]:
        for step in filters["test_steps"]:
            for form_factor in filters["form_factors"]:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=design_id,
                            facility=filters["facility"],
                        )
                        commands.append(command)
                    except ValueError as e:
                        logger.warning(f"Invalid command parameters: {e}")

    total_calls = len(commands)
    logger.info(f"Total frpt calls to make: {total_calls} (running in parallel)")

    if total_calls == 0:
        logger.warning("No calls to make (empty filters)")
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()
    cache_hits_text = st.empty()

    cache_mode = "with cache" if use_cache else "no cache"
    status_text.text(f"Running {total_calls} queries in parallel ({cache_mode})...")

    # Progress callback for UI updates
    completed_count = [0]  # Use list for mutable closure
    cache_hits = [0]

    def progress_callback(completed: int, total: int, cmd: FrptCommand) -> None:
        completed_count[0] = completed
        progress_bar.progress(completed / total)
        status_text.text(
            f"Completed {completed}/{total}: {cmd.step}/{cmd.form_factor}/WW{cmd.workweek}"
        )

    # Run commands in parallel
    results = []
    errors = []

    try:
        parallel_results = runner.run_parallel(commands, progress_callback)

        for command, result in parallel_results:
            logger.info(f"Processing result: {command.step}/WW{command.workweek} success={result.success} stdout_len={len(result.stdout) if result.stdout else 0}")
            if result.success and result.stdout:
                results.append((result.stdout, command.step, command.form_factor))
                logger.info(f"Added result for {command.step}/{command.form_factor}/WW{command.workweek}")
            elif not result.success:
                err_msg = f"{command.step}/{command.form_factor}/WW{command.workweek}: {result.stderr[:200]}"
                errors.append(err_msg)
                logger.error(f"Command failed: {err_msg}")
            else:
                logger.warning(f"Skipped result (no stdout): {command.step}/WW{command.workweek}")

    except Exception as e:
        logger.error(f"Parallel execution error: {e}", exc_info=True)
        raise RuntimeError(f"Parallel execution failed: {e}") from e

    progress_bar.empty()
    status_text.empty()
    cache_hits_text.empty()

    logger.info(f"Fetch complete. Results collected: {len(results)}, Errors: {len(errors)}")
    if errors:
        logger.warning("Fetch errors: %s", errors[:5])

    if not results:
        logger.warning("No results to parse")
        return pd.DataFrame()

    # Debug: Check if results have pipe character (bin data)
    for i, (stdout, step, ff) in enumerate(results[:2]):  # Check first 2
        pipe_count = stdout.count('|')
        data_lines = [l for l in stdout.split('\n') if '|' in l and '202' in l]
        logger.info(f"Result {i} [{step}/{ff}]: {len(stdout)} bytes, {pipe_count} pipes, {len(data_lines)} data lines with pipe")
        if data_lines:
            logger.info(f"  Sample line: {data_lines[0][:120]}")

    try:
        df = parser.parse_multiple(results)
        # Check for both new format (Bin_1_GOOD) and legacy format (BIN1)
        bin_cols = [c for c in df.columns if c.startswith('BIN') or c.startswith('Bin_')]
        logger.info(f"Parsed DataFrame: {len(df)} rows, columns={list(df.columns) if not df.empty else []}")
        logger.info(f"BIN columns after parsing: {bin_cols}")

        # WORKAROUND: If no BIN columns, extract them manually from results
        # This should rarely trigger now that the parser handles Bin_ columns
        if not bin_cols and not df.empty:
            logger.info("Attempting manual bin extraction...")
            bin_data = []
            bin_names = {}  # Map BIN1 -> "GOOD", BIN20 -> "CONT", etc.
            bin_numbers = []  # List of actual bin numbers [1, 20, 65, ...]

            for stdout, step, ff in results:
                lines = stdout.split('\n')

                # Extract actual bin numbers from header (line with "Bin_" pattern)
                # Format: |    Bin_1   Bin_20   Bin_65
                for line in lines:
                    if 'Bin_' in line and '|' in line:
                        parts = line.split('|')
                        if len(parts) > 1:
                            bin_tokens = parts[1].strip().split()
                            bin_numbers = []
                            for token in bin_tokens:
                                if token.startswith('Bin_'):
                                    try:
                                        bin_num = int(token.replace('Bin_', ''))
                                        bin_numbers.append(bin_num)
                                    except ValueError:
                                        pass
                            logger.info(f"Extracted bin numbers: {bin_numbers}")
                        break

                # Extract bin names from MYQUICK or MULTI- line (line has bin names after |)
                # Format HMFN: MYQUICK: ... |     GOOD     CONT      SLT
                # Format HMB1/QMON: MULTI- ... |     GOOD   RETEST FUNC-FAI
                for line in lines:
                    if ('MYQUICK' in line or 'MULTI-' in line) and '|' in line:
                        parts = line.split('|')
                        if len(parts) > 1:
                            names = parts[1].strip().split()
                            # Map actual bin numbers to names
                            for i, name in enumerate(names):
                                if i < len(bin_numbers):
                                    bin_names[f'BIN{bin_numbers[i]}'] = name
                            logger.info(f"Bin names mapping: {bin_names}")
                        break

                # Extract bin values from data lines
                for line in lines:
                    if '|' in line and '202' in line:
                        parts = line.split('|')
                        if len(parts) > 1:
                            main_part = parts[0].strip().split()
                            bin_part = parts[1].strip()
                            if main_part and bin_part:
                                myquick = main_part[0]
                                bin_values = bin_part.split()
                                row_data = {'MYQUICK': myquick}
                                for i, bv in enumerate(bin_values):
                                    if i < len(bin_numbers):
                                        try:
                                            if bv != '-':
                                                row_data[f'BIN{bin_numbers[i]}'] = float(bv.replace(',', ''))
                                        except ValueError:
                                            pass
                                if len(row_data) > 1:  # Has at least one bin
                                    bin_data.append(row_data)

            if bin_data:
                bin_df = pd.DataFrame(bin_data)
                logger.info(f"Manual bin extraction: {len(bin_df)} rows, columns={list(bin_df.columns)}")
                logger.info(f"Bin names mapping: {bin_names}")
                # Store bin names in session state for chart labels
                st.session_state.bin_names = bin_names
                # Merge bin data with main df
                df = df.merge(bin_df, on='MYQUICK', how='left')
                bin_cols = [c for c in df.columns if c.startswith('BIN')]
                logger.info(f"After merge: BIN columns = {bin_cols}")

        if not df.empty:
            logger.debug(f"First few rows:\n{df.head()}")
        return df
    except Exception as e:
        logger.error("Failed to parse results: %s", e, exc_info=True)
        raise RuntimeError(f"Failed to parse frpt output: {e}") from e


def render_yield_trend_chart(processor: DataProcessor) -> None:
    """Render weekly yield trend line chart with volume bars."""
    st.subheader("Weekly Yield Trend")

    try:
        trend_data = processor.get_weekly_yield_trend()
        if trend_data.empty:
            st.info("No trend data available")
            return

        trend_data = trend_data.copy()

        # Full series name: design_id + form_factor + speed + density + step
        trend_data["series"] = (
            trend_data["design_id"].fillna("") + "_" +
            trend_data["form_factor"].fillna("") + "_" +
            trend_data["speed"].fillna("") + "_" +
            trend_data["density"].fillna("") + "_" +
            trend_data["step"].fillna("")
        )

        # Ensure workweek is string for proper categorical display
        trend_data["workweek"] = trend_data["workweek"].astype(str)

        # Sort by workweek chronologically
        trend_data["_ww_sort"] = trend_data["workweek"].astype(int)
        trend_data = trend_data.sort_values("_ww_sort").drop(columns=["_ww_sort"])

        # Get sorted unique workweeks
        sorted_workweeks = sorted(trend_data["workweek"].unique().tolist(), key=int)

        # Get unique series for filter
        all_series = sorted(trend_data["series"].unique().tolist())

        # Series filter - DEFAULT TO EMPTY so user picks what to display
        selected_series = st.multiselect(
            "Select Series to Display",
            options=all_series,
            default=[],  # Empty by default - user picks
            key="trend_series_filter",
            help="Select which product combinations to show in the chart"
        )

        if not selected_series:
            st.info("👆 Select one or more series above to display the trend chart")
            return

        # Chart options
        opt_col1, opt_col2 = st.columns([1, 2])
        with opt_col1:
            show_labels = st.checkbox("Show data labels", value=False, key="trend_show_labels")
        with opt_col2:
            y_min = st.slider("Y-axis Min %", min_value=0, max_value=99, value=94, step=1, key="trend_y_min")

        # Filter data by selected series
        filtered_data = trend_data[trend_data["series"].isin(selected_series)]

        # Aggregate by series and workweek (in case of duplicates)
        agg_data = filtered_data.groupby(["series", "workweek"]).agg({
            "yield_pct": "mean",
            "UIN": "sum"
        }).reset_index()

        # Get fiscal month labels for x-axis
        tick_labels = get_workweek_labels_with_months(sorted_workweeks)

        # Color palette
        colors = px.colors.qualitative.Set2

        # Create figure with secondary y-axis for volume
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        for i, series_name in enumerate(selected_series):
            series_data = agg_data[agg_data["series"] == series_name].copy()
            series_data = series_data.sort_values("workweek", key=lambda x: x.astype(int))
            color = colors[i % len(colors)]

            # Yield line (primary y-axis) - paired with volume via legendgroup
            trace_mode = "lines+markers+text" if show_labels else "lines+markers"
            text_values = series_data["yield_pct"].apply(lambda x: f"{x:.1f}%") if show_labels else None

            fig.add_trace(
                go.Scatter(
                    x=series_data["workweek"],
                    y=series_data["yield_pct"],
                    mode=trace_mode,
                    name=series_name,
                    legendgroup=series_name,  # Group with volume
                    line=dict(color=color, width=2.5),
                    marker=dict(size=8),
                    text=text_values,
                    textposition="top center",
                    textfont=dict(size=10),
                    hovertemplate=f"<b>{series_name}</b><br>WW%{{x}}<br>Yield: %{{y:.2f}}%<extra></extra>",
                ),
                secondary_y=False,
            )

            # Volume bars (secondary y-axis) - paired with yield via legendgroup
            fig.add_trace(
                go.Bar(
                    x=series_data["workweek"],
                    y=series_data["UIN"],
                    name=f"{series_name} (Vol)",
                    legendgroup=series_name,  # Group with yield - clicking legend toggles both
                    marker=dict(color=color, opacity=0.3),
                    hovertemplate=f"<b>{series_name}</b><br>WW%{{x}}<br>Volume: %{{y:,}}<extra></extra>",
                    showlegend=False,  # Only show one legend entry per series
                ),
                secondary_y=True,
            )

        fig.update_layout(
            title="Yield % and Volume by Work Week",
            xaxis_title="Work Week",
            hovermode="x unified",
            xaxis=dict(
                type="category",
                categoryorder="array",
                categoryarray=sorted_workweeks,
                ticktext=tick_labels,
                tickvals=sorted_workweeks,
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            ),
            barmode="group",
            height=450,
            margin=dict(t=80, b=60),
        )

        # Update y-axes with user-controlled range
        fig.update_yaxes(title_text="Yield %", range=[y_min, 100], secondary_y=False)
        fig.update_yaxes(title_text="Volume (UIN)", secondary_y=True)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error("Failed to render trend chart: %s", e)
        st.error("Failed to render trend chart")


def create_gauge_chart(value: float, target: float, title: str, height: int = 150) -> go.Figure:
    """Create an animated gauge chart showing yield vs target."""
    if value is None:
        return None

    # Determine color based on gap to target
    gap = value - target
    if gap >= 0:
        bar_color = "#00ff88"  # Green
        gauge_color = "rgba(0, 255, 136, 0.3)"
    elif gap >= -1:
        bar_color = "#ffaa00"  # Orange
        gauge_color = "rgba(255, 170, 0, 0.3)"
    else:
        bar_color = "#ff4466"  # Red
        gauge_color = "rgba(255, 68, 102, 0.3)"

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        number={'suffix': '%', 'font': {'size': 24, 'color': bar_color}},
        delta={'reference': target, 'suffix': '%', 'font': {'size': 12},
               'increasing': {'color': '#00ff88'}, 'decreasing': {'color': '#ff4466'}},
        title={'text': title, 'font': {'size': 14, 'color': '#888'}},
        gauge={
            'axis': {'range': [90, 100], 'tickwidth': 1, 'tickcolor': '#444',
                     'tickfont': {'size': 10, 'color': '#666'}},
            'bar': {'color': bar_color, 'thickness': 0.7},
            'bgcolor': 'rgba(30,30,60,0.5)',
            'borderwidth': 0,
            'steps': [
                {'range': [90, target - 2], 'color': 'rgba(255, 68, 102, 0.2)'},
                {'range': [target - 2, target], 'color': 'rgba(255, 170, 0, 0.2)'},
                {'range': [target, 100], 'color': 'rgba(0, 255, 136, 0.2)'}
            ],
            'threshold': {
                'line': {'color': '#00d4ff', 'width': 3},
                'thickness': 0.8,
                'value': target
            }
        }
    ))

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=10),
        height=height,
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': '#fff'}
    )

    return fig


def create_sparkline(values: list, color: str = "#00d4ff", height: int = 30) -> go.Figure:
    """Create a mini sparkline chart."""
    if not values or len(values) < 2:
        return None

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=values,
        mode='lines',
        line=dict(color=color, width=2),
        fill='tozeroy',
        fillcolor=f'rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.2)',
        hoverinfo='skip'
    ))

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )

    return fig


def get_weekly_yields_for_sparkline(df: pd.DataFrame, did: str, step: str) -> list:
    """Get weekly yield values for sparkline chart."""
    if df.empty:
        return []

    # Find column names
    did_col = next((c for c in ['DBASE', 'design_id', 'DESIGN_ID'] if c in df.columns), None)
    step_col = next((c for c in ['STEP', 'step'] if c in df.columns), None)
    ww_col = next((c for c in ['workweek', 'MFG_WORKWEEK'] if c in df.columns), None)

    if not all([did_col, step_col, ww_col]):
        return []

    # Filter and aggregate
    filtered = df[(df[did_col] == did) & (df[step_col].str.lower() == step.lower())]
    if filtered.empty:
        return []

    weekly = filtered.groupby(ww_col).agg({'UIN': 'sum', 'UPASS': 'sum'}).reset_index()
    weekly['yield'] = (weekly['UPASS'] / weekly['UIN'] * 100).round(2)
    weekly = weekly.sort_values(ww_col)

    return weekly['yield'].tolist()


def render_summary_metrics(processor: DataProcessor) -> None:
    """Render summary metric cards with quick stats bar."""
    try:
        summary = processor.get_yield_summary()
        df = processor.dataframe

        # Quick stats bar
        num_dids = df['design_id'].nunique() if 'design_id' in df.columns else 0
        num_weeks = df['workweek'].nunique() if 'workweek' in df.columns else 0
        num_steps = df['step'].nunique() if 'step' in df.columns else 0

        st.markdown(f"""
        <div class="stats-bar">
            <span class="stats-item"><span class="stats-label">📦 Records:</span><span class="stats-value">{len(df):,}</span></span>
            <span class="stats-item"><span class="stats-label">🔷 DIDs:</span><span class="stats-value">{num_dids}</span></span>
            <span class="stats-item"><span class="stats-label">📅 Weeks:</span><span class="stats-value">{num_weeks}</span></span>
            <span class="stats-item"><span class="stats-label">🧪 Steps:</span><span class="stats-value">{num_steps}</span></span>
        </div>
        """, unsafe_allow_html=True)

        # Overall metrics row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Units In", f"{summary.total_uin:,}")
        with col2:
            st.metric("Total Units Pass", f"{summary.total_upass:,}")
        with col3:
            st.metric("Overall Yield", f"{summary.overall_yield:.2f}%")
        with col4:
            st.metric(
                "Yield Range",
                f"{summary.min_yield:.1f}% - {summary.max_yield:.1f}%",
            )

    except Exception as e:
        import traceback
        logger.error("Failed to render top metrics: %s", e)
        st.error("Failed to render summary metrics")


def render_did_breakdown(processor: DataProcessor) -> None:
    """Render DID breakdown by step for latest workweek."""
    try:
        # DID breakdown by step for latest week - using local function to avoid import issues
        did_breakdown = get_did_breakdown_local(processor.dataframe, by_step=True)
        rolled_4week = get_4week_rolled_yields(processor.dataframe)

        if did_breakdown.empty:
            st.info("No DID breakdown data available")
            return

        latest_ww = did_breakdown['workweek'].iloc[0] if 'workweek' in did_breakdown.columns else "N/A"

        # Header with view toggle
        hdr1, hdr2 = st.columns([3, 1])
        with hdr1:
            st.markdown(f"#### 📊 WW{latest_ww} Summary")
        with hdr2:
            show_gauges = st.toggle("Gauges", value=False, key="did_gauge_toggle", help="Show gauge charts")

        # Step-specific targets for color coding
        step_targets = {
            'hmfn': 99.0,
            'slt': 97.0,
            'elc': 96.0
        }

        def get_yield_color(yield_pct: float, step: str) -> str:
            """Get color based on yield relative to step target."""
            target = step_targets.get(step.lower(), 97.0)
            gap = yield_pct - target
            if gap >= 0:
                return "#00C853"  # Green - at or above target
            elif gap >= -1:
                return "#8BC34A"  # Light green - within 1% of target
            elif gap >= -2:
                return "#FFEB3B"  # Yellow - within 2% of target
            else:
                return "#FF5722"  # Red - more than 2% below target

        # Group by DID
        dids = did_breakdown['design_id'].unique()
        num_dids = len(dids)

        for idx, did in enumerate(dids):
            did_data = did_breakdown[did_breakdown['design_id'] == did]

            # Extract yields per step
            hmfn_yield = None
            hmb1_yield = None
            qmon_yield = None
            hmfn_uin = 0
            hmb1_uin = 0
            qmon_uin = 0

            for _, row in did_data.iterrows():
                step = row.get('step', '').lower()
                if step == 'hmfn':
                    hmfn_yield = row['yield_pct']
                    hmfn_uin = int(row['uin'])
                elif step == 'hmb1':
                    hmb1_yield = row['yield_pct']
                    hmb1_uin = int(row['uin'])
                elif step == 'qmon':
                    qmon_yield = row['yield_pct']
                    qmon_uin = int(row['uin'])

            # Calculate SLT = HMB1 × QMON
            slt_yield = None
            slt_uin = min(hmb1_uin, qmon_uin) if hmb1_uin and qmon_uin else 0
            if hmb1_yield is not None and qmon_yield is not None:
                slt_yield = round((hmb1_yield / 100) * (qmon_yield / 100) * 100, 2)

            # Calculate ELC = HMFN × SLT
            elc_yield = None
            elc_uin = min(hmfn_uin, slt_uin) if hmfn_uin and slt_uin else 0
            if hmfn_yield is not None and slt_yield is not None:
                elc_yield = round((hmfn_yield / 100) * (slt_yield / 100) * 100, 2)

            # Get 4-week rolled data for this DID
            hmfn_4wk = rolled_4week.get((did, 'hmfn'), {})
            hmb1_4wk = rolled_4week.get((did, 'hmb1'), {})
            qmon_4wk = rolled_4week.get((did, 'qmon'), {})

            # Calculate 4-week SLT and ELC
            slt_4wk_yield = None
            elc_4wk_yield = None
            if hmb1_4wk.get('yield') and qmon_4wk.get('yield'):
                slt_4wk_yield = round((hmb1_4wk['yield'] / 100) * (qmon_4wk['yield'] / 100) * 100, 2)
            if hmfn_4wk.get('yield') and slt_4wk_yield:
                elc_4wk_yield = round((hmfn_4wk['yield'] / 100) * (slt_4wk_yield / 100) * 100, 2)

            # Targets
            targets = {'hmfn': 99.0, 'slt': 97.0, 'elc': 96.0}

            # Calculate trend
            trend_diff = None
            if elc_yield is not None and elc_4wk_yield is not None:
                trend_diff = elc_yield - elc_4wk_yield

            # Format volume compactly
            def fmt_vol(v):
                if not v:
                    return "-"
                return f"{v/1000:.1f}K" if v >= 10000 else f"{v:,}"

            # Status emoji based on target
            def status_emoji(val, target):
                if val is None:
                    return "⚫"
                gap = val - target
                if gap >= 0:
                    return "🟢"
                elif gap >= -1:
                    return "🟡"
                else:
                    return "🔴"

            # Trend emoji
            trend_emoji = "⚪"
            if trend_diff is not None:
                if trend_diff > 0.1:
                    trend_emoji = "📈"
                elif trend_diff < -0.1:
                    trend_emoji = "📉"

            # Get sparkline data for this DID
            hmfn_spark = get_weekly_yields_for_sparkline(processor.dataframe, did, 'hmfn')

            # Compact card with border
            with st.container(border=True):
                # Header row with DID and trend
                hdr_col1, hdr_col2 = st.columns([3, 1])
                with hdr_col1:
                    trend_str = f"{trend_diff:+.2f}%" if trend_diff else ""
                    st.markdown(f"### {did} &nbsp; {trend_emoji} {trend_str}")
                with hdr_col2:
                    # Mini sparkline for HMFN trend
                    if len(hmfn_spark) >= 2:
                        spark_color = "#00ff88" if trend_diff and trend_diff > 0 else "#ff6b8a" if trend_diff and trend_diff < 0 else "#00d4ff"
                        spark_fig = create_sparkline(hmfn_spark, color=spark_color, height=35)
                        if spark_fig:
                            st.plotly_chart(spark_fig, use_container_width=True, config={'displayModeBar': False}, key=f"spark_{did}")

                # Three metrics side by side
                c1, c2, c3 = st.columns(3)

                if show_gauges:
                    # Gauge view
                    with c1:
                        gauge_fig = create_gauge_chart(hmfn_yield, targets['hmfn'], "HMFN", height=140)
                        if gauge_fig:
                            st.plotly_chart(gauge_fig, use_container_width=True, config={'displayModeBar': False}, key=f"gauge_hmfn_{did}")
                        st.caption(f"n={fmt_vol(hmfn_uin)}")

                    with c2:
                        gauge_fig = create_gauge_chart(slt_yield, targets['slt'], "SLT", height=140)
                        if gauge_fig:
                            st.plotly_chart(gauge_fig, use_container_width=True, config={'displayModeBar': False}, key=f"gauge_slt_{did}")
                        st.caption(f"n={fmt_vol(slt_uin)}")

                    with c3:
                        gauge_fig = create_gauge_chart(elc_yield, targets['elc'], "ELC", height=140)
                        if gauge_fig:
                            st.plotly_chart(gauge_fig, use_container_width=True, config={'displayModeBar': False}, key=f"gauge_elc_{did}")
                        st.caption(f"n={fmt_vol(elc_uin)}")
                else:
                    # Metrics view (default)
                    with c1:
                        hmfn_emoji = status_emoji(hmfn_yield, targets['hmfn'])
                        hmfn_delta = f"{hmfn_yield - targets['hmfn']:+.2f}%" if hmfn_yield else None
                        st.metric(
                            f"{hmfn_emoji} HMFN",
                            f"{hmfn_yield:.2f}%" if hmfn_yield else "N/A",
                            delta=hmfn_delta
                        )
                        fourwk_hmfn = f"{hmfn_4wk.get('yield', 0):.1f}%" if hmfn_4wk.get('yield') else "-"
                        st.caption(f"n={fmt_vol(hmfn_uin)} | 4wk:{fourwk_hmfn}")

                    with c2:
                        slt_emoji = status_emoji(slt_yield, targets['slt'])
                        slt_delta = f"{slt_yield - targets['slt']:+.2f}%" if slt_yield else None
                        st.metric(
                            f"{slt_emoji} SLT",
                            f"{slt_yield:.2f}%" if slt_yield else "N/A",
                            delta=slt_delta
                        )
                        fourwk_slt = f"{slt_4wk_yield:.1f}%" if slt_4wk_yield else "-"
                        st.caption(f"n={fmt_vol(slt_uin)} | 4wk:{fourwk_slt}")

                    with c3:
                        elc_emoji = status_emoji(elc_yield, targets['elc'])
                        elc_delta = f"{elc_yield - targets['elc']:+.2f}%" if elc_yield else None
                        st.metric(
                            f"{elc_emoji} ELC",
                            f"{elc_yield:.2f}%" if elc_yield else "N/A",
                            delta=elc_delta
                        )
                        fourwk_elc = f"{elc_4wk_yield:.1f}%" if elc_4wk_yield else "-"
                        st.caption(f"n={fmt_vol(elc_uin)} | 4wk:{fourwk_elc}")

    except Exception as e:
        import traceback
        logger.error("Failed to render DID breakdown: %s", e)
        logger.error("Full traceback: %s", traceback.format_exc())
        st.error("Failed to render DID breakdown")


def render_summary_table(processor: DataProcessor) -> None:
    """Render yield summary table."""
    st.subheader("Yield Summary Table")

    try:
        table_data = processor.get_summary_table()
        if table_data.empty:
            st.info("No data available for table")
            return

        st.dataframe(
            table_data,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Units In": st.column_config.NumberColumn(format="%d"),
                "Units Pass": st.column_config.NumberColumn(format="%d"),
                "Yield %": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )

        csv = table_data.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="yield_summary.csv",
            mime="text/csv",
        )
    except Exception as e:
        logger.error("Failed to render table: %s", e)
        st.error("Failed to render summary table")


def render_bin_distribution_chart(processor: DataProcessor) -> None:
    """Render bin distribution bar chart with series filter."""
    st.subheader("Bin Distribution")

    try:
        # Get raw data to build series
        df = processor.dataframe
        if df.empty:
            st.info("No bin distribution data available")
            logger.warning("Bin chart: DataFrame is empty")
            return

        # Show bin type description based on test steps in data
        if "step" in df.columns:
            steps_in_data = df["step"].unique().tolist()
            bin_descriptions = []
            for step in steps_in_data:
                if step == "HMFN":
                    bin_descriptions.append(f"**{step}**: Soft Bin (Bin_1, Bin_20, Bin_65, etc.)")
                elif step in ["HMB1", "QMON"]:
                    bin_descriptions.append(f"**{step}**: Hard Bin (Bin_1, Bin_2, Bin_3)")
                else:
                    bin_descriptions.append(f"**{step}**: Bin type varies")
            if bin_descriptions:
                st.caption(" | ".join(bin_descriptions))

        # Find BIN columns (parser creates Bin_1_GOOD, Bin_20_CONT, etc. or legacy BIN1, BIN2)
        bin_cols = [col for col in df.columns if col.startswith("BIN") or col.startswith("Bin_")]
        logger.info(f"Bin chart: Found columns={list(df.columns)}")
        logger.info(f"Bin chart: BIN columns found={bin_cols}")

        if not bin_cols:
            st.info("No bin data available in the dataset")
            logger.warning("Bin chart: No BIN columns found in data")
            return

        # Check if bins have actual data (not all zeros/NaN)
        bin_data_sample = df[bin_cols].head()
        logger.info(f"Bin chart: Sample bin data:\n{bin_data_sample}")

        # Check for non-zero values
        total_non_zero = (df[bin_cols] > 0).sum().sum()
        logger.info(f"Bin chart: Total non-zero bin values={total_non_zero}")

        # Create series column
        df = df.copy()
        df["series"] = (
            df["design_id"].fillna("") + "_" +
            df["form_factor"].fillna("") + "_" +
            df["speed"].fillna("") + "_" +
            df["density"].fillna("") + "_" +
            df["step"].fillna("")
        )

        # Get unique series for filter
        all_series = sorted(df["series"].unique().tolist())

        # Series filter - DEFAULT TO EMPTY so user picks what to display
        selected_series = st.multiselect(
            "Select Series to Display",
            options=all_series,
            default=[],  # Empty by default - user picks
            key="bin_series_filter",
            help="Select which product combinations to show in the chart"
        )

        if not selected_series:
            st.info("👆 Select one or more series above to display the bin distribution chart")
            return

        # Filter data by selected series
        filtered_df = df[df["series"].isin(selected_series)]

        # Melt bin columns to long format
        bin_data = filtered_df.melt(
            id_vars=["series", "step"],
            value_vars=bin_cols,
            var_name="bin",
            value_name="percentage"
        )

        # Drop NaN values
        logger.info(f"Bin chart: Melted data rows before dropna={len(bin_data)}")
        bin_data = bin_data.dropna(subset=["percentage"])
        logger.info(f"Bin chart: Melted data rows after dropna={len(bin_data)}")

        if bin_data.empty:
            st.info("No bin data available for selected series")
            logger.warning("Bin chart: All bin data was NaN")
            return

        # Group by series and bin, take mean
        bin_data = bin_data.groupby(["series", "bin"])["percentage"].mean().reset_index()

        # Sort bins naturally by bin number
        # Handle both formats: Bin_1_GOOD (new) and BIN1 (legacy)
        def extract_bin_num(col_name):
            import re
            match = re.search(r'(\d+)', col_name)
            return int(match.group(1)) if match else 0

        bin_data["bin_num"] = bin_data["bin"].apply(extract_bin_num)
        bin_data = bin_data.sort_values(["bin_num", "series"])

        # Create display labels from column names
        # New format: Bin_1_GOOD -> "Bin_1: GOOD"
        # Legacy format: BIN1 -> "Bin_1"
        def format_bin_label(col_name):
            if col_name.startswith("Bin_"):
                # New format: Bin_1_GOOD -> extract parts
                parts = col_name.split("_", 2)  # Split into max 3 parts
                if len(parts) >= 3:
                    return f"Bin_{parts[1]}: {parts[2]}"
                elif len(parts) == 2:
                    return f"Bin_{parts[1]}"
            elif col_name.startswith("BIN"):
                # Legacy format: BIN1 -> Bin_1
                return f"Bin_{col_name[3:]}"
            return col_name

        bin_data["bin_label"] = bin_data["bin"].apply(format_bin_label)

        # Option to pin data labels on chart
        show_bin_labels = st.checkbox("Show data labels on chart", value=False, key="bin_show_labels")

        # Add text column for labels
        bin_data["label_text"] = bin_data["percentage"].apply(lambda x: f"{x:.1f}%")

        fig = px.bar(
            bin_data,
            x="bin_label",
            y="percentage",
            color="series",
            barmode="group",
            title="Bin Distribution by Series",
            labels={
                "bin_label": "Bin",
                "percentage": "Percentage %",
                "series": "Series",
            },
            custom_data=["series", "bin"],
            text="label_text" if show_bin_labels else None,
        )

        # Enhanced hover template with multiple data points
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" +
                          "Series: %{customdata[0]}<br>" +
                          "Percentage: %{y:.2f}%<br>" +
                          "<extra></extra>",
            textposition="outside" if show_bin_labels else None,
            textfont=dict(size=9) if show_bin_labels else None,
        )

        fig.update_layout(
            xaxis_title="Bin",
            yaxis_title="Percentage %",
            legend_title="Series",
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
        )

        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        logger.error("Failed to render bin chart: %s", e)
        st.error("Failed to render bin distribution chart")


def render_density_speed_heatmap(processor: DataProcessor) -> None:
    """Render yield heatmap by density and speed - compact grid layout (rows=DIDs, cols=steps)."""
    st.subheader("Yield by Density & Speed")

    try:
        data = processor.get_yield_by_density_speed()
        if data.empty:
            st.info("No density/speed data available")
            return

        df = processor.dataframe
        unique_steps = sorted(df["step"].unique().tolist()) if "step" in df.columns else []
        unique_design_ids = sorted(df["design_id"].unique().tolist()) if "design_id" in df.columns else []

        # Define step order for consistent column layout
        step_order = ['hmfn', 'hmb1', 'qmon']
        # Filter and sort steps based on order
        ordered_steps = [s for s in step_order if s in [x.lower() for x in unique_steps]]
        # Map back to original case
        step_map = {s.lower(): s for s in unique_steps}
        display_steps = [step_map.get(s, s) for s in ordered_steps]
        # Add any steps not in the predefined order
        for s in unique_steps:
            if s.lower() not in step_order:
                display_steps.append(s)

        if not display_steps:
            display_steps = unique_steps

        raw_df = df.copy()

        # Grid layout: one row per DID, columns for each step
        for design_id in unique_design_ids:
            st.markdown(f"**{design_id}**")

            # Create columns for steps
            num_steps = len(display_steps)
            cols = st.columns(num_steps)

            for col_idx, step in enumerate(display_steps):
                with cols[col_idx]:
                    # Filter data for this DID + step combination
                    filtered = raw_df[(raw_df["design_id"] == design_id) & (raw_df["step"] == step)]

                    if filtered.empty or "density" not in filtered.columns or "speed" not in filtered.columns:
                        st.caption(f"{step.upper()}")
                        st.info("No data")
                        continue

                    # Calculate yield by density/speed
                    grouped = (
                        filtered.groupby(["density", "speed"])
                        .agg({"UIN": "sum", "UPASS": "sum"})
                        .reset_index()
                    )
                    grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(2)

                    if grouped.empty:
                        st.caption(f"{step.upper()}")
                        st.info("No data")
                        continue

                    pivot = grouped.pivot_table(
                        index="density",
                        columns="speed",
                        values="yield_pct",
                        aggfunc="mean",
                    )

                    if pivot.empty:
                        st.caption(f"{step.upper()}")
                        st.info("No data")
                        continue

                    # Compact heatmap
                    fig = px.imshow(
                        pivot,
                        text_auto=".1f",
                        color_continuous_scale="RdYlGn",
                        aspect="auto",
                        labels={"color": "Yield %"},
                    )

                    fig.update_traces(
                        hovertemplate="<b>Density:</b> %{y}<br>" +
                                      "<b>Speed:</b> %{x}<br>" +
                                      "<b>Yield:</b> %{z:.2f}%<br>" +
                                      f"<b>Design:</b> {design_id}<br>" +
                                      f"<b>Step:</b> {step}<br>" +
                                      "<extra></extra>"
                    )

                    fig.update_layout(
                        title=dict(text=step.upper(), font=dict(size=14)),
                        xaxis_title="",
                        yaxis_title="",
                        margin=dict(l=10, r=10, t=30, b=10),
                        height=250,
                        coloraxis_showscale=False,  # Hide color bar to save space
                    )

                    st.plotly_chart(fig, use_container_width=True, key=f"heatmap_{design_id}_{step}")

            st.divider()

    except Exception as e:
        logger.error("Failed to render heatmap: %s", e)
        st.error("Failed to render density/speed heatmap")


def render_smt6_yield_section(filters: dict[str, Any]) -> None:
    """Render the SMT6 Machine Yield Trend section."""
    st.subheader("SMT6 Yield Trend")

    import streamlit.components.v1 as components

    # Check if HMFN is selected - SMT6 testers only perform HMFN testing
    selected_steps = filters.get("test_steps", [])
    hmfn_selected = "HMFN" in [s.upper() for s in selected_steps]

    if not hmfn_selected:
        st.warning(
            "⚠️ **HMFN step not selected.** SMT6 testers only perform HMFN testing. "
            "Please add **HMFN** to your test step selection in the sidebar to view SMT6 tester data."
        )
        return  # Exit early - no point showing the rest

    st.markdown("""
    **Machine-level yield tracking** for SMT6 testers at HMFN step.
    Shows yield trend by machine and site-level breakdown.
    """)

    # Fetch button and info
    col1, col2 = st.columns([1, 3])
    with col1:
        fetch_smt6 = st.button(
            "Fetch Machine Data",
            type="primary",
            use_container_width=True,
            key="fetch_smt6_btn"
        )
    with col2:
        try:
            workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
            cache_stats = get_smt6_cache_stats()
            st.caption(f"{len(filters['design_ids'])} DIDs × {len(workweeks)} weeks | Cache: {cache_stats['valid_entries']} entries")
        except Exception:
            st.caption("Configure workweek range in sidebar")

    # Fetch machine-level data
    if fetch_smt6:
        try:
            workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
            design_ids = filters.get("design_ids", ["Y63N", "Y6CP", "Y62P"])

            # Get density/speed filters (use first value if list provided)
            densities = filters.get("densities")
            speeds = filters.get("speeds")
            density_filter = densities[0] if densities else None
            speed_filter = speeds[0] if speeds else None

            filter_info = ""
            if density_filter:
                filter_info += f" density={density_filter}"
            if speed_filter:
                filter_info += f" speed={speed_filter}"

            with st.spinner(f"Fetching SMT6 machine data for {len(workweeks)} weeks{filter_info}..."):
                smt6_df = fetch_smt6_yield_data(
                    design_ids=design_ids,
                    workweeks=[str(ww) for ww in workweeks],
                    form_factor="socamm2",
                    max_workers=8,
                    density=density_filter,
                    speed=speed_filter
                )

            if not smt6_df.empty:
                st.session_state.smt6_data = smt6_df
                st.session_state.smt6_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success(f"Loaded {len(smt6_df)} SMT6 machine records!")
            else:
                # Check if user selected HMFN step - SMT6 only fetches HMFN data
                selected_steps = filters.get("test_steps", [])
                hmfn_selected = "HMFN" in [s.upper() for s in selected_steps]
                if not hmfn_selected and selected_steps:
                    st.warning("⚠️ No SMT6 data returned. SMT6 testers only perform **HMFN** testing. "
                              f"Your selected steps ({', '.join(selected_steps)}) do not include HMFN. "
                              "Please add HMFN to your step selection to view SMT6 tester data.")
                else:
                    st.warning("No SMT6 machine data returned for the selected filters.")

        except Exception as e:
            st.error(f"Failed to fetch SMT6 data: {e}")
            logger.exception("SMT6 fetch error")

    # Display SMT6 data if available
    has_machine_data = not st.session_state.smt6_data.empty
    has_site_data = not st.session_state.smt6_site_data.empty

    if has_machine_data or has_site_data:
        if st.session_state.smt6_last_fetch_time:
            st.caption(f"Last fetched: {st.session_state.smt6_last_fetch_time}")

        # Use main dashboard's design_ids filter (no redundant filter here)
        selected_design_ids = [did.upper() for did in filters.get("design_ids", [])]

        # Filter data based on main dashboard's design_ids
        if has_machine_data:
            if selected_design_ids:
                filtered_machine_df = st.session_state.smt6_data[
                    st.session_state.smt6_data['design_id'].isin(selected_design_ids)
                ]
            else:
                filtered_machine_df = st.session_state.smt6_data
        else:
            filtered_machine_df = pd.DataFrame()

        if has_site_data:
            if selected_design_ids:
                # Case-insensitive comparison for design_id
                selected_dids_upper = [d.upper() for d in selected_design_ids]
                filtered_site_df = st.session_state.smt6_site_data[
                    st.session_state.smt6_site_data['design_id'].str.upper().isin(selected_dids_upper)
                ]
            else:
                filtered_site_df = st.session_state.smt6_site_data
        else:
            filtered_site_df = pd.DataFrame()

        # =====================================================================
        # TESTER FLEET + MACHINE SUMMARY (Side by Side) + TREND CHART
        # =====================================================================
        if not filtered_machine_df.empty or not filtered_site_df.empty:
            cards_df = filtered_machine_df if not filtered_machine_df.empty else filtered_site_df
            latest_ww = cards_df['workweek'].max()

            # -----------------------------------------------------------------
            # SIDE BY SIDE: Tester Fleet (left) | Machine Summary (right)
            # -----------------------------------------------------------------
            col_fleet, col_summary = st.columns([1, 1])

            with col_fleet:
                st.markdown(f"##### 🖥️ Tester Fleet <span style='font-size:12px; color:#888;'>(WW{latest_ww})</span>", unsafe_allow_html=True)

                # Get all unique machines from full dataset
                all_machines = sorted(cards_df['machine_id'].unique())
                latest_df = cards_df[cards_df['workweek'] == latest_ww]

                # Build summary for ALL machines
                machine_stats = []
                for machine in all_machines:
                    machine_latest = latest_df[latest_df['machine_id'] == machine]
                    if not machine_latest.empty:
                        uin = machine_latest['uin_adj'].sum()
                        upass = machine_latest['upass_adj'].sum()
                        yield_val = (upass / uin * 100) if uin > 0 else 0
                        machine_stats.append({'machine': machine, 'yield': yield_val, 'uin': int(uin), 'has_data': True})
                    else:
                        machine_stats.append({'machine': machine, 'yield': None, 'uin': 0, 'has_data': False})

                # Build cards in a flex container
                cards_html = '<div style="display:flex; gap:12px; flex-wrap:wrap;">'
                for stats in machine_stats:
                    machine = stats['machine'].upper()
                    if stats['has_data']:
                        yield_val = stats['yield']
                        uin = stats['uin']
                        if yield_val >= 99.0:
                            status, color = "✅", "#00C853"
                        elif yield_val >= 96.0:
                            status, color = "⚠️", "#FFB300"
                        else:
                            status, color = "🔴", "#FF1744"

                        cards_html += f'''
                        <div style="text-align:center; padding:12px 16px; background:linear-gradient(135deg,#1a1a2e,#16213e);
                            border-radius:10px; border-left:4px solid {color}; flex:1; min-width:100px;
                            box-shadow: 0 2px 8px rgba(0,0,0,0.3);">
                            <div style="font-size:12px; font-weight:600; color:#fff;">{status} {machine}</div>
                            <div style="font-size:20px; font-weight:700; color:{color}; margin:4px 0;">{yield_val:.1f}%</div>
                            <div style="font-size:10px; color:#888;">{uin:,} UIN</div>
                        </div>'''
                    else:
                        cards_html += f'''
                        <div style="text-align:center; padding:12px 16px; background:linear-gradient(135deg,#2a2a3e,#1e1e2e);
                            border-radius:10px; border-left:4px solid #555; flex:1; min-width:100px;
                            box-shadow: 0 2px 8px rgba(0,0,0,0.2); opacity:0.6;">
                            <div style="font-size:12px; font-weight:600; color:#888;">⏸️ {machine}</div>
                            <div style="font-size:16px; font-weight:700; color:#666; margin:4px 0;">No Data</div>
                            <div style="font-size:10px; color:#666;">WW{latest_ww}</div>
                        </div>'''
                cards_html += '</div>'
                st.markdown(cards_html, unsafe_allow_html=True)

            with col_summary:
                st.markdown("##### 📋 Machine Summary <span style='font-size:12px; color:#888;'>(All Weeks)</span>", unsafe_allow_html=True)
                if not filtered_machine_df.empty:
                    summary_html = create_smt6_summary_table(filtered_machine_df, dark_mode=True)
                    if summary_html:
                        num_machines = filtered_machine_df['machine_id'].nunique()
                        table_height = 28 + (num_machines * 26) + 24
                        components.html(summary_html, height=min(table_height, 200), scrolling=False)

            # -----------------------------------------------------------------
            # CHART OPTIONS ROW
            # -----------------------------------------------------------------
            opt_col1, opt_col2, opt_col3 = st.columns([1, 1, 4])
            with opt_col1:
                show_data_labels = st.checkbox("Show Labels", value=False, key="smt6_show_labels")
            with opt_col2:
                data_min = filtered_machine_df['yield_pct'].min() if not filtered_machine_df.empty else 90
                default_min = max(0, int(data_min - 5))
                y_axis_min = st.slider("Y-Axis Min", 0, 95, default_min, 5, key="smt6_y_min")

            # -----------------------------------------------------------------
            # FULL-WIDTH TREND CHART
            # -----------------------------------------------------------------
            if not filtered_machine_df.empty:
                design_ids = filters.get("design_ids", [])
                design_id_label = ", ".join(design_ids) if design_ids else None

                fig = create_smt6_yield_chart(
                    filtered_machine_df,
                    design_id=design_id_label,
                    dark_mode=True,
                    show_data_labels=show_data_labels,
                    y_axis_min=float(y_axis_min)
                )
                if fig:
                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True})

        # =====================================================================
        # SOCKET/SITE ANALYSIS - COMPACT UNIFIED VERSION
        # =====================================================================
        st.divider()

        # Compact CSS for this section
        st.markdown("""<style>
        .site-section [data-testid="stVerticalBlock"] > div { gap: 0.2rem !important; }
        .site-section .stMarkdown { margin-bottom: 0 !important; }
        </style>""", unsafe_allow_html=True)

        # Get machine list from session state (updated after each fetch)
        all_machines = st.session_state.get('smt6_available_machines', [])

        # If session state is empty, try to populate from current data
        if not all_machines:
            if not filtered_site_df.empty:
                all_machines = sorted(filtered_site_df['machine_id'].unique())
            elif not filtered_machine_df.empty:
                all_machines = sorted(filtered_machine_df['machine_id'].unique())
            elif 'smt6_site_data' in st.session_state and not st.session_state.smt6_site_data.empty:
                all_machines = sorted(st.session_state.smt6_site_data['machine_id'].unique())
            if all_machines:
                st.session_state.smt6_available_machines = all_machines

        # Header with inline controls (always show machine filter)
        site_col1, site_col2, site_col3 = st.columns([3, 2, 2])
        with site_col1:
            st.markdown("#### 🔌 Socket & Site Health")
        with site_col2:
            site_fetch_mode = st.radio(
                "Range", options=["Latest Week", "Full Range"],
                key="smt6_site_mode", horizontal=True, label_visibility="collapsed"
            )

        # Auto-fetch: Check if we need to load data (BEFORE creating multiselect)
        def should_auto_fetch():
            """Check if auto-fetch is needed based on current state."""
            if 'smt6_site_data' not in st.session_state:
                return True
            if st.session_state.get('smt6_last_fetch_mode') != site_fetch_mode:
                return True
            if st.session_state.get('smt6_last_fetch_ww') != filters.get("end_ww"):
                return True
            return False

        # Auto-fetch on first load or mode change (BEFORE multiselect widget)
        if should_auto_fetch() and not st.session_state.get('smt6_fetching', False):
            st.session_state.smt6_fetching = True
            if site_fetch_mode == "Latest Week":
                wws = [str(filters["end_ww"])]
            else:
                wws = [str(ww) for ww in Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])]

            densities = filters.get("densities")
            speeds = filters.get("speeds")
            density_filter = densities[0] if densities else None
            speed_filter = speeds[0] if speeds else None

            with st.spinner(f"Loading site data ({site_fetch_mode})..."):
                site_df = fetch_smt6_site_data(
                    design_ids=filters.get("design_ids", ["Y6CP"]),
                    workweeks=wws,
                    form_factor=filters.get("form_factor", "socamm2").lower(),
                    density=density_filter, speed=speed_filter
                )
                if not site_df.empty:
                    st.session_state.smt6_site_data = site_df
                    st.session_state.smt6_last_fetch_mode = site_fetch_mode
                    st.session_state.smt6_last_fetch_ww = filters.get("end_ww")

                    # Filter by selected DIDs (case-insensitive comparison)
                    if selected_design_ids:
                        # Normalize both sides to uppercase for comparison
                        selected_dids_upper = [d.upper() for d in selected_design_ids]
                        filtered_for_machines = site_df[site_df['design_id'].str.upper().isin(selected_dids_upper)]
                    else:
                        filtered_for_machines = site_df

                    new_machines = sorted(filtered_for_machines['machine_id'].unique())
                    st.session_state.smt6_available_machines = new_machines

                    # Explicitly SET the filter to all machines (not just delete - that doesn't work)
                    st.session_state['smt6_machine_filter'] = new_machines

                    # Debug: Log the machine count for verification
                    logger.info(f"[SMT6 Filter] Mode='{site_fetch_mode}', DIDs={selected_design_ids}, machines={len(new_machines)}")

            st.session_state.smt6_fetching = False
            # Rerun to refresh - multiselect will recreate with new machines as default
            st.rerun()

        # Update all_machines from session state (may have been updated by auto-fetch)
        all_machines = st.session_state.get('smt6_available_machines', all_machines)

        with site_col3:
            if all_machines:
                selected_machines = st.multiselect(
                    "Machines", options=all_machines, default=all_machines,
                    format_func=lambda x: x.upper(), key="smt6_machine_filter",
                    label_visibility="collapsed", placeholder="Select machines..."
                )
            else:
                st.caption("Loading machines...")
                selected_machines = []

        # Apply machine filter
        if not filtered_site_df.empty and selected_machines:
            analysis_df = filtered_site_df[filtered_site_df['machine_id'].isin(selected_machines)].copy()
        else:
            analysis_df = pd.DataFrame()

        # Show analysis
        if not analysis_df.empty:
            latest_ww = analysis_df['workweek'].max()
            ww_options = sorted(analysis_df['workweek'].unique(), reverse=True)
            ww_label = f"WW{ww_options[0]}" if len(ww_options) == 1 else f"WW{ww_options[-1]}-{ww_options[0]}"
            latest_data = analysis_df[analysis_df['workweek'] == latest_ww] if site_fetch_mode == "Latest Week" else analysis_df
            machine_list = sorted(latest_data['machine_id'].unique())
            num_machines = len(machine_list)

            # Check for issues to determine auto-expand
            site_summary = latest_data.groupby(['machine_id', 'site']).agg({
                'uin_adj': 'sum', 'upass_adj': 'sum'
            }).reset_index()
            site_summary['yield_pct'] = (site_summary['upass_adj'] / site_summary['uin_adj'] * 100).round(2)
            has_issues = (site_summary['yield_pct'] < 95).any()

            # =========== SIDE-BY-SIDE LAYOUT: Site Heatmap | Site Health Summary ===========
            # Calculate adaptive heights based on actual data
            num_sites = len(site_summary['site'].unique())

            # Heatmap height: based on sites (rows)
            heatmap_height = min(450, max(200, num_sites * 24 + 80))

            # Summary height: based on machines with issues
            machines_with_issues = [m for m in machine_list if (site_summary[site_summary['machine_id'] == m]['yield_pct'] < 95).any()]
            num_issues = len(machines_with_issues)

            if num_issues == 0:
                summary_height = 80  # Compact "all healthy" message
            else:
                # Each machine card: header + 5 sockets, cards stack vertically in narrow column
                summary_height = min(450, 80 + num_issues * 220)

            # Use common height for consistent side-by-side display
            common_height = max(heatmap_height, summary_height)

            heatmap_col, summary_col = st.columns([1, 1])

            with heatmap_col:
                with st.expander(f"🗺️ Site Heatmap ({ww_label})", expanded=has_issues):
                    # Create pivot tables for yield, UIN, and UFAIL
                    pivot_yield = site_summary.pivot_table(index='site', columns='machine_id', values='yield_pct', aggfunc='mean')
                    pivot_uin = site_summary.pivot_table(index='site', columns='machine_id', values='uin_adj', aggfunc='sum')
                    pivot_upass = site_summary.pivot_table(index='site', columns='machine_id', values='upass_adj', aggfunc='sum')

                    if not pivot_yield.empty:
                        pivot_yield = pivot_yield.sort_index()
                        pivot_uin = pivot_uin.reindex(pivot_yield.index)
                        pivot_upass = pivot_upass.reindex(pivot_yield.index)
                        pivot_ufail = pivot_uin - pivot_upass

                        # Create custom text with yield%, UIN, UFAIL
                        custom_text = []
                        for i, row_idx in enumerate(pivot_yield.index):
                            row_text = []
                            for j, col_idx in enumerate(pivot_yield.columns):
                                y = pivot_yield.iloc[i, j]
                                uin = pivot_uin.iloc[i, j]
                                ufail = pivot_ufail.iloc[i, j]
                                if pd.notna(y):
                                    row_text.append(f"{y:.0f}% UIN:{int(uin)} UFAIL:{int(ufail)}")
                                else:
                                    row_text.append("-")
                            custom_text.append(row_text)

                        # Simplified text for cells - just yield % (details in hover)
                        simple_text = [[f"{v:.0f}%" if pd.notna(v) else "-" for v in row] for row in pivot_yield.values]

                        fig = go.Figure(data=go.Heatmap(
                            z=pivot_yield.values, x=[m.upper() for m in pivot_yield.columns], y=pivot_yield.index.tolist(),
                            colorscale=[[0, '#dc3545'], [0.3, '#ffc107'], [0.7, '#17a2b8'], [1, '#28a745']],
                            zmin=90, zmax=100,
                            text=simple_text,
                            texttemplate="%{text}", textfont={"size": 11, "color": "white"},
                            hovertemplate="<b>%{y}</b> @ %{x}<br>Yield: %{z:.1f}%<br>UIN: %{customdata[0]:,}<br>UFAIL: %{customdata[1]:,}<extra></extra>",
                            customdata=[[(pivot_uin.iloc[i, j], pivot_ufail.iloc[i, j]) for j in range(len(pivot_yield.columns))] for i in range(len(pivot_yield.index))],
                            colorbar=dict(title="", ticksuffix="%", len=0.6, thickness=12, tickfont=dict(size=10))
                        ))
                        fig.update_layout(
                            xaxis=dict(tickfont=dict(size=11, color='#333'), side='top'),
                            yaxis=dict(autorange='reversed', tickfont=dict(size=10, color='#333')),
                            height=common_height, margin=dict(l=80, r=15, t=50, b=10)
                        )
                        # Toolbar shows on hover only (doesn't block content)
                        heatmap_config = {
                            'displayModeBar': 'hover',
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'toImageButtonOptions': {
                                'format': 'png',
                                'filename': f'site_heatmap_{ww_label}',
                                'height': 800,
                                'width': 1200,
                                'scale': 2
                            }
                        }
                        # Add "Open in New Tab" button
                        if st.button("🔗 Open Heatmap in New Tab", key=f"heatmap_newtab_{ww_label}", help="Open full-size heatmap in a new browser tab"):
                            # Convert figure to HTML and open in new tab
                            import base64
                            html_content = fig.to_html(include_plotlyjs='cdn', full_html=True, config={'displayModeBar': True, 'displaylogo': False})
                            b64 = base64.b64encode(html_content.encode()).decode()
                            js_code = f'''
                            <script>
                                var newTab = window.open();
                                newTab.document.write(atob("{b64}"));
                                newTab.document.close();
                            </script>
                            '''
                            components.html(js_code, height=0)
                        st.plotly_chart(fig, use_container_width=True, config=heatmap_config)

            with summary_col:
                with st.expander(f"📊 Site Health Summary ({ww_label})", expanded=has_issues):
                    summary_html = create_site_channel_summary_html(
                        latest_data,
                        max_issues=5  # Always show max 5 critical sockets per machine
                    )
                    if summary_html:
                        # Add "Open in New Tab" button for Site Health Summary
                        if st.button("🔗 Open Summary in New Tab", key=f"summary_newtab_{ww_label}", help="Open full-size summary in a new browser tab"):
                            import base64
                            full_html = f'''
                            <!DOCTYPE html>
                            <html>
                            <head>
                                <title>Site Health Summary - {ww_label}</title>
                                <style>
                                    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; }}
                                    h1 {{ color: #fff; text-align: center; margin-bottom: 20px; }}
                                </style>
                            </head>
                            <body>
                                <h1>Site Health Summary - {ww_label}</h1>
                                {summary_html}
                            </body>
                            </html>
                            '''
                            b64 = base64.b64encode(full_html.encode()).decode()
                            js_code = f'''
                            <script>
                                var newTab = window.open();
                                newTab.document.write(atob("{b64}"));
                                newTab.document.close();
                            </script>
                            '''
                            components.html(js_code, height=0)
                        # Display the summary HTML
                        components.html(summary_html, height=common_height + 30, scrolling=True)

        elif not filtered_site_df.empty:
            st.warning("Select at least one machine.")
        else:
            st.info("Loading site data..." if st.session_state.get('smt6_fetching') else "No site data available. Check filters.")

    else:
        st.info("Click 'Fetch Machine Data' to load machine-level yield data.")


def render_dashboard(processor: DataProcessor, filters: dict[str, Any] = None) -> None:
    """Render all dashboard components."""
    # Summary metrics at top (Total Units In, Pass, Yield, Range)
    render_summary_metrics(processor)
    st.divider()

    # DID breakdown and Summary table side by side
    left_col, right_col = st.columns([1, 1])
    with left_col:
        render_did_breakdown(processor)
    with right_col:
        render_summary_table(processor)
    st.divider()

    # Charts section (collapsible)
    with st.expander("📈 Yield Trends & Distribution", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            render_yield_trend_chart(processor)
        with col2:
            render_bin_distribution_chart(processor)

    # Heatmaps section (collapsible)
    with st.expander("🗺️ Yield by Density & Speed", expanded=True):
        render_density_speed_heatmap(processor)


def fetch_elc_data(filters: dict[str, Any], use_cache: bool = True) -> pd.DataFrame:
    """Fetch data for ELC yield calculation (HMFN, HMB1, QMON steps).

    Args:
        filters: Filter parameters (design_ids, form_factors, workweeks, etc.)
        use_cache: Whether to use cached results

    Returns:
        DataFrame with yield data for all three steps
    """
    logger.info("=" * 60)
    logger.info(f"FETCH ELC DATA STARTED (cache={'ON' if use_cache else 'OFF'})")
    logger.info(f"Filters: {filters}")

    # Use more workers for ELC since it's 3 steps
    runner = FrptRunner(max_workers=8, use_cache=use_cache)
    parser = FrptParser()

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        logger.info(f"Workweeks to fetch: {workweeks}")
    except Exception as e:
        logger.error("Failed to generate workweek range: %s", e)
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    # Build commands for HMFN, HMB1, QMON steps
    elc_steps = ["HMFN", "HMB1", "QMON"]
    commands = []
    for design_id in filters["design_ids"]:
        for step in elc_steps:
            for form_factor in filters["form_factors"]:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=design_id,
                            facility=filters["facility"],
                        )
                        commands.append(command)
                    except ValueError as e:
                        logger.warning(f"Invalid command parameters: {e}")

    total_calls = len(commands)
    logger.info(f"Total frpt calls for ELC: {total_calls}")

    if total_calls == 0:
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(completed: int, total: int, cmd: FrptCommand) -> None:
        progress_bar.progress(completed / total)
        status_text.text(f"Completed {completed}/{total}: {cmd.step}/{cmd.form_factor}/WW{cmd.workweek}")

    # Execute all commands in parallel
    cmd_results = runner.run_parallel(commands, progress_callback=progress_callback)

    progress_bar.empty()
    status_text.empty()

    # Collect results - run_parallel returns list of (FrptCommand, FrptResult) tuples
    results = []
    for cmd, result in cmd_results:
        if result.success and result.stdout:
            results.append((result.stdout, cmd.step, cmd.form_factor))

    if not results:
        logger.warning("No successful results for ELC data")
        return pd.DataFrame()

    # Parse results
    try:
        all_parsed = []
        for stdout, step, ff in results:
            parsed = parser.parse(stdout, step)
            if not parsed.empty:
                all_parsed.append(parsed)

        if not all_parsed:
            return pd.DataFrame()

        df = pd.concat(all_parsed, ignore_index=True)
        logger.info(f"ELC data parsed: {len(df)} rows")
        return df

    except Exception as e:
        logger.error("Failed to parse ELC results: %s", e)
        raise RuntimeError(f"Failed to parse frpt output: {e}") from e


def calculate_elc_yields(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate HMFN, SLT, and ELC yields from raw data.

    SLT yield = HMB1 yield × QMON yield
    ELC yield = SLT yield × HMFN yield

    Args:
        df: DataFrame with yield data including step column

    Returns:
        DataFrame with calculated yields per design_id, form_factor, density, speed, workweek
    """
    if df.empty:
        return pd.DataFrame()

    # Group by key dimensions and step, calculate yield
    group_cols = ["design_id", "form_factor", "density", "speed", "workweek"]
    available_cols = [c for c in group_cols if c in df.columns]

    if not available_cols:
        logger.warning("No grouping columns available for ELC calculation")
        return pd.DataFrame()

    # Calculate yield per group
    grouped = df.groupby(available_cols + ["step"]).agg({
        "UIN": "sum",
        "UPASS": "sum"
    }).reset_index()

    grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(4)

    # Pivot to get HMFN, HMB1, QMON as separate columns
    pivot = grouped.pivot_table(
        index=available_cols,
        columns="step",
        values="yield_pct",
        aggfunc="first"
    ).reset_index()

    # Rename columns for clarity
    pivot.columns.name = None

    # Calculate SLT and ELC yields
    if "HMB1" in pivot.columns and "QMON" in pivot.columns:
        pivot["SLT"] = (pivot["HMB1"] * pivot["QMON"] / 100).round(2)
    else:
        pivot["SLT"] = None

    if "HMFN" in pivot.columns and "SLT" in pivot.columns:
        pivot["ELC"] = (pivot["HMFN"] * pivot["SLT"] / 100).round(2)
    else:
        pivot["ELC"] = None

    # Round individual yields
    for col in ["HMFN", "HMB1", "QMON"]:
        if col in pivot.columns:
            pivot[col] = pivot[col].round(2)

    logger.info(f"ELC yields calculated: {len(pivot)} rows")
    return pivot


def render_elc_yield_tab(filters: dict[str, Any]) -> None:
    """Render the Module ELC Yield tab content."""
    # Header
    st.header("Module ELC Yield")
    st.info("📈 **ELC = HMFN × SLT** | SLT = HMB1×QMON (or single step if only one selected) — Required: WW range, Form Factor, DID, Facility, Step (HMFN/HMB1/QMON) | Optional: Density, Speed")

    # Cache controls
    use_cache = st.session_state.get("use_cache", True)

    # Calculate estimated queries
    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        total_queries = len(filters["design_ids"]) * 3 * len(filters["form_factors"]) * len(workweeks)
        est_time = max((total_queries // 8) * 0.5, 1)  # ~30sec per batch of 8
    except Exception:
        total_queries = 0
        est_time = 0

    # ========================================================================
    # SECTION 1: DATA FETCH
    # ========================================================================
    st.subheader("1️⃣ Fetch ELC Data")
    col1, col2 = st.columns([1, 4])
    with col1:
        fetch_elc = st.button(
            "Fetch ELC Data",
            type="primary",
            use_container_width=True,
            key="fetch_elc_btn"
        )
    with col2:
        if total_queries > 0:
            st.caption(f"Will fetch {total_queries} queries (3 steps × {len(workweeks)} weeks). Est. time: ~{est_time:.0f} min if not cached.")

    if fetch_elc:
        try:
            st.warning(f"Fetching HMFN, HMB1, and QMON data ({total_queries} queries)... Cached results are instant, fresh queries take ~30s each.")
            elc_raw = fetch_elc_data(filters, use_cache=use_cache)

            if elc_raw.empty:
                st.warning("No data returned for ELC calculation.")
                return

            # Apply speed and density filters if specified
            if filters.get("speeds") and "speed" in elc_raw.columns:
                elc_raw = elc_raw[elc_raw["speed"].isin(filters["speeds"])]
                logger.info(f"Filtered by speeds {filters['speeds']}: {len(elc_raw)} rows")

            if filters.get("densities") and "density" in elc_raw.columns:
                elc_raw = elc_raw[elc_raw["density"].isin(filters["densities"])]
                logger.info(f"Filtered by densities {filters['densities']}: {len(elc_raw)} rows")

            if elc_raw.empty:
                st.warning("No data after applying speed/density filters.")
                return

            # Calculate yields
            elc_yields = calculate_elc_yields(elc_raw)
            st.session_state.elc_data = elc_yields
            st.session_state.elc_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success(f"Loaded {len(elc_yields)} ELC yield records!")

        except Exception as e:
            st.error(f"Failed to fetch ELC data: {e}")
            logger.exception("ELC fetch error")
            return

    # Display ELC data if available
    if not st.session_state.elc_data.empty:
        elc_df = st.session_state.elc_data.copy()

        # Build filter context string for display
        filter_parts = []
        if filters.get("design_ids"):
            filter_parts.append(f"DID: {', '.join(filters['design_ids'])}")
        if filters.get("form_factors"):
            filter_parts.append(f"Form: {', '.join(filters['form_factors'])}")
        if filters.get("densities"):
            filter_parts.append(f"Density: {', '.join(filters['densities'])}")
        if filters.get("speeds"):
            filter_parts.append(f"Speed: {', '.join(filters['speeds'])}")
        if filters.get("start_ww") and filters.get("end_ww"):
            filter_parts.append(f"WW: {filters['start_ww']}-{filters['end_ww']}")
        filter_context = " | ".join(filter_parts) if filter_parts else "All data"

        # Show last fetch time and filter context
        if st.session_state.elc_last_fetch_time:
            st.caption(f"📊 **Data based on:** {filter_context}")
            st.caption(f"🕐 Last fetched: {st.session_state.elc_last_fetch_time}")

        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            avg_hmfn = elc_df["HMFN"].mean() if "HMFN" in elc_df.columns else 0
            st.metric("Avg HMFN Yield", f"{avg_hmfn:.2f}%")
        with col2:
            avg_slt = elc_df["SLT"].mean() if "SLT" in elc_df.columns else 0
            st.metric("Avg SLT Yield", f"{avg_slt:.2f}%")
        with col3:
            avg_elc = elc_df["ELC"].mean() if "ELC" in elc_df.columns else 0
            st.metric("Avg ELC Yield", f"{avg_elc:.2f}%")

        st.divider()

        # ========================================================================
        # SECTION 2: CHART CONFIGURATION & SERIES SELECTION
        # ========================================================================
        st.subheader("2️⃣ Configure Chart")

        # Prepare data for chart
        # Ensure workweek is in YYYYWW format (remove any WW prefix if present)
        elc_df["workweek"] = elc_df["workweek"].astype(str).str.replace("WW", "")

        # Sort by workweek numerically
        elc_df["ww_sort"] = elc_df["workweek"].astype(int)
        elc_df = elc_df.sort_values("ww_sort")

        # Get sorted workweeks for x-axis ordering
        sorted_workweeks = elc_df["workweek"].unique().tolist()

        # Available yield types
        available_yield_types = [c for c in ["HMFN", "SLT", "ELC"] if c in elc_df.columns]

        # Build combined series options: DID_STEP_DENSITY_SPEED format
        # Create base series identifier (DID_DENSITY_SPEED)
        base_series_cols = [c for c in ["design_id", "density", "speed"] if c in elc_df.columns]
        if base_series_cols:
            elc_df["base_series"] = elc_df[base_series_cols].astype(str).agg("_".join, axis=1)
        else:
            elc_df["base_series"] = "All"

        # Get unique base series
        unique_base_series = elc_df["base_series"].unique().tolist()

        # Build combined options: DID_STEP_DENSITY_SPEED
        combined_series_options = []
        for base in unique_base_series:
            for yield_type in available_yield_types:
                # Parse base series to rebuild as DID_STEP_DENSITY_SPEED
                parts = base.split("_")
                if len(parts) >= 3:
                    did, density, speed = parts[0], parts[1], parts[2]
                    combined_label = f"{did}_{yield_type}_{density}_{speed}"
                else:
                    combined_label = f"{base}_{yield_type}"
                combined_series_options.append(combined_label)

        # Sort options for better organization (by DID, then by step)
        combined_series_options = sorted(combined_series_options)

        # Check if user filtered for specific density/speed
        user_filtered_density = bool(filters.get("densities"))
        user_filtered_speed = bool(filters.get("speeds"))

        # Series selection
        st.markdown("**Select Yield Lines to Display:**")
        selected_series = st.multiselect(
            "Series (DID_STEP_DENSITY_SPEED)",
            options=combined_series_options,
            default=[],  # No default - user must choose
            key="elc_series_selection",
            label_visibility="collapsed",
            help="Select which yield series to display on the chart. Format: DID_STEP_DENSITY_SPEED"
        )

        # Show explanatory note about available options
        if not user_filtered_density or not user_filtered_speed:
            filter_note_parts = []
            if not user_filtered_density:
                filter_note_parts.append("density")
            if not user_filtered_speed:
                filter_note_parts.append("speed")
            st.caption(
                f"💡 *All available {' and '.join(filter_note_parts)} combinations are shown because no specific "
                f"{' or '.join(filter_note_parts)} filter was applied in the sidebar. "
                f"To narrow down options, select specific {' and '.join(filter_note_parts)} values in the sidebar filters before fetching data.*"
            )

        # Chart options row 1: Display options
        opt_row1 = st.columns([1, 1, 1, 1])
        with opt_row1[0]:
            show_elc_labels = st.checkbox("Show data labels", value=False, key="elc_trend_show_labels")
        with opt_row1[1]:
            y_min = st.slider("Y-axis min %", min_value=0, max_value=98, value=90, step=1, key="elc_yaxis_min")
        with opt_row1[2]:
            # Target curve selector (moved from Section 4)
            selected_curve = st.selectbox(
                "Target Curve",
                options=CURVE_ORDER[::-1],
                index=0,
                key="elc_target_curve",
                help="Select curve for target lines"
            )
        with opt_row1[3]:
            # Single "Show Targets" toggle - auto-shows targets for selected yield types
            show_targets = st.checkbox("Show Targets", value=False, key="elc_show_targets",
                                       help="Show target lines for selected series")
            show_future = st.checkbox("+ Future (8wk)", value=False, key="elc_show_future_targets",
                                      help="Extend with next 8 weeks' targets")

        # Extract selected yield types from selected series for target configuration
        selected_yields = list(set(
            s.split("_")[1] for s in selected_series if len(s.split("_")) >= 2
        ))

        # Get selected filters for target key building (needed for chart targets)
        chart_selected_dids = [d.upper() for d in (filters.get("design_ids") or [])]
        chart_selected_densities = [normalize_density(d) for d in (filters.get("densities") or [])]
        chart_selected_speeds = [normalize_speed(s) for s in (filters.get("speeds") or [])]

        # Fallback: infer from data if filters are empty but data has single unique value
        if not chart_selected_densities and "density" in elc_df.columns:
            unique_densities = elc_df["density"].dropna().unique()
            if len(unique_densities) == 1:
                chart_selected_densities = [normalize_density(str(unique_densities[0]))]

        if not chart_selected_speeds and "speed" in elc_df.columns:
            unique_speeds = elc_df["speed"].dropna().unique()
            if len(unique_speeds) == 1:
                chart_selected_speeds = [normalize_speed(str(unique_speeds[0]))]

        if not chart_selected_dids and "design_id" in elc_df.columns:
            unique_dids = elc_df["design_id"].dropna().unique()
            if len(unique_dids) == 1:
                chart_selected_dids = [str(unique_dids[0]).upper()]

        # NEW: Extract unique configs from selected series (format: DID_STEP_DENSITY_SPEED)
        # This allows targets when user selects specific series even without sidebar filter restriction
        series_configs = set()
        for s in selected_series:
            parts = s.split("_")
            if len(parts) >= 4:
                # Format: DID_STEP_DENSITY_SPEED -> extract DID, DENSITY, SPEED
                did = parts[0].upper()
                density = normalize_density(parts[2])
                speed = normalize_speed(parts[3])
                series_configs.add((did, density, speed))

        # Check if we can show targets:
        # 1. From sidebar filters (single DID + Density + Speed), OR
        # 2. From selected series (all series share same DID + Density + Speed)
        can_show_targets = (
            len(chart_selected_dids) == 1 and
            len(chart_selected_densities) == 1 and
            len(chart_selected_speeds) == 1
        )

        # If sidebar doesn't give single config, check if selected series do
        if not can_show_targets and len(series_configs) == 1:
            config = list(series_configs)[0]
            chart_selected_dids = [config[0]]
            chart_selected_densities = [config[1]]
            chart_selected_speeds = [config[2]]
            can_show_targets = True

        # Set defaults for target configuration (will be updated in Section 4 if available)
        selected_curve = st.session_state.get("elc_target_curve", ACTIVE_CURVE)
        target_key = None
        target_label = None
        show_hmfn_target = False
        show_slt_target = False
        show_elc_target = False

        if can_show_targets:
            target_key = f"{chart_selected_dids[0]}_{chart_selected_densities[0]}_{chart_selected_speeds[0]}"
            target_label = f"{chart_selected_dids[0]} {chart_selected_densities[0]} {chart_selected_speeds[0]}"

        st.divider()

        # ========================================================================
        # SECTION 3: YIELD TREND CHART (Chart renders first!)
        # ========================================================================
        if "workweek" in elc_df.columns:
            st.subheader("3️⃣ ELC Yield Trend Chart")

            if not selected_series:
                st.info("👆 Select series above to display on the chart (format: DID_STEP_DENSITY_SPEED)")
            else:
                # Build chart using graph objects for better control
                fig = go.Figure()

                # Color map for yield types
                colors = {"HMFN": "#636EFA", "SLT": "#EF553B", "ELC": "#00CC96"}

                # Plot each selected combined series
                for combined_series in selected_series:
                    # Parse combined series: DID_STEP_DENSITY_SPEED
                    parts = combined_series.split("_")
                    if len(parts) >= 4:
                        did = parts[0]
                        yield_type = parts[1]
                        density = parts[2]
                        speed = parts[3]
                        base_series = f"{did}_{density}_{speed}"
                    else:
                        # Fallback for unexpected format
                        continue

                    # Filter data for this base series
                    mask = elc_df["base_series"] == base_series
                    series_data = elc_df[mask].sort_values("workweek")

                    if series_data.empty or yield_type not in series_data.columns:
                        continue

                    # Determine trace mode based on show_labels
                    trace_mode = "lines+markers+text" if show_elc_labels else "lines+markers"

                    # Convert to lists for plotly
                    x_vals = series_data["workweek"].tolist()
                    y_vals = series_data[yield_type].tolist()
                    text_vals = [f"{v:.1f}%" for v in y_vals] if show_elc_labels else None

                    fig.add_trace(
                        go.Scatter(
                            x=x_vals,
                            y=y_vals,
                            mode=trace_mode,
                            name=combined_series,
                            text=text_vals,
                            textposition="top center",
                            textfont=dict(size=9),
                            line=dict(color=colors.get(yield_type, "#636EFA")),
                            marker=dict(size=8),
                            hovertemplate="<b>Work Week:</b> %{x}<br>" +
                                          f"<b>Series:</b> {combined_series}<br>" +
                                          "<b>Yield:</b> %{y:.2f}%<br>" +
                                          "<extra></extra>",
                        )
                    )


                # ============================================================
                # ADD TARGET LINES (auto-show for selected yield types when "Show Targets" is on)
                # ============================================================
                show_targets = st.session_state.get("elc_show_targets", False)
                selected_curve = st.session_state.get("elc_target_curve", ACTIVE_CURVE)

                # Auto-derive which targets to show based on selected series
                show_hmfn_target = show_targets and "HMFN" in selected_yields
                show_slt_target = show_targets and "SLT" in selected_yields
                show_elc_target = show_targets and "ELC" in selected_yields

                if can_show_targets and target_key and show_targets:

                    # HMFN Target (from HMFN_TARGETS)
                    if show_hmfn_target:
                        hmfn_target_x = []
                        hmfn_target_y = []
                        for ww in sorted_workweeks:
                            year, month = get_calendar_year_month(ww)
                            target = get_target(HMFN_TARGETS, chart_selected_dids[0], chart_selected_densities[0], chart_selected_speeds[0], year, month)
                            if target is not None:
                                hmfn_target_x.append(ww)
                                hmfn_target_y.append(target)

                        if hmfn_target_y:
                            fig.add_trace(
                                go.Scatter(
                                    x=hmfn_target_x,
                                    y=hmfn_target_y,
                                    mode="lines",
                                    name=f"HMFN Target [{selected_curve}] ({target_label})",
                                    line=dict(color="#00BFFF", width=3, dash="dot", shape="hv"),
                                    showlegend=True,
                                    hovertemplate=f"<b>HMFN Target [{selected_curve}] ({target_label}):</b> %{{y:.2f}}%<extra></extra>",
                                )
                            )

                    # SLT Target (from SLT_TARGETS)
                    if show_slt_target:
                        slt_target_x = []
                        slt_target_y = []
                        for ww in sorted_workweeks:
                            year, month = get_calendar_year_month(ww)
                            target = get_target(SLT_TARGETS, chart_selected_dids[0], chart_selected_densities[0], chart_selected_speeds[0], year, month)
                            if target is not None:
                                slt_target_x.append(ww)
                                slt_target_y.append(target)

                        if slt_target_y:
                            fig.add_trace(
                                go.Scatter(
                                    x=slt_target_x,
                                    y=slt_target_y,
                                    mode="lines",
                                    name=f"SLT Target [{selected_curve}] ({target_label})",
                                    line=dict(color="#FF1744", width=3, dash="dot", shape="hv"),
                                    showlegend=True,
                                    hovertemplate=f"<b>SLT Target [{selected_curve}] ({target_label}):</b> %{{y:.2f}}%<extra></extra>",
                                )
                            )

                    # ELC Target (from ELC_CURVES based on selected curve)
                    if show_elc_target:
                        elc_target_x = []
                        elc_target_y = []
                        for ww in sorted_workweeks:
                            year, month = get_calendar_year_month(ww)
                            target = get_curve_target(selected_curve, target_key, year, month)
                            if target is not None:
                                elc_target_x.append(ww)
                                elc_target_y.append(target)

                        # Color varies if viewing historical curve: green for D1, orange for others
                        elc_line_color = "#39FF14" if selected_curve == ACTIVE_CURVE else "#FFA726"
                        elc_line_style = "dot" if selected_curve == ACTIVE_CURVE else "dash"

                        if elc_target_y:
                            fig.add_trace(
                                go.Scatter(
                                    x=elc_target_x,
                                    y=elc_target_y,
                                    mode="lines",
                                    name=f"ELC Target [{selected_curve}] ({target_label})",
                                    line=dict(color=elc_line_color, width=3, dash=elc_line_style, shape="hv"),
                                    showlegend=True,
                                    hovertemplate=f"<b>ELC Target [{selected_curve}] ({target_label}):</b> %{{y:.2f}}%<extra></extra>",
                                )
                            )

                # ============================================================
                # ADD FUTURE TARGET LINES (SLT & ELC, extends x-axis)
                # Shows upcoming target values from selected curve
                # ============================================================
                show_future_targets = st.session_state.get("elc_show_future_targets", False)
                future_wws = []  # Will be used to extend x-axis

                if show_future_targets and can_show_targets and target_key:
                    selected_curve = st.session_state.get("elc_target_curve", ACTIVE_CURVE)

                    # Generate next 8 weeks' workweeks
                    last_ww = int(sorted_workweeks[-1])
                    forecast_wws = []

                    for i in range(1, 9):  # Next 8 weeks
                        next_ww = last_ww + i
                        year = next_ww // 100
                        week = next_ww % 100
                        if week > 52:
                            year += 1
                            week = week - 52
                        forecast_wws.append(f"{year}{week:02d}")

                    # SLT Future Targets (if SLT is in selected yields)
                    if show_slt_target:
                        slt_future_y = []
                        for ww_str in forecast_wws:
                            cal_year, cal_month = get_calendar_year_month(ww_str)
                            target = get_target(SLT_TARGETS, chart_selected_dids[0], chart_selected_densities[0], chart_selected_speeds[0], cal_year, cal_month)
                            slt_future_y.append(target if target else None)

                        valid_slt = [(w, y) for w, y in zip(forecast_wws, slt_future_y) if y is not None]
                        if valid_slt:
                            slt_wws, slt_y = zip(*valid_slt)
                            future_wws = list(slt_wws)  # Use for x-axis extension
                            fig.add_trace(
                                go.Scatter(
                                    x=list(slt_wws),
                                    y=list(slt_y),
                                    mode="lines+markers",
                                    name=f"Future SLT Target [{selected_curve}]",
                                    line=dict(color="#FF1744", width=2, dash="dash"),
                                    marker=dict(size=6, symbol="diamond"),
                                    hovertemplate="<b>Future Week:</b> %{x}<br>" +
                                                  f"<b>Target Curve:</b> {selected_curve}<br>" +
                                                  "<b>SLT Target:</b> %{y:.2f}%<br>" +
                                                  "<extra></extra>",
                                )
                            )

                    # ELC Future Targets (if ELC is in selected yields)
                    if show_elc_target:
                        elc_future_y = []
                        for ww_str in forecast_wws:
                            cal_year, cal_month = get_calendar_year_month(ww_str)
                            target = get_curve_target(selected_curve, target_key, cal_year, cal_month)
                            elc_future_y.append(target if target else None)

                        valid_elc = [(w, y) for w, y in zip(forecast_wws, elc_future_y) if y is not None]
                        if valid_elc:
                            elc_wws, elc_y = zip(*valid_elc)
                            future_wws = list(elc_wws)  # Use for x-axis extension
                            fig.add_trace(
                                go.Scatter(
                                    x=list(elc_wws),
                                    y=list(elc_y),
                                    mode="lines+markers",
                                    name=f"Future ELC Target [{selected_curve}]",
                                    line=dict(color="#9C27B0", width=2, dash="dash"),
                                    marker=dict(size=6, symbol="diamond"),
                                    hovertemplate="<b>Future Week:</b> %{x}<br>" +
                                                  f"<b>Target Curve:</b> {selected_curve}<br>" +
                                                  "<b>ELC Target:</b> %{y:.2f}%<br>" +
                                                  "<extra></extra>",
                                )
                            )

                # ============================================================
                # ADD ANNOTATION MARKERS (callouts on data points)
                # Places highlighted markers on actual yield values with note balloons
                # ============================================================
                # Initialize pending annotations list
                pending_annotations = []

                if st.session_state.get("elc_annotations") and selected_series:
                    ann_colors = ["#FF6B6B", "#FFB347", "#87CEEB", "#DDA0DD", "#90EE90"]
                    ann_debug_info = []

                    for ann_idx, ann in enumerate(st.session_state.elc_annotations):
                        ann_ww_str = str(ann["workweek"])
                        sorted_ww_str = [str(ww) for ww in sorted_workweeks]
                        ww_match = ann_ww_str in sorted_ww_str
                        ann_debug_info.append(f"Ann#{ann_idx}: WW={ann_ww_str}, in_data={ww_match}")

                        if ww_match:
                            ann_color = ann_colors[ann_idx % len(ann_colors)]
                            ann_text = ann['note'][:25] + "..." if len(ann['note']) > 25 else ann['note']

                            # Find ONE yield value at this workweek (use first matching series)
                            # Only need one marker per annotation, not one per series
                            ann_y_val = None
                            ann_series_label = None

                            for series in selected_series:
                                parts = series.split("_")
                                if len(parts) >= 4:
                                    did, yield_type, density, speed = parts[0], parts[1], parts[2], parts[3]

                                    # Get the yield value at this workweek
                                    mask = (
                                        (elc_df["design_id"].astype(str).str.upper() == did.upper()) &
                                        (elc_df["workweek"].astype(str) == ann_ww_str)
                                    )
                                    if "density" in elc_df.columns:
                                        mask &= (elc_df["density"].astype(str).str.upper() == density.upper())
                                    if "speed" in elc_df.columns:
                                        mask &= (elc_df["speed"].astype(str).str.upper() == speed.upper())

                                    matched = elc_df[mask]
                                    ann_debug_info.append(f"  Series={series}, mask_sum={mask.sum()}, matched={len(matched)}")
                                    if not matched.empty and yield_type in matched.columns:
                                        y_val = matched[yield_type].iloc[0]
                                        if pd.notna(y_val) and ann_y_val is None:
                                            ann_y_val = y_val
                                            ann_series_label = yield_type
                                            ann_debug_info.append(f"    -> Using point: x={ann_ww_str}, y={y_val:.2f} ({yield_type})")

                            # Store annotation info for adding AFTER layout is set
                            if ann_y_val is not None:
                                pending_annotations.append({
                                    "x": ann_ww_str,
                                    "y": ann_y_val,
                                    "text": ann_text,
                                    "color": ann_color,
                                    "series_label": ann_series_label
                                })

                # Extend x-axis with future weeks if showing future targets
                all_workweeks = sorted_workweeks + future_wws

                fig.update_layout(
                    title="HMFN, SLT & ELC Yield Trend",
                    xaxis_title="Work Week (YYYYWW)",
                    yaxis_title="Yield %",
                    yaxis=dict(range=[y_min, 102]),  # Extended to 102 for label visibility
                    legend_title="Yield Type",
                    hovermode="x unified",
                    xaxis=dict(
                        type="category",
                        categoryorder="array",
                        categoryarray=all_workweeks
                    )
                )

                # Add Micron fiscal month labels below workweek on x-axis
                tick_labels = get_workweek_labels_with_months(all_workweeks)

                fig.update_xaxes(
                    ticktext=tick_labels,
                    tickvals=all_workweeks,
                )

                # Add annotation callouts AFTER layout is set (no scatter trace to avoid axis issues)
                if 'pending_annotations' in dir() and pending_annotations:
                    for pann in pending_annotations:
                        # Only add callout annotation (no marker trace)
                        fig.add_annotation(
                            x=pann["x"],
                            y=pann["y"],
                            xref="x",
                            yref="y",
                            text=f"📝 {pann['text']}",
                            showarrow=True,
                            arrowhead=2,
                            arrowsize=1,
                            arrowwidth=2,
                            arrowcolor=pann["color"],
                            ax=50,
                            ay=-50,
                            bordercolor=pann["color"],
                            borderwidth=2,
                            borderpad=4,
                            bgcolor="white",
                            opacity=0.9,
                            font=dict(size=10, color=pann["color"]),
                        )

                st.plotly_chart(fig, use_container_width=True)

                # Debug info for annotations (temporary)
                if st.session_state.get("elc_annotations"):
                    with st.expander("🔍 Annotation Debug Info", expanded=False):
                        st.write(f"**sorted_workweeks:** {sorted_workweeks[:5]}... (total: {len(sorted_workweeks)})")
                        st.write(f"**selected_series:** {selected_series}")
                        st.write(f"**Annotations stored:** {len(st.session_state.elc_annotations)}")
                        for i, ann in enumerate(st.session_state.elc_annotations):
                            st.write(f"  Ann #{i}: WW={ann['workweek']}, Note={ann['note'][:30]}...")
                        if 'ann_debug_info' in dir():
                            for line in ann_debug_info:
                                st.caption(line)

        st.divider()

        # ========================================================================
        # SECTION 4: ANNOTATIONS & DATA
        # ========================================================================
        st.subheader("4️⃣ Annotations & Data")

        # Initialize annotations
        if "elc_annotations" not in st.session_state:
            st.session_state.elc_annotations = []

        # Show curve info badge if targets are enabled
        selected_curve = st.session_state.get("elc_target_curve", ACTIVE_CURVE)
        if st.session_state.get("elc_show_targets", False) and can_show_targets:
            badge_color = "#00C853" if selected_curve == ACTIVE_CURVE else "#FF9800"
            badge_text = "✓ ACTIVE" if selected_curve == ACTIVE_CURVE else "HISTORICAL"
            curve_desc = CURVE_INFO.get(selected_curve, {}).get('description', '')
            st.caption(f"📊 Target Curve: **{selected_curve}** ({badge_text}) - _{curve_desc}_")

        # ---- ROW 2: Two-column layout (Annotations | Data & Export) ----
        left_col, right_col = st.columns([1, 1.2])

        # LEFT: Annotations
        with left_col:
            st.markdown("**📝 Annotations**")

            # User restriction: only asegaran can add/delete annotations
            current_user = os.environ.get("USER", "")
            is_annotation_admin = current_user == "asegaran"

            if is_annotation_admin:
                ann_input_cols = st.columns([1, 2, 0.5])
                with ann_input_cols[0]:
                    ann_ww = st.selectbox("WW", options=sorted_workweeks if 'sorted_workweeks' in dir() and sorted_workweeks else [], key="elc_ann_ww", label_visibility="collapsed")
                with ann_input_cols[1]:
                    ann_note = st.text_input("Note", placeholder="Add note...", key="elc_ann_note", label_visibility="collapsed")
                with ann_input_cols[2]:
                    if st.button("➕", key="elc_add_ann", help="Add annotation"):
                        if ann_ww and ann_note:
                            st.session_state.elc_annotations.append({
                                "workweek": ann_ww, "note": ann_note,
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
                            })
                            st.rerun()

            if st.session_state.elc_annotations:
                for idx, ann in enumerate(st.session_state.elc_annotations):
                    if is_annotation_admin:
                        ann_row = st.columns([0.15, 0.85])
                        with ann_row[0]:
                            if st.button("🗑️", key=f"del_ann_{idx}"):
                                st.session_state.elc_annotations.pop(idx)
                                st.rerun()
                        with ann_row[1]:
                            st.caption(f"**WW{ann['workweek']}**: {ann['note']}")
                    else:
                        st.caption(f"**WW{ann['workweek']}**: {ann['note']}")
            else:
                st.caption("_No annotations_")

            if not is_annotation_admin:
                st.caption("_View only - contact asegaran to add annotations_")

            # Target History (compact)
            st.markdown("**📜 Target History**")
            if can_show_targets and target_key:
                history = get_curve_history_for_config(target_key)
                if history:
                    changes_data = []
                    for i, h in enumerate(history):
                        curve_name, is_active = h['curve'], h['is_active']
                        detailed_changes = h.get('detailed_changes', [])
                        transition = "Base" if i == 0 else f"{history[i-1]['curve']}→{curve_name}"
                        if detailed_changes:
                            # Group by step and show all changes
                            step_changes = {}
                            for c in detailed_changes:
                                step = c['step']
                                period = c.get('period', '')
                                if step not in step_changes:
                                    step_changes[step] = []
                                step_changes[step].append(f"{c['from']:.1f}→{c['to']:.1f}% ({period})" if period else f"{c['from']:.1f}→{c['to']:.1f}%")
                            # Format: STEP: change1, change2 | STEP2: change
                            remark_parts = []
                            for step, changes in step_changes.items():
                                if len(changes) > 1:
                                    remark_parts.append(f"{step}: {', '.join(changes)}")
                                else:
                                    remark_parts.append(f"{step}: {changes[0]}")
                            remark = " | ".join(remark_parts)
                        else:
                            remark = "Initial" if i == 0 else "No change"
                        if detailed_changes or i == 0:
                            changes_data.append({"Curve": transition, "": "🟢" if is_active else "", "Change": remark})
                    if changes_data:
                        st.dataframe(pd.DataFrame(changes_data[::-1]), use_container_width=True, hide_index=True, height=180)
                        st.caption(f"_Config: {target_key}_")
                else:
                    st.caption("_No history_")
            else:
                st.caption("_Select single DID+Density+Speed_")

        # RIGHT: Data Table & Export
        with right_col:
            st.markdown("**📋 ELC Yield Data**")
            display_cols = ["design_id", "form_factor", "density", "speed", "workweek", "HMFN", "HMB1", "QMON", "SLT", "ELC"]
            available_display = [c for c in display_cols if c in elc_df.columns]
            sorted_df = elc_df[available_display].sort_values(by=["workweek"] if "workweek" in elc_df.columns else available_display[:1], ascending=True)
            st.dataframe(sorted_df, use_container_width=True, hide_index=True, height=280)

            # Export buttons (side by side)
            export_cols = st.columns(3)
            with export_cols[0]:
                csv_data = sorted_df.to_csv(index=False)
                st.download_button("📥 CSV", data=csv_data, file_name=f"elc_data_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv", key="elc_csv_download")
            with export_cols[1]:
                st.download_button("🔗 Link", data=f"?{urlencode({k: v for k, v in {'tab': 'elc', 'did': ','.join(filters.get('design_ids', [])), 'ff': ','.join(filters.get('form_factors', [])), 'density': ','.join(filters.get('densities', []) or []), 'speed': ','.join(filters.get('speeds', []) or []), 'start_ww': filters.get('start_ww', ''), 'end_ww': filters.get('end_ww', '')}.items() if v})}", file_name="share_link.txt", key="elc_link_export")
            with export_cols[2]:
                if st.button("📄 Report", key="elc_pdf_export"):
                    report_html = f"""<!DOCTYPE html><html><head><title>ELC Report {datetime.now().strftime('%Y-%m-%d')}</title>
                    <style>body{{font-family:Arial;margin:40px}}h1{{color:#1f77b4;border-bottom:2px solid #1f77b4}}
                    .filter-box{{background:#f5f5f5;padding:15px;border-radius:8px;margin:20px 0}}
                    table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px}}
                    th{{background:#1f77b4;color:white}}.annotation{{background:#fff3cd;padding:10px;margin:5px 0;border-left:4px solid #ffc107}}</style></head>
                    <body><h1>📊 ELC Yield Report</h1><p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
                    <div class="filter-box"><p><b>DID:</b> {', '.join(filters.get('design_ids', ['All']))}</p>
                    <p><b>Form Factor:</b> {', '.join(filters.get('form_factors', ['All']))}</p>
                    <p><b>Density:</b> {', '.join(filters.get('densities', [])) or 'All'}</p>
                    <p><b>Speed:</b> {', '.join(filters.get('speeds', [])) or 'All'}</p>
                    <p><b>WW Range:</b> {filters.get('start_ww', 'N/A')} - {filters.get('end_ww', 'N/A')}</p></div>
                    <h2>📈 Metrics</h2><p>HMFN: {elc_df['HMFN'].mean():.2f}% | SLT: {elc_df['SLT'].mean() if 'SLT' in elc_df.columns else 0:.2f}% | ELC: {elc_df['ELC'].mean() if 'ELC' in elc_df.columns else 0:.2f}%</p>"""
                    if st.session_state.get("elc_annotations"):
                        report_html += "<h2>📝 Annotations</h2>"
                        for ann in st.session_state.elc_annotations:
                            report_html += f'<div class="annotation"><b>WW{ann["workweek"]}:</b> {ann["note"]}</div>'
                    report_html += f"<h2>📋 Data</h2>{sorted_df.to_html(index=False)}</body></html>"
                    st.download_button("📥 Download", data=report_html, file_name=f"elc_report_{datetime.now().strftime('%Y%m%d')}.html", mime="text/html", key="elc_html_download")

        # Heatmap by density/speed (full width below)
        if "density" in elc_df.columns and "speed" in elc_df.columns and "ELC" in elc_df.columns:
            st.divider()
            st.subheader("ELC Yield by Density & Speed")

            pivot = elc_df.pivot_table(
                index="density",
                columns="speed",
                values="ELC",
                aggfunc="mean"
            )

            if not pivot.empty:
                fig = px.imshow(
                    pivot,
                    text_auto=".1f",
                    color_continuous_scale="RdYlGn",
                    aspect="auto",
                    title="Average ELC Yield % by Density and Speed",
                    labels={"color": "ELC Yield %"}
                )

                # Enhanced hover for ELC heatmap
                fig.update_traces(
                    hovertemplate="<b>Density:</b> %{y}<br>" +
                                  "<b>Speed:</b> %{x}<br>" +
                                  "<b>ELC Yield:</b> %{z:.2f}%<br>" +
                                  "<extra></extra>"
                )

                fig.update_layout(
                    xaxis_title="Speed",
                    yaxis_title="Density"
                )
                st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Click 'Fetch ELC Data' to load HMFN, HMB1, and QMON yield data for ELC calculation.")


def parse_failed_registers(stdout: str, step: str, workweek: str, design_id: str,
                           form_factor: str) -> list[dict]:
    """Parse failed register data from frpt output.

    Args:
        stdout: Raw frpt output
        step: Test step (HMFN, HMB1, QMON)
        workweek: Work week (YYYYWW)
        design_id: Design ID
        form_factor: Module form factor

    Returns:
        List of dicts with register failure data
    """
    results = []
    lines = stdout.split('\n')

    # Find the register failure section (starts after "Register Name" header)
    in_register_section = False
    for line in lines:
        if 'Register Name' in line and 'FFail' in line:
            in_register_section = True
            continue

        if in_register_section:
            # Skip separator lines
            if line.startswith('~~~') or not line.strip():
                continue

            # Parse register line: "register_name#bin    count    %    per_myquick_values..."
            parts = line.split()
            if len(parts) >= 3:
                try:
                    register_name = parts[0]
                    ffail = int(parts[1])
                    pct = float(parts[2])

                    # Extract MYQUICK-specific percentages if available
                    myquick_pcts = {}
                    if len(parts) > 3:
                        # Remaining parts are percentages per MYQUICK column
                        for i, val in enumerate(parts[3:]):
                            try:
                                if val != '-':
                                    myquick_pcts[f'pct_{i}'] = float(val)
                            except ValueError:
                                pass

                    results.append({
                        'register_name': register_name,
                        'ffail': ffail,
                        'fallout_pct': pct,
                        'step': step,
                        'workweek': workweek,
                        'design_id': design_id,
                        'form_factor': form_factor,
                        **myquick_pcts
                    })
                except (ValueError, IndexError):
                    continue

    return results


def fetch_pareto_data(filters: dict[str, Any], use_cache: bool = True) -> pd.DataFrame:
    """Fetch pareto data for HMFN, HMB1, QMON steps.

    Args:
        filters: Filter parameters
        use_cache: Whether to use cached results

    Returns:
        DataFrame with failed register data
    """
    logger.info("=" * 60)
    logger.info(f"FETCH PARETO DATA STARTED (cache={'ON' if use_cache else 'OFF'})")

    runner = FrptRunner(max_workers=8, use_cache=use_cache)

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
    except Exception as e:
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    # Build commands for HMFN, HMB1, QMON
    pareto_steps = ["HMFN", "HMB1", "QMON"]
    commands = []
    for design_id in filters["design_ids"]:
        for step in pareto_steps:
            for form_factor in filters["form_factors"]:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=design_id,
                            facility=filters["facility"],
                        )
                        commands.append(command)
                    except ValueError as e:
                        logger.warning(f"Invalid command: {e}")

    total_calls = len(commands)
    logger.info(f"Total frpt calls for Pareto: {total_calls}")

    if total_calls == 0:
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(completed: int, total: int, cmd: FrptCommand) -> None:
        progress_bar.progress(completed / total)
        status_text.text(f"Completed {completed}/{total}: {cmd.step}/{cmd.form_factor}/WW{cmd.workweek}")

    cmd_results = runner.run_parallel(commands, progress_callback=progress_callback)

    progress_bar.empty()
    status_text.empty()

    # Parse failed registers from each result
    all_registers = []
    for cmd, result in cmd_results:
        if result.success and result.stdout:
            registers = parse_failed_registers(
                result.stdout,
                cmd.step,
                cmd.workweek,
                cmd.dbase,
                cmd.form_factor
            )
            all_registers.extend(registers)

    if not all_registers:
        return pd.DataFrame()

    df = pd.DataFrame(all_registers)
    logger.info(f"Pareto data: {len(df)} register failures parsed")
    return df


def render_failcrawler_subtab(filters: dict[str, Any]) -> None:
    """Render the FAILCRAWLER DPM sub-tab content."""
    st.markdown("""
    **FAILCRAWLER cDPM Analysis** by test step (HMFN, HMB1, QMON, SLT).
    Shows failure signature patterns breakdown by workweek with volume overlay.
    """)

    use_cache = st.session_state.get("use_cache", True)

    # Calculate workweeks from filters
    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
    except Exception:
        workweeks = []

    # Get steps from main dashboard filter
    steps_to_show = filters.get("test_steps", ["HMFN", "HMB1", "QMON", "SLT"])

    # Display current filter info
    st.caption(f"**Filters:** {', '.join(filters['design_ids'])} | {', '.join(steps_to_show)} | WW{filters['start_ww']}-{filters['end_ww']} ({len(workweeks)} weeks)")

    # Fetch controls
    col1, col2 = st.columns([1, 4])
    with col1:
        fetch_fc = st.button(
            "🔄 Fetch Live Data",
            type="primary",
            use_container_width=True,
            key="fetch_failcrawler_btn"
        )
    with col2:
        show_labels = st.checkbox("Data Labels", value=False, key="fc_show_labels")

    if fetch_fc:
        try:
            with st.spinner(f"Fetching FAILCRAWLER data for {len(filters['design_ids'])} DIDs × {len(steps_to_show)} steps × {len(workweeks)} weeks..."):
                # Fetch main FAILCRAWLER cDPM data (without MSN_STATUS grouping)
                fc_df = fetch_failcrawler_data(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks,
                    cache_dir="cache" if use_cache else None
                )

                if fc_df.empty:
                    st.warning("No FAILCRAWLER data returned. Check filters or try again.")
                    return

                # Fetch MSN_STATUS correlation data separately (with MSN_STATUS grouping)
                msn_corr_df = fetch_msn_status_correlation_data(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks
                )

                # Fetch FID-level data for unique module counts per MSN_STATUS
                fid_counts_df = fetch_msn_status_fid_counts(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks
                )

                # Fetch total UIN (all modules tested including Pass) for accurate cDPM calculation
                total_uin_df = fetch_total_uin_by_step(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks
                )

                # Fetch new DPM metrics (cDPM, MDPM) using step-specific commands
                cdpm_df = fetch_cdpm_data(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks
                )
                mdpm_df = fetch_mdpm_data(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks
                )

                # Fetch FCFM decode quality data
                fcfm_df = fetch_fcfm_decode_quality(
                    design_ids=filters['design_ids'],
                    steps=steps_to_show,
                    workweeks=workweeks
                )

                st.session_state.failcrawler_data = fc_df
                st.session_state.failcrawler_msn_corr_data = msn_corr_df
                st.session_state.failcrawler_fid_counts = fid_counts_df
                st.session_state.failcrawler_total_uin = total_uin_df
                st.session_state.failcrawler_cdpm_data = cdpm_df
                st.session_state.failcrawler_mdpm_data = mdpm_df
                st.session_state.failcrawler_fcfm_data = fcfm_df
                st.session_state.failcrawler_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.failcrawler_filters = filters.copy()
                st.success(f"Loaded {len(fc_df):,} FAILCRAWLER + {len(cdpm_df):,} cDPM + {len(mdpm_df):,} MDPM records!")
                st.rerun()

        except Exception as e:
            st.error(f"Failed to fetch FAILCRAWLER data: {e}")
            logger.exception("FAILCRAWLER fetch error")
            return

    # Display FAILCRAWLER data if available
    if not st.session_state.failcrawler_data.empty:
        fc_df = st.session_state.failcrawler_data.copy()

        if st.session_state.failcrawler_last_fetch_time:
            st.caption(f"📅 Last fetched: {st.session_state.failcrawler_last_fetch_time}")

        st.divider()

        # DID filter for viewing per-DID or cumulative
        # Filter out NaN values before sorting
        available_dids = [d for d in fc_df['DESIGN_ID'].unique().tolist()
                          if d is not None and isinstance(d, str)] if 'DESIGN_ID' in fc_df.columns else []
        did_options = ["All (Cumulative)"] + sorted(available_dids)

        selected_did_view = st.selectbox(
            "View by Design ID",
            options=did_options,
            index=0,
            key="fc_did_filter",
            help="View cumulative data or filter to a specific Design ID"
        )

        # Determine the design_id to pass to processing
        if selected_did_view == "All (Cumulative)":
            filter_design_id = None
            design_id_label = "All DIDs"
        else:
            filter_design_id = selected_did_view
            design_id_label = selected_did_view

        # Display charts for each selected step
        import streamlit.components.v1 as components

        # Get cDPM and MDPM data from session state
        cdpm_df = st.session_state.get('failcrawler_cdpm_data', pd.DataFrame())
        mdpm_df = st.session_state.get('failcrawler_mdpm_data', pd.DataFrame())

        # Get the latest workweek from the data for metrics display
        latest_ww = None
        if 'MFG_WORKWEEK' in fc_df.columns:
            latest_ww = int(fc_df['MFG_WORKWEEK'].max())

        # Detect and display excursions (WoW alerts)
        excursions = detect_excursions(fc_df, cdpm_df, mdpm_df, steps_to_show)
        if excursions:
            alert_html = create_alert_summary_html(excursions, dark_mode=False)
            if alert_html:
                components.html(alert_html, height=50, scrolling=False)

        st.divider()

        # Get FCFM decode quality data from session state
        fcfm_df = st.session_state.get('failcrawler_fcfm_data', pd.DataFrame())

        for step in steps_to_show:
            data = process_failcrawler_data(fc_df, step, design_id=filter_design_id)
            if data is None:
                continue

            st.subheader(f"📊 {step} FAILCRAWLER cDPM")

            # Show decode quality indicator (UE% vs UNKNOWN%)
            if not fcfm_df.empty:
                decode_data = calculate_decode_quality(fcfm_df, step, workweek=latest_ww)
                if decode_data:
                    decode_html = create_decode_quality_html(decode_data, dark_mode=False)
                    if decode_html:
                        components.html(decode_html, height=40, scrolling=False)

            # Show DPM metrics summary cards for this step (with WoW trends and sparklines)
            if not cdpm_df.empty or not mdpm_df.empty:
                summary_html = create_dpm_metrics_summary_html(
                    cdpm_df, mdpm_df, fc_df, step, workweek=latest_ww, dark_mode=False,
                    fcfm_df=fcfm_df, show_trends=True
                )
                if summary_html:
                    components.html(summary_html, height=180, scrolling=False)

            # Show Top Movers (FAILCRAWLERs with >25% WoW increase)
            fc_changes = calculate_failcrawler_wow_changes(fc_df, step)
            if fc_changes:
                top_movers_html = create_top_movers_html(fc_changes, step, threshold=25.0, dark_mode=False)
                if top_movers_html:
                    components.html(top_movers_html, height=80, scrolling=False)

            # Create chart (uses light mode colors for compatibility with dashboard theme)
            fig = create_failcrawler_chart(
                data,
                design_id=design_id_label,
                dark_mode=False,
                show_data_labels=show_labels
            )

            if fig:
                # Plotly config
                plotly_config = {
                    'displayModeBar': True,
                    'responsive': True,
                    'displaylogo': False,
                    'modeBarButtonsToAdd': ['toggleSpikelines'],
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': f'failcrawler_{step}_{design_id_label}',
                        'height': 800,
                        'width': 1400,
                        'scale': 2
                    }
                }

                # Render chart
                st.plotly_chart(fig, use_container_width=True, config=plotly_config)

            # Pareto Summary table (80/20 analysis)
            with st.expander(f"📋 {step} Pareto Summary (80/20 Analysis)", expanded=False):
                pareto_html = create_pareto_summary_html(data, dark_mode=False)
                if pareto_html:
                    components.html(pareto_html, height=400, scrolling=True)

            # MSN_STATUS Correlation (FAILCRAWLER × MSN_STATUS contribution analysis)
            st.subheader(f"🔗 {step} MSN_STATUS Correlation")

            # Use separate MSN_STATUS correlation data from session state
            msn_corr_df = st.session_state.get('failcrawler_msn_corr_data', pd.DataFrame())
            fid_counts_df = st.session_state.get('failcrawler_fid_counts', pd.DataFrame())
            total_uin_df = st.session_state.get('failcrawler_total_uin', pd.DataFrame())
            if msn_corr_df.empty:
                st.info("MSN_STATUS correlation data not loaded. Click 'Fetch Live Data' to load.")
            else:
                # Toggle between Latest WW and Cumulative
                corr_toggle_col1, corr_toggle_col2 = st.columns([1, 3])
                with corr_toggle_col1:
                    corr_time_range = st.radio(
                        "Time Range",
                        options=["Latest WW", "Cumulative"],
                        horizontal=True,
                        key=f"msn_corr_time_range_{step}",
                        label_visibility="collapsed"
                    )

                # Filter data based on toggle selection
                filtered_msn_corr_df = msn_corr_df.copy()
                step_total_muin = None  # Total MUIN for MDPM denominator
                step_total_uin = None   # Total component UIN for cDPM denominator

                if corr_time_range == "Latest WW" and 'MFG_WORKWEEK' in msn_corr_df.columns:
                    # Get the latest workweek from the data
                    latest_ww = int(msn_corr_df['MFG_WORKWEEK'].max())
                    filtered_msn_corr_df = msn_corr_df[msn_corr_df['MFG_WORKWEEK'] == latest_ww].copy()
                    # Filter raw data by workweek and count unique MSNs and FIDs
                    if not fid_counts_df.empty and 'MFG_WORKWEEK' in fid_counts_df.columns:
                        filtered_raw = fid_counts_df[fid_counts_df['MFG_WORKWEEK'] == latest_ww].copy()
                        # Normalize STEP to uppercase for matching
                        filtered_raw['STEP'] = filtered_raw['STEP'].str.upper()
                        # Count unique MSNs (modules) and FIDs (components)
                        filtered_fid_counts_df = filtered_raw.groupby(
                            ['STEP', 'MSN_STATUS'], as_index=False
                        ).agg(UNIQUE_MODULES=('MSN', 'nunique'), UNIQUE_FIDS=('FID', 'nunique'))
                    else:
                        filtered_fid_counts_df = pd.DataFrame()
                    # Get total MUIN and UIN for this step and workweek
                    if not total_uin_df.empty and 'STEP' in total_uin_df.columns:
                        step_uin_mask = (total_uin_df['STEP'].str.upper() == step.upper()) & (total_uin_df['MFG_WORKWEEK'] == latest_ww)
                        step_uin_row = total_uin_df[step_uin_mask]
                        if not step_uin_row.empty:
                            if 'TOTAL_MUIN' in step_uin_row.columns:
                                step_total_muin = int(step_uin_row['TOTAL_MUIN'].sum())
                            if 'TOTAL_UIN' in step_uin_row.columns:
                                step_total_uin = int(step_uin_row['TOTAL_UIN'].sum())
                    with corr_toggle_col2:
                        uin_info = f"MUIN: {step_total_muin:,}" if step_total_muin else ""
                        if step_total_uin:
                            uin_info += f" | cUIN: {step_total_uin:,}" if uin_info else f"cUIN: {step_total_uin:,}"
                        st.caption(f"📅 WW{latest_ww} ({uin_info})" if uin_info else f"📅 WW{latest_ww} only")
                else:
                    # Count unique MSNs and FIDs across ALL workweeks (true cumulative)
                    if not fid_counts_df.empty and 'MSN' in fid_counts_df.columns:
                        fid_counts_copy = fid_counts_df.copy()
                        # Normalize STEP to uppercase for matching
                        fid_counts_copy['STEP'] = fid_counts_copy['STEP'].str.upper()
                        # Count unique MSNs (modules) and FIDs (components)
                        filtered_fid_counts_df = fid_counts_copy.groupby(
                            ['STEP', 'MSN_STATUS'], as_index=False
                        ).agg(UNIQUE_MODULES=('MSN', 'nunique'), UNIQUE_FIDS=('FID', 'nunique'))
                    else:
                        filtered_fid_counts_df = pd.DataFrame()
                    # Get total MUIN and UIN for this step across all workweeks
                    if not total_uin_df.empty and 'STEP' in total_uin_df.columns:
                        step_uin_mask = total_uin_df['STEP'].str.upper() == step.upper()
                        step_uin_rows = total_uin_df[step_uin_mask]
                        if not step_uin_rows.empty:
                            if 'TOTAL_MUIN' in step_uin_rows.columns:
                                step_total_muin = int(step_uin_rows['TOTAL_MUIN'].sum())
                            if 'TOTAL_UIN' in step_uin_rows.columns:
                                step_total_uin = int(step_uin_rows['TOTAL_UIN'].sum())
                    with corr_toggle_col2:
                        uin_info = f"MUIN: {step_total_muin:,}" if step_total_muin else ""
                        if step_total_uin:
                            uin_info += f" | cUIN: {step_total_uin:,}" if uin_info else f"cUIN: {step_total_uin:,}"
                        st.caption(f"📅 Cumulative ({uin_info})" if uin_info else "📅 Cumulative (all weeks)")

                correlation_data = process_msn_status_correlation(
                    filtered_msn_corr_df, step, design_id=filter_design_id, fid_counts=filtered_fid_counts_df,
                    total_muin=step_total_muin, total_uin=step_total_uin
                )
                if correlation_data:
                    # Display heatmap and ranked table side by side
                    corr_col1, corr_col2 = st.columns([1, 1])

                    with corr_col1:
                        # Heatmap: FAILCRAWLER × MSN_STATUS
                        corr_fig = create_msn_status_correlation_chart(correlation_data, dark_mode=False)
                        if corr_fig:
                            st.plotly_chart(corr_fig, use_container_width=True)

                    with corr_col2:
                        # Ranked table: MSN_STATUS by CDPM contribution
                        ranked_html = create_msn_status_ranked_table_html(correlation_data, dark_mode=False)
                        if ranked_html:
                            components.html(ranked_html, height=450, scrolling=True)

                    # Drill-down section: Select FAILCRAWLER × MSN_STATUS to see affected MSNs
                    st.markdown("##### 🔍 Drill-down: View Affected MSNs")
                    st.caption("Select a FAILCRAWLER and MSN_STATUS from the heatmap to see affected modules")

                    # Get available options from the correlation data
                    fc_list = get_failcrawler_list_for_step(filtered_msn_corr_df, step)
                    msn_status_list = get_msn_status_list_for_step(filtered_msn_corr_df, step)

                    if fc_list and msn_status_list:
                        drill_col1, drill_col2, drill_col3 = st.columns([2, 2, 1])

                        with drill_col1:
                            selected_fc = st.selectbox(
                                "FAILCRAWLER",
                                options=fc_list,
                                key=f"drill_fc_{step}",
                                help="Select FAILCRAWLER category"
                            )

                        with drill_col2:
                            selected_msn_status = st.selectbox(
                                "MSN_STATUS",
                                options=["All"] + msn_status_list,
                                key=f"drill_status_{step}",
                                help="Select MSN_STATUS (or All for any status)"
                            )

                        with drill_col3:
                            drill_btn = st.button(
                                "🔎 Show MSNs",
                                key=f"drill_btn_{step}",
                                use_container_width=True
                            )

                        # Store drilldown data per step
                        drilldown_key = f"heatmap_drilldown_{step}"

                        if drill_btn and selected_fc:
                            with st.spinner(f"Fetching MSNs for {selected_fc}..."):
                                # Use current workweeks from filter
                                drill_wws = workweeks if workweeks else []
                                drill_dids = filters.get('design_ids', [])

                                # Pass MSN_STATUS filter if not "All"
                                msn_status_filter = selected_msn_status if selected_msn_status != "All" else None

                                # Debug logging
                                logger.info(f"Drill-down: FC={selected_fc}, Status={msn_status_filter}, DIDs={drill_dids}, WWs={drill_wws}, Step={step}")

                                drilldown_df = fetch_failcrawler_msn_drilldown(
                                    design_ids=drill_dids,
                                    steps=[step],
                                    workweeks=drill_wws,
                                    failcrawler=selected_fc,
                                    msn_status=msn_status_filter
                                )

                                logger.info(f"Drill-down result: {len(drilldown_df)} rows, cols={drilldown_df.columns.tolist() if not drilldown_df.empty else 'empty'}")

                                # Store in session state
                                st.session_state[drilldown_key] = {
                                    'df': drilldown_df,
                                    'failcrawler': selected_fc,
                                    'msn_status': msn_status_filter,
                                    'step': step
                                }

                        # Display drilldown if available
                        if drilldown_key in st.session_state and st.session_state[drilldown_key]:
                            dd_info = st.session_state[drilldown_key]
                            if not dd_info['df'].empty:
                                drilldown_html = create_failcrawler_drilldown_html(
                                    dd_info['df'],
                                    dd_info['failcrawler'],
                                    dd_info['step'],
                                    msn_status=dd_info['msn_status'],
                                    dark_mode=False
                                )
                                components.html(drilldown_html, height=400, scrolling=True)
                            else:
                                filter_desc = dd_info['failcrawler']
                                if dd_info['msn_status']:
                                    filter_desc += f" × {dd_info['msn_status']}"
                                st.info(f"No MSN data found for {filter_desc} at {step}")
                    else:
                        st.info("No drill-down data available")

                else:
                    st.info(f"No MSN_STATUS correlation data for {step}. All failures may be 'Pass' status.")

            st.divider()

    else:
        st.info("👆 Click 'Fetch Live Data' to load FAILCRAWLER cDPM data using current dashboard filters.")


def render_register_fallout_subtab(filters: dict[str, Any]) -> None:
    """Render the Register Fallout sub-tab content (original Pareto content)."""
    st.markdown("""
    **Top 5 Failed Registers** by test step (HMFN, HMB1, QMON).
    Shows register fallout breakdown by design_id, form_factor, speed, density, and workweek.
    """)

    use_cache = st.session_state.get("use_cache", True)

    # Calculate estimated queries
    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        total_queries = len(filters["design_ids"]) * 3 * len(filters["form_factors"]) * len(workweeks)
    except Exception:
        total_queries = 0

    # Fetch button
    col1, col2 = st.columns([1, 4])
    with col1:
        fetch_pareto = st.button(
            "Fetch Pareto Data",
            type="primary",
            use_container_width=True,
            key="fetch_pareto_btn"
        )
    with col2:
        if total_queries > 0:
            st.caption(f"Will fetch {total_queries} queries (3 steps × {len(workweeks)} weeks).")

    if fetch_pareto:
        try:
            st.warning(f"Fetching failed register data for HMFN, HMB1, QMON ({total_queries} queries)...")
            pareto_df = fetch_pareto_data(filters, use_cache=use_cache)

            if pareto_df.empty:
                st.warning("No failed register data returned.")
                return

            st.session_state.pareto_data = pareto_df
            st.session_state.pareto_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success(f"Loaded {len(pareto_df)} register failure records!")

        except Exception as e:
            st.error(f"Failed to fetch pareto data: {e}")
            logger.exception("Pareto fetch error")
            return

    # Display pareto data if available
    if not st.session_state.pareto_data.empty:
        pareto_df = st.session_state.pareto_data.copy()

        if st.session_state.pareto_last_fetch_time:
            st.caption(f"Last fetched: {st.session_state.pareto_last_fetch_time}")

        # Ensure workweek is in YYYYWW format
        if "workweek" in pareto_df.columns:
            pareto_df["workweek"] = pareto_df["workweek"].astype(str).str.replace("WW", "")

        st.divider()

        # Option to pin data labels on chart
        show_pareto_labels = st.checkbox("Show data labels on chart", value=False, key="pareto_show_labels")

        # Display by step
        for step in ["HMFN", "HMB1", "QMON"]:
            step_data = pareto_df[pareto_df["step"] == step]
            if step_data.empty:
                continue

            st.subheader(f"{step} - Top 5 Failed Registers by Fallout %")

            # Get unique combinations of design_id, form_factor, density
            group_cols = [c for c in ["design_id", "form_factor"] if c in step_data.columns]

            if not group_cols:
                continue

            # Get unique groups
            unique_groups = step_data[group_cols].drop_duplicates()

            for _, group_row in unique_groups.iterrows():
                # Filter data for this group
                mask = True
                group_label_parts = []
                for col in group_cols:
                    mask = mask & (step_data[col] == group_row[col])
                    group_label_parts.append(f"{group_row[col]}")

                group_data = step_data[mask]
                group_label = " / ".join(group_label_parts)

                if group_data.empty:
                    continue

                st.markdown(f"**{group_label}**")

                # Get top 5 registers by average fallout % across all weeks
                top_registers = (
                    group_data.groupby("register_name")["fallout_pct"]
                    .mean()
                    .nlargest(5)
                    .index.tolist()
                )

                if not top_registers:
                    continue

                # Filter to only top 5 registers
                top_data = group_data[group_data["register_name"].isin(top_registers)]

                # Pivot: registers as rows, workweeks as columns
                pivot_df = top_data.pivot_table(
                    index="register_name",
                    columns="workweek",
                    values="fallout_pct",
                    aggfunc="first"
                )

                # Sort columns (workweeks) numerically
                sorted_cols = sorted(pivot_df.columns, key=lambda x: int(str(x)))
                pivot_df = pivot_df[sorted_cols]

                # Sort rows by average fallout % descending
                pivot_df["avg"] = pivot_df.mean(axis=1)
                pivot_df = pivot_df.sort_values("avg", ascending=False)
                pivot_df = pivot_df.drop(columns=["avg"])

                # Reset index to make register_name a column
                pivot_df = pivot_df.reset_index()
                pivot_df = pivot_df.rename(columns={"register_name": "Register Name"})

                # Display table
                st.dataframe(
                    pivot_df,
                    use_container_width=True,
                    hide_index=True
                )

                # Pareto chart for this group - fallout %
                st.markdown(f"**{step} Pareto Chart - {group_label} (Avg Fallout %)**")

                chart_data = (
                    group_data.groupby("register_name").agg({
                        "fallout_pct": "mean",
                        "ffail": "sum"
                    }).reset_index()
                )
                chart_data = chart_data.nlargest(10, "fallout_pct")
                chart_data = chart_data.sort_values("fallout_pct", ascending=True)

                # Add text column for labels
                chart_data["label_text"] = chart_data["fallout_pct"].apply(lambda x: f"{x:.2f}%")

                fig = px.bar(
                    chart_data,
                    y="register_name",
                    x="fallout_pct",
                    orientation="h",
                    title=f"{step} - Top 10 Failed Registers (Avg Fallout %)",
                    labels={"register_name": "Register", "fallout_pct": "Avg Fallout %"},
                    custom_data=["ffail"],
                    text="label_text" if show_pareto_labels else None,
                )

                # Enhanced hover template for Pareto chart
                fig.update_traces(
                    hovertemplate="<b>Register:</b> %{y}<br>" +
                                  "<b>Avg Fallout:</b> %{x:.2f}%<br>" +
                                  "<b>Total Fail Count:</b> %{customdata[0]:,}<br>" +
                                  f"<b>Step:</b> {step}<br>" +
                                  f"<b>Group:</b> {group_label}<br>" +
                                  "<extra></extra>",
                    textposition="outside" if show_pareto_labels else None,
                    textfont=dict(size=9) if show_pareto_labels else None,
                )

                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")

            st.divider()

    else:
        st.info("Click 'Fetch Pareto Data' to load failed register data for HMFN, HMB1, and QMON.")


def render_pareto_tab(filters: dict[str, Any]) -> None:
    """Render the Pareto Analysis tab with sub-tabs for FAILCRAWLER and Register Fallout."""
    st.header("Pareto Analysis")
    st.info("📉 **FAILCRAWLER DPM:** cDPM by fail mode with MSN_STATUS | **Register Fallout:** Top failed registers — Required: WW range, Form Factor, DID, Facility, Step")

    # Create sub-tabs
    fc_tab, reg_tab = st.tabs(["FAILCRAWLER DPM", "Register Fallout"])

    with fc_tab:
        render_failcrawler_subtab(filters)

    with reg_tab:
        render_register_fallout_subtab(filters)


def render_grace_motherboard_section(filters: dict[str, Any]) -> None:
    """Render GRACE Motherboard Health Monitoring section with FM-based analysis."""
    st.markdown("### GRACE Motherboard Health Monitoring")
    st.markdown("""
    Monitor NVGRACE motherboard performance using Fail Mode (FM) cDPM analysis.
    Identifies machines with Hang failures and drills down to find 100% fail cases (UIN=4, UPASS=0).
    """)

    # Use filters from main dashboard - Required
    start_ww = str(filters.get("start_ww", "202606"))
    end_ww = str(filters.get("end_ww", "202615"))
    form_factors = [ff.lower() for ff in filters.get("form_factors", ["SOCAMM", "SOCAMM2"])]

    # Optional filters from main dashboard (ensure they are lists)
    design_ids = filters.get("design_ids", []) or []
    densities = filters.get("densities", []) or []
    speeds = filters.get("speeds", []) or []
    facility = filters.get("facility", "") or ""

    # Ensure lists are actually lists (not strings or None)
    if not isinstance(design_ids, list):
        design_ids = [design_ids] if design_ids else []
    if not isinstance(densities, list):
        densities = [densities] if densities else []
    if not isinstance(speeds, list):
        speeds = [speeds] if speeds else []

    # Calculate previous week for comparison
    selected_ww = end_ww
    prev_ww = get_previous_workweek(selected_ww)

    # Build filter context display
    filter_parts = [
        f"{', '.join([ff.upper() for ff in form_factors])}",
        "HMB1, QMON",
        f"WW{start_ww}-{end_ww} (10 weeks)"
    ]
    # Add optional filters if specified
    if design_ids:
        filter_parts.append(f"DIDs: {', '.join(design_ids)}")
    if densities:
        filter_parts.append(f"Density: {', '.join(densities)}")
    if speeds:
        filter_parts.append(f"Speed: {', '.join(speeds)}")
    if facility:
        filter_parts.append(f"Facility: {facility}")

    st.caption(f"**Filters:** {' | '.join(filter_parts)}")

    # Session state initialization
    if 'grace_fm_df' not in st.session_state:
        st.session_state.grace_fm_df = None
    if 'grace_available_weeks' not in st.session_state:
        st.session_state.grace_available_weeks = []
    if 'grace_hang_analysis' not in st.session_state:
        st.session_state.grace_hang_analysis = {}

    # Fetch FM data button
    fetch_btn = st.button("🔄 Fetch GRACE Data", key="fetch_grace_fm_data", type="primary")

    if fetch_btn:
        with st.spinner(f"Fetching GRACE motherboard data (WW{start_ww}-{end_ww})..."):
            # Don't pass facility if it's "all" or empty
            facility_filter = facility if facility and facility.lower() != "all" else None
            fm_df = fetch_grace_fm_data(
                start_ww=start_ww,
                end_ww=end_ww,
                form_factors=form_factors,
                design_ids=design_ids if design_ids else None,
                densities=densities if densities else None,
                speeds=speeds if speeds else None,
                facility=facility_filter
            )

            if fm_df is not None and not fm_df.empty:
                st.session_state.grace_fm_df = fm_df
                # Get available work weeks from the data
                if 'MFG_WORKWEEK' in fm_df.columns:
                    weeks = sorted(fm_df['MFG_WORKWEEK'].unique(), reverse=True)
                    st.session_state.grace_available_weeks = [str(int(w)) for w in weeks]
                st.success(f"Loaded {len(fm_df):,} NVGRACE records across {fm_df['MFG_WORKWEEK'].nunique()} work weeks")
            else:
                st.error("No GRACE motherboard data found for the specified filters.")
                st.session_state.grace_fm_df = None
                st.session_state.grace_available_weeks = []

    # Display analysis if data available
    if st.session_state.grace_fm_df is not None and not st.session_state.grace_fm_df.empty:
        fm_df = st.session_state.grace_fm_df
        available_weeks = st.session_state.grace_available_weeks

        st.markdown("---")

        # ============================================
        # Hang cDPM by Machine (Selected Week)
        # ============================================
        st.markdown(f"#### 📈 Hang cDPM by Machine (WW{selected_ww})")

        # Filter data for selected week only - ALL machines
        week_data = fm_df[fm_df['MFG_WORKWEEK'] == int(selected_ww)].copy()

        if not week_data.empty:
            # Sort by Hang cDPM descending (problem machines first)
            week_data = week_data.sort_values('Hang', ascending=False)

            # Get previous week data for comparison
            prev_week_data = fm_df[fm_df['MFG_WORKWEEK'] == int(prev_ww)].copy() if prev_ww else pd.DataFrame()

            # Summary metrics
            total_machines = len(week_data)
            machines_with_hang = len(week_data[week_data['Hang'] > 0])
            prev_machines_with_hang = len(prev_week_data[prev_week_data['Hang'] > 0]) if not prev_week_data.empty else 0

            # Calculate WoW change
            hang_delta = machines_with_hang - prev_machines_with_hang
            if hang_delta < 0:
                wow_status = "✅ Improved"
                delta_color = "normal"
            elif hang_delta > 0:
                wow_status = "⚠️ Declined"
                delta_color = "inverse"
            else:
                wow_status = "➖ No Change"
                delta_color = "off"

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Machines", f"{total_machines:,}")
            with col2:
                st.metric("Machines with Hang > 0", f"{machines_with_hang:,}")
            with col3:
                st.metric(
                    f"vs WW{prev_ww}",
                    wow_status,
                    delta=f"{hang_delta:+d} machines" if hang_delta != 0 else None,
                    delta_color=delta_color,
                    help=f"WW{selected_ww}: {machines_with_hang} machines | WW{prev_ww}: {prev_machines_with_hang} machines"
                )

            # Create chart with ALL machines
            fig_trend = go.Figure()

            # Bar chart for UIN (Volume) - secondary y-axis
            fig_trend.add_trace(go.Bar(
                x=week_data['MACHINE_ID'],
                y=week_data['UIN'],
                name='Volume (UIN)',
                marker_color='rgba(99, 110, 250, 0.5)',
                yaxis='y2'
            ))

            # Line chart for Hang cDPM - primary y-axis
            # Color markers based on Hang value: Red for Hang > 0, Green for Hang = 0
            marker_colors = ['#FF1744' if h > 0 else '#00C853' for h in week_data['Hang']]

            fig_trend.add_trace(go.Scatter(
                x=week_data['MACHINE_ID'],
                y=week_data['Hang'],
                name='Hang cDPM',
                mode='lines+markers',
                line=dict(color='#888888', width=1),  # Gray line connecting points
                marker=dict(
                    size=10,
                    color=marker_colors,
                    line=dict(width=1, color='white')  # White border for visibility
                )
            ))

            fig_trend.update_layout(
                height=450,
                margin=dict(l=50, r=50, t=30, b=120),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
                xaxis=dict(
                    title="Machine ID",
                    tickangle=-45,
                    tickfont=dict(size=11, family="Arial Black, sans-serif"),  # Bigger and bolder
                    rangeslider=dict(visible=True)  # Add range slider for navigation
                ),
                yaxis=dict(
                    title=dict(text="Hang cDPM", font=dict(color='#FF6B6B')),
                    tickfont=dict(color='#FF6B6B'),
                    side='left'
                ),
                yaxis2=dict(
                    title=dict(text="Volume (UIN)", font=dict(color='rgba(99, 110, 250, 0.8)')),
                    tickfont=dict(color='rgba(99, 110, 250, 0.8)'),
                    overlaying='y',
                    side='right'
                ),
                hovermode="x unified",
                barmode='group'
            )

            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.warning(f"No data found for WW{selected_ww}")

        # Check if selected week has data
        if selected_ww in available_weeks and prev_ww in available_weeks:
            # ============================================
            # Week-over-Week Hang Comparison (Consolidated)
            # ============================================
            st.markdown(f"#### 🔥 Hang Failures: Week-over-Week (WW{selected_ww} vs WW{prev_ww})")

            # Description and methodology
            with st.expander("ℹ️ How this data is collected and categorized", expanded=False):
                st.markdown("""
**Data Source:** `mtsums` with Fail Mode (+fm) analysis

**Command:**
```
mtsums -modff=socamm,socamm2 -ww={start_ww},{end_ww} -step=hmb1,qmon +fm -format+=machine_id,mfg_workweek =islatest =isvalid +stdf +quiet +csv
```

**Categorization Logic:**
| Status | Condition |
|--------|-----------|
| 🆕 **New Issue** | Hang cDPM > 0 in current week, but was 0 in previous week |
| ✅ **Fixed** | Hang cDPM > 0 in previous week, but is 0 in current week |
| 🔄 **Recurring** | Hang cDPM > 0 in both weeks (ongoing issue) |

**Hang cDPM** = Defects Per Million units attributed to "Hang" fail mode on NVGRACE motherboards.
                """)

            comparison_df = compare_weeks(fm_df, selected_ww, prev_ww)

            if not comparison_df.empty:
                # Summary metrics
                new_issues = len(comparison_df[comparison_df['is_new']])
                resolved = len(comparison_df[comparison_df['is_resolved']])
                chronic = len(comparison_df[comparison_df['is_chronic']])
                current_week_issues = new_issues + chronic  # Machines with Hang > 0 in current week

                comp_cols = st.columns(4)
                with comp_cols[0]:
                    st.metric("🆕 New Issues", f"{new_issues}", help="Newly failing: Hang > 0 this week, but was 0 last week")
                with comp_cols[1]:
                    st.metric("✅ Fixed", f"{resolved}", delta=f"+{resolved}" if resolved > 0 else None, delta_color="normal", help="Problem resolved: Hang > 0 last week, but is 0 this week")
                with comp_cols[2]:
                    st.metric("🔄 Recurring", f"{chronic}", delta_color="inverse", help="Ongoing chronic issue: Hang > 0 in both weeks")
                with comp_cols[3]:
                    st.metric("Total Tracked", f"{len(comparison_df)}", help="All machines that had Hang > 0 in either week")

                # Prepare display dataframe
                comp_display = comparison_df.copy()

                # Add status column with clearer labels
                def get_hang_status(row):
                    if row['is_new']:
                        return "🆕 New Issue"
                    elif row['is_resolved']:
                        return "✅ Fixed"
                    elif row['is_chronic']:
                        return "🔄 Recurring"
                    return "—"

                comp_display['Hang Status'] = comp_display.apply(get_hang_status, axis=1)

                # Get list of problematic machines (New Issue + Recurring), sorted by Hang cDPM, limit to top 10
                problematic_machines_df = comparison_df[
                    (comparison_df['is_new']) | (comparison_df['is_chronic'])
                ].sort_values(f'hang_cDPM_{selected_ww}', ascending=False).head(10)
                current_issue_machines = problematic_machines_df['machine_id'].tolist()

                # Calculate next week
                next_ww = f"{int(selected_ww[:4])}{int(selected_ww[4:])+1:02d}" if int(selected_ww[4:]) < 52 else f"{int(selected_ww[:4])+1}01"

                # Check if drill-down results are available
                has_drill_results = 'grace_drill_results' in st.session_state and st.session_state.grace_drill_results is not None

                # Prepare problematic_display for left column (always visible)
                problematic_display = comp_display[
                    (comp_display['Hang Status'].str.contains('New Issue|Recurring', regex=True, na=False))
                ].sort_values(f'hang_cDPM_{selected_ww}', ascending=False).head(10)

                if current_issue_machines:
                    st.markdown(f"**Top {len(problematic_display)} Problematic Motherboards** (sorted by Hang cDPM)")

                    # Two-column layout: Left = WoW Hang | Right = 100% Fail Analysis
                    left_col, right_col = st.columns(2)

                    # ========== LEFT COLUMN: WoW Hang Comparison (Always Visible) ==========
                    with left_col:
                        st.markdown("### 📊 WoW Hang Comparison")
                        st.caption("First-level findings: Machines with Hang cDPM > 0")

                        wow_cols = ['machine_id', 'Hang Status', f'hang_cDPM_{selected_ww}', f'hang_cDPM_{prev_ww}', 'hang_delta']
                        wow_display = problematic_display[wow_cols].copy()
                        wow_display.columns = ['Machine ID', 'Status', f'WW{selected_ww}', f'WW{prev_ww}', 'Delta']

                        def highlight_wow_status(row):
                            if '🔄 Recurring' in str(row['Status']):
                                return ['background-color: #FFCDD2; font-weight: bold'] * len(row)  # Light red
                            elif '🆕 New Issue' in str(row['Status']):
                                return ['background-color: #FFF9C4'] * len(row)  # Light yellow
                            return [''] * len(row)

                        styled_wow = wow_display.style.apply(highlight_wow_status, axis=1).format({
                            f'WW{selected_ww}': '{:.2f}',
                            f'WW{prev_ww}': '{:.2f}',
                            'Delta': '{:+.2f}'
                        })
                        st.dataframe(styled_wow, use_container_width=True, hide_index=True, height=450)

                    # ========== RIGHT COLUMN: 100% Fail Analysis ==========
                    with right_col:
                        st.markdown("### 🔬 100% Fail Analysis")
                        st.caption("Deep-dive: UIN=4, UPASS=0 (all modules failed)")

                        # Methodology expander
                        with st.expander("ℹ️ Methodology", expanded=False):
                            st.markdown(f"""
**Data Source:** `tsums` (lot-level) + `mtsums -msn` (module-level)

**100% Fail:** `UIN=4` and `UPASS=0`

**Status:**
- 🔄 **Chronic** - 100% fail in both WW{selected_ww} and WW{prev_ww}
- 🆕 **New** - Only in WW{selected_ww}
- ✅ **Resolved** - Only in WW{prev_ww}

**Recovery (WW{next_ww}):** Checks if machine passed next week

**⚠️ SOP Violation:** Retest after HUNG2 on same MOBO
- HUNG1 → OK to retest on same MOBO
- HUNG2 → Must move to different MOBO after this
- HUNG (3rd+) → **Violation!** (retested after HUNG2)
                            """)

                        # Analyze button
                        analyze_btn = st.button(
                            f"🔎 Analyze {len(current_issue_machines)} Machines",
                            key="grace_batch_analyze",
                            help="Runs tsums drill-down for 100% fail cases"
                        )

                        if analyze_btn:
                            with st.spinner(f"Analyzing {len(current_issue_machines)} machines..."):
                                drill_df = analyze_machines_100pct_fails(
                                    machine_ids=current_issue_machines,
                                    current_ww=selected_ww,
                                    previous_ww=prev_ww,
                                    days=30
                                )
                                st.session_state.grace_drill_results = drill_df
                                has_drill_results = True

                        # Show results if available
                        if has_drill_results:
                            drill_df = st.session_state.grace_drill_results

                            # Summary metrics row - only show relevant metrics
                            recovered_count = len(drill_df[drill_df['recovery_status'].str.contains('Recovered', na=False)])
                            still_failing_count = len(drill_df[drill_df['recovery_status'].str.contains('Still Failing', na=False)])
                            sop_violation_count = len(drill_df[drill_df['sop_violation'] == True])
                            not_reseated_total = int(drill_df['not_reseated'].sum()) if 'not_reseated' in drill_df.columns else 0

                            # Dynamic columns based on data (hide "Not Reseated" when 0)
                            if not_reseated_total > 0:
                                metric_cols = st.columns(4)
                            else:
                                metric_cols = st.columns(3)

                            with metric_cols[0]:
                                st.metric("✅ Recovered", recovered_count)
                            with metric_cols[1]:
                                st.metric("❌ Still Failing", still_failing_count)
                            with metric_cols[2]:
                                st.metric("⚠️ SOP Violations", sop_violation_count)
                            if not_reseated_total > 0:
                                with metric_cols[3]:
                                    st.metric("🔍 Not Reseated", not_reseated_total, help="Modules tested at same site (not physically reseated)")

                            # Add WoW Status from the comparison table to link the two views
                            # Map machine_id to WoW status from problematic_display
                            wow_status_map = dict(zip(
                                problematic_display['machine_id'],
                                problematic_display['Hang Status']
                            ))
                            drill_df['wow_status'] = drill_df['machine_id'].map(wow_status_map).fillna('—')

                            # Build display table with WoW Status for context
                            fail_display = drill_df[['machine_id', 'wow_status', 'status', 'count_current', 'recovery_status', 'remarks']].copy()
                            fail_display.columns = ['Machine ID', 'WoW Hang', '100% Fail', f'Lots (WW{selected_ww})', f'WW{next_ww} Status', 'Remarks']

                            # Simplified highlighting: SOP violations or Bad MOBO
                            def highlight_row(row):
                                remarks = str(row.get('Remarks', ''))
                                if '🔴 Bad MOBO' in remarks:
                                    return ['background-color: #FFCDD2'] * len(row)  # Light red for Bad MOBO
                                elif '⚠️ SOP' in remarks:
                                    return ['background-color: #FFF3E0'] * len(row)  # Light orange for SOP
                                return [''] * len(row)

                            styled_fail = fail_display.style.apply(highlight_row, axis=1)
                            # Auto-fit height based on row count (no scrolling)
                            st.dataframe(styled_fail, use_container_width=True, hide_index=True)

                            # Combined Details expander (Lot Details + SOP Rules in parallel grid)
                            with st.expander("📋 Details (Lots & SOP Rules)", expanded=False):
                                detail_col1, detail_col2 = st.columns([3, 2])

                                # Left: Lot Details
                                with detail_col1:
                                    st.markdown("**Lot Details (100% Fail)**")
                                    fail_machines = drill_df[drill_df['status'].str.contains('Chronic|New', regex=True)]
                                    if not fail_machines.empty:
                                        for _, row in fail_machines.iterrows():
                                            sop_badge = "⚠️" if row.get('sop_violation') else ""
                                            st.markdown(f"{sop_badge} **{row['machine_id']}** - {row['status']}")
                                            if row['lots_current']:
                                                st.caption(f"WW{selected_ww}: {row['lots_current'][:60]}{'...' if len(str(row['lots_current'])) > 60 else ''}")
                                            if row['lots_prev']:
                                                st.caption(f"WW{prev_ww}: {row['lots_prev'][:60]}{'...' if len(str(row['lots_prev'])) > 60 else ''}")
                                    else:
                                        st.info("No 100% fail cases")

                                # Right: SOP Rules (compact)
                                with detail_col2:
                                    st.markdown("**SOP Rules (HANG)**")
                                    st.markdown("""
`HUNG1` → OK to retest same MOBO
`HUNG2` → Must move to diff MOBO
`HUNG` → ⚠️ **Violation** (retest after HUNG2)
""")
                                    if sop_violation_count > 0:
                                        st.markdown(f"**{sop_violation_count} violation(s)** in WW{selected_ww}")

                        else:
                            # Placeholder when no analysis yet
                            st.info("👆 Click **Analyze** to run 100% fail drill-down")

            else:
                st.success(f"No machines with Hang failures in WW{selected_ww} or WW{prev_ww}")


        else:
            # Selected week not in available data
            st.warning(f"WW{selected_ww} not found in fetched data. Available weeks: {', '.join([f'WW{w}' for w in available_weeks])}")
            st.info("Adjust the main dashboard Work Week filter or fetch fresh data.")

    else:
        # No data - show placeholder
        st.info("👆 Click **Fetch GRACE Data** to load motherboard health metrics using Fail Mode (FM) cDPM analysis.")

        placeholder_html = """
        <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border-radius: 12px; padding: 40px; margin: 20px 0; text-align: center; border: 2px dashed rgba(255,255,255,0.2);">
            <div style="font-size: 48px; margin-bottom: 15px;">🖥️</div>
            <div style="font-size: 18px; color: #888; margin-bottom: 10px;">GRACE Motherboard Health Monitoring</div>
            <div style="font-size: 14px; color: #666;">Data source: MTSUMS +fm (Fail Mode cDPM analysis)</div>
            <div style="font-size: 12px; color: #555; margin-top: 10px;">
                <br>• Fetch FM data to see machines with Hang failures
                <br>• Compare week-over-week to identify new, resolved, and chronic issues
                <br>• Drill down to find 100% fail cases (UIN=4, UPASS=0)
            </div>
        </div>
        """
        st.markdown(placeholder_html, unsafe_allow_html=True)


def render_machine_trend_tab(filters: dict[str, Any]) -> None:
    """Render the Machine Trend Analysis tab for SMT6 tester monitoring and GRACE Motherboard analysis."""
    st.subheader("Machine Trend Analysis")
    st.info("🔧 **SMT6 Tester Yield:** Socket & site trends (Step=HMFN) | **GRACE Motherboard:** Hang & SOP violation tracking (HMB1+QMON integrated) — Required: WW range, Form Factor, Facility | Optional: DID, Density, Speed")

    # Sub-tabs within Machine Trend Analysis
    machine_subtab1, machine_subtab2 = st.tabs([
        "🔧 SMT6 Tester Yield",
        "🖥️ GRACE Motherboard"
    ])

    with machine_subtab1:
        # SMT6 Yield Trend content
        render_smt6_yield_section(filters)

    with machine_subtab2:
        # GRACE Motherboard Health Monitoring
        render_grace_motherboard_section(filters)


def render_fail_viewer_tab(filters: dict[str, Any]) -> None:
    """Render the Fail Viewer tab for visualizing fail address patterns."""
    import os
    import numpy as np

    st.subheader("Fail Viewer")
    st.info("🔍 **Visualize fail address patterns** with die map, DQ/Bank distribution — Required: Upload CSV or generate sample data | Options: Part Type, Color By")

    # Part type selection
    col1, col2, col3 = st.columns(3)
    with col1:
        part_type = st.selectbox(
            "Part Type",
            options=["y62p", "y6cp", "y63n"],
            index=0,
            help="Select the part type for die geometry"
        )
    with col2:
        color_by = st.selectbox(
            "Color By",
            options=["dq", "bank", "none"],
            index=0,
            help="Color fail points by DQ, Bank, or single color"
        )
    with col3:
        marker_size = st.slider("Marker Size", min_value=1, max_value=10, value=3)

    # File upload or sample data
    st.markdown("---")
    data_source = st.radio(
        "Data Source",
        options=["Upload CSV", "Module BE", "Generate Sample Data"],
        horizontal=True
    )

    fail_df = None

    if data_source == "Module BE":
        st.markdown("**Fetch fail addresses from Module Backend**")

        col1, col2 = st.columns(2)
        with col1:
            test_summary = st.text_input(
                "Test Summary",
                placeholder="e.g., JAB/AY/S9/001NB",
                help="Enter the test summary (e.g., JAB/AY/S9/001NB)"
            )
        with col2:
            fid = st.text_input(
                "FID",
                placeholder="e.g., 785322L:14:P15:04",
                help="Enter the FID (FABLOT:WW:XPOS:YPOS)"
            )

        if st.button("Fetch Fail Data", type="primary", key="fetch_fdat95"):
            if not test_summary or not fid:
                st.error("Please enter both Test Summary and FID")
            else:
                with st.spinner(f"Fetching fail data for FID {fid}..."):
                    try:
                        import subprocess

                        # Step 1: Auto-detect DID using mtsums
                        detected_did = None
                        mtsums_cmd = f'/u/dramsoft/bin/mtsums -FORCEAPI {test_summary} -fid=/{fid}/ -format+=fid_status2 +quiet 2>&1 | grep -v "^~" | grep -v "^FID" | head -1'
                        mtsums_result = subprocess.run(
                            mtsums_cmd,
                            shell=True,
                            capture_output=True,
                            text=True,
                            timeout=60
                        )

                        if mtsums_result.stdout.strip():
                            # Parse mtsums output to get DESIGN column (column index may vary)
                            # Format: FID ULOC WREG MSN ... DESIGN FAB STEP ...
                            fields = mtsums_result.stdout.strip().split()
                            # Find DESIGN field - it's typically after SUMMARY
                            for i, field in enumerate(fields):
                                if field.upper() in ['Y62P', 'Y6CP', 'Y63N', 'Y42M']:
                                    detected_did = field.lower()
                                    break

                        if detected_did:
                            st.session_state.fail_viewer_part_type = detected_did
                            st.info(f"Auto-detected DID: **{detected_did.upper()}**")
                        else:
                            st.warning("Could not auto-detect DID, using selected Part Type")

                        # Step 2: Use mtsums +fa to get fail addresses
                        # Format: FID,DESIGN_ID,ROW,COL,DQ
                        cmd = f"/u/dramsoft/bin/mtsums -FORCEAPI +quiet +csv {test_summary} -fid=/{fid}/ -format=FID,DESIGN_ID,ROW,COL,DQ +fa"

                        # Run the command
                        result = subprocess.run(
                            cmd,
                            shell=True,
                            capture_output=True,
                            text=True,
                            timeout=120
                        )

                        if result.returncode != 0 and not result.stdout:
                            st.error(f"mtsums +fa command failed: {result.stderr}")
                        else:
                            # Parse CSV output - skip header, extract ROW,COL,DQ
                            lines = result.stdout.strip().split('\n')
                            data = []

                            for line in lines[1:]:  # Skip header
                                line = line.strip()
                                if not line or ',' not in line:
                                    continue

                                parts = line.split(',')
                                # Format: FID,DESIGN_ID,ROW,COL,DQ
                                if len(parts) >= 5:
                                    try:
                                        row = int(parts[2])  # ROW (decimal from mtsums)
                                        col = int(parts[3])  # COL (decimal from mtsums)
                                        dq = int(parts[4])   # DQ
                                        data.append({'row': row, 'col': col, 'dq': dq})

                                        # Also auto-detect DID from first row if not already detected
                                        if not detected_did and parts[1].upper() in ['Y62P', 'Y6CP', 'Y63N', 'Y42M']:
                                            detected_did = parts[1].lower()
                                            st.session_state.fail_viewer_part_type = detected_did
                                    except ValueError:
                                        continue

                            if data:
                                fail_df = pd.DataFrame(data)
                                st.session_state.fail_viewer_data = fail_df
                                did_label = detected_did.upper() if detected_did else part_type.upper()
                                st.session_state.fail_viewer_source = f"Module BE: {fid} ({did_label})"
                                # Store test summary and FID for repair loading
                                st.session_state.fail_viewer_test_summary = test_summary
                                st.session_state.fail_viewer_fid = fid
                                st.success(f"Loaded {len(fail_df)} fail addresses from FID {fid} ({did_label})")
                            else:
                                st.warning("No valid fail addresses found in the output")
                                if result.stdout:
                                    with st.expander("Raw output"):
                                        st.code(result.stdout[:2000])
                    except subprocess.TimeoutExpired:
                        st.error("Command timed out after 120 seconds")
                    except Exception as e:
                        st.error(f"Error fetching data: {e}")

        # Use stored data if available
        if "fail_viewer_data" in st.session_state:
            fail_df = st.session_state.fail_viewer_data
            if "fail_viewer_source" in st.session_state:
                st.info(f"Using data from: {st.session_state.fail_viewer_source}")

    elif data_source == "Upload CSV":
        uploaded_file = st.file_uploader(
            "Upload Fail CSV",
            type=["csv"],
            help="CSV format: row,col,dq (no header) or with header columns named 'row', 'col', 'dq'"
        )

        if uploaded_file is not None:
            try:
                # Try to detect if file has header
                content = uploaded_file.getvalue().decode('utf-8')
                first_line = content.split('\n')[0]
                has_header = not first_line.replace(',', '').replace('.', '').replace('-', '').isdigit()

                uploaded_file.seek(0)
                if has_header:
                    fail_df = pd.read_csv(uploaded_file)
                    fail_df.columns = [c.lower().strip() for c in fail_df.columns]
                else:
                    fail_df = pd.read_csv(uploaded_file, header=None, names=['row', 'col', 'dq'])

                # Ensure numeric
                fail_df['row'] = pd.to_numeric(fail_df['row'], errors='coerce')
                fail_df['col'] = pd.to_numeric(fail_df['col'], errors='coerce')
                fail_df['dq'] = pd.to_numeric(fail_df['dq'], errors='coerce')
                fail_df = fail_df.dropna()

                st.success(f"Loaded {len(fail_df)} fail addresses")
            except Exception as e:
                st.error(f"Error loading CSV: {e}")

    else:  # Generate Sample Data
        col1, col2 = st.columns(2)
        with col1:
            n_fails = st.number_input("Number of Fails", min_value=100, max_value=50000, value=2000, step=500)
        with col2:
            pattern_type = st.selectbox(
                "Pattern Type",
                options=["Random + Column Plane", "Random + Row Pattern", "Random Only", "Block Pattern"],
                index=0
            )

        if st.button("Generate Sample Data", type="primary"):
            try:
                geometry = load_geometry(part_type)
                row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
                col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)

                np.random.seed(42)
                data = []

                if pattern_type == "Random Only":
                    for _ in range(n_fails):
                        data.append({
                            'row': np.random.randint(0, row_per_bank),
                            'col': np.random.randint(0, col_per_bank),
                            'dq': np.random.randint(0, 16)
                        })
                elif pattern_type == "Random + Column Plane":
                    # 60% random, 40% column plane
                    for _ in range(int(n_fails * 0.6)):
                        data.append({
                            'row': np.random.randint(0, row_per_bank),
                            'col': np.random.randint(0, col_per_bank),
                            'dq': np.random.randint(0, 16)
                        })
                    col_plane = np.random.randint(1000, col_per_bank - 1000)
                    for _ in range(int(n_fails * 0.4)):
                        data.append({
                            'row': np.random.randint(0, row_per_bank),
                            'col': col_plane + np.random.randint(-50, 50),
                            'dq': np.random.choice([3, 7])
                        })
                elif pattern_type == "Random + Row Pattern":
                    # 60% random, 40% row pattern
                    for _ in range(int(n_fails * 0.6)):
                        data.append({
                            'row': np.random.randint(0, row_per_bank),
                            'col': np.random.randint(0, col_per_bank),
                            'dq': np.random.randint(0, 16)
                        })
                    row_fail = np.random.randint(10000, row_per_bank - 10000)
                    for _ in range(int(n_fails * 0.4)):
                        data.append({
                            'row': row_fail + np.random.randint(-20, 20),
                            'col': np.random.randint(0, col_per_bank),
                            'dq': np.random.randint(0, 16)
                        })
                else:  # Block Pattern
                    # 50% random, 50% block
                    for _ in range(int(n_fails * 0.5)):
                        data.append({
                            'row': np.random.randint(0, row_per_bank),
                            'col': np.random.randint(0, col_per_bank),
                            'dq': np.random.randint(0, 16)
                        })
                    block_row = np.random.randint(5000, row_per_bank - 10000)
                    block_col = np.random.randint(1000, col_per_bank - 5000)
                    for _ in range(int(n_fails * 0.5)):
                        data.append({
                            'row': block_row + np.random.randint(0, 5000),
                            'col': block_col + np.random.randint(0, 3000),
                            'dq': np.random.choice([0, 1, 2, 3])
                        })

                fail_df = pd.DataFrame(data)
                st.session_state.fail_viewer_data = fail_df
                st.success(f"Generated {len(fail_df)} fail addresses with '{pattern_type}' pattern")
            except Exception as e:
                st.error(f"Error generating data: {e}")

        # Use stored data if available
        if "fail_viewer_data" in st.session_state:
            fail_df = st.session_state.fail_viewer_data

    # Render visualizations if data is available
    if fail_df is not None and not fail_df.empty:
        st.markdown("---")

        # Use auto-detected part type from Module BE if available, otherwise use dropdown selection
        effective_part_type = st.session_state.get('fail_viewer_part_type', part_type)

        # Show which part type is being used
        if 'fail_viewer_part_type' in st.session_state and st.session_state.fail_viewer_part_type != part_type:
            st.info(f"Using auto-detected DID: **{effective_part_type.upper()}** (from Module BE)")

        # Process data for physical coordinates
        try:
            geometry = load_geometry(effective_part_type)
            processed_df = process_fail_data(fail_df, geometry)

            # Show data summary
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Fails", len(processed_df))
            with col2:
                st.metric("Unique DQs", processed_df['dq'].nunique())
            with col3:
                st.metric("Unique Banks", processed_df['bank'].nunique())
            with col4:
                top_dq = processed_df['dq'].mode().iloc[0] if len(processed_df) > 0 else "N/A"
                st.metric("Top DQ", f"DQ{int(top_dq)}" if top_dq != "N/A" else "N/A")

            # Visualization tabs
            viz_tab1, viz_tab2, viz_tab3, viz_tab4 = st.tabs([
                "Die Map", "Heatmap", "DQ Distribution", "Bank Distribution"
            ])

            with viz_tab1:
                st.markdown("### Fail Map (Die View)")

                # Display options
                opt_col1, opt_col2, opt_col3 = st.columns(3)
                with opt_col1:
                    show_grid = st.checkbox("Show Bank Grid", value=True, key="fv_grid")
                    show_labels = st.checkbox("Show Bank Labels", value=True, key="fv_labels")
                with opt_col2:
                    show_repairs = st.checkbox("Show Repair Overlay", value=False, key="fv_repairs")
                with opt_col3:
                    if show_repairs:
                        repair_source = st.selectbox(
                            "Repair Source",
                            options=["Mock Data (Demo)", "From Test Artifacts"],
                            index=0,
                            key="fv_repair_source",
                            help="Select repair data source"
                        )

                # Create base fail viewer
                fig = create_fail_viewer(
                    processed_df,
                    part_type=effective_part_type,
                    title=f"Fail Viewer - {effective_part_type.upper()}",
                    show_bank_grid=show_grid,
                    show_bank_labels=show_labels,
                    color_by=color_by if color_by != "none" else None,
                    marker_size=marker_size,
                    width=900,
                    height=700
                )

                # Add repair overlay if enabled
                if show_repairs:
                    try:
                        # Extract FID from session state
                        fid = None
                        test_summary = None
                        source_info = st.session_state.get('fail_viewer_source', '')

                        if 'Module BE' in source_info:
                            # Parse FID from source info like "Module BE: 785322L:14:P15:04 (Y6CP)"
                            parts = source_info.replace('Module BE: ', '').split(' ')
                            if parts:
                                fid = parts[0]
                            # Get test summary from session state if available
                            test_summary = st.session_state.get('fail_viewer_test_summary')

                        if repair_source == "Mock Data (Demo)":
                            # Generate mock repair data for demonstration
                            mock_fid = fid or 'SAMPLE:00:P00:00'
                            repair_data = create_mock_repair_data(
                                fid=mock_fid,
                                did=effective_part_type.upper(),
                                test_step="HMFN"
                            )
                            repair_data = apply_did_equations(repair_data, geometry)

                            # Add repair overlay to figure
                            fig = add_repair_overlay(fig, repair_data, part_type=effective_part_type, line_width=2)

                            # Show repair summary
                            summary = get_repair_summary(repair_data)
                            st.caption(
                                f"Repair Overlay: {summary['row_repairs']} Row repairs (blue), "
                                f"{summary['column_repairs']} Column repairs (green) | "
                                f"Source: Mock Data"
                            )
                        else:
                            # Try to load real repair data from artifacts
                            if fid:
                                # Check available sources
                                sources = get_available_repair_sources(fid, effective_part_type.upper())

                                if sources.get('stress_fail_artifact'):
                                    st.info(f"Found artifact: {sources['stress_fail_path']}")

                                # Try to load repair data
                                repair_data = load_repair_data(
                                    fid=fid,
                                    did=effective_part_type.upper(),
                                    test_step="HMFN",
                                    test_summary=test_summary
                                )

                                if repair_data and repair_data.repairs:
                                    # Check if we have actual coordinate data or just metadata
                                    has_real_coords = any(
                                        r.logical_address.row is not None or r.logical_address.column is not None
                                        for r in repair_data.repairs
                                    )

                                    if has_real_coords:
                                        repair_overlay = apply_did_equations(repair_data, geometry)
                                        fig = add_repair_overlay(fig, repair_overlay, part_type=effective_part_type, line_width=2)
                                        summary = get_repair_summary(repair_overlay)
                                        st.success(
                                            f"Loaded {summary['total_repairs']} repairs from artifacts | "
                                            f"Row: {summary['row_repairs']}, Column: {summary['column_repairs']}"
                                        )
                                    else:
                                        # Got metadata but no coordinates - show info and use mock
                                        meta_repair = repair_data.repairs[0] if repair_data.repairs else None
                                        if meta_repair and meta_repair.metadata.get('source') == 'mtsums_metadata':
                                            st.warning(
                                                f"Repair metadata found (ROWCNT={meta_repair.metadata.get('rowcnt', 0)}, "
                                                f"COLCNT={meta_repair.metadata.get('colcnt', 0)}) but full coordinates "
                                                f"require .bin artifact parsing. Using mock overlay for visualization."
                                            )

                                        # Fall back to mock data with real FID
                                        repair_data = create_mock_repair_data(
                                            fid=fid,
                                            did=effective_part_type.upper(),
                                            test_step="HMFN"
                                        )
                                        repair_data = apply_did_equations(repair_data, geometry)
                                        fig = add_repair_overlay(fig, repair_data, part_type=effective_part_type, line_width=2)
                                        st.caption("Showing mock repair overlay (metadata available but full parsing not yet implemented)")
                                else:
                                    st.info(
                                        f"No repair artifacts found for FID {fid}. "
                                        f"Recommendations: {', '.join(sources.get('recommendations', []))}"
                                    )
                                    # Use mock data as fallback
                                    repair_data = create_mock_repair_data(
                                        fid=fid,
                                        did=effective_part_type.upper(),
                                        test_step="HMFN"
                                    )
                                    repair_data = apply_did_equations(repair_data, geometry)
                                    fig = add_repair_overlay(fig, repair_data, part_type=effective_part_type, line_width=2)
                                    st.caption("Showing mock repair overlay (no artifacts found)")
                            else:
                                st.warning("No FID available - upload data via Module BE to enable real repair loading")

                    except Exception as e:
                        st.warning(f"Could not load repair data: {e}")
                        logger.exception("Repair loading error")

                st.plotly_chart(fig, use_container_width=True)

            with viz_tab2:
                st.markdown("### Fail Density Heatmap")
                bin_size = st.slider("Bin Size", min_value=50, max_value=1000, value=200, step=50, key="fv_bin")

                fig = create_fail_heatmap(
                    processed_df,
                    part_type=effective_part_type,
                    title=f"Fail Density - {effective_part_type.upper()}",
                    bin_size=bin_size,
                    width=900,
                    height=700
                )
                st.plotly_chart(fig, use_container_width=True)

            with viz_tab3:
                st.markdown("### Fail Distribution by DQ")
                fig = create_dq_distribution(fail_df, title="Fails per DQ")
                st.plotly_chart(fig, use_container_width=True)

            with viz_tab4:
                st.markdown("### Fail Distribution by Bank")
                fig = create_bank_distribution(processed_df, part_type=effective_part_type, title="Fails per Bank")
                st.plotly_chart(fig, use_container_width=True)

            # Data table
            with st.expander("View Raw Data"):
                st.dataframe(processed_df.head(1000), use_container_width=True)
                if len(processed_df) > 1000:
                    st.caption(f"Showing first 1000 of {len(processed_df)} rows")

        except Exception as e:
            st.error(f"Error processing fail data: {e}")
            logger.exception("Fail viewer error")

    else:
        st.info("Upload a CSV file or generate sample data to visualize fail patterns.")


def main() -> None:
    """Main application entry point."""
    setup_page()
    init_session_state()

    filters = render_sidebar()

    # Cache controls (in sidebar)
    st.sidebar.divider()
    st.sidebar.subheader("Cache Settings")

    cache = FrptCache()
    cache_stats = cache.get_stats()
    st.sidebar.caption(
        f"Cached: {cache_stats['valid_entries']} queries | "
        f"Size: {cache_stats['total_size_mb']:.1f} MB"
    )

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Clear Cache", use_container_width=True):
            cleared = cache.clear()
            st.sidebar.success(f"Cleared {cleared} entries")
    with col2:
        use_cache = st.checkbox("Use Cache", value=True, help="Uncheck to force fresh data fetch")

    # Store cache preference in session state
    st.session_state.use_cache = use_cache

    # PDF Export section
    st.sidebar.divider()
    st.sidebar.subheader("📄 Export Report")

    # Tab selector for export - export only the selected tab's view
    export_tab_options = {
        "Yield Analysis": "yield",
        "Module ELC Yield": "elc",
        "Pareto Analysis": "pareto",
        "Machine Trends - SMT6": "smt6",
        "Machine Trends - GRACE": "grace"
    }
    selected_export_tab = st.sidebar.selectbox(
        "Export Tab",
        options=list(export_tab_options.keys()),
        key="export_tab_selector",
        help="Select which tab view to export"
    )
    export_tab_key = export_tab_options[selected_export_tab]

    if st.sidebar.button("Generate PDF Report", use_container_width=True, type="primary"):
        with st.sidebar.status(f"Generating {selected_export_tab} PDF...", expanded=True) as status:
            try:
                # Debug: Log selected tab
                logger.info(f"[PDF Export] Selected tab: {selected_export_tab}, key: {export_tab_key}")

                charts = {}
                use_cache = st.session_state.get("use_cache", True)

                # Initialize all data as None - only fetch what's needed for selected tab
                yield_data = None
                elc_data = None
                smt6_data = None
                grace_data = None

                # Fetch only the data needed for the selected tab
                if export_tab_key == "yield":
                    logger.info("[PDF Export] Fetching YIELD data")
                    status.update(label="Fetching Yield Analysis data...")
                    yield_data = st.session_state.get('data', None)
                    if yield_data is None or (hasattr(yield_data, 'empty') and yield_data.empty):
                        try:
                            yield_data = fetch_data(filters, use_cache=use_cache)
                            if yield_data is not None and not yield_data.empty:
                                st.session_state.data = yield_data
                            else:
                                yield_data = None
                        except Exception as e:
                            logger.warning(f"Failed to fetch yield data for PDF: {e}")

                elif export_tab_key == "elc":
                    logger.info("[PDF Export] Fetching ELC data")
                    status.update(label="Fetching ELC data...")
                    elc_data = st.session_state.get('elc_data', None)
                    if elc_data is None or (hasattr(elc_data, 'empty') and elc_data.empty):
                        try:
                            elc_data = fetch_elc_data(filters, use_cache=use_cache)
                            if elc_data is not None and not elc_data.empty:
                                st.session_state.elc_data = elc_data
                            else:
                                elc_data = None
                        except Exception as e:
                            logger.warning(f"Failed to fetch ELC data for PDF: {e}")

                elif export_tab_key == "smt6":
                    logger.info("[PDF Export] Fetching SMT6 data")
                    status.update(label="Fetching SMT6 site data...")
                    smt6_data = st.session_state.get('smt6_site_data', None)
                    if smt6_data is None or (hasattr(smt6_data, 'empty') and smt6_data.empty):
                        smt6_data = st.session_state.get('smt6_data', None)
                    if smt6_data is None or (hasattr(smt6_data, 'empty') and smt6_data.empty):
                        try:
                            from src.smt6_yield import fetch_smt6_site_data
                            wws = [str(ww) for ww in Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])]
                            smt6_data = fetch_smt6_site_data(
                                design_ids=filters.get("design_ids", ["Y6CP"]),
                                workweeks=wws,
                                form_factor=filters.get("form_factor", "socamm2").lower()
                            )
                            if smt6_data is not None and not smt6_data.empty:
                                st.session_state.smt6_site_data = smt6_data
                            else:
                                smt6_data = None
                        except Exception as e:
                            logger.warning(f"Failed to fetch SMT6 data for PDF: {e}")

                elif export_tab_key == "grace":
                    logger.info("[PDF Export] Fetching GRACE data")
                    status.update(label="Fetching GRACE data...")
                    grace_data = st.session_state.get('grace_fm_data', None)
                    if grace_data is None or (hasattr(grace_data, 'empty') and grace_data.empty):
                        try:
                            from src.grace_motherboard import fetch_grace_fm_data
                            # Don't pass facility if it's "all" or empty
                            facility_raw = filters.get("facility", "")
                            facility_filter = facility_raw if facility_raw and facility_raw.lower() != "all" else None
                            grace_data = fetch_grace_fm_data(
                                start_ww=filters["start_ww"],
                                end_ww=filters["end_ww"],
                                design_ids=filters.get("design_ids"),
                                facility=facility_filter
                            )
                            if grace_data is not None and not grace_data.empty:
                                st.session_state.grace_fm_data = grace_data
                            else:
                                grace_data = None
                        except Exception as e:
                            logger.warning(f"Failed to fetch GRACE data for PDF: {e}")

                elif export_tab_key == "pareto":
                    status.update(label="Fetching Pareto data...")
                    # Pareto uses yield_data as base
                    yield_data = st.session_state.get('data', None)
                    if yield_data is None or (hasattr(yield_data, 'empty') and yield_data.empty):
                        try:
                            yield_data = fetch_data(filters, use_cache=use_cache)
                            if yield_data is not None and not yield_data.empty:
                                st.session_state.data = yield_data
                            else:
                                yield_data = None
                        except Exception as e:
                            logger.warning(f"Failed to fetch pareto data for PDF: {e}")

                # Check if data available for selected tab
                tab_data = {
                    "yield": yield_data, "elc": elc_data, "smt6": smt6_data,
                    "grace": grace_data, "pareto": yield_data
                }
                if tab_data.get(export_tab_key) is None:
                    st.sidebar.warning(f"No data available for {selected_export_tab}. Check your filters.")
                    status.update(label="No data found", state="error")
                else:
                    # Generate PDF for selected tab only
                    status.update(label=f"Building {selected_export_tab} PDF...")
                    pdf_bytes = create_dashboard_pdf(
                        filters=filters,
                        yield_data=yield_data,
                        elc_data=elc_data,
                        smt6_data=smt6_data,
                        grace_data=grace_data,
                        charts=charts
                    )

                    # Create download button
                    status.update(label="PDF Ready!", state="complete")
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                    filename = f"yield_dashboard_report_{timestamp}.pdf"

                    st.sidebar.download_button(
                        label="⬇️ Download PDF",
                        data=pdf_bytes,
                        file_name=filename,
                        mime="application/pdf",
                        use_container_width=True
                    )

            except Exception as e:
                status.update(label=f"Error: {str(e)}", state="error")
                st.sidebar.error(f"PDF generation failed: {str(e)}")

    # HTML Export option (uses same tab selector as PDF)
    if st.sidebar.button("Generate HTML Report", use_container_width=True):
        with st.sidebar.status(f"Generating {selected_export_tab} HTML...", expanded=True) as status:
            try:
                from src.html_export import create_shareable_html
                use_cache = st.session_state.get("use_cache", True)

                # Collect sections based on selected tab only
                sections = []

                if export_tab_key == "yield":
                    # Yield Analysis tab
                    status.update(label="Fetching Yield Analysis data...")
                    if 'data' not in st.session_state or st.session_state.data.empty:
                        try:
                            yield_data = fetch_data(filters, use_cache=use_cache)
                            if yield_data is not None and not yield_data.empty:
                                st.session_state.data = yield_data
                        except Exception as e:
                            logger.warning(f"Failed to fetch yield data for HTML: {e}")

                    if 'data' in st.session_state and not st.session_state.data.empty:
                        yield_df = st.session_state.data
                        summary_stats = f"""
                        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:15px;">
                            <div style="background:#16213e;padding:15px;border-radius:8px;text-align:center;">
                                <div style="font-size:24px;font-weight:bold;color:#00C853;">{len(yield_df):,}</div>
                                <div style="font-size:12px;color:#888;">Total Records</div>
                            </div>
                            <div style="background:#16213e;padding:15px;border-radius:8px;text-align:center;">
                                <div style="font-size:24px;font-weight:bold;color:#00B0FF;">{yield_df['workweek'].nunique()}</div>
                                <div style="font-size:12px;color:#888;">Work Weeks</div>
                            </div>
                            <div style="background:#16213e;padding:15px;border-radius:8px;text-align:center;">
                                <div style="font-size:24px;font-weight:bold;color:#FFB300;">{yield_df['design_id'].nunique() if 'design_id' in yield_df.columns else 'N/A'}</div>
                                <div style="font-size:12px;color:#888;">Design IDs</div>
                            </div>
                        </div>
                        """
                        sections.append({'title': 'Yield Analysis Summary', 'content': summary_stats, 'type': 'html'})

                elif export_tab_key == "smt6":
                    # SMT6 tab
                    status.update(label="Fetching SMT6 site data...")
                    if 'smt6_site_data' not in st.session_state or st.session_state.smt6_site_data.empty:
                        try:
                            from src.smt6_yield import fetch_smt6_site_data
                            wws = [str(ww) for ww in Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])]
                            smt6_data = fetch_smt6_site_data(
                                design_ids=filters.get("design_ids", ["Y6CP"]),
                                workweeks=wws,
                                form_factor=filters.get("form_factor", "socamm2").lower()
                            )
                            if smt6_data is not None and not smt6_data.empty:
                                st.session_state.smt6_site_data = smt6_data
                        except Exception as e:
                            logger.warning(f"Failed to fetch SMT6 data for HTML: {e}")

                    if 'smt6_site_data' in st.session_state and not st.session_state.smt6_site_data.empty:
                        site_df = st.session_state.smt6_site_data
                        site_summary = site_df.groupby(['machine_id', 'site']).agg({
                            'uin_adj': 'sum', 'upass_adj': 'sum'
                        }).reset_index()
                        site_summary['yield_pct'] = (site_summary['upass_adj'] / site_summary['uin_adj'] * 100).round(2)

                        # Create heatmap
                        pivot = site_summary.pivot_table(index='site', columns='machine_id', values='yield_pct', aggfunc='mean')
                        if not pivot.empty:
                            heatmap_fig = go.Figure(data=go.Heatmap(
                                z=pivot.values, x=[m.upper() for m in pivot.columns], y=pivot.index.tolist(),
                                colorscale=[[0, '#dc3545'], [0.3, '#ffc107'], [0.7, '#17a2b8'], [1, '#28a745']],
                                zmin=90, zmax=100,
                                text=[[f"{v:.0f}%" if pd.notna(v) else "-" for v in row] for row in pivot.values],
                                texttemplate="%{text}", hovertemplate="<b>%{y}</b> @ %{x}: %{z:.1f}%<extra></extra>"
                            ))
                            heatmap_fig.update_layout(
                                title="SMT6 Site Yield Heatmap",
                                xaxis=dict(tickfont=dict(size=9), side='top'),
                                yaxis=dict(autorange='reversed', tickfont=dict(size=8)),
                                height=min(500, max(300, len(pivot.index) * 14 + 80))
                            )
                            sections.append({'title': 'SMT6 Site Yield Heatmap', 'content': heatmap_fig, 'type': 'plotly'})

                        # Add machine summary table
                        machine_summary = site_df.groupby('machine_id').agg({
                            'uin_adj': 'sum', 'upass_adj': 'sum'
                        }).reset_index()
                        machine_summary['yield_pct'] = (machine_summary['upass_adj'] / machine_summary['uin_adj'] * 100).round(2)
                        machine_summary.columns = ['Machine', 'UIN', 'UPASS', 'Yield %']
                        machine_summary['Machine'] = machine_summary['Machine'].str.upper()
                        sections.append({'title': 'Machine Summary', 'content': machine_summary, 'type': 'table'})

                elif export_tab_key == "grace":
                    # GRACE Motherboard tab
                    status.update(label="Fetching GRACE data...")
                    if 'grace_fm_data' not in st.session_state or st.session_state.get('grace_fm_data') is None:
                        try:
                            from src.grace_motherboard import fetch_grace_fm_data
                            # Don't pass facility if it's "all" or empty
                            facility_raw = filters.get("facility", "")
                            facility_filter = facility_raw if facility_raw and facility_raw.lower() != "all" else None
                            grace_data = fetch_grace_fm_data(
                                start_ww=filters["start_ww"],
                                end_ww=filters["end_ww"],
                                design_ids=filters.get("design_ids"),
                                facility=facility_filter
                            )
                            if grace_data is not None and not grace_data.empty:
                                st.session_state.grace_fm_data = grace_data
                        except Exception as e:
                            logger.warning(f"Failed to fetch GRACE data for HTML: {e}")

                    if 'grace_fm_data' in st.session_state and st.session_state.grace_fm_data is not None and not st.session_state.grace_fm_data.empty:
                        grace_df = st.session_state.grace_fm_data
                        # Create summary
                        hang_machines = grace_df[grace_df.get('hang_cdpm', grace_df.get('Hang', pd.Series([0]))).fillna(0) > 0]
                        summary_html = f"""
                        <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:15px;">
                            <div style="background:#16213e;padding:15px;border-radius:8px;text-align:center;">
                                <div style="font-size:24px;font-weight:bold;color:#00C853;">{len(grace_df['machine_id'].unique()) if 'machine_id' in grace_df.columns else 'N/A'}</div>
                                <div style="font-size:12px;color:#888;">Total Machines</div>
                            </div>
                            <div style="background:#16213e;padding:15px;border-radius:8px;text-align:center;">
                                <div style="font-size:24px;font-weight:bold;color:#FF5722;">{len(hang_machines['machine_id'].unique()) if 'machine_id' in hang_machines.columns else 0}</div>
                                <div style="font-size:12px;color:#888;">Machines with Hang</div>
                            </div>
                        </div>
                        """
                        sections.append({'title': 'GRACE Motherboard Summary', 'content': summary_html, 'type': 'html'})

                elif export_tab_key == "elc":
                    # ELC tab
                    status.update(label="Fetching ELC data...")
                    if 'elc_data' not in st.session_state or st.session_state.elc_data.empty:
                        try:
                            elc_data = fetch_elc_data(filters, use_cache=use_cache)
                            if elc_data is not None and not elc_data.empty:
                                st.session_state.elc_data = elc_data
                        except Exception as e:
                            logger.warning(f"Failed to fetch ELC data for HTML: {e}")

                    if 'elc_data' in st.session_state and not st.session_state.elc_data.empty:
                        elc_df = st.session_state.elc_data
                        # Step summary
                        uin_col = 'UIN' if 'UIN' in elc_df.columns else 'uin'
                        upass_col = 'UPASS' if 'UPASS' in elc_df.columns else 'upass'
                        if 'step' in elc_df.columns:
                            step_summary = elc_df.groupby('step').agg({uin_col: 'sum', upass_col: 'sum'}).reset_index()
                            step_summary['yield_pct'] = (step_summary[upass_col] / step_summary[uin_col] * 100).round(2)
                            step_summary.columns = ['Step', 'UIN', 'UPASS', 'Yield %']
                            sections.append({'title': 'ELC Step Summary', 'content': step_summary, 'type': 'table'})

                elif export_tab_key == "pareto":
                    # Pareto tab placeholder
                    status.update(label="Fetching Pareto data...")
                    sections.append({'title': 'Pareto Analysis', 'content': '<p>Pareto analysis export coming soon.</p>', 'type': 'html'})

                if not sections:
                    st.sidebar.warning("No data available to export. Load some data first.")
                else:
                    status.update(label="Building HTML...")
                    report_path = create_shareable_html(
                        title="Module Yield Dashboard Report",
                        filters=filters,
                        sections=sections,
                        output_dir="/tmp"
                    )

                    with open(report_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()

                    status.update(label="HTML Ready!", state="complete")
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                    st.sidebar.download_button(
                        label="⬇️ Download HTML",
                        data=html_content,
                        file_name=f"yield_report_{timestamp}.html",
                        mime="text/html",
                        use_container_width=True
                    )

            except Exception as e:
                status.update(label=f"Error: {str(e)}", state="error")
                st.sidebar.error(f"HTML generation failed: {str(e)}")

    # Create tabs with Home tab first
    tab_home, tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏠 Home", "📊 Yield Analysis", "📈 Module ELC Yield", "📉 Pareto Analysis", "🔍 Fail Viewer", "🔧 Machine Trends"])

    # ==================== HOME TAB ====================
    with tab_home:
        # Feature Cards - Emerald Modern Theme
        card_style = """
            <div style="background: linear-gradient(145deg, #022c22 0%, #064e3b 100%); border-radius: 10px; padding: 15px; height: 140px; border: 1px solid #047857; box-shadow: 0 2px 8px rgba(4, 120, 87, 0.15);">
                <div style="font-size: 24px; margin-bottom: 8px;">{icon}</div>
                <div style="color: #6ee7b7; font-weight: bold; font-size: 14px; margin-bottom: 5px;">{title}</div>
                <div style="color: #a7f3d0; font-size: 11px; line-height: 1.4;">{desc}</div>
            </div>
        """

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.markdown(card_style.format(
                icon="📊",
                title="Yield Analysis",
                desc="Weekly trends, bin distribution, density/speed heatmaps"
            ), unsafe_allow_html=True)
        with col2:
            st.markdown(card_style.format(
                icon="📈",
                title="Module ELC Yield",
                desc="HMFN → SLT → ELC flow with target lines & DID breakdown"
            ), unsafe_allow_html=True)
        with col3:
            st.markdown(card_style.format(
                icon="📉",
                title="Pareto Analysis",
                desc="FAILCRAWLER DPM & Register fallout top failures"
            ), unsafe_allow_html=True)
        with col4:
            st.markdown(card_style.format(
                icon="🔍",
                title="Fail Viewer",
                desc="Die map visualization, DQ/Bank distribution from CSV"
            ), unsafe_allow_html=True)
        with col5:
            st.markdown(card_style.format(
                icon="🔧",
                title="Machine Trends",
                desc="SMT6 tester yield & GRACE motherboard health"
            ), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Help Assistant Section - Load AI robot image
        ai_robot_base64 = get_ai_robot_image_base64()
        if ai_robot_base64:
            ai_icon_html = f'<img src="data:image/png;base64,{ai_robot_base64}" alt="AI Assistant" style="height: 80px; width: auto; border-radius: 8px; filter: drop-shadow(0 0 15px rgba(110, 231, 183, 0.6));"/>'
        else:
            ai_icon_html = '<div style="font-size: 48px; line-height: 1; filter: drop-shadow(0 0 10px rgba(110, 231, 183, 0.5));">🤖</div>'

        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #022c22 0%, #064e3b 100%); border-radius: 12px; padding: 20px; margin: 20px 0 15px 0; border: 1px solid #10b981; box-shadow: 0 4px 15px rgba(16, 185, 129, 0.15);">
            <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 12px;">
                {ai_icon_html}
                <div>
                    <div style="color: #6ee7b7; font-weight: bold; font-size: 18px;">AI Assistant</div>
                    <div style="color: #a7f3d0; font-size: 13px;">How can I help you today?</div>
                </div>
            </div>
            <span style="color: #d1fae5; font-size: 14px;">👋 Welcome to <b>SOCAMM 1-Stop</b>! Tell me what you're looking for and I'll guide you to the right tab.</span>
        </div>
        """, unsafe_allow_html=True)

        user_query = st.text_input(
            "",
            placeholder="e.g., 'check weekly yield', 'find top failures', 'motherboard hang issues'...",
            key="help_query",
            label_visibility="collapsed"
        )

        if user_query:
            query_lower = user_query.lower()
            response = None

            # Helper function: count keyword matches (requires at least 2 for a match)
            def count_matches(keywords: list[str], query: str) -> int:
                return sum(1 for kw in keywords if kw in query)

            # Define keyword sets for each feature
            yield_kw = ['yield', 'trend', 'weekly', 'bin', 'heatmap', 'density', 'speed', 'uin', 'upass']
            elc_kw = ['elc', 'hmfn', 'slt', 'hmb1', 'qmon', 'end of line', 'target', 'end line', 'module yield']
            pareto_kw = ['pareto', 'failure', 'failcrawler', 'register', 'fallout', 'top fail', 'dpm', 'fail mode', 'cdpm']
            fail_viewer_kw = ['fail viewer', 'die map', 'dq', 'bank', 'address', 'pattern', 'csv', 'upload', 'diemap', 'visualization']
            machine_kw = ['machine', 'smt6', 'tester', 'socket', 'site', 'grace', 'motherboard', 'hang', 'sop', 'mobo', 'nvgrace']

            # Calculate match scores (require at least 2 keyword matches)
            scores = {
                'yield': count_matches(yield_kw, query_lower),
                'elc': count_matches(elc_kw, query_lower),
                'pareto': count_matches(pareto_kw, query_lower),
                'fail_viewer': count_matches(fail_viewer_kw, query_lower),
                'machine': count_matches(machine_kw, query_lower),
            }

            # Find best match with at least 2 keywords
            best_match = max(scores, key=scores.get)
            best_score = scores[best_match]

            # Feature matching logic - require at least 2 keyword matches
            if best_score >= 2 and best_match == 'yield':
                response = {
                    'tab': '📊 Yield Analysis',
                    'desc': 'View weekly yield trends, bin distribution charts, and density/speed heatmaps.',
                    'steps': '1. Select **WW range**, **Form Factor**, **DID**, **Facility**, **Step** in sidebar\n2. Go to **📊 Yield Analysis** tab\n3. Click **Fetch Module Yield Data**'
                }
            elif best_score >= 2 and best_match == 'elc':
                response = {
                    'tab': '📈 Module ELC Yield',
                    'desc': 'Track HMFN → SLT → ELC yield flow with target lines and DID breakdown.',
                    'steps': '1. Select **WW range**, **Form Factor**, **DID**, **Facility**, **Steps (HMFN/HMB1/QMON)** in sidebar\n2. Go to **📈 Module ELC Yield** tab\n3. Click **Fetch ELC Data**'
                }
            elif best_score >= 2 and best_match == 'pareto':
                response = {
                    'tab': '📉 Pareto Analysis',
                    'desc': 'Identify top failures with FAILCRAWLER DPM and Register Fallout analysis.',
                    'steps': '1. Select **WW range**, **Form Factor**, **DID**, **Facility**, **Step** in sidebar\n2. Go to **📉 Pareto Analysis** tab\n3. Choose **FAILCRAWLER DPM** or **Register Fallout** subtab'
                }
            elif best_score >= 2 and best_match == 'fail_viewer':
                response = {
                    'tab': '🔍 Fail Viewer',
                    'desc': 'Visualize fail address patterns with die maps and DQ/Bank distribution.',
                    'steps': '1. Go to **🔍 Fail Viewer** tab\n2. Upload a **CSV file** with fail addresses OR generate sample data\n3. Select **Part Type** and **Color By** options'
                }
            elif best_score >= 2 and best_match == 'machine':
                response = {
                    'tab': '🔧 Machine Trend Analysis',
                    'desc': 'Monitor SMT6 tester performance and GRACE motherboard health.',
                    'steps': '**For SMT6 Tester Yield:**\n1. Select **Step=HMFN** in sidebar\n2. Go to **🔧 Machine Trend Analysis** → **SMT6 Tester Yield**\n\n**For GRACE Motherboard:**\n1. Select **WW range**, **Form Factor**, **Facility** in sidebar\n2. Go to **🔧 Machine Trend Analysis** → **GRACE Motherboard**\n3. Click **Fetch GRACE Data** (uses HMB1+QMON integrated)'
                }
            else:
                # No match with 2+ keywords - show guide
                response = {
                    'tab': '🔎 Need more details',
                    'desc': f"I found {best_score} keyword match(es). Try adding more specific terms:",
                    'steps': '• **Yield trends & bins** → 📊 Yield Analysis (try: "weekly yield trend")\n• **ELC/HMFN/SLT** → 📈 Module ELC Yield (try: "elc target yield")\n• **Top failures** → 📉 Pareto Analysis (try: "failcrawler dpm")\n• **Fail patterns** → 🔍 Fail Viewer (try: "die map pattern")\n• **Machine/Motherboard** → 🔧 Machine Trends (try: "grace motherboard hang")'
                }

            # Log query to CSV for learning
            import csv
            from pathlib import Path
            log_file = Path("/home/asegaran/MODULE_YIELD_DASHBOARD/help_assistant_log.csv")
            matched_kw_str = f"{best_match}:{best_score}"

            # Store in session state for feedback tracking
            st.session_state.last_help_query = user_query
            st.session_state.last_help_tab = response['tab']
            st.session_state.last_help_score = best_score
            st.session_state.last_help_match = best_match

            # Display response
            st.markdown(f"""
            <div style="background: #064e3b; border-radius: 8px; padding: 15px; margin-top: 10px; border-left: 4px solid #10b981;">
                <div style="color: #6ee7b7; font-weight: bold; font-size: 16px; margin-bottom: 8px;">🎯 {response['tab']}</div>
                <div style="color: #d1fae5; font-size: 13px; margin-bottom: 10px;">{response['desc']}</div>
            </div>
            """, unsafe_allow_html=True)
            st.markdown(f"**How to get there:**\n\n{response['steps']}")

            # Feedback buttons
            st.markdown("<br>", unsafe_allow_html=True)
            fb_col1, fb_col2, fb_col3 = st.columns([1, 1, 4])
            with fb_col1:
                if st.button("👍 Helpful", key="fb_yes", use_container_width=True):
                    # Log successful suggestion
                    with open(log_file, 'a', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([datetime.now().isoformat(), user_query, response['tab'], matched_kw_str, best_score, 'helpful'])
                    st.success("Thanks! Logged as helpful.")
            with fb_col2:
                if st.button("👎 Wrong", key="fb_no", use_container_width=True):
                    # Log unsuccessful suggestion
                    with open(log_file, 'a', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([datetime.now().isoformat(), user_query, response['tab'], matched_kw_str, best_score, 'wrong'])
                    st.warning("Thanks! I'll learn from this.")

        # Quick Start Guide - Emerald Theme
        st.markdown("""
        <div style="background: #022c22; border-radius: 8px; padding: 12px 20px; margin: 10px 0; border-left: 4px solid #10b981;">
            <span style="color: #6ee7b7; font-weight: bold;">⚡ Quick Start:</span>
            <span style="color: #d1fae5; font-size: 13px;"> Select <b>WW range</b>, <b>Form Factor</b>, <b>DID</b>, <b>Facility</b> in sidebar → Choose a tab above → Click <b>Fetch Data</b></span>
        </div>
        """, unsafe_allow_html=True)

    with tab1:
        st.info("📊 **Weekly yield trends, bin distribution, density/speed heatmaps** — Required: WW range, Form Factor, DID, Facility, Step | Optional: Density, Speed")
        # Fetch button for Module Yield data
        col1, col2 = st.columns([1, 4])
        with col1:
            fetch_button = st.button(
                "Fetch Module Yield Data",
                type="primary",
                use_container_width=True,
                disabled=st.session_state.fetch_in_progress,
                key="fetch_module_yield_btn"
            )
        with col2:
            # Estimate number of calls
            try:
                workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
                total_calls = len(filters["design_ids"]) * len(filters["test_steps"]) * len(filters["form_factors"]) * len(workweeks)
                parallel_batches = (total_calls + 3) // 4
                estimated_time = max(parallel_batches * 0.5, 1)
                st.caption(f"Will fetch {total_calls} queries ({len(filters['design_ids'])} designs × {len(workweeks)} weeks × {len(filters['test_steps'])} steps × {len(filters['form_factors'])} forms). Est. time: ~{estimated_time:.0f} min if not cached.")
            except Exception:
                pass

        if fetch_button:
            st.session_state.fetch_in_progress = True
            logger.info("Fetch button clicked")

            try:
                use_cache = st.session_state.get("use_cache", True)
                if use_cache:
                    st.info("Fetching data... Cached results will be used when available (instant). Fresh queries may take a few minutes.")
                else:
                    st.warning("Fetching fresh data (cache disabled)... This may take several minutes. Please wait.")

                data = fetch_data(filters, use_cache=use_cache)
                st.session_state.data = data
                st.session_state.last_error = None
                st.session_state.last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.last_fetch_filters = str(filters)

                if data.empty:
                    st.error("No data returned. Check your filter parameters or try different workweeks.")
                    logger.warning("Fetch returned empty DataFrame")
                else:
                    st.success(f"Loaded {len(data)} records successfully!")
                    logger.info(f"Fetch successful: {len(data)} records")

            except RuntimeError as e:
                st.session_state.last_error = str(e)
                st.error(f"Failed to fetch data: {e}")
                logger.error("Fetch error: %s", e)
            except Exception as e:
                st.session_state.last_error = str(e)
                st.error(f"Unexpected error: {e}")
                logger.exception("Unexpected fetch error")
            finally:
                st.session_state.fetch_in_progress = False

        # Show last error if any
        if st.session_state.last_error:
            st.error(f"Last error: {st.session_state.last_error}")

        # Display dashboard if data exists
        if not st.session_state.data.empty:
            processor = DataProcessor(st.session_state.data)
            processor = processor.filter_data(
                form_factors=filters["form_factors"],
                steps=filters["test_steps"],
                densities=filters["densities"],
                speeds=filters["speeds"],
            )
            render_dashboard(processor, filters)
        else:
            st.info("Click 'Fetch Data' to load yield data. Note: Each workweek may take 2-5 minutes to fetch.")

    with tab2:
        render_elc_yield_tab(filters)

    with tab3:
        render_pareto_tab(filters)

    with tab4:
        render_fail_viewer_tab(filters)

    with tab5:
        render_machine_trend_tab(filters)


if __name__ == "__main__":
    main()
