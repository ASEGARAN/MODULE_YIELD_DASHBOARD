"""Module Yield Dashboard - Main Streamlit Application."""

import logging
import importlib
import re
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

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
    process_failcrawler_data,
    create_failcrawler_chart,
    create_failcrawler_summary_table,
    create_pareto_summary_html,
    create_weekly_cdpm_table_html,
    process_msn_status_correlation,
    create_msn_status_correlation_chart,
    create_msn_status_ranked_table_html,
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
    create_slice_channel_map_html,
    get_slice_list,
    clear_smt6_cache,
    get_smt6_cache_stats,
    analyze_site_trends,
    create_site_trend_heatmap,
    create_site_trend_summary_html,
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
    get_previous_workweek,
)
from config.settings import Settings

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
    """Get current work week in YYYYWW format."""
    now = datetime.now()
    week = now.isocalendar()[1]
    return f"{now.year}{week:02d}"


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




def setup_page() -> None:
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="Module Yield Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Inject custom CSS
    inject_custom_css()

    # Header with timestamp
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📊 Module Yield Dashboard")
    with col2:
        st.markdown(f"""
        <div style="text-align: right; padding-top: 20px;">
            <span style="color: #888; font-size: 12px;">Last refreshed</span><br>
            <span style="color: #00d4ff; font-size: 14px; font-weight: 500;">{datetime.now().strftime('%H:%M:%S')}</span>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("Weekly yield tracking for SOCAMM/SOCAMM2 modules")


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
    st.markdown("""
    **Machine-level yield tracking** for SMT6 testers at HMFN step.
    Shows yield trend by machine and site-level breakdown.
    """)

    import streamlit.components.v1 as components

    # Fetch buttons in columns
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        fetch_smt6 = st.button(
            "Fetch Machine Data",
            type="primary",
            use_container_width=True,
            key="fetch_smt6_btn"
        )
    with col2:
        clear_cache = st.button(
            "Clear Cache",
            type="secondary",
            use_container_width=True,
            key="clear_smt6_cache_btn",
            help="Clear cached SMT6 data to force fresh fetch"
        )
    with col3:
        try:
            workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
            cache_stats = get_smt6_cache_stats()
            st.caption(f"{len(filters['design_ids'])} DIDs × {len(workweeks)} weeks | Cache: {cache_stats['valid_entries']} entries")
        except Exception:
            st.caption("Configure workweek range in sidebar")

    # Handle cache clear
    if clear_cache:
        count = clear_smt6_cache()
        st.info(f"Cleared {count} cached SMT6 entries. Next fetch will query fresh data.")

    # Fetch machine-level data
    if fetch_smt6:
        try:
            workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
            design_ids = filters.get("design_ids", ["Y63N", "Y6CP", "Y62P"])

            with st.spinner(f"Fetching SMT6 machine data for {len(workweeks)} weeks..."):
                smt6_df = fetch_smt6_yield_data(
                    design_ids=design_ids,
                    workweeks=[str(ww) for ww in workweeks],
                    form_factor="socamm2",
                    max_workers=8
                )

            if not smt6_df.empty:
                st.session_state.smt6_data = smt6_df
                st.session_state.smt6_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success(f"Loaded {len(smt6_df)} SMT6 machine records!")
            else:
                st.warning("No SMT6 machine data returned.")

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
                filtered_site_df = st.session_state.smt6_site_data[
                    st.session_state.smt6_site_data['design_id'].isin(selected_design_ids)
                ]
            else:
                filtered_site_df = st.session_state.smt6_site_data
        else:
            filtered_site_df = pd.DataFrame()

        # =====================================================================
        # MACHINE YIELD CARDS + TREND CHART (Side by Side)
        # =====================================================================
        if not filtered_machine_df.empty or not filtered_site_df.empty:
            # Create 2-column layout: Cards on left, Trend chart on right
            col_cards, col_chart = st.columns([1, 2])

            with col_cards:
                st.markdown("##### 🖥️ Tester Fleet")
                cards_df = filtered_machine_df if not filtered_machine_df.empty else filtered_site_df
                cards_html = create_machine_yield_cards(cards_df, dark_mode=True)
                if cards_html:
                    # Compact fit - reduced padding/margins in HTML
                    num_machines = cards_df['machine_id'].nunique() if 'machine_id' in cards_df.columns else 1
                    cards_per_row = 2  # Fewer cards per row in narrower column
                    rows = (num_machines + cards_per_row - 1) // cards_per_row
                    # Header=35px + each row of cards=215px (compact)
                    card_height = 35 + (rows * 215)
                    components.html(cards_html, height=min(card_height, 500), scrolling=True)

            with col_chart:
                if not filtered_machine_df.empty:
                    st.markdown("##### 📈 Machine Yield Trend")

                    # Chart options in a compact row
                    opt_col1, opt_col2 = st.columns([1, 2])
                    with opt_col1:
                        show_data_labels = st.checkbox(
                            "Data Labels",
                            value=False,
                            key="smt6_show_labels",
                            help="Display yield values on data points"
                        )
                    with opt_col2:
                        # Calculate data range for slider
                        data_min = filtered_machine_df['yield_pct'].min()
                        default_min = max(0, int(data_min - 5))
                        y_axis_min = st.slider(
                            "Y-Axis Start",
                            min_value=0,
                            max_value=95,
                            value=default_min,
                            step=5,
                            key="smt6_y_min",
                            help="Adjust the starting point of Y-axis"
                        )

                    # Use the main dashboard's design_ids filter
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
                        plotly_cdn = 'https://cdn.plot.ly/plotly-2.27.0.min.js'
                        chart_html = fig.to_html(
                            full_html=True,
                            include_plotlyjs=plotly_cdn,
                            config={'displayModeBar': True, 'responsive': True}
                        )
                        components.html(chart_html, height=450, scrolling=False)

            # Machine summary table (full width below)
            if not filtered_machine_df.empty:
                with st.expander("Machine Summary Table", expanded=False):
                    summary_html = create_smt6_summary_table(filtered_machine_df, dark_mode=True)
                    if summary_html:
                        components.html(summary_html, height=350, scrolling=True)

        # =====================================================================
        # SOCKET/SITE ANALYSIS - STREAMLINED VERSION
        # =====================================================================
        st.divider()
        st.markdown("#### 🔌 Socket & Site Analysis")

        # Site data fetch controls
        header_col1, header_col2, header_col3 = st.columns([2, 2, 1])
        with header_col1:
            st.caption("Fetch site-level data to analyze socket health per machine")
        with header_col2:
            site_fetch_mode = st.radio(
                "Site Data Range",
                options=["Latest Week", "Full Range"],
                key="smt6_site_fetch_mode_inline",
                horizontal=True,
                label_visibility="collapsed"
            )
        with header_col3:
            if st.button("📡 Fetch Site Data", key="fetch_site_inline", type="secondary", use_container_width=True):
                st.session_state.smt6_fetch_site_inline = True
                st.session_state.smt6_site_fetch_mode_selected = site_fetch_mode
                st.rerun()

        # Handle inline site fetch
        if st.session_state.get('smt6_fetch_site_inline', False):
            st.session_state.smt6_fetch_site_inline = False
            fetch_mode = st.session_state.get('smt6_site_fetch_mode_selected', 'Latest Week')

            if fetch_mode == "Latest Week":
                wws = [str(filters["end_ww"])]
                spinner_text = "Fetching site data for latest week..."
            else:
                wws = [str(ww) for ww in Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])]
                spinner_text = f"Fetching site data for {len(wws)} weeks..."

            with st.spinner(spinner_text):
                design_ids_for_fetch = filters.get("design_ids", ["Y6CP"])
                form_factor_val = filters.get("form_factor", "socamm2").lower()

                site_df = fetch_smt6_site_data(
                    design_ids=design_ids_for_fetch,
                    workweeks=wws,
                    form_factor=form_factor_val,
                    progress_callback=None
                )
                if not site_df.empty:
                    st.session_state.smt6_site_data = site_df
                    st.session_state.smt6_site_fetch_mode_used = fetch_mode
                    weeks_fetched = site_df['workweek'].nunique()
                    st.success(f"✅ Loaded {len(site_df)} site records ({weeks_fetched} week{'s' if weeks_fetched > 1 else ''})")
                    st.rerun()
                else:
                    st.warning("No site data found")

        # Show site analysis if data is available
        if not filtered_site_df.empty:
            machines = sorted(filtered_site_df['machine_id'].unique())
            site_mode = st.session_state.get("smt6_site_fetch_mode_used", "Latest Week")
            weeks_in_data = filtered_site_df['workweek'].nunique()

            # =====================================================================
            # SINGLE MACHINE FILTER AT TOP
            # =====================================================================
            filter_col1, filter_col2 = st.columns([1, 3])
            with filter_col1:
                selected_machine = st.selectbox(
                    "Filter by Machine",
                    options=["All Machines"] + machines,
                    format_func=lambda x: x.upper() if x != "All Machines" else x,
                    key="smt6_global_machine_filter"
                )

            # Apply machine filter
            if selected_machine != "All Machines":
                analysis_df = filtered_site_df[filtered_site_df['machine_id'] == selected_machine.lower()].copy()
            else:
                analysis_df = filtered_site_df.copy()

            if analysis_df.empty:
                st.warning("No data for selected machine.")
            else:
                # =====================================================================
                # FLEET HEALTH SUMMARY (Always shown)
                # =====================================================================
                st.markdown("##### 📊 Fleet Health Summary")

                # Calculate fleet metrics
                fleet_summary = analysis_df.groupby('machine_id').agg({
                    'uin_adj': 'sum',
                    'upass_adj': 'sum'
                }).reset_index()
                fleet_summary['yield_pct'] = (fleet_summary['upass_adj'] / fleet_summary['uin_adj'] * 100).round(2)

                # Quick fleet stats
                total_machines = fleet_summary['machine_id'].nunique()
                avg_yield = fleet_summary['yield_pct'].mean()
                min_yield = fleet_summary['yield_pct'].min()
                worst_machine = fleet_summary.loc[fleet_summary['yield_pct'].idxmin(), 'machine_id'] if not fleet_summary.empty else "N/A"

                # Fleet metrics in cards
                metric_cols = st.columns(4)
                with metric_cols[0]:
                    st.metric("Machines", total_machines)
                with metric_cols[1]:
                    st.metric("Avg Yield", f"{avg_yield:.2f}%")
                with metric_cols[2]:
                    st.metric("Min Yield", f"{min_yield:.2f}%")
                with metric_cols[3]:
                    st.metric("Worst Machine", worst_machine.upper())

                # =====================================================================
                # SITE TREND ANALYSIS (Only with Full Range data)
                # =====================================================================
                if site_mode == "Full Range" and weeks_in_data > 1:
                    st.markdown("---")
                    st.markdown("##### 📈 Site Trend Analysis")
                    st.caption(f"Analyzing {weeks_in_data} weeks of data")

                    # Get trend analysis
                    trend_df = analyze_site_trends(analysis_df, target_yield=99.0)

                    if not trend_df.empty:
                        trend_counts = trend_df['trend_class'].value_counts()
                        total_sites = len(trend_df)
                        healthy_count = trend_counts.get('STABLE_GOOD', 0) + trend_counts.get('IMPROVING', 0)
                        attention_count = trend_counts.get('DEGRADING', 0) + trend_counts.get('STABLE_BAD', 0)
                        health_pct = (healthy_count / total_sites * 100) if total_sites > 0 else 0

                        # Color-coded trend cards
                        trend_cards_html = f"""
                        <div style="display: flex; gap: 8px; margin: 10px 0 15px 0;">
                            <div style="flex: 1; background: linear-gradient(135deg, #00C853 0%, #00E676 100%); border-radius: 8px; padding: 12px; text-align: center;">
                                <div style="font-size: 24px; font-weight: bold; color: white;">{trend_counts.get('STABLE_GOOD', 0)}</div>
                                <div style="font-size: 11px; color: rgba(255,255,255,0.9);">✅ Stable Good</div>
                            </div>
                            <div style="flex: 1; background: linear-gradient(135deg, #2196F3 0%, #42A5F5 100%); border-radius: 8px; padding: 12px; text-align: center;">
                                <div style="font-size: 24px; font-weight: bold; color: white;">{trend_counts.get('IMPROVING', 0)}</div>
                                <div style="font-size: 11px; color: rgba(255,255,255,0.9);">📈 Improving</div>
                            </div>
                            <div style="flex: 1; background: linear-gradient(135deg, #FF9800 0%, #FFB74D 100%); border-radius: 8px; padding: 12px; text-align: center;">
                                <div style="font-size: 24px; font-weight: bold; color: white;">{trend_counts.get('VOLATILE', 0)}</div>
                                <div style="font-size: 11px; color: rgba(255,255,255,0.9);">⚡ Volatile</div>
                            </div>
                            <div style="flex: 1; background: linear-gradient(135deg, #FF5722 0%, #FF8A65 100%); border-radius: 8px; padding: 12px; text-align: center;">
                                <div style="font-size: 24px; font-weight: bold; color: white;">{trend_counts.get('DEGRADING', 0)}</div>
                                <div style="font-size: 11px; color: rgba(255,255,255,0.9);">📉 Degrading</div>
                            </div>
                            <div style="flex: 1; background: linear-gradient(135deg, #F44336 0%, #E57373 100%); border-radius: 8px; padding: 12px; text-align: center;">
                                <div style="font-size: 24px; font-weight: bold; color: white;">{trend_counts.get('STABLE_BAD', 0)}</div>
                                <div style="font-size: 11px; color: rgba(255,255,255,0.9);">❌ Stable Bad</div>
                            </div>
                        </div>
                        """
                        st.markdown(trend_cards_html, unsafe_allow_html=True)

                        # Performance Insights
                        if health_pct >= 80:
                            health_color, health_icon, health_status = "#00C853", "✅", "Excellent"
                        elif health_pct >= 60:
                            health_color, health_icon, health_status = "#2196F3", "👍", "Good"
                        elif health_pct >= 40:
                            health_color, health_icon, health_status = "#FF9800", "⚠️", "Needs Attention"
                        else:
                            health_color, health_icon, health_status = "#F44336", "🚨", "Critical"

                        attention_sites = trend_df[trend_df['trend_class'].isin(['DEGRADING', 'STABLE_BAD'])].sort_values('avg_yield').head(5)
                        improving_sites = trend_df[trend_df['trend_class'] == 'IMPROVING'].sort_values('avg_yield', ascending=False).head(3)

                        insights_html = f"""
                        <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border-radius: 10px; padding: 15px; margin: 10px 0; border: 1px solid rgba(255,255,255,0.1);">
                            <div style="display: flex; align-items: center; margin-bottom: 12px;">
                                <span style="font-size: 18px; margin-right: 8px;">📊</span>
                                <span style="font-size: 14px; font-weight: bold; color: white;">Performance Insights</span>
                                <span style="margin-left: auto; background: {health_color}; color: white; padding: 4px 10px; border-radius: 15px; font-size: 11px; font-weight: bold;">
                                    {health_icon} {health_status}: {health_pct:.0f}% Healthy
                                </span>
                            </div>
                        """
                        if not attention_sites.empty:
                            insights_html += '<div style="margin-bottom: 10px;"><div style="color: #FF8A65; font-weight: bold; font-size: 12px; margin-bottom: 5px;">⚠️ Sites Needing Attention</div>'
                            for _, row in attention_sites.iterrows():
                                icon = "📉" if row['trend_class'] == 'DEGRADING' else "❌"
                                insights_html += f'<div style="color: #ddd; font-size: 11px; padding: 2px 0;">{icon} <b>{row["site"]}</b> — {row["avg_yield"]:.1f}%</div>'
                            insights_html += '</div>'
                        if not improving_sites.empty:
                            insights_html += '<div><div style="color: #81C784; font-weight: bold; font-size: 12px; margin-bottom: 5px;">🎯 Positive Momentum</div>'
                            for _, row in improving_sites.iterrows():
                                insights_html += f'<div style="color: #ddd; font-size: 11px; padding: 2px 0;">📈 <b>{row["site"]}</b> — {row["avg_yield"]:.1f}%</div>'
                            insights_html += '</div>'
                        insights_html += '</div>'
                        st.markdown(insights_html, unsafe_allow_html=True)

                    # =====================================================================
                    # SITE TREND HEATMAP
                    # =====================================================================
                    with st.expander("🗺️ Site Yield Heatmap (Sites × Weeks)", expanded=True):
                        pivot = analysis_df.pivot_table(index='site', columns='workweek', values='yield_pct', aggfunc='mean')
                        if not pivot.empty:
                            pivot = pivot.reindex(sorted(pivot.columns), axis=1).sort_index()
                            chart_height = max(350, len(pivot.index) * 16 + 100)

                            fig = go.Figure(data=go.Heatmap(
                                z=pivot.values,
                                x=[str(ww) for ww in pivot.columns],
                                y=pivot.index.tolist(),
                                colorscale=[[0, '#dc3545'], [0.3, '#ffc107'], [0.7, '#17a2b8'], [1, '#28a745']],
                                zmin=94, zmax=100,
                                text=[[f"{v:.1f}%" if pd.notna(v) else "-" for v in row] for row in pivot.values],
                                texttemplate="%{text}", textfont={"size": 8},
                                hovertemplate="<b>Site:</b> %{y}<br><b>Week:</b> WW%{x}<br><b>Yield:</b> %{z:.2f}%<extra></extra>",
                                colorbar=dict(title="Yield %", ticksuffix="%")
                            ))
                            fig.update_layout(
                                xaxis_title="Work Week", yaxis_title="Site",
                                xaxis=dict(type='category', tickprefix="WW"),
                                yaxis=dict(autorange='reversed'),
                                height=chart_height, margin=dict(l=80, r=50, t=30, b=50)
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            st.caption(f"Showing {len(pivot.index)} sites × {len(pivot.columns)} weeks")

                    # =====================================================================
                    # SITE PERFORMANCE TABLE
                    # =====================================================================
                    with st.expander("📋 Site Performance Table", expanded=False):
                        if not trend_df.empty:
                            site_summary = analysis_df.groupby('site').agg({
                                'uin_adj': 'sum', 'upass_adj': 'sum', 'workweek': 'nunique', 'machine_id': 'first'
                            }).reset_index()
                            site_summary['yield_pct'] = (site_summary['upass_adj'] / site_summary['uin_adj'] * 100).round(2)
                            site_summary = site_summary.merge(trend_df[['site', 'trend_class', 'avg_yield', 'std_yield']], on='site', how='left')

                            trend_display = {'STABLE_GOOD': '✅ Good', 'IMPROVING': '📈 Up', 'VOLATILE': '⚡ Volatile', 'DEGRADING': '📉 Down', 'STABLE_BAD': '❌ Bad'}
                            site_summary['Trend'] = site_summary['trend_class'].map(trend_display).fillna('—')
                            site_summary = site_summary.rename(columns={'site': 'Site', 'machine_id': 'Machine', 'uin_adj': 'UIN', 'upass_adj': 'UPASS', 'yield_pct': 'Yield %', 'workweek': 'Weeks', 'std_yield': 'Std Dev'})

                            cols = ['Site', 'Machine', 'UIN', 'Yield %', 'Trend', 'Std Dev'] if selected_machine == "All Machines" else ['Site', 'UIN', 'Yield %', 'Trend', 'Std Dev']
                            st.dataframe(site_summary[cols].sort_values('Yield %'), use_container_width=True, hide_index=True,
                                column_config={'Yield %': st.column_config.NumberColumn(format="%.2f%%"), 'Std Dev': st.column_config.NumberColumn(format="%.2f")})

                # =====================================================================
                # SOCKET HEALTH VIEW (Only when specific machine selected)
                # =====================================================================
                if selected_machine != "All Machines":
                    st.markdown("---")
                    with st.expander(f"🔧 Socket Health: {selected_machine.upper()}", expanded=True):
                        ww_options = sorted(analysis_df['workweek'].unique(), reverse=True)
                        ww_label = f"WW{ww_options[0]}" if len(ww_options) == 1 else f"WW{ww_options[-1]}-{ww_options[0]}"

                        grid_html = create_site_grid_html(
                            analysis_df,
                            machine_id=selected_machine,
                            title=f"🔧 {selected_machine.upper()} - Socket Health ({ww_label})",
                            view_mode="socket"
                        )
                        if grid_html:
                            num_sockets = analysis_df['site'].apply(lambda x: re.match(r'S\d+C\d+P(\d+)', x).group(1) if re.match(r'S\d+C\d+P(\d+)', x) else '0').nunique()
                            grid_height = 420 if num_sockets <= 4 else 200 + ((num_sockets + 3) // 4) * 180
                            components.html(grid_html, height=grid_height, scrolling=False)
        else:
            st.info("👆 Click **Fetch Site Data** above to load socket-level data.")

    else:
        st.info("Click 'Fetch Machine Data' to load machine-level yield data, or 'Fetch Site Data' for site-level breakdown.")


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
    st.header("Module ELC Yield")
    st.markdown("""
    **Yield Calculations:**
    - **HMFN**: Hot Module Final Test yield
    - **SLT**: System Level Test yield = HMB1 × QMON
    - **ELC**: End-of-Line yield = HMFN × SLT
    """)

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

    # Fetch button for ELC data
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

        if st.session_state.elc_last_fetch_time:
            st.caption(f"Last fetched: {st.session_state.elc_last_fetch_time}")

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

        # Display order for columns
        display_cols = ["design_id", "form_factor", "density", "speed", "workweek",
                        "HMFN", "HMB1", "QMON", "SLT", "ELC"]
        available_display = [c for c in display_cols if c in elc_df.columns]

        # Data table
        st.subheader("ELC Yield Data")
        st.dataframe(
            elc_df[available_display].sort_values(
                by=["workweek"] if "workweek" in elc_df.columns else available_display[:1],
                ascending=True
            ),
            use_container_width=True,
            hide_index=True
        )

        st.divider()

        # ELC Trend Chart
        if "workweek" in elc_df.columns:
            st.subheader("ELC Yield Trend")

            # Chart options
            col1, col2 = st.columns(2)
            with col1:
                # Option to pin data labels on chart
                show_elc_labels = st.checkbox("Show data labels on chart", value=False, key="elc_trend_show_labels")
            with col2:
                # Y-axis scale adjustment
                y_min = st.slider(
                    "Y-axis minimum (%)",
                    min_value=0,
                    max_value=98,
                    value=90,
                    step=1,
                    key="elc_yaxis_min",
                    help="Adjust to zoom in on data points close together"
                )

            # Create series identifier
            series_cols = [c for c in ["design_id", "form_factor", "density", "speed"] if c in elc_df.columns]
            if series_cols:
                elc_df["series"] = elc_df[series_cols].astype(str).agg("_".join, axis=1)
            else:
                elc_df["series"] = "All"

            # Ensure workweek is in YYYYWW format (remove any WW prefix if present)
            elc_df["workweek"] = elc_df["workweek"].astype(str).str.replace("WW", "")

            # Sort by workweek numerically
            elc_df["ww_sort"] = elc_df["workweek"].astype(int)
            elc_df = elc_df.sort_values("ww_sort")

            # Get sorted workweeks for x-axis ordering
            sorted_workweeks = elc_df["workweek"].unique().tolist()

            # Melt for plotting
            plot_cols = [c for c in ["HMFN", "SLT", "ELC"] if c in elc_df.columns]
            if plot_cols:
                plot_df = elc_df.melt(
                    id_vars=["workweek", "series"],
                    value_vars=plot_cols,
                    var_name="Yield Type",
                    value_name="Yield %"
                )

                # Add text column for labels
                plot_df["label_text"] = plot_df["Yield %"].apply(lambda x: f"{x:.1f}%")

                # Build chart using graph objects for better control
                fig = go.Figure()

                # Color map for yield types
                colors = {"HMFN": "#636EFA", "SLT": "#EF553B", "ELC": "#00CC96"}

                # Group by series and yield type
                for series_name in plot_df["series"].unique():
                    for yield_type in plot_cols:
                        mask = (plot_df["series"] == series_name) & (plot_df["Yield Type"] == yield_type)
                        series_data = plot_df[mask].sort_values("workweek")

                        if series_data.empty:
                            continue

                        # Determine trace mode based on show_labels
                        trace_mode = "lines+markers+text" if show_elc_labels else "lines+markers"

                        # Convert to lists for plotly
                        x_vals = series_data["workweek"].tolist()
                        y_vals = series_data["Yield %"].tolist()
                        text_vals = series_data["label_text"].tolist() if show_elc_labels else None

                        fig.add_trace(
                            go.Scatter(
                                x=x_vals,
                                y=y_vals,
                                mode=trace_mode,
                                name=f"{yield_type} ({series_name})" if series_name != "All" else yield_type,
                                text=text_vals,
                                textposition="top center",
                                textfont=dict(size=9),
                                line=dict(color=colors.get(yield_type, "#636EFA")),
                                marker=dict(size=8),
                                hovertemplate="<b>Work Week:</b> %{x}<br>" +
                                              f"<b>Yield Type:</b> {yield_type}<br>" +
                                              "<b>Yield:</b> %{y:.2f}%<br>" +
                                              f"<b>Series:</b> {series_name}<br>" +
                                              "<extra></extra>",
                            )
                        )

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
                        categoryarray=sorted_workweeks
                    )
                )

                # Add HMFN yield target marker (99% dotted line) - blue neon
                fig.add_trace(
                    go.Scatter(
                        x=[sorted_workweeks[0], sorted_workweeks[-1]],
                        y=[99, 99],
                        mode="lines",
                        name="HMFN Target: 99%",
                        line=dict(color="#00BFFF", width=3, dash="dot"),
                        showlegend=True,
                        hoverinfo="skip",
                    )
                )

                # SLT target schedule by month (Y6CP 7.5Gbps)
                # Format: {(year, month): target_pct}
                slt_target_schedule = {
                    (2025, 12): 96.50,  # Dec'25
                    (2026, 1): 96.50,   # Jan'26
                    (2026, 2): 96.50,   # Feb'26
                    (2026, 3): 97.00,   # Mar'26
                    (2026, 4): 97.00,   # Apr'26
                    (2026, 5): 97.00,   # May'26
                    (2026, 6): 97.00,   # Jun'26 (default forward)
                }

                def get_slt_target(ww_str):
                    """Get SLT target for a given workweek using Micron fiscal calendar."""
                    year, month = get_calendar_year_month(ww_str)
                    # Find target, default to latest known target
                    if (year, month) in slt_target_schedule:
                        return slt_target_schedule[(year, month)]
                    # Default to 97% for future months
                    return 97.00

                # Build stepped SLT target line
                slt_target_x = []
                slt_target_y = []
                for ww in sorted_workweeks:
                    target = get_slt_target(ww)
                    slt_target_x.append(ww)
                    slt_target_y.append(target)

                # Add SLT yield target marker (stepped line) - red neon
                fig.add_trace(
                    go.Scatter(
                        x=slt_target_x,
                        y=slt_target_y,
                        mode="lines",
                        name="SLT Target",
                        line=dict(color="#FF1744", width=3, dash="dot", shape="hv"),
                        showlegend=True,
                        hovertemplate="<b>SLT Target:</b> %{y:.2f}%<extra></extra>",
                    )
                )

                # ELC target schedule by month (C2 Y6CP 7.5Gbps)
                # Format: {(year, month): target_pct}
                elc_target_schedule = {
                    (2025, 12): 95.54,  # Dec'25
                    (2026, 1): 95.54,   # Jan'26
                    (2026, 2): 94.57,   # Feb'26
                    (2026, 3): 96.03,   # Mar'26
                    (2026, 4): 96.03,   # Apr'26
                    (2026, 5): 96.03,   # May'26
                    (2026, 6): 96.03,   # Jun'26 (default forward)
                }

                def get_elc_target(ww_str):
                    """Get ELC target for a given workweek using Micron fiscal calendar."""
                    year, month = get_calendar_year_month(ww_str)
                    # Find target, default to latest known target
                    if (year, month) in elc_target_schedule:
                        return elc_target_schedule[(year, month)]
                    # Default to 96.03% for future months
                    return 96.03

                # Build stepped ELC target line
                elc_target_x = []
                elc_target_y = []
                for ww in sorted_workweeks:
                    target = get_elc_target(ww)
                    elc_target_x.append(ww)
                    elc_target_y.append(target)

                # Add ELC yield target marker (stepped line) - green neon dotted
                fig.add_trace(
                    go.Scatter(
                        x=elc_target_x,
                        y=elc_target_y,
                        mode="lines",
                        name="ELC Target",
                        line=dict(color="#39FF14", width=3, dash="dot", shape="hv"),
                        showlegend=True,
                        hovertemplate="<b>ELC Target:</b> %{y:.2f}%<extra></extra>",
                    )
                )

                # Add Micron fiscal month labels below workweek on x-axis
                tick_labels = get_workweek_labels_with_months(sorted_workweeks)

                fig.update_xaxes(
                    ticktext=tick_labels,
                    tickvals=sorted_workweeks,
                )

                st.plotly_chart(fig, use_container_width=True)

        # Heatmap by density/speed
        if "density" in elc_df.columns and "speed" in elc_df.columns and "ELC" in elc_df.columns:
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

                st.session_state.failcrawler_data = fc_df
                st.session_state.failcrawler_msn_corr_data = msn_corr_df
                st.session_state.failcrawler_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.failcrawler_filters = filters.copy()
                st.success(f"Loaded {len(fc_df):,} FAILCRAWLER records + {len(msn_corr_df):,} MSN_STATUS correlation records!")
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

        for step in steps_to_show:
            data = process_failcrawler_data(fc_df, step, design_id=filter_design_id)
            if data is None:
                continue

            st.subheader(f"📊 {step} FAILCRAWLER cDPM")

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

            # Weekly cDPM table with WoW and anomalies
            with st.expander(f"📈 {step} Weekly cDPM Data", expanded=False):
                weekly_html = create_weekly_cdpm_table_html(data, dark_mode=False)
                if weekly_html:
                    components.html(weekly_html, height=600, scrolling=True)

            # MSN_STATUS Correlation (FAILCRAWLER × MSN_STATUS contribution analysis)
            st.subheader(f"🔗 {step} MSN_STATUS Correlation")
            st.caption("CDPM contribution by MSN_STATUS - ranked by contribution %, not count")

            # Use separate MSN_STATUS correlation data from session state
            msn_corr_df = st.session_state.get('failcrawler_msn_corr_data', pd.DataFrame())
            if msn_corr_df.empty:
                st.info("MSN_STATUS correlation data not loaded. Click 'Fetch Live Data' to load.")
            else:
                correlation_data = process_msn_status_correlation(msn_corr_df, step, design_id=filter_design_id)
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
    end_ww = str(filters.get("end_ww", "202614"))
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

    # Build filter context display
    filter_parts = [
        f"{', '.join([ff.upper() for ff in form_factors])}",
        "HMB1, QMON",
        f"Last 30 days"
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
        with st.spinner("Fetching GRACE motherboard data from mtsums (+fm flag)..."):
            # Don't pass facility if it's "all" or empty
            facility_filter = facility if facility and facility.lower() != "all" else None
            fm_df = fetch_grace_fm_data(
                form_factors=form_factors,
                days=30,
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

        # Work week selector for comparison
        col1, col2 = st.columns(2)
        with col1:
            selected_ww = st.selectbox(
                "Select Work Week (Current)",
                options=available_weeks,
                index=0 if available_weeks else None,
                key="grace_selected_ww"
            )
        with col2:
            # Automatically calculate previous week
            if selected_ww:
                prev_ww = get_previous_workweek(selected_ww)
                st.text_input(
                    "Previous Work Week (Auto)",
                    value=prev_ww,
                    disabled=True,
                    key="grace_prev_ww_display"
                )
            else:
                prev_ww = None

        if selected_ww:
            # ============================================
            # Machines with Hang cDPM > 0
            # ============================================
            st.markdown("#### 🔥 Machines with Hang Failures")

            hang_machines = get_hang_machines(fm_df, selected_ww)

            if not hang_machines.empty:
                st.warning(f"Found **{len(hang_machines)}** machines with Hang cDPM > 0 in WW{selected_ww}")

                # Display hang machines table
                hang_display = hang_machines.copy()
                hang_display['Hang_cDPM'] = hang_display['Hang_cDPM'].apply(lambda x: f"{x:,.0f}")
                hang_display['UIN'] = hang_display['UIN'].apply(lambda x: f"{x:,}")
                hang_display.columns = ['Machine ID', 'Work Week', 'Hang cDPM', 'UIN']

                st.dataframe(
                    hang_display,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Machine ID": st.column_config.TextColumn("Machine ID", width="medium"),
                        "Work Week": st.column_config.NumberColumn("Work Week", format="%d"),
                        "Hang cDPM": st.column_config.TextColumn("Hang cDPM"),
                        "UIN": st.column_config.TextColumn("UIN")
                    }
                )

                # Week-over-week comparison
                if prev_ww and prev_ww in available_weeks:
                    st.markdown("---")
                    st.markdown(f"#### 📊 Week-over-Week Comparison (WW{selected_ww} vs WW{prev_ww})")

                    comparison_df = compare_weeks(fm_df, selected_ww, prev_ww)

                    if not comparison_df.empty:
                        # Summary metrics
                        new_issues = len(comparison_df[comparison_df['is_new']])
                        resolved = len(comparison_df[comparison_df['is_resolved']])
                        chronic = len(comparison_df[comparison_df['is_chronic']])

                        comp_cols = st.columns(4)
                        with comp_cols[0]:
                            st.metric("New Issues", f"{new_issues}", help="Machines with Hang > 0 in current week but not previous")
                        with comp_cols[1]:
                            st.metric("Resolved", f"{resolved}", delta=f"+{resolved}" if resolved > 0 else None, delta_color="normal", help="Machines with Hang > 0 in previous week but not current")
                        with comp_cols[2]:
                            st.metric("Chronic", f"{chronic}", delta_color="inverse", help="Machines with Hang > 0 in both weeks")
                        with comp_cols[3]:
                            st.metric("Total Tracked", f"{len(comparison_df)}")

                        # Comparison table
                        with st.expander("📋 Detailed Comparison Table", expanded=True):
                            comp_display = comparison_df.copy()

                            # Add status column
                            def get_status(row):
                                if row['is_new']:
                                    return "🆕 New"
                                elif row['is_resolved']:
                                    return "✅ Resolved"
                                elif row['is_chronic']:
                                    return "⚠️ Chronic"
                                return "—"

                            comp_display['Status'] = comp_display.apply(get_status, axis=1)

                            # Select and rename columns for display
                            display_cols = ['machine_id', 'Status', f'hang_cDPM_{selected_ww}', f'hang_cDPM_{prev_ww}', 'hang_delta', f'uin_{selected_ww}', f'uin_{prev_ww}']
                            comp_display = comp_display[display_cols].copy()
                            comp_display.columns = ['Machine ID', 'Status', f'Hang WW{selected_ww}', f'Hang WW{prev_ww}', 'Delta', f'UIN WW{selected_ww}', f'UIN WW{prev_ww}']

                            st.dataframe(comp_display, use_container_width=True, hide_index=True)

                # ============================================
                # Drill-down: UIN=4, UPASS=0 Analysis
                # ============================================
                st.markdown("---")
                st.markdown("#### 🔍 Drill-Down: 100% Fail Cases (UIN=4, UPASS=0)")

                # Machine selector for drill-down
                machine_options = hang_machines['MACHINE_ID'].tolist()
                selected_machine = st.selectbox(
                    "Select Machine for Drill-Down",
                    options=machine_options,
                    key="grace_drill_machine"
                )

                if selected_machine:
                    drill_btn = st.button(f"🔎 Analyze {selected_machine}", key="grace_drill_btn")

                    if drill_btn:
                        with st.spinner(f"Running tsums drill-down for {selected_machine}..."):
                            analysis = analyze_hang_failures(
                                machine_id=selected_machine,
                                current_ww=selected_ww,
                                previous_ww=prev_ww if prev_ww else get_previous_workweek(selected_ww),
                                days=30
                            )
                            st.session_state.grace_hang_analysis[selected_machine] = analysis

                    # Display analysis results if available
                    if selected_machine in st.session_state.grace_hang_analysis:
                        analysis = st.session_state.grace_hang_analysis[selected_machine]

                        if 'error' in analysis and analysis['error']:
                            st.error(f"Analysis error: {analysis['error']}")
                        else:
                            # Analysis summary
                            analysis_cols = st.columns(4)
                            with analysis_cols[0]:
                                st.metric(
                                    f"100% Fails WW{analysis.get('current_ww', selected_ww)}",
                                    f"{analysis.get('current_ww_count', 0)}"
                                )
                            with analysis_cols[1]:
                                st.metric(
                                    f"100% Fails WW{analysis.get('previous_ww', prev_ww)}",
                                    f"{analysis.get('previous_ww_count', 0)}"
                                )
                            with analysis_cols[2]:
                                is_chronic = analysis.get('is_chronic', False)
                                st.metric(
                                    "Chronic Issue?",
                                    "YES" if is_chronic else "NO",
                                    delta="⚠️" if is_chronic else None,
                                    delta_color="inverse" if is_chronic else "normal"
                                )
                            with analysis_cols[3]:
                                bios_versions = analysis.get('bios_versions', [])
                                st.metric("BIOS Versions", f"{len(bios_versions)}")

                            # Current week failures
                            current_failures = analysis.get('current_ww_failures', [])
                            if current_failures:
                                st.markdown(f"##### 100% Fail Lots in WW{analysis.get('current_ww', selected_ww)}")
                                current_df = pd.DataFrame(current_failures)
                                st.dataframe(current_df, use_container_width=True, hide_index=True)
                            else:
                                st.info(f"No 100% fail cases found in WW{analysis.get('current_ww', selected_ww)}")

                            # Previous week failures
                            prev_failures = analysis.get('previous_ww_failures', [])
                            if prev_failures:
                                st.markdown(f"##### 100% Fail Lots in WW{analysis.get('previous_ww', prev_ww)}")
                                prev_df = pd.DataFrame(prev_failures)
                                st.dataframe(prev_df, use_container_width=True, hide_index=True)

                            # BIOS versions seen
                            if bios_versions:
                                with st.expander("🔧 BIOS Versions Observed"):
                                    for bv in bios_versions:
                                        st.code(bv)

            else:
                st.success(f"No machines with Hang cDPM > 0 in WW{selected_ww}")

            # ============================================
            # Full FM Data Summary (Collapsible)
            # ============================================
            with st.expander("📋 Full FM Data Summary", expanded=False):
                # Show available columns and sample data
                st.markdown(f"**Available columns:** {', '.join(fm_df.columns.tolist())}")
                st.markdown(f"**Total records:** {len(fm_df):,}")
                st.markdown(f"**Unique machines:** {fm_df['MACHINE_ID'].nunique()}")
                st.markdown(f"**Work weeks:** {', '.join([f'WW{w}' for w in sorted(fm_df['MFG_WORKWEEK'].unique())])}")

                # Sample data
                st.markdown("**Sample data (first 20 rows):**")
                st.dataframe(fm_df.head(20), use_container_width=True, hide_index=True)

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
    st.markdown("""
    Monitor SMT6 tester fleet performance, track machine-level yield trends, and analyze hardware health.
    """)

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
    st.markdown("""
    Visualize raw fail address data from ATE testing. Upload a CSV file with fail addresses
    or generate sample data to explore the viewer.
    """)

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

    # Validate filters
    validation_error = validate_filters(filters)
    if validation_error:
        st.warning(validation_error)
        return

    # Cache controls
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

    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Yield Analysis", "Module ELC Yield", "Pareto Analysis", "Fail Viewer", "Machine Trend Analysis"])

    with tab1:
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
