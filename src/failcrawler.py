"""
FAILCRAWLER DPM Analysis Module
Fetches, processes, and visualizes FAILCRAWLER data for HMFN, HMB1, QMON steps.
"""
import subprocess
import pandas as pd
import plotly.graph_objects as go
from io import StringIO
from datetime import datetime
import os
import logging

from src.fiscal_calendar import get_fiscal_month

logger = logging.getLogger(__name__)

# Color palette for FAILCRAWLER categories - optimized for both light and dark mode
# Using vibrant, high-contrast colors that are visible on any background
FAILCRAWLER_COLORS = {
    'MULTI_BANK_MULTI_DQ': '#3498DB',      # Bright blue
    'SINGLE_BURST_SINGLE_ROW': '#E74C3C',  # Bright red
    'HGDC': '#F39C12',                      # Orange
    'MULTI_HALFBANK_MULTI_DQ': '#9B59B6',  # Purple
    'HANG': '#1ABC9C',                      # Teal
    'SYS_EVEN_BURST_BIT': '#E91E63',       # Pink
    'DB': '#00BCD4',                        # Cyan
    'MULTI_HALFBANK_SINGLE_DQ': '#CDDC39', # Lime
    'SB': '#4CAF50',                        # Green
    'ROW': '#FF9800',                       # Amber
    'SINGLE_BANK_MULTI_DQ': '#795548',     # Brown
    'MULTI_BURST_SINGLE_ROW': '#607D8B',   # Blue grey
    'Other': '#9E9E9E'                      # Grey
}

# Top FAILCRAWLERs to display (rest grouped into Other)
TOP_FAILCRAWLERS = [
    'MULTI_BANK_MULTI_DQ', 'SINGLE_BURST_SINGLE_ROW', 'HGDC', 'HANG',
    'SYS_EVEN_BURST_BIT', 'DB', 'MULTI_HALFBANK_SINGLE_DQ', 'SB',
    'MULTI_HALFBANK_MULTI_DQ', 'ROW', 'SINGLE_BANK_MULTI_DQ',
    'MULTI_BURST_SINGLE_ROW', 'CPU_THERMAL', 'MOD_SYS', 'DECODE', 'Other'
]

TARGET_CDPM = 20

# Mapping for column names from mtsums output
COLUMN_MAPPING = {
    'MFG_WORKWEEK': 'MFG_WORKWEEK',
    'DESIGN_ID': 'DESIGN_ID',
    'STEP': 'STEP',
    'FAILCRAWLER': 'FAILCRAWLER',
    'UIN': 'UIN',
    'UFAIL': 'UFAIL',
    'UPASS': 'UPASS'
}


def sort_workweek(ww):
    """Sort workweek in YYYYWW format."""
    ww_str = str(int(ww))
    year = int(ww_str[:4])
    week = int(ww_str[4:])
    return year * 100 + week


# =============================================================================
# WoW Trend Detection & Alert Thresholds
# =============================================================================

# Alert thresholds for WoW % change (Moderate setting)
ALERT_THRESHOLD_RED = 50      # >50% increase = Red alert
ALERT_THRESHOLD_YELLOW = 25   # >25% increase = Yellow warning
# <=25% = Green (stable/improving)


def calculate_wow_change(current_value: float, previous_value: float) -> dict:
    """
    Calculate week-over-week change and determine alert level.

    Args:
        current_value: Current week's DPM value
        previous_value: Previous week's DPM value

    Returns:
        Dictionary with change %, direction, and alert level
    """
    if previous_value is None or previous_value == 0:
        return {
            'change_pct': None,
            'direction': 'flat',
            'arrow': '―',
            'alert_level': 'none',
            'alert_color': '#9E9E9E'  # Grey
        }

    change_pct = ((current_value - previous_value) / previous_value) * 100

    # Determine direction and arrow
    if change_pct > 1:
        direction = 'up'
        arrow = '↑'
    elif change_pct < -1:
        direction = 'down'
        arrow = '↓'
    else:
        direction = 'flat'
        arrow = '―'

    # Determine alert level (only for increases)
    if change_pct > ALERT_THRESHOLD_RED:
        alert_level = 'red'
        alert_color = '#E74C3C'  # Red
    elif change_pct > ALERT_THRESHOLD_YELLOW:
        alert_level = 'yellow'
        alert_color = '#F39C12'  # Orange/Yellow
    elif change_pct < -10:
        alert_level = 'improving'
        alert_color = '#27AE60'  # Green (improving)
    else:
        alert_level = 'stable'
        alert_color = '#3498DB'  # Blue (stable)

    return {
        'change_pct': round(change_pct, 1),
        'direction': direction,
        'arrow': arrow,
        'alert_level': alert_level,
        'alert_color': alert_color
    }


def calculate_dpm_trend_by_week(
    df: pd.DataFrame,
    step: str,
    metric: str = 'fcdpm'
) -> dict:
    """
    Calculate DPM values per workweek for trend analysis.

    Args:
        df: DataFrame with DPM data (fcdpm_df, cdpm_df, or mdpm_df)
        step: Test step to filter
        metric: Which metric ('fcdpm', 'cdpm', 'mdpm')

    Returns:
        Dictionary with workweeks as keys and DPM values
    """
    if df.empty:
        return {}

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in df.columns else 'STEP'
    if step_col not in df.columns:
        return {}

    step_df = df[df[step_col].str.upper() == step.upper()].copy()
    if step_df.empty or 'MFG_WORKWEEK' not in step_df.columns:
        return {}

    trend_data = {}

    if metric == 'fcdpm':
        # For FCDPM, sum FAILCRAWLER columns per workweek
        metadata_cols = ['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'FCFM', 'QUERY_STEP',
                         'UIN', 'UFAIL', 'UPASS', 'ALL', 'ALL(DPM)', 'UNKNOWN',
                         'MOD_CUSTOM_TEST_FLOW', 'MSN_STATUS', 'VERIFIED', 'FID_STATUS',
                         'MUIN', 'MUFAIL']
        fc_cols = [col for col in step_df.columns if col not in metadata_cols]

        for ww in step_df['MFG_WORKWEEK'].unique():
            ww_df = step_df[step_df['MFG_WORKWEEK'] == ww]
            total_dpm = 0
            for fc_col in fc_cols:
                if fc_col in ww_df.columns:
                    val = pd.to_numeric(ww_df[fc_col], errors='coerce').mean()
                    if pd.notna(val) and val > 0:
                        total_dpm += val
            trend_data[int(ww)] = round(total_dpm, 2)

    elif metric == 'cdpm':
        # For cDPM, calculate (UFAIL / UIN) * 1M per workweek
        for ww in step_df['MFG_WORKWEEK'].unique():
            ww_df = step_df[step_df['MFG_WORKWEEK'] == ww]
            uin = pd.to_numeric(ww_df['UIN'], errors='coerce').sum() if 'UIN' in ww_df.columns else 0
            ufail = pd.to_numeric(ww_df['UFAIL'], errors='coerce').sum() if 'UFAIL' in ww_df.columns else 0
            if uin > 0:
                trend_data[int(ww)] = round((ufail / uin) * 1_000_000, 2)

    elif metric == 'mdpm':
        # For MDPM, calculate (MUFAIL / MUIN) * 1M per workweek
        for ww in step_df['MFG_WORKWEEK'].unique():
            ww_df = step_df[step_df['MFG_WORKWEEK'] == ww]
            muin = pd.to_numeric(ww_df['MUIN'], errors='coerce').sum() if 'MUIN' in ww_df.columns else 0
            mufail = pd.to_numeric(ww_df['MUFAIL'], errors='coerce').sum() if 'MUFAIL' in ww_df.columns else 0
            if muin > 0:
                trend_data[int(ww)] = round((mufail / muin) * 1_000_000, 2)

    return trend_data


def generate_sparkline_svg(
    values: list[float],
    width: int = 80,
    height: int = 20,
    color: str = '#3498DB',
    alert_color: str = None
) -> str:
    """
    Generate an inline SVG sparkline.

    Args:
        values: List of values to plot
        width: SVG width in pixels
        height: SVG height in pixels
        color: Line color
        alert_color: Optional color for the last point (alert indicator)

    Returns:
        SVG string for inline HTML
    """
    if not values or len(values) < 2:
        return ''

    # Normalize values to fit in SVG
    min_val = min(values)
    max_val = max(values)
    val_range = max_val - min_val if max_val != min_val else 1

    # Calculate points
    points = []
    x_step = width / (len(values) - 1)
    for i, val in enumerate(values):
        x = i * x_step
        y = height - ((val - min_val) / val_range * (height - 4) + 2)  # 2px padding
        points.append(f"{x:.1f},{y:.1f}")

    polyline_points = ' '.join(points)

    # Last point for dot
    last_x = (len(values) - 1) * x_step
    last_y = height - ((values[-1] - min_val) / val_range * (height - 4) + 2)
    dot_color = alert_color if alert_color else color

    svg = f'''<svg width="{width}" height="{height}" style="vertical-align: middle;">
        <polyline points="{polyline_points}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linecap="round"/>
        <circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="3" fill="{dot_color}"/>
    </svg>'''

    return svg


def detect_excursions(
    fcdpm_df: pd.DataFrame,
    cdpm_df: pd.DataFrame,
    mdpm_df: pd.DataFrame,
    steps: list[str]
) -> list[dict]:
    """
    Detect WoW excursions across all steps.

    Args:
        fcdpm_df: FAILCRAWLER DPM data
        cdpm_df: Component DPM data
        mdpm_df: Module DPM data
        steps: List of test steps to check

    Returns:
        List of excursion dictionaries with step, metric, change %, alert level
    """
    excursions = []

    for step in steps:
        # Check FCDPM trend
        fcdpm_trend = calculate_dpm_trend_by_week(fcdpm_df, step, 'fcdpm')
        if len(fcdpm_trend) >= 2:
            sorted_wws = sorted(fcdpm_trend.keys())
            current_ww = sorted_wws[-1]
            previous_ww = sorted_wws[-2]
            wow = calculate_wow_change(fcdpm_trend[current_ww], fcdpm_trend[previous_ww])

            if wow['alert_level'] in ['red', 'yellow']:
                excursions.append({
                    'step': step,
                    'metric': 'FCDPM',
                    'current_ww': current_ww,
                    'current_value': fcdpm_trend[current_ww],
                    'previous_value': fcdpm_trend[previous_ww],
                    'change_pct': wow['change_pct'],
                    'alert_level': wow['alert_level'],
                    'alert_color': wow['alert_color']
                })

    return excursions


def create_alert_summary_html(excursions: list[dict], dark_mode: bool = False) -> str:
    """
    Create HTML alert summary banner for excursions.

    Args:
        excursions: List of excursion dictionaries from detect_excursions
        dark_mode: Theme setting

    Returns:
        HTML string for alert banner, or empty string if no excursions
    """
    if not excursions:
        return ''

    bg_color = '#2d2d2d' if dark_mode else '#fff3cd'
    border_color = '#E74C3C' if any(e['alert_level'] == 'red' for e in excursions) else '#F39C12'
    text_color = '#ffffff' if dark_mode else '#856404'

    # Build excursion list
    excursion_items = []
    for exc in sorted(excursions, key=lambda x: abs(x['change_pct']), reverse=True):
        arrow = '↑' if exc['change_pct'] > 0 else '↓'
        color = exc['alert_color']
        excursion_items.append(
            f"<span style='color: {color}; font-weight: bold;'>{exc['step']}</span> "
            f"<span style='color: {color};'>{arrow}{abs(exc['change_pct']):.0f}% WoW</span>"
        )

    excursion_text = ' | '.join(excursion_items)
    icon = '🔴' if any(e['alert_level'] == 'red' for e in excursions) else '🟡'

    html = f'''
    <div style="background-color: {bg_color}; border-left: 4px solid {border_color};
                border-radius: 4px; padding: 10px 14px; margin-bottom: 16px;">
        <span style="font-size: 14px; color: {text_color};">
            {icon} <b>{len(excursions)} excursion{'s' if len(excursions) > 1 else ''} detected:</b>
            {excursion_text}
        </span>
    </div>
    '''

    return html


def calculate_failcrawler_wow_changes(
    fcdpm_df: pd.DataFrame,
    step: str
) -> list[dict]:
    """
    Calculate WoW changes for each FAILCRAWLER category.

    Args:
        fcdpm_df: FAILCRAWLER DPM data (wide format with FC columns)
        step: Test step to filter

    Returns:
        List of dictionaries with FAILCRAWLER name, current/previous values, and change %
    """
    if fcdpm_df.empty:
        return []

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in fcdpm_df.columns else 'STEP'
    if step_col not in fcdpm_df.columns:
        return []

    step_df = fcdpm_df[fcdpm_df[step_col].str.upper() == step.upper()].copy()
    if step_df.empty or 'MFG_WORKWEEK' not in step_df.columns:
        return []

    # Get sorted workweeks
    workweeks = sorted(step_df['MFG_WORKWEEK'].unique())
    if len(workweeks) < 2:
        return []

    current_ww = workweeks[-1]
    previous_ww = workweeks[-2]

    # Get FAILCRAWLER columns (exclude metadata)
    metadata_cols = ['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'FCFM', 'QUERY_STEP',
                     'UIN', 'UFAIL', 'UPASS', 'ALL', 'ALL(DPM)', 'UNKNOWN',
                     'MOD_CUSTOM_TEST_FLOW', 'MSN_STATUS', 'VERIFIED', 'FID_STATUS',
                     'MUIN', 'MUFAIL']
    fc_cols = [col for col in step_df.columns if col not in metadata_cols]

    # Calculate WoW change for each FAILCRAWLER
    changes = []
    current_df = step_df[step_df['MFG_WORKWEEK'] == current_ww]
    previous_df = step_df[step_df['MFG_WORKWEEK'] == previous_ww]

    for fc_col in fc_cols:
        current_val = pd.to_numeric(current_df[fc_col], errors='coerce').mean() if fc_col in current_df.columns else 0
        previous_val = pd.to_numeric(previous_df[fc_col], errors='coerce').mean() if fc_col in previous_df.columns else 0

        # Skip if both are 0 or negligible
        if current_val < 0.1 and previous_val < 0.1:
            continue

        # Calculate change
        if previous_val > 0:
            change_pct = ((current_val - previous_val) / previous_val) * 100
        elif current_val > 0:
            change_pct = 100  # New failure (didn't exist before)
        else:
            continue

        changes.append({
            'failcrawler': fc_col,
            'current_value': round(current_val, 1),
            'previous_value': round(previous_val, 1),
            'change_pct': round(change_pct, 1),
            'current_ww': int(current_ww),
            'previous_ww': int(previous_ww)
        })

    # Sort by change % descending (biggest increases first)
    changes.sort(key=lambda x: x['change_pct'], reverse=True)

    return changes


def create_top_movers_html(
    changes: list[dict],
    step: str,
    threshold: float = 25.0,
    dark_mode: bool = False
) -> str:
    """
    Create HTML for Top Movers section showing FAILCRAWLERs with significant increases.

    Args:
        changes: List from calculate_failcrawler_wow_changes
        step: Test step name
        threshold: Minimum % increase to show (default 25% matches yellow alert)
        dark_mode: Theme setting

    Returns:
        HTML string for Top Movers section, or empty if no significant movers
    """
    # Filter to only increases above threshold
    movers = [c for c in changes if c['change_pct'] >= threshold]

    if not movers:
        return ''

    bg_color = '#2d2d2d' if dark_mode else '#fff8f0'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    border_color = '#E74C3C' if any(m['change_pct'] >= 50 for m in movers) else '#F39C12'

    # Build mover tags
    mover_tags = []
    for m in movers:
        # Color based on severity
        if m['change_pct'] >= 50:
            tag_color = '#E74C3C'  # Red for >50%
        else:
            tag_color = '#F39C12'  # Yellow for 25-50%

        fc_color = FAILCRAWLER_COLORS.get(m['failcrawler'], '#888888')
        mover_tags.append(
            f"<span style='background-color: {fc_color}20; color: {text_color}; "
            f"padding: 4px 8px; border-radius: 4px; margin: 2px; display: inline-block; "
            f"border-left: 3px solid {tag_color};'>"
            f"<span style='color: {fc_color}; font-weight: bold;'>■</span> "
            f"{m['failcrawler']} <span style='color: {tag_color}; font-weight: bold;'>↑+{m['change_pct']:.0f}%</span>"
            f"</span>"
        )

    html = f'''
    <div style="background-color: {bg_color}; border-left: 4px solid {border_color};
                border-radius: 4px; padding: 10px 14px; margin-bottom: 12px;">
        <div style="font-size: 12px; font-weight: bold; color: {text_color}; margin-bottom: 8px;">
            🔥 {step} Top Movers (>{threshold:.0f}% WoW increase)
        </div>
        <div style="display: flex; flex-wrap: wrap; gap: 4px;">
            {''.join(mover_tags)}
        </div>
    </div>
    '''

    return html


def _build_step_specific_cmd(
    step: str,
    design_ids: list[str],
    workweeks: list[str],
    metric_flags: list[str],
    format_cols: list[str]
) -> list[str]:
    """
    Build step-specific mtsums command with proper filters.

    Step-specific filters:
    - HMFN: '-step<>HMB1' (exclude HMB1)
    - HMB1: '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW' and '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW'
    - QMON: '-step<>HMB1' (exclude HMB1)
    - SLT: '-step<>HMB1' (same as QMON pattern)

    Args:
        step: Test step (HMFN, HMB1, QMON, SLT)
        design_ids: List of design IDs
        workweeks: List of workweeks
        metric_flags: List of metric flags (e.g., ['+fidag'], ['+msnag'], ['+fidag', '+fc'])
        format_cols: List of format columns

    Returns:
        List of command arguments
    """
    design_id_str = ','.join(design_ids)
    workweek_str = ','.join([str(ww) for ww in workweeks])
    format_str = ','.join(format_cols)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        f'-DESIGN_ID={design_id_str}',
        f'-mfg_workweek={workweek_str}',
        f'-step={step.lower()}',
        f'-format={format_str}',
    ]

    # Add metric-specific flags
    cmd.extend(metric_flags)

    # Add step-specific filters
    if step.upper() == 'HMB1':
        # HMB1 uses MOD_CUSTOM_TEST_FLOW filters
        cmd.extend([
            '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
            '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW'
        ])
    else:
        # HMFN, QMON, SLT use -step<>HMB1 filter
        cmd.append('-step<>HMB1')

    return cmd


def fetch_cdpm_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch cDPM (Component DPM) data using mtsums +fidag.

    cDPM = (Failing FIDs / Total FID UIN) × 1,000,000
    FID = Package level (not die level)
    SOCAMM/SOCAMM2 has 4 packages per module

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with FID-level aggregate data by step and workweek
    """
    all_results = []

    for step in steps:
        cmd = _build_step_specific_cmd(
            step=step,
            design_ids=design_ids,
            workweeks=workweeks,
            metric_flags=['+fidag'],
            format_cols=['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'VERIFIED', 'FID_STATUS']
        )

        logger.info(f"Fetching cDPM data for {step}...")
        logger.debug(f"Command: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600
            )

            if result.returncode != 0:
                logger.error(f"mtsums cDPM error for {step}: {result.stderr.decode() if result.stderr else 'Unknown'}")
                continue

            output = result.stdout.decode()
            if not output.strip():
                logger.warning(f"mtsums cDPM returned empty output for {step}")
                continue

            df = pd.read_csv(StringIO(output))
            df.columns = [col.upper() for col in df.columns]
            df['QUERY_STEP'] = step.upper()  # Track which step query this came from
            all_results.append(df)
            logger.info(f"Fetched {len(df)} cDPM records for {step}")

        except subprocess.TimeoutExpired:
            logger.error(f"mtsums cDPM timed out for {step}")
        except Exception as e:
            logger.exception(f"Error fetching cDPM for {step}: {e}")

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)
    logger.info(f"Total cDPM records: {len(combined)}")
    return combined


def fetch_mdpm_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch MDPM (Module DPM) data using mtsums +msnag.

    MDPM = (Failing Modules / Total Module UIN) × 1,000,000
    MSN = Module Serial Number (module level)

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with MSN-level aggregate data by step and workweek
    """
    all_results = []

    for step in steps:
        cmd = _build_step_specific_cmd(
            step=step,
            design_ids=design_ids,
            workweeks=workweeks,
            metric_flags=['+msnag'],
            format_cols=['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'VERIFIED', 'MSN_STATUS']
        )

        logger.info(f"Fetching MDPM data for {step}...")
        logger.debug(f"Command: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600
            )

            if result.returncode != 0:
                logger.error(f"mtsums MDPM error for {step}: {result.stderr.decode() if result.stderr else 'Unknown'}")
                continue

            output = result.stdout.decode()
            if not output.strip():
                logger.warning(f"mtsums MDPM returned empty output for {step}")
                continue

            df = pd.read_csv(StringIO(output))
            df.columns = [col.upper() for col in df.columns]
            df['QUERY_STEP'] = step.upper()
            all_results.append(df)
            logger.info(f"Fetched {len(df)} MDPM records for {step}")

        except subprocess.TimeoutExpired:
            logger.error(f"mtsums MDPM timed out for {step}")
        except Exception as e:
            logger.exception(f"Error fetching MDPM for {step}: {e}")

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)
    logger.info(f"Total MDPM records: {len(combined)}")
    return combined


def fetch_fcdpm_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch FCDPM (FAILCRAWLER cDPM) data using mtsums +fidag +fc.

    FCDPM = cDPM broken down by FAILCRAWLER category
    Uses FID-level (package) aggregation with FAILCRAWLER classification

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with FAILCRAWLER cDPM data by step and workweek
    """
    all_results = []

    for step in steps:
        cmd = _build_step_specific_cmd(
            step=step,
            design_ids=design_ids,
            workweeks=workweeks,
            metric_flags=['+fidag', '+fc'],
            format_cols=['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'FCFM']
        )

        logger.info(f"Fetching FCDPM data for {step}...")
        logger.debug(f"Command: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600
            )

            if result.returncode != 0:
                logger.error(f"mtsums FCDPM error for {step}: {result.stderr.decode() if result.stderr else 'Unknown'}")
                continue

            output = result.stdout.decode()
            if not output.strip():
                logger.warning(f"mtsums FCDPM returned empty output for {step}")
                continue

            df = pd.read_csv(StringIO(output))
            df.columns = [col.upper() for col in df.columns]
            df['QUERY_STEP'] = step.upper()
            all_results.append(df)
            logger.info(f"Fetched {len(df)} FCDPM records for {step}")

        except subprocess.TimeoutExpired:
            logger.error(f"mtsums FCDPM timed out for {step}")
        except Exception as e:
            logger.exception(f"Error fetching FCDPM for {step}: {e}")

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)
    logger.info(f"Total FCDPM records: {len(combined)}")
    return combined


def fetch_all_dpm_metrics(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> dict[str, pd.DataFrame]:
    """
    Fetch all three DPM metrics (cDPM, MDPM, FCDPM) in one call.

    Returns dictionary with keys:
    - 'cdpm': Component DPM data (FID/package level)
    - 'mdpm': Module DPM data (MSN/module level)
    - 'fcdpm': FAILCRAWLER cDPM data (FID level by FAILCRAWLER category)

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        Dictionary of DataFrames
    """
    logger.info(f"Fetching all DPM metrics for {len(design_ids)} DIDs × {len(steps)} steps × {len(workweeks)} weeks")

    return {
        'cdpm': fetch_cdpm_data(design_ids, steps, workweeks),
        'mdpm': fetch_mdpm_data(design_ids, steps, workweeks),
        'fcdpm': fetch_fcdpm_data(design_ids, steps, workweeks)
    }


def fetch_fcfm_decode_quality(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch FCFM (Fail Category Fail Mechanism) breakdown for decode quality metrics.

    Returns data showing how much of the FCDPM is:
    - UE (Uncorrectable Error) = Successfully decoded, known fail mechanism
    - ECC = ECC-related failures
    - UNKNOWN = Fail mechanism could not be determined

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with FCFM breakdown by step and workweek
    """
    all_results = []

    for step in steps:
        cmd = _build_step_specific_cmd(
            step=step,
            design_ids=design_ids,
            workweeks=workweeks,
            metric_flags=['+fidag', '+fc'],
            format_cols=['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'FCFM']
        )

        logger.info(f"Fetching FCFM decode quality for {step}...")

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600
            )

            if result.returncode != 0:
                logger.error(f"mtsums FCFM error for {step}: {result.stderr.decode() if result.stderr else 'Unknown'}")
                continue

            output = result.stdout.decode()
            if not output.strip():
                continue

            df = pd.read_csv(StringIO(output))
            df.columns = [col.upper() for col in df.columns]
            df['QUERY_STEP'] = step.upper()
            all_results.append(df)
            logger.info(f"Fetched {len(df)} FCFM records for {step}")

        except subprocess.TimeoutExpired:
            logger.error(f"mtsums FCFM timed out for {step}")
        except Exception as e:
            logger.exception(f"Error fetching FCFM for {step}: {e}")

    if not all_results:
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True)
    logger.info(f"Total FCFM records: {len(combined)}")
    return combined


def calculate_decode_quality(
    fcfm_df: pd.DataFrame,
    step: str,
    workweek: int = None
) -> dict:
    """
    Calculate decode quality metrics from FCFM data.

    Returns percentage breakdown of:
    - UE% = Decoded failures (known fail mechanism)
    - ECC% = ECC-related failures
    - UNKNOWN% = Undecoded failures

    Args:
        fcfm_df: DataFrame from fetch_fcfm_decode_quality
        step: Test step to filter
        workweek: Optional specific workweek (None = all)

    Returns:
        Dictionary with decode quality metrics
    """
    if fcfm_df.empty:
        return None

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in fcfm_df.columns else 'STEP'
    step_df = fcfm_df[fcfm_df[step_col].str.upper() == step.upper()].copy()

    if workweek is not None and 'MFG_WORKWEEK' in step_df.columns:
        step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]

    if step_df.empty or 'FCFM' not in step_df.columns:
        return None

    # Aggregate by FCFM category
    # Use UFAIL column for fail counts, or ALL if available
    fail_col = 'UFAIL' if 'UFAIL' in step_df.columns else 'ALL' if 'ALL' in step_df.columns else None
    uin_col = 'UIN' if 'UIN' in step_df.columns else None

    if fail_col is None:
        return None

    # Convert to numeric
    step_df[fail_col] = pd.to_numeric(step_df[fail_col], errors='coerce').fillna(0)

    fcfm_agg = step_df.groupby('FCFM').agg({fail_col: 'sum'}).reset_index()
    total_fails = fcfm_agg[fail_col].sum()

    if total_fails == 0:
        return None

    # Calculate percentages
    result = {
        'step': step,
        'workweek': workweek,
        'total_fails': float(total_fails),
        'ue_pct': 0.0,
        'ecc_pct': 0.0,
        'unknown_pct': 0.0,
        'ue_fails': 0.0,
        'ecc_fails': 0.0,
        'unknown_fails': 0.0
    }

    for _, row in fcfm_agg.iterrows():
        fcfm = str(row['FCFM']).upper()
        fails = float(row[fail_col])
        pct = round((fails / total_fails) * 100, 1)

        if fcfm == 'UE':
            result['ue_pct'] = pct
            result['ue_fails'] = fails
        elif fcfm == 'ECC':
            result['ecc_pct'] = pct
            result['ecc_fails'] = fails
        elif fcfm == 'UNKNOWN':
            result['unknown_pct'] = pct
            result['unknown_fails'] = fails

    return result


def create_decode_quality_html(decode_data: dict, dark_mode: bool = False) -> str:
    """
    Create HTML indicator for decode quality (UE% vs UNKNOWN%).

    Args:
        decode_data: Dictionary from calculate_decode_quality
        dark_mode: Theme setting

    Returns:
        HTML string for the decode quality indicator
    """
    if decode_data is None:
        return ""

    ue_pct = decode_data.get('ue_pct', 0)
    ecc_pct = decode_data.get('ecc_pct', 0)
    unknown_pct = decode_data.get('unknown_pct', 0)

    # Colors
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    subtext_color = '#aaaaaa' if dark_mode else '#666666'
    bg_color = '#2d2d2d' if dark_mode else '#f8f9fa'

    # Color coding for quality
    if ue_pct >= 80:
        quality_color = '#28a745'  # Green - good decode coverage
        quality_label = 'Good'
    elif ue_pct >= 50:
        quality_color = '#ffc107'  # Yellow - moderate
        quality_label = 'Moderate'
    else:
        quality_color = '#dc3545'  # Red - poor decode coverage
        quality_label = 'Low'

    html = f'''
    <div style="display: inline-flex; align-items: center; gap: 12px; background: {bg_color};
                border-radius: 6px; padding: 6px 12px; font-size: 11px; margin-bottom: 8px;">
        <span style="color: {subtext_color}; font-weight: 500;">Decode Quality:</span>
        <span style="color: {quality_color}; font-weight: bold;">{quality_label}</span>
        <span style="color: #28a745;">UE {ue_pct:.0f}%</span>
        <span style="color: {subtext_color};">|</span>
        <span style="color: #dc3545;">UNKNOWN {unknown_pct:.0f}%</span>
        {f'<span style="color: {subtext_color};">|</span><span style="color: #17a2b8;">ECC {ecc_pct:.0f}%</span>' if ecc_pct > 0 else ''}
    </div>
    '''

    return html


def process_dpm_metrics(
    cdpm_df: pd.DataFrame,
    mdpm_df: pd.DataFrame,
    step: str,
    workweek: int = None
) -> dict:
    """
    Process cDPM and MDPM data for a specific step and optionally workweek.

    Calculates:
    - cDPM: (Total UFAIL / Total UIN) × 1,000,000 at FID level
    - MDPM: (Total MUFAIL / Total MUIN) × 1,000,000 at MSN level

    Args:
        cdpm_df: cDPM DataFrame from fetch_cdpm_data
        mdpm_df: MDPM DataFrame from fetch_mdpm_data
        step: Test step to filter
        workweek: Optional specific workweek (None = all)

    Returns:
        Dictionary with processed metrics
    """
    result = {
        'step': step,
        'workweek': workweek,
        'cdpm': None,
        'mdpm': None,
        'cdpm_by_status': {},
        'mdpm_by_status': {},
        'uin': 0,
        'muin': 0
    }

    # Process cDPM
    if not cdpm_df.empty:
        step_df = cdpm_df[cdpm_df['QUERY_STEP'].str.upper() == step.upper()].copy()
        if workweek is not None:
            step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]

        if not step_df.empty:
            total_uin = step_df['UIN'].sum() if 'UIN' in step_df.columns else 0
            total_ufail = step_df['UFAIL'].sum() if 'UFAIL' in step_df.columns else 0

            result['uin'] = int(total_uin)
            if total_uin > 0:
                result['cdpm'] = round((total_ufail / total_uin) * 1_000_000, 2)

            # cDPM by FID_STATUS
            if 'FID_STATUS' in step_df.columns:
                status_agg = step_df.groupby('FID_STATUS').agg({'UIN': 'sum', 'UFAIL': 'sum'}).reset_index()
                for _, row in status_agg.iterrows():
                    status = row['FID_STATUS']
                    if row['UIN'] > 0 and status != 'Pass':
                        cdpm_val = round((row['UFAIL'] / total_uin) * 1_000_000, 2)
                        result['cdpm_by_status'][status] = {
                            'ufail': int(row['UFAIL']),
                            'uin': int(row['UIN']),
                            'cdpm': cdpm_val
                        }

    # Process MDPM
    if not mdpm_df.empty:
        step_df = mdpm_df[mdpm_df['QUERY_STEP'].str.upper() == step.upper()].copy()
        if workweek is not None:
            step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]

        if not step_df.empty:
            total_muin = step_df['MUIN'].sum() if 'MUIN' in step_df.columns else 0
            total_mufail = step_df['MUFAIL'].sum() if 'MUFAIL' in step_df.columns else 0

            result['muin'] = int(total_muin)
            if total_muin > 0:
                result['mdpm'] = round((total_mufail / total_muin) * 1_000_000, 2)

            # MDPM by MSN_STATUS
            if 'MSN_STATUS' in step_df.columns:
                status_agg = step_df.groupby('MSN_STATUS').agg({'MUIN': 'sum', 'MUFAIL': 'sum'}).reset_index()
                for _, row in status_agg.iterrows():
                    status = row['MSN_STATUS']
                    if row['MUIN'] > 0 and status != 'Pass':
                        mdpm_val = round((row['MUFAIL'] / total_muin) * 1_000_000, 2)
                        result['mdpm_by_status'][status] = {
                            'mufail': int(row['MUFAIL']),
                            'muin': int(row['MUIN']),
                            'mdpm': mdpm_val
                        }

    return result


def fetch_failcrawler_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str],
    cache_dir: str = None
) -> pd.DataFrame:
    """
    Fetch FAILCRAWLER data using mtsums command.

    Args:
        design_ids: List of design IDs (e.g., ['Y6CP', 'Y62P'])
        steps: List of test steps (e.g., ['HMFN', 'HMB1', 'QMON', 'SLT'])
        workweeks: List of workweeks in YYYYWW format
        cache_dir: Directory for caching (optional)

    Returns:
        DataFrame with FAILCRAWLER data
    """
    import hashlib

    # Create cache key based on parameters
    cache_key = hashlib.md5(
        f"{sorted(design_ids)}_{sorted(steps)}_{sorted(workweeks)}".encode()
    ).hexdigest()[:12]

    # Check cache first
    if cache_dir:
        cache_file = os.path.join(cache_dir, f'failcrawler_{cache_key}.csv')
        if os.path.exists(cache_file):
            cache_age = datetime.now().timestamp() - os.path.getmtime(cache_file)
            # Use cache if less than 24 hours old
            if cache_age < 86400:
                logger.info(f"Using cached FAILCRAWLER data from {cache_file}")
                try:
                    return pd.read_csv(cache_file)
                except Exception:
                    pass  # Cache corrupted, fetch fresh

    # Build mtsums command for FAILCRAWLER data
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    # Command to fetch FAILCRAWLER breakdown by workweek
    # Original format WITHOUT MSN_STATUS grouping for correct cDPM values
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        f'-DESIGN_ID={design_id_str}',
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag', '+fc',
        f'-mfg_workweek={workweek_str}',
        '-round=1',
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK',
        f'-step={step_str}',
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
        '-format+=MOD_CUSTOM_TEST_FLOW',
        '+fm'
    ]

    logger.info(f"Fetching FAILCRAWLER data: {len(design_ids)} DIDs, {len(steps)} steps, {len(workweeks)} weeks")
    logger.debug(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600  # 10 minute timeout for large queries
        )

        if result.returncode != 0:
            stderr = result.stderr.decode() if result.stderr else "Unknown error"
            logger.error(f"mtsums error (code {result.returncode}): {stderr}")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            logger.warning("mtsums returned empty output")
            return pd.DataFrame()

        # Parse CSV output
        df = pd.read_csv(StringIO(output))

        if df.empty:
            logger.warning("mtsums returned no data rows")
            return pd.DataFrame()

        # Normalize column names to uppercase
        df.columns = [col.upper() for col in df.columns]

        # Ensure required metadata columns exist (FAILCRAWLER data is in wide format - each FC is a column)
        required_cols = ['MFG_WORKWEEK', 'STEP', 'UIN']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}. Available: {list(df.columns)}")
            return pd.DataFrame()

        logger.info(f"Fetched {len(df)} FAILCRAWLER records (wide format with {len(df.columns)} columns)")

        # Save to cache
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
            df.to_csv(cache_file, index=False)
            logger.info(f"FAILCRAWLER data cached to {cache_file}")

        return df

    except subprocess.TimeoutExpired:
        logger.error("mtsums command timed out after 10 minutes")
        return pd.DataFrame()
    except Exception as e:
        logger.exception(f"Error fetching FAILCRAWLER data: {e}")
        return pd.DataFrame()


def fetch_msn_status_correlation_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch FAILCRAWLER data WITH MSN_STATUS grouping for correlation analysis.

    This is a separate query from the main FAILCRAWLER fetch because adding
    MSN_STATUS to the format changes the DPM calculation (breaks down by MSN_STATUS).

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with FAILCRAWLER data grouped by MSN_STATUS
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        f'-DESIGN_ID={design_id_str}',
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag', '+fc',
        f'-mfg_workweek={workweek_str}',
        '-round=1',
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK,MSN_STATUS',
        f'-step={step_str}',
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
        '-format+=MOD_CUSTOM_TEST_FLOW',
        '-msn_status!=Pass',
        '+fm'
    ]

    logger.info(f"Fetching MSN_STATUS correlation data...")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"mtsums error: {result.stderr.decode() if result.stderr else 'Unknown'}")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [col.upper() for col in df.columns]

        logger.info(f"Fetched {len(df)} MSN_STATUS correlation records")
        return df

    except Exception as e:
        logger.exception(f"Error fetching MSN_STATUS correlation data: {e}")
        return pd.DataFrame()


def fetch_total_uin_by_step(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch total UIN (units tested) per step and workweek.

    Returns both:
    - TOTAL_MUIN: Module-level UIN (for MDPM calculation)
    - TOTAL_UIN: Component-level UIN (for cDPM calculation)

    Uses +fidag which returns both UIN (component) and MUIN (module) per FID_STATUS,
    including Pass status for total population.

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with STEP, MFG_WORKWEEK, TOTAL_MUIN, TOTAL_UIN columns
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    logger.info(f"Fetching total UIN (module + component) per step/workweek...")

    # Use +fidag to get both UIN (component) and MUIN (module) including Pass
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        f'-DESIGN_ID={design_id_str}',
        f'-mfg_workweek={workweek_str}',
        f'-step={step_str}',
        '-format=STEP,MFG_WORKWEEK',
        '+fidag'
    ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"mtsums error: {result.stderr.decode() if result.stderr else 'Unknown'}")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [col.upper() for col in df.columns]

        # Aggregate across all FID_STATUS (including Pass) to get totals
        agg_dict = {}
        if 'MUIN' in df.columns:
            agg_dict['MUIN'] = 'sum'
        if 'UIN' in df.columns:
            agg_dict['UIN'] = 'sum'

        if not agg_dict:
            logger.warning("No UIN or MUIN columns found in mtsums output")
            return pd.DataFrame()

        result_df = df.groupby(['STEP', 'MFG_WORKWEEK'], as_index=False).agg(agg_dict)

        # Rename columns
        rename_map = {}
        if 'MUIN' in result_df.columns:
            rename_map['MUIN'] = 'TOTAL_MUIN'
        if 'UIN' in result_df.columns:
            rename_map['UIN'] = 'TOTAL_UIN'
        result_df = result_df.rename(columns=rename_map)

        # Convert to integers
        if 'TOTAL_MUIN' in result_df.columns:
            result_df['TOTAL_MUIN'] = result_df['TOTAL_MUIN'].round().astype(int)
        if 'TOTAL_UIN' in result_df.columns:
            result_df['TOTAL_UIN'] = result_df['TOTAL_UIN'].round().astype(int)

        logger.info(f"Fetched UIN for {len(result_df)} step/workweek combinations")
        return result_df

    except Exception as e:
        logger.exception(f"Error fetching total UIN: {e}")
        return pd.DataFrame()


def fetch_msn_status_fid_counts(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str]
) -> pd.DataFrame:
    """
    Fetch FID-level data to count unique failing modules per MSN_STATUS.

    This query fetches individual FID records and counts distinct FIDs
    per MSN_STATUS for accurate unique failing module counts.

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks

    Returns:
        DataFrame with unique FID counts per STEP and MSN_STATUS
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        f'-DESIGN_ID={design_id_str}',
        f'-mfg_workweek={workweek_str}',
        f'-step={step_str}',
        '-format=STEP,MFG_WORKWEEK,MSN_STATUS,MSN,FID',
        '-msn_status!=Pass',
        '+fid'
    ]

    logger.info(f"Fetching FID-level data for unique module counts...")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"mtsums error: {result.stderr.decode() if result.stderr else 'Unknown'}")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [col.upper() for col in df.columns]

        # Return raw MSN and FID data for flexible aggregation
        # MSN = Module Serial Number (module-level identifier)
        # FID = Component identifier (die-level)
        # Aggregation will be done in app.py based on time range selection
        logger.info(f"Fetched {len(df)} records for unique module/FID counting")
        return df[['STEP', 'MFG_WORKWEEK', 'MSN_STATUS', 'MSN', 'FID']]

    except Exception as e:
        logger.exception(f"Error fetching FID counts: {e}")
        return pd.DataFrame()


def process_failcrawler_data(df: pd.DataFrame, step: str, design_id: str = None) -> dict:
    """
    Process raw FAILCRAWLER data into chart-ready format for a specific step.

    Handles WIDE format from mtsums where each FAILCRAWLER is a column with DPM values.

    Args:
        df: Raw DataFrame from mtsums (wide format with FAILCRAWLER columns)
        step: Test step (HMFN, HMB1, QMON, SLT)
        design_id: Optional Design ID to filter (None = cumulative across all DIDs)

    Returns:
        Dictionary with processed data for charting
    """
    if df.empty:
        return None

    # Make a copy to avoid modifying original
    df = df.copy()

    # Normalize column names
    df.columns = [col.upper() for col in df.columns]

    # Filter to specific step (exclude Total row)
    step_df = df[
        (df['STEP'].str.upper() == step.upper()) &
        (df['STEPTYPE'].str.upper() != 'TOTAL')
    ].copy()

    # Filter by design_id if specified
    if design_id is not None and 'DESIGN_ID' in step_df.columns:
        step_df = step_df[step_df['DESIGN_ID'] == design_id].copy()

    if step_df.empty:
        return None

    # Sort workweeks
    workweeks = sorted(step_df['MFG_WORKWEEK'].unique(), key=sort_workweek)

    # Create fiscal labels
    fiscal_labels = []
    prev_month = None
    for ww in workweeks:
        month = get_fiscal_month(str(int(ww)))
        if month != prev_month:
            fiscal_labels.append(f"{int(ww)}<br> <br><b>{month}</b>")
            prev_month = month
        else:
            fiscal_labels.append(str(int(ww)))

    # Get volume per workweek
    volume_by_ww = step_df.groupby('MFG_WORKWEEK')['UIN'].sum()

    # Identify FAILCRAWLER columns (exclude metadata columns)
    metadata_cols = ['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'MOD_CUSTOM_TEST_FLOW',
                     'MSN_STATUS', 'ALL(DPM)', 'ALL', 'UIN', 'UFAIL', 'UPASS', 'UNKNOWN']
    fc_columns = [col for col in step_df.columns if col not in metadata_cols and col != 'UNKNOWN']

    if not fc_columns:
        return None

    # Aggregate DPM by workweek (sum across design IDs)
    # The data is already in DPM format per the mtsums output
    agg_dict = {col: 'mean' for col in fc_columns}  # Average DPM across design IDs
    agg_dict['UIN'] = 'sum'  # Sum volume
    pivot_dpm = step_df.groupby('MFG_WORKWEEK').agg(agg_dict)
    pivot_dpm = pivot_dpm.reindex(workweeks).fillna(0)

    # Extract volume and remove from pivot
    volume_series = pivot_dpm['UIN']
    pivot_dpm = pivot_dpm.drop(columns=['UIN'])

    # Identify main FAILCRAWLERs (those in TOP_FAILCRAWLERS list)
    main_fcs = [fc for fc in TOP_FAILCRAWLERS if fc != 'Other' and fc in pivot_dpm.columns]
    other_fcs = [fc for fc in pivot_dpm.columns if fc not in main_fcs and fc not in ['UNKNOWN']]

    # Group minor FAILCRAWLERs into Other
    if other_fcs:
        pivot_dpm['Other'] = pivot_dpm[other_fcs].sum(axis=1)
        # Drop the individual other columns
        pivot_dpm = pivot_dpm.drop(columns=other_fcs)

    # Calculate total DPM
    dpm_cols = [fc for fc in main_fcs if fc in pivot_dpm.columns]
    if 'Other' in pivot_dpm.columns:
        dpm_cols.append('Other')
    total_dpm = pivot_dpm[dpm_cols].sum(axis=1)

    # Calculate 4-week rolling average
    rolling_avg = total_dpm.rolling(window=4, min_periods=1).mean()

    return {
        'step': step,
        'workweeks': workweeks,
        'fiscal_labels': fiscal_labels,
        'pivot_dpm': pivot_dpm,
        'total_dpm': total_dpm,
        'rolling_avg': rolling_avg,
        'volume': volume_series,
        'main_fcs': [fc for fc in main_fcs if fc in pivot_dpm.columns] + (['Other'] if other_fcs else [])
    }


def create_failcrawler_chart(data: dict, design_id: str = None, dark_mode: bool = True, show_data_labels: bool = False) -> go.Figure:
    """
    Create a FAILCRAWLER cDPM stacked bar chart with volume on secondary y-axis.

    Args:
        data: Processed data dictionary from process_failcrawler_data
        design_id: Design ID for title (optional)
        dark_mode: If True, use colors compatible with dark backgrounds
        show_data_labels: If True, show data labels on chart

    Returns:
        Plotly Figure object
    """
    if data is None:
        return None

    step_name = data['step']
    workweeks = data['workweeks']
    fiscal_labels = data['fiscal_labels']
    pivot_dpm = data['pivot_dpm']
    total_dpm = data['total_dpm']
    rolling_avg = data['rolling_avg']
    volume = data['volume']
    main_fcs = data['main_fcs']

    # Theme-adaptive colors - high contrast for both light and dark modes
    if dark_mode:
        total_line_color = '#FFFFFF'
        volume_color = 'rgba(100, 100, 100, 0.3)'
        font_color = '#E0E0E0'
        grid_color = 'rgba(255,255,255,0.15)'
        paper_bg = 'rgba(0,0,0,0)'
        plot_bg = 'rgba(30,30,30,0.5)'
        legend_bg = 'rgba(40,40,40,0.9)'
        target_color = '#FF6B6B'
        rolling_color = '#00E5FF'
        title_color = '#FFFFFF'
    else:
        # Light mode - darker colors for better visibility on white background
        total_line_color = '#1a1a2e'
        volume_color = 'rgba(180, 180, 180, 0.4)'
        font_color = '#1a1a1a'  # Very dark for visibility
        grid_color = 'rgba(0,0,0,0.12)'
        paper_bg = '#FFFFFF'
        plot_bg = '#FFFFFF'
        legend_bg = 'rgba(255,255,255,0.98)'
        target_color = '#C0392B'  # Darker red
        rolling_color = '#0097A7'  # Darker cyan
        title_color = '#1a1a1a'

    # Create figure
    fig = go.Figure()

    # Add stacked bars for each FAILCRAWLER on primary y-axis FIRST
    fcs_to_plot = [fc for fc in TOP_FAILCRAWLERS if fc in main_fcs]
    for fc in fcs_to_plot:
        if fc in pivot_dpm.columns:
            y_vals = [float(v) if pd.notna(v) else 0.0 for v in pivot_dpm[fc]]
            fig.add_trace(
                go.Bar(
                    x=fiscal_labels,
                    y=y_vals,
                    name=fc,
                    marker=dict(color=FAILCRAWLER_COLORS.get(fc, '#888888')),
                    yaxis='y'
                )
            )

    # Add target line
    fig.add_trace(
        go.Scatter(
            x=fiscal_labels,
            y=[TARGET_CDPM] * len(fiscal_labels),
            name=f'Target ({TARGET_CDPM} cDPM)',
            mode='lines',
            line=dict(color=target_color, width=2, dash='dash'),
            yaxis='y'
        )
    )

    # Add total FCcDPM line with optional data labels
    fig.add_trace(
        go.Scatter(
            x=fiscal_labels,
            y=total_dpm.tolist(),
            name='Total FCcDPM',
            mode='lines+markers+text' if show_data_labels else 'lines+markers',
            text=[f'{v:.0f}' for v in total_dpm] if show_data_labels else None,
            textposition='top center',
            textfont=dict(size=9, color=font_color),
            line=dict(color=total_line_color, width=3),
            marker=dict(size=8, color=total_line_color),
            yaxis='y'
        )
    )

    # Add 4-week rolling average
    fig.add_trace(
        go.Scatter(
            x=fiscal_labels,
            y=rolling_avg.tolist(),
            name='4-Wk Rolling Avg',
            mode='lines',
            line=dict(color=rolling_color, width=2, dash='dot'),
            yaxis='y'
        )
    )

    # Add volume bars LAST (on secondary y-axis)
    fig.add_trace(
        go.Bar(
            x=fiscal_labels,
            y=volume.tolist(),
            name='Volume',
            marker=dict(color=volume_color),
            yaxis='y2',
            opacity=0.4
        )
    )

    # Build title
    title_parts = ['<b>FAILCRAWLER cDPM']
    if design_id:
        title_parts.append(f'- {design_id}')
    title_parts.append(f'- {step_name}</b>')
    title = ' '.join(title_parts)

    # Update layout - use autorange for y-axis (no fixed cap)
    # Optimized font sizes for better readability
    fig.update_layout(
        title=dict(text=title, font=dict(color=font_color, size=18)),
        barmode='stack',
        height=550,
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        font=dict(color=font_color, family='-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif', size=13),
        legend=dict(
            orientation='v',
            yanchor='top',
            y=0.98,
            xanchor='left',
            x=1.02,
            font=dict(color=font_color, size=13),
            bgcolor=legend_bg,
            bordercolor='rgba(0,0,0,0.2)' if not dark_mode else 'rgba(128,128,128,0.3)',
            borderwidth=1
        ),
        margin=dict(l=70, r=200, t=70, b=90),
        xaxis=dict(
            type='category',
            tickangle=-45,
            tickfont=dict(color=font_color, size=12),
            gridcolor=grid_color,
            title=dict(text='Work Week', font=dict(color=font_color, size=14))
        ),
        yaxis=dict(
            title=dict(text='cDPM (Defects Per Million)', font=dict(color=font_color, size=14)),
            autorange=True,
            rangemode='tozero',
            side='left',
            tickfont=dict(color=font_color, size=12),
            gridcolor=grid_color
        ),
        yaxis2=dict(
            title=dict(text='Volume (UIN)', font=dict(color=font_color, size=14)),
            overlaying='y',
            side='right',
            showgrid=False,
            tickfont=dict(color=font_color, size=12)
        ),
        hovermode='x unified'
    )

    return fig


def create_failcrawler_summary_table(data: dict) -> pd.DataFrame:
    """
    Create a summary table of FAILCRAWLER data.

    Args:
        data: Processed data dictionary

    Returns:
        DataFrame with summary statistics
    """
    if data is None:
        return pd.DataFrame()

    pivot_dpm = data['pivot_dpm']
    main_fcs = [fc for fc in TOP_FAILCRAWLERS if fc in data['main_fcs']]

    # Calculate totals per FAILCRAWLER
    summary = []
    for fc in main_fcs:
        if fc in pivot_dpm.columns:
            total = pivot_dpm[fc].sum()
            avg = pivot_dpm[fc].mean()
            max_val = pivot_dpm[fc].max()
            summary.append({
                'FAILCRAWLER': fc,
                'Total cDPM': total,
                'Avg cDPM': avg,
                'Max cDPM': max_val
            })

    df = pd.DataFrame(summary)
    if not df.empty:
        df = df.sort_values('Total cDPM', ascending=False)

        # Add percentage of total
        total_cdpm = df['Total cDPM'].sum()
        if total_cdpm > 0:
            df['% of Total'] = (df['Total cDPM'] / total_cdpm * 100).round(1)
            df['Cumulative %'] = df['% of Total'].cumsum().round(1)

    return df


def create_pareto_summary_html(data: dict, dark_mode: bool = True) -> str:
    """
    Create HTML table for FAILCRAWLER Pareto Summary (80/20 Analysis).

    Args:
        data: Processed data dictionary from process_failcrawler_data
        dark_mode: If True, use colors compatible with dark backgrounds

    Returns:
        HTML string for the Pareto Summary table
    """
    if data is None:
        return ""

    pivot_dpm = data['pivot_dpm']
    main_fcs = [fc for fc in TOP_FAILCRAWLERS if fc in data['main_fcs']]

    # Calculate totals per FAILCRAWLER
    summary = []
    for fc in main_fcs:
        if fc in pivot_dpm.columns:
            total = pivot_dpm[fc].sum()
            summary.append({
                'FAILCRAWLER': fc,
                'Total cDPM': total,
                'Color': FAILCRAWLER_COLORS.get(fc, '#888888')
            })

    # Sort by total descending
    summary = sorted(summary, key=lambda x: x['Total cDPM'], reverse=True)

    # Calculate percentages
    total_cdpm = sum(s['Total cDPM'] for s in summary)
    cumulative = 0.0
    for s in summary:
        if total_cdpm > 0:
            s['% of Total'] = (s['Total cDPM'] / total_cdpm * 100)
            cumulative += s['% of Total']
            s['Cumulative %'] = cumulative
        else:
            s['% of Total'] = 0.0
            s['Cumulative %'] = 0.0

    # Style colors based on mode - darker text for light mode visibility
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#3d3d3d' if dark_mode else '#e8e8e8'
    border_color = '#555555' if dark_mode else '#cccccc'
    highlight_80 = '#4a1a1a' if dark_mode else '#ffe0e0'  # Highlight for top 80% contributors

    # Build HTML
    html = f'''
    <div style="margin-bottom: 20px;">
        <h3 style="color: {text_color}; margin-bottom: 10px;">FAILCRAWLER Pareto Summary (80/20 Analysis)</h3>
        <table style="border-collapse: collapse; width: 80%; font-size: 12px; font-family: Arial, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: left; color: {text_color};">Rank</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: left; color: {text_color};">FAILCRAWLER</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Total cDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">% of Total</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Cumulative %</th>
                </tr>
            </thead>
            <tbody>
    '''

    for i, s in enumerate(summary, 1):
        # Highlight rows that contribute to 80% of total
        row_style = f'background-color: {highlight_80};' if s['Cumulative %'] <= 80 or (i == 1) else ''
        html += f'''
            <tr style="{row_style}">
                <td style="border: 1px solid {border_color}; padding: 8px; color: {text_color};">{i}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; color: {text_color};"><span style="color:{s['Color']};">■</span> {s['FAILCRAWLER']}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{s['Total cDPM']:,.1f}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{s['% of Total']:.1f}%</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{s['Cumulative %']:.1f}%</td>
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        <p style="font-size: 11px; color: {'#aaaaaa' if dark_mode else '#666666'}; margin-top: 5px;">Red rows = Top FAILCRAWLERs contributing to 80% of total cDPM (focus areas)</p>
    </div>
    '''

    return html


def create_weekly_cdpm_table_html(data: dict, dark_mode: bool = True) -> str:
    """
    Create HTML table for Weekly cDPM Data with WoW change and anomaly highlighting.

    Args:
        data: Processed data dictionary from process_failcrawler_data
        dark_mode: If True, use colors compatible with dark backgrounds

    Returns:
        HTML string for the Weekly cDPM table
    """
    if data is None:
        return ""

    workweeks = data['workweeks']
    pivot_dpm = data['pivot_dpm']
    total_dpm = data['total_dpm']
    volume = data['volume']
    main_fcs = [fc for fc in TOP_FAILCRAWLERS if fc in data['main_fcs']]

    # Style colors based on mode - darker text for light mode visibility
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#3d3d3d' if dark_mode else '#e8e8e8'
    border_color = '#555555' if dark_mode else '#cccccc'
    anomaly_bg = '#5c4a00' if dark_mode else '#FFF3CD'  # Yellow for anomalies
    up_color = '#CC0000' if not dark_mode else '#FF4444'  # Darker red for light mode
    down_color = '#228B22' if not dark_mode else '#44AA44'  # Darker green for light mode

    # Build header row with FAILCRAWLER columns
    fc_headers = ''
    for fc in main_fcs:
        color = FAILCRAWLER_COLORS.get(fc, '#888888')
        fc_headers += f'<th style="border: 1px solid {border_color}; padding: 8px; text-align: right; background-color: {color}20; color: {text_color};">{fc}</th>'

    html = f'''
    <div style="margin-bottom: 20px;">
        <h3 style="color: {text_color}; margin-bottom: 10px;">Weekly cDPM Data</h3>
        <div style="overflow-x: auto;">
        <table style="border-collapse: collapse; width: 100%; font-size: 12px; font-family: Arial, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: left; color: {text_color};">Workweek</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Volume</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Total FCcDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: center; color: {text_color};">WoW</th>
                    {fc_headers}
                </tr>
            </thead>
            <tbody>
    '''

    # Reverse workweeks so most recent is first
    reversed_wws = list(reversed(workweeks))

    for i, ww in enumerate(reversed_wws):
        total = total_dpm.get(ww, 0)
        vol = volume.get(ww, 0)

        # Calculate WoW change
        if i < len(reversed_wws) - 1:
            prev_ww = reversed_wws[i + 1]
            prev_total = total_dpm.get(prev_ww, 0)
            if prev_total > 0:
                wow_pct = ((total - prev_total) / prev_total) * 100
                if wow_pct > 0:
                    wow_str = f'<span style="color:{up_color};">▲{abs(wow_pct):.0f}%</span>'
                elif wow_pct < 0:
                    wow_str = f'<span style="color:{down_color};">▼{abs(wow_pct):.0f}%</span>'
                else:
                    wow_str = '-'
            else:
                if total > 0:
                    wow_str = f'<span style="color:{up_color};">▲inf%</span>'
                else:
                    wow_str = '-'
        else:
            wow_str = '-'

        # Highlight anomalies (>500% increase or total > 500 cDPM)
        is_anomaly = (total > 500) or (i < len(reversed_wws) - 1 and
                     total_dpm.get(reversed_wws[i + 1], 0) > 0 and
                     total / max(total_dpm.get(reversed_wws[i + 1], 0), 0.001) > 5)
        row_style = f'background-color: {anomaly_bg};' if is_anomaly else ''

        # Build FAILCRAWLER values
        fc_values = ''
        for fc in main_fcs:
            val = pivot_dpm[fc].get(ww, 0) if fc in pivot_dpm.columns else 0
            fc_values += f'<td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{val:.1f}</td>'

        html += f'''
            <tr style="{row_style}">
                <td style="border: 1px solid {border_color}; padding: 8px; font-weight: bold; color: {text_color};">{int(ww)}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{int(vol):,}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{total:.1f}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: center;">{wow_str}</td>
                {fc_values}
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        </div>
        <p style="font-size: 11px; color: {'#aaaaaa' if dark_mode else '#666666'}; margin-top: 5px;">Yellow rows = Anomalies (>500 cDPM or >500% WoW increase)</p>
    </div>
    '''

    return html


def process_msn_status_correlation(
    df: pd.DataFrame,
    step: str,
    design_id: str = None,
    fid_counts: pd.DataFrame = None,
    total_muin: int = None,
    total_uin: int = None
) -> dict:
    """
    Process FAILCRAWLER data to compute MSN_STATUS correlation.

    Calculates both MDPM and cDPM by MSN_STATUS:
    - MDPM = (Unique Failed Modules / Total MUIN) × 1,000,000
    - cDPM = (Unique Failed FIDs / Total Component UIN) × 1,000,000

    Following mtsums best practices:
    - Rank by contribution %, not raw count
    - Flag low-volume populations
    - Exclude Mod-Sys, ModOnly, NoFA, Multi-Mod

    Args:
        df: Raw DataFrame from mtsums (wide format with FAILCRAWLER columns, grouped by MSN_STATUS)
        step: Test step (HMFN, HMB1, QMON)
        design_id: Optional Design ID to filter
        fid_counts: Optional DataFrame with unique module and FID counts per MSN_STATUS
        total_muin: Total modules tested (for MDPM denominator)
        total_uin: Total components tested (for cDPM denominator)

    Returns:
        Dictionary with correlation data for visualization
    """
    if df.empty:
        return None

    df = df.copy()
    df.columns = [col.upper() for col in df.columns]

    # Check if MSN_STATUS column exists
    if 'MSN_STATUS' not in df.columns:
        logger.warning("MSN_STATUS column not found in data - correlation not available")
        return None

    # Filter to specific step (exclude Total row)
    step_df = df[
        (df['STEP'].str.upper() == step.upper()) &
        (df['STEPTYPE'].str.upper() != 'TOTAL')
    ].copy()

    # Filter by design_id if specified
    if design_id is not None and 'DESIGN_ID' in step_df.columns:
        step_df = step_df[step_df['DESIGN_ID'] == design_id].copy()

    if step_df.empty:
        return None

    # Identify FAILCRAWLER columns (exclude metadata columns)
    metadata_cols = ['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'MOD_CUSTOM_TEST_FLOW',
                     'MSN_STATUS', 'ALL(DPM)', 'ALL', 'UIN', 'UFAIL', 'UPASS', 'UNKNOWN']
    fc_columns = [col for col in step_df.columns if col not in metadata_cols and col != 'UNKNOWN']

    if not fc_columns:
        return None

    # Filter out excluded MSN_STATUS values (should already be filtered by mtsums, but double-check)
    # Exclude only Pass (not a failure)
    step_df = step_df[step_df['MSN_STATUS'].str.upper() != 'PASS']

    if step_df.empty:
        return None

    # Aggregate by MSN_STATUS using fail counts (ALL column), not DPM
    # DPM values cannot be summed - they are rates, not counts
    # Use ALL (fail count) and UIN for proper calculation
    agg_dict = {'UIN': 'sum'}
    if 'ALL' in step_df.columns:
        agg_dict['ALL'] = 'sum'
    msn_status_agg = step_df.groupby('MSN_STATUS').agg(agg_dict).reset_index()

    # Store component fail count from ALL column (for contribution % calculation)
    if 'ALL' in msn_status_agg.columns:
        msn_status_agg['TOTAL_FAILS'] = msn_status_agg['ALL']
    else:
        msn_status_agg['TOTAL_FAILS'] = 0

    # Calculate grand total fails
    grand_total_fails = msn_status_agg['TOTAL_FAILS'].sum()

    if grand_total_fails == 0:
        return None

    # Calculate contribution percentage based on fail count (not DPM)
    msn_status_agg['CONTRIBUTION_PCT'] = (msn_status_agg['TOTAL_FAILS'] / grand_total_fails * 100).round(2)

    # Sort by fail count contribution (descending)
    msn_status_agg = msn_status_agg.sort_values('TOTAL_FAILS', ascending=False)

    # Calculate cumulative percentage
    msn_status_agg['CUMULATIVE_PCT'] = msn_status_agg['CONTRIBUTION_PCT'].cumsum().round(2)

    # Flag low-volume populations (UIN < 100)
    msn_status_agg['LOW_VOLUME'] = msn_status_agg['UIN'] < 100

    # Merge unique module and FID counts if provided
    if fid_counts is not None and not fid_counts.empty:
        step_fid_counts = fid_counts[fid_counts['STEP'].str.upper() == step.upper()].copy()
        if not step_fid_counts.empty:
            # Determine which columns are available
            merge_cols = ['MSN_STATUS']
            if 'UNIQUE_MODULES' in step_fid_counts.columns:
                merge_cols.append('UNIQUE_MODULES')
            if 'UNIQUE_FIDS' in step_fid_counts.columns:
                merge_cols.append('UNIQUE_FIDS')

            msn_status_agg = msn_status_agg.merge(
                step_fid_counts[merge_cols],
                on='MSN_STATUS',
                how='left'
            )
            if 'UNIQUE_MODULES' in msn_status_agg.columns:
                msn_status_agg['UNIQUE_MODULES'] = msn_status_agg['UNIQUE_MODULES'].fillna(0).astype(int)
            else:
                msn_status_agg['UNIQUE_MODULES'] = 0
            if 'UNIQUE_FIDS' in msn_status_agg.columns:
                msn_status_agg['UNIQUE_FIDS'] = msn_status_agg['UNIQUE_FIDS'].fillna(0).astype(int)
            else:
                msn_status_agg['UNIQUE_FIDS'] = 0
        else:
            msn_status_agg['UNIQUE_MODULES'] = 0
            msn_status_agg['UNIQUE_FIDS'] = 0
    else:
        msn_status_agg['UNIQUE_MODULES'] = 0
        msn_status_agg['UNIQUE_FIDS'] = 0

    # Calculate MDPM: (Unique Failed Modules / Total MUIN) × 1,000,000
    if total_muin is not None and total_muin > 0:
        msn_status_agg['MDPM'] = (msn_status_agg['UNIQUE_MODULES'] / total_muin * 1_000_000).round(2)
    else:
        msn_status_agg['MDPM'] = 0.0

    # Calculate cDPM: (Unique Failed FIDs / Total Component UIN) × 1,000,000
    if total_uin is not None and total_uin > 0:
        msn_status_agg['CDPM'] = (msn_status_agg['UNIQUE_FIDS'] / total_uin * 1_000_000).round(2)
    else:
        msn_status_agg['CDPM'] = 0.0

    # Build correlation matrix (FAILCRAWLER × MSN_STATUS) from original step_df
    # Use COLUMN-WISE percentages: For each MSN_STATUS, what % came from each FAILCRAWLER
    # Note: Each MSN_STATUS column sums to 100% (answers "What causes this MSN_STATUS?")

    # Step 1: Calculate total for each MSN_STATUS across all FAILCRAWLERs
    msn_totals = {}
    for msn_status in step_df['MSN_STATUS'].unique():
        msn_df = step_df[step_df['MSN_STATUS'] == msn_status]
        msn_total = 0
        for fc in fc_columns:
            if fc in msn_df.columns:
                msn_total += msn_df[fc].sum()
        msn_totals[msn_status] = msn_total

    # Step 2: Build correlation matrix with column-wise percentages
    correlation_matrix = {}
    for fc in fc_columns:
        if fc in step_df.columns:
            fc_by_msn = step_df.groupby('MSN_STATUS')[fc].sum()
            correlation_matrix[fc] = {}
            for msn_status in fc_by_msn.index:
                msn_total = msn_totals.get(msn_status, 0)
                if msn_total > 0:
                    correlation_matrix[fc][msn_status] = round(fc_by_msn[msn_status] / msn_total * 100, 1)
                else:
                    correlation_matrix[fc][msn_status] = 0.0

    # Get top FAILCRAWLERs (by total DPM across all MSN_STATUS)
    fc_totals = {}
    for fc in fc_columns:
        if fc in step_df.columns:
            fc_totals[fc] = step_df[fc].sum()
    top_fcs = sorted(fc_totals.keys(), key=lambda x: fc_totals[x], reverse=True)[:10]

    # Get MSN_STATUS list (sorted by contribution)
    msn_statuses = msn_status_agg['MSN_STATUS'].tolist()

    return {
        'step': step,
        'msn_status_summary': msn_status_agg,
        'correlation_matrix': correlation_matrix,
        'fc_columns': fc_columns,
        'top_fcs': top_fcs,
        'msn_statuses': msn_statuses,
        'grand_total_fails': grand_total_fails,
        'total_muin': total_muin,
        'total_uin': total_uin
    }


def create_msn_status_correlation_chart(data: dict, dark_mode: bool = False) -> go.Figure:
    """
    Create a heatmap showing FAILCRAWLER × MSN_STATUS CDPM contribution.

    Args:
        data: Processed correlation data from process_msn_status_correlation
        dark_mode: Theme setting

    Returns:
        Plotly Figure (heatmap)
    """
    if data is None:
        return None

    correlation_matrix = data['correlation_matrix']
    top_fcs = data['top_fcs']
    msn_statuses = data['msn_statuses']
    step = data['step']

    # Build z-values matrix
    z_values = []
    for fc in top_fcs:
        row = []
        for msn in msn_statuses:
            val = correlation_matrix.get(fc, {}).get(msn, 0)
            row.append(val)
        z_values.append(row)

    # Theme colors - using red shades for failure/error data
    if dark_mode:
        font_color = '#E0E0E0'
        paper_bg = 'rgba(0,0,0,0)'
        # Custom colorscale: dark red to bright coral (readable in dark mode)
        colorscale = [
            [0, '#450a0a'],      # Very dark red for low values
            [0.5, '#dc2626'],    # Medium red
            [1, '#f87171']       # Bright coral for high values
        ]
    else:
        font_color = '#1a1a1a'
        paper_bg = '#FFFFFF'
        # Custom colorscale: light pink to medium red (keeps text readable)
        colorscale = [
            [0, '#fef2f2'],      # Very light pink for low values
            [0.3, '#fecaca'],    # Light red/pink
            [0.6, '#f87171'],    # Coral red
            [1, '#dc2626']       # Medium red for high values
        ]

    # Calculate max value to determine text color threshold
    max_val = max(max(row) for row in z_values) if z_values and z_values[0] else 1

    # Create text colors: white for high values (>60% of max), dark for low values
    text_colors = []
    for row in z_values:
        row_colors = []
        for val in row:
            if val > max_val * 0.6:
                row_colors.append('#ffffff' if not dark_mode else '#1a1a1a')
            else:
                row_colors.append('#1a1a1a' if not dark_mode else '#ffffff')
        text_colors.append(row_colors)

    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=msn_statuses,
        y=top_fcs,
        colorscale=colorscale,
        hovertemplate='MSN_STATUS: %{x}<br>FAILCRAWLER: %{y}<br>Contribution: %{z:.1f}% of this MSN_STATUS<extra></extra>',
        colorbar=dict(
            title=dict(text='Contribution %', font=dict(color=font_color)),
            tickfont=dict(color=font_color)
        ),
        showscale=True
    ))

    # Add annotations with adaptive text colors for each cell
    annotations = []
    for i, fc in enumerate(top_fcs):
        for j, msn in enumerate(msn_statuses):
            val = z_values[i][j]
            text_color = text_colors[i][j]
            annotations.append(dict(
                x=msn,
                y=fc,
                text=f'{val:.1f}%',
                showarrow=False,
                font=dict(size=10, color=text_color)
            ))

    fig.update_layout(
        annotations=annotations,
        title=dict(
            text=f'<b>{step} FAILCRAWLER × MSN_STATUS</b><br><sub>What causes each MSN_STATUS? (Column = 100%)</sub>',
            font=dict(color=font_color, size=14)
        ),
        xaxis=dict(
            title='MSN_STATUS',
            tickfont=dict(color=font_color, size=10),
            tickangle=-45
        ),
        yaxis=dict(
            title='FAILCRAWLER',
            tickfont=dict(color=font_color, size=10)
        ),
        paper_bgcolor=paper_bg,
        plot_bgcolor=paper_bg,
        font=dict(color=font_color),
        height=400,
        margin=dict(l=150, r=50, t=50, b=100)
    )

    return fig


def create_msn_status_ranked_table_html(data: dict, dark_mode: bool = False) -> str:
    """
    Create HTML table showing MSN_STATUS ranked by fail count contribution.

    Shows both MDPM and cDPM:
    - MDPM = (Unique Modules / Total MUIN) × 1,000,000
    - cDPM = (Unique FIDs / Total Component UIN) × 1,000,000

    Args:
        data: Processed correlation data
        dark_mode: Theme setting

    Returns:
        HTML string for the ranked table
    """
    if data is None:
        return ""

    summary = data['msn_status_summary']
    step = data['step']
    grand_total = data['grand_total_fails']
    total_muin = data.get('total_muin')
    total_uin = data.get('total_uin')

    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#3d3d3d' if dark_mode else '#e8e8e8'
    border_color = '#555555' if dark_mode else '#cccccc'
    highlight_80 = '#4a1a1a' if dark_mode else '#ffe0e0'
    subtext_color = '#aaaaaa' if dark_mode else '#666666'

    # Volume info text
    volume_info = []
    if total_muin:
        volume_info.append(f"Module UIN: {total_muin:,}")
    if total_uin:
        volume_info.append(f"Component UIN: {total_uin:,}")
    volume_text = " | ".join(volume_info) if volume_info else ""

    html = f'''
    <div style="margin-bottom: 20px;">
        <h4 style="color: {text_color}; margin-bottom: 5px;">{step} MSN_STATUS Ranked by Fail Count</h4>
        <p style="font-size: 11px; color: {subtext_color}; margin-bottom: 10px;">
            {volume_text}
        </p>
        <table style="border-collapse: collapse; width: 100%; font-size: 11px; font-family: Arial, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: left; color: {text_color};">Rank</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: left; color: {text_color};">MSN_STATUS</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};" title="Unique Module Serial Numbers that failed">Modules</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};" title="MDPM = (Modules / MUIN) × 1M">MDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};" title="Unique Component (Die) IDs that failed">FIDs</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};" title="cDPM = (FIDs / Component UIN) × 1M">cDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">Contrib %</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">Cumul %</th>
                </tr>
            </thead>
            <tbody>
    '''

    for i, (_, row) in enumerate(summary.iterrows(), 1):
        # Highlight top 80% contributors
        row_style = ''
        if row['CUMULATIVE_PCT'] <= 80 or i == 1:
            row_style = f'background-color: {highlight_80};'

        unique_modules = int(row['UNIQUE_MODULES']) if 'UNIQUE_MODULES' in row else 0
        unique_fids = int(row['UNIQUE_FIDS']) if 'UNIQUE_FIDS' in row else 0
        mdpm = row['MDPM'] if 'MDPM' in row else 0
        cdpm = row['CDPM'] if 'CDPM' in row else 0

        html += f'''
            <tr style="{row_style}">
                <td style="border: 1px solid {border_color}; padding: 6px; color: {text_color};">{i}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; font-weight: bold; color: {text_color};">{row['MSN_STATUS']}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{unique_modules:,}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{mdpm:,.2f}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{unique_fids:,}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{cdpm:,.2f}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{row['CONTRIBUTION_PCT']:.1f}%</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{row['CUMULATIVE_PCT']:.1f}%</td>
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        <p style="font-size: 10px; color: {subtext_color}; margin-top: 5px;">
            🔴 Red rows = Top 80% contributors | MDPM = Module DPM | cDPM = Component DPM
        </p>
    </div>
    '''

    return html


def create_dpm_metrics_summary_html(
    cdpm_df: pd.DataFrame,
    mdpm_df: pd.DataFrame,
    fcdpm_df: pd.DataFrame,
    step: str,
    workweek: int = None,
    dark_mode: bool = False,
    fcfm_df: pd.DataFrame = None,
    show_trends: bool = True
) -> str:
    """
    Create HTML summary table showing cDPM, MDPM, and FCDPM side by side.

    Args:
        cdpm_df: cDPM DataFrame
        mdpm_df: MDPM DataFrame
        fcdpm_df: FCDPM DataFrame
        step: Test step
        workweek: Optional specific workweek (None = all)
        dark_mode: Theme setting
        fcfm_df: Optional FCFM DataFrame for UE/UNKNOWN breakdown
        show_trends: Whether to show WoW trends and sparklines

    Returns:
        HTML string for the DPM summary card
    """
    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#1a237e' if not dark_mode else '#283593'  # Dashboard theme blue
    border_color = '#555555' if dark_mode else '#cccccc'
    card_bg = '#f5f5f5' if not dark_mode else '#3d3d3d'
    subtext_color = '#aaaaaa' if dark_mode else '#666666'

    # Calculate metrics
    cdpm_val = None
    mdpm_val = None
    uin = 0
    muin = 0
    fcdpm_total = None
    fcdpm_decoded = None  # UE only (matches moduledat)
    fcdpm_undecoded = None  # UNKNOWN only
    fcdpm_breakdown = []
    unknown_failcrawlers = set()  # FAILCRAWLERs with FCFM=UNKNOWN

    # Get UNKNOWN FAILCRAWLERs from FCFM data if available
    if fcfm_df is not None and not fcfm_df.empty:
        step_col = 'QUERY_STEP' if 'QUERY_STEP' in fcfm_df.columns else 'STEP'
        fcfm_step_df = fcfm_df[fcfm_df[step_col].str.upper() == step.upper()].copy() if step_col in fcfm_df.columns else fcfm_df.copy()
        if workweek is not None and 'MFG_WORKWEEK' in fcfm_step_df.columns:
            fcfm_step_df = fcfm_step_df[fcfm_step_df['MFG_WORKWEEK'] == workweek]
        if not fcfm_step_df.empty and 'FCFM' in fcfm_step_df.columns and 'FAILCRAWLER' in fcfm_step_df.columns:
            unknown_rows = fcfm_step_df[fcfm_step_df['FCFM'].str.upper() == 'UNKNOWN']
            unknown_failcrawlers = set(unknown_rows['FAILCRAWLER'].str.upper().unique())

    # Helper to filter by step (handles both QUERY_STEP and STEP columns)
    def filter_by_step(df: pd.DataFrame, step_name: str) -> pd.DataFrame:
        if df.empty:
            return df
        step_col = 'QUERY_STEP' if 'QUERY_STEP' in df.columns else 'STEP'
        if step_col in df.columns:
            return df[df[step_col].str.upper() == step_name.upper()].copy()
        return df.copy()

    # Process cDPM
    if not cdpm_df.empty:
        step_df = filter_by_step(cdpm_df, step)
        if workweek is not None and 'MFG_WORKWEEK' in step_df.columns:
            step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]
        if not step_df.empty:
            total_uin = pd.to_numeric(step_df['UIN'], errors='coerce').sum() if 'UIN' in step_df.columns else 0
            total_ufail = pd.to_numeric(step_df['UFAIL'], errors='coerce').sum() if 'UFAIL' in step_df.columns else 0
            uin = int(total_uin) if total_uin > 0 else 0
            if total_uin > 0:
                cdpm_val = round((total_ufail / total_uin) * 1_000_000, 2)

    # Process MDPM
    if not mdpm_df.empty:
        step_df = filter_by_step(mdpm_df, step)
        if workweek is not None and 'MFG_WORKWEEK' in step_df.columns:
            step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]
        if not step_df.empty:
            total_muin = pd.to_numeric(step_df['MUIN'], errors='coerce').sum() if 'MUIN' in step_df.columns else 0
            total_mufail = pd.to_numeric(step_df['MUFAIL'], errors='coerce').sum() if 'MUFAIL' in step_df.columns else 0
            muin = int(total_muin) if total_muin > 0 else 0
            if total_muin > 0:
                mdpm_val = round((total_mufail / total_muin) * 1_000_000, 2)

    # Process FCDPM
    if not fcdpm_df.empty:
        step_df = filter_by_step(fcdpm_df, step)
        if workweek is not None and 'MFG_WORKWEEK' in step_df.columns:
            step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]
        if not step_df.empty:
            # Get FAILCRAWLER category columns (exclude ALL metadata and non-FC columns)
            metadata_cols = ['STEPTYPE', 'DESIGN_ID', 'STEP', 'MFG_WORKWEEK', 'FCFM', 'QUERY_STEP',
                             'UIN', 'UFAIL', 'UPASS', 'ALL', 'ALL(DPM)', 'UNKNOWN',
                             'MOD_CUSTOM_TEST_FLOW', 'MSN_STATUS', 'VERIFIED', 'FID_STATUS',
                             'MUIN', 'MUFAIL']
            fc_cols = [col for col in step_df.columns if col not in metadata_cols]

            total_uin_fc = pd.to_numeric(step_df['UIN'], errors='coerce').sum() if 'UIN' in step_df.columns else 0
            if total_uin_fc > 0:
                total_dpm = 0
                decoded_dpm = 0
                undecoded_dpm = 0
                # Use MEAN to match chart calculation (averages across design IDs)
                # Chart uses: agg_dict = {col: 'mean' for col in fc_columns}
                for fc_col in fc_cols:  # All FAILCRAWLER columns
                    fc_dpm = pd.to_numeric(step_df[fc_col], errors='coerce').mean() if fc_col in step_df.columns else 0
                    if fc_dpm > 0:
                        is_unknown = fc_col.upper() in unknown_failcrawlers
                        fcdpm_breakdown.append({
                            'category': fc_col,
                            'dpm': round(float(fc_dpm), 2),
                            'is_unknown': is_unknown
                        })
                        total_dpm += fc_dpm
                        if is_unknown:
                            undecoded_dpm += fc_dpm
                        else:
                            decoded_dpm += fc_dpm
                fcdpm_total = round(float(total_dpm), 2)
                fcdpm_decoded = round(float(decoded_dpm), 2)
                fcdpm_undecoded = round(float(undecoded_dpm), 2)
                fcdpm_breakdown.sort(key=lambda x: x['dpm'], reverse=True)

    # Format workweek display
    ww_display = f"WW{workweek}" if workweek else "Cumulative"

    # Calculate trends and WoW changes if enabled
    cdpm_wow = {'arrow': '', 'change_pct': None, 'alert_color': '#3498DB'}
    mdpm_wow = {'arrow': '', 'change_pct': None, 'alert_color': '#E74C3C'}
    fcdpm_wow = {'arrow': '', 'change_pct': None, 'alert_color': '#F39C12'}

    cdpm_sparkline = ''
    mdpm_sparkline = ''
    fcdpm_sparkline = ''

    if show_trends and workweek is not None:
        # Calculate FCDPM trend
        fcdpm_trend = calculate_dpm_trend_by_week(fcdpm_df, step, 'fcdpm')
        if len(fcdpm_trend) >= 2:
            sorted_wws = sorted(fcdpm_trend.keys())
            if workweek in fcdpm_trend and sorted_wws.index(workweek) > 0:
                prev_idx = sorted_wws.index(workweek) - 1
                prev_ww = sorted_wws[prev_idx]
                fcdpm_wow = calculate_wow_change(fcdpm_trend[workweek], fcdpm_trend[prev_ww])
            # Generate sparkline
            trend_values = [fcdpm_trend[ww] for ww in sorted_wws if ww in fcdpm_trend]
            if len(trend_values) >= 2:
                fcdpm_sparkline = generate_sparkline_svg(trend_values, color='#F39C12', alert_color=fcdpm_wow.get('alert_color'))

        # Calculate cDPM trend
        cdpm_trend = calculate_dpm_trend_by_week(cdpm_df, step, 'cdpm')
        if len(cdpm_trend) >= 2:
            sorted_wws = sorted(cdpm_trend.keys())
            if workweek in cdpm_trend and sorted_wws.index(workweek) > 0:
                prev_idx = sorted_wws.index(workweek) - 1
                prev_ww = sorted_wws[prev_idx]
                cdpm_wow = calculate_wow_change(cdpm_trend[workweek], cdpm_trend[prev_ww])
            trend_values = [cdpm_trend[ww] for ww in sorted_wws if ww in cdpm_trend]
            if len(trend_values) >= 2:
                cdpm_sparkline = generate_sparkline_svg(trend_values, color='#3498DB', alert_color=cdpm_wow.get('alert_color'))

        # Calculate MDPM trend
        mdpm_trend = calculate_dpm_trend_by_week(mdpm_df, step, 'mdpm')
        if len(mdpm_trend) >= 2:
            sorted_wws = sorted(mdpm_trend.keys())
            if workweek in mdpm_trend and sorted_wws.index(workweek) > 0:
                prev_idx = sorted_wws.index(workweek) - 1
                prev_ww = sorted_wws[prev_idx]
                mdpm_wow = calculate_wow_change(mdpm_trend[workweek], mdpm_trend[prev_ww])
            trend_values = [mdpm_trend[ww] for ww in sorted_wws if ww in mdpm_trend]
            if len(trend_values) >= 2:
                mdpm_sparkline = generate_sparkline_svg(trend_values, color='#E74C3C', alert_color=mdpm_wow.get('alert_color'))

    # Helper to build WoW indicator HTML
    def wow_indicator_html(wow_data: dict) -> str:
        if wow_data.get('change_pct') is None:
            return ''
        arrow = wow_data.get('arrow', '')
        pct = wow_data.get('change_pct', 0)
        color = wow_data.get('alert_color', '#666666')
        sign = '+' if pct > 0 else ''
        return f'<span style="font-size: 12px; color: {color}; margin-left: 8px;">{arrow} {sign}{pct:.0f}%</span>'

    # Build HTML
    html = f'''
    <div style="background-color: {bg_color}; border-radius: 8px; padding: 16px; margin-bottom: 16px;">
        <h3 style="color: {text_color}; margin-bottom: 12px; font-size: 16px;">
            📊 {step} DPM Metrics Summary ({ww_display})
        </h3>

        <div style="display: flex; gap: 16px; flex-wrap: wrap;">
            <!-- cDPM Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid {cdpm_wow.get('alert_color', '#3498DB')};">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">
                    cDPM {wow_indicator_html(cdpm_wow)}
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="font-size: 24px; font-weight: bold; color: #3498DB;">
                        {cdpm_val if cdpm_val is not None else 'N/A'}
                    </span>
                    {cdpm_sparkline}
                </div>
                <div style="font-size: 10px; color: {subtext_color};">Component/Package Level</div>
                <div style="font-size: 10px; color: {subtext_color}; margin-top: 4px;">UIN: {uin:,}</div>
            </div>

            <!-- MDPM Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid {mdpm_wow.get('alert_color', '#E74C3C')};">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">
                    MDPM {wow_indicator_html(mdpm_wow)}
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="font-size: 24px; font-weight: bold; color: #E74C3C;">
                        {mdpm_val if mdpm_val is not None else 'N/A'}
                    </span>
                    {mdpm_sparkline}
                </div>
                <div style="font-size: 10px; color: {subtext_color};">Module Level</div>
                <div style="font-size: 10px; color: {subtext_color}; margin-top: 4px;">MUIN: {muin:,}</div>
            </div>

            <!-- FCDPM Total Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid {fcdpm_wow.get('alert_color', '#F39C12')};">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">
                    FCDPM Total {wow_indicator_html(fcdpm_wow)}
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="font-size: 24px; font-weight: bold; color: #F39C12;">
                        {fcdpm_total if fcdpm_total is not None else 'N/A'}
                    </span>
                    {fcdpm_sparkline}
                </div>
                <div style="font-size: 10px; color: {subtext_color};">All FAILCRAWLERs (UE+UNKNOWN)</div>
                <div style="font-size: 10px; color: {subtext_color}; margin-top: 4px;">Target: {TARGET_CDPM}</div>
            </div>

            <!-- FCDPM Decoded Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid #27AE60;">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">FCDPM Decoded</div>
                <div style="font-size: 24px; font-weight: bold; color: #27AE60; margin: 4px 0;">
                    {fcdpm_decoded if fcdpm_decoded is not None else 'N/A'}
                </div>
                <div style="font-size: 10px; color: {subtext_color};">UE Only (matches moduledat)</div>
                <div style="font-size: 10px; color: #E74C3C; margin-top: 4px;">Undecoded: {fcdpm_undecoded if fcdpm_undecoded is not None else 'N/A'}</div>
            </div>
        </div>
    </div>
    '''

    return html


def create_dpm_comparison_table_html(
    cdpm_df: pd.DataFrame,
    mdpm_df: pd.DataFrame,
    steps: list[str],
    workweek: int = None,
    dark_mode: bool = False
) -> str:
    """
    Create HTML table comparing cDPM and MDPM across multiple steps.

    Args:
        cdpm_df: cDPM DataFrame
        mdpm_df: MDPM DataFrame
        steps: List of test steps to compare
        workweek: Optional specific workweek
        dark_mode: Theme setting

    Returns:
        HTML string for the comparison table
    """
    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#1a237e' if not dark_mode else '#283593'
    border_color = '#555555' if dark_mode else '#cccccc'
    subtext_color = '#aaaaaa' if dark_mode else '#666666'

    ww_display = f"WW{workweek}" if workweek else "Cumulative"

    html = f'''
    <div style="margin-bottom: 20px;">
        <h4 style="color: {text_color}; margin-bottom: 10px;">DPM Comparison by Step ({ww_display})</h4>
        <table style="border-collapse: collapse; width: 100%; font-size: 12px; font-family: Arial, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: left; color: white;">Step</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: white;">cDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: white;">UIN (Pkg)</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: white;">MDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: white;">MUIN (Mod)</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: white;">Ratio (c/M)</th>
                </tr>
            </thead>
            <tbody>
    '''

    # Helper to filter by step (handles both QUERY_STEP and STEP columns)
    def filter_by_step(df: pd.DataFrame, step_name: str) -> pd.DataFrame:
        if df.empty:
            return df
        step_col = 'QUERY_STEP' if 'QUERY_STEP' in df.columns else 'STEP'
        if step_col in df.columns:
            return df[df[step_col].str.upper() == step_name.upper()].copy()
        return df.copy()

    for step in steps:
        cdpm_val = None
        mdpm_val = None
        uin = 0
        muin = 0

        # Calculate cDPM
        if not cdpm_df.empty:
            step_df = filter_by_step(cdpm_df, step)
            if workweek is not None and 'MFG_WORKWEEK' in step_df.columns:
                step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]
            if not step_df.empty:
                total_uin = pd.to_numeric(step_df['UIN'], errors='coerce').sum() if 'UIN' in step_df.columns else 0
                total_ufail = pd.to_numeric(step_df['UFAIL'], errors='coerce').sum() if 'UFAIL' in step_df.columns else 0
                uin = int(total_uin) if total_uin > 0 else 0
                if total_uin > 0:
                    cdpm_val = round((total_ufail / total_uin) * 1_000_000, 2)

        # Calculate MDPM
        if not mdpm_df.empty:
            step_df = filter_by_step(mdpm_df, step)
            if workweek is not None and 'MFG_WORKWEEK' in step_df.columns:
                step_df = step_df[step_df['MFG_WORKWEEK'] == workweek]
            if not step_df.empty:
                total_muin = pd.to_numeric(step_df['MUIN'], errors='coerce').sum() if 'MUIN' in step_df.columns else 0
                total_mufail = pd.to_numeric(step_df['MUFAIL'], errors='coerce').sum() if 'MUFAIL' in step_df.columns else 0
                muin = int(total_muin) if total_muin > 0 else 0
                if total_muin > 0:
                    mdpm_val = round((total_mufail / total_muin) * 1_000_000, 2)

        # Calculate ratio
        ratio = None
        if cdpm_val is not None and mdpm_val is not None and mdpm_val > 0:
            ratio = round(cdpm_val / mdpm_val, 2)

        html += f'''
            <tr>
                <td style="border: 1px solid {border_color}; padding: 8px; font-weight: bold; color: {text_color};">{step}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: #3498DB; font-weight: bold;">
                    {cdpm_val if cdpm_val is not None else 'N/A'}
                </td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{uin:,}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: #E74C3C; font-weight: bold;">
                    {mdpm_val if mdpm_val is not None else 'N/A'}
                </td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{muin:,}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">
                    {ratio if ratio is not None else 'N/A'}
                </td>
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        <p style="font-size: 10px; color: {subtext_color}; margin-top: 5px;">
            cDPM = Component/Package DPM (FID level) | MDPM = Module DPM (MSN level) | Ratio = cDPM/MDPM (≈4 expected for 4-pkg modules)
        </p>
    </div>
    '''

    return html


# =============================================================================
# FAILCRAWLER Drill-down Functions
# =============================================================================

def fetch_failcrawler_msn_drilldown(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[int],
    failcrawler: str,
    msn_status: str = None
) -> pd.DataFrame:
    """
    Fetch MSN-level data for a specific FAILCRAWLER category and optionally MSN_STATUS.

    Args:
        design_ids: List of Design IDs
        steps: List of test steps
        workweeks: List of workweeks in YYYYWW format
        failcrawler: FAILCRAWLER category to drill down on
        msn_status: Optional MSN_STATUS filter (e.g., 'DQ', 'Downbin', 'Good')

    Returns:
        DataFrame with MSN-level fail details
    """
    if not design_ids or not steps or not workweeks:
        return pd.DataFrame()

    # Build mtsums command with MSN grouping
    dbase = ','.join(design_ids)
    step_str = ','.join(steps)
    ww_str = ','.join(str(ww) for ww in workweeks)

    # Build command list with full path (for systemd compatibility)
    cmd_parts = [
        '/u/dramsoft/bin/mtsums',
        f'-dbase={dbase}',
        f'-step={step_str}',
        f'-ww={ww_str}',
        f'-failcrawler={failcrawler}',
    ]

    # Add MSN_STATUS filter if provided
    if msn_status:
        cmd_parts.append(f'-msn_status={msn_status}')

    # Add output format flags
    cmd_parts.extend([
        '+msnag', '+fc',
        '-format+=msn,mfg_workweek,step,failcrawler,msn_status',
        '=islatest', '=isvalid', '+stdf', '+quiet', '+csv'
    ])

    logger.info(f"FAILCRAWLER drilldown command: {' '.join(cmd_parts)}")

    try:
        result = subprocess.run(
            cmd_parts,
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            logger.error(f"mtsums error: {result.stderr}")
            return pd.DataFrame()

        if not result.stdout.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(result.stdout))
        logger.info(f"FAILCRAWLER drilldown returned {len(df)} rows")
        return df

    except Exception as e:
        logger.exception(f"FAILCRAWLER drilldown error: {e}")
        return pd.DataFrame()


def create_failcrawler_drilldown_html(
    drilldown_df: pd.DataFrame,
    failcrawler: str,
    step: str,
    msn_status: str = None,
    dark_mode: bool = False,
    max_rows: int = 20
) -> str:
    """
    Create HTML table showing MSNs affected by a FAILCRAWLER category and optionally MSN_STATUS.

    Args:
        drilldown_df: DataFrame with MSN-level data
        failcrawler: FAILCRAWLER category name
        step: Test step
        msn_status: Optional MSN_STATUS filter
        dark_mode: Use dark mode styling
        max_rows: Maximum rows to display

    Returns:
        HTML string for the drilldown table
    """
    if drilldown_df.empty:
        filter_desc = f"{failcrawler}" + (f" × {msn_status}" if msn_status else "")
        return f"<p style='color: #888;'>No MSN data found for {filter_desc}</p>"

    # Filter by step if present
    df = drilldown_df.copy()
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in df.columns else 'STEP'
    if step_col in df.columns:
        df = df[df[step_col].str.upper() == step.upper()]

    if df.empty:
        filter_desc = f"{failcrawler}" + (f" × {msn_status}" if msn_status else "")
        return f"<p style='color: #888;'>No MSN data found for {filter_desc} at {step}</p>"

    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#1a237e' if not dark_mode else '#283593'
    border_color = '#555555' if dark_mode else '#e0e0e0'

    # Get FAILCRAWLER color
    fc_color = FAILCRAWLER_COLORS.get(failcrawler, '#888')

    # Aggregate by MSN
    msn_col = 'MSN' if 'MSN' in df.columns else None
    if not msn_col:
        return f"<p style='color: #888;'>MSN column not found in data</p>"

    # Calculate metrics per MSN
    msn_data = []
    for msn in df[msn_col].unique():
        msn_df = df[df[msn_col] == msn]
        ufail = pd.to_numeric(msn_df['UFAIL'], errors='coerce').sum() if 'UFAIL' in msn_df.columns else 0
        uin = pd.to_numeric(msn_df['UIN'], errors='coerce').sum() if 'UIN' in msn_df.columns else 0

        # Get workweek (use most recent)
        ww = None
        if 'MFG_WORKWEEK' in msn_df.columns:
            ww = msn_df['MFG_WORKWEEK'].max()

        # Get MSN_STATUS if available
        status = None
        if 'MSN_STATUS' in msn_df.columns:
            status = msn_df['MSN_STATUS'].iloc[0] if len(msn_df) > 0 else None

        msn_data.append({
            'msn': msn,
            'ufail': int(ufail),
            'uin': int(uin),
            'workweek': ww,
            'status': status
        })

    # Sort by UFAIL descending
    msn_data = sorted(msn_data, key=lambda x: -x['ufail'])

    total_msns = len(msn_data)
    total_ufail = sum(m['ufail'] for m in msn_data)

    # Build title with optional MSN_STATUS
    title_parts = [failcrawler]
    if msn_status:
        title_parts.append(msn_status)
    title = " × ".join(title_parts)

    html = f'''
    <div style="background-color: {bg_color}; padding: 12px; border-radius: 8px; border: 1px solid {border_color}; margin: 8px 0;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
            <span style="font-size: 13px; font-weight: 600; color: {text_color};">
                <span style="display: inline-block; width: 12px; height: 12px; background-color: {fc_color}; border-radius: 3px; margin-right: 8px;"></span>
                🔍 {title} - Affected MSNs ({step})
            </span>
            <span style="font-size: 11px; color: #888;">{total_msns} modules | {total_ufail:,} total FID fails</span>
        </div>
        <table style="border-collapse: collapse; width: 100%; font-size: 11px; font-family: monospace;">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="padding: 6px 8px; text-align: left; color: white;">MSN</th>
                    <th style="padding: 6px 8px; text-align: right; color: white;">FID Fails</th>
                    <th style="padding: 6px 8px; text-align: right; color: white;">UIN</th>
                    <th style="padding: 6px 8px; text-align: center; color: white;">WW</th>
                </tr>
            </thead>
            <tbody>
    '''

    # Show top MSNs
    for i, msn_info in enumerate(msn_data[:max_rows]):
        row_bg = bg_color if i % 2 == 0 else ('#363636' if dark_mode else '#f8f9fa')

        html += f'''
            <tr style="background-color: {row_bg};">
                <td style="padding: 5px 8px; color: {text_color}; border-bottom: 1px solid {border_color};">
                    {msn_info['msn']}
                </td>
                <td style="padding: 5px 8px; text-align: right; color: #E74C3C; font-weight: 600; border-bottom: 1px solid {border_color};">
                    {msn_info['ufail']:,}
                </td>
                <td style="padding: 5px 8px; text-align: right; color: {text_color}; border-bottom: 1px solid {border_color};">
                    {msn_info['uin']:,}
                </td>
                <td style="padding: 5px 8px; text-align: center; color: #888; border-bottom: 1px solid {border_color};">
                    {msn_info['workweek'] if msn_info['workweek'] else '-'}
                </td>
            </tr>
        '''

    # Show "more" indicator if truncated
    if total_msns > max_rows:
        remaining = total_msns - max_rows
        remaining_ufail = sum(m['ufail'] for m in msn_data[max_rows:])
        html += f'''
            <tr style="background-color: {bg_color};">
                <td colspan="4" style="padding: 8px; text-align: center; color: #888; font-style: italic; border-bottom: 1px solid {border_color};">
                    ... and {remaining} more MSNs ({remaining_ufail:,} additional FID fails)
                </td>
            </tr>
        '''

    html += '''
            </tbody>
        </table>
    </div>
    '''

    return html


def get_failcrawler_list_for_step(fc_df: pd.DataFrame, step: str) -> list[str]:
    """
    Get list of FAILCRAWLER categories for a step, sorted by cDPM.

    Handles two data formats:
    1. Row format: DataFrame has 'FAILCRAWLER' column with one row per category
    2. Pivoted format: DataFrame has FAILCRAWLER names as columns (cDPM values)

    Args:
        fc_df: FAILCRAWLER DataFrame (either format)
        step: Test step

    Returns:
        List of FAILCRAWLER names sorted by cDPM descending
    """
    if fc_df.empty:
        return []

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in fc_df.columns else 'STEP'
    df = fc_df.copy()

    if step_col in df.columns:
        df = df[df[step_col].str.upper() == step.upper()]

    if df.empty:
        return []

    # Metadata columns to exclude
    exclude_cols = {
        'STEPTYPE', 'DESIGN_ID', 'STEP', 'QUERY_STEP', 'MFG_WORKWEEK', 'MSN_STATUS',
        'MOD_CUSTOM_TEST_FLOW', 'ALL(DPM)', 'ALL', 'UIN', 'UFAIL', 'MUIN', 'MUFAIL',
        'ALL(SUM)', 'SUM', 'TOTAL', 'PASS', 'UNKNOWN'
    }

    # Check if data is in row format (has FAILCRAWLER column)
    if 'FAILCRAWLER' in df.columns:
        # Row format: aggregate by FAILCRAWLER
        exclude_vals = {'ALL', 'SUM', 'TOTAL', 'PASS', 'ALL(DPM)', 'ALL(SUM)', 'MOD_CUSTOM_TEST_FLOW'}
        df = df[~df['FAILCRAWLER'].str.upper().isin(exclude_vals)]

        fc_data = []
        for fc in df['FAILCRAWLER'].unique():
            if fc and pd.notna(fc):
                fc_df_filtered = df[df['FAILCRAWLER'] == fc]
                ufail = pd.to_numeric(fc_df_filtered['UFAIL'], errors='coerce').sum() if 'UFAIL' in fc_df_filtered.columns else 0
                fc_data.append((fc, ufail))

        fc_data = sorted(fc_data, key=lambda x: -x[1])
        return [fc for fc, _ in fc_data]

    else:
        # Pivoted format: FAILCRAWLER names are columns (cDPM values)
        # Get numeric columns that are potential FAILCRAWLERs
        fc_data = []
        for col in df.columns:
            if col.upper() not in exclude_cols:
                # Sum the cDPM values across all rows for this FAILCRAWLER
                total_cdpm = pd.to_numeric(df[col], errors='coerce').sum()
                if total_cdpm > 0:
                    fc_data.append((col, total_cdpm))

        # Sort by total cDPM descending
        fc_data = sorted(fc_data, key=lambda x: -x[1])
        return [fc for fc, _ in fc_data]


def get_msn_status_list_for_step(msn_corr_df: pd.DataFrame, step: str) -> list[str]:
    """
    Get list of MSN_STATUS values for a step, sorted by UFAIL.

    Args:
        msn_corr_df: MSN_STATUS correlation DataFrame
        step: Test step

    Returns:
        List of MSN_STATUS values sorted by UFAIL descending
    """
    if msn_corr_df.empty:
        return []

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
    df = msn_corr_df.copy()

    if step_col in df.columns:
        df = df[df[step_col].str.upper() == step.upper()]

    if df.empty or 'MSN_STATUS' not in df.columns:
        return []

    # Exclude Pass status (typically not useful for drill-down)
    df = df[df['MSN_STATUS'].str.upper() != 'PASS']

    # Aggregate by MSN_STATUS and sort by UFAIL
    status_data = []
    for status in df['MSN_STATUS'].unique():
        if status and pd.notna(status):
            status_df = df[df['MSN_STATUS'] == status]
            ufail = pd.to_numeric(status_df['UFAIL'], errors='coerce').sum() if 'UFAIL' in status_df.columns else 0
            status_data.append((status, ufail))

    # Sort by UFAIL descending
    status_data = sorted(status_data, key=lambda x: -x[1])

    return [status for status, _ in status_data]


def get_heatmap_combinations(msn_corr_df: pd.DataFrame, step: str) -> list[tuple[str, str, float]]:
    """
    Get FAILCRAWLER × MSN_STATUS combinations from correlation data.

    Args:
        msn_corr_df: MSN_STATUS correlation DataFrame
        step: Test step

    Returns:
        List of (FAILCRAWLER, MSN_STATUS, cDPM) tuples sorted by cDPM descending
    """
    if msn_corr_df.empty:
        return []

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
    df = msn_corr_df.copy()

    if step_col in df.columns:
        df = df[df[step_col].str.upper() == step.upper()]

    if df.empty:
        return []

    # Check required columns
    if 'FAILCRAWLER' not in df.columns or 'MSN_STATUS' not in df.columns:
        return []

    # Exclude metadata rows
    exclude_fcs = {'ALL', 'SUM', 'TOTAL', 'PASS', 'ALL(DPM)', 'ALL(SUM)', 'MOD_CUSTOM_TEST_FLOW'}
    df = df[~df['FAILCRAWLER'].str.upper().isin(exclude_fcs)]
    df = df[df['MSN_STATUS'].str.upper() != 'PASS']

    # Calculate cDPM for each combination
    combinations = []
    for (fc, status), group in df.groupby(['FAILCRAWLER', 'MSN_STATUS']):
        if fc and status and pd.notna(fc) and pd.notna(status):
            uin = pd.to_numeric(group['UIN'], errors='coerce').sum() if 'UIN' in group.columns else 0
            ufail = pd.to_numeric(group['UFAIL'], errors='coerce').sum() if 'UFAIL' in group.columns else 0
            cdpm = (ufail / uin) * 1_000_000 if uin > 0 else 0
            if cdpm > 0:
                combinations.append((fc, status, round(cdpm, 2)))

    # Sort by cDPM descending
    return sorted(combinations, key=lambda x: -x[2])


# =============================================================================
# cDPM RECOVERY SIMULATION (Hybrid DPM Approach)
# =============================================================================

# Module-level vs FID-level MSN_STATUS categories
MODULE_LEVEL_FAILURES = {'Mod-Sys', 'Hang', 'Multi-Mod', 'Boot'}
FID_LEVEL_FAILURES = {'DQ', 'Row', 'SB_Int', 'Multi-DQ', 'SB', 'Col', 'Column'}

# Recovery Configuration
# New RPx Fix (VERIFIED) - False miscompare detection via signature analysis
# RPx runs actual script to verify false miscompares - not hardcoded FAILCRAWLERs
RPX_SCRIPT_PATH = '/home/nmewes/Y6CP_FA/socamm_false_miscompare.py'
RPX_EXCLUDE_MSN_STATUS = {'Hang', 'Boot'}  # No fail addresses to screen

# New BIOS Fix (PROJECTED) - Timing/speed fix
# - MULTI_BANK_MULTI_DQ: 100% recovery
# - Other BANK/BURST/PERIPH patterns: 50% recovery (non-DRAM related)
BIOS_FIX_FAILCRAWLERS_100PCT = {'MULTI_BANK_MULTI_DQ'}  # Full recovery
BIOS_FIX_PATTERNS_50PCT = {'BANK', 'BURST', 'PERIPH'}  # 50% recovery (pattern substrings)
BIOS_PARTIAL_RECOVERY_RATE = 0.50  # 50% projected recovery for pattern matches

# HW+SOP Fix (PROJECTED) - Debris cleanup + HUNG2 retest for Hang
HW_SOP_MSN_STATUS = {'Hang'}

# DRAM-Related (Potential silicon issue - NOT recoverable)
# - SB/DB FAILCRAWLERs: Single Bit / Double Bit errors
# - ROW MSN_STATUS: Row-related failures not cleared by RPx
DRAM_FAILCRAWLERS = {'SB', 'DB', 'SB_INT', 'DB_INT'}  # Single/Double bit errors
DRAM_MSN_STATUS = {'Row'}  # Row failures that persist after RPx screening

# Excluded from recovery analysis
EXCLUDE_MSN_STATUS = {'Pass', 'Boot'}


def fetch_slash_for_failures(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str],
    exclude_msn_status: set = None
) -> pd.DataFrame:
    """
    Fetch SLASH (summary paths) for failed modules to run false miscompare script.

    Args:
        design_ids: List of Design IDs
        steps: List of test steps (HMB1, QMON)
        workweeks: List of workweeks
        exclude_msn_status: MSN_STATUS values to exclude (default: Hang, Boot)

    Returns:
        DataFrame with MSN, SLASH, MSN_STATUS, FAILCRAWLER columns
    """
    if exclude_msn_status is None:
        exclude_msn_status = RPX_EXCLUDE_MSN_STATUS

    all_data = []
    seen_msn_step = set()  # Track MSN+STEP to ensure 1 summary per MSN per step

    for step in steps:
        for ww in workweeks:
            # Build mtsums command to get SLASH for failures
            # Include islatest,isvalid in format and filter in Python
            cmd = (
                f"mtsums -dbase=y6cp -step={step} "
                f"-module_form_factor=socamm,socamm2 "
                f"-mfg_workweek={ww} "
                f"-design_id={','.join(design_ids)} "
                f"-standard_flow=yes "
                f"-format=msn,slash,msn_status,failcrawler,islatest,isvalid "
                f"-header +quiet 2>/dev/null"
            )

            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
                if result.returncode == 0 and result.stdout.strip():
                    for line in result.stdout.strip().split('\n'):
                        parts = line.split(',')
                        # Format: msn,slash,msn_status,failcrawler,islatest,isvalid
                        if len(parts) >= 6:
                            msn, slash, msn_status, failcrawler, islatest, isvalid = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
                            # Filter: islatest=Y AND isvalid=Y
                            if islatest != 'Y' or isvalid != 'Y':
                                continue
                            # Exclude specified MSN_STATUS (Hang, Boot - no fail addresses)
                            if msn_status not in exclude_msn_status and msn_status != 'Pass':
                                # Only keep 1 summary per MSN per step
                                msn_step_key = (msn, step.upper())
                                if msn_step_key not in seen_msn_step:
                                    seen_msn_step.add(msn_step_key)
                                    all_data.append({
                                        'MSN': msn,
                                        'SLASH': slash,
                                        'MSN_STATUS': msn_status,
                                        'FAILCRAWLER': failcrawler,
                                        'STEP': step.upper(),
                                        'MFG_WORKWEEK': ww
                                    })
            except Exception as e:
                logger.warning(f"Failed to fetch SLASH for {step} WW{ww}: {e}")

    return pd.DataFrame(all_data)


def run_false_miscompare_script(slashes: list[str], batch_size: int = 50) -> pd.DataFrame:
    """
    Run the false miscompare script against summary paths.

    Args:
        slashes: List of SLASH (summary) paths
        batch_size: Number of summaries to process per batch

    Returns:
        DataFrame with false miscompare results
    """
    if not slashes:
        return pd.DataFrame()

    # Deduplicate slashes
    unique_slashes = list(set(slashes))

    all_results = []

    # Process in batches to avoid command line length limits
    for i in range(0, len(unique_slashes), batch_size):
        batch = unique_slashes[i:i + batch_size]

        # Build command
        sums_arg = ' '.join(batch)
        cmd = f"{RPX_SCRIPT_PATH} --sums {sums_arg}"

        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
            if result.returncode == 0 and result.stdout.strip():
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:  # Has header + data
                    header = lines[0].split(',')
                    for line in lines[1:]:
                        parts = line.split(',')
                        if len(parts) >= len(header):
                            row_data = dict(zip(header, parts))
                            all_results.append(row_data)
        except subprocess.TimeoutExpired:
            logger.warning(f"False miscompare script timed out for batch {i//batch_size + 1}")
        except Exception as e:
            logger.warning(f"Failed to run false miscompare script: {e}")

    return pd.DataFrame(all_results)


def calculate_verified_rpx_recovery(
    slash_df: pd.DataFrame,
    hybrid_dpm_df: pd.DataFrame,
    msn_corr_df: pd.DataFrame,
    step: str
) -> dict:
    """
    Calculate VERIFIED RPx recovery by running the false miscompare script.

    This is based on actual verified data, not projected estimates.

    Args:
        slash_df: DataFrame with SLASH paths for failed modules
        hybrid_dpm_df: Hybrid DPM data (MSN_STATUS level)
        msn_corr_df: FAILCRAWLER × MSN_STATUS correlation
        step: Test step

    Returns:
        Dictionary with verified RPx recovery data
    """
    if slash_df.empty:
        return {
            'verified_false_fails': 0,
            'total_fails_screened': 0,
            'rpx_recovery_rate': 0.0,
            'rpx_dpm': 0.0,
            'details': []
        }

    # Filter for this step
    step_slash_df = slash_df[slash_df['STEP'].str.upper() == step.upper()].copy()

    if step_slash_df.empty:
        return {
            'verified_false_fails': 0,
            'total_fails_screened': 0,
            'rpx_recovery_rate': 0.0,
            'rpx_dpm': 0.0,
            'details': []
        }

    # Get unique SLASHes
    slashes = step_slash_df['SLASH'].unique().tolist()
    total_fails_screened = len(step_slash_df)

    # Run false miscompare script
    rpx_results = run_false_miscompare_script(slashes)

    if rpx_results.empty:
        return {
            'verified_false_fails': 0,
            'total_fails_screened': total_fails_screened,
            'rpx_recovery_rate': 0.0,
            'rpx_dpm': 0.0,
            'details': []
        }

    # Count verified false fails (where "False Fail?" == "True")
    if 'False Fail?' in rpx_results.columns:
        verified_false_fails = (rpx_results['False Fail?'] == 'True').sum()
    else:
        verified_false_fails = 0

    # Calculate recovery rate
    rpx_recovery_rate = verified_false_fails / total_fails_screened if total_fails_screened > 0 else 0.0

    # Calculate RPx DPM based on verified recovery rate
    # Apply to non-Hang, non-Boot DPM
    msn_dpm_lookup = dict(zip(hybrid_dpm_df['MSN_STATUS'], hybrid_dpm_df['DPM']))

    rpx_eligible_dpm = sum(
        dpm for msn, dpm in msn_dpm_lookup.items()
        if msn not in RPX_EXCLUDE_MSN_STATUS and msn != 'Pass'
    )

    rpx_dpm = rpx_eligible_dpm * rpx_recovery_rate

    # Get details by MSN_STATUS
    details = []
    if 'MSN_STATUS' in rpx_results.columns and 'False Fail?' in rpx_results.columns:
        for msn_status in rpx_results['MSN_STATUS'].unique():
            msn_results = rpx_results[rpx_results['MSN_STATUS'] == msn_status]
            msn_verified = (msn_results['False Fail?'] == 'True').sum()
            msn_total = len(msn_results)
            details.append({
                'msn_status': msn_status,
                'verified': msn_verified,
                'total': msn_total,
                'rate': msn_verified / msn_total if msn_total > 0 else 0
            })

    return {
        'verified_false_fails': verified_false_fails,
        'total_fails_screened': total_fails_screened,
        'rpx_recovery_rate': round(rpx_recovery_rate, 4),
        'rpx_dpm': round(rpx_dpm, 2),
        'details': details
    }


def calculate_hybrid_dpm(
    msn_corr_df: pd.DataFrame,
    fid_counts_df: pd.DataFrame,
    step: str,
    total_muin: int = None,
    total_uin: int = None,
    design_id: str = None
) -> pd.DataFrame:
    """
    Calculate DPM using Kevin Roos's approach:
    - ALL failure types: Unique failing MSNs / Total FIDs × 1M
    - Each MSN is counted ONCE per MSN_STATUS (no double-counting)

    This aligns with Kevin's DPM table generator methodology where
    DPM = (num_failing_msns / total_fids) * 1_000_000

    Args:
        msn_corr_df: FAILCRAWLER × MSN_STATUS correlation data
        fid_counts_df: FID-level data with unique module/FID counts
        step: Test step (HMFN, HMB1, QMON)
        total_muin: Total modules tested (not used in Kevin's approach)
        total_uin: Total FIDs tested (denominator)
        design_id: Optional design ID filter

    Returns:
        DataFrame with MSN_STATUS, Level, Count, DPM, Percent columns
    """
    if msn_corr_df.empty or not total_uin:
        return pd.DataFrame()

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
    if step_col in msn_corr_df.columns:
        df = msn_corr_df[msn_corr_df[step_col].str.upper() == step.upper()].copy()
    else:
        df = msn_corr_df.copy()

    if df.empty:
        return pd.DataFrame()

    # Filter by design_id if provided
    if design_id and 'DESIGN_ID' in df.columns:
        df = df[df['DESIGN_ID'] == design_id]

    # Exclude Pass
    df = df[df['MSN_STATUS'] != 'Pass']

    if df.empty:
        return pd.DataFrame()

    # Get unique modules per MSN_STATUS from fid_counts_df
    step_fid_counts = pd.DataFrame()
    if not fid_counts_df.empty:
        fid_step_col = 'STEP' if 'STEP' in fid_counts_df.columns else None
        if fid_step_col:
            step_fid_counts = fid_counts_df[fid_counts_df[fid_step_col].str.upper() == step.upper()]

    results = []
    for msn_status in df['MSN_STATUS'].unique():
        if pd.isna(msn_status):
            continue

        msn_df = df[df['MSN_STATUS'] == msn_status]
        is_module_level = msn_status in MODULE_LEVEL_FAILURES

        # Get unique module count - Kevin's approach uses unique MSNs for ALL failure types
        unique_modules = 0
        if not step_fid_counts.empty and 'MSN_STATUS' in step_fid_counts.columns:
            status_counts = step_fid_counts[step_fid_counts['MSN_STATUS'] == msn_status]
            if not status_counts.empty and 'UNIQUE_MODULES' in status_counts.columns:
                unique_modules = int(status_counts['UNIQUE_MODULES'].sum())

        # Fallback to UFAIL if unique_modules not available
        ufail = int(pd.to_numeric(msn_df['UFAIL'], errors='coerce').sum()) if 'UFAIL' in msn_df.columns else 0

        # Kevin's approach: Always use unique MSNs / total FIDs × 1M
        # Each MSN is counted ONCE regardless of how many FIDs failed
        count = unique_modules if unique_modules > 0 else ufail
        dpm = (count / total_uin) * 1_000_000
        level = 'MODULE' if is_module_level else 'FID'
        count_label = f'{count} MSNs'

        results.append({
            'MSN_STATUS': msn_status,
            'Level': level,
            'Count': count_label,
            'DPM': round(dpm, 2),
            'raw_count': count
        })

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values('DPM', ascending=False)
        total_dpm = result_df['DPM'].sum()
        result_df['Percent'] = (result_df['DPM'] / total_dpm * 100).round(1) if total_dpm > 0 else 0

    return result_df


def calculate_recovery_projection(
    hybrid_dpm_df: pd.DataFrame,
    msn_corr_df: pd.DataFrame,
    step: str,
    verified_rpx_data: dict = None,
    total_muin: int = 0
) -> dict:
    """
    Calculate recovery projections based on hybrid DPM data.

    Recovery Types:
    - New RPx (VERIFIED): False miscompare detection via script - actual verified data
    - New BIOS (PROJECTED): Targets MULTI_BANK_MULTI_DQ FAILCRAWLER (100%) and BANK/BURST/PERIPH (50%)
    - HW+SOP (PROJECTED): Targets Hang MSN_STATUS

    DPM Calculation (Kevin Roos Method):
        DPM = (Unique Failing MSNs / Total FIDs) × 1,000,000
        - Reflects fail mode severity relative to component population

    Yield Recovery Calculation (Module-Based):
        Yield (%) = (Recovered MSNs / Total MUIN) × 100
        - Reflects actual manufacturing yield impact

    Args:
        hybrid_dpm_df: Output from calculate_hybrid_dpm()
        msn_corr_df: FAILCRAWLER × MSN_STATUS correlation data
        step: Test step
        verified_rpx_data: Optional verified RPx recovery from false miscompare script
        total_muin: Total modules tested (MUIN) for yield calculation

    Returns:
        Dictionary with recovery projections (DPM and Yield)
    """
    if hybrid_dpm_df.empty:
        return None

    total_dpm = hybrid_dpm_df['DPM'].sum()

    # Calculate HW+SOP recovery (Hang MSN_STATUS) - directly from hybrid DPM
    hw_sop_df = hybrid_dpm_df[hybrid_dpm_df['MSN_STATUS'].isin(HW_SOP_MSN_STATUS)]
    hw_sop_dpm = hw_sop_df['DPM'].sum() if not hw_sop_df.empty else 0

    # Build MSN_STATUS → DPM lookup from hybrid_dpm_df
    # This respects the hybrid approach (MODULE-level vs FID-level denominator)
    msn_dpm_lookup = dict(zip(hybrid_dpm_df['MSN_STATUS'], hybrid_dpm_df['DPM']))

    # RPx Recovery (VERIFIED) - from false miscompare script
    # This is actual verified data, not projected
    rpx_dpm = 0
    rpx_verified_count = 0
    rpx_screened_count = 0
    rpx_recovery_rate = 0.0

    if verified_rpx_data:
        rpx_verified_count = verified_rpx_data.get('verified_false_fails', 0)
        rpx_screened_count = verified_rpx_data.get('total_fails_screened', 0)
        rpx_recovery_rate = verified_rpx_data.get('rpx_recovery_rate', 0.0)

        # Calculate RPx DPM: apply verified recovery rate to eligible MSN_STATUS
        # RPx applies to all non-Hang, non-Boot, non-Pass MSN_STATUS
        rpx_eligible_dpm = sum(
            dpm for msn, dpm in msn_dpm_lookup.items()
            if msn not in RPX_EXCLUDE_MSN_STATUS and msn != 'Pass'
        )
        rpx_dpm = rpx_eligible_dpm * rpx_recovery_rate

    # Calculate BIOS recovery from FAILCRAWLER data (PROJECTED)
    # Recovery is calculated PER MSN_STATUS to respect hybrid DPM levels
    bios_dpm = 0
    bios_partial_dpm = 0  # 50% recovery for BANK/BURST/PERIPH patterns

    # Helper to check BIOS 50% pattern match
    def matches_bios_50_pattern_inner(fc):
        if fc in BIOS_FIX_FAILCRAWLERS_100PCT:
            return False  # Already counted at 100%
        fc_upper = str(fc).upper()
        return any(pattern in fc_upper for pattern in BIOS_FIX_PATTERNS_50PCT)

    if not msn_corr_df.empty:
        step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
        step_df = msn_corr_df[msn_corr_df[step_col].str.upper() == step.upper()] if step_col in msn_corr_df.columns else msn_corr_df

        # Check if data is in WIDE format (FAILCRAWLER names as columns)
        # Detect FAILCRAWLER columns by excluding known metadata columns
        metadata_cols = {
            'MSN_STATUS', 'MFG_WORKWEEK', 'STEP', 'QUERY_STEP', 'UIN', 'MUIN',
            'UFAIL', 'MUFAIL', 'TOTAL', 'DPM', 'MDPM', 'CDPM', 'YIELD', 'PASS',
            'DESIGN_ID', 'FORM_FACTOR', 'DENSITY', 'SPEED', 'FACILITY', 'DBASE',
            'COUNT', 'PERCENT', 'LEVEL', 'RAW_COUNT', 'INDEX'
        }
        # Filter to only columns with numeric data (likely FAILCRAWLERs)
        fc_columns = []
        for col in step_df.columns:
            if col.upper() not in metadata_cols:
                try:
                    col_numeric = pd.to_numeric(step_df[col], errors='coerce')
                    if col_numeric.notna().any() and col_numeric.sum() > 0:
                        fc_columns.append(col)
                except (TypeError, ValueError):
                    pass

        if fc_columns and 'MSN_STATUS' in step_df.columns and 'FAILCRAWLER' not in step_df.columns:
            # WIDE FORMAT: FAILCRAWLER names as columns with DPM values
            for _, row in step_df.iterrows():
                msn_status = row.get('MSN_STATUS')
                if pd.isna(msn_status) or msn_status == 'Pass' or msn_status in HW_SOP_MSN_STATUS or msn_status == 'Total':
                    continue

                msn_dpm = msn_dpm_lookup.get(msn_status, 0)
                if msn_dpm == 0:
                    continue

                # Sum all FAILCRAWLER DPM values for this MSN_STATUS (for proportion calc)
                total_fc_dpm = 0
                for fc in fc_columns:
                    try:
                        val = pd.to_numeric(row.get(fc, 0), errors='coerce')
                        if pd.notna(val):
                            total_fc_dpm += val
                    except (TypeError, ValueError):
                        pass
                if total_fc_dpm == 0:
                    continue

                # Calculate BIOS recovery based on FAILCRAWLER proportions
                for fc in fc_columns:
                    try:
                        fc_val = pd.to_numeric(row.get(fc, 0), errors='coerce')
                        if pd.isna(fc_val) or fc_val <= 0:
                            continue
                    except (TypeError, ValueError):
                        continue

                    # FAILCRAWLER's share of this MSN_STATUS's DPM
                    fc_share = fc_val / total_fc_dpm
                    fc_dpm = fc_share * msn_dpm

                    # Classify BIOS recovery type
                    if fc.upper() in [f.upper() for f in BIOS_FIX_FAILCRAWLERS_100PCT]:
                        bios_dpm += fc_dpm  # 100% recovery
                    elif matches_bios_50_pattern_inner(fc):
                        bios_partial_dpm += fc_dpm * BIOS_PARTIAL_RECOVERY_RATE  # 50% recovery

        elif 'FAILCRAWLER' in step_df.columns:
            # LONG FORMAT: FAILCRAWLER as a column
            for msn_status in step_df['MSN_STATUS'].unique():
                if msn_status == 'Pass' or msn_status in HW_SOP_MSN_STATUS:
                    continue

                msn_dpm = msn_dpm_lookup.get(msn_status, 0)
                if msn_dpm == 0:
                    continue

                msn_fc_df = step_df[step_df['MSN_STATUS'] == msn_status]
                msn_total_ufail = msn_fc_df['UFAIL'].sum() if 'UFAIL' in msn_fc_df.columns else 0

                if msn_total_ufail == 0:
                    continue

                for _, row in msn_fc_df.iterrows():
                    fc = row['FAILCRAWLER']
                    ufail = row.get('UFAIL', 0)
                    if ufail == 0:
                        continue

                    fc_dpm = (ufail / msn_total_ufail) * msn_dpm

                    if fc in BIOS_FIX_FAILCRAWLERS_100PCT:
                        bios_dpm += fc_dpm
                    elif matches_bios_50_pattern_inner(fc):
                        bios_partial_dpm += fc_dpm * BIOS_PARTIAL_RECOVERY_RATE

    # Total BIOS = 100% recovery + 50% partial recovery
    total_bios_dpm = bios_dpm + bios_partial_dpm
    combined_dpm = rpx_dpm + total_bios_dpm + hw_sop_dpm
    remaining_dpm = total_dpm - combined_dpm

    # Helper to check if FAILCRAWLER matches 50% BIOS patterns
    def matches_bios_50_pattern(fc):
        if fc in BIOS_FIX_FAILCRAWLERS_100PCT:
            return False  # Already counted at 100%
        fc_upper = str(fc).upper()
        return any(pattern in fc_upper for pattern in BIOS_FIX_PATTERNS_50PCT)

    # Build breakdown with recovery info
    breakdown = []
    for _, row in hybrid_dpm_df.iterrows():
        msn_status = row['MSN_STATUS']
        is_rpx = False
        is_bios_100 = False
        is_bios_50 = False
        is_hw_sop = msn_status in HW_SOP_MSN_STATUS
        is_rpx_eligible = msn_status not in RPX_EXCLUDE_MSN_STATUS and msn_status != 'Pass'

        # RPx is verified - applies to all eligible MSN_STATUS based on verified rate
        # Show RPx as verified if we have verified data and this MSN_STATUS is eligible
        if is_rpx_eligible and verified_rpx_data and rpx_recovery_rate > 0:
            is_rpx = True

        # Check FAILCRAWLERs for BIOS recovery (PROJECTED)
        if not msn_corr_df.empty and not is_hw_sop:
            step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
            step_df = msn_corr_df[msn_corr_df[step_col].str.upper() == step.upper()] if step_col in msn_corr_df.columns else msn_corr_df
            status_df = step_df[step_df['MSN_STATUS'] == msn_status]

            if not status_df.empty:
                # Check for LONG format (FAILCRAWLER column exists)
                if 'FAILCRAWLER' in status_df.columns:
                    fcs = set(status_df['FAILCRAWLER'].unique())
                    is_bios_100 = bool(fcs & BIOS_FIX_FAILCRAWLERS_100PCT)
                    is_bios_50 = any(matches_bios_50_pattern(fc) for fc in fcs) and not is_bios_100
                else:
                    # WIDE format: FAILCRAWLER names are column names
                    # Detect all potential FAILCRAWLER columns (exclude known metadata columns)
                    metadata_cols = {
                        'MSN_STATUS', 'MFG_WORKWEEK', 'STEP', 'QUERY_STEP', 'UIN', 'MUIN',
                        'UFAIL', 'MUFAIL', 'TOTAL', 'DPM', 'MDPM', 'CDPM', 'YIELD', 'PASS',
                        'DESIGN_ID', 'FORM_FACTOR', 'DENSITY', 'SPEED', 'FACILITY', 'DBASE',
                        'COUNT', 'PERCENT', 'LEVEL', 'RAW_COUNT', 'INDEX'
                    }
                    # Find numeric columns that aren't metadata - these are likely FAILCRAWLERs
                    fc_cols = []
                    for col in status_df.columns:
                        if col.upper() not in metadata_cols:
                            # Check if column has numeric-like values
                            try:
                                col_sum = pd.to_numeric(status_df[col], errors='coerce').sum()
                                if col_sum > 0:
                                    fc_cols.append(col)
                            except (TypeError, ValueError):
                                pass

                    # Build set of FAILCRAWLERs with non-zero values
                    fcs = {col.upper() for col in fc_cols}

                    is_bios_100 = bool(fcs & {f.upper() for f in BIOS_FIX_FAILCRAWLERS_100PCT})
                    is_bios_50 = any(matches_bios_50_pattern(fc) for fc in fcs) and not is_bios_100

        # ROW MSN_STATUS: BIOS patterns do NOT apply - only RPx can recover Row failures
        # Row failures are potential DRAM issues; SINGLE_BURST_SINGLE_ROW contains "BURST"
        # but since MSN_STATUS=Row, the failure is classified as row-level, not burst-recoverable
        if msn_status in DRAM_MSN_STATUS:
            is_bios_100 = False
            is_bios_50 = False

        # Check for DRAM-related failures (potential actual silicon issues - NO recovery)
        # - SB/DB FAILCRAWLERs: Single Bit / Double Bit errors (can't be fixed by RPx)
        # - ROW MSN_STATUS: Row failures that don't get cleared by RPx
        is_dram_fc = False  # DRAM FAILCRAWLER (SB/DB) - takes priority over RPx
        is_dram_row = False  # ROW MSN_STATUS without other recovery

        if not msn_corr_df.empty and not is_hw_sop:
            step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
            step_df = msn_corr_df[msn_corr_df[step_col].str.upper() == step.upper()] if step_col in msn_corr_df.columns else msn_corr_df
            status_df = step_df[step_df['MSN_STATUS'] == msn_status]

            if not status_df.empty:
                # Check for DRAM FAILCRAWLERs (SB, DB) - these are actual bit errors
                if 'FAILCRAWLER' in status_df.columns:
                    fcs = set(status_df['FAILCRAWLER'].str.upper().unique())
                    is_dram_fc = bool(fcs & {f.upper() for f in DRAM_FAILCRAWLERS})
                else:
                    # Wide format - check column names
                    for col in status_df.columns:
                        if col.upper() in {f.upper() for f in DRAM_FAILCRAWLERS}:
                            try:
                                if pd.to_numeric(status_df[col], errors='coerce').sum() > 0:
                                    is_dram_fc = True
                                    break
                            except (TypeError, ValueError):
                                pass

        # ROW MSN_STATUS without RPx recovery is DRAM-related (potential silicon issue)
        # Note: BIOS patterns don't apply to ROW - only RPx verification can recover
        if msn_status in DRAM_MSN_STATUS and not is_rpx:
            is_dram_row = True

        is_dram = is_dram_fc or is_dram_row

        # Determine recovery type and calculate recovered DPM
        # Priority: HW+SOP > DRAM FC (SB/DB) > BIOS 100% > BIOS 50% > RPx > DRAM ROW > None
        # Note: SB/DB FAILCRAWLERs are actual silicon failures - RPx can't fix them
        if is_hw_sop:
            recovery_type = 'HW+SOP'
            recovered_dpm = row['DPM']
        elif is_dram_fc:
            # SB/DB FAILCRAWLERs = actual DRAM failures, no recovery possible
            recovery_type = 'No Recovery'
            recovered_dpm = 0
        elif is_bios_100:
            recovery_type = 'BIOS'
            recovered_dpm = row['DPM']
        elif is_bios_50:
            recovery_type = 'BIOS*'  # Asterisk indicates 50% recovery
            recovered_dpm = row['DPM'] * BIOS_PARTIAL_RECOVERY_RATE
        elif is_rpx:
            recovery_type = 'RPx✓'  # Checkmark indicates verified
            recovered_dpm = row['DPM'] * rpx_recovery_rate
        elif is_dram_row:
            # ROW MSN_STATUS without other recovery = potential DRAM failure
            recovery_type = 'No Recovery'
            recovered_dpm = 0
        else:
            recovery_type = None
            recovered_dpm = 0

        # Calculate recovered MSNs for yield (module-based)
        # Use raw_count (numeric) instead of Count (formatted string like "30 MSNs")
        msn_count = float(row.get('raw_count', 0)) if pd.notna(row.get('raw_count')) else 0
        recovered_msns = 0.0
        if is_hw_sop:
            recovered_msns = msn_count  # 100% recovery
        elif is_bios_100:
            recovered_msns = msn_count  # 100% recovery
        elif is_bios_50:
            recovered_msns = msn_count * BIOS_PARTIAL_RECOVERY_RATE  # 50% recovery
        elif is_rpx:
            recovered_msns = msn_count * rpx_recovery_rate  # Verified rate recovery
        # DRAM: No recovery (recovered_msns stays 0)

        breakdown.append({
            'msn_status': msn_status,
            'level': row['Level'],
            'count': row['Count'],
            'dpm': row['DPM'],
            'percent': row.get('Percent', 0),
            'is_rpx_target': is_rpx,
            'is_rpx_eligible': is_rpx_eligible,
            'is_bios_target': is_bios_100 or is_bios_50,
            'is_bios_partial': is_bios_50,
            'is_hw_sop_target': is_hw_sop,
            'is_dram': is_dram,
            'recovery_type': recovery_type,
            'recovered_dpm': recovered_dpm,
            'recovered_msns': recovered_msns
        })

    # Calculate total failed MSNs (for actual yield calculation)
    # Note: This may overcount if same module fails multiple MSN_STATUS types
    # but it's a reasonable approximation for yield impact
    total_failed_msns = sum(
        float(row.get('raw_count', 0)) if pd.notna(row.get('raw_count')) else 0
        for _, row in hybrid_dpm_df.iterrows()
    )

    # Calculate summary DPMs FROM breakdown (using priority-based recovery_type assignments)
    # This prevents double-counting that occurs when calculating independently
    rpx_dpm_actual = sum(b['recovered_dpm'] for b in breakdown if b['recovery_type'] == 'RPx✓')
    bios_dpm_actual = sum(b['recovered_dpm'] for b in breakdown if b['recovery_type'] in ('BIOS', 'BIOS*'))
    hw_sop_dpm_actual = sum(b['recovered_dpm'] for b in breakdown if b['recovery_type'] == 'HW+SOP')
    dram_dpm = sum(b['dpm'] for b in breakdown if b['recovery_type'] == 'No Recovery')  # DRAM-related, no recovery

    # Combined = all recoverable DPM (excludes DRAM which has no recovery)
    combined_dpm_actual = rpx_dpm_actual + bios_dpm_actual + hw_sop_dpm_actual
    remaining_dpm_actual = total_dpm - combined_dpm_actual - dram_dpm  # Unknown failures

    # Calculate module-based yield recovery (MSNs / MUIN × 100)
    rpx_recovered_msns = sum(b['recovered_msns'] for b in breakdown if b['recovery_type'] == 'RPx✓')
    bios_recovered_msns = sum(b['recovered_msns'] for b in breakdown if b['recovery_type'] in ('BIOS', 'BIOS*'))
    hw_sop_recovered_msns = sum(b['recovered_msns'] for b in breakdown if b['recovery_type'] == 'HW+SOP')
    total_recovered_msns = rpx_recovered_msns + bios_recovered_msns + hw_sop_recovered_msns

    # Calculate yield percentages (module-based denominator)
    if total_muin > 0:
        rpx_yield = round(rpx_recovered_msns / total_muin * 100, 3)
        bios_yield = round(bios_recovered_msns / total_muin * 100, 3)
        hw_sop_yield = round(hw_sop_recovered_msns / total_muin * 100, 3)
        combined_yield = round(total_recovered_msns / total_muin * 100, 3)
    else:
        rpx_yield = bios_yield = hw_sop_yield = combined_yield = 0

    return {
        'step': step,
        'total_dpm': round(total_dpm, 2),
        'total_muin': total_muin,
        'rpx_dpm': round(rpx_dpm_actual, 2),
        'rpx_verified': True if verified_rpx_data else False,
        'rpx_verified_count': rpx_verified_count,
        'rpx_screened_count': rpx_screened_count,
        'rpx_recovery_rate': rpx_recovery_rate,
        'bios_dpm': round(bios_dpm_actual, 2),
        'bios_100_dpm': round(bios_dpm, 2),  # Keep for detail breakdown
        'bios_50_dpm': round(bios_partial_dpm, 2),
        'hw_sop_dpm': round(hw_sop_dpm_actual, 2),
        'dram_dpm': round(dram_dpm, 2),  # DRAM = no recovery
        'combined_dpm': round(combined_dpm_actual, 2),
        'remaining_dpm': round(remaining_dpm_actual, 2),
        'rpx_pct': round(rpx_dpm_actual / total_dpm * 100, 1) if total_dpm > 0 else 0,
        'bios_pct': round(bios_dpm_actual / total_dpm * 100, 1) if total_dpm > 0 else 0,
        'hw_sop_pct': round(hw_sop_dpm_actual / total_dpm * 100, 1) if total_dpm > 0 else 0,
        'dram_pct': round(dram_dpm / total_dpm * 100, 1) if total_dpm > 0 else 0,
        'combined_pct': round(combined_dpm_actual / total_dpm * 100, 1) if total_dpm > 0 else 0,
        'remaining_pct': round(remaining_dpm_actual / total_dpm * 100, 1) if total_dpm > 0 else 0,
        # Module-based yield recovery (percentage points)
        'rpx_yield': rpx_yield,
        'bios_yield': bios_yield,
        'hw_sop_yield': hw_sop_yield,
        'combined_yield': combined_yield,
        'rpx_recovered_msns': round(rpx_recovered_msns, 1),
        'bios_recovered_msns': round(bios_recovered_msns, 1),
        'hw_sop_recovered_msns': round(hw_sop_recovered_msns, 1),
        'total_recovered_msns': round(total_recovered_msns, 1),
        'total_failed_msns': round(total_failed_msns, 1),
        'breakdown': breakdown
    }


def create_hybrid_dpm_table_html(
    hybrid_dpm_df: pd.DataFrame,
    step: str,
    total_uin: int = None,
    dark_mode: bool = False
) -> str:
    """
    Create HTML table for hybrid DPM data.

    Args:
        hybrid_dpm_df: Output from calculate_hybrid_dpm()
        step: Test step
        total_uin: Total UIN for display
        dark_mode: Use dark mode styling

    Returns:
        HTML string for the table
    """
    if hybrid_dpm_df.empty:
        return "<p style='color: #888;'>No DPM data available</p>"

    bg_color = '#1e1e1e' if dark_mode else '#ffffff'
    text_color = '#e0e0e0' if dark_mode else '#1a1a1a'
    border_color = '#444' if dark_mode else '#e0e0e0'
    header_bg = '#283593' if dark_mode else '#1a237e'

    total_dpm = hybrid_dpm_df['DPM'].sum()

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 12px;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <span style="font-size: 12px; font-weight: 600; color: {text_color};">
                📊 Hybrid DPM - {step}
            </span>
            <span style="font-size: 10px; color: #888;">
                Total UIN: {total_uin:,} FIDs
            </span>
        </div>
        <table style="width: 100%; border-collapse: collapse; font-size: 10px;">
            <thead>
                <tr style="background: {header_bg};">
                    <th style="padding: 6px; text-align: left; color: white; border-radius: 4px 0 0 0;">MSN_STATUS</th>
                    <th style="padding: 6px; text-align: center; color: white;">Level</th>
                    <th style="padding: 6px; text-align: right; color: white;">Count</th>
                    <th style="padding: 6px; text-align: right; color: white;">DPM</th>
                    <th style="padding: 6px; text-align: right; color: white; border-radius: 0 4px 0 0;">%</th>
                </tr>
            </thead>
            <tbody>
    '''

    for i, (_, row) in enumerate(hybrid_dpm_df.iterrows()):
        row_bg = '#f5f5f5' if i % 2 == 0 else '#ffffff'
        if dark_mode:
            row_bg = '#2d2d2d' if i % 2 == 0 else '#1e1e1e'

        level_badge = '<span style="background: #9c27b0; color: white; padding: 1px 4px; border-radius: 2px; font-size: 8px;">M</span>' if row['Level'] == 'MODULE' else '<span style="background: #00897b; color: white; padding: 1px 4px; border-radius: 2px; font-size: 8px;">F</span>'

        html += f'''
            <tr style="background: {row_bg};">
                <td style="padding: 5px 6px; color: {text_color}; border-bottom: 1px solid {border_color};">{row['MSN_STATUS']}</td>
                <td style="padding: 5px 6px; text-align: center; border-bottom: 1px solid {border_color};">{level_badge}</td>
                <td style="padding: 5px 6px; text-align: right; color: #888; border-bottom: 1px solid {border_color};">{row['Count']}</td>
                <td style="padding: 5px 6px; text-align: right; color: {text_color}; font-weight: 600; border-bottom: 1px solid {border_color};">{row['DPM']:.2f}</td>
                <td style="padding: 5px 6px; text-align: right; color: #888; border-bottom: 1px solid {border_color};">{row['Percent']:.1f}%</td>
            </tr>
        '''

    # Total row
    html += f'''
            <tr style="background: {'#363636' if dark_mode else '#e8eaf6'}; font-weight: bold;">
                <td style="padding: 6px; color: {text_color};">TOTAL</td>
                <td style="padding: 6px;"></td>
                <td style="padding: 6px;"></td>
                <td style="padding: 6px; text-align: right; color: {text_color};">{total_dpm:.2f}</td>
                <td style="padding: 6px; text-align: right; color: {text_color};">100%</td>
            </tr>
        </tbody>
    </table>
    <div style="margin-top: 6px; font-size: 9px; color: #888;">
        <b>DPM = Unique MSNs / Total FIDs × 1M</b> (Kevin Roos method) |
        <span style="background: #9c27b0; color: white; padding: 0 3px; border-radius: 2px;">M</span> Module-level |
        <span style="background: #00897b; color: white; padding: 0 3px; border-radius: 2px;">F</span> FID-level
    </div>
    </div>
    '''

    return html


def create_recovery_projection_html(
    recovery_data: dict,
    dark_mode: bool = False
) -> str:
    """
    Create HTML visualization for cDPM recovery projection.

    Args:
        recovery_data: Output from calculate_recovery_projection()
        dark_mode: Use dark mode styling

    Returns:
        HTML string for the recovery projection card
    """
    if not recovery_data:
        return ""

    step = recovery_data.get('step', '')
    total_dpm = recovery_data['total_dpm']
    rpx_dpm = recovery_data.get('rpx_dpm', 0)
    bios_dpm = recovery_data['bios_dpm']
    hw_sop_dpm = recovery_data['hw_sop_dpm']
    combined_dpm = recovery_data['combined_dpm']
    remaining_dpm = recovery_data['remaining_dpm']
    rpx_pct = recovery_data.get('rpx_pct', 0)
    bios_pct = recovery_data['bios_pct']
    hw_sop_pct = recovery_data['hw_sop_pct']
    combined_pct = recovery_data['combined_pct']
    remaining_pct = recovery_data['remaining_pct']
    breakdown = recovery_data.get('breakdown', [])

    bg_color = '#1e1e1e' if dark_mode else '#ffffff'
    text_color = '#e0e0e0' if dark_mode else '#1a1a1a'
    border_color = '#444' if dark_mode else '#e0e0e0'
    header_bg = '#283593' if dark_mode else '#1a237e'

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 16px; font-family: Arial, sans-serif;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            <span style="font-size: 14px; font-weight: 600; color: {text_color};">
                🔮 Recovery Projection - {step}
            </span>
            <span style="font-size: 11px; color: #888;">
                {total_dpm:.1f} → {remaining_dpm:.1f} DPM ({combined_pct:.0f}% recoverable)
            </span>
        </div>

        <!-- Summary Cards -->
        <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 6px; margin-bottom: 12px;">
            <!-- New RPx Fix -->
            <div style="background: linear-gradient(135deg, #e8f5e9, #c8e6c9); padding: 8px; border-radius: 6px; border-left: 4px solid #4caf50;">
                <div style="font-size: 16px; font-weight: bold; color: #2e7d32;">{rpx_dpm:.1f} <span style="font-size: 11px; font-weight: normal;">({rpx_pct:.0f}%)</span></div>
                <div style="font-size: 9px; color: #388e3c;">New RPx Fix</div>
                <div style="font-size: 8px; color: #666; margin-top: 2px;">
                    <span style="background: #4caf50; color: white; padding: 1px 3px; border-radius: 2px; font-size: 7px;">VERIFIED</span>
                </div>
            </div>

            <!-- New BIOS Fix -->
            <div style="background: linear-gradient(135deg, #e3f2fd, #bbdefb); padding: 8px; border-radius: 6px; border-left: 4px solid #1976d2;">
                <div style="font-size: 16px; font-weight: bold; color: #1565c0;">{bios_dpm:.1f} <span style="font-size: 11px; font-weight: normal;">({bios_pct:.0f}%)</span></div>
                <div style="font-size: 9px; color: #1976d2;">New BIOS Fix</div>
                <div style="font-size: 8px; color: #666; margin-top: 2px;">
                    <span style="background: #ff9800; color: white; padding: 1px 3px; border-radius: 2px; font-size: 7px;">PROJECTED</span>
                </div>
            </div>

            <!-- HW+SOP Fix -->
            <div style="background: linear-gradient(135deg, #fce4ec, #f8bbd0); padding: 8px; border-radius: 6px; border-left: 4px solid #c2185b;">
                <div style="font-size: 16px; font-weight: bold; color: #ad1457;">{hw_sop_dpm:.1f} <span style="font-size: 11px; font-weight: normal;">({hw_sop_pct:.0f}%)</span></div>
                <div style="font-size: 9px; color: #c2185b;">HW+SOP Fix</div>
                <div style="font-size: 8px; color: #666; margin-top: 2px;">
                    <span style="background: #ff9800; color: white; padding: 1px 3px; border-radius: 2px; font-size: 7px;">PROJECTED</span>
                </div>
            </div>

            <!-- Combined -->
            <div style="background: linear-gradient(135deg, #ede7f6, #d1c4e9); padding: 8px; border-radius: 6px; border-left: 4px solid #7b1fa2;">
                <div style="font-size: 16px; font-weight: bold; color: #6a1b9a;">{combined_dpm:.1f} <span style="font-size: 11px; font-weight: normal;">({combined_pct:.0f}%)</span></div>
                <div style="font-size: 9px; color: #7b1fa2;">Total Recoverable</div>
            </div>

            <!-- Remaining -->
            <div style="background: linear-gradient(135deg, #fff3e0, #ffe0b2); padding: 8px; border-radius: 6px; border-left: 4px solid #f57c00;">
                <div style="font-size: 16px; font-weight: bold; color: #e65100;">{remaining_dpm:.1f} <span style="font-size: 11px; font-weight: normal;">({remaining_pct:.0f}%)</span></div>
                <div style="font-size: 9px; color: #f57c00;">Remaining</div>
            </div>
        </div>

        <!-- Recovery Progress Bar -->
        <div style="background: #eceff1; border-radius: 4px; height: 24px; overflow: hidden; position: relative; margin-bottom: 12px;">
    '''

    # Progress bar segments
    if total_dpm > 0:
        html += f'''
            <div style="position: absolute; left: 0; top: 0; height: 100%; width: {rpx_pct}%; background: linear-gradient(90deg, #388e3c, #4caf50); display: flex; align-items: center; justify-content: center;">
                <span style="color: white; font-size: 8px; font-weight: bold;">{rpx_pct:.0f}% RPx</span>
            </div>
            <div style="position: absolute; left: {rpx_pct}%; top: 0; height: 100%; width: {bios_pct}%; background: linear-gradient(90deg, #1976d2, #42a5f5); display: flex; align-items: center; justify-content: center;">
                <span style="color: white; font-size: 8px; font-weight: bold;">{bios_pct:.0f}% BIOS</span>
            </div>
            <div style="position: absolute; left: {rpx_pct + bios_pct}%; top: 0; height: 100%; width: {hw_sop_pct}%; background: linear-gradient(90deg, #c2185b, #e91e63); display: flex; align-items: center; justify-content: center;">
                <span style="color: white; font-size: 8px; font-weight: bold;">{hw_sop_pct:.0f}% HW</span>
            </div>
            <div style="position: absolute; left: {rpx_pct + bios_pct + hw_sop_pct}%; top: 0; height: 100%; width: {remaining_pct}%; background: #bdbdbd; display: flex; align-items: center; justify-content: center;">
                <span style="color: #424242; font-size: 8px; font-weight: bold;">{remaining_pct:.0f}% Remaining</span>
            </div>
        '''

    html += '</div>'

    # Breakdown Table
    if breakdown:
        html += f'''
        <div style="margin-top: 12px;">
            <div style="font-size: 11px; font-weight: 600; color: {text_color}; margin-bottom: 6px;">Recovery Breakdown by MSN_STATUS</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 10px;">
                <thead>
                    <tr style="background: {header_bg};">
                        <th style="padding: 6px; text-align: left; color: white; border-radius: 4px 0 0 0;">MSN_STATUS</th>
                        <th style="padding: 6px; text-align: center; color: white;">Level</th>
                        <th style="padding: 6px; text-align: right; color: white;">DPM</th>
                        <th style="padding: 6px; text-align: center; color: white;">Recovery</th>
                        <th style="padding: 6px; text-align: right; color: white; border-radius: 0 4px 0 0;">Recovered</th>
                    </tr>
                </thead>
                <tbody>
        '''

        for i, item in enumerate(breakdown[:8]):
            row_bg = '#f5f5f5' if i % 2 == 0 else '#ffffff'
            if dark_mode:
                row_bg = '#2d2d2d' if i % 2 == 0 else '#1e1e1e'

            recovery_badge = ''
            recovered_dpm = item.get('recovered_dpm', 0)
            if item.get('is_hw_sop_target'):
                recovery_badge = '<span style="background: #c2185b; color: white; padding: 2px 6px; border-radius: 3px; font-size: 8px;">HW+SOP</span>'
            elif item.get('is_bios_target') or item.get('is_bios_partial'):
                # Combined BIOS badge (100% + 50% under one label per VP request)
                recovery_badge = '<span style="background: #1976d2; color: white; padding: 2px 6px; border-radius: 3px; font-size: 8px;">BIOS</span>'
            elif item.get('is_rpx_target'):
                recovery_badge = '<span style="background: #4caf50; color: white; padding: 2px 6px; border-radius: 3px; font-size: 8px;">RPx</span>'
            elif item.get('is_dram'):
                recovery_badge = '<span style="background: #d32f2f; color: white; padding: 2px 6px; border-radius: 3px; font-size: 8px;">No Recovery<br><span style="font-size: 6px; opacity: 0.8;">(DRAM)</span></span>'
            else:
                recovery_badge = '<span style="color: #888;">-</span>'
                recovered_dpm = 0

            level_badge = '<span style="background: #9c27b0; color: white; padding: 1px 4px; border-radius: 2px; font-size: 8px;">M</span>' if item['level'] == 'MODULE' else '<span style="background: #00897b; color: white; padding: 1px 4px; border-radius: 2px; font-size: 8px;">F</span>'

            html += f'''
                <tr style="background: {row_bg};">
                    <td style="padding: 5px 6px; color: {text_color}; border-bottom: 1px solid {border_color};">{item['msn_status']}</td>
                    <td style="padding: 5px 6px; text-align: center; border-bottom: 1px solid {border_color};">{level_badge}</td>
                    <td style="padding: 5px 6px; text-align: right; color: {text_color}; border-bottom: 1px solid {border_color};">{item['dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: center; border-bottom: 1px solid {border_color};">{recovery_badge}</td>
                    <td style="padding: 5px 6px; text-align: right; color: {'#4caf50' if recovered_dpm > 0 else '#888'}; font-weight: {'bold' if recovered_dpm > 0 else 'normal'}; border-bottom: 1px solid {border_color};">{f'{recovered_dpm:.2f}' if recovered_dpm > 0 else '-'}</td>
                </tr>
            '''

        html += '''
                </tbody>
            </table>
        </div>
        '''

    html += '''
        <div style="margin-top: 8px; font-size: 9px; color: #888; text-align: right;">
            <b>RPx:</b> False miscompare | <b>BIOS:</b> MULTI_BANK_MULTI_DQ + Bank/Burst/Periph patterns | <b>HW+SOP:</b> Hang | <b style="color:#d32f2f;">No Recovery:</b> SB/DB/Row (DRAM-related)
        </div>
    </div>
    '''

    return html


def create_dpm_formula_info_html(dark_mode: bool = False) -> str:
    """
    Create an HTML info box explaining DPM calculation methodology.

    Args:
        dark_mode: Whether to use dark mode styling

    Returns:
        HTML string with DPM formula explanation
    """
    bg_color = '#1e1e1e' if dark_mode else '#f8f9fa'
    border_color = '#444' if dark_mode else '#e0e0e0'
    text_color = '#e0e0e0' if dark_mode else '#333'
    muted_color = '#888' if dark_mode else '#666'
    highlight_bg = '#2d2d2d' if dark_mode else '#fff'
    accent_color = '#4fc3f7' if dark_mode else '#1976d2'
    warning_bg = '#fff3e0' if not dark_mode else '#3e2723'

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 12px; font-family: 'Segoe UI', sans-serif; font-size: 10px;">
        <div style="font-size: 12px; font-weight: 600; color: {accent_color}; margin-bottom: 10px;">
            📐 DPM & Yield Calculation Methodology
        </div>

        <!-- Kevin Roos DPM Method -->
        <div style="background: {highlight_bg}; border-radius: 6px; padding: 10px; margin-bottom: 10px; border-left: 3px solid {accent_color};">
            <div style="font-size: 10px; font-weight: 600; color: {accent_color}; margin-bottom: 6px;">
                DPM Calculation (Kevin Roos Method)
            </div>
            <div style="font-family: monospace; font-size: 11px; color: {text_color}; background: {bg_color}; padding: 6px; border-radius: 4px; margin-bottom: 6px;">
                DPM = (Unique Failing MSNs / Total FIDs) × 1,000,000
            </div>
            <div style="font-size: 9px; color: {muted_color};">
                • Each MSN (module) is counted <b>once</b> regardless of how many FIDs failed<br>
                • Denominator is Total FIDs (component UIN) from <code>+fidag</code><br>
                • Essentially mDPM ÷ (FIDs per MSN)
            </div>
            <div style="font-size: 9px; color: {muted_color}; margin-top: 6px; padding-top: 6px; border-top: 1px dashed {border_color};">
                <b>Why count unique MSNs?</b> For Mod-Sys, Hang, and Multi-Mod fails, we assume the failure occurs at the MSN level.
                Analysis shows that fails on different FIDs within the same MSN (or different packages in the same MSN) are
                <b>not independent events</b> — FIDs and packages fail together from a single event or cause.
                Counting each failed FID would overcount the actual failure rate.
            </div>
        </div>

        <!-- Yield Recovery Calculation -->
        <div style="background: {warning_bg}; border-radius: 6px; padding: 10px; margin-bottom: 10px; border-left: 3px solid #ff9800;">
            <div style="font-size: 10px; font-weight: 600; color: #ff9800; margin-bottom: 6px;">
                ⚠️ Yield Recovery Calculation (Module-Based)
            </div>
            <div style="font-family: monospace; font-size: 11px; color: {text_color}; background: {bg_color}; padding: 6px; border-radius: 4px; margin-bottom: 6px;">
                Yield Recovery (%) = (Recovered MSNs / Total MUIN) × 100
            </div>
            <div style="font-size: 9px; color: {muted_color};">
                <b>Why module-based?</b> Yield is measured at the module level for shipping decisions.<br>
                • DPM uses FID denominator (for fail mode analysis)<br>
                • Yield uses MUIN denominator (for actual yield impact)<br>
                • Example: 40 modules recovered / 2,675 MUIN = <b>1.49% yield gain</b>
            </div>
        </div>

        <!-- DPM vs Yield Comparison -->
        <div style="display: flex; gap: 8px; margin-bottom: 10px;">
            <div style="flex: 1; background: {highlight_bg}; border-radius: 6px; padding: 8px;">
                <div style="font-size: 9px; color: {muted_color}; margin-bottom: 4px;">DPM (Fail Mode Analysis)</div>
                <div style="font-size: 10px; color: {text_color};">
                    Numerator: Unique MSNs<br>
                    Denominator: <b>Total FIDs</b>
                </div>
            </div>
            <div style="flex: 1; background: {highlight_bg}; border-radius: 6px; padding: 8px;">
                <div style="font-size: 9px; color: {muted_color}; margin-bottom: 4px;">Yield (Shipping Impact)</div>
                <div style="font-size: 10px; color: {text_color};">
                    Numerator: Unique MSNs<br>
                    Denominator: <b>Total MUIN</b>
                </div>
            </div>
        </div>

        <!-- MSN_STATUS Classification -->
        <div style="background: {highlight_bg}; border-radius: 6px; padding: 10px; margin-bottom: 10px; border-left: 3px solid #9c27b0;">
            <div style="font-size: 10px; font-weight: 600; color: #9c27b0; margin-bottom: 6px;">
                🏷️ MSN_STATUS Classification
            </div>
            <div style="display: flex; gap: 10px;">
                <div style="flex: 1;">
                    <div style="font-size: 9px; color: {muted_color}; margin-bottom: 4px;">
                        <span style="background: #9c27b0; color: white; padding: 1px 4px; border-radius: 2px; font-size: 8px;">M</span> Module-Level
                    </div>
                    <div style="font-size: 9px; color: {text_color};">
                        Mod-Sys, Hang, Multi-Mod, Boot
                    </div>
                </div>
                <div style="flex: 1;">
                    <div style="font-size: 9px; color: {muted_color}; margin-bottom: 4px;">
                        <span style="background: #00897b; color: white; padding: 1px 4px; border-radius: 2px; font-size: 8px;">F</span> FID-Level
                    </div>
                    <div style="font-size: 9px; color: {text_color};">
                        DQ, Row, SB_Int, Multi-DQ, SB, Col
                    </div>
                </div>
            </div>
        </div>

        <!-- Recovery Types -->
        <div style="background: {highlight_bg}; border-radius: 6px; padding: 10px; margin-bottom: 10px; border-left: 3px solid #4caf50;">
            <div style="font-size: 10px; font-weight: 600; color: #4caf50; margin-bottom: 6px;">
                🔧 Recovery Types & Targets
            </div>
            <table style="width: 100%; font-size: 9px; color: {text_color};">
                <tr style="border-bottom: 1px solid {border_color};">
                    <td style="padding: 4px 0; width: 30%;"><span style="background: #4caf50; color: white; padding: 1px 4px; border-radius: 2px;">RPx✓</span></td>
                    <td style="width: 35%;">All except Hang/Boot</td>
                    <td>False miscompare (VERIFIED)</td>
                </tr>
                <tr style="border-bottom: 1px solid {border_color};">
                    <td style="padding: 4px 0;"><span style="background: #1976d2; color: white; padding: 1px 4px; border-radius: 2px;">BIOS</span></td>
                    <td>MULTI_BANK_MULTI_DQ</td>
                    <td>100% BIOS timing fix</td>
                </tr>
                <tr style="border-bottom: 1px solid {border_color};">
                    <td style="padding: 4px 0;"><span style="background: #1976d2; color: white; padding: 1px 4px; border-radius: 2px;">BIOS</span></td>
                    <td>MULTI_BANK_MULTI_DQ + BANK/BURST/PERIPH</td>
                    <td>Combined BIOS fix (100% + 50%)</td>
                </tr>
                <tr style="border-bottom: 1px solid {border_color};">
                    <td style="padding: 4px 0;"><span style="background: #ff9800; color: white; padding: 1px 4px; border-radius: 2px;">HW+SOP</span></td>
                    <td>Hang MSN_STATUS only</td>
                    <td>Hardware + SOP fix</td>
                </tr>
                <tr>
                    <td style="padding: 4px 0;"><span style="background: #d32f2f; color: white; padding: 1px 4px; border-radius: 2px;">No Recovery</span></td>
                    <td>SB/DB FAILCRAWLERs, Row MSN_STATUS</td>
                    <td>DRAM-related (no fix available)</td>
                </tr>
            </table>
            <div style="font-size: 8px; color: {muted_color}; margin-top: 6px; padding-top: 6px; border-top: 1px dashed {border_color};">
                <b>Note:</b> ROW MSN_STATUS never gets BIOS recovery (even with BURST patterns like SINGLE_BURST_SINGLE_ROW).
                Row-classified failures are potential DRAM issues — only RPx verification can recover them.
            </div>
        </div>

        <!-- Priority Order -->
        <div style="background: {highlight_bg}; border-radius: 6px; padding: 10px; border-left: 3px solid #607d8b;">
            <div style="font-size: 10px; font-weight: 600; color: #607d8b; margin-bottom: 6px;">
                ⚡ Recovery Priority Order
            </div>
            <div style="font-size: 9px; color: {text_color}; font-family: monospace; background: {bg_color}; padding: 6px; border-radius: 4px;">
                HW+SOP → No Recovery (SB/DB) → BIOS → RPx → No Recovery (Row)
            </div>
            <div style="font-size: 8px; color: {muted_color}; margin-top: 4px;">
                SB/DB FAILCRAWLERs are actual bit errors — RPx can't fix silicon issues
            </div>
        </div>

        <div style="font-size: 8px; color: {muted_color}; margin-top: 8px; padding-top: 6px; border-top: 1px solid {border_color};">
            RPx runs <code>/home/nmewes/Y6CP_FA/socamm_false_miscompare.py</code> with <code>-standard_flow=yes -islatest=Y -isvalid=Y</code>
        </div>
    </div>
    '''

    return html


def create_failcrawler_breakdown_html(
    msn_corr_df: pd.DataFrame,
    step: str,
    total_uin: int,
    dark_mode: bool = False,
    verified_rpx_data: dict = None
) -> str:
    """
    Create detailed MSN_STATUS → FAILCRAWLER breakdown with recovery mapping.

    Args:
        msn_corr_df: FAILCRAWLER × MSN_STATUS correlation data
        step: Test step (HMB1, QMON)
        total_uin: Total UIN for DPM calculation
        dark_mode: Whether to use dark mode styling
        verified_rpx_data: Optional verified RPx recovery data from false miscompare script

    Returns:
        HTML string with detailed breakdown table
    """
    if msn_corr_df.empty or total_uin == 0:
        return ""

    bg_color = '#1e1e1e' if dark_mode else '#fff'
    border_color = '#444' if dark_mode else '#e0e0e0'
    text_color = '#e0e0e0' if dark_mode else '#333'
    muted_color = '#888' if dark_mode else '#666'
    header_bg = '#2d2d2d' if dark_mode else '#f5f5f5'

    # Filter by step
    step_col = 'QUERY_STEP' if 'QUERY_STEP' in msn_corr_df.columns else 'STEP'
    if step_col in msn_corr_df.columns:
        step_df = msn_corr_df[msn_corr_df[step_col].str.upper() == step.upper()].copy()
    else:
        step_df = msn_corr_df.copy()

    if step_df.empty:
        return ""

    # Exclude Pass status
    if 'MSN_STATUS' in step_df.columns:
        step_df = step_df[step_df['MSN_STATUS'] != 'Pass']

    if step_df.empty:
        return ""

    # Check for WIDE format (FAILCRAWLER names as columns)
    # In wide format, the values ARE already DPM, not UFAIL counts
    # Detect FAILCRAWLER columns by excluding known metadata columns
    metadata_cols = {
        'MSN_STATUS', 'MFG_WORKWEEK', 'STEP', 'QUERY_STEP', 'UIN', 'MUIN',
        'UFAIL', 'MUFAIL', 'TOTAL', 'DPM', 'MDPM', 'CDPM', 'YIELD', 'PASS',
        'DESIGN_ID', 'FORM_FACTOR', 'DENSITY', 'SPEED', 'FACILITY', 'DBASE',
        'COUNT', 'PERCENT', 'LEVEL', 'RAW_COUNT', 'INDEX', 'FAILCRAWLER'
    }
    # Find numeric columns that aren't metadata - these are likely FAILCRAWLERs
    fc_columns = []
    for col in step_df.columns:
        if col.upper() not in metadata_cols:
            try:
                col_numeric = pd.to_numeric(step_df[col], errors='coerce')
                if col_numeric.notna().any() and col_numeric.sum() > 0:
                    fc_columns.append(col)
            except (TypeError, ValueError):
                pass

    is_wide_format = fc_columns and 'FAILCRAWLER' not in step_df.columns

    # Convert wide format to long format if needed
    if is_wide_format:
        # Wide format: FAILCRAWLER columns contain DPM values (not UFAIL)
        id_cols = [col for col in step_df.columns if col not in fc_columns]
        long_df = step_df.melt(
            id_vars=id_cols,
            value_vars=fc_columns,
            var_name='FAILCRAWLER',
            value_name='DPM'  # These ARE DPM values, not UFAIL
        )
        # Filter out zero/null values
        long_df = long_df[long_df['DPM'] > 0]
        step_df = long_df
        # Calculate yield loss from DPM (already have DPM)
        step_df['YIELD_LOSS'] = step_df['DPM'] / 10_000  # DPM to yield loss %
        # Back-calculate UFAIL using row-level UIN (DPM * row_UIN / 1M)
        # Use row-level UIN if available, otherwise use total_uin
        if 'UIN' in step_df.columns:
            step_df['UFAIL'] = (step_df['DPM'] * step_df['UIN'] / 1_000_000).round(0).astype(int)
        else:
            step_df['UFAIL'] = (step_df['DPM'] * total_uin / 1_000_000).round(0).astype(int)
    else:
        # Long format: Calculate DPM from UFAIL
        if 'UFAIL' not in step_df.columns:
            return ""
        step_df['DPM'] = (step_df['UFAIL'] / total_uin) * 1_000_000
        step_df['YIELD_LOSS'] = step_df['DPM'] / 10_000  # DPM to yield loss %

    # Aggregate by MSN_STATUS × FAILCRAWLER (combine multiple workweeks)
    if 'MSN_STATUS' in step_df.columns and 'FAILCRAWLER' in step_df.columns:
        # Sum UFAIL across workweeks
        agg_df = step_df.groupby(['MSN_STATUS', 'FAILCRAWLER'], as_index=False).agg({
            'UFAIL': 'sum'
        })
        # Kevin's approach: DPM = UFAIL / total_uin × 1M (consistent denominator)
        agg_df['DPM'] = (agg_df['UFAIL'] / total_uin) * 1_000_000
        step_df = agg_df

    # Ensure YIELD_LOSS column exists (needed for recovery calculations)
    if 'YIELD_LOSS' not in step_df.columns and 'DPM' in step_df.columns:
        step_df['YIELD_LOSS'] = step_df['DPM'] / 10_000  # DPM to yield loss %

    # Get verified RPx recovery rate if available
    rpx_recovery_rate = 0.0
    if verified_rpx_data:
        rpx_recovery_rate = verified_rpx_data.get('rpx_recovery_rate', 0.0)

    # Determine recovery type for each FAILCRAWLER
    def get_recovery_info(row):
        fc = row['FAILCRAWLER']
        msn = row.get('MSN_STATUS', '')
        fc_upper = str(fc).upper()

        # HW+SOP: Hang MSN_STATUS (100% projected)
        if msn in HW_SOP_MSN_STATUS:
            return ('HW+SOP', 1.0, '#ff9800')
        # DRAM FAILCRAWLER: SB/DB = actual bit errors, no recovery possible
        if fc_upper in {f.upper() for f in DRAM_FAILCRAWLERS}:
            return ('No Recovery', 0.0, '#d32f2f')  # Red = no recovery, actual failure
        # BIOS 100%: MULTI_BANK_MULTI_DQ (projected)
        if fc in BIOS_FIX_FAILCRAWLERS_100PCT:
            return ('BIOS', 1.0, '#2196f3')
        # BIOS 50%: BANK/BURST/PERIPH patterns (projected)
        if any(pattern in fc_upper for pattern in BIOS_FIX_PATTERNS_50PCT):
            return ('BIOS*', 0.5, '#03a9f4')
        # RPx: All other failures (not Hang/Boot) - verified via script
        if msn not in RPX_EXCLUDE_MSN_STATUS and rpx_recovery_rate > 0:
            return ('RPx✓', rpx_recovery_rate, '#9c27b0')  # Checkmark = verified
        # DRAM ROW: ROW MSN_STATUS without RPx = potential DRAM failure
        if msn in DRAM_MSN_STATUS:
            return ('No Recovery', 0.0, '#d32f2f')  # Red = no recovery
        return (None, 0.0, '#888')

    step_df['recovery_info'] = step_df.apply(get_recovery_info, axis=1)
    step_df['recovery_type'] = step_df['recovery_info'].apply(lambda x: x[0])
    step_df['recovery_rate'] = step_df['recovery_info'].apply(lambda x: x[1])
    step_df['recovery_color'] = step_df['recovery_info'].apply(lambda x: x[2])
    step_df['recovered_dpm'] = step_df['DPM'] * step_df['recovery_rate']
    step_df['recovered_yield'] = step_df['YIELD_LOSS'] * step_df['recovery_rate']

    # Sort by DPM descending
    step_df = step_df.sort_values('DPM', ascending=False)

    # Build HTML table
    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 12px; font-family: 'Segoe UI', sans-serif;">
        <div style="font-size: 11px; font-weight: 600; color: {text_color}; margin-bottom: 8px;">
            📋 {step} FAILCRAWLER Breakdown (DPM → Yield Impact)
        </div>

        <table style="width: 100%; border-collapse: collapse; font-size: 10px;">
            <thead>
                <tr style="background: {header_bg};">
                    <th style="padding: 6px; text-align: left; border-bottom: 2px solid {border_color}; color: {text_color};">MSN_STATUS</th>
                    <th style="padding: 6px; text-align: left; border-bottom: 2px solid {border_color}; color: {text_color};">FAILCRAWLER</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: {text_color};">UFAIL</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: {text_color};">DPM</th>
                    <th style="padding: 6px; text-align: center; border-bottom: 2px solid {border_color}; color: {text_color};">Recovery</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: #4caf50;">Recovered DPM</th>
                </tr>
            </thead>
            <tbody>
    '''

    for idx, (_, row) in enumerate(step_df.iterrows()):
        row_bg = header_bg if idx % 2 == 0 else bg_color
        recovery_type = row['recovery_type']
        recovery_color = row['recovery_color']

        recovery_badge = f'<span style="background: {recovery_color}; color: #fff; padding: 2px 6px; border-radius: 3px; font-size: 9px;">{recovery_type}</span>' if recovery_type else '-'
        recovered_dpm = row['DPM'] * row['recovery_rate'] if row['recovery_rate'] > 0 else 0
        recovered_val = f"{recovered_dpm:,.2f}" if recovered_dpm > 0 else '-'

        html += f'''
            <tr style="background: {row_bg};">
                <td style="padding: 5px 6px; color: {text_color}; border-bottom: 1px solid {border_color};">{row['MSN_STATUS']}</td>
                <td style="padding: 5px 6px; color: {text_color}; border-bottom: 1px solid {border_color}; font-family: monospace; font-size: 9px;">{row['FAILCRAWLER']}</td>
                <td style="padding: 5px 6px; text-align: right; color: {text_color}; border-bottom: 1px solid {border_color};">{int(row['UFAIL']):,}</td>
                <td style="padding: 5px 6px; text-align: right; color: {text_color}; border-bottom: 1px solid {border_color};">{row['DPM']:,.2f}</td>
                <td style="padding: 5px 6px; text-align: center; border-bottom: 1px solid {border_color};">{recovery_badge}</td>
                <td style="padding: 5px 6px; text-align: right; color: #4caf50; border-bottom: 1px solid {border_color};">{recovered_val}</td>
            </tr>
        '''

    # Summary row - calculate properly using total_uin as denominator
    # Don't sum DPM values (they have different denominators) - recalculate from UFAIL
    total_ufail = step_df['UFAIL'].sum()
    total_dpm = (total_ufail / total_uin) * 1_000_000 if total_uin > 0 else 0

    # Calculate recovered DPM properly
    total_recovered_ufail = (step_df['UFAIL'] * step_df['recovery_rate']).sum()
    total_recovered_dpm = (total_recovered_ufail / total_uin) * 1_000_000 if total_uin > 0 else 0

    html += f'''
            <tr style="background: {header_bg}; font-weight: bold;">
                <td colspan="2" style="padding: 6px; color: {text_color}; border-top: 2px solid {border_color};">TOTAL</td>
                <td style="padding: 6px; text-align: right; color: {text_color}; border-top: 2px solid {border_color};">{int(total_ufail):,}</td>
                <td style="padding: 6px; text-align: right; color: {text_color}; border-top: 2px solid {border_color};">{total_dpm:,.2f}</td>
                <td style="padding: 6px; text-align: center; border-top: 2px solid {border_color};">-</td>
                <td style="padding: 6px; text-align: right; color: #4caf50; border-top: 2px solid {border_color};">{total_recovered_dpm:,.2f}</td>
            </tr>
        </tbody>
        </table>
    </div>
    '''

    return html


def calculate_slt_combined_recovery(
    hmb1_recovery: dict,
    qmon_recovery: dict
) -> dict:
    """
    Calculate combined SLT (HMB1 × QMON) recovery projection.

    SLT Yield = HMB1 Yield × QMON Yield

    Uses module-based yield calculation:
        Step Yield = (Total MUIN - Failed MSNs) / Total MUIN × 100

    Args:
        hmb1_recovery: Recovery data from HMB1
        qmon_recovery: Recovery data from QMON

    Returns:
        Combined SLT recovery projection
    """
    if not hmb1_recovery or not qmon_recovery:
        return None

    # Calculate module-based yield (CORRECT method)
    # Yield = (Total MUIN - Failed MSNs) / Total MUIN × 100
    def calc_module_yield(total_muin, failed_msns):
        if total_muin <= 0:
            return 100.0
        return ((total_muin - failed_msns) / total_muin) * 100

    # Get MUIN and failed MSN counts
    hmb1_muin = hmb1_recovery.get('total_muin', 0)
    qmon_muin = qmon_recovery.get('total_muin', 0)
    hmb1_failed = hmb1_recovery.get('total_failed_msns', 0)
    qmon_failed = qmon_recovery.get('total_failed_msns', 0)
    hmb1_recovered = hmb1_recovery.get('total_recovered_msns', 0)
    qmon_recovered = qmon_recovery.get('total_recovered_msns', 0)

    # Current yields (before recovery) - module-based
    hmb1_current_yield = calc_module_yield(hmb1_muin, hmb1_failed)
    qmon_current_yield = calc_module_yield(qmon_muin, qmon_failed)
    slt_current_yield = (hmb1_current_yield / 100) * (qmon_current_yield / 100) * 100

    # Projected yields (after recovery) - module-based
    # Failed after recovery = Failed - Recovered
    hmb1_failed_after = max(0, hmb1_failed - hmb1_recovered)
    qmon_failed_after = max(0, qmon_failed - qmon_recovered)
    hmb1_projected_yield = calc_module_yield(hmb1_muin, hmb1_failed_after)
    qmon_projected_yield = calc_module_yield(qmon_muin, qmon_failed_after)
    slt_projected_yield = (hmb1_projected_yield / 100) * (qmon_projected_yield / 100) * 100

    # Recovery amounts (yield gain in percentage points)
    hmb1_yield_gain = hmb1_projected_yield - hmb1_current_yield
    qmon_yield_gain = qmon_projected_yield - qmon_current_yield
    slt_yield_gain = slt_projected_yield - slt_current_yield

    return {
        'hmb1': {
            'total_muin': hmb1_muin,
            'total_failed_msns': hmb1_failed,
            'total_recovered_msns': hmb1_recovered,
            'current_dpm': hmb1_recovery['total_dpm'],
            'remaining_dpm': hmb1_recovery['remaining_dpm'],
            'current_yield': round(hmb1_current_yield, 4),
            'projected_yield': round(hmb1_projected_yield, 4),
            'yield_gain': round(hmb1_yield_gain, 4),
            'rpx_dpm': hmb1_recovery['rpx_dpm'],
            'bios_dpm': hmb1_recovery['bios_dpm'],
            'hw_sop_dpm': hmb1_recovery['hw_sop_dpm'],
        },
        'qmon': {
            'total_muin': qmon_muin,
            'total_failed_msns': qmon_failed,
            'total_recovered_msns': qmon_recovered,
            'current_dpm': qmon_recovery['total_dpm'],
            'remaining_dpm': qmon_recovery['remaining_dpm'],
            'current_yield': round(qmon_current_yield, 4),
            'projected_yield': round(qmon_projected_yield, 4),
            'yield_gain': round(qmon_yield_gain, 4),
            'rpx_dpm': qmon_recovery['rpx_dpm'],
            'bios_dpm': qmon_recovery['bios_dpm'],
            'hw_sop_dpm': qmon_recovery['hw_sop_dpm'],
        },
        'slt': {
            'current_yield': round(slt_current_yield, 4),
            'projected_yield': round(slt_projected_yield, 4),
            'yield_gain': round(slt_yield_gain, 4),
        }
    }


def create_slt_combined_html(slt_data: dict, dark_mode: bool = False) -> str:
    """
    Create HTML visualization for combined SLT (HMB1 × QMON) recovery.

    Args:
        slt_data: Output from calculate_slt_combined_recovery()
        dark_mode: Whether to use dark mode styling

    Returns:
        HTML string with SLT combined visualization
    """
    if not slt_data:
        return ""

    bg_color = '#1e1e1e' if dark_mode else '#fff'
    border_color = '#444' if dark_mode else '#e0e0e0'
    text_color = '#e0e0e0' if dark_mode else '#333'
    muted_color = '#888' if dark_mode else '#666'
    card_bg = '#2d2d2d' if dark_mode else '#f5f5f5'

    hmb1 = slt_data['hmb1']
    qmon = slt_data['qmon']
    slt = slt_data['slt']

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 16px; font-family: 'Segoe UI', sans-serif;">
        <div style="font-size: 13px; font-weight: 600; color: {text_color}; margin-bottom: 12px; text-align: center;">
            🔗 Combined SLT Recovery (HMB1 × QMON)
        </div>

        <!-- SLT Formula Visual -->
        <div style="display: flex; align-items: center; justify-content: center; gap: 12px; margin-bottom: 16px;">
            <!-- HMB1 Card -->
            <div style="background: {card_bg}; border-radius: 8px; padding: 12px; text-align: center; min-width: 120px;">
                <div style="font-size: 10px; color: {muted_color};">HMB1</div>
                <div style="font-size: 18px; font-weight: bold; color: {text_color};">{hmb1['current_yield']:.2f}%</div>
                <div style="font-size: 10px; color: #4caf50;">→ {hmb1['projected_yield']:.2f}%</div>
                <div style="font-size: 9px; color: #4caf50;">(+{hmb1['yield_gain']:.4f}%)</div>
            </div>

            <div style="font-size: 20px; color: {muted_color};">×</div>

            <!-- QMON Card -->
            <div style="background: {card_bg}; border-radius: 8px; padding: 12px; text-align: center; min-width: 120px;">
                <div style="font-size: 10px; color: {muted_color};">QMON</div>
                <div style="font-size: 18px; font-weight: bold; color: {text_color};">{qmon['current_yield']:.2f}%</div>
                <div style="font-size: 10px; color: #4caf50;">→ {qmon['projected_yield']:.2f}%</div>
                <div style="font-size: 9px; color: #4caf50;">(+{qmon['yield_gain']:.4f}%)</div>
            </div>

            <div style="font-size: 20px; color: {muted_color};">=</div>

            <!-- SLT Result -->
            <div style="background: linear-gradient(135deg, #1976d2, #1565c0); border-radius: 8px; padding: 12px; text-align: center; min-width: 140px;">
                <div style="font-size: 10px; color: rgba(255,255,255,0.8);">SLT Yield</div>
                <div style="font-size: 22px; font-weight: bold; color: #fff;">{slt['current_yield']:.2f}%</div>
                <div style="font-size: 11px; color: #a5d6a7;">→ {slt['projected_yield']:.2f}%</div>
                <div style="font-size: 10px; color: #a5d6a7; font-weight: bold;">(+{slt['yield_gain']:.4f}%)</div>
            </div>
        </div>

        <!-- Recovery Breakdown Table -->
        <table style="width: 100%; border-collapse: collapse; font-size: 10px; margin-top: 8px;">
            <thead>
                <tr style="background: {card_bg};">
                    <th style="padding: 6px; text-align: left; border-bottom: 2px solid {border_color}; color: {text_color};">Step</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: {text_color};">Current DPM</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: #9c27b0;">RPx</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: #2196f3;">BIOS</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: #ff9800;">HW+SOP</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: #4caf50;">Remaining</th>
                    <th style="padding: 6px; text-align: right; border-bottom: 2px solid {border_color}; color: {text_color};">Yield Gain</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="padding: 5px 6px; color: {text_color}; border-bottom: 1px solid {border_color}; font-weight: bold;">HMB1</td>
                    <td style="padding: 5px 6px; text-align: right; color: {text_color}; border-bottom: 1px solid {border_color};">{hmb1['current_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #9c27b0; border-bottom: 1px solid {border_color};">{hmb1['rpx_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #2196f3; border-bottom: 1px solid {border_color};">{hmb1['bios_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #ff9800; border-bottom: 1px solid {border_color};">{hmb1['hw_sop_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #4caf50; border-bottom: 1px solid {border_color};">{hmb1['remaining_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #4caf50; border-bottom: 1px solid {border_color};">+{hmb1['yield_gain']:.4f}%</td>
                </tr>
                <tr style="background: {card_bg};">
                    <td style="padding: 5px 6px; color: {text_color}; border-bottom: 1px solid {border_color}; font-weight: bold;">QMON</td>
                    <td style="padding: 5px 6px; text-align: right; color: {text_color}; border-bottom: 1px solid {border_color};">{qmon['current_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #9c27b0; border-bottom: 1px solid {border_color};">{qmon['rpx_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #2196f3; border-bottom: 1px solid {border_color};">{qmon['bios_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #ff9800; border-bottom: 1px solid {border_color};">{qmon['hw_sop_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #4caf50; border-bottom: 1px solid {border_color};">{qmon['remaining_dpm']:.2f}</td>
                    <td style="padding: 5px 6px; text-align: right; color: #4caf50; border-bottom: 1px solid {border_color};">+{qmon['yield_gain']:.4f}%</td>
                </tr>
            </tbody>
        </table>

        <div style="margin-top: 10px; font-size: 9px; color: {muted_color}; text-align: center;">
            SLT Yield = HMB1 Yield × QMON Yield | Combined SLT only shown when both steps are selected
        </div>
    </div>
    '''

    return html
