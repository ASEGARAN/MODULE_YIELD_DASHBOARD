"""
SOCAMM2 LPDRAMM De-pop & Re-ball Tracker

Tracks the "request → execution → outcome" loop for de-pop/reball operations.
Supports HOLD/GO decisions based on success rates.

SUCCESS DEFINITION (FINAL):
- An attempt is SUCCESSFUL if and only if:
  - reball_attempted = TRUE
  - component_functional_after_reball = TRUE
- Module retest / SLT pass is NOT required for success.
- Target threshold = 80%
- Below threshold: HOLD new depop & re-ball requests
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

SUCCESS_THRESHOLD = 0.80  # 80% target
HOLD_LOOKBACK_WEEKS = 2   # Trailing 2-week window for HOLD/GO decision

# Status mappings from Excel to standardized values
STATUS_MAP = {
    'Pass': 'PASS',
    'PASS': 'PASS',
    'pass': 'PASS',
    'Fail': 'FAIL',
    'FAIL': 'FAIL',
    'fail': 'FAIL',
    'Damage': 'DAMAGE',
    'DAMAGE': 'DAMAGE',
    'damage': 'DAMAGE',
    'Pending': 'PENDING',
    'PENDING': 'PENDING',
    'pending': 'PENDING',
    'Waiting Test': 'PENDING',
    'In Progress': 'PENDING',
}

# Failure reason categories
FAILURE_REASONS = [
    'pad_lift',
    'damage',
    'cannot_detect',
    'short',
    'open',
    'solder_bridge',
    'component_crack',
    'pcb_damage',
    'unknown',
]

# =============================================================================
# Data Loading
# =============================================================================

def load_doe_excel(filepath: str) -> dict:
    """
    Load DOE Excel file and return structured data.

    Args:
        filepath: Path to DOE_SOCAMM_SUMMARY.xlsx

    Returns:
        Dictionary with 'requests', 'attempts', 'raw_pivot' DataFrames
    """
    try:
        xlsx = pd.ExcelFile(filepath)

        # Load the pivot sheet which has normalized data
        pivot_df = pd.read_excel(xlsx, sheet_name='DOE Pivot & Charts', header=2)

        # Clean column names
        pivot_df.columns = ['Week', 'MSN', 'Product', 'Unit', 'Status', 'Remark'] + \
                          list(pivot_df.columns[6:])

        # Filter to actual data rows (has Week and MSN)
        pivot_df = pivot_df[pivot_df['Week'].notna() & pivot_df['MSN'].notna()].copy()
        pivot_df = pivot_df[['Week', 'MSN', 'Product', 'Unit', 'Status', 'Remark']].copy()

        # Standardize status
        pivot_df['Status'] = pivot_df['Status'].map(lambda x: STATUS_MAP.get(str(x), 'UNKNOWN'))

        # Extract workweek number
        pivot_df['Workweek'] = pivot_df['Week'].apply(extract_workweek)

        # Create attempts DataFrame
        attempts_df = create_attempts_from_pivot(pivot_df)

        # Create requests DataFrame (aggregate by MSN)
        requests_df = create_requests_from_attempts(attempts_df)

        return {
            'requests': requests_df,
            'attempts': attempts_df,
            'raw_pivot': pivot_df,
        }

    except Exception as e:
        logger.exception(f"Error loading DOE Excel: {e}")
        return {
            'requests': pd.DataFrame(),
            'attempts': pd.DataFrame(),
            'raw_pivot': pd.DataFrame(),
        }


def extract_workweek(week_str: str) -> int:
    """Extract workweek number from string like 'WW13' or 'ww13'."""
    if pd.isna(week_str):
        return 0
    week_str = str(week_str).upper().strip()
    if week_str.startswith('WW'):
        try:
            return int(week_str[2:])
        except ValueError:
            return 0
    return 0


def create_attempts_from_pivot(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pivot data to attempts table structure.

    Each row in pivot is one unit test result = one attempt outcome.
    """
    attempts = []

    for idx, row in pivot_df.iterrows():
        msn = str(row['MSN']).strip()
        product = str(row['Product']).strip() if pd.notna(row['Product']) else 'Unknown'
        unit = str(row['Unit']).strip() if pd.notna(row['Unit']) else 'Unknown'
        status = row['Status']
        workweek = row['Workweek']
        remark = str(row['Remark']) if pd.notna(row['Remark']) else ''

        # Determine success flags based on status
        # Status = PASS means component is functional after reball
        component_functional = 1 if status == 'PASS' else 0
        depop_success = 1 if status in ['PASS', 'FAIL'] else 0  # Damage = depop failed
        reball_attempted = 1 if status != 'PENDING' else 0
        reball_success = 1 if status == 'PASS' else 0

        # Determine failure reason
        failure_reason = None
        if status == 'FAIL':
            failure_reason = 'unknown'  # Default, can be enhanced with remark parsing
        elif status == 'DAMAGE':
            failure_reason = 'damage'

        attempts.append({
            'attempt_id': f"{msn}_{unit}_{workweek}",
            'jira_key': None,  # To be linked if available
            'msn': msn,
            'component_uloc': unit,
            'product': product,
            'workweek': workweek,
            'attempt_date': None,
            'pic_name': None,
            'method_used': None,
            'depop_success': depop_success,
            'reball_attempted': reball_attempted,
            'reball_success': reball_success,
            'component_functional_after_reball': component_functional,
            'failure_reason': failure_reason,
            'status_raw': status,
            'remark': remark,
        })

    return pd.DataFrame(attempts)


def create_requests_from_attempts(attempts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate attempts to request level (one row per MSN).
    """
    if attempts_df.empty:
        return pd.DataFrame()

    requests = []

    for msn, group in attempts_df.groupby('msn'):
        # Determine overall request status
        statuses = group['status_raw'].unique()
        if 'PENDING' in statuses:
            overall_status = 'In Progress'
        elif all(s == 'PASS' for s in statuses):
            overall_status = 'Success'
        elif 'DAMAGE' in statuses:
            overall_status = 'Damaged'
        else:
            overall_status = 'Partial'

        # Count outcomes
        total_units = len(group)
        passed = (group['component_functional_after_reball'] == 1).sum()
        failed = (group['status_raw'] == 'FAIL').sum()
        damaged = (group['status_raw'] == 'DAMAGE').sum()
        pending = (group['status_raw'] == 'PENDING').sum()

        requests.append({
            'msn': msn,
            'product': group['product'].iloc[0],
            'workweek': group['workweek'].min(),
            'total_units': total_units,
            'units_passed': passed,
            'units_failed': failed,
            'units_damaged': damaged,
            'units_pending': pending,
            'success_rate': passed / (total_units - pending) if (total_units - pending) > 0 else None,
            'status': overall_status,
            'jira_key': group['jira_key'].iloc[0],
        })

    return pd.DataFrame(requests)


# =============================================================================
# KPI Calculations
# =============================================================================

def calculate_weekly_metrics(attempts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate weekly KPIs from attempts data.

    Returns DataFrame with one row per workweek.
    """
    if attempts_df.empty:
        return pd.DataFrame()

    weekly_metrics = []

    for ww, group in attempts_df.groupby('workweek'):
        if ww == 0:
            continue

        # Filter to reball attempted only for success rate
        reball_attempted = group[group['reball_attempted'] == 1]

        # Core counts
        total_attempts = len(group)
        reball_count = len(reball_attempted)

        # Success counts
        depop_success = (group['depop_success'] == 1).sum()
        reball_success = (reball_attempted['reball_success'] == 1).sum() if not reball_attempted.empty else 0
        functional_success = (reball_attempted['component_functional_after_reball'] == 1).sum() if not reball_attempted.empty else 0

        # Failure breakdown
        damage_count = (group['status_raw'] == 'DAMAGE').sum()
        fail_count = (group['status_raw'] == 'FAIL').sum()
        pending_count = (group['status_raw'] == 'PENDING').sum()

        # Success rates
        depop_rate = depop_success / total_attempts if total_attempts > 0 else 0
        reball_rate = reball_success / reball_count if reball_count > 0 else 0

        # PRIMARY KPI: End-to-End Success Rate
        # = component_functional_after_reball / reball_attempted
        e2e_success_rate = functional_success / reball_count if reball_count > 0 else 0

        weekly_metrics.append({
            'workweek': ww,
            'workweek_label': f'WW{ww}',
            'total_attempts': total_attempts,
            'reball_attempted': reball_count,
            'depop_success_count': depop_success,
            'reball_success_count': reball_success,
            'functional_success_count': functional_success,
            'damage_count': damage_count,
            'fail_count': fail_count,
            'pending_count': pending_count,
            'depop_success_rate': depop_rate,
            'reball_success_rate': reball_rate,
            'e2e_success_rate': e2e_success_rate,
            'meets_target': e2e_success_rate >= SUCCESS_THRESHOLD,
        })

    df = pd.DataFrame(weekly_metrics)
    if not df.empty:
        df = df.sort_values('workweek')
    return df


def calculate_trailing_success_rate(weekly_metrics: pd.DataFrame, num_weeks: int = 2) -> float:
    """
    Calculate trailing N-week end-to-end success rate.

    Used for HOLD/GO decision logic.
    """
    if weekly_metrics.empty:
        return 0.0

    # Get last N weeks
    recent = weekly_metrics.tail(num_weeks)

    total_functional = recent['functional_success_count'].sum()
    total_reball = recent['reball_attempted'].sum()

    if total_reball == 0:
        return 0.0

    return total_functional / total_reball


def get_hold_go_status(trailing_rate: float) -> dict:
    """
    Determine HOLD/GO status based on trailing success rate.

    Returns dict with status, message, and color.
    """
    if trailing_rate >= SUCCESS_THRESHOLD:
        return {
            'status': 'GO',
            'message': f'Trailing 2-week success rate: {trailing_rate:.1%} (above {SUCCESS_THRESHOLD:.0%} threshold)',
            'color': '#27AE60',  # Green
            'icon': '✅',
            'action': 'Continue accepting new depop & re-ball requests',
        }
    else:
        return {
            'status': 'HOLD',
            'message': f'Trailing 2-week success rate: {trailing_rate:.1%} (below {SUCCESS_THRESHOLD:.0%} threshold)',
            'color': '#E74C3C',  # Red
            'icon': '🛑',
            'action': 'HOLD new SOCAMM2 LPDRAMM depop & re-ball requests - Component functional success below threshold',
        }


def calculate_pic_metrics(attempts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate PIC (Person-in-Charge) level metrics.

    For each PIC × Week, calculate success rate and whether they meet 80% target.
    """
    if attempts_df.empty or 'pic_name' not in attempts_df.columns:
        return pd.DataFrame()

    # Filter out null PICs
    pic_data = attempts_df[attempts_df['pic_name'].notna()].copy()

    if pic_data.empty:
        return pd.DataFrame()

    pic_metrics = []

    for (pic, ww), group in pic_data.groupby(['pic_name', 'workweek']):
        reball_attempted = group[group['reball_attempted'] == 1]

        if reball_attempted.empty:
            continue

        functional_success = (reball_attempted['component_functional_after_reball'] == 1).sum()
        total = len(reball_attempted)
        success_rate = functional_success / total if total > 0 else 0

        pic_metrics.append({
            'pic_name': pic,
            'workweek': ww,
            'attempts': total,
            'successes': functional_success,
            'success_rate': success_rate,
            'meets_target': success_rate >= SUCCESS_THRESHOLD,
        })

    return pd.DataFrame(pic_metrics)


def calculate_failure_pareto(attempts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate failure reason Pareto analysis.

    Returns sorted DataFrame of failure reasons with counts and percentages.
    """
    if attempts_df.empty:
        return pd.DataFrame()

    # Filter to failures only
    failures = attempts_df[
        (attempts_df['status_raw'].isin(['FAIL', 'DAMAGE'])) &
        (attempts_df['failure_reason'].notna())
    ].copy()

    if failures.empty:
        return pd.DataFrame()

    # Count by failure reason
    pareto = failures.groupby('failure_reason').agg(
        count=('attempt_id', 'count')
    ).reset_index()

    pareto = pareto.sort_values('count', ascending=False)
    pareto['percentage'] = pareto['count'] / pareto['count'].sum() * 100
    pareto['cumulative_pct'] = pareto['percentage'].cumsum()

    return pareto


def calculate_method_comparison(attempts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare success rates by method (hot air vs mechanical).
    """
    if attempts_df.empty or 'method_used' not in attempts_df.columns:
        return pd.DataFrame()

    method_data = attempts_df[
        (attempts_df['method_used'].notna()) &
        (attempts_df['reball_attempted'] == 1)
    ].copy()

    if method_data.empty:
        return pd.DataFrame()

    comparison = method_data.groupby('method_used').agg(
        attempts=('attempt_id', 'count'),
        successes=('component_functional_after_reball', 'sum'),
    ).reset_index()

    comparison['success_rate'] = comparison['successes'] / comparison['attempts']

    return comparison


# =============================================================================
# Summary Statistics
# =============================================================================

def get_overall_summary(attempts_df: pd.DataFrame) -> dict:
    """
    Get overall summary statistics.
    """
    if attempts_df.empty:
        return {
            'total_attempts': 0,
            'total_msn': 0,
            'total_pass': 0,
            'total_fail': 0,
            'total_damage': 0,
            'total_pending': 0,
            'e2e_success_rate': 0,
            'depop_success_rate': 0,
        }

    total = len(attempts_df)
    reball_attempted = attempts_df[attempts_df['reball_attempted'] == 1]

    pass_count = (attempts_df['status_raw'] == 'PASS').sum()
    fail_count = (attempts_df['status_raw'] == 'FAIL').sum()
    damage_count = (attempts_df['status_raw'] == 'DAMAGE').sum()
    pending_count = (attempts_df['status_raw'] == 'PENDING').sum()

    functional_success = (reball_attempted['component_functional_after_reball'] == 1).sum() if not reball_attempted.empty else 0
    reball_count = len(reball_attempted)

    depop_success = (attempts_df['depop_success'] == 1).sum()

    return {
        'total_attempts': total,
        'total_msn': attempts_df['msn'].nunique(),
        'total_pass': int(pass_count),
        'total_fail': int(fail_count),
        'total_damage': int(damage_count),
        'total_pending': int(pending_count),
        'e2e_success_rate': functional_success / reball_count if reball_count > 0 else 0,
        'depop_success_rate': depop_success / total if total > 0 else 0,
        'reball_attempted': reball_count,
        'functional_success': int(functional_success),
    }


# =============================================================================
# Data Export
# =============================================================================

def export_weekly_report(weekly_metrics: pd.DataFrame, filepath: str):
    """Export weekly metrics to Excel."""
    if weekly_metrics.empty:
        return

    # Format for export
    export_df = weekly_metrics.copy()
    export_df['depop_success_rate'] = export_df['depop_success_rate'].apply(lambda x: f'{x:.1%}')
    export_df['reball_success_rate'] = export_df['reball_success_rate'].apply(lambda x: f'{x:.1%}')
    export_df['e2e_success_rate'] = export_df['e2e_success_rate'].apply(lambda x: f'{x:.1%}')

    export_df.to_excel(filepath, index=False)
    logger.info(f"Exported weekly report to {filepath}")
