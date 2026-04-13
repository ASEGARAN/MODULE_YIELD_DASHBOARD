"""
GRACE Motherboard Health Monitoring

Data source: mtsums (MTSUMS database)
Monitors NVGRACE motherboard performance for HMB1/QMON test steps.
"""

import subprocess
import pandas as pd
from io import StringIO
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# MSN_STATUS categorization for health monitoring
FAIL_CATEGORIES = {
    'system': ['Mod-Sys'],
    'hang': ['Hang'],
    'boot': ['Boot'],  # May not exist in all data
    'other': ['Row', 'DQ', 'Multi-DQ', 'SB_Int', 'Col', 'Block', 'Repair']  # Catch-all for other fail types
}


def generate_workweek_range(start_ww: str, end_ww: str) -> list[str]:
    """
    Generate list of work weeks between start and end (inclusive).

    Args:
        start_ww: Start work week (YYYYWW format, e.g., '202610')
        end_ww: End work week (YYYYWW format, e.g., '202614')

    Returns:
        List of work weeks as strings
    """
    weeks = []
    start_year = int(start_ww[:4])
    start_week = int(start_ww[4:])
    end_year = int(end_ww[:4])
    end_week = int(end_ww[4:])

    current_year = start_year
    current_week = start_week

    while (current_year < end_year) or (current_year == end_year and current_week <= end_week):
        weeks.append(f"{current_year}{current_week:02d}")
        current_week += 1
        # Handle year rollover (assume max 53 weeks per year)
        if current_week > 53:
            current_week = 1
            current_year += 1

    return weeks


def fetch_grace_health_data(
    start_ww: str,
    end_ww: str,
    form_factors: list[str] = ['socamm', 'socamm2'],
    steps: list[str] = ['hmb1', 'qmon'],
    design_ids: Optional[list[str]] = None,
    densities: Optional[list[str]] = None,
    speeds: Optional[list[str]] = None,
    facility: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Fetch GRACE motherboard health data from mtsums.

    Args:
        start_ww: Start work week (e.g., '202610')
        end_ww: End work week (e.g., '202614')
        form_factors: Module form factors to include
        steps: Test steps to include
        design_ids: Optional list of design IDs (e.g., ['Y6CP', 'Y63N'])
        densities: Optional list of module densities (e.g., ['192GB'])
        speeds: Optional list of module speeds (e.g., ['7500MTPS'])
        facility: Optional test facility (e.g., 'PENANG')

    Returns:
        DataFrame with columns: MFG_WORKWEEK, MACHINE_ID, STEP, MSN_STATUS, MUFAIL, MUIN
    """
    # Generate full list of work weeks
    weeks = generate_workweek_range(start_ww, end_ww)
    ww_list = ','.join(weeks)

    modff = ','.join(form_factors)
    step_list = ','.join(steps)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        f'-ww={ww_list}',
        '+msnag',
        f'-modff={modff}',
        f'-step={step_list}',
        '-format=mfg_workweek,machine_id,step,msn_status',
        '+quiet',
        '+csv'
    ]

    # Add optional filters
    if design_ids:
        cmd.append(f'-dbase={",".join(design_ids)}')
    if densities:
        cmd.append(f'-module_density={",".join(densities)}')
    if speeds:
        cmd.append(f'-module_speed={",".join(speeds)}')
    if facility:
        cmd.append(f'-test_facility={facility}')

    logger.info(f"Running mtsums command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode != 0:
            logger.error(f"mtsums failed: {result.stderr}")
            return None

        # Parse CSV output
        output = result.stdout
        if not output.strip():
            logger.warning("mtsums returned empty output")
            return None

        df = pd.read_csv(StringIO(output))

        # Filter for NVGRACE machines only
        df = df[df['MACHINE_ID'].str.contains('NVGRACE', case=False, na=False)]

        if df.empty:
            logger.warning("No NVGRACE data found")
            return None

        logger.info(f"Fetched {len(df)} NVGRACE records")
        return df

    except subprocess.TimeoutExpired:
        logger.error("mtsums command timed out")
        return None
    except Exception as e:
        logger.error(f"Error fetching GRACE data: {e}")
        return None


def categorize_msn_status(status: str) -> str:
    """Categorize MSN_STATUS into high-level fail categories."""
    if status == 'Pass':
        return 'pass'
    for category, statuses in FAIL_CATEGORIES.items():
        if status in statuses:
            return category
    return 'other'  # Catch-all for unknown fail types


def aggregate_weekly_health(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate GRACE data to weekly health metrics.

    Returns DataFrame with columns:
        - week: Work week (YYYYWW)
        - tested: Total modules tested (MUIN)
        - total_fails: Total fails
        - system_fails: Mod-Sys fails
        - hang_fails: Hang fails
        - boot_fails: Boot fails
        - other_fails: Other fail types
        - dpm: DPM (fails per million)
        - yield_pct: Yield percentage
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Add fail category column
    df = df.copy()
    df['fail_category'] = df['MSN_STATUS'].apply(categorize_msn_status)

    # Aggregate by week
    weekly_data = []

    for week in sorted(df['MFG_WORKWEEK'].unique()):
        week_df = df[df['MFG_WORKWEEK'] == week]

        tested = week_df['MUIN'].sum()
        total_fails = week_df[week_df['MSN_STATUS'] != 'Pass']['MUFAIL'].sum()
        system_fails = week_df[week_df['fail_category'] == 'system']['MUFAIL'].sum()
        hang_fails = week_df[week_df['fail_category'] == 'hang']['MUFAIL'].sum()
        boot_fails = week_df[week_df['fail_category'] == 'boot']['MUFAIL'].sum()
        other_fails = week_df[week_df['fail_category'] == 'other']['MUFAIL'].sum()

        dpm = (total_fails / tested * 1_000_000) if tested > 0 else 0
        yield_pct = ((tested - total_fails) / tested * 100) if tested > 0 else 0

        weekly_data.append({
            'week': int(week),
            'tested': int(tested),
            'total_fails': int(total_fails),
            'system_fails': int(system_fails),
            'hang_fails': int(hang_fails),
            'boot_fails': int(boot_fails),
            'other_fails': int(other_fails),
            'dpm': round(dpm, 1),
            'yield_pct': round(yield_pct, 2)
        })

    return pd.DataFrame(weekly_data)


def calculate_rolling_metrics(weekly_df: pd.DataFrame, window: int = 4) -> pd.DataFrame:
    """
    Calculate rolling metrics (e.g., 4-week rolling DPM).

    Args:
        weekly_df: Weekly aggregated DataFrame
        window: Rolling window size (default 4 weeks)

    Returns:
        DataFrame with additional rolling columns
    """
    if weekly_df.empty:
        return weekly_df

    df = weekly_df.copy()
    df = df.sort_values('week')

    # Calculate rolling sums for DPM calculation
    df['rolling_tested'] = df['tested'].rolling(window=window, min_periods=1).sum()
    df['rolling_fails'] = df['total_fails'].rolling(window=window, min_periods=1).sum()
    df['rolling_dpm'] = (df['rolling_fails'] / df['rolling_tested'] * 1_000_000).round(1)

    # Week-over-week delta
    df['wow_delta_dpm'] = df['dpm'].diff()
    df['wow_delta_pct'] = ((df['dpm'] - df['dpm'].shift(1)) / df['dpm'].shift(1) * 100).round(1)

    return df


def aggregate_by_machine(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate GRACE data by machine for chronic exposure analysis.

    Returns DataFrame with per-machine metrics.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Aggregate by machine
    machine_data = []

    for machine_id in df['MACHINE_ID'].unique():
        machine_df = df[df['MACHINE_ID'] == machine_id]

        tested = machine_df['MUIN'].sum()
        fails = machine_df[machine_df['MSN_STATUS'] != 'Pass']['MUFAIL'].sum()
        weeks_active = machine_df['MFG_WORKWEEK'].nunique()

        dpm = (fails / tested * 1_000_000) if tested > 0 else 0
        yield_pct = ((tested - fails) / tested * 100) if tested > 0 else 0

        # Get fail type breakdown
        fail_types = machine_df[machine_df['MSN_STATUS'] != 'Pass'].groupby('MSN_STATUS')['MUFAIL'].sum().to_dict()

        machine_data.append({
            'machine_id': machine_id,
            'tested': int(tested),
            'fails': int(fails),
            'dpm': round(dpm, 1),
            'yield_pct': round(yield_pct, 2),
            'weeks_active': weeks_active,
            'fail_breakdown': fail_types
        })

    result = pd.DataFrame(machine_data)
    return result.sort_values('dpm', ascending=False)


def get_health_status(dpm: float, thresholds: dict = None) -> tuple[str, str]:
    """
    Determine RAG health status based on DPM.

    Args:
        dpm: Current DPM value
        thresholds: Dict with 'green' and 'yellow' thresholds

    Returns:
        Tuple of (status, color) e.g., ('Good', '#00C853')
    """
    if thresholds is None:
        thresholds = {
            'green': 2000,   # DPM < 2000 = Green
            'yellow': 5000   # DPM < 5000 = Yellow, >= 5000 = Red
        }

    if dpm < thresholds['green']:
        return 'Good', '#00C853'
    elif dpm < thresholds['yellow']:
        return 'Warning', '#FFB300'
    else:
        return 'Critical', '#FF1744'
