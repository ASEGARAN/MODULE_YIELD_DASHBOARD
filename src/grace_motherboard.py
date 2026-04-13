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


# =============================================================================
# NEW: Fail Mode (FM) based analysis with week-over-week comparison
# =============================================================================

def fetch_grace_fm_data(
    start_ww: str,
    end_ww: str,
    form_factors: list[str] = ['socamm', 'socamm2'],
    design_ids: Optional[list[str]] = None,
    densities: Optional[list[str]] = None,
    speeds: Optional[list[str]] = None,
    facility: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Fetch GRACE motherboard data with fail mode (FM) cDPM breakdown.

    Uses: mtsums -modff=socamm2,socamm -ww=YYYYWW,YYYYWW +fm -format+=machine_id,mfg_workweek =islatest =isvalid +stdf

    Args:
        start_ww: Start work week (e.g., '202606')
        end_ww: End work week (e.g., '202615')
        form_factors: Module form factors
        design_ids: Optional list of design IDs
        densities: Optional list of module densities
        speeds: Optional list of module speeds
        facility: Optional test facility

    Returns:
        DataFrame with MACHINE_ID, MFG_WORKWEEK, and cDPM columns for each fail mode
    """
    # Generate work week range
    weeks = generate_workweek_range(start_ww, end_ww)
    ww_list = ','.join(weeks)
    modff = ','.join(form_factors)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        f'-modff={modff}',
        f'-ww={ww_list}',
        '+fm',
        '-format+=machine_id,mfg_workweek',
        '=islatest',
        '=isvalid',
        '+stdf',
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

    logger.info(f"Running mtsums +fm command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode != 0:
            logger.error(f"mtsums +fm failed: {result.stderr}")
            return None

        output = result.stdout
        if not output.strip():
            logger.warning("mtsums +fm returned empty output")
            return None

        df = pd.read_csv(StringIO(output))

        # Filter for NVGRACE machines only
        if 'MACHINE_ID' in df.columns:
            df = df[df['MACHINE_ID'].str.contains('NVGRACE', case=False, na=False)]

        if df.empty:
            logger.warning("No NVGRACE data found in +fm output")
            return None

        logger.info(f"Fetched {len(df)} NVGRACE FM records")
        return df

    except subprocess.TimeoutExpired:
        logger.error("mtsums +fm command timed out")
        return None
    except Exception as e:
        logger.error(f"Error fetching GRACE FM data: {e}")
        return None


def get_hang_machines(fm_df: pd.DataFrame, selected_ww: str) -> pd.DataFrame:
    """
    Find machines with Hang cDPM > 0 for the selected work week.

    Args:
        fm_df: DataFrame from fetch_grace_fm_data()
        selected_ww: Work week to filter (e.g., '202614')

    Returns:
        DataFrame with machines that have Hang failures
    """
    if fm_df is None or fm_df.empty:
        return pd.DataFrame()

    # Filter to selected work week
    ww_int = int(selected_ww)
    ww_df = fm_df[fm_df['MFG_WORKWEEK'] == ww_int].copy()

    if ww_df.empty:
        return pd.DataFrame()

    # Filter for Hang > 0
    if 'Hang' in ww_df.columns:
        hang_machines = ww_df[ww_df['Hang'] > 0][['MACHINE_ID', 'MFG_WORKWEEK', 'Hang', 'UIN']].copy()
        hang_machines = hang_machines.rename(columns={'Hang': 'Hang_cDPM'})
        return hang_machines.sort_values('Hang_cDPM', ascending=False)

    return pd.DataFrame()


def compare_weeks(fm_df: pd.DataFrame, current_ww: str, previous_ww: str) -> pd.DataFrame:
    """
    Compare fail mode data between two work weeks.

    Args:
        fm_df: DataFrame from fetch_grace_fm_data()
        current_ww: Current work week (e.g., '202614')
        previous_ww: Previous work week (e.g., '202613')

    Returns:
        DataFrame with comparison metrics
    """
    if fm_df is None or fm_df.empty:
        return pd.DataFrame()

    current_int = int(current_ww)
    previous_int = int(previous_ww)

    current_df = fm_df[fm_df['MFG_WORKWEEK'] == current_int].copy()
    previous_df = fm_df[fm_df['MFG_WORKWEEK'] == previous_int].copy()

    if current_df.empty and previous_df.empty:
        return pd.DataFrame()

    # Get all machines from both weeks
    all_machines = set(current_df['MACHINE_ID'].unique()) | set(previous_df['MACHINE_ID'].unique())

    comparison_data = []
    for machine in all_machines:
        curr = current_df[current_df['MACHINE_ID'] == machine]
        prev = previous_df[previous_df['MACHINE_ID'] == machine]

        curr_hang = curr['Hang'].values[0] if not curr.empty and 'Hang' in curr.columns else 0
        prev_hang = prev['Hang'].values[0] if not prev.empty and 'Hang' in prev.columns else 0
        curr_uin = curr['UIN'].values[0] if not curr.empty and 'UIN' in curr.columns else 0
        prev_uin = prev['UIN'].values[0] if not prev.empty and 'UIN' in prev.columns else 0

        # Only include if either week has Hang > 0
        if curr_hang > 0 or prev_hang > 0:
            comparison_data.append({
                'machine_id': machine,
                f'hang_cDPM_{current_ww}': curr_hang,
                f'hang_cDPM_{previous_ww}': prev_hang,
                f'uin_{current_ww}': curr_uin,
                f'uin_{previous_ww}': prev_uin,
                'hang_delta': curr_hang - prev_hang,
                'is_new': prev_hang == 0 and curr_hang > 0,
                'is_resolved': prev_hang > 0 and curr_hang == 0,
                'is_chronic': prev_hang > 0 and curr_hang > 0
            })

    return pd.DataFrame(comparison_data).sort_values('hang_delta', ascending=False)


def fetch_machine_tsums(
    machine_id: str,
    steps: list[str] = ['hmb1', 'qmon'],
    days: int = 30
) -> Optional[pd.DataFrame]:
    """
    Fetch detailed tsums data for a specific machine.

    Uses: tsums -machine_id=NVGRACE-XXXXXX -step=hmb1,qmon -standard_flow=yes -30 -ta -format+=mfg_workweek,bios_version +echo

    Args:
        machine_id: Machine ID (e.g., 'NVGRACE-099562')
        steps: Test steps to query
        days: Number of days to look back

    Returns:
        DataFrame with lot-level details including UIN, UPASS, MFG_WORKWEEK, BIOS_VERSION
    """
    step_list = ','.join(steps)

    cmd = f"tsums -machine_id={machine_id} -step={step_list} -standard_flow=yes -{days} -ta -format+=mfg_workweek,bios_version +echo"

    logger.info(f"Running tsums command: {cmd}")

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            logger.error(f"tsums failed: {result.stderr}")
            return None

        output = result.stdout
        if not output.strip() or 'Command Echo' not in output:
            logger.warning("tsums returned no data")
            return None

        # Parse the output (space-separated, skip the Command Echo line)
        lines = output.strip().split('\n')
        data_lines = [l for l in lines if not l.startswith('Command Echo') and l.strip()]

        if not data_lines:
            return None

        # Parse each line into structured data
        records = []
        for line in data_lines:
            parts = line.split()
            if len(parts) >= 20:  # Ensure we have enough columns
                try:
                    records.append({
                        'lot': parts[0],
                        'uin': int(parts[3]),
                        'upass': int(parts[4]),
                        'step': parts[-3],
                        'mfg_workweek': parts[-2],
                        'bios_version': parts[-1]
                    })
                except (ValueError, IndexError) as e:
                    logger.debug(f"Skipping line due to parse error: {e}")
                    continue

        if not records:
            return None

        return pd.DataFrame(records)

    except subprocess.TimeoutExpired:
        logger.error("tsums command timed out")
        return None
    except Exception as e:
        logger.error(f"Error fetching machine tsums: {e}")
        return None


def analyze_hang_failures(
    machine_id: str,
    current_ww: str,
    previous_ww: str,
    days: int = 30
) -> dict:
    """
    Analyze hang failures for a specific machine, comparing two work weeks.

    Identifies cases where UIN=4 and UPASS=0 (100% fail) in both weeks.

    Args:
        machine_id: Machine ID to analyze
        current_ww: Current work week
        previous_ww: Previous work week
        days: Days to look back for tsums data

    Returns:
        Dict with analysis results including:
        - current_ww_failures: List of lots with UIN=4, UPASS=0 in current week
        - previous_ww_failures: List of lots with UIN=4, UPASS=0 in previous week
        - is_chronic: True if failures in both weeks
        - bios_versions: Unique BIOS versions seen
    """
    tsums_df = fetch_machine_tsums(machine_id, days=days)

    if tsums_df is None or tsums_df.empty:
        return {
            'machine_id': machine_id,
            'current_ww_failures': [],
            'previous_ww_failures': [],
            'is_chronic': False,
            'bios_versions': [],
            'error': 'No tsums data available'
        }

    # Filter for 100% fail cases (UIN=4, UPASS=0)
    fail_100pct = tsums_df[(tsums_df['uin'] == 4) & (tsums_df['upass'] == 0)]

    current_failures = fail_100pct[fail_100pct['mfg_workweek'] == current_ww].to_dict('records')
    previous_failures = fail_100pct[fail_100pct['mfg_workweek'] == previous_ww].to_dict('records')

    # Get unique BIOS versions
    bios_versions = tsums_df['bios_version'].unique().tolist()

    return {
        'machine_id': machine_id,
        'current_ww': current_ww,
        'previous_ww': previous_ww,
        'current_ww_failures': current_failures,
        'previous_ww_failures': previous_failures,
        'current_ww_count': len(current_failures),
        'previous_ww_count': len(previous_failures),
        'is_chronic': len(current_failures) > 0 and len(previous_failures) > 0,
        'bios_versions': bios_versions,
        'total_100pct_fails': len(fail_100pct)
    }


def get_previous_workweek(ww: str) -> str:
    """
    Get the previous work week given a work week string.

    Args:
        ww: Work week in YYYYWW format (e.g., '202614')

    Returns:
        Previous work week (e.g., '202613')
    """
    year = int(ww[:4])
    week = int(ww[4:])

    if week > 1:
        return f"{year}{week-1:02d}"
    else:
        # Handle year boundary - assume previous year has 52 weeks
        return f"{year-1}52"
