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
    # Use DPM values for relative distribution within each FAILCRAWLER category
    # Note: This shows "for each FAILCRAWLER, what % came from each MSN_STATUS"
    correlation_matrix = {}
    for fc in fc_columns:
        if fc in step_df.columns:
            fc_by_msn = step_df.groupby('MSN_STATUS')[fc].sum()
            fc_total = fc_by_msn.sum()
            if fc_total > 0:
                correlation_matrix[fc] = {}
                for msn_status in fc_by_msn.index:
                    correlation_matrix[fc][msn_status] = round(fc_by_msn[msn_status] / fc_total * 100, 1)

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
        hovertemplate='FAILCRAWLER: %{y}<br>MSN_STATUS: %{x}<br>Contribution: %{z:.1f}%<extra></extra>',
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
            text=f'<b>{step} FAILCRAWLER × MSN_STATUS Contribution</b>',
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
    fcfm_df: pd.DataFrame = None
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

    # Build HTML
    html = f'''
    <div style="background-color: {bg_color}; border-radius: 8px; padding: 16px; margin-bottom: 16px;">
        <h3 style="color: {text_color}; margin-bottom: 12px; font-size: 16px;">
            📊 {step} DPM Metrics Summary ({ww_display})
        </h3>

        <div style="display: flex; gap: 16px; flex-wrap: wrap;">
            <!-- cDPM Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid #3498DB;">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">cDPM</div>
                <div style="font-size: 24px; font-weight: bold; color: #3498DB; margin: 4px 0;">
                    {cdpm_val if cdpm_val is not None else 'N/A'}
                </div>
                <div style="font-size: 10px; color: {subtext_color};">Component/Package Level</div>
                <div style="font-size: 10px; color: {subtext_color}; margin-top: 4px;">UIN: {uin:,}</div>
            </div>

            <!-- MDPM Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid #E74C3C;">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">MDPM</div>
                <div style="font-size: 24px; font-weight: bold; color: #E74C3C; margin: 4px 0;">
                    {mdpm_val if mdpm_val is not None else 'N/A'}
                </div>
                <div style="font-size: 10px; color: {subtext_color};">Module Level</div>
                <div style="font-size: 10px; color: {subtext_color}; margin-top: 4px;">MUIN: {muin:,}</div>
            </div>

            <!-- FCDPM Total Card -->
            <div style="flex: 1; min-width: 150px; background-color: {card_bg}; border-radius: 8px; padding: 12px; border-left: 4px solid #F39C12;">
                <div style="font-size: 11px; color: {subtext_color}; text-transform: uppercase; letter-spacing: 0.5px;">FCDPM Total</div>
                <div style="font-size: 24px; font-weight: bold; color: #F39C12; margin: 4px 0;">
                    {fcdpm_total if fcdpm_total is not None else 'N/A'}
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
    '''

    # Add top FAILCRAWLER breakdown if available
    if fcdpm_breakdown:
        html += f'''
        <div style="margin-top: 16px;">
            <div style="font-size: 12px; font-weight: bold; color: {text_color}; margin-bottom: 8px;">Top FAILCRAWLERs:</div>
            <div style="display: flex; flex-wrap: wrap; gap: 8px;">
        '''
        for fc in fcdpm_breakdown[:5]:
            fc_color = FAILCRAWLER_COLORS.get(fc['category'], '#888888')
            html += f'''
                <div style="background-color: {fc_color}20; border-radius: 4px; padding: 4px 8px; display: flex; align-items: center; gap: 4px;">
                    <span style="color: {fc_color}; font-weight: bold;">■</span>
                    <span style="font-size: 11px; color: {text_color};">{fc['category']}: {fc['dpm']}</span>
                </div>
            '''
        html += '''
            </div>
        </div>
        '''

    html += '''
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
