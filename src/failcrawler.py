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
        '-msn_status!=Mod-Sys,ModOnly,NoFA,Multi-Mod',
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
    fig.update_layout(
        title=dict(text=title, font=dict(color=font_color, size=16)),
        barmode='stack',
        height=550,
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        font=dict(color=font_color, family='-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'),
        legend=dict(
            orientation='v',
            yanchor='top',
            y=0.98,
            xanchor='left',
            x=1.02,
            font=dict(color=font_color, size=11),
            bgcolor=legend_bg,
            bordercolor='rgba(0,0,0,0.2)' if not dark_mode else 'rgba(128,128,128,0.3)',
            borderwidth=1
        ),
        margin=dict(l=60, r=180, t=60, b=80),
        xaxis=dict(
            type='category',
            tickangle=-45,
            tickfont=dict(color=font_color, size=10),
            gridcolor=grid_color,
            title=dict(text='Work Week', font=dict(color=font_color, size=12))
        ),
        yaxis=dict(
            title=dict(text='cDPM (Defects Per Million)', font=dict(color=font_color, size=12)),
            autorange=True,
            rangemode='tozero',
            side='left',
            tickfont=dict(color=font_color, size=10),
            gridcolor=grid_color
        ),
        yaxis2=dict(
            title=dict(text='Volume (UIN)', font=dict(color=font_color, size=12)),
            overlaying='y',
            side='right',
            showgrid=False,
            tickfont=dict(color=font_color, size=10)
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


def process_msn_status_correlation(df: pd.DataFrame, step: str, design_id: str = None) -> dict:
    """
    Process FAILCRAWLER data to compute MSN_STATUS correlation.

    Calculates CDPM contribution by MSN_STATUS for each FAILCRAWLER category.
    Following mtsums best practices:
    - Rank by CDPM contribution %, not raw count
    - Flag low-volume populations
    - Exclude Mod-Sys, ModOnly, NoFA, Multi-Mod

    Args:
        df: Raw DataFrame from mtsums (wide format with FAILCRAWLER columns, grouped by MSN_STATUS)
        step: Test step (HMFN, HMB1, QMON)
        design_id: Optional Design ID to filter

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
    excluded_statuses = ['MOD-SYS', 'MODONLY', 'NOFA', 'MULTI-MOD', 'PASS']
    step_df = step_df[~step_df['MSN_STATUS'].str.upper().isin(excluded_statuses)]

    if step_df.empty:
        return None

    # Aggregate CDPM by MSN_STATUS
    # Sum across workweeks and design IDs to get total CDPM per MSN_STATUS per FAILCRAWLER
    agg_dict = {col: 'sum' for col in fc_columns}
    agg_dict['UIN'] = 'sum'
    msn_status_agg = step_df.groupby('MSN_STATUS').agg(agg_dict).reset_index()

    # Calculate total CDPM for each MSN_STATUS (sum of all FAILCRAWLER columns)
    msn_status_agg['TOTAL_CDPM'] = msn_status_agg[fc_columns].sum(axis=1)

    # Calculate grand total CDPM
    grand_total_cdpm = msn_status_agg['TOTAL_CDPM'].sum()

    if grand_total_cdpm == 0:
        return None

    # Calculate contribution percentage for each MSN_STATUS
    msn_status_agg['CONTRIBUTION_PCT'] = (msn_status_agg['TOTAL_CDPM'] / grand_total_cdpm * 100).round(2)

    # Sort by CDPM contribution (descending)
    msn_status_agg = msn_status_agg.sort_values('TOTAL_CDPM', ascending=False)

    # Calculate cumulative percentage
    msn_status_agg['CUMULATIVE_PCT'] = msn_status_agg['CONTRIBUTION_PCT'].cumsum().round(2)

    # Flag low-volume populations (UIN < 100)
    msn_status_agg['LOW_VOLUME'] = msn_status_agg['UIN'] < 100

    # Build correlation matrix (FAILCRAWLER × MSN_STATUS)
    # Rows = FAILCRAWLER, Columns = MSN_STATUS, Values = CDPM contribution %
    correlation_matrix = {}
    for fc in fc_columns:
        fc_total = msn_status_agg[fc].sum()
        if fc_total > 0:
            correlation_matrix[fc] = {}
            for _, row in msn_status_agg.iterrows():
                msn_status = row['MSN_STATUS']
                fc_cdpm = row[fc]
                correlation_matrix[fc][msn_status] = round(fc_cdpm / fc_total * 100, 1) if fc_total > 0 else 0

    # Get top FAILCRAWLERs (by total CDPM)
    fc_totals = {fc: msn_status_agg[fc].sum() for fc in fc_columns}
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
        'grand_total_cdpm': grand_total_cdpm
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

    # Theme colors
    if dark_mode:
        font_color = '#E0E0E0'
        paper_bg = 'rgba(0,0,0,0)'
        colorscale = 'Viridis'
    else:
        font_color = '#1a1a1a'
        paper_bg = '#FFFFFF'
        colorscale = 'Blues'

    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=msn_statuses,
        y=top_fcs,
        colorscale=colorscale,
        text=[[f'{v:.1f}%' for v in row] for row in z_values],
        texttemplate='%{text}',
        textfont=dict(size=10, color=font_color),
        hovertemplate='FAILCRAWLER: %{y}<br>MSN_STATUS: %{x}<br>Contribution: %{z:.1f}%<extra></extra>',
        colorbar=dict(
            title=dict(text='Contribution %', font=dict(color=font_color)),
            tickfont=dict(color=font_color)
        )
    ))

    fig.update_layout(
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
    Create HTML table showing MSN_STATUS ranked by CDPM contribution.

    Following mtsums best practices:
    - Rank by CDPM contribution %, not count
    - Flag low-volume populations
    - Show cumulative % for Pareto analysis

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
    grand_total = data['grand_total_cdpm']

    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#1a1a1a'
    header_bg = '#3d3d3d' if dark_mode else '#e8e8e8'
    border_color = '#555555' if dark_mode else '#cccccc'
    highlight_80 = '#4a1a1a' if dark_mode else '#ffe0e0'
    low_vol_color = '#5c4a00' if dark_mode else '#FFF3CD'

    html = f'''
    <div style="margin-bottom: 20px;">
        <h4 style="color: {text_color}; margin-bottom: 10px;">{step} MSN_STATUS Ranked by CDPM Contribution</h4>
        <p style="font-size: 11px; color: {'#aaaaaa' if dark_mode else '#666666'}; margin-bottom: 10px;">
            Total CDPM: {grand_total:,.1f} | Ranked by contribution %, not count
        </p>
        <table style="border-collapse: collapse; width: 100%; font-size: 12px; font-family: Arial, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: left; color: {text_color};">Rank</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: left; color: {text_color};">MSN_STATUS</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Total CDPM</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Contribution %</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Cumulative %</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">Volume (UIN)</th>
                    <th style="border: 1px solid {border_color}; padding: 8px; text-align: center; color: {text_color};">Flag</th>
                </tr>
            </thead>
            <tbody>
    '''

    for i, (_, row) in enumerate(summary.iterrows(), 1):
        # Highlight top 80% contributors
        row_style = ''
        if row['CUMULATIVE_PCT'] <= 80 or i == 1:
            row_style = f'background-color: {highlight_80};'
        elif row['LOW_VOLUME']:
            row_style = f'background-color: {low_vol_color};'

        flag = '⚠️ Low Vol' if row['LOW_VOLUME'] else ''

        html += f'''
            <tr style="{row_style}">
                <td style="border: 1px solid {border_color}; padding: 8px; color: {text_color};">{i}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; font-weight: bold; color: {text_color};">{row['MSN_STATUS']}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{row['TOTAL_CDPM']:,.1f}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{row['CONTRIBUTION_PCT']:.1f}%</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{row['CUMULATIVE_PCT']:.1f}%</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: right; color: {text_color};">{int(row['UIN']):,}</td>
                <td style="border: 1px solid {border_color}; padding: 8px; text-align: center; color: {text_color};">{flag}</td>
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        <p style="font-size: 11px; color: {'#aaaaaa' if dark_mode else '#666666'}; margin-top: 5px;">
            🔴 Red rows = Top 80% contributors (focus areas) | 🟡 Yellow = Low volume (&lt;100 UIN, interpret with caution)
        </p>
    </div>
    '''

    return html
