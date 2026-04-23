# -*- coding: utf-8 -*-
"""
Hybrid DPM Analysis Tool - Kevin's Approach

Generates MSN_STATUS x FAILCRAWLER correlation analysis for:
- Y6CP and Y63N Design IDs
- HMB1 and QMON test steps
- Single requested workweek AND 4-week cumulative
- RPx Recovery Simulation (false miscompare detection)

Kevin's Hybrid DPM Formula:
- MODULE-level (Mod-Sys, Hang, Multi-Mod, Boot): MSNs / Total FIDs x 1M
- FID-level (DQ, Row, SB_Int, Multi-DQ, etc.): FIDs / Total FIDs x 1M
"""

import subprocess
import pandas as pd
from io import StringIO
import sys
import os
import argparse
import json

# Add parent paths for imports
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD')
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD/sandbox/cdpm_recovery_simulation')

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Import recovery simulation functions
from rpx_recovery_simulation import analyze_recovery, create_recovery_chart

# Configuration
MODULE_LEVEL_FAILURES = {'Mod-Sys', 'Hang', 'Multi-Mod', 'Boot'}
DESIGN_IDS = ['Y6CP', 'Y63N']
STEPS = ['HMB1', 'QMON']
OUTPUT_DIR = '/home/asegaran/MODULE_YIELD_DASHBOARD/sandbox/cdpm_recovery_simulation/output'


def get_workweek_range(requested_ww, num_weeks=4):
    """Get list of workweeks for cumulative analysis."""
    year = int(str(requested_ww)[:4])
    week = int(str(requested_ww)[4:])

    workweeks = []
    for i in range(num_weeks - 1, -1, -1):
        w = week - i
        y = year
        if w <= 0:
            y -= 1
            w += 52
        workweeks.append('{}{:02d}'.format(y, w))

    return workweeks


def fetch_fid_data(design_ids, steps, workweeks):
    """Fetch FID-level data with FAILCRAWLER breakdown."""
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join(workweeks)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id_str),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag',
        '-mfg_workweek={}'.format(workweek_str),
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK,MSN_STATUS,FAILCRAWLER',
        '-step={}'.format(step_str),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300)
        if result.returncode != 0:
            return pd.DataFrame()
        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()
        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception as e:
        print("Error fetching FID data: {}".format(e))
        return pd.DataFrame()


def fetch_module_counts(design_ids, steps, workweeks):
    """Fetch module-level counts (MUFAIL) per MSN_STATUS."""
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join(workweeks)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id_str),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+msnag',
        '-mfg_workweek={}'.format(workweek_str),
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK,MSN_STATUS',
        '-step={}'.format(step_str),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300)
        if result.returncode != 0:
            return pd.DataFrame()
        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()
        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception as e:
        print("Error fetching module counts: {}".format(e))
        return pd.DataFrame()


def fetch_total_uin(design_ids, steps, workweeks):
    """Fetch total UIN (FID count) for denominator."""
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join(workweeks)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id_str),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag',
        '-mfg_workweek={}'.format(workweek_str),
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK',
        '-step={}'.format(step_str),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300)
        if result.returncode != 0:
            return pd.DataFrame()
        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()
        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception as e:
        print("Error fetching total UIN: {}".format(e))
        return pd.DataFrame()


def calculate_hybrid_dpm(fid_df, msn_df, uin_df, design_id, step, workweeks, cumulative=False):
    """
    Calculate hybrid DPM for given parameters.

    Args:
        fid_df: FID-level data with FAILCRAWLER breakdown
        msn_df: Module-level data with MUFAIL
        uin_df: Total UIN data
        design_id: Design ID to filter
        step: Test step to filter
        workweeks: List of workweeks to include
        cumulative: If True, aggregate across all workweeks

    Returns:
        DataFrame with MSN_STATUS, FAILCRAWLER, DPM, Level columns
    """
    # Filter data
    fid_filtered = fid_df[
        (fid_df['DESIGN_ID'] == design_id) &
        (fid_df['STEP'].str.upper() == step.upper()) &
        (fid_df['MFG_WORKWEEK'].astype(str).isin(workweeks))
    ].copy()

    msn_filtered = msn_df[
        (msn_df['DESIGN_ID'] == design_id) &
        (msn_df['STEP'].str.upper() == step.upper()) &
        (msn_df['MFG_WORKWEEK'].astype(str).isin(workweeks))
    ].copy()

    uin_filtered = uin_df[
        (uin_df['DESIGN_ID'] == design_id) &
        (uin_df['STEP'].str.upper() == step.upper()) &
        (uin_df['MFG_WORKWEEK'].astype(str).isin(workweeks))
    ].copy()

    if fid_filtered.empty or uin_filtered.empty:
        return pd.DataFrame()

    # Calculate total UIN
    total_uin = uin_filtered['UIN'].sum()
    if total_uin == 0:
        return pd.DataFrame()

    # Exclude Pass status
    fid_filtered = fid_filtered[fid_filtered['MSN_STATUS'] != 'Pass']
    msn_filtered = msn_filtered[msn_filtered['MSN_STATUS'] != 'Pass']

    # Build module counts lookup
    module_counts = {}
    for msn_status in msn_filtered['MSN_STATUS'].unique():
        mufail = msn_filtered[msn_filtered['MSN_STATUS'] == msn_status]['MUFAIL'].sum()
        module_counts[msn_status] = mufail

    # Calculate DPM for each MSN_STATUS × FAILCRAWLER combination
    results = []

    for msn_status in fid_filtered['MSN_STATUS'].unique():
        is_module_level = msn_status in MODULE_LEVEL_FAILURES
        msn_fid_data = fid_filtered[fid_filtered['MSN_STATUS'] == msn_status]

        # Get total UFAIL for this MSN_STATUS (for weight calculation)
        total_ufail_for_msn = msn_fid_data['UFAIL'].sum()

        # Get module count for MODULE-level failures
        if is_module_level:
            total_msn_count = module_counts.get(msn_status, 0)
            total_dpm_for_msn = (total_msn_count / total_uin) * 1_000_000

        # Calculate DPM per FAILCRAWLER
        for fc in msn_fid_data['FAILCRAWLER'].unique():
            fc_data = msn_fid_data[msn_fid_data['FAILCRAWLER'] == fc]
            ufail = fc_data['UFAIL'].sum()

            if is_module_level:
                # Distribute module DPM proportionally
                if total_ufail_for_msn > 0:
                    weight = ufail / total_ufail_for_msn
                    dpm = total_dpm_for_msn * weight
                else:
                    dpm = 0
            else:
                # FID-level: standard cDPM
                dpm = (ufail / total_uin) * 1_000_000

            results.append({
                'MSN_STATUS': msn_status,
                'FAILCRAWLER': fc,
                'DPM': round(dpm, 2),
                'LEVEL': 'MODULE' if is_module_level else 'FID',
                'COUNT': int(total_msn_count) if is_module_level else int(ufail),
                'UFAIL_WEIGHT': round(ufail, 2)
            })

    result_df = pd.DataFrame(results)
    result_df['TOTAL_UIN'] = total_uin

    return result_df


def create_summary_table(dpm_df):
    """Create summary table by MSN_STATUS."""
    if dpm_df.empty:
        return pd.DataFrame()

    summary = dpm_df.groupby(['MSN_STATUS', 'LEVEL']).agg({
        'DPM': 'sum',
        'COUNT': 'first'
    }).reset_index()

    summary = summary.sort_values('DPM', ascending=False)
    summary['PCT'] = (summary['DPM'] / summary['DPM'].sum() * 100).round(1)

    return summary


def create_correlation_heatmap(dpm_df, title, total_uin):
    """Create MSN_STATUS × FAILCRAWLER correlation heatmap."""
    if dpm_df.empty:
        return None, None

    # Pivot for heatmap
    pivot_df = dpm_df.pivot_table(
        index='MSN_STATUS',
        columns='FAILCRAWLER',
        values='DPM',
        aggfunc='sum',
        fill_value=0
    )

    # Sort by row totals
    row_totals = pivot_df.sum(axis=1).sort_values(ascending=False)
    pivot_df = pivot_df.loc[row_totals.index]

    # Sort columns by totals, limit to top 12
    col_totals = pivot_df.sum(axis=0).sort_values(ascending=False)
    top_cols = col_totals.head(12).index
    pivot_df = pivot_df[top_cols]

    # Store original index for data table
    original_index = pivot_df.index.tolist()

    # Add level indicators to row labels
    level_map = {row: '[M]' if row in MODULE_LEVEL_FAILURES else '[F]'
                 for row in pivot_df.index}
    new_index = ["{} {}".format(row, level_map[row]) for row in pivot_df.index]
    pivot_df.index = new_index

    # Use full column names for display
    display_cols = list(pivot_df.columns)

    # Create heatmap - convert to list to avoid binary encoding
    # Red color scale (white to dark red)
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values.tolist(),
        x=list(display_cols),
        y=list(pivot_df.index),
        colorscale=[
            [0, '#fff5f0'],
            [0.2, '#fee0d2'],
            [0.4, '#fcbba1'],
            [0.6, '#fc9272'],
            [0.8, '#de2d26'],
            [1.0, '#a50f15']
        ],
        text=[['{:.1f}'.format(v) if v > 0 else '' for v in row] for row in pivot_df.values],
        texttemplate='%{text}',
        textfont={"size": 10, "color": "black"},
        hovertemplate='<b>%{y}</b><br>FAILCRAWLER: %{x}<br>DPM: %{z:.2f}<extra></extra>',
        colorbar=dict(
            title=dict(text='DPM', side='right'),
            thickness=12,
            len=0.8
        )
    ))

    fig.update_layout(
        title=dict(
            text='MSN_STATUS x FAILCRAWLER',
            x=0.5,
            font=dict(size=10)
        ),
        xaxis=dict(
            title='',
            tickangle=60,
            tickfont=dict(size=7),
            side='bottom'
        ),
        yaxis=dict(
            title='',
            tickfont=dict(size=8),
            autorange='reversed'
        ),
        height=260,
        width=340,
        margin=dict(l=70, r=30, t=30, b=100),
        paper_bgcolor='white',
        plot_bgcolor='white'
    )

    # Create detailed breakdown data
    breakdown_data = []
    for msn_status in original_index:
        msn_data = dpm_df[dpm_df['MSN_STATUS'] == msn_status]
        level = '[M]' if msn_status in MODULE_LEVEL_FAILURES else '[F]'
        total_dpm = msn_data['DPM'].sum()

        # Get top FAILCRAWLERs for this MSN_STATUS
        fc_breakdown = msn_data.groupby('FAILCRAWLER')['DPM'].sum().sort_values(ascending=False)
        top_fcs = fc_breakdown.head(5)

        breakdown_data.append({
            'msn_status': msn_status,
            'level': level,
            'total_dpm': total_dpm,
            'top_failcrawlers': [(fc, dpm) for fc, dpm in top_fcs.items() if dpm > 0]
        })

    return fig, breakdown_data


def create_heatmap_with_breakdown(dpm_df, title, total_uin):
    """Create heatmap with detailed breakdown data."""
    return create_correlation_heatmap(dpm_df, title, total_uin)


def print_analysis(design_id, step, requested_ww, single_week_df, cumulative_df,
                   single_uin, cumulative_uin, workweek_range):
    """Print formatted analysis results."""
    print("\n" + "=" * 90)
    print("{} {} - Hybrid DPM Analysis (Kevin's Approach)".format(design_id, step))
    print("=" * 90)

    # Single week analysis
    print("\n### Single Week: WW{} ###".format(requested_ww))
    if single_week_df.empty:
        print("No data available")
    else:
        print("Total UIN: {:,} FIDs".format(int(single_uin)))
        summary = create_summary_table(single_week_df)
        print("\n{:<15} {:>8} {:>10} {:>12} {:>10}".format(
            "MSN_STATUS", "Level", "Count", "DPM", "% Total"))
        print("-" * 60)
        for _, row in summary.iterrows():
            unit = "MSNs" if row['LEVEL'] == 'MODULE' else "FIDs"
            print("{:<15} {:>8} {:>8} {} {:>10.2f} {:>9.1f}%".format(
                row['MSN_STATUS'], row['LEVEL'], int(row['COUNT']), unit,
                row['DPM'], row['PCT']))
        print("-" * 60)
        print("{:<15} {:>19} {:>10.2f} {:>10}".format(
            "TOTAL", "", summary['DPM'].sum(), "100.0%"))

    # Cumulative analysis
    print("\n### 4-Week Cumulative: WW{} to WW{} ###".format(
        workweek_range[0], workweek_range[-1]))
    if cumulative_df.empty:
        print("No data available")
    else:
        print("Total UIN: {:,} FIDs".format(int(cumulative_uin)))
        summary = create_summary_table(cumulative_df)
        print("\n{:<15} {:>8} {:>10} {:>12} {:>10}".format(
            "MSN_STATUS", "Level", "Count", "DPM", "% Total"))
        print("-" * 60)
        for _, row in summary.iterrows():
            unit = "MSNs" if row['LEVEL'] == 'MODULE' else "FIDs"
            print("{:<15} {:>8} {:>8} {} {:>10.2f} {:>9.1f}%".format(
                row['MSN_STATUS'], row['LEVEL'], int(row['COUNT']), unit,
                row['DPM'], row['PCT']))
        print("-" * 60)
        print("{:<15} {:>19} {:>10.2f} {:>10}".format(
            "TOTAL", "", summary['DPM'].sum(), "100.0%"))


def generate_html_report(all_results, requested_ww, workweek_range, output_file):
    """Generate HTML report with all heatmaps."""
    html_parts = ["""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Hybrid DPM Analysis - WW{}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 10px; background: #f0f0f0; font-size: 12px; }}
        h1 {{ color: #c62828; text-align: center; margin: 5px 0; font-size: 18px; }}
        h2 {{ color: white; background: #c62828; padding: 5px 10px; margin: 0; font-size: 13px; border-radius: 4px 4px 0 0; }}
        .container {{ max-width: 100%; margin: 0 auto; }}
        .section {{ background: white; margin: 8px 0; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }}
        .step-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; background: #ddd; }}
        .cell {{ background: white; padding: 8px; }}
        .cell-header {{ background: #f5f5f5; font-weight: bold; font-size: 11px; text-align: center; padding: 5px; }}
        .legend {{ background: #ffebee; padding: 8px; font-size: 11px; border-left: 3px solid #c62828; margin: 8px 0; }}
        table {{ border-collapse: collapse; width: 100%; font-size: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 2px 4px; text-align: right; }}
        th {{ background: #c62828; color: white; font-weight: 500; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        .msn-status {{ text-align: left; font-weight: 500; }}
        .module {{ color: #1565c0; }}
        .fid {{ color: #2e7d32; }}
        .uin-label {{ font-size: 10px; color: #666; margin: 2px 0; }}
        .heatmap-container {{ margin-top: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Hybrid DPM Analysis - WW{}</h1>
        <div class="legend">
            <strong>Kevin's Hybrid DPM:</strong>
            <span class="module">[M] MODULE</span>: MSNs/FIDs x 1M |
            <span class="fid">[F] FID</span>: FIDs/FIDs x 1M |
            Single: WW{} | 4-Week: WW{}-{}
        </div>
""".format(requested_ww, requested_ww, requested_ww, workweek_range[0], workweek_range[-1])]

    for design_id in DESIGN_IDS:
        html_parts.append('<div class="section">')
        html_parts.append('<h2>{}</h2>'.format(design_id))
        html_parts.append('<div class="step-grid">')

        # Headers row
        for step in STEPS:
            html_parts.append('<div class="cell-header">{} - WW{}</div>'.format(step, requested_ww))
            html_parts.append('<div class="cell-header">{} - 4 Week</div>'.format(step))

        # Data row
        for step in STEPS:
            key = (design_id, step)
            result = all_results.get(key, {})

            # Single week cell
            html_parts.append('<div class="cell">')
            if not result or result.get('single_week_df') is None or result['single_week_df'].empty:
                html_parts.append('<p>No data</p>')
            else:
                html_parts.append('<div class="uin-label">UIN: {:,}</div>'.format(int(result['single_uin'])))
                summary = create_summary_table(result['single_week_df'])
                html_parts.append(summary_to_html_table(summary))
                fig = result.get('single_heatmap')
                if fig:
                    html_parts.append('<div class="heatmap-container">')
                    heatmap_html = fig.to_html(include_plotlyjs=False, full_html=False)
                    html_parts.append(heatmap_html)
                    html_parts.append('</div>')
            html_parts.append('</div>')

            # Cumulative cell
            html_parts.append('<div class="cell">')
            if not result or result.get('cumulative_df') is None or result['cumulative_df'].empty:
                html_parts.append('<p>No data</p>')
            else:
                html_parts.append('<div class="uin-label">UIN: {:,}</div>'.format(int(result['cumulative_uin'])))
                summary = create_summary_table(result['cumulative_df'])
                html_parts.append(summary_to_html_table(summary))
                fig = result.get('cumulative_heatmap')
                if fig:
                    html_parts.append('<div class="heatmap-container">')
                    heatmap_html = fig.to_html(include_plotlyjs=False, full_html=False)
                    html_parts.append(heatmap_html)
                    html_parts.append('</div>')
            html_parts.append('</div>')

        html_parts.append('</div>')  # Close step-grid

        # Recovery Simulation Section - Three Scenarios
        html_parts.append('<div style="margin-top:15px;padding:10px;background:#f5f5f5;border-radius:4px;">')
        html_parts.append('<h3 style="margin:0 0 10px 0;color:#333;">Recovery Simulation (WW{})</h3>'.format(requested_ww))

        for step in STEPS:
            key = (design_id, step)
            result = all_results.get(key, {})
            recovery = result.get('recovery_result')

            html_parts.append('<div style="background:white;padding:10px;border-radius:4px;margin-bottom:10px;border:1px solid #ddd;">')
            html_parts.append('<strong style="font-size:14px;">{}</strong>'.format(step))

            if recovery:
                # Current DPM header
                html_parts.append('<div style="text-align:center;margin:10px 0;padding:8px;background:#ffebee;border-radius:4px;">')
                html_parts.append('<span style="font-size:12px;color:#666;">Current DPM:</span> ')
                html_parts.append('<span style="font-size:24px;font-weight:bold;color:#c62828;">{:.1f}</span>'.format(recovery['total_dpm']))
                html_parts.append('</div>')

                # Four recovery scenarios side by side
                html_parts.append('<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin:10px 0;">')

                # 1. New RPx Fix (Verified)
                html_parts.append('<div style="background:#e8f5e9;padding:6px;border-radius:4px;border-left:3px solid #4caf50;">')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">')
                html_parts.append('<span style="font-weight:bold;color:#2e7d32;font-size:10px;">New RPx Fix</span>')
                html_parts.append('<span style="font-size:7px;background:#4caf50;color:white;padding:1px 3px;border-radius:3px;">VERIFIED</span>')
                html_parts.append('</div>')
                html_parts.append('<div style="font-size:8px;color:#666;margin-bottom:4px;">False Miscompare</div>')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;">')
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rec:</span><br><span style="font-size:14px;font-weight:bold;color:#4caf50;">{:.1f}</span></div>'.format(recovery['rpx_recoverable_dpm']))
                html_parts.append('<div><span style="font-size:8px;color:#666;">After:</span><br><span style="font-size:14px;font-weight:bold;color:#1565c0;">{:.1f}</span></div>'.format(recovery['rpx_remaining_dpm']))
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rate:</span><br><span style="font-size:14px;font-weight:bold;color:#4caf50;">{:.0f}%</span></div>'.format(recovery['rpx_recovery_rate']))
                html_parts.append('</div></div>')

                # 2. New BIOS Fix (Projected)
                html_parts.append('<div style="background:#e3f2fd;padding:6px;border-radius:4px;border-left:3px solid #1976d2;">')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">')
                html_parts.append('<span style="font-weight:bold;color:#1565c0;font-size:10px;">New BIOS Fix</span>')
                html_parts.append('<span style="font-size:7px;background:#ff9800;color:white;padding:1px 3px;border-radius:3px;">PROJECTED</span>')
                html_parts.append('</div>')
                html_parts.append('<div style="font-size:8px;color:#666;margin-bottom:4px;">MULTI_BANK_MULTI_DQ - CCE</div>')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;">')
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rec:</span><br><span style="font-size:14px;font-weight:bold;color:#1976d2;">{:.1f}</span></div>'.format(recovery['bios_recoverable_dpm']))
                html_parts.append('<div><span style="font-size:8px;color:#666;">After:</span><br><span style="font-size:14px;font-weight:bold;color:#1565c0;">{:.1f}</span></div>'.format(recovery['bios_remaining_dpm']))
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rate:</span><br><span style="font-size:14px;font-weight:bold;color:#1976d2;">{:.0f}%</span></div>'.format(recovery['bios_recovery_rate']))
                html_parts.append('</div></div>')

                # 3. HW + SOP Fix (Projected) - targets Hang
                html_parts.append('<div style="background:#fce4ec;padding:6px;border-radius:4px;border-left:3px solid #c2185b;">')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">')
                html_parts.append('<span style="font-weight:bold;color:#c2185b;font-size:10px;">HW + SOP Fix</span>')
                html_parts.append('<span style="font-size:7px;background:#ff9800;color:white;padding:1px 3px;border-radius:3px;">PROJECTED</span>')
                html_parts.append('</div>')
                html_parts.append('<div style="font-size:8px;color:#666;margin-bottom:4px;">Hang - Debris/HUNG2</div>')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;">')
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rec:</span><br><span style="font-size:14px;font-weight:bold;color:#c2185b;">{:.1f}</span></div>'.format(recovery.get('hw_sop_recoverable_dpm', 0)))
                html_parts.append('<div><span style="font-size:8px;color:#666;">After:</span><br><span style="font-size:14px;font-weight:bold;color:#1565c0;">{:.1f}</span></div>'.format(recovery.get('hw_sop_remaining_dpm', recovery['total_dpm'])))
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rate:</span><br><span style="font-size:14px;font-weight:bold;color:#c2185b;">{:.0f}%</span></div>'.format(recovery.get('hw_sop_recovery_rate', 0)))
                html_parts.append('</div></div>')

                # 4. Combined (All Fixes)
                html_parts.append('<div style="background:#fff3e0;padding:6px;border-radius:4px;border-left:3px solid #f57c00;">')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">')
                html_parts.append('<span style="font-weight:bold;color:#e65100;font-size:10px;">All Fixes</span>')
                html_parts.append('<span style="font-size:7px;background:#9e9e9e;color:white;padding:1px 3px;border-radius:3px;">COMBINED</span>')
                html_parts.append('</div>')
                html_parts.append('<div style="font-size:8px;color:#666;margin-bottom:4px;">RPx + BIOS + HW/SOP</div>')
                html_parts.append('<div style="display:flex;justify-content:space-between;align-items:center;">')
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rec:</span><br><span style="font-size:14px;font-weight:bold;color:#f57c00;">{:.1f}</span></div>'.format(recovery['combined_recoverable_dpm']))
                html_parts.append('<div><span style="font-size:8px;color:#666;">After:</span><br><span style="font-size:14px;font-weight:bold;color:#1565c0;">{:.1f}</span></div>'.format(recovery['combined_remaining_dpm']))
                html_parts.append('<div><span style="font-size:8px;color:#666;">Rate:</span><br><span style="font-size:14px;font-weight:bold;color:#f57c00;">{:.0f}%</span></div>'.format(recovery['combined_recovery_rate']))
                html_parts.append('</div></div>')

                html_parts.append('</div>')  # Close grid

                # Breakdown table with all recovery types
                html_parts.append('<details style="margin-top:8px;font-size:9px;"><summary style="cursor:pointer;font-weight:bold;">MSN_STATUS Breakdown</summary>')
                html_parts.append('<table style="font-size:8px;width:100%;margin-top:5px;border-collapse:collapse;">')
                html_parts.append('<tr style="background:#424242;color:white;"><th style="text-align:left;padding:3px;">MSN_STATUS</th><th>MSNs</th><th>DPM</th>')
                html_parts.append('<th style="background:#4caf50;">RPx</th><th style="background:#1976d2;">BIOS</th><th style="background:#c2185b;">HW+SOP</th><th style="background:#f57c00;">All</th><th>After</th></tr>')
                for b in recovery['breakdown'][:8]:
                    html_parts.append('<tr style="border-bottom:1px solid #eee;"><td style="text-align:left;padding:2px;">{}</td><td>{}</td><td>{:.1f}</td>'.format(
                        b['msn_status'], b['total_msns'], b['total_dpm']))
                    html_parts.append('<td style="color:#4caf50;font-weight:bold;">{:.1f}</td>'.format(b['rpx_recoverable_dpm']))
                    html_parts.append('<td style="color:#1976d2;font-weight:bold;">{:.1f}</td>'.format(b['bios_recoverable_dpm']))
                    html_parts.append('<td style="color:#c2185b;font-weight:bold;">{:.1f}</td>'.format(b.get('hw_sop_recoverable_dpm', 0)))
                    html_parts.append('<td style="color:#f57c00;font-weight:bold;">{:.1f}</td>'.format(b['combined_recoverable_dpm']))
                    html_parts.append('<td>{:.1f}</td></tr>'.format(b['combined_remaining_dpm']))
                html_parts.append('</table></details>')

                # FAILCRAWLER details with recovery type indicators
                html_parts.append('<details style="margin-top:5px;font-size:9px;"><summary style="cursor:pointer;font-weight:bold;">FAILCRAWLER Details</summary>')
                html_parts.append('<table style="font-size:8px;width:100%;margin-top:3px;border-collapse:collapse;">')
                html_parts.append('<tr style="background:#616161;color:white;"><th style="text-align:left;padding:3px;">FAILCRAWLER</th><th>Tot</th>')
                html_parts.append('<th style="background:#4caf50;">RPx</th><th style="background:#1976d2;">BIOS</th><th style="background:#c2185b;">HW+SOP</th><th style="background:#f57c00;">Comb</th><th>Fix Type</th></tr>')
                for b in recovery['breakdown']:
                    for fc in b['failcrawlers'][:5]:
                        if fc['total'] > 0:
                            fix_types = []
                            if fc['rpx_recoverable'] > 0:
                                fix_types.append('RPx')
                            if fc.get('is_bios_target'):
                                fix_types.append('BIOS')
                            if fc.get('is_hw_sop_target') or fc.get('hw_sop_recoverable', 0) > 0:
                                fix_types.append('HW+SOP')
                            fix_type_str = '+'.join(fix_types) if fix_types else '-'
                            fix_color = '#f57c00' if len(fix_types) > 1 else ('#4caf50' if 'RPx' in fix_types else ('#1976d2' if 'BIOS' in fix_types else ('#c2185b' if 'HW+SOP' in fix_types else '#999')))
                            html_parts.append('<tr style="border-bottom:1px solid #eee;"><td style="text-align:left;padding:2px;">{}</td><td>{}</td>'.format(fc['failcrawler'], fc['total']))
                            html_parts.append('<td style="color:#4caf50;">{}</td>'.format(fc['rpx_recoverable']))
                            html_parts.append('<td style="color:#1976d2;">{}</td>'.format(fc['bios_recoverable']))
                            html_parts.append('<td style="color:#c2185b;">{}</td>'.format(fc.get('hw_sop_recoverable', 0)))
                            html_parts.append('<td style="color:#f57c00;">{}</td>'.format(fc['combined_recoverable']))
                            html_parts.append('<td style="color:{};font-weight:bold;">{}</td></tr>'.format(fix_color, fix_type_str))
                html_parts.append('</table></details>')

            else:
                html_parts.append('<p style="color:#999;font-size:10px;">No recovery data</p>')

            html_parts.append('</div>')

        html_parts.append('</div>')  # Close recovery section

        html_parts.append('</div>')  # Close section

    html_parts.append('</div></body></html>')

    with open(output_file, 'w') as f:
        f.write('\n'.join(html_parts))

    return output_file


def summary_to_html_table(summary):
    """Convert summary DataFrame to HTML table."""
    if summary.empty:
        return '<p>No data</p>'

    html = ['<table>']
    html.append('<tr><th class="msn-status">MSN_STATUS</th><th>Level</th><th>Count</th><th>DPM</th><th>%</th></tr>')

    for _, row in summary.iterrows():
        level_class = 'module' if row['LEVEL'] == 'MODULE' else 'fid'
        unit = "MSNs" if row['LEVEL'] == 'MODULE' else "FIDs"
        html.append('<tr><td class="msn-status">{}</td><td class="{}">{}</td><td>{} {}</td><td>{:.2f}</td><td>{:.1f}%</td></tr>'.format(
            row['MSN_STATUS'], level_class, row['LEVEL'], int(row['COUNT']), unit, row['DPM'], row['PCT']))

    html.append('<tr style="font-weight:bold;background:#e3f2fd;"><td>TOTAL</td><td></td><td></td><td>{:.2f}</td><td>100.0%</td></tr>'.format(
        summary['DPM'].sum()))
    html.append('</table>')

    return '\n'.join(html)


def breakdown_to_html(breakdown_data):
    """Convert breakdown data to HTML details section."""
    if not breakdown_data:
        return ''

    html = ['<div class="breakdown-section">']
    html.append('<details><summary><strong>FAILCRAWLER Breakdown by MSN_STATUS</strong></summary>')
    html.append('<div style="padding: 10px; background: #fafafa; margin-top: 5px;">')

    for item in breakdown_data:
        msn_status = item['msn_status']
        level = item['level']
        total_dpm = item['total_dpm']
        level_class = 'module' if level == '[M]' else 'fid'

        html.append('<div style="margin-bottom: 12px;">')
        html.append('<strong class="{}">{} {}</strong> - Total: {:.2f} DPM'.format(
            level_class, msn_status, level, total_dpm))

        if item['top_failcrawlers']:
            html.append('<ul style="margin: 5px 0 0 20px; list-style: none;">')
            for fc, dpm in item['top_failcrawlers']:
                pct = (dpm / total_dpm * 100) if total_dpm > 0 else 0
                html.append('<li style="font-size: 0.9em;">└─ <code>{}</code>: {:.2f} DPM ({:.1f}%)</li>'.format(
                    fc, dpm, pct))
            html.append('</ul>')
        html.append('</div>')

    html.append('</div></details></div>')
    return '\n'.join(html)


def main(requested_ww=None):
    """Main function."""
    if requested_ww is None:
        # Default to current week (for testing)
        requested_ww = '202615'

    requested_ww = str(requested_ww)
    workweek_range = get_workweek_range(requested_ww, num_weeks=4)

    print("\n" + "=" * 90)
    print("HYBRID DPM ANALYSIS - Kevin's Approach")
    print("=" * 90)
    print("Requested Workweek: WW{}".format(requested_ww))
    print("4-Week Range: WW{} to WW{}".format(workweek_range[0], workweek_range[-1]))
    print("Design IDs: {}".format(', '.join(DESIGN_IDS)))
    print("Steps: {}".format(', '.join(STEPS)))

    # Fetch all data
    print("\nFetching data...")
    fid_df = fetch_fid_data(DESIGN_IDS, STEPS, workweek_range)
    msn_df = fetch_module_counts(DESIGN_IDS, STEPS, workweek_range)
    uin_df = fetch_total_uin(DESIGN_IDS, STEPS, workweek_range)

    if fid_df.empty:
        print("No data available for the requested workweeks.")
        return

    print("  FID data: {} records".format(len(fid_df)))
    print("  Module data: {} records".format(len(msn_df)))

    all_results = {}

    # Process each DID and Step
    for design_id in DESIGN_IDS:
        for step in STEPS:
            # Single week analysis
            single_week_df = calculate_hybrid_dpm(
                fid_df, msn_df, uin_df, design_id, step, [requested_ww]
            )
            single_uin = uin_df[
                (uin_df['DESIGN_ID'] == design_id) &
                (uin_df['STEP'].str.upper() == step.upper()) &
                (uin_df['MFG_WORKWEEK'].astype(str) == requested_ww)
            ]['UIN'].sum()

            # Cumulative analysis
            cumulative_df = calculate_hybrid_dpm(
                fid_df, msn_df, uin_df, design_id, step, workweek_range
            )
            cumulative_uin = uin_df[
                (uin_df['DESIGN_ID'] == design_id) &
                (uin_df['STEP'].str.upper() == step.upper()) &
                (uin_df['MFG_WORKWEEK'].astype(str).isin(workweek_range))
            ]['UIN'].sum()

            # Print analysis
            print_analysis(design_id, step, requested_ww, single_week_df, cumulative_df,
                          single_uin, cumulative_uin, workweek_range)

            # Create heatmaps with breakdown data
            single_heatmap = None
            single_breakdown = None
            cumulative_heatmap = None
            cumulative_breakdown = None

            if not single_week_df.empty:
                single_heatmap, single_breakdown = create_heatmap_with_breakdown(
                    single_week_df,
                    '{} {} - WW{}'.format(design_id, step, requested_ww),
                    single_uin
                )

            if not cumulative_df.empty:
                cumulative_heatmap, cumulative_breakdown = create_heatmap_with_breakdown(
                    cumulative_df,
                    '{} {} - WW{} to WW{} (4-Week)'.format(
                        design_id, step, workweek_range[0], workweek_range[-1]),
                    cumulative_uin
                )

            # Run RPx recovery analysis for single week
            print("  Running RPx recovery analysis...")
            recovery_result = None
            try:
                recovery_result = analyze_recovery(design_id, step, requested_ww, verbose=False)
            except Exception as e:
                print("  Recovery analysis error: {}".format(e))

            recovery_chart = None
            if recovery_result:
                recovery_chart = create_recovery_chart(recovery_result)

            all_results[(design_id, step)] = {
                'single_week_df': single_week_df,
                'cumulative_df': cumulative_df,
                'single_uin': single_uin,
                'cumulative_uin': cumulative_uin,
                'single_heatmap': single_heatmap,
                'single_breakdown': single_breakdown,
                'cumulative_heatmap': cumulative_heatmap,
                'cumulative_breakdown': cumulative_breakdown,
                'recovery_result': recovery_result,
                'recovery_chart': recovery_chart
            }

    # Generate HTML report
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = '{}/hybrid_dpm_analysis_WW{}.html'.format(OUTPUT_DIR, requested_ww)
    generate_html_report(all_results, requested_ww, workweek_range, output_file)

    print("\n" + "=" * 90)
    print("HTML Report saved: {}".format(output_file))
    print("=" * 90)

    return output_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Hybrid DPM Analysis Tool')
    parser.add_argument('--ww', type=str, default='202615',
                        help='Requested workweek (e.g., 202615)')
    args = parser.parse_args()

    output_file = main(args.ww)

    # Open in browser (Microsoft Edge)
    if output_file:
        os.system('microsoft-edge {} 2>/dev/null &'.format(output_file))
