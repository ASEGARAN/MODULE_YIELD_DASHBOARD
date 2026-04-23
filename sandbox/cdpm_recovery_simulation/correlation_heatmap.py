"""
MSN_STATUS × FAILCRAWLER Correlation Heatmap Visualization

Generates heatmap visualizations showing DPM correlation between
MSN_STATUS and FAILCRAWLER categories using the hybrid DPM approach.
"""

import subprocess
import pandas as pd
import numpy as np
from io import StringIO
import sys
import os

# Add parent paths for imports
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD')
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD/src')

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# Define which MSN_STATUS categories are module-level vs FID-level
MODULE_LEVEL_FAILURES = {'Mod-Sys', 'Hang', 'Multi-Mod', 'Boot'}
FID_LEVEL_FAILURES = {'DQ', 'Row', 'SB_Int', 'Multi-DQ', 'SB', 'Col', 'Column'}

FIDS_PER_MODULE = 64

# Output directory
OUTPUT_DIR = '/home/asegaran/MODULE_YIELD_DASHBOARD/sandbox/cdpm_recovery_simulation/output'


def fetch_correlation_data(design_ids, steps, workweeks):
    """Fetch MSN_STATUS × FAILCRAWLER correlation data (FID-level)."""
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

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
        print("Error: {}".format(e))
        return pd.DataFrame()


def fetch_module_counts(design_ids, steps, workweeks):
    """Fetch module-level counts (MUFAIL) per MSN_STATUS using +msnag."""
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

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
            return {}
        output = result.stdout.decode()
        if not output.strip():
            return {}
        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]

        # Build lookup: (step, ww, msn_status) -> MUFAIL
        counts = {}
        for _, row in df.iterrows():
            key = (row['STEP'].upper(), str(row['MFG_WORKWEEK']), row['MSN_STATUS'])
            mufail = row.get('MUFAIL', 0)
            counts[key] = mufail

        return counts
    except Exception as e:
        print("Error fetching module counts: {}".format(e))
        return {}


def fetch_total_uin(design_ids, steps, workweeks):
    """Fetch total UIN for denominator."""
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

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
            return {}
        output = result.stdout.decode()
        if not output.strip():
            return {}
        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]

        totals = {}
        for step in steps:
            step_df = df[df['STEP'].str.upper() == step.upper()]
            for ww in workweeks:
                ww_df = step_df[step_df['MFG_WORKWEEK'] == int(ww)]
                if 'UIN' in ww_df.columns:
                    totals[(step.upper(), str(ww))] = ww_df['UIN'].sum()
        return totals
    except Exception as e:
        return {}


def build_correlation_matrix(df, step, workweek, total_uin, module_counts=None):
    """
    Build correlation matrix with hybrid DPM values.

    For MODULE-level failures:
    - Use MUFAIL from +msnag for total module count
    - Distribute DPM proportionally across FAILCRAWLERs based on UFAIL weight

    For FID-level failures:
    - Use UFAIL directly for each FAILCRAWLER
    """
    if df.empty or total_uin == 0:
        return None

    step_df = df[df['STEP'].str.upper() == step.upper()].copy()
    if 'MFG_WORKWEEK' in step_df.columns:
        step_df = step_df[step_df['MFG_WORKWEEK'] == int(workweek)]

    if step_df.empty:
        return None

    step_df = step_df[step_df['MSN_STATUS'] != 'Pass']

    msn_statuses = sorted(step_df['MSN_STATUS'].unique())
    failcrawlers = sorted(step_df['FAILCRAWLER'].unique())

    matrix_data = []

    for msn_status in msn_statuses:
        is_module_level = msn_status in MODULE_LEVEL_FAILURES
        msn_df = step_df[step_df['MSN_STATUS'] == msn_status]

        # For MODULE-level: get MUFAIL from module_counts
        if is_module_level:
            total_ufail_for_msn = msn_df['UFAIL'].sum() if 'UFAIL' in msn_df.columns else 0

            # Get actual module count from +msnag data
            if module_counts:
                key = (step.upper(), workweek, msn_status)
                total_msn_count = module_counts.get(key, 0)
            else:
                # Fallback: estimate from UFAIL
                total_msn_count = max(1, int(total_ufail_for_msn)) if total_ufail_for_msn > 0 else 0

            total_dpm_for_msn = (total_msn_count / total_uin) * 1_000_000

        for fc in failcrawlers:
            fc_df = msn_df[msn_df['FAILCRAWLER'] == fc]

            if fc_df.empty:
                dpm = 0
            else:
                ufail = fc_df['UFAIL'].sum() if 'UFAIL' in fc_df.columns else 0

                if is_module_level:
                    # Distribute MSN DPM proportionally based on UFAIL weight
                    if total_ufail_for_msn > 0:
                        weight = ufail / total_ufail_for_msn
                        dpm = total_dpm_for_msn * weight
                    else:
                        dpm = 0
                else:
                    # FID-level: use UFAIL directly
                    dpm = (ufail / total_uin) * 1_000_000

            level = 'MODULE' if is_module_level else 'FID'
            matrix_data.append({
                'MSN_STATUS': msn_status,
                'FAILCRAWLER': fc,
                'DPM': round(dpm, 2),
                'LEVEL': level
            })

    return pd.DataFrame(matrix_data)


def create_heatmap(matrix_df, design_id, step, workweek, total_uin):
    """Create a single heatmap for one DID/Step/WW combination."""
    if matrix_df is None or matrix_df.empty:
        return None

    # Pivot for heatmap
    pivot_df = matrix_df.pivot(index='MSN_STATUS', columns='FAILCRAWLER', values='DPM').fillna(0)

    # Sort by row totals (descending)
    row_totals = pivot_df.sum(axis=1).sort_values(ascending=False)
    pivot_df = pivot_df.loc[row_totals.index]

    # Sort columns by column totals (descending)
    col_totals = pivot_df.sum(axis=0).sort_values(ascending=False)
    pivot_df = pivot_df[col_totals.index]

    # Limit to top 12 FAILCRAWLERs for readability
    if len(pivot_df.columns) > 12:
        pivot_df = pivot_df.iloc[:, :12]

    # Add level indicator to MSN_STATUS labels
    level_map = {msn: '[M]' if msn in MODULE_LEVEL_FAILURES else '[F]'
                 for msn in pivot_df.index}
    new_index = ["{} {}".format(msn, level_map[msn]) for msn in pivot_df.index]
    pivot_df.index = new_index

    # Truncate long FAILCRAWLER names
    new_cols = [fc[:15] if len(fc) > 15 else fc for fc in pivot_df.columns]
    pivot_df.columns = new_cols

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale=[
            [0, '#f7fbff'],
            [0.2, '#deebf7'],
            [0.4, '#9ecae1'],
            [0.6, '#4292c6'],
            [0.8, '#2171b5'],
            [1.0, '#084594']
        ],
        text=pivot_df.values,
        texttemplate='%{text:.1f}',
        textfont={"size": 10},
        hovertemplate='MSN_STATUS: %{y}<br>FAILCRAWLER: %{x}<br>DPM: %{z:.2f}<extra></extra>',
        colorbar=dict(
            title=dict(text='DPM', side='right')
        )
    ))

    fig.update_layout(
        title=dict(
            text='{} {} - MSN_STATUS × FAILCRAWLER Correlation (WW{})<br><sub>Total UIN: {:,} FIDs | Hybrid DPM Approach</sub>'.format(
                design_id, step, workweek, int(total_uin)),
            x=0.5,
            font=dict(size=16)
        ),
        xaxis=dict(
            title='FAILCRAWLER',
            tickangle=45,
            tickfont=dict(size=10)
        ),
        yaxis=dict(
            title='MSN_STATUS',
            tickfont=dict(size=11)
        ),
        height=500,
        width=900,
        margin=dict(l=120, r=50, t=100, b=150)
    )

    return fig


def create_combined_heatmap(all_data, design_id, step, workweeks):
    """Create a combined heatmap showing trend across workweeks."""
    if not all_data:
        return None

    # Combine all workweeks into single view
    combined_data = []
    for ww, matrix_df in all_data.items():
        if matrix_df is not None:
            df = matrix_df.copy()
            df['WORKWEEK'] = ww
            combined_data.append(df)

    if not combined_data:
        return None

    combined_df = pd.concat(combined_data, ignore_index=True)

    # Aggregate by MSN_STATUS (sum across workweeks for overview)
    agg_df = combined_df.groupby(['MSN_STATUS', 'FAILCRAWLER', 'LEVEL'])['DPM'].mean().reset_index()

    # Pivot
    pivot_df = agg_df.pivot(index='MSN_STATUS', columns='FAILCRAWLER', values='DPM').fillna(0)

    # Sort
    row_totals = pivot_df.sum(axis=1).sort_values(ascending=False)
    pivot_df = pivot_df.loc[row_totals.index]
    col_totals = pivot_df.sum(axis=0).sort_values(ascending=False)
    pivot_df = pivot_df[col_totals.index]

    if len(pivot_df.columns) > 10:
        pivot_df = pivot_df.iloc[:, :10]

    # Add level indicators
    level_map = {msn: '[M]' if msn in MODULE_LEVEL_FAILURES else '[F]'
                 for msn in pivot_df.index}
    new_index = ["{} {}".format(msn, level_map[msn]) for msn in pivot_df.index]
    pivot_df.index = new_index

    new_cols = [fc[:12] if len(fc) > 12 else fc for fc in pivot_df.columns]
    pivot_df.columns = new_cols

    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='Blues',
        text=pivot_df.values,
        texttemplate='%{text:.1f}',
        textfont={"size": 11},
        hovertemplate='MSN_STATUS: %{y}<br>FAILCRAWLER: %{x}<br>Avg DPM: %{z:.2f}<extra></extra>',
        colorbar=dict(title=dict(text='Avg DPM', side='right'))
    ))

    fig.update_layout(
        title=dict(
            text='{} {} - Average MSN_STATUS × FAILCRAWLER Correlation<br><sub>WW{} to WW{} | Hybrid DPM</sub>'.format(
                design_id, step, workweeks[0], workweeks[-1]),
            x=0.5,
            font=dict(size=16)
        ),
        xaxis=dict(title='FAILCRAWLER', tickangle=45, tickfont=dict(size=10)),
        yaxis=dict(title='MSN_STATUS', tickfont=dict(size=11)),
        height=450,
        width=800,
        margin=dict(l=120, r=50, t=100, b=130)
    )

    return fig


def create_trend_heatmap(all_data, design_id, step, workweeks):
    """Create heatmap showing MSN_STATUS DPM trend across workweeks."""
    if not all_data:
        return None

    trend_data = []
    for ww in workweeks:
        if ww in all_data and all_data[ww] is not None:
            df = all_data[ww]
            msn_totals = df.groupby('MSN_STATUS')['DPM'].sum().reset_index()
            msn_totals['WORKWEEK'] = 'WW' + ww
            trend_data.append(msn_totals)

    if not trend_data:
        return None

    trend_df = pd.concat(trend_data, ignore_index=True)
    pivot_df = trend_df.pivot(index='MSN_STATUS', columns='WORKWEEK', values='DPM').fillna(0)

    # Sort by average DPM
    pivot_df['avg'] = pivot_df.mean(axis=1)
    pivot_df = pivot_df.sort_values('avg', ascending=False).drop('avg', axis=1)

    # Add level indicators
    level_map = {msn: '[M]' if msn in MODULE_LEVEL_FAILURES else '[F]'
                 for msn in pivot_df.index}
    new_index = ["{} {}".format(msn, level_map[msn]) for msn in pivot_df.index]
    pivot_df.index = new_index

    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale=[
            [0, '#f7fcf5'],
            [0.25, '#c7e9c0'],
            [0.5, '#74c476'],
            [0.75, '#31a354'],
            [1.0, '#006d2c']
        ],
        text=pivot_df.values,
        texttemplate='%{text:.1f}',
        textfont={"size": 12, "color": "black"},
        hovertemplate='MSN_STATUS: %{y}<br>%{x}<br>DPM: %{z:.2f}<extra></extra>',
        colorbar=dict(title=dict(text='DPM', side='right'))
    ))

    fig.update_layout(
        title=dict(
            text='{} {} - MSN_STATUS DPM Trend<br><sub>Hybrid DPM by Workweek</sub>'.format(design_id, step),
            x=0.5,
            font=dict(size=16)
        ),
        xaxis=dict(title='Workweek', tickfont=dict(size=12)),
        yaxis=dict(title='MSN_STATUS', tickfont=dict(size=11)),
        height=400,
        width=600,
        margin=dict(l=120, r=50, t=100, b=80)
    )

    return fig


def main():
    """Generate all heatmap visualizations."""
    # Configuration
    design_ids = ['Y6CP', 'Y63N']
    steps = ['HMB1', 'QMON']
    workweeks = ['202613', '202614', '202615']

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 80)
    print("MSN_STATUS × FAILCRAWLER Correlation Heatmaps")
    print("=" * 80)

    for design_id in design_ids:
        print("\nProcessing {}...".format(design_id))

        # Fetch FID-level data for FAILCRAWLER breakdown
        df = fetch_correlation_data([design_id], steps, workweeks)
        if df.empty:
            print("  No data for {}".format(design_id))
            continue

        # Fetch module-level counts (MUFAIL) for MODULE-level failures
        module_counts = fetch_module_counts([design_id], steps, workweeks)

        total_uin_map = fetch_total_uin([design_id], steps, workweeks)

        for step in steps:
            print("  {} {}...".format(design_id, step))

            all_matrices = {}

            # Generate individual heatmaps for each workweek
            for ww in workweeks:
                total_uin = total_uin_map.get((step.upper(), ww), 0)
                if total_uin == 0:
                    continue

                matrix_df = build_correlation_matrix(df, step, ww, total_uin, module_counts)
                all_matrices[ww] = matrix_df

                if matrix_df is not None:
                    fig = create_heatmap(matrix_df, design_id, step, ww, total_uin)
                    if fig:
                        filename = "{}/heatmap_{}_{}_WW{}.html".format(OUTPUT_DIR, design_id, step, ww)
                        fig.write_html(filename)
                        print("    Saved: {}".format(filename))

            # Generate combined average heatmap
            if all_matrices:
                fig_combined = create_combined_heatmap(all_matrices, design_id, step, workweeks)
                if fig_combined:
                    filename = "{}/heatmap_{}_{}_combined.html".format(OUTPUT_DIR, design_id, step)
                    fig_combined.write_html(filename)
                    print("    Saved: {}".format(filename))

                # Generate trend heatmap
                fig_trend = create_trend_heatmap(all_matrices, design_id, step, workweeks)
                if fig_trend:
                    filename = "{}/heatmap_{}_{}_trend.html".format(OUTPUT_DIR, design_id, step)
                    fig_trend.write_html(filename)
                    print("    Saved: {}".format(filename))

    # Generate a summary dashboard with all heatmaps
    print("\nGenerating summary dashboard...")
    create_summary_dashboard(design_ids, steps, workweeks)

    print("\n" + "=" * 80)
    print("All heatmaps saved to: {}".format(OUTPUT_DIR))
    print("=" * 80)


def create_summary_dashboard(design_ids, steps, workweeks):
    """Create a summary HTML dashboard with all heatmaps."""
    html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>MSN_STATUS × FAILCRAWLER Correlation Dashboard</title>
    <style>
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #1a237e;
            text-align: center;
            margin-bottom: 30px;
        }}
        h2 {{
            color: #333;
            border-bottom: 2px solid #1a237e;
            padding-bottom: 10px;
            margin-top: 40px;
        }}
        h3 {{
            color: #555;
            margin-top: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .section {{
            background: white;
            padding: 20px;
            margin: 20px 0;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .heatmap-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .heatmap-item {{
            background: #fff;
            padding: 10px;
            border-radius: 5px;
            border: 1px solid #ddd;
        }}
        .heatmap-item iframe {{
            width: 100%;
            height: 450px;
            border: none;
        }}
        .legend {{
            background: #e3f2fd;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .legend code {{
            background: #fff;
            padding: 2px 6px;
            border-radius: 3px;
        }}
        a {{
            color: #1565c0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>MSN_STATUS × FAILCRAWLER Correlation Analysis</h1>
        <p style="text-align: center; color: #666;">Hybrid DPM Approach (per Kevin Roos) | WW{start_ww} - WW{end_ww}</p>

        <div class="legend">
            <strong>Legend:</strong><br>
            <code>[M]</code> = MODULE-level failure (DPM = Failing MSNs / Total FIDs × 1M)<br>
            <code>[F]</code> = FID-level failure (DPM = Failing FIDs / Total FIDs × 1M)
        </div>
""".format(start_ww=workweeks[0], end_ww=workweeks[-1])

    for design_id in design_ids:
        html_content += """
        <div class="section">
            <h2>{}</h2>
""".format(design_id)

        for step in steps:
            html_content += """
            <h3>{} {}</h3>
            <div class="heatmap-grid">
""".format(design_id, step)

            # Add trend heatmap
            trend_file = "heatmap_{}_{}_trend.html".format(design_id, step)
            if os.path.exists("{}/{}".format(OUTPUT_DIR, trend_file)):
                html_content += """
                <div class="heatmap-item">
                    <strong>DPM Trend by Workweek</strong>
                    <iframe src="{}"></iframe>
                </div>
""".format(trend_file)

            # Add combined heatmap
            combined_file = "heatmap_{}_{}_combined.html".format(design_id, step)
            if os.path.exists("{}/{}".format(OUTPUT_DIR, combined_file)):
                html_content += """
                <div class="heatmap-item">
                    <strong>Average Correlation (All Weeks)</strong>
                    <iframe src="{}"></iframe>
                </div>
""".format(combined_file)

            html_content += """
            </div>

            <details>
                <summary style="cursor: pointer; color: #1565c0; margin: 10px 0;">View Individual Workweeks</summary>
                <div class="heatmap-grid">
"""

            # Add individual workweek heatmaps
            for ww in workweeks:
                ww_file = "heatmap_{}_{}_WW{}.html".format(design_id, step, ww)
                if os.path.exists("{}/{}".format(OUTPUT_DIR, ww_file)):
                    html_content += """
                    <div class="heatmap-item">
                        <strong>WW{}</strong>
                        <iframe src="{}"></iframe>
                    </div>
""".format(ww, ww_file)

            html_content += """
                </div>
            </details>
"""

        html_content += """
        </div>
"""

    html_content += """
    </div>
</body>
</html>
"""

    dashboard_file = "{}/correlation_dashboard.html".format(OUTPUT_DIR)
    with open(dashboard_file, 'w') as f:
        f.write(html_content)
    print("  Dashboard saved: {}".format(dashboard_file))


if __name__ == "__main__":
    main()
