"""
MSN_STATUS × FAILCRAWLER Correlation Analysis - Hybrid DPM Approach

Creates a correlation matrix showing DPM breakdown:
- Rows: MSN_STATUS categories
- Columns: FAILCRAWLER categories
- Values: Hybrid DPM (MODULE-level or FID-level based on MSN_STATUS type)

Uses Kevin Roos's hybrid approach:
- MODULE-level (Mod-Sys, Hang, Multi-Mod, Boot): MSNs / Total FIDs × 1M
- FID-level (DQ, Row, SB_Int, Multi-DQ, etc.): UFAILs / Total FIDs × 1M
"""

import subprocess
import pandas as pd
from io import StringIO
import sys

# Add parent paths for imports
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD')
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD/src')

# Define which MSN_STATUS categories are module-level vs FID-level
MODULE_LEVEL_FAILURES = {'Mod-Sys', 'Hang', 'Multi-Mod', 'Boot'}
FID_LEVEL_FAILURES = {'DQ', 'Row', 'SB_Int', 'Multi-DQ', 'SB', 'Col', 'Column'}

# SOCAMM/SOCAMM2 configuration
FIDS_PER_MODULE = 64


def fetch_correlation_data(design_ids, steps, workweeks):
    """
    Fetch MSN_STATUS × FAILCRAWLER correlation data.
    Returns FID-level data with grouping by MSN_STATUS and FAILCRAWLER.
    """
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

    print("Fetching correlation data...")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300
        )

        if result.returncode != 0:
            print("Error: mtsums failed")
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

        counts = {}
        for _, row in df.iterrows():
            key = (row['STEP'].upper(), str(row['MFG_WORKWEEK']), row['MSN_STATUS'])
            mufail = row.get('MUFAIL', 0)
            counts[key] = mufail

        return counts
    except Exception as e:
        return {}


def fetch_total_uin(design_ids, steps, workweeks):
    """Fetch total UIN (FID count) for denominator."""
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
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300
        )

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
    Build MSN_STATUS × FAILCRAWLER correlation matrix with hybrid DPM.

    For MODULE-level failures:
    - Use MUFAIL from +msnag for total module count
    - Distribute DPM proportionally across FAILCRAWLERs based on UFAIL weight

    For FID-level failures:
    - Use UFAIL directly for each FAILCRAWLER

    Returns:
    - matrix_df: DataFrame with MSN_STATUS as rows, FAILCRAWLER as columns, DPM as values
    - row_totals: Total DPM per MSN_STATUS
    - col_totals: Total DPM per FAILCRAWLER
    """
    if df.empty or total_uin == 0:
        return None, None, None

    # Filter by step and workweek
    step_df = df[df['STEP'].str.upper() == step.upper()].copy()
    if 'MFG_WORKWEEK' in step_df.columns:
        step_df = step_df[step_df['MFG_WORKWEEK'] == int(workweek)]

    if step_df.empty:
        return None, None, None

    # Exclude Pass status
    step_df = step_df[step_df['MSN_STATUS'] != 'Pass']

    # Get unique MSN_STATUS and FAILCRAWLER values
    msn_statuses = sorted(step_df['MSN_STATUS'].unique())
    failcrawlers = sorted(step_df['FAILCRAWLER'].unique())

    # Build correlation matrix
    matrix_data = {}
    row_totals = {}
    col_totals = {fc: 0 for fc in failcrawlers}

    for msn_status in msn_statuses:
        is_module_level = msn_status in MODULE_LEVEL_FAILURES
        msn_df = step_df[step_df['MSN_STATUS'] == msn_status]

        matrix_data[msn_status] = {}
        row_total = 0

        # For MODULE-level: get MUFAIL from module_counts
        if is_module_level:
            total_ufail_for_msn = msn_df['UFAIL'].sum() if 'UFAIL' in msn_df.columns else 0

            # Get actual module count from +msnag data
            if module_counts:
                key = (step.upper(), workweek, msn_status)
                total_msn_count = module_counts.get(key, 0)
            else:
                # Fallback: use the raw UFAIL value (already fractional module count)
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

            matrix_data[msn_status][fc] = round(dpm, 2)
            row_total += dpm
            col_totals[fc] += dpm

        row_totals[msn_status] = round(row_total, 2)

    # Round column totals
    col_totals = {fc: round(v, 2) for fc, v in col_totals.items()}

    # Convert to DataFrame
    matrix_df = pd.DataFrame(matrix_data).T
    matrix_df = matrix_df[failcrawlers]  # Ensure column order

    return matrix_df, row_totals, col_totals


def print_correlation_matrix(matrix_df, row_totals, col_totals, design_id, step, workweek, total_uin):
    """Print the correlation matrix in a readable format."""
    if matrix_df is None:
        print("No data available for {} {} WW{}".format(design_id, step, workweek))
        return

    print("\n" + "=" * 100)
    print("{} {} - MSN_STATUS × FAILCRAWLER Correlation (WW{})".format(design_id, step, workweek))
    print("=" * 100)
    print("Total UIN: {:,} FIDs | Hybrid DPM Approach".format(total_uin))
    print("-" * 100)

    # Get top FAILCRAWLERs by total DPM (limit to top 8 for display)
    top_fcs = sorted(col_totals.items(), key=lambda x: x[1], reverse=True)[:8]
    top_fc_names = [fc for fc, _ in top_fcs]

    # Print header
    header = "{:<12}".format("MSN_STATUS")
    for fc in top_fc_names:
        # Truncate long names
        fc_short = fc[:10] if len(fc) > 10 else fc
        header += " {:>10}".format(fc_short)
    header += " {:>10}".format("TOTAL")
    print(header)
    print("-" * 100)

    # Sort MSN_STATUS by total DPM descending
    sorted_msn = sorted(row_totals.items(), key=lambda x: x[1], reverse=True)

    for msn_status, total in sorted_msn:
        is_module = msn_status in MODULE_LEVEL_FAILURES
        level_marker = "[M]" if is_module else "[F]"

        row = "{:<9}{}".format(msn_status[:9], level_marker)
        for fc in top_fc_names:
            val = matrix_df.loc[msn_status, fc] if fc in matrix_df.columns else 0
            if val > 0:
                row += " {:>10.2f}".format(val)
            else:
                row += " {:>10}".format("-")
        row += " {:>10.2f}".format(total)
        print(row)

    # Print column totals
    print("-" * 100)
    footer = "{:<12}".format("TOTAL")
    for fc in top_fc_names:
        footer += " {:>10.2f}".format(col_totals[fc])
    footer += " {:>10.2f}".format(sum(row_totals.values()))
    print(footer)
    print("-" * 100)
    print("\n[M] = MODULE-level (MSNs/FIDs×1M) | [F] = FID-level (UFAILs/FIDs×1M)")


def print_detailed_correlation(matrix_df, row_totals, col_totals, design_id, step, workweek):
    """Print detailed correlation with all FAILCRAWLERs."""
    if matrix_df is None:
        return

    print("\n" + "-" * 80)
    print("Detailed FAILCRAWLER Breakdown by MSN_STATUS")
    print("-" * 80)

    sorted_msn = sorted(row_totals.items(), key=lambda x: x[1], reverse=True)

    for msn_status, total in sorted_msn:
        is_module = msn_status in MODULE_LEVEL_FAILURES
        level = "MODULE" if is_module else "FID"

        print("\n{} [{}] - Total: {:.2f} DPM".format(msn_status, level, total))

        # Get non-zero FAILCRAWLERs for this MSN_STATUS
        fc_values = matrix_df.loc[msn_status]
        non_zero = fc_values[fc_values > 0].sort_values(ascending=False)

        for fc, dpm in non_zero.items():
            pct = (dpm / total * 100) if total > 0 else 0
            print("    └─ {:30} {:>8.2f} DPM ({:>5.1f}%)".format(fc, dpm, pct))


def export_to_csv(matrix_df, row_totals, col_totals, design_id, step, workweek, output_dir):
    """Export correlation matrix to CSV."""
    if matrix_df is None:
        return

    # Add row totals
    export_df = matrix_df.copy()
    export_df['TOTAL_DPM'] = [row_totals[msn] for msn in export_df.index]

    # Add column totals row
    col_total_row = {fc: col_totals.get(fc, 0) for fc in export_df.columns if fc != 'TOTAL_DPM'}
    col_total_row['TOTAL_DPM'] = sum(row_totals.values())
    export_df.loc['TOTAL'] = col_total_row

    # Add level indicator
    export_df.insert(0, 'LEVEL', ['MODULE' if msn in MODULE_LEVEL_FAILURES else 'FID'
                                   for msn in export_df.index[:-1]] + [''])

    filename = "{}/correlation_{}_{}_WW{}.csv".format(output_dir, design_id, step, workweek)
    export_df.to_csv(filename)
    print("\nExported to: {}".format(filename))


def main():
    """Main function to generate correlation matrices."""
    # Configuration
    design_ids = ['Y6CP', 'Y63N']
    steps = ['HMB1', 'QMON']
    workweeks = ['202613', '202614', '202615']

    print("\n" + "=" * 100)
    print("MSN_STATUS × FAILCRAWLER CORRELATION ANALYSIS")
    print("Hybrid DPM Approach (per Kevin Roos)")
    print("=" * 100)
    print("Design IDs: {}".format(', '.join(design_ids)))
    print("Steps: {}".format(', '.join(steps)))
    print("Workweeks: {}".format(', '.join(workweeks)))

    for design_id in design_ids:
        print("\n")
        print("*" * 100)
        print("*" + " " * 45 + design_id + " " * 45 + "*")
        print("*" * 100)

        # Fetch FID-level data for FAILCRAWLER breakdown
        df = fetch_correlation_data([design_id], steps, workweeks)
        if df.empty:
            print("No data available for {}".format(design_id))
            continue

        # Fetch module-level counts (MUFAIL) for MODULE-level failures
        module_counts = fetch_module_counts([design_id], steps, workweeks)

        total_uin_map = fetch_total_uin([design_id], steps, workweeks)

        for step in steps:
            print("\n" + "#" * 100)
            print("# {} - {}".format(design_id, step))
            print("#" * 100)

            for ww in workweeks:
                total_uin = total_uin_map.get((step.upper(), ww), 0)
                if total_uin == 0:
                    print("\n[WW{}] No data available".format(ww))
                    continue

                matrix_df, row_totals, col_totals = build_correlation_matrix(
                    df, step, ww, total_uin, module_counts
                )

                print_correlation_matrix(
                    matrix_df, row_totals, col_totals,
                    design_id, step, ww, total_uin
                )

                # Print detailed breakdown
                print_detailed_correlation(
                    matrix_df, row_totals, col_totals,
                    design_id, step, ww
                )


if __name__ == "__main__":
    main()
