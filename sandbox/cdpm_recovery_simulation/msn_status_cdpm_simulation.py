"""
cDPM Recovery Simulation - MSN_STATUS Based (Hybrid Approach)

Simulates cDPM recovery by analyzing MSN_STATUS → FAILCRAWLER breakdown.
Vice versa of the current FAILCRAWLER → MSN_STATUS correlation view.

Focus: HMB1 and QMON steps for SOCAMM2 Y6CP

HYBRID DPM APPROACH (per Kevin Roos):
======================================
For MODULE-LEVEL failures (Mod-Sys, Hang, Multi-Mod, Boot):
  - These are correlated events - one cause triggers all FIDs to fail
  - DPM = (Failing MSNs / Total FIDs) × 1M
  - This is essentially MDPM / 64 (normalized by component density)
  - Prevents inflation from counting one event as 64 failures

For FID-LEVEL failures (DQ, Row, SB_Int, Multi-DQ):
  - These are independent component failures
  - DPM = (Failing FIDs / Total FIDs) × 1M
  - Standard cDPM calculation

For SOCAMM/SOCAMM2:
- 4 packages per module
- 16 DQ per package
- 64 FIDs total per module
"""

import subprocess
import pandas as pd
from io import StringIO
import sys

# Add parent paths for imports
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD')
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD/src')

from src.fiscal_calendar import get_fiscal_month

# Define which MSN_STATUS categories are module-level vs FID-level
MODULE_LEVEL_FAILURES = {'Mod-Sys', 'Hang', 'Multi-Mod', 'Boot'}
FID_LEVEL_FAILURES = {'DQ', 'Row', 'SB_Int', 'Multi-DQ', 'SB', 'Col'}

# SOCAMM/SOCAMM2 configuration
FIDS_PER_MODULE = 64


def fetch_msn_failcrawler_data(design_ids, steps, workweeks):
    """
    Fetch FAILCRAWLER data with MSN_STATUS grouping.
    Uses +fidag for component-level (FID) aggregation.

    Returns DataFrame with columns:
    - MFG_WORKWEEK, DESIGN_ID, STEP, MSN_STATUS, FAILCRAWLER, UIN, UFAIL
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    # Command with MSN_STATUS and FAILCRAWLER grouping for detailed breakdown
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id_str),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag',  # FID aggregation for component-level counts
        '-mfg_workweek={}'.format(workweek_str),
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK,MSN_STATUS,FAILCRAWLER',
        '-step={}'.format(step_str),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    print("Fetching MSN_STATUS × FAILCRAWLER data...")
    print("  DIDs: {}".format(design_id_str))
    print("  Steps: {}".format(step_str))
    print("  Workweeks: {} weeks".format(len(workweeks)))

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300
        )

        if result.returncode != 0:
            print("Error: mtsums failed")
            print(result.stderr.decode())
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            print("Warning: No data returned")
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]

        print("  Fetched {} records".format(len(df)))
        return df

    except Exception as e:
        print("Error fetching data: {}".format(e))
        return pd.DataFrame()


def fetch_module_level_data(design_ids, steps, workweeks):
    """
    Fetch module-level (MSN) data for counting unique failing modules.
    Uses +msnag for module-level aggregation.
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
        '+msnag',  # Module-level aggregation
        '-mfg_workweek={}'.format(workweek_str),
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK,MSN_STATUS,FAILCRAWLER',
        '-step={}'.format(step_str),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    print("Fetching module-level (MSN) data...")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300
        )

        if result.returncode != 0:
            print("Error fetching module data")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]

        print("  Fetched {} module-level records".format(len(df)))
        return df

    except Exception as e:
        print("Error: {}".format(e))
        return pd.DataFrame()


def fetch_total_uin(design_ids, steps, workweeks):
    """
    Fetch total UIN (component level) for the denominator.
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
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK',
        '-step={}'.format(step_str),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    print("Fetching total UIN for denominator...")

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
        print("Error: {}".format(e))
        return {}


def get_hybrid_dpm_breakdown(fid_df, msn_df, step, workweek, total_uin):
    """
    Get DPM breakdown using HYBRID approach:
    - Module-level failures (Mod-Sys, Hang, Multi-Mod): MSNs / Total FIDs
    - FID-level failures (DQ, Row, SB, etc.): UFAILs / Total FIDs

    Returns dict with breakdown by MSN_STATUS.
    """
    if fid_df.empty or total_uin == 0:
        return None

    # Filter by step and workweek
    fid_step = fid_df[fid_df['STEP'].str.upper() == step.upper()].copy()
    msn_step = msn_df[msn_df['STEP'].str.upper() == step.upper()].copy() if not msn_df.empty else pd.DataFrame()

    if 'MFG_WORKWEEK' in fid_step.columns:
        fid_step = fid_step[fid_step['MFG_WORKWEEK'] == int(workweek)]
    if not msn_step.empty and 'MFG_WORKWEEK' in msn_step.columns:
        msn_step = msn_step[msn_step['MFG_WORKWEEK'] == int(workweek)]

    if fid_step.empty:
        return None

    results = {
        'msn_status': [],
        'failure_level': [],
        'count': [],  # MSNs for module-level, UFAILs for FID-level
        'dpm': [],
        'pct_of_total': [],
        'failcrawler_breakdown': {}
    }

    total_dpm = 0
    msn_data = []

    for msn_status in fid_step['MSN_STATUS'].unique():
        if msn_status == 'Pass':
            continue

        fid_msn = fid_step[fid_step['MSN_STATUS'] == msn_status]
        msn_msn = msn_step[msn_step['MSN_STATUS'] == msn_status] if not msn_step.empty else pd.DataFrame()

        # Determine if module-level or FID-level failure
        is_module_level = msn_status in MODULE_LEVEL_FAILURES

        if is_module_level:
            # Module-level: count unique MSNs, divide by total FIDs
            # This gives DPM = MDPM / 64
            if not msn_msn.empty and 'UFAIL' in msn_msn.columns:
                count = msn_msn['UFAIL'].sum()  # Unique failing MSNs
            else:
                # Estimate from FID data: UFAIL / 64 (approximate MSN count)
                ufail = fid_msn['UFAIL'].sum() if 'UFAIL' in fid_msn.columns else 0
                count = max(1, ufail // FIDS_PER_MODULE)

            dpm = (count / total_uin) * 1_000_000
            failure_level = 'MODULE'
        else:
            # FID-level: count UFAILs (component failures)
            count = fid_msn['UFAIL'].sum() if 'UFAIL' in fid_msn.columns else 0
            dpm = (count / total_uin) * 1_000_000
            failure_level = 'FID'

        # Get FAILCRAWLER breakdown
        fc_breakdown = {}
        if 'FAILCRAWLER' in fid_msn.columns and 'UFAIL' in fid_msn.columns:
            fc_agg = fid_msn.groupby('FAILCRAWLER')['UFAIL'].sum()
            for fc, fc_ufail in fc_agg.items():
                if fc_ufail > 0:
                    if is_module_level:
                        # For module-level, estimate MSN count from UFAIL
                        fc_count = max(1, fc_ufail // FIDS_PER_MODULE)
                        fc_dpm = (fc_count / total_uin) * 1_000_000
                    else:
                        fc_dpm = (fc_ufail / total_uin) * 1_000_000
                    fc_breakdown[fc] = {
                        'ufail': int(fc_ufail),
                        'dpm': round(fc_dpm, 2)
                    }

        msn_data.append({
            'msn_status': msn_status,
            'failure_level': failure_level,
            'count': count,
            'dpm': dpm,
            'fc_breakdown': fc_breakdown
        })
        total_dpm += dpm

    # Sort by DPM descending
    msn_data = sorted(msn_data, key=lambda x: x['dpm'], reverse=True)

    # Build results
    for item in msn_data:
        results['msn_status'].append(item['msn_status'])
        results['failure_level'].append(item['failure_level'])
        results['count'].append(int(item['count']))
        results['dpm'].append(round(item['dpm'], 2))
        pct = (item['dpm'] / total_dpm * 100) if total_dpm > 0 else 0
        results['pct_of_total'].append(round(pct, 1))
        results['failcrawler_breakdown'][item['msn_status']] = item['fc_breakdown']

    results['total_uin'] = int(total_uin)
    results['total_dpm'] = round(total_dpm, 2)

    return results


def simulate_recovery(breakdown, msn_statuses_to_fix):
    """
    Simulate DPM recovery if specified MSN_STATUS issues are fixed.
    """
    if breakdown is None:
        return None

    current_dpm = breakdown['total_dpm']
    fixed_dpm = 0
    fixed_items = []

    for i, msn_status in enumerate(breakdown['msn_status']):
        if msn_status in msn_statuses_to_fix:
            fixed_dpm += breakdown['dpm'][i]
            fixed_items.append({
                'msn_status': msn_status,
                'failure_level': breakdown['failure_level'][i],
                'count_removed': breakdown['count'][i],
                'dpm_removed': breakdown['dpm'][i],
                'pct_of_total': breakdown['pct_of_total'][i]
            })

    recovered_dpm = current_dpm - fixed_dpm
    recovery_pct = (fixed_dpm / current_dpm * 100) if current_dpm > 0 else 0

    return {
        'current_dpm': round(current_dpm, 2),
        'recovered_dpm': round(recovered_dpm, 2),
        'recovery_amount': round(fixed_dpm, 2),
        'recovery_pct': round(recovery_pct, 1),
        'fixed_items': fixed_items
    }


def print_breakdown(breakdown, step, workweek):
    """Print MSN_STATUS breakdown with hybrid DPM."""
    if breakdown is None:
        print("No data available for {} WW{}".format(step, workweek))
        return

    print("\n" + "=" * 80)
    print("{} MSN_STATUS Breakdown - HYBRID DPM (WW{})".format(step, workweek))
    print("=" * 80)
    print("Total UIN (FIDs tested): {:,}".format(breakdown['total_uin']))
    print("Total DPM: {:.2f}".format(breakdown['total_dpm']))
    print("-" * 80)
    print("{:<15} {:>8} {:>12} {:>12} {:>12}".format(
        "MSN_STATUS", "Level", "Count", "DPM", "% of Total"))
    print("-" * 80)

    for i, msn_status in enumerate(breakdown['msn_status']):
        level = breakdown['failure_level'][i]
        count_label = "MSNs" if level == 'MODULE' else "FIDs"
        print("{:<15} {:>8} {:>10} {} {:>12.2f} {:>11.1f}%".format(
            msn_status or "Unknown",
            level,
            breakdown['count'][i],
            count_label,
            breakdown['dpm'][i],
            breakdown['pct_of_total'][i]
        ))

        # Show top 3 FAILCRAWLERs
        fc_breakdown = breakdown['failcrawler_breakdown'].get(msn_status, {})
        if fc_breakdown:
            top_fcs = sorted(fc_breakdown.items(), key=lambda x: x[1]['dpm'], reverse=True)[:3]
            for fc, data in top_fcs:
                print("    └─ {}: {:.2f} DPM".format(fc, data['dpm']))

    print("-" * 80)
    print("\nLegend:")
    print("  MODULE = Correlated failure (one event → all FIDs fail)")
    print("           DPM = (Failing MSNs / Total FIDs) × 1M")
    print("  FID    = Independent component failure")
    print("           DPM = (Failing FIDs / Total FIDs) × 1M")


def print_simulation(simulation, step, msn_statuses_fixed):
    """Print simulation results."""
    if simulation is None:
        print("No simulation data available")
        return

    print("\n" + "=" * 80)
    print("{} DPM RECOVERY SIMULATION".format(step))
    print("=" * 80)
    print("If we fix: {}".format(", ".join(msn_statuses_fixed)))
    print("-" * 80)
    print("Current DPM:      {:>12.2f}".format(simulation['current_dpm']))
    print("After Fix:        {:>12.2f}".format(simulation['recovered_dpm']))
    print("Recovery:         {:>12.2f} ({:.1f}% reduction)".format(
        simulation['recovery_amount'], simulation['recovery_pct']))
    print("-" * 80)

    if simulation['fixed_items']:
        print("\nFixed Items Detail:")
        for item in simulation['fixed_items']:
            unit = "MSNs" if item['failure_level'] == 'MODULE' else "FIDs"
            print("  - {} [{}]: {} {} removed, -{:.2f} DPM ({:.1f}%)".format(
                item['msn_status'],
                item['failure_level'],
                item['count_removed'],
                unit,
                item['dpm_removed'],
                item['pct_of_total']
            ))
    print("=" * 80)


def print_comparison_table():
    """Print explanation of hybrid vs standard DPM approaches."""
    print("\n" + "=" * 80)
    print("DPM CALCULATION COMPARISON")
    print("=" * 80)
    print("""
┌─────────────────┬────────────────────────┬────────────────────────────────────┐
│ Approach        │ Formula                │ Use Case                           │
├─────────────────┼────────────────────────┼────────────────────────────────────┤
│ Standard cDPM   │ Failing FIDs / Total   │ All failures treated as            │
│                 │ FIDs × 1M              │ independent component events       │
├─────────────────┼────────────────────────┼────────────────────────────────────┤
│ MDPM            │ Failing MSNs / Total   │ Module-level yield metric          │
│                 │ MSNs × 1M              │                                    │
├─────────────────┼────────────────────────┼────────────────────────────────────┤
│ Kevin's Hybrid  │ MODULE: MSNs / FIDs    │ Prevents inflation from            │
│                 │ FID: UFAILs / FIDs     │ correlated failures (Hang, etc.)   │
└─────────────────┴────────────────────────┴────────────────────────────────────┘

Example - 1 Hang failure (all 64 FIDs fail together):
  Standard cDPM: 64 failures counted → inflated DPM
  Kevin's Hybrid: 1 MSN failure counted → accurate DPM
""")


def fetch_data_by_did(design_id, steps, workweeks):
    """Fetch all data for a specific Design ID."""
    fid_df = fetch_msn_failcrawler_data([design_id], steps, workweeks)
    msn_df = fetch_module_level_data([design_id], steps, workweeks)
    total_uin_map = fetch_total_uin([design_id], steps, workweeks)
    return fid_df, msn_df, total_uin_map


def analyze_did_step(fid_df, msn_df, total_uin_map, design_id, step, workweeks):
    """Analyze a specific DID and step across all workweeks."""
    print("\n" + "#" * 80)
    print("# {} - {} Analysis".format(design_id, step))
    print("#" * 80)

    for ww in workweeks:
        total_uin = total_uin_map.get((step.upper(), ww), 0)
        if total_uin == 0:
            print("\n[WW{}] No data available for {} {}".format(ww, design_id, step))
            continue

        breakdown = get_hybrid_dpm_breakdown(fid_df, msn_df, step, ww, total_uin)
        print_breakdown(breakdown, "{} {}".format(design_id, step), ww)

        if breakdown:
            # Key recovery scenarios
            print("\n--- Recovery Scenarios for {} {} WW{} ---".format(design_id, step, ww))

            # Module-level fixes
            msn_to_fix = ['Hang']
            simulation = simulate_recovery(breakdown, msn_to_fix)
            if simulation and simulation['recovery_amount'] > 0:
                print_simulation(simulation, step, msn_to_fix)

            msn_to_fix = ['Mod-Sys']
            simulation = simulate_recovery(breakdown, msn_to_fix)
            if simulation and simulation['recovery_amount'] > 0:
                print_simulation(simulation, step, msn_to_fix)

            # FID-level fixes
            msn_to_fix = ['DQ', 'Multi-DQ']
            simulation = simulate_recovery(breakdown, msn_to_fix)
            if simulation and simulation['recovery_amount'] > 0:
                print_simulation(simulation, step, msn_to_fix)

            # Combined: All fixable
            msn_to_fix = ['Hang', 'Mod-Sys', 'DQ', 'Multi-DQ']
            simulation = simulate_recovery(breakdown, msn_to_fix)
            if simulation and simulation['recovery_amount'] > 0:
                print_simulation(simulation, step, msn_to_fix)


def main():
    """Main function to run the simulation."""
    # Configuration - Updated per user request
    design_ids = ['Y6CP', 'Y63N']
    steps = ['HMB1', 'QMON']
    workweeks = ['202613', '202614', '202615']

    print("\n" + "=" * 80)
    print("cDPM Recovery Simulation - HYBRID APPROACH (per Kevin Roos)")
    print("=" * 80)
    print("Design IDs: {}".format(', '.join(design_ids)))
    print("Steps: {}".format(', '.join(steps)))
    print("Workweeks: {}".format(', '.join(workweeks)))

    print_comparison_table()

    # Process each Design ID separately
    for design_id in design_ids:
        print("\n")
        print("*" * 80)
        print("*" + " " * 30 + design_id + " " * 30 + "*")
        print("*" * 80)

        # Fetch data for this DID
        fid_df, msn_df, total_uin_map = fetch_data_by_did(design_id, steps, workweeks)

        if fid_df.empty:
            print("No data available for {}".format(design_id))
            continue

        # Show MSN_STATUS classification for this DID
        if 'MSN_STATUS' in fid_df.columns:
            print("\n{} MSN_STATUS Classification:".format(design_id))
            for status in sorted(fid_df['MSN_STATUS'].unique()):
                if status == 'Pass':
                    continue
                level = 'MODULE' if status in MODULE_LEVEL_FAILURES else 'FID'
                count = len(fid_df[fid_df['MSN_STATUS'] == status])
                print("  - {} [{}] ({} records)".format(status, level, count))

        # Analyze each step separately
        for step in steps:
            analyze_did_step(fid_df, msn_df, total_uin_map, design_id, step, workweeks)


if __name__ == "__main__":
    main()
