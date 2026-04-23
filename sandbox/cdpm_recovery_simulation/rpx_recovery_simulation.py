# -*- coding: utf-8 -*-
"""
RPx Recovery Simulation Tool

Analyzes failed MSNs to identify false miscompares that can be recovered
with the RPx fix. Uses the socamm_false_miscompare.py script to verify.

Workflow:
1. Get failed MSNs (excluding Hang, Boot) with MSN_STATUS and FAILCRAWLER
2. Get SLASH for each MSN
3. Run false_miscompare.py to check if recoverable
4. Aggregate by MSN_STATUS x FAILCRAWLER
5. Calculate recoverable DPM
"""

import subprocess
import pandas as pd
from io import StringIO
import sys
import os
import argparse
import json
import hashlib
from datetime import datetime

sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD')

import plotly.graph_objects as go

# Configuration
DESIGN_IDS = ['Y6CP', 'Y63N']
STEPS = ['HMB1', 'QMON']
EXCLUDE_MSN_STATUS = {'Pass', 'Boot'}  # Removed Hang - now tracked separately
FALSE_MISCOMPARE_SCRIPT = '/home/nmewes/Y6CP_FA/socamm_false_miscompare.py'
OUTPUT_DIR = '/home/asegaran/MODULE_YIELD_DASHBOARD/sandbox/cdpm_recovery_simulation/output'
CACHE_DIR = '/home/asegaran/MODULE_YIELD_DASHBOARD/sandbox/cdpm_recovery_simulation/cache'

# BIOS Fix Configuration - Timing/speed related failures on Grace systems
# New NVIDIA patch expected to fix these FAILCRAWLERs
BIOS_FIX_FAILCRAWLERS = {'MULTI_BANK_MULTI_DQ'}

# HW + SOP Fix Configuration
# Targets Hang MSN_STATUS failures addressed by:
# - HW Fix: Debris and Speed related issues
# - SOP Fix: HUNG2 retest handling procedures
HW_SOP_MSN_STATUS = {'Hang'}  # MSN_STATUS values targeted by HW + SOP fix
HW_SOP_RECOVERY_RATE = 1.0    # 100% projected recovery


def get_cache_path(cache_type, key):
    """Get cache file path for a given key."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    key_hash = hashlib.md5(key.encode()).hexdigest()[:12]
    return os.path.join(CACHE_DIR, '{}_{}.json'.format(cache_type, key_hash))


def load_cache(cache_type, key):
    """Load cached data if available."""
    cache_path = get_cache_path(cache_type, key)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
                # Check if cache is from today
                if data.get('date') == datetime.now().strftime('%Y-%m-%d'):
                    return data.get('value')
        except:
            pass
    return None


def save_cache(cache_type, key, value):
    """Save data to cache."""
    cache_path = get_cache_path(cache_type, key)
    try:
        with open(cache_path, 'w') as f:
            json.dump({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'key': key,
                'value': value
            }, f)
    except:
        pass


def fetch_failed_msns(design_id, step, workweek):
    """
    Fetch failed MSNs with MSN_STATUS and FAILCRAWLER.
    Excludes Pass, Boot. Includes Hang for separate recovery tracking.
    Returns DataFrame with unique MSN, MSN_STATUS, FAILCRAWLER combinations.
    """
    cache_key = 'failed_msns_{}_{}_WW{}'.format(design_id, step, workweek)
    cached = load_cache('msns', cache_key)
    if cached:
        return pd.DataFrame(cached)

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+msnag',
        '-mfg_workweek={}'.format(workweek),
        '-format=MSN,MSN_STATUS,FAILCRAWLER',
        '-step={}'.format(step.lower()),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
        if result.returncode != 0:
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]

        # Filter out excluded statuses
        df = df[~df['MSN_STATUS'].isin(EXCLUDE_MSN_STATUS)]

        # Get unique combinations
        df = df.drop_duplicates(subset=['MSN', 'MSN_STATUS', 'FAILCRAWLER'])

        # Cache result
        save_cache('msns', cache_key, df.to_dict('records'))

        return df

    except Exception as e:
        print("Error fetching failed MSNs: {}".format(e))
        return pd.DataFrame()


def fetch_total_uin(design_id, step, workweek):
    """Fetch total UIN for DPM calculation."""
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag',
        '-mfg_workweek={}'.format(workweek),
        '-format=STEPTYPE,DESIGN_ID,STEP,MFG_WORKWEEK',
        '-step={}'.format(step.lower()),
        '-MOD_CUSTOM_TEST_FLOW<>HMB1_NPI_FLOW',
    ]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
        if result.returncode != 0:
            return 0

        output = result.stdout.decode()
        if not output.strip():
            return 0

        df = pd.read_csv(StringIO(output))
        df.columns = [c.upper() for c in df.columns]

        return df['UIN'].sum() if 'UIN' in df.columns else 0

    except Exception as e:
        print("Error fetching UIN: {}".format(e))
        return 0


def get_slash_for_msn(msn, step):
    """Get SLASH (summary) for a given MSN and step."""
    cache_key = 'slash_{}_{}'.format(msn, step)
    cached = load_cache('slash', cache_key)
    if cached:
        return cached

    cmd = '/u/dramsoft/bin/mtsums {} -step={} -format=summary'.format(msn, step.lower())

    try:
        result = subprocess.run(
            cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=30
        )

        output = result.stdout.decode().strip()
        lines = output.split('\n')

        # Format is: SUMMARY, ~~~~, <actual_slash>, Returned...
        # Find line that looks like a SLASH (contains /)
        for line in lines:
            line = line.strip()
            if '/' in line and not line.startswith('Returned') and line != 'SUMMARY':
                save_cache('slash', cache_key, line)
                return line

    except Exception as e:
        pass

    return None


def check_false_miscompare(slash):
    """
    Run false_miscompare.py for a SLASH and check if it's a false fail.
    Returns dict with is_false_fail, signature, msn_status from script.
    """
    if not slash:
        return {'is_false_fail': False, 'signature': None, 'rows': 0}

    cache_key = 'falsemis_{}'.format(slash)
    cached = load_cache('falsemis', cache_key)
    if cached:
        return cached

    cmd = '{} --sums {}'.format(FALSE_MISCOMPARE_SCRIPT, slash)

    try:
        result = subprocess.run(
            cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=60
        )

        output = result.stdout.decode().strip()
        if not output:
            result_data = {'is_false_fail': False, 'signature': None, 'rows': 0}
            save_cache('falsemis', cache_key, result_data)
            return result_data

        # Parse CSV output
        lines = output.split('\n')
        if len(lines) < 2:
            result_data = {'is_false_fail': False, 'signature': None, 'rows': 0}
            save_cache('falsemis', cache_key, result_data)
            return result_data

        # Check for True in False Fail? column
        has_true = False
        signature = None
        true_count = 0

        for line in lines[1:]:  # Skip header
            if not line.strip():
                continue
            parts = line.split(',')
            if len(parts) >= 4:
                false_fail = parts[2].strip()
                if false_fail == 'True':
                    has_true = True
                    true_count += 1
                    if not signature:
                        signature = parts[3].strip()

        result_data = {
            'is_false_fail': has_true,
            'signature': signature,
            'rows': true_count
        }
        save_cache('falsemis', cache_key, result_data)
        return result_data

    except Exception as e:
        return {'is_false_fail': False, 'signature': None, 'rows': 0, 'error': str(e)}


def analyze_recovery(design_id, step, workweek, verbose=True):
    """
    Analyze recovery potential for a given design_id, step, workweek.

    Returns dict with:
    - total_uin: Total FID count
    - total_msns: Total failed MSNs (excluding Hang, Boot)
    - recoverable_msns: MSNs with false miscompare
    - breakdown: Per MSN_STATUS breakdown with recovery counts
    - failcrawler_breakdown: Per FAILCRAWLER breakdown
    """
    if verbose:
        print("\n  Fetching failed MSNs...")

    msn_df = fetch_failed_msns(design_id, step, workweek)
    if msn_df.empty:
        return None

    total_uin = fetch_total_uin(design_id, step, workweek)
    if total_uin == 0:
        return None

    # Get unique MSNs
    unique_msns = msn_df['MSN'].unique()
    total_msns = len(unique_msns)

    # Separate HANG MSNs from other failures
    hang_df = msn_df[msn_df['MSN_STATUS'].isin(HW_SOP_MSN_STATUS)].copy()
    non_hang_df = msn_df[~msn_df['MSN_STATUS'].isin(HW_SOP_MSN_STATUS)].copy()

    hang_msns = set(hang_df['MSN'].unique()) if not hang_df.empty else set()
    non_hang_msns = set(non_hang_df['MSN'].unique()) if not non_hang_df.empty else set()

    if verbose:
        print("  Found {} unique failed MSNs ({} Hang, {} other)".format(
            total_msns, len(hang_msns), len(non_hang_msns)))
        print("  Checking false miscompares (RPx), BIOS fix, and HANG recovery...")

    # Build MSN to FAILCRAWLER mapping first
    msn_failcrawlers = {}
    msn_statuses = {}
    for _, row in msn_df.iterrows():
        msn = row['MSN']
        failcrawler = row['FAILCRAWLER']
        msn_status = row['MSN_STATUS']
        if msn not in msn_failcrawlers:
            msn_failcrawlers[msn] = set()
        msn_failcrawlers[msn].add(failcrawler)
        msn_statuses[msn] = msn_status

    # Check each MSN for all recovery types
    msn_results = {}
    rpx_recoverable_count = 0
    bios_recoverable_count = 0
    hw_sop_recoverable_count = 0
    combined_recoverable_count = 0

    for i, msn in enumerate(unique_msns):
        if verbose and (i + 1) % 10 == 0:
            print("    Processed {}/{}...".format(i + 1, total_msns))

        msn_status = msn_statuses.get(msn, '')
        is_hw_sop_target = msn_status in HW_SOP_MSN_STATUS

        # For HW+SOP target MSNs (Hang), skip RPx check (different recovery path)
        if is_hw_sop_target:
            # HW + SOP recovery - 100% projected
            is_hw_sop_recoverable = True  # Projected 100% recovery
            msn_results[msn] = {
                'slash': None,
                'msn_status': msn_status,
                'is_rpx_recoverable': False,
                'is_bios_recoverable': False,
                'is_hw_sop_recoverable': is_hw_sop_recoverable,
                'is_combined_recoverable': is_hw_sop_recoverable,
                'signature': None,
                'failcrawlers': list(msn_failcrawlers.get(msn, set()))
            }
            hw_sop_recoverable_count += 1
            combined_recoverable_count += 1
        else:
            # Non-Hang: Check RPx and BIOS recovery
            # Get SLASH for RPx check
            slash = get_slash_for_msn(msn, step)

            # Check false miscompare (RPx recovery)
            fm_result = check_false_miscompare(slash)
            is_rpx_recoverable = fm_result['is_false_fail']

            # Check BIOS fix recovery (MULTI_BANK_MULTI_DQ FAILCRAWLER)
            msn_fcs = msn_failcrawlers.get(msn, set())
            is_bios_recoverable = bool(msn_fcs & BIOS_FIX_FAILCRAWLERS)

            # Combined recovery (either fix would help)
            is_combined_recoverable = is_rpx_recoverable or is_bios_recoverable

            msn_results[msn] = {
                'slash': slash,
                'msn_status': msn_status,
                'is_rpx_recoverable': is_rpx_recoverable,
                'is_bios_recoverable': is_bios_recoverable,
                'is_hw_sop_recoverable': False,
                'is_combined_recoverable': is_combined_recoverable,
                'signature': fm_result['signature'],
                'failcrawlers': list(msn_fcs)
            }

            if is_rpx_recoverable:
                rpx_recoverable_count += 1
            if is_bios_recoverable:
                bios_recoverable_count += 1
            if is_combined_recoverable:
                combined_recoverable_count += 1

    if verbose:
        print("  RPx Recoverable: {}/{} MSNs".format(rpx_recoverable_count, total_msns))
        print("  BIOS Fix Recoverable: {}/{} MSNs".format(bios_recoverable_count, total_msns))
        print("  HW+SOP Fix Recoverable: {}/{} MSNs".format(hw_sop_recoverable_count, total_msns))
        print("  Combined Recoverable: {}/{} MSNs".format(combined_recoverable_count, total_msns))

    # Build breakdown by MSN_STATUS with all recovery types
    msn_status_breakdown = {}
    for _, row in msn_df.iterrows():
        msn = row['MSN']
        msn_status = row['MSN_STATUS']
        failcrawler = row['FAILCRAWLER']

        if msn_status not in msn_status_breakdown:
            msn_status_breakdown[msn_status] = {
                'total_msns': set(),
                'rpx_recoverable_msns': set(),
                'bios_recoverable_msns': set(),
                'hw_sop_recoverable_msns': set(),
                'combined_recoverable_msns': set(),
                'failcrawlers': {}
            }

        msn_status_breakdown[msn_status]['total_msns'].add(msn)

        msn_result = msn_results.get(msn, {})
        if msn_result.get('is_rpx_recoverable'):
            msn_status_breakdown[msn_status]['rpx_recoverable_msns'].add(msn)
        if msn_result.get('is_bios_recoverable'):
            msn_status_breakdown[msn_status]['bios_recoverable_msns'].add(msn)
        if msn_result.get('is_hw_sop_recoverable'):
            msn_status_breakdown[msn_status]['hw_sop_recoverable_msns'].add(msn)
        if msn_result.get('is_combined_recoverable') or msn_result.get('is_hw_sop_recoverable'):
            msn_status_breakdown[msn_status]['combined_recoverable_msns'].add(msn)

        # Track failcrawler with all recovery types
        if failcrawler not in msn_status_breakdown[msn_status]['failcrawlers']:
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler] = {
                'total': set(),
                'rpx_recoverable': set(),
                'bios_recoverable': set(),
                'hw_sop_recoverable': set(),
                'combined_recoverable': set()
            }
        msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['total'].add(msn)
        if msn_result.get('is_rpx_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['rpx_recoverable'].add(msn)
        if msn_result.get('is_bios_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['bios_recoverable'].add(msn)
        if msn_result.get('is_hw_sop_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['hw_sop_recoverable'].add(msn)
        if msn_result.get('is_combined_recoverable') or msn_result.get('is_hw_sop_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['combined_recoverable'].add(msn)

    # Convert sets to counts and calculate DPM for all scenarios
    breakdown = []
    for msn_status, data in msn_status_breakdown.items():
        total = len(data['total_msns'])
        rpx_rec = len(data['rpx_recoverable_msns'])
        bios_rec = len(data['bios_recoverable_msns'])
        hw_sop_rec = len(data['hw_sop_recoverable_msns'])
        combined_rec = len(data['combined_recoverable_msns'])

        # DPM calculation (MSN-based)
        total_dpm = (total / total_uin) * 1_000_000
        rpx_rec_dpm = (rpx_rec / total_uin) * 1_000_000
        bios_rec_dpm = (bios_rec / total_uin) * 1_000_000
        hw_sop_rec_dpm = (hw_sop_rec / total_uin) * 1_000_000
        combined_rec_dpm = (combined_rec / total_uin) * 1_000_000

        # Failcrawler breakdown with all recovery types
        fc_breakdown = []
        for fc, fc_data in data['failcrawlers'].items():
            fc_total = len(fc_data['total'])
            fc_rpx = len(fc_data['rpx_recoverable'])
            fc_bios = len(fc_data['bios_recoverable'])
            fc_hw_sop = len(fc_data['hw_sop_recoverable'])
            fc_combined = len(fc_data['combined_recoverable'])
            is_bios_target = fc in BIOS_FIX_FAILCRAWLERS
            is_hw_sop_target = msn_status in HW_SOP_MSN_STATUS
            fc_breakdown.append({
                'failcrawler': fc,
                'total': fc_total,
                'rpx_recoverable': fc_rpx,
                'bios_recoverable': fc_bios,
                'hw_sop_recoverable': fc_hw_sop,
                'combined_recoverable': fc_combined,
                'is_bios_target': is_bios_target,
                'is_hw_sop_target': is_hw_sop_target,
                'recovery_rate': round(fc_combined / fc_total * 100, 1) if fc_total > 0 else 0
            })

        breakdown.append({
            'msn_status': msn_status,
            'total_msns': total,
            # RPx recovery
            'rpx_recoverable_msns': rpx_rec,
            'rpx_recovery_rate': round(rpx_rec / total * 100, 1) if total > 0 else 0,
            'rpx_recoverable_dpm': round(rpx_rec_dpm, 2),
            'rpx_remaining_dpm': round(total_dpm - rpx_rec_dpm, 2),
            # BIOS fix recovery
            'bios_recoverable_msns': bios_rec,
            'bios_recovery_rate': round(bios_rec / total * 100, 1) if total > 0 else 0,
            'bios_recoverable_dpm': round(bios_rec_dpm, 2),
            'bios_remaining_dpm': round(total_dpm - bios_rec_dpm, 2),
            # HW + SOP fix recovery
            'hw_sop_recoverable_msns': hw_sop_rec,
            'hw_sop_recovery_rate': round(hw_sop_rec / total * 100, 1) if total > 0 else 0,
            'hw_sop_recoverable_dpm': round(hw_sop_rec_dpm, 2),
            'hw_sop_remaining_dpm': round(total_dpm - hw_sop_rec_dpm, 2),
            # Combined recovery (all three fixes)
            'combined_recoverable_msns': combined_rec,
            'combined_recovery_rate': round(combined_rec / total * 100, 1) if total > 0 else 0,
            'combined_recoverable_dpm': round(combined_rec_dpm, 2),
            'combined_remaining_dpm': round(total_dpm - combined_rec_dpm, 2),
            # Common fields
            'total_dpm': round(total_dpm, 2),
            # Legacy fields for backward compatibility
            'recoverable_msns': rpx_rec,
            'remaining_msns': total - rpx_rec,
            'recovery_rate': round(rpx_rec / total * 100, 1) if total > 0 else 0,
            'recoverable_dpm': round(rpx_rec_dpm, 2),
            'remaining_dpm': round(total_dpm - rpx_rec_dpm, 2),
            'failcrawlers': sorted(fc_breakdown, key=lambda x: x['total'], reverse=True)
        })

    # Sort by total DPM
    breakdown = sorted(breakdown, key=lambda x: x['total_dpm'], reverse=True)

    # Calculate totals for all scenarios
    total_dpm = sum(b['total_dpm'] for b in breakdown)
    rpx_recoverable_dpm = sum(b['rpx_recoverable_dpm'] for b in breakdown)
    bios_recoverable_dpm = sum(b['bios_recoverable_dpm'] for b in breakdown)
    hw_sop_recoverable_dpm = sum(b['hw_sop_recoverable_dpm'] for b in breakdown)
    combined_recoverable_dpm = sum(b['combined_recoverable_dpm'] for b in breakdown)

    return {
        'design_id': design_id,
        'step': step,
        'workweek': workweek,
        'total_uin': total_uin,
        'total_msns': total_msns,
        'total_dpm': round(total_dpm, 2),
        # RPx recovery summary
        'rpx_recoverable_msns': rpx_recoverable_count,
        'rpx_recoverable_dpm': round(rpx_recoverable_dpm, 2),
        'rpx_remaining_dpm': round(total_dpm - rpx_recoverable_dpm, 2),
        'rpx_recovery_rate': round(rpx_recoverable_count / total_msns * 100, 1) if total_msns > 0 else 0,
        # BIOS fix recovery summary
        'bios_recoverable_msns': bios_recoverable_count,
        'bios_recoverable_dpm': round(bios_recoverable_dpm, 2),
        'bios_remaining_dpm': round(total_dpm - bios_recoverable_dpm, 2),
        'bios_recovery_rate': round(bios_recoverable_count / total_msns * 100, 1) if total_msns > 0 else 0,
        # HW + SOP fix recovery summary
        'hw_sop_recoverable_msns': hw_sop_recoverable_count,
        'hw_sop_recoverable_dpm': round(hw_sop_recoverable_dpm, 2),
        'hw_sop_remaining_dpm': round(total_dpm - hw_sop_recoverable_dpm, 2),
        'hw_sop_recovery_rate': round(hw_sop_recoverable_count / total_msns * 100, 1) if total_msns > 0 else 0,
        # Combined recovery summary (all three fixes) - breakdown already includes all
        'combined_recoverable_msns': sum(b['combined_recoverable_msns'] for b in breakdown),
        'combined_recoverable_dpm': round(combined_recoverable_dpm, 2),
        'combined_remaining_dpm': round(total_dpm - combined_recoverable_dpm, 2),
        'combined_recovery_rate': round(sum(b['combined_recoverable_msns'] for b in breakdown) / total_msns * 100, 1) if total_msns > 0 else 0,
        # Legacy fields for backward compatibility
        'recoverable_msns': rpx_recoverable_count,
        'recoverable_dpm': round(rpx_recoverable_dpm, 2),
        'remaining_dpm': round(total_dpm - rpx_recoverable_dpm, 2),
        'overall_recovery_rate': round(rpx_recoverable_count / total_msns * 100, 1) if total_msns > 0 else 0,
        'breakdown': breakdown,
        'msn_results': msn_results
    }


def create_recovery_chart(result):
    """Create horizontal bar chart showing recovery by MSN_STATUS."""
    if not result or not result.get('breakdown'):
        return None

    breakdown = result['breakdown']

    labels = [b['msn_status'] for b in breakdown]
    remaining = [b['remaining_dpm'] for b in breakdown]
    recoverable = [b['recoverable_dpm'] for b in breakdown]

    fig = go.Figure()

    # Remaining (red)
    fig.add_trace(go.Bar(
        y=labels,
        x=remaining,
        name='Remaining',
        orientation='h',
        marker_color='#c62828',
        text=['{:.1f}'.format(v) for v in remaining],
        textposition='inside',
        textfont=dict(size=9, color='white')
    ))

    # Recoverable (green)
    fig.add_trace(go.Bar(
        y=labels,
        x=recoverable,
        name='Recoverable (RPx)',
        orientation='h',
        marker_color='#4caf50',
        text=['{:.1f}'.format(v) if v > 0 else '' for v in recoverable],
        textposition='inside',
        textfont=dict(size=9, color='white')
    ))

    fig.update_layout(
        barmode='stack',
        title=dict(
            text='{} {} - DPM Recovery'.format(result['design_id'], result['step']),
            x=0.5,
            font=dict(size=11)
        ),
        xaxis=dict(title='DPM', tickfont=dict(size=9)),
        yaxis=dict(tickfont=dict(size=9), autorange='reversed'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5, font=dict(size=9)),
        height=220,
        width=380,
        margin=dict(l=80, r=20, t=50, b=40),
        paper_bgcolor='white',
        plot_bgcolor='white'
    )

    return fig


def generate_html_report(all_results, workweek, output_file):
    """Generate HTML report for recovery simulation."""
    html_parts = ["""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>RPx Recovery Simulation - WW{}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 10px; background: #f0f0f0; font-size: 12px; }}
        h1 {{ color: #2e7d32; text-align: center; margin: 5px 0; font-size: 18px; }}
        h2 {{ color: white; background: #2e7d32; padding: 5px 10px; margin: 0; font-size: 13px; border-radius: 4px 4px 0 0; }}
        .container {{ max-width: 100%; margin: 0 auto; }}
        .section {{ background: white; margin: 8px 0; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }}
        .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; padding: 10px; }}
        .cell {{ background: #fafafa; padding: 10px; border-radius: 4px; border: 1px solid #e0e0e0; }}
        .cell-title {{ font-weight: bold; font-size: 12px; color: #333; margin-bottom: 8px; border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
        .legend {{ background: #e8f5e9; padding: 8px; font-size: 11px; border-left: 3px solid #4caf50; margin: 8px 0; }}
        .summary-box {{ display: flex; gap: 15px; margin-bottom: 10px; }}
        .metric {{ text-align: center; padding: 8px; background: white; border-radius: 4px; border: 1px solid #ddd; flex: 1; }}
        .metric-value {{ font-size: 20px; font-weight: bold; }}
        .metric-label {{ font-size: 10px; color: #666; }}
        .red {{ color: #c62828; }}
        .green {{ color: #4caf50; }}
        .blue {{ color: #1565c0; }}
        table {{ border-collapse: collapse; width: 100%; font-size: 10px; margin: 5px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 3px 5px; text-align: right; }}
        th {{ background: #4caf50; color: white; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        .msn-col {{ text-align: left; }}
        .chart-container {{ margin-top: 10px; }}
        .fc-details {{ font-size: 9px; color: #666; margin-top: 3px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>RPx Recovery Simulation - WW{}</h1>
        <div class="legend">
            <strong>RPx False Miscompare Recovery:</strong> Identifies bogus failures that can be cleared with RPx fix.
            <span class="green">Green = Recoverable</span> | <span class="red">Red = Real Fails</span>
        </div>
""".format(workweek, workweek)]

    for design_id in DESIGN_IDS:
        html_parts.append('<div class="section">')
        html_parts.append('<h2>{}</h2>'.format(design_id))
        html_parts.append('<div class="grid">')

        for step in STEPS:
            key = (design_id, step)
            result = all_results.get(key)

            html_parts.append('<div class="cell">')
            html_parts.append('<div class="cell-title">{}</div>'.format(step))

            if not result:
                html_parts.append('<p>No data available</p>')
            else:
                # Summary metrics
                html_parts.append('<div class="summary-box">')
                html_parts.append('<div class="metric"><div class="metric-value red">{:.1f}</div><div class="metric-label">Current DPM</div></div>'.format(result['total_dpm']))
                html_parts.append('<div class="metric"><div class="metric-value green">{:.1f}</div><div class="metric-label">Recoverable</div></div>'.format(result['recoverable_dpm']))
                html_parts.append('<div class="metric"><div class="metric-value blue">{:.1f}</div><div class="metric-label">With New RPx</div></div>'.format(result['remaining_dpm']))
                html_parts.append('<div class="metric"><div class="metric-value green">{:.0f}%</div><div class="metric-label">Recovery Rate</div></div>'.format(result['overall_recovery_rate']))
                html_parts.append('</div>')

                # Breakdown table
                html_parts.append('<table>')
                html_parts.append('<tr><th class="msn-col">MSN_STATUS</th><th>MSNs</th><th>Recov</th><th>Rate</th><th>DPM</th><th>Recov DPM</th><th>After</th></tr>')

                for b in result['breakdown']:
                    fc_str = ', '.join(['{}({})'.format(f['failcrawler'], f['recoverable']) for f in b['failcrawlers'][:3] if f['recoverable'] > 0])
                    html_parts.append('<tr><td class="msn-col">{}</td><td>{}</td><td>{}</td><td>{:.0f}%</td><td>{:.1f}</td><td class="green">{:.1f}</td><td>{:.1f}</td></tr>'.format(
                        b['msn_status'], b['total_msns'], b['recoverable_msns'], b['recovery_rate'],
                        b['total_dpm'], b['recoverable_dpm'], b['remaining_dpm']))

                html_parts.append('<tr style="font-weight:bold;background:#e8f5e9;"><td>TOTAL</td><td>{}</td><td>{}</td><td>{:.0f}%</td><td>{:.1f}</td><td class="green">{:.1f}</td><td>{:.1f}</td></tr>'.format(
                    result['total_msns'], result['recoverable_msns'], result['overall_recovery_rate'],
                    result['total_dpm'], result['recoverable_dpm'], result['remaining_dpm']))
                html_parts.append('</table>')

                # Chart
                fig = create_recovery_chart(result)
                if fig:
                    html_parts.append('<div class="chart-container">')
                    html_parts.append(fig.to_html(include_plotlyjs=False, full_html=False))
                    html_parts.append('</div>')

                # MSN_STATUS x FAILCRAWLER detailed table
                html_parts.append('<details style="margin-top:10px;"><summary style="cursor:pointer;color:#2e7d32;font-weight:bold;">MSN_STATUS x FAILCRAWLER Details</summary>')
                html_parts.append('<table style="margin-top:5px;">')
                html_parts.append('<tr><th class="msn-col">MSN_STATUS</th><th class="msn-col">FAILCRAWLER</th><th>Tot</th><th>Rec</th><th>Rate</th><th>DPM</th><th>Rec</th><th>After</th></tr>')

                for b in result['breakdown']:
                    msn_status = b['msn_status']
                    for fc in b['failcrawlers']:
                        if fc['total'] > 0:
                            fc_dpm = (fc['total'] / result['total_uin']) * 1_000_000
                            fc_rec_dpm = (fc['recoverable'] / result['total_uin']) * 1_000_000
                            fc_after = fc_dpm - fc_rec_dpm
                            rec_class = ' class="green"' if fc['recoverable'] > 0 else ''
                            html_parts.append('<tr><td class="msn-col">{}</td><td class="msn-col">{}</td><td>{}</td><td{}>{}</td><td>{:.0f}%</td><td>{:.1f}</td><td{}>{:.1f}</td><td>{:.1f}</td></tr>'.format(
                                msn_status, fc['failcrawler'],
                                fc['total'], rec_class, fc['recoverable'], fc['recovery_rate'],
                                fc_dpm, rec_class, fc_rec_dpm, fc_after))

                html_parts.append('</table></details>')

            html_parts.append('</div>')

        html_parts.append('</div>')  # Close grid
        html_parts.append('</div>')  # Close section

    html_parts.append('</div></body></html>')

    with open(output_file, 'w') as f:
        f.write('\n'.join(html_parts))

    return output_file


def main(requested_ww=None):
    """Main function."""
    if requested_ww is None:
        requested_ww = '202615'

    requested_ww = str(requested_ww)

    print("\n" + "=" * 80)
    print("RPx RECOVERY SIMULATION")
    print("=" * 80)
    print("Workweek: WW{}".format(requested_ww))
    print("Design IDs: {}".format(', '.join(DESIGN_IDS)))
    print("Steps: {}".format(', '.join(STEPS)))
    print("Excluding: {}".format(', '.join(EXCLUDE_MSN_STATUS)))

    all_results = {}

    for design_id in DESIGN_IDS:
        print("\n" + "-" * 60)
        print("{}".format(design_id))
        print("-" * 60)

        for step in STEPS:
            print("\n[{} {}]".format(design_id, step))

            result = analyze_recovery(design_id, step, requested_ww, verbose=True)

            if result:
                all_results[(design_id, step)] = result

                # Print summary
                print("\n  SUMMARY:")
                print("  {:>15}: {:.2f} DPM".format("Current", result['total_dpm']))
                print("  {:>15}: {:.2f} DPM ({:.0f}%)".format("Recoverable", result['recoverable_dpm'], result['overall_recovery_rate']))
                print("  {:>15}: {:.2f} DPM".format("With New RPx", result['remaining_dpm']))

                print("\n  BREAKDOWN BY MSN_STATUS:")
                print("  {:<15} {:>6} {:>6} {:>6} {:>8} {:>8}".format(
                    "MSN_STATUS", "MSNs", "Recov", "Rate", "DPM", "Recov"))
                for b in result['breakdown']:
                    print("  {:<15} {:>6} {:>6} {:>5.0f}% {:>8.1f} {:>8.1f}".format(
                        b['msn_status'], b['total_msns'], b['recoverable_msns'],
                        b['recovery_rate'], b['total_dpm'], b['recoverable_dpm']))

                # Print MSN_STATUS x FAILCRAWLER breakdown
                print("\n  MSN_STATUS x FAILCRAWLER (with recovery):")
                print("  {:<15} {:<30} {:>5} {:>5} {:>6} {:>8} {:>8} {:>8}".format(
                    "MSN_STATUS", "FAILCRAWLER", "Tot", "Rec", "Rate", "DPM", "Rec DPM", "After"))
                print("  " + "-" * 100)
                for b in result['breakdown']:
                    msn_status = b['msn_status']
                    for fc in b['failcrawlers']:
                        if fc['total'] > 0:
                            fc_dpm = (fc['total'] / result['total_uin']) * 1_000_000
                            fc_rec_dpm = (fc['recoverable'] / result['total_uin']) * 1_000_000
                            fc_after = fc_dpm - fc_rec_dpm
                            print("  {:<15} {:<30} {:>5} {:>5} {:>5.0f}% {:>8.1f} {:>8.1f} {:>8.1f}".format(
                                msn_status, fc['failcrawler'],
                                fc['total'], fc['recoverable'], fc['recovery_rate'],
                                fc_dpm, fc_rec_dpm, fc_after))

    # Generate HTML report
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = '{}/rpx_recovery_simulation_WW{}.html'.format(OUTPUT_DIR, requested_ww)
    generate_html_report(all_results, requested_ww, output_file)

    print("\n" + "=" * 80)
    print("HTML Report: {}".format(output_file))
    print("=" * 80)

    return output_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RPx Recovery Simulation Tool')
    parser.add_argument('--ww', type=str, default='202615',
                        help='Workweek (e.g., 202615)')
    args = parser.parse_args()

    output_file = main(args.ww)

    # Open in Microsoft Edge
    if output_file:
        os.system('microsoft-edge {} 2>/dev/null &'.format(output_file))
