#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SOCAMM2 cDPM Recovery Simulation Tool
=====================================

Analyzes actual DPM vs projected recovery using Kevin Roos's hybrid DPM approach.

Recovery Types:
- New RPx Fix (VERIFIED): False miscompare recovery via signature detection
- New BIOS Fix (PROJECTED): MULTI_BANK_MULTI_DQ timing fix - Ongoing CCE validation
- HW + SOP Fix (PROJECTED): Hang recovery - Debris/Speed + HUNG2 retest

Usage:
    python cdpm_recovery_sim.py --did Y6CP,Y63N --steps HMB1,QMON --ww 202615

Output:
    - HTML report with correlation heatmaps and recovery simulation
    - Terminal summary with key metrics

Author: Manufacturing Engineering Team
Version: 1.0.0
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

try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("Warning: plotly not installed. Charts will be disabled.")
    print("Install with: pip install plotly")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Module-level vs FID-level MSN_STATUS categories (Kevin's hybrid approach)
MODULE_LEVEL_FAILURES = {'Mod-Sys', 'Hang', 'Multi-Mod', 'Boot'}
FID_LEVEL_FAILURES = {'DQ', 'Row', 'SB_Int', 'Multi-DQ', 'SB', 'Col', 'Column'}

# Recovery Configuration
EXCLUDE_MSN_STATUS = {'Pass', 'Boot'}  # Excluded from recovery analysis
FALSE_MISCOMPARE_SCRIPT = '/home/nmewes/Y6CP_FA/socamm_false_miscompare.py'

# BIOS Fix - targets MULTI_BANK_MULTI_DQ FAILCRAWLER (timing/speed related)
BIOS_FIX_FAILCRAWLERS = {'MULTI_BANK_MULTI_DQ'}

# HW + SOP Fix - targets Hang MSN_STATUS (Debris/Speed + HUNG2 retest)
HW_SOP_MSN_STATUS = {'Hang'}

# Cache directory
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')

# =============================================================================
# CACHE UTILITIES
# =============================================================================

def get_cache_path(cache_type, key):
    """Get cache file path for a given key."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    key_hash = hashlib.md5(key.encode()).hexdigest()[:12]
    return os.path.join(CACHE_DIR, '{}_{}.json'.format(cache_type, key_hash))


def load_cache(cache_type, key):
    """Load cached data if available and fresh (same day)."""
    cache_path = get_cache_path(cache_type, key)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
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


# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_fid_data(design_id, steps, workweeks):
    """Fetch FID-level data with MSN_STATUS and FAILCRAWLER breakdown."""
    design_id_str = ','.join([design_id] if isinstance(design_id, str) else design_id)
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
        print("Error fetching FID data: {}".format(e))
        return pd.DataFrame()


def fetch_module_counts(design_id, steps, workweeks):
    """Fetch module-level counts (MUFAIL) per MSN_STATUS."""
    design_id_str = ','.join([design_id] if isinstance(design_id, str) else design_id)
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
    except:
        return {}


def fetch_total_uin(design_id, steps, workweeks):
    """Fetch total UIN (FID count) for denominator."""
    design_id_str = ','.join([design_id] if isinstance(design_id, str) else design_id)
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
    except:
        return {}


def fetch_failed_msns(design_id, step, workweek):
    """Fetch failed MSNs with MSN_STATUS and FAILCRAWLER for recovery analysis."""
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
        df = df[~df['MSN_STATUS'].isin(EXCLUDE_MSN_STATUS)]

        if not df.empty:
            save_cache('msns', cache_key, df.to_dict('records'))
        return df
    except:
        return pd.DataFrame()


def fetch_recovery_uin(design_id, step, workweek):
    """Fetch total UIN for recovery DPM calculation."""
    cache_key = 'uin_{}_{}_WW{}'.format(design_id, step, workweek)
    cached = load_cache('uin', cache_key)
    if cached:
        return cached

    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        '-DESIGN_ID={}'.format(design_id),
        '-MOD_CUSTOM_TEST_FLOW-+HMB1_NPI_FLOW',
        '+fidag',
        '-mfg_workweek={}'.format(workweek),
        '-format=STEP',
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
        total_uin = df['UIN'].sum() if 'UIN' in df.columns else 0

        if total_uin > 0:
            save_cache('uin', cache_key, total_uin)
        return total_uin
    except:
        return 0


# =============================================================================
# RPx RECOVERY (FALSE MISCOMPARE DETECTION)
# =============================================================================

def get_slash_for_msn(msn, step):
    """Get SLASH for an MSN using mtsums."""
    cache_key = 'slash_{}_{}'.format(msn, step)
    cached = load_cache('slash', cache_key)
    if cached:
        return cached

    cmd = [
        '/u/dramsoft/bin/mtsums',
        msn,
        '-step={}'.format(step.lower()),
        '-format=summary'
    ]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if result.returncode != 0:
            return None
        output = result.stdout.decode()
        lines = output.strip().split('\n')

        for line in lines:
            line = line.strip()
            if '/' in line and not line.startswith('Returned') and line != 'SUMMARY':
                save_cache('slash', cache_key, line)
                return line
        return None
    except:
        return None


def check_false_miscompare(slash):
    """Check if SLASH indicates false miscompare using nmewes script."""
    if not slash:
        return {'is_false_fail': False, 'signature': None}

    cache_key = 'fm_{}'.format(slash)
    cached = load_cache('fm', cache_key)
    if cached is not None:
        return cached

    if not os.path.exists(FALSE_MISCOMPARE_SCRIPT):
        return {'is_false_fail': False, 'signature': None}

    cmd = ['python', FALSE_MISCOMPARE_SCRIPT, '--sums', slash]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        output = result.stdout.decode()

        is_false_fail = False
        signature = None

        for line in output.split('\n'):
            if 'False Fail?' in line and 'True' in line:
                is_false_fail = True
            if 'expected inverse actual' in line.lower():
                signature = 'expected_inverse_actual'

        result_dict = {'is_false_fail': is_false_fail, 'signature': signature}
        save_cache('fm', cache_key, result_dict)
        return result_dict
    except:
        return {'is_false_fail': False, 'signature': None}


# =============================================================================
# HYBRID DPM CALCULATION
# =============================================================================

def calculate_hybrid_dpm(df, step, workweek, total_uin, module_counts=None, workweeks=None):
    """
    Calculate DPM using Kevin's hybrid approach:
    - MODULE-level (Mod-Sys, Hang, Multi-Mod, Boot): MSNs / Total FIDs x 1M
    - FID-level (DQ, Row, SB_Int, etc.): UFAILs / Total FIDs x 1M

    Args:
        workweek: Single workweek (for single week analysis)
        workweeks: List of workweeks (for cumulative analysis, overrides workweek filter)
    """
    if df.empty or total_uin == 0:
        return pd.DataFrame()

    step_df = df[df['STEP'].str.upper() == step.upper()].copy()

    # Filter by workweek(s)
    if 'MFG_WORKWEEK' in step_df.columns:
        if workweeks:
            # Cumulative: filter by list of workweeks
            step_df = step_df[step_df['MFG_WORKWEEK'].isin([int(ww) for ww in workweeks])]
        else:
            # Single week
            step_df = step_df[step_df['MFG_WORKWEEK'] == int(workweek)]

    if step_df.empty:
        return pd.DataFrame()

    step_df = step_df[step_df['MSN_STATUS'] != 'Pass']

    results = []
    for msn_status in step_df['MSN_STATUS'].unique():
        is_module_level = msn_status in MODULE_LEVEL_FAILURES
        msn_df = step_df[step_df['MSN_STATUS'] == msn_status]

        if is_module_level:
            if module_counts:
                # Sum module counts across all workweeks
                if workweeks:
                    count = sum(module_counts.get((step.upper(), ww, msn_status), 0) for ww in workweeks)
                else:
                    key = (step.upper(), workweek, msn_status)
                    count = module_counts.get(key, 0)
            else:
                count = msn_df['UFAIL'].sum() if 'UFAIL' in msn_df.columns else 0
            dpm = (count / total_uin) * 1_000_000
            level = 'MODULE'
            count_label = '{} MSNs'.format(int(count))
        else:
            count = msn_df['UFAIL'].sum() if 'UFAIL' in msn_df.columns else 0
            dpm = (count / total_uin) * 1_000_000
            level = 'FID'
            count_label = '{} FIDs'.format(int(count))

        results.append({
            'MSN_STATUS': msn_status,
            'Level': level,
            'Count': count_label,
            'DPM': round(dpm, 2)
        })

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values('DPM', ascending=False)
        total_dpm = result_df['DPM'].sum()
        result_df['Percent'] = (result_df['DPM'] / total_dpm * 100).round(1)

    return result_df


# =============================================================================
# RECOVERY ANALYSIS
# =============================================================================

def analyze_recovery(design_id, step, workweek, verbose=True):
    """
    Analyze recovery potential for all three fix types:
    - RPx (verified via false_miscompare)
    - BIOS (projected - MULTI_BANK_MULTI_DQ)
    - HW+SOP (projected - Hang)
    """
    if verbose:
        print("  Fetching failed MSNs...")

    msn_df = fetch_failed_msns(design_id, step, workweek)
    if msn_df.empty:
        return None

    total_uin = fetch_recovery_uin(design_id, step, workweek)
    if total_uin == 0:
        return None

    unique_msns = msn_df['MSN'].unique()
    total_msns = len(unique_msns)

    # Separate HW+SOP target MSNs
    hang_df = msn_df[msn_df['MSN_STATUS'].isin(HW_SOP_MSN_STATUS)].copy()
    hang_msns = set(hang_df['MSN'].unique()) if not hang_df.empty else set()

    if verbose:
        print("  Found {} unique failed MSNs ({} Hang, {} other)".format(
            total_msns, len(hang_msns), total_msns - len(hang_msns)))
        print("  Analyzing recovery potential...")

    # Build mappings
    msn_failcrawlers = {}
    msn_statuses = {}
    for _, row in msn_df.iterrows():
        msn = row['MSN']
        if msn not in msn_failcrawlers:
            msn_failcrawlers[msn] = set()
        msn_failcrawlers[msn].add(row['FAILCRAWLER'])
        msn_statuses[msn] = row['MSN_STATUS']

    # Analyze each MSN
    msn_results = {}
    rpx_count = bios_count = hw_sop_count = 0

    for i, msn in enumerate(unique_msns):
        if verbose and (i + 1) % 20 == 0:
            print("    Processed {}/{}...".format(i + 1, total_msns))

        msn_status = msn_statuses.get(msn, '')
        is_hw_sop_target = msn_status in HW_SOP_MSN_STATUS

        if is_hw_sop_target:
            msn_results[msn] = {
                'msn_status': msn_status,
                'is_rpx_recoverable': False,
                'is_bios_recoverable': False,
                'is_hw_sop_recoverable': True,
                'failcrawlers': list(msn_failcrawlers.get(msn, set()))
            }
            hw_sop_count += 1
        else:
            slash = get_slash_for_msn(msn, step)
            fm_result = check_false_miscompare(slash)
            is_rpx = fm_result['is_false_fail']

            msn_fcs = msn_failcrawlers.get(msn, set())
            is_bios = bool(msn_fcs & BIOS_FIX_FAILCRAWLERS)

            msn_results[msn] = {
                'msn_status': msn_status,
                'is_rpx_recoverable': is_rpx,
                'is_bios_recoverable': is_bios,
                'is_hw_sop_recoverable': False,
                'failcrawlers': list(msn_fcs)
            }
            if is_rpx:
                rpx_count += 1
            if is_bios:
                bios_count += 1

    if verbose:
        print("  RPx: {}, BIOS: {}, HW+SOP: {}".format(rpx_count, bios_count, hw_sop_count))

    # Build breakdown by MSN_STATUS
    msn_status_breakdown = {}
    for _, row in msn_df.iterrows():
        msn = row['MSN']
        msn_status = row['MSN_STATUS']
        failcrawler = row['FAILCRAWLER']

        if msn_status not in msn_status_breakdown:
            msn_status_breakdown[msn_status] = {
                'total_msns': set(),
                'rpx_msns': set(),
                'bios_msns': set(),
                'hw_sop_msns': set(),
                'failcrawlers': {}
            }

        msn_status_breakdown[msn_status]['total_msns'].add(msn)

        mr = msn_results.get(msn, {})
        if mr.get('is_rpx_recoverable'):
            msn_status_breakdown[msn_status]['rpx_msns'].add(msn)
        if mr.get('is_bios_recoverable'):
            msn_status_breakdown[msn_status]['bios_msns'].add(msn)
        if mr.get('is_hw_sop_recoverable'):
            msn_status_breakdown[msn_status]['hw_sop_msns'].add(msn)

        if failcrawler not in msn_status_breakdown[msn_status]['failcrawlers']:
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler] = {
                'total': set(), 'rpx': set(), 'bios': set(), 'hw_sop': set()
            }
        msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['total'].add(msn)
        if mr.get('is_rpx_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['rpx'].add(msn)
        if mr.get('is_bios_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['bios'].add(msn)
        if mr.get('is_hw_sop_recoverable'):
            msn_status_breakdown[msn_status]['failcrawlers'][failcrawler]['hw_sop'].add(msn)

    # Convert to counts and DPM
    breakdown = []
    for msn_status, data in msn_status_breakdown.items():
        total = len(data['total_msns'])
        rpx = len(data['rpx_msns'])
        bios = len(data['bios_msns'])
        hw_sop = len(data['hw_sop_msns'])
        combined = len(data['rpx_msns'] | data['bios_msns'] | data['hw_sop_msns'])

        total_dpm = (total / total_uin) * 1_000_000
        rpx_dpm = (rpx / total_uin) * 1_000_000
        bios_dpm = (bios / total_uin) * 1_000_000
        hw_sop_dpm = (hw_sop / total_uin) * 1_000_000
        combined_dpm = (combined / total_uin) * 1_000_000

        fc_breakdown = []
        for fc, fc_data in data['failcrawlers'].items():
            fc_breakdown.append({
                'failcrawler': fc,
                'total': len(fc_data['total']),
                'rpx_recoverable': len(fc_data['rpx']),
                'bios_recoverable': len(fc_data['bios']),
                'hw_sop_recoverable': len(fc_data['hw_sop']),
                'combined_recoverable': len(fc_data['rpx'] | fc_data['bios'] | fc_data['hw_sop']),
                'is_bios_target': fc in BIOS_FIX_FAILCRAWLERS,
                'is_hw_sop_target': msn_status in HW_SOP_MSN_STATUS
            })

        breakdown.append({
            'msn_status': msn_status,
            'total_msns': total,
            'total_dpm': round(total_dpm, 2),
            'rpx_recoverable_msns': rpx,
            'rpx_recoverable_dpm': round(rpx_dpm, 2),
            'bios_recoverable_msns': bios,
            'bios_recoverable_dpm': round(bios_dpm, 2),
            'hw_sop_recoverable_msns': hw_sop,
            'hw_sop_recoverable_dpm': round(hw_sop_dpm, 2),
            'combined_recoverable_msns': combined,
            'combined_recoverable_dpm': round(combined_dpm, 2),
            'combined_remaining_dpm': round(total_dpm - combined_dpm, 2),
            'failcrawlers': sorted(fc_breakdown, key=lambda x: x['total'], reverse=True)
        })

    breakdown = sorted(breakdown, key=lambda x: x['total_dpm'], reverse=True)

    # Calculate totals
    total_dpm = sum(b['total_dpm'] for b in breakdown)
    rpx_dpm = sum(b['rpx_recoverable_dpm'] for b in breakdown)
    bios_dpm = sum(b['bios_recoverable_dpm'] for b in breakdown)
    hw_sop_dpm = sum(b['hw_sop_recoverable_dpm'] for b in breakdown)
    combined_dpm = sum(b['combined_recoverable_dpm'] for b in breakdown)
    combined_msns = sum(b['combined_recoverable_msns'] for b in breakdown)

    return {
        'design_id': design_id,
        'step': step,
        'workweek': workweek,
        'total_uin': total_uin,
        'total_msns': total_msns,
        'total_dpm': round(total_dpm, 2),
        'rpx_recoverable_msns': rpx_count,
        'rpx_recoverable_dpm': round(rpx_dpm, 2),
        'rpx_recovery_rate': round(rpx_count / total_msns * 100, 1) if total_msns > 0 else 0,
        'bios_recoverable_msns': bios_count,
        'bios_recoverable_dpm': round(bios_dpm, 2),
        'bios_recovery_rate': round(bios_count / total_msns * 100, 1) if total_msns > 0 else 0,
        'hw_sop_recoverable_msns': hw_sop_count,
        'hw_sop_recoverable_dpm': round(hw_sop_dpm, 2),
        'hw_sop_recovery_rate': round(hw_sop_count / total_msns * 100, 1) if total_msns > 0 else 0,
        'combined_recoverable_msns': combined_msns,
        'combined_recoverable_dpm': round(combined_dpm, 2),
        'combined_remaining_dpm': round(total_dpm - combined_dpm, 2),
        'combined_recovery_rate': round(combined_msns / total_msns * 100, 1) if total_msns > 0 else 0,
        'breakdown': breakdown
    }


# =============================================================================
# CORRELATION HEATMAP
# =============================================================================

def create_correlation_heatmap(df, step, workweek, total_uin, module_counts=None, workweeks=None):
    """Create MSN_STATUS x FAILCRAWLER correlation heatmap.

    Args:
        workweek: Single workweek (for single week analysis)
        workweeks: List of workweeks (for cumulative analysis, overrides workweek filter)
    """
    if not PLOTLY_AVAILABLE or df.empty or total_uin == 0:
        return None, None

    step_df = df[df['STEP'].str.upper() == step.upper()].copy()

    # Filter by workweek(s)
    if 'MFG_WORKWEEK' in step_df.columns:
        if workweeks:
            # Cumulative: filter by list of workweeks
            step_df = step_df[step_df['MFG_WORKWEEK'].isin([int(ww) for ww in workweeks])]
        else:
            # Single week
            step_df = step_df[step_df['MFG_WORKWEEK'] == int(workweek)]

    if step_df.empty:
        return None, None

    step_df = step_df[step_df['MSN_STATUS'] != 'Pass']

    # Calculate DPM for each MSN_STATUS x FAILCRAWLER combination
    dpm_data = []
    for msn_status in step_df['MSN_STATUS'].unique():
        is_module = msn_status in MODULE_LEVEL_FAILURES
        msn_df = step_df[step_df['MSN_STATUS'] == msn_status]

        if is_module and module_counts:
            # Sum module counts across all workweeks
            if workweeks:
                total_count = sum(module_counts.get((step.upper(), ww, msn_status), 0) for ww in workweeks)
            else:
                total_count = module_counts.get((step.upper(), workweek, msn_status), 0)
            total_ufail = msn_df['UFAIL'].sum() if 'UFAIL' in msn_df.columns else 1

            for fc in msn_df['FAILCRAWLER'].unique():
                fc_df = msn_df[msn_df['FAILCRAWLER'] == fc]
                fc_ufail = fc_df['UFAIL'].sum() if 'UFAIL' in fc_df.columns else 0
                weight = fc_ufail / total_ufail if total_ufail > 0 else 0
                dpm = (total_count * weight / total_uin) * 1_000_000
                dpm_data.append({'MSN_STATUS': msn_status, 'FAILCRAWLER': fc, 'DPM': dpm})
        else:
            for fc in msn_df['FAILCRAWLER'].unique():
                fc_df = msn_df[msn_df['FAILCRAWLER'] == fc]
                ufail = fc_df['UFAIL'].sum() if 'UFAIL' in fc_df.columns else 0
                dpm = (ufail / total_uin) * 1_000_000
                dpm_data.append({'MSN_STATUS': msn_status, 'FAILCRAWLER': fc, 'DPM': dpm})

    dpm_df = pd.DataFrame(dpm_data)
    if dpm_df.empty:
        return None, None

    # Create pivot table
    pivot_df = dpm_df.pivot_table(index='MSN_STATUS', columns='FAILCRAWLER', values='DPM', fill_value=0)

    # Sort by total DPM
    row_totals = pivot_df.sum(axis=1).sort_values(ascending=False)
    col_totals = pivot_df.sum(axis=0).sort_values(ascending=False)
    pivot_df = pivot_df.loc[row_totals.index, col_totals.index[:10]]  # Top 10 FAILCRAWLERs

    # Calculate percentage contribution per row (MSN_STATUS)
    row_totals_aligned = pivot_df.sum(axis=1)
    pct_df = pivot_df.div(row_totals_aligned, axis=0) * 100

    # Add level indicators
    level_map = {row: '[M]' if row in MODULE_LEVEL_FAILURES else '[F]' for row in pivot_df.index}
    new_index = ["{} {}".format(row, level_map[row]) for row in pivot_df.index]
    pivot_df.index = new_index
    pct_df.index = new_index

    # Create text annotations with percentage only
    text_annotations = []
    for i, row in enumerate(pivot_df.values):
        row_text = []
        for j, v in enumerate(row):
            if v > 0:
                pct = pct_df.values[i][j]
                row_text.append('{:.0f}%'.format(pct))
            else:
                row_text.append('')
        text_annotations.append(row_text)

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values.tolist(),
        x=list(pivot_df.columns),
        y=list(pivot_df.index),
        colorscale=[[0, '#fff5f0'], [0.2, '#fee0d2'], [0.4, '#fcbba1'],
                    [0.6, '#fc9272'], [0.8, '#de2d26'], [1.0, '#a50f15']],
        hovertemplate='<b>%{y}</b><br>FAILCRAWLER: %{x}<br>DPM: %{z:.2f}<br>Contribution: %{text}<extra></extra>',
        text=text_annotations,
        colorbar=dict(title=dict(text='DPM', side='right'), thickness=12, len=0.8)
    ))

    # Add text annotations manually for each cell
    annotations = []
    for i, y_val in enumerate(pivot_df.index):
        for j, x_val in enumerate(pivot_df.columns):
            text = text_annotations[i][j]
            if text:  # Only add annotation if there's text
                annotations.append(dict(
                    x=x_val,
                    y=y_val,
                    text=text,
                    showarrow=False,
                    font=dict(size=9, color='black', family='Arial'),
                    align='center'
                ))

    fig.update_layout(
        title=dict(text='MSN_STATUS x FAILCRAWLER', x=0.5, font=dict(size=10)),
        xaxis=dict(title='', tickangle=60, tickfont=dict(size=7), side='bottom'),
        yaxis=dict(title='', tickfont=dict(size=8), autorange='reversed'),
        height=400, width=550,
        margin=dict(l=100, r=50, t=40, b=150),
        paper_bgcolor='white', plot_bgcolor='white',
        annotations=annotations
    )

    return fig, pivot_df


# =============================================================================
# HTML REPORT GENERATION
# =============================================================================

def generate_html_report(results, output_file):
    """Generate comprehensive HTML report."""
    html = ['<!DOCTYPE html><html><head>']
    html.append('<meta charset="UTF-8">')
    html.append('<title>cDPM Recovery Simulation Report</title>')
    html.append('<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>')
    html.append('<style>')
    html.append('body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }')
    html.append('.container { max-width: 1400px; margin: 0 auto; }')
    html.append('.header { background: linear-gradient(135deg, #1a237e, #283593); color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }')
    html.append('.section { background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }')
    html.append('.grid-2 { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; align-items: start; }')
    html.append('.grid-2-compact { display: grid; grid-template-columns: minmax(280px, 1fr) minmax(400px, 1.5fr); gap: 20px; align-items: start; }')
    html.append('.grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }')
    html.append('.metric-card { padding: 10px; border-radius: 6px; text-align: center; }')
    html.append('.metric-value { font-size: 24px; font-weight: bold; }')
    html.append('.metric-label { font-size: 11px; color: #666; }')
    html.append('.rpx { background: #e8f5e9; border-left: 4px solid #4caf50; }')
    html.append('.bios { background: #e3f2fd; border-left: 4px solid #1976d2; }')
    html.append('.hwsop { background: #fce4ec; border-left: 4px solid #c2185b; }')
    html.append('.combined { background: #fff3e0; border-left: 4px solid #f57c00; }')
    html.append('.badge { font-size: 8px; padding: 2px 6px; border-radius: 3px; color: white; }')
    html.append('.badge-verified { background: #4caf50; }')
    html.append('.badge-projected { background: #ff9800; }')
    html.append('table { width: 100%; border-collapse: collapse; font-size: 11px; }')
    html.append('th, td { padding: 6px; text-align: center; border-bottom: 1px solid #eee; }')
    html.append('th { background: #424242; color: white; }')
    html.append('.text-left { text-align: left; }')
    html.append('.text-green { color: #4caf50; font-weight: bold; }')
    html.append('.text-blue { color: #1976d2; font-weight: bold; }')
    html.append('.text-pink { color: #c2185b; font-weight: bold; }')
    html.append('.text-orange { color: #f57c00; font-weight: bold; }')
    html.append('</style></head><body>')
    html.append('<div class="container">')

    # Header
    html.append('<div class="header">')
    html.append('<h1 style="margin:0;">cDPM Recovery Simulation Report</h1>')
    html.append('<p style="margin:5px 0 0 0;opacity:0.9;">SOCAMM2 Hybrid DPM Analysis with Recovery Projections</p>')
    html.append('<p style="margin:5px 0 0 0;font-size:12px;">Generated: {}</p>'.format(datetime.now().strftime('%Y-%m-%d %H:%M')))
    html.append('</div>')

    # Process each design ID
    for design_id, design_data in results.items():
        html.append('<div class="section">')
        html.append('<h2 style="margin-top:0;color:#1a237e;">{}</h2>'.format(design_id))

        for step, step_data in design_data.items():
            html.append('<div style="margin-bottom:20px;padding:15px;background:#fafafa;border-radius:6px;">')
            html.append('<h3 style="margin-top:0;">{}</h3>'.format(step))

            # Single Week and 4-Week sections
            for period in ['single_week', 'four_week']:
                period_data = step_data.get(period, {})
                if not period_data:
                    continue

                period_label = period_data.get('label', period)
                html.append('<div style="margin-bottom:15px;">')
                html.append('<h4 style="margin:0 0 10px 0;color:#555;">{}</h4>'.format(period_label))

                # Side-by-side layout for table and heatmap
                dpm_summary = period_data.get('dpm_summary')
                heatmap_html = period_data.get('heatmap_html')

                if dpm_summary is not None and not dpm_summary.empty:
                    total_uin = period_data.get('total_uin', 0)

                    # Start grid container for side-by-side layout
                    html.append('<div class="grid-2-compact">')

                    # Left side: DPM Summary table
                    html.append('<div>')
                    html.append('<p style="font-size:11px;color:#666;margin:0 0 8px 0;">Total UIN: {:,} FIDs</p>'.format(total_uin))
                    html.append('<table style="font-size:10px;">')
                    html.append('<tr><th class="text-left">MSN_STATUS</th><th>Level</th><th>Count</th><th>DPM</th><th>%</th></tr>')
                    for _, row in dpm_summary.iterrows():
                        html.append('<tr><td class="text-left">{}</td><td>{}</td><td>{}</td><td>{:.2f}</td><td>{:.1f}%</td></tr>'.format(
                            row['MSN_STATUS'], row['Level'], row['Count'], row['DPM'], row.get('Percent', 0)))
                    total_dpm = dpm_summary['DPM'].sum()
                    html.append('<tr style="font-weight:bold;background:#f5f5f5;"><td class="text-left">TOTAL</td><td></td><td></td><td>{:.2f}</td><td>100%</td></tr>'.format(total_dpm))
                    html.append('</table>')
                    html.append('</div>')

                    # Right side: Heatmap
                    if heatmap_html:
                        html.append('<div style="display:flex;justify-content:center;align-items:flex-start;">')
                        html.append(heatmap_html)
                        html.append('</div>')

                    html.append('</div>')  # End grid container

                html.append('</div>')

            # Recovery Simulation
            recovery = step_data.get('recovery')
            if recovery:
                html.append('<div style="margin-top:15px;padding:15px;background:#f0f0f0;border-radius:6px;">')
                html.append('<h4 style="margin:0 0 15px 0;">Recovery Simulation (WW{})</h4>'.format(recovery['workweek']))

                # Current DPM
                html.append('<div style="text-align:center;margin-bottom:15px;padding:10px;background:#ffebee;border-radius:6px;">')
                html.append('<span style="color:#666;">Current DPM:</span> ')
                html.append('<span style="font-size:28px;font-weight:bold;color:#c62828;">{:.1f}</span>'.format(recovery['total_dpm']))
                html.append('</div>')

                # Four recovery cards
                html.append('<div class="grid-4">')

                # RPx
                html.append('<div class="metric-card rpx">')
                html.append('<div style="display:flex;justify-content:space-between;margin-bottom:5px;">')
                html.append('<span style="font-weight:bold;color:#2e7d32;font-size:11px;">New RPx Fix</span>')
                html.append('<span class="badge badge-verified">VERIFIED</span>')
                html.append('</div>')
                html.append('<div style="font-size:9px;color:#666;margin-bottom:8px;">False Miscompare</div>')
                html.append('<div class="metric-value text-green">{:.1f}</div>'.format(recovery['rpx_recoverable_dpm']))
                html.append('<div class="metric-label">DPM Recoverable ({:.0f}%)</div>'.format(recovery['rpx_recovery_rate']))
                html.append('</div>')

                # BIOS
                html.append('<div class="metric-card bios">')
                html.append('<div style="display:flex;justify-content:space-between;margin-bottom:5px;">')
                html.append('<span style="font-weight:bold;color:#1565c0;font-size:11px;">New BIOS Fix</span>')
                html.append('<span class="badge badge-projected">PROJECTED</span>')
                html.append('</div>')
                html.append('<div style="font-size:9px;color:#666;margin-bottom:8px;">MULTI_BANK_MULTI_DQ - CCE</div>')
                html.append('<div class="metric-value text-blue">{:.1f}</div>'.format(recovery['bios_recoverable_dpm']))
                html.append('<div class="metric-label">DPM Recoverable ({:.0f}%)</div>'.format(recovery['bios_recovery_rate']))
                html.append('</div>')

                # HW+SOP
                html.append('<div class="metric-card hwsop">')
                html.append('<div style="display:flex;justify-content:space-between;margin-bottom:5px;">')
                html.append('<span style="font-weight:bold;color:#c2185b;font-size:11px;">HW + SOP Fix</span>')
                html.append('<span class="badge badge-projected">PROJECTED</span>')
                html.append('</div>')
                html.append('<div style="font-size:9px;color:#666;margin-bottom:8px;">Hang - Debris/HUNG2</div>')
                html.append('<div class="metric-value text-pink">{:.1f}</div>'.format(recovery['hw_sop_recoverable_dpm']))
                html.append('<div class="metric-label">DPM Recoverable ({:.0f}%)</div>'.format(recovery['hw_sop_recovery_rate']))
                html.append('</div>')

                # Combined
                html.append('<div class="metric-card combined">')
                html.append('<div style="display:flex;justify-content:space-between;margin-bottom:5px;">')
                html.append('<span style="font-weight:bold;color:#e65100;font-size:11px;">All Fixes</span>')
                html.append('<span class="badge" style="background:#9e9e9e;">COMBINED</span>')
                html.append('</div>')
                html.append('<div style="font-size:9px;color:#666;margin-bottom:8px;">RPx + BIOS + HW/SOP</div>')
                html.append('<div class="metric-value text-orange">{:.1f}</div>'.format(recovery['combined_recoverable_dpm']))
                html.append('<div class="metric-label">DPM Recoverable ({:.0f}%)</div>'.format(recovery['combined_recovery_rate']))
                html.append('</div>')

                html.append('</div>')  # grid-4

                # After fix summary
                html.append('<div style="text-align:center;margin-top:15px;padding:10px;background:#e8f5e9;border-radius:6px;">')
                html.append('<span style="color:#666;">After All Fixes:</span> ')
                html.append('<span style="font-size:24px;font-weight:bold;color:#2e7d32;">{:.1f}</span>'.format(recovery['combined_remaining_dpm']))
                html.append('<span style="color:#666;"> DPM</span>')
                html.append('</div>')

                # Breakdown table
                html.append('<details style="margin-top:15px;"><summary style="cursor:pointer;font-weight:bold;">MSN_STATUS Breakdown</summary>')
                html.append('<table style="margin-top:10px;">')
                html.append('<tr><th class="text-left">MSN_STATUS</th><th>MSNs</th><th>DPM</th>')
                html.append('<th style="background:#4caf50;">RPx</th><th style="background:#1976d2;">BIOS</th>')
                html.append('<th style="background:#c2185b;">HW+SOP</th><th style="background:#f57c00;">All</th><th>After</th></tr>')
                for b in recovery['breakdown']:
                    html.append('<tr><td class="text-left">{}</td><td>{}</td><td>{:.1f}</td>'.format(
                        b['msn_status'], b['total_msns'], b['total_dpm']))
                    html.append('<td class="text-green">{:.1f}</td>'.format(b['rpx_recoverable_dpm']))
                    html.append('<td class="text-blue">{:.1f}</td>'.format(b['bios_recoverable_dpm']))
                    html.append('<td class="text-pink">{:.1f}</td>'.format(b['hw_sop_recoverable_dpm']))
                    html.append('<td class="text-orange">{:.1f}</td>'.format(b['combined_recoverable_dpm']))
                    html.append('<td>{:.1f}</td></tr>'.format(b['combined_remaining_dpm']))
                html.append('</table></details>')

                # FAILCRAWLER details
                html.append('<details style="margin-top:10px;"><summary style="cursor:pointer;font-weight:bold;">FAILCRAWLER Details</summary>')
                html.append('<table style="margin-top:10px;">')
                html.append('<tr><th class="text-left">FAILCRAWLER</th><th>Tot</th>')
                html.append('<th style="background:#4caf50;">RPx</th><th style="background:#1976d2;">BIOS</th>')
                html.append('<th style="background:#c2185b;">HW+SOP</th><th style="background:#f57c00;">Comb</th><th>Fix Type</th></tr>')
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
                            fix_str = '+'.join(fix_types) if fix_types else '-'
                            html.append('<tr><td class="text-left">{}</td><td>{}</td>'.format(fc['failcrawler'], fc['total']))
                            html.append('<td class="text-green">{}</td>'.format(fc['rpx_recoverable']))
                            html.append('<td class="text-blue">{}</td>'.format(fc['bios_recoverable']))
                            html.append('<td class="text-pink">{}</td>'.format(fc.get('hw_sop_recoverable', 0)))
                            html.append('<td class="text-orange">{}</td>'.format(fc['combined_recoverable']))
                            html.append('<td style="font-weight:bold;">{}</td></tr>'.format(fix_str))
                html.append('</table></details>')

                html.append('</div>')  # Recovery section

            html.append('</div>')  # Step section

        html.append('</div>')  # Design section

    html.append('</div></body></html>')

    with open(output_file, 'w') as f:
        f.write('\n'.join(html))

    return output_file


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def get_workweek_range(requested_ww):
    """Get 4-week range ending at requested workweek."""
    year = int(str(requested_ww)[:4])
    week = int(str(requested_ww)[4:])

    weeks = []
    for i in range(4):
        w = week - i
        y = year
        if w <= 0:
            y -= 1
            w += 52
        weeks.append('{}{:02d}'.format(y, w))

    return list(reversed(weeks))


def main():
    parser = argparse.ArgumentParser(
        description='SOCAMM2 cDPM Recovery Simulation Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python cdpm_recovery_sim.py --did Y6CP --steps HMB1,QMON --ww 202615
  python cdpm_recovery_sim.py --did Y6CP,Y63N --steps HMB1 --ww 202615
  python cdpm_recovery_sim.py --did Y6CP --steps HMB1,QMON --ww 202615 --output report.html

Recovery Types:
  - New RPx Fix (VERIFIED): False miscompare recovery
  - New BIOS Fix (PROJECTED): MULTI_BANK_MULTI_DQ timing - CCE validation
  - HW + SOP Fix (PROJECTED): Hang recovery - Debris/HUNG2 retest
        '''
    )
    parser.add_argument('--did', required=True, help='Design ID(s), comma-separated (e.g., Y6CP,Y63N)')
    parser.add_argument('--steps', required=True, help='Test step(s), comma-separated (e.g., HMB1,QMON)')
    parser.add_argument('--ww', required=True, help='Work week in YYYYWW format (e.g., 202615)')
    parser.add_argument('--output', help='Output HTML file path (default: cdpm_recovery_WW{ww}.html)')
    parser.add_argument('--browser', action='store_true', help='Open browser after generating report')

    args = parser.parse_args()

    design_ids = [d.strip().upper() for d in args.did.split(',')]
    steps = [s.strip().upper() for s in args.steps.split(',')]
    requested_ww = args.ww
    output_file = args.output or 'cdpm_recovery_WW{}.html'.format(requested_ww)

    print('='*70)
    print('SOCAMM2 cDPM Recovery Simulation')
    print('='*70)
    print('Design IDs: {}'.format(', '.join(design_ids)))
    print('Steps: {}'.format(', '.join(steps)))
    print('Work Week: {}'.format(requested_ww))
    print('='*70)

    # Get 4-week range
    workweeks = get_workweek_range(requested_ww)
    print('4-Week Range: {} to {}'.format(workweeks[0], workweeks[-1]))

    results = {}

    for design_id in design_ids:
        print('\n' + '*'*70)
        print('Processing: {}'.format(design_id))
        print('*'*70)

        results[design_id] = {}

        # Fetch data
        print('\nFetching data...')
        fid_df = fetch_fid_data(design_id, steps, workweeks)
        module_counts = fetch_module_counts(design_id, steps, workweeks)
        uin_map = fetch_total_uin(design_id, steps, workweeks)

        for step in steps:
            print('\n--- {} {} ---'.format(design_id, step))
            results[design_id][step] = {}

            # Single week analysis
            single_ww = requested_ww
            single_uin = uin_map.get((step.upper(), single_ww), 0)

            if single_uin > 0:
                print('\nSingle Week (WW{}): {:,} FIDs'.format(single_ww, single_uin))

                dpm_summary = calculate_hybrid_dpm(fid_df, step, single_ww, single_uin, module_counts)
                heatmap_fig, _ = create_correlation_heatmap(fid_df, step, single_ww, single_uin, module_counts)

                results[design_id][step]['single_week'] = {
                    'label': 'Single Week: WW{}'.format(single_ww),
                    'workweek': single_ww,
                    'total_uin': single_uin,
                    'dpm_summary': dpm_summary,
                    'heatmap_html': heatmap_fig.to_html(include_plotlyjs=False, full_html=False) if heatmap_fig else None
                }

                if not dpm_summary.empty:
                    print('\n{:<15} {:>10} {:>12} {:>10}'.format('MSN_STATUS', 'Level', 'DPM', '%'))
                    print('-'*50)
                    for _, row in dpm_summary.iterrows():
                        print('{:<15} {:>10} {:>12.2f} {:>9.1f}%'.format(
                            row['MSN_STATUS'][:15], row['Level'], row['DPM'], row.get('Percent', 0)))
                    print('-'*50)
                    print('{:<15} {:>10} {:>12.2f}'.format('TOTAL', '', dpm_summary['DPM'].sum()))

            # 4-week cumulative
            cumulative_uin = sum(uin_map.get((step.upper(), ww), 0) for ww in workweeks)

            if cumulative_uin > 0:
                print('\n4-Week Cumulative (WW{} to WW{}): {:,} FIDs'.format(workweeks[0], workweeks[-1], cumulative_uin))

                # Combine data for all weeks
                combined_df = fid_df[fid_df['MFG_WORKWEEK'].isin([int(ww) for ww in workweeks])]
                combined_df = combined_df[combined_df['STEP'].str.upper() == step.upper()]

                # Calculate cumulative DPM
                if not combined_df.empty:
                    dpm_summary_4w = calculate_hybrid_dpm(combined_df, step, workweeks[-1], cumulative_uin, module_counts, workweeks=workweeks)
                    heatmap_fig_4w, _ = create_correlation_heatmap(combined_df, step, workweeks[-1], cumulative_uin, module_counts, workweeks=workweeks)

                    results[design_id][step]['four_week'] = {
                        'label': '4-Week Cumulative: WW{} to WW{}'.format(workweeks[0], workweeks[-1]),
                        'workweeks': workweeks,
                        'total_uin': cumulative_uin,
                        'dpm_summary': dpm_summary_4w,
                        'heatmap_html': heatmap_fig_4w.to_html(include_plotlyjs=False, full_html=False) if heatmap_fig_4w else None
                    }

            # Recovery analysis (single week)
            print('\nRecovery Analysis (WW{})...'.format(requested_ww))
            recovery = analyze_recovery(design_id, step, requested_ww, verbose=True)

            if recovery:
                results[design_id][step]['recovery'] = recovery

                print('\n' + '='*50)
                print('RECOVERY SUMMARY')
                print('='*50)
                print('Total DPM: {:.2f}'.format(recovery['total_dpm']))
                print()
                print('{:<15} {:>12} {:>10}'.format('Fix Type', 'Recoverable', 'Rate'))
                print('-'*40)
                print('{:<15} {:>12.2f} {:>9.0f}%'.format('New RPx', recovery['rpx_recoverable_dpm'], recovery['rpx_recovery_rate']))
                print('{:<15} {:>12.2f} {:>9.0f}%'.format('New BIOS', recovery['bios_recoverable_dpm'], recovery['bios_recovery_rate']))
                print('{:<15} {:>12.2f} {:>9.0f}%'.format('HW + SOP', recovery['hw_sop_recoverable_dpm'], recovery['hw_sop_recovery_rate']))
                print('-'*40)
                print('{:<15} {:>12.2f} {:>9.0f}%'.format('COMBINED', recovery['combined_recoverable_dpm'], recovery['combined_recovery_rate']))
                print()
                print('After All Fixes: {:.2f} DPM'.format(recovery['combined_remaining_dpm']))

    # Generate HTML report
    print('\n' + '='*70)
    print('Generating HTML report...')
    generate_html_report(results, output_file)
    print('Report saved: {}'.format(os.path.abspath(output_file)))
    print('='*70)

    # Open in browser
    if args.browser:
        try:
            import webbrowser
            webbrowser.open('file://' + os.path.abspath(output_file))
        except:
            pass

    return results


if __name__ == '__main__':
    main()
