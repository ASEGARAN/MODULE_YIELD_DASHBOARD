"""
Correlation Sanity Check Module
Validates RCA conclusions from MSN_STATUS × FAILCRAWLER analysis.

This module provides a validation layer that confirms the RCA conclusion,
not re-derives it. It shows evidence per dimension with consistency indicators.

Key Principle: "This tab exists to validate the RCA conclusion, not to re‑derive it."
"""

import subprocess
import pandas as pd
import logging
from io import StringIO
from typing import Optional

logger = logging.getLogger(__name__)

# =============================================================================
# MSN_STATUS × FAILCRAWLER Debug Flows
# =============================================================================
# Maps each MSN_STATUS × FAILCRAWLER combination to expected debug dimensions

DEBUG_FLOWS = {
    # HANG failures - always check machine first (HW+SOP expected)
    ('Hang', 'HANG'): {
        'rca_type': 'HW+SOP',
        'expected_recovery': 100,
        'primary_checks': ['MACHINE_ID', 'TESTER', 'SLOT_ID'],
        'secondary_checks': ['TEST_FACILITY'],
        'description': 'Hardware/SOP issue - check machine clustering',
        'validation_criteria': {
            'machine_clustering': 'Strong signal if >50% on single machine',
            'retest_pattern': 'Should improve after MOBO rotation per SOP',
        }
    },

    # Mod-Sys with MULTI_BANK_MULTI_DQ - BIOS recoverable
    ('Mod-Sys', 'MULTI_BANK_MULTI_DQ'): {
        'rca_type': 'BIOS',
        'expected_recovery': 100,
        'primary_checks': ['TEST_VERSION', 'FLOW'],
        'secondary_checks': ['MACHINE_ID', 'TESTER'],
        'description': 'System-level, BIOS-recoverable - check test config',
        'validation_criteria': {
            'config_correlation': 'Check if specific test version shows spike',
            'dq_spread': 'Multi-bank + multi-DQ = wide distribution expected',
        }
    },

    # DQ failures with single DQ pattern - could be BIOS or DRAM
    ('DQ', 'SINGLE_BURST_SINGLE_ROW'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['FABLOT', 'ULOC'],
        'secondary_checks': ['MACHINE_ID', 'PCB_SUPPLIER'],
        'description': 'Single-bit DQ issue - check die/fablot clustering',
        'validation_criteria': {
            'address_stability': 'Same DQ line across retests = true defect',
            'fablot_clustering': '>3 fails from same fablot = silicon issue',
        }
    },

    # DQ with system-even-burst pattern
    ('DQ', 'SYS_EVEN_BURST_BIT'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['MACHINE_ID', 'TEST_VERSION'],
        'secondary_checks': ['FABLOT', 'ULOC'],
        'description': 'Even-burst DQ pattern - may be test artifact',
        'validation_criteria': {
            'machine_correlation': 'Single machine = likely test issue',
            'pattern_consistency': 'Even bits across modules = system signature',
        }
    },

    # Multi-DQ - typically BIOS recoverable
    ('Multi-DQ', 'MULTI_BANK_MULTI_DQ'): {
        'rca_type': 'BIOS',
        'expected_recovery': 100,
        'primary_checks': ['TEST_VERSION', 'MACHINE_ID'],
        'secondary_checks': ['FLOW'],
        'description': 'Multi-DQ multi-bank - classic BIOS issue',
        'validation_criteria': {
            'bank_distribution': 'Spread across banks = system-level',
            'dq_distribution': 'Random DQ pattern = not single-bit defect',
        }
    },

    # Row failures - typically DRAM defect, not recoverable
    ('Row', 'SINGLE_BURST_SINGLE_ROW'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'ULOC', 'DRAMFAIL'],
        'secondary_checks': ['PCB_SUPPLIER', 'ASSEMBLY_FACILITY'],
        'description': 'Row defect - check fablot clustering for DRAM issue',
        'validation_criteria': {
            'dramfail_flag': 'DRAMFAIL=YES confirms DRAM defect',
            'fablot_clustering': 'Multiple fails same fablot = fab issue',
            'row_consistency': 'Same row address = true defect',
        }
    },

    # Row with DB (Double Bit) - DRAM defect
    ('Row', 'DB'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'ULOC', 'DRAMFAIL'],
        'secondary_checks': [],
        'description': 'Double-bit row failure - DRAM defect confirmed',
        'validation_criteria': {
            'dramfail_flag': 'DRAMFAIL=YES expected',
            'bit_proximity': 'Adjacent bits in same row = cell defect',
        }
    },

    # SB_Int - Single Bit Intermittent, DRAM issue
    ('SB_Int', 'SB'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'ULOC'],
        'secondary_checks': ['RETEST_COUNT'],
        'description': 'Intermittent single-bit - marginal DRAM',
        'validation_criteria': {
            'address_stability': 'Same bit across retests = true defect',
            'retest_pattern': 'Intermittent appearance = marginal cell',
        }
    },

    # Boot failures - not recoverable
    ('Boot', 'BOOT'): {
        'rca_type': 'NO_FIX',
        'expected_recovery': 0,
        'primary_checks': ['MACHINE_ID', 'ASSEMBLY_FACILITY'],
        'secondary_checks': ['PCB_SUPPLIER'],
        'description': 'Boot failure - check connectivity/assembly',
        'validation_criteria': {
            'machine_correlation': 'Single machine could be MOBO issue',
            'assembly_correlation': 'Supplier clustering = assembly issue',
        }
    },
}

# Default flow for unknown combinations
DEFAULT_DEBUG_FLOW = {
    'rca_type': 'UNCLASSIFIED',
    'expected_recovery': 0,
    'primary_checks': ['MACHINE_ID', 'FABLOT', 'TEST_FACILITY'],
    'secondary_checks': ['ASSEMBLY_FACILITY', 'PCB_SUPPLIER'],
    'description': 'Unknown pattern - run full correlation check',
    'validation_criteria': {}
}


# =============================================================================
# Sanity Check Dimensions
# =============================================================================

SANITY_CHECK_DIMENSIONS = {
    'MACHINE_ID': {
        'category': 'Equipment',
        'icon': '🔧',
        'description': 'Test machine correlation',
        'signal_threshold': 50,  # >50% on single machine = strong signal
    },
    'TESTER': {
        'category': 'Equipment',
        'icon': '🔧',
        'description': 'Tester correlation',
        'signal_threshold': 50,
    },
    'TEST_FACILITY': {
        'category': 'Location',
        'icon': '🏭',
        'description': 'Test facility correlation',
        'signal_threshold': 80,  # Most tests at one facility is normal
    },
    'ASSEMBLY_FACILITY': {
        'category': 'Location',
        'icon': '🏭',
        'description': 'Assembly facility correlation',
        'signal_threshold': 60,
    },
    'PCB_SUPPLIER': {
        'category': 'Materials',
        'icon': '📦',
        'description': 'PCB supplier correlation',
        'signal_threshold': 60,
    },
    'REGISTER_SUPPLIER': {
        'category': 'Materials',
        'icon': '📦',
        'description': 'Register supplier correlation',
        'signal_threshold': 60,
    },
    'FABLOT': {
        'category': 'Silicon',
        'icon': '💎',
        'description': 'Fab lot clustering (DRAM source)',
        'signal_threshold': 30,  # >30% from single fablot = strong signal
    },
    'ULOC': {
        'category': 'Silicon',
        'icon': '📍',
        'description': 'Unit location (die position)',
        'signal_threshold': 40,
    },
}


def get_debug_flow(msn_status: str, failcrawler: str) -> dict:
    """
    Get the debug flow for a MSN_STATUS × FAILCRAWLER combination.

    Args:
        msn_status: The MSN_STATUS value
        failcrawler: The FAILCRAWLER category

    Returns:
        Debug flow dictionary with RCA type and check dimensions
    """
    key = (msn_status, failcrawler)
    return DEBUG_FLOWS.get(key, DEFAULT_DEBUG_FLOW)


def fetch_sanity_check_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str],
    densities: list[str] = None,
    speeds: list[str] = None
) -> pd.DataFrame:
    """
    Fetch failure data with all sanity check dimensions.

    Includes both environment and silicon dimensions for comprehensive validation.

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks
        densities: Optional list of densities
        speeds: Optional list of speeds

    Returns:
        DataFrame with failure data and sanity check dimensions
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    # Comprehensive query with all dimensions using +fidag for FID-level data
    # Note: +fidag gives raw MSN/FID-level data, unlike +modfm which pivots by MSN_STATUS
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        f'-DESIGN_ID={design_id_str}',
        f'-mfg_workweek={workweek_str}',
        # Core fields
        '-format=MSN,FID,DESIGN_ID,STEP,MFG_WORKWEEK,FAILCRAWLER,MSN_STATUS',
        # Equipment dimensions
        '-format+=MACHINE_ID,TESTER',
        # Location dimensions
        '-format+=TEST_FACILITY,ASSEMBLY_FACILITY',
        # Materials dimensions
        '-format+=PCB_SUPPLIER,REGISTER_SUPPLIER',
        # Test config dimensions
        '-format+=FLOW,TEST_VERSION',
        # DRAM dimensions
        '-format+=ULOC,DRAMFAIL',
        # Address dimensions (for stability analysis)
        '-format+=ROWCNT,COLCNT,DQCNT',
        f'-step={step_str}',
        '-msn_status!=Pass',
    ]

    # Add optional filters
    # Note: mtsums uses -module_density and -module_speed (not -density/-speed)
    if densities:
        cmd.append(f'-module_density={",".join(densities)}')
    if speeds:
        cmd.append(f'-module_speed={",".join(speeds)}')

    # Use +fidag for FID-level aggregation (raw data per MSN/FID)
    cmd.append('+fidag')

    logger.info(f"Fetching sanity check data...")

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

        # Extract FABLOT from FID if available
        if 'FID' in df.columns:
            df['FABLOT'] = df['FID'].apply(lambda x: str(x).split(':')[0] if pd.notna(x) else '')

        logger.info(f"Fetched {len(df)} sanity check records")
        return df

    except Exception as e:
        logger.exception(f"Error fetching sanity check data: {e}")
        return pd.DataFrame()


def analyze_dimension(
    df: pd.DataFrame,
    dimension: str,
    total_fails: int
) -> dict:
    """
    Analyze a single dimension for concentration and signal strength.

    Args:
        df: DataFrame with failure data
        dimension: Column name to analyze
        total_fails: Total number of failures for percentage calculation

    Returns:
        Dictionary with dimension analysis results
    """
    if dimension not in df.columns or df[dimension].isna().all():
        return None

    # Count by dimension value
    counts = df[dimension].value_counts()

    if counts.empty:
        return None

    # Get top value and its concentration
    top_value = counts.index[0]
    top_count = counts.iloc[0]
    top_pct = (top_count / total_fails * 100) if total_fails > 0 else 0

    # Get dimension metadata
    dim_info = SANITY_CHECK_DIMENSIONS.get(dimension, {
        'category': 'Other',
        'icon': '📊',
        'signal_threshold': 50,
    })

    # Determine signal strength
    threshold = dim_info.get('signal_threshold', 50)
    if top_pct >= threshold:
        signal = 'STRONG'
        signal_color = '#27AE60'  # Green
    elif top_pct >= threshold * 0.7:
        signal = 'MODERATE'
        signal_color = '#F39C12'  # Orange
    else:
        signal = 'WEAK'
        signal_color = '#9E9E9E'  # Grey

    return {
        'dimension': dimension,
        'category': dim_info.get('category', 'Other'),
        'icon': dim_info.get('icon', '📊'),
        'top_value': str(top_value),
        'top_count': int(top_count),
        'top_pct': round(top_pct, 1),
        'total_values': len(counts),
        'signal': signal,
        'signal_color': signal_color,
        'threshold': threshold,
        'distribution': counts.head(5).to_dict(),
    }


def analyze_address_stability(df: pd.DataFrame) -> dict:
    """
    Analyze ROW/DQ address stability across failures.

    Stable addresses (same ROW/DQ across modules) suggest true defect.
    Varying addresses suggest test artifact or system issue.

    Args:
        df: DataFrame with failure data including ROWCNT, COLCNT, DQCNT

    Returns:
        Dictionary with address stability analysis
    """
    result = {
        'row_stability': None,
        'dq_stability': None,
        'conclusion': 'INSUFFICIENT_DATA',
    }

    if df.empty:
        return result

    # ROW stability
    if 'ROWCNT' in df.columns and not df['ROWCNT'].isna().all():
        row_counts = df['ROWCNT'].value_counts()
        if len(row_counts) > 0:
            dominant_row = row_counts.iloc[0]
            row_stability = dominant_row / len(df) * 100
            result['row_stability'] = {
                'dominant_value': int(row_counts.index[0]),
                'concentration': round(row_stability, 1),
                'is_stable': row_stability >= 70,
                'unique_values': len(row_counts),
            }

    # DQ stability
    if 'DQCNT' in df.columns and not df['DQCNT'].isna().all():
        dq_counts = df['DQCNT'].value_counts()
        if len(dq_counts) > 0:
            dominant_dq = dq_counts.iloc[0]
            dq_stability = dominant_dq / len(df) * 100
            result['dq_stability'] = {
                'dominant_value': int(dq_counts.index[0]),
                'concentration': round(dq_stability, 1),
                'is_stable': dq_stability >= 70,
                'unique_values': len(dq_counts),
            }

    # Determine overall conclusion
    row_stable = result['row_stability'] and result['row_stability'].get('is_stable', False)
    dq_stable = result['dq_stability'] and result['dq_stability'].get('is_stable', False)

    if row_stable and dq_stable:
        result['conclusion'] = 'STABLE_ADDRESS'
        result['interpretation'] = 'Same fail addresses across modules - likely true silicon defect'
    elif row_stable or dq_stable:
        result['conclusion'] = 'PARTIAL_STABILITY'
        result['interpretation'] = 'Partial address consistency - may be combination of issues'
    else:
        result['conclusion'] = 'RANDOM_ADDRESS'
        result['interpretation'] = 'Random fail addresses - likely system/test issue, not silicon'

    return result


def run_sanity_check(
    df: pd.DataFrame,
    msn_status: str,
    failcrawler: str,
    step: str
) -> dict:
    """
    Run sanity check validation for a specific MSN_STATUS × FAILCRAWLER combination.

    Args:
        df: DataFrame with failure data
        msn_status: The MSN_STATUS value
        failcrawler: The FAILCRAWLER category
        step: Test step

    Returns:
        Dictionary with sanity check results and confidence assessment
    """
    # Get debug flow for this combination
    debug_flow = get_debug_flow(msn_status, failcrawler)

    # Filter data
    filtered_df = df.copy()
    if msn_status and 'MSN_STATUS' in df.columns:
        filtered_df = filtered_df[filtered_df['MSN_STATUS'] == msn_status]
    if failcrawler and 'FAILCRAWLER' in df.columns:
        filtered_df = filtered_df[filtered_df['FAILCRAWLER'] == failcrawler]

    if filtered_df.empty:
        return {
            'status': 'NO_DATA',
            'msn_status': msn_status,
            'failcrawler': failcrawler,
            'debug_flow': debug_flow,
        }

    total_fails = len(filtered_df)

    # Analyze primary dimensions
    primary_results = []
    for dim in debug_flow['primary_checks']:
        analysis = analyze_dimension(filtered_df, dim, total_fails)
        if analysis:
            primary_results.append(analysis)

    # Analyze secondary dimensions
    secondary_results = []
    for dim in debug_flow['secondary_checks']:
        analysis = analyze_dimension(filtered_df, dim, total_fails)
        if analysis:
            secondary_results.append(analysis)

    # Analyze address stability for DRAM-type issues
    address_stability = None
    if debug_flow['rca_type'] in ('DRAM', 'BIOS_PARTIAL'):
        address_stability = analyze_address_stability(filtered_df)

    # Calculate confidence score
    confidence = calculate_confidence(
        primary_results, secondary_results, address_stability, debug_flow
    )

    return {
        'status': 'OK',
        'msn_status': msn_status,
        'failcrawler': failcrawler,
        'step': step,
        'total_fails': total_fails,
        'debug_flow': debug_flow,
        'primary_results': primary_results,
        'secondary_results': secondary_results,
        'address_stability': address_stability,
        'confidence': confidence,
    }


def calculate_confidence(
    primary_results: list,
    secondary_results: list,
    address_stability: dict,
    debug_flow: dict
) -> dict:
    """
    Calculate confidence score for the RCA conclusion.

    Args:
        primary_results: Analysis results for primary dimensions
        secondary_results: Analysis results for secondary dimensions
        address_stability: Address stability analysis
        debug_flow: The expected debug flow

    Returns:
        Dictionary with confidence assessment
    """
    score = 0
    max_score = 0
    evidence = []

    # Primary dimensions (weighted 2x)
    for result in primary_results:
        max_score += 2
        if result['signal'] == 'STRONG':
            score += 2
            evidence.append(f"✅ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")
        elif result['signal'] == 'MODERATE':
            score += 1
            evidence.append(f"⚠️ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")
        else:
            evidence.append(f"❌ {result['dimension']}: No clear signal")

    # Secondary dimensions (weighted 1x)
    for result in secondary_results:
        max_score += 1
        if result['signal'] == 'STRONG':
            score += 1
            evidence.append(f"✅ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")
        elif result['signal'] == 'MODERATE':
            score += 0.5
            evidence.append(f"⚠️ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")

    # Address stability for DRAM issues
    if address_stability and debug_flow['rca_type'] in ('DRAM', 'BIOS_PARTIAL'):
        max_score += 2
        if address_stability['conclusion'] == 'STABLE_ADDRESS':
            score += 2
            evidence.append(f"✅ Address stable - confirms silicon defect")
        elif address_stability['conclusion'] == 'RANDOM_ADDRESS':
            evidence.append(f"❌ Address random - may not be silicon defect")
        else:
            score += 1
            evidence.append(f"⚠️ Address partially stable")

    # Calculate percentage
    confidence_pct = (score / max_score * 100) if max_score > 0 else 0

    # Determine verdict
    if confidence_pct >= 70:
        verdict = 'HIGH_CONFIDENCE'
        verdict_text = f"High confidence in {debug_flow['rca_type']} classification"
        verdict_color = '#27AE60'
    elif confidence_pct >= 40:
        verdict = 'MODERATE_CONFIDENCE'
        verdict_text = f"Moderate confidence - additional investigation may help"
        verdict_color = '#F39C12'
    else:
        verdict = 'LOW_CONFIDENCE'
        verdict_text = f"Low confidence - RCA classification may need review"
        verdict_color = '#E74C3C'

    return {
        'score': round(score, 1),
        'max_score': max_score,
        'percentage': round(confidence_pct, 1),
        'verdict': verdict,
        'verdict_text': verdict_text,
        'verdict_color': verdict_color,
        'evidence': evidence,
    }


def create_sanity_check_summary_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML summary for sanity check results.

    Args:
        result: Sanity check result dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for the summary
    """
    if result['status'] == 'NO_DATA':
        return '<div style="padding: 1rem; color: #999;">No data for this combination</div>'

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    debug_flow = result['debug_flow']
    confidence = result['confidence']

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
            <div>
                <h3 style="margin: 0; color: {text_color};">
                    {result['msn_status']} × {result['failcrawler']}
                </h3>
                <p style="margin: 0.25rem 0 0 0; color: #888; font-size: 0.9rem;">
                    {debug_flow['description']}
                </p>
            </div>
            <div style="text-align: right;">
                <div style="background: {confidence['verdict_color']}; color: white; padding: 0.5rem 1rem; border-radius: 4px; font-weight: bold;">
                    {confidence['percentage']:.0f}% Confidence
                </div>
                <div style="font-size: 0.8rem; color: #888; margin-top: 0.25rem;">
                    Expected: {debug_flow['rca_type']} ({debug_flow['expected_recovery']}% recovery)
                </div>
            </div>
        </div>

        <div style="margin-bottom: 1rem;">
            <strong style="color: {text_color};">Evidence:</strong>
            <ul style="margin: 0.5rem 0; padding-left: 1.5rem; color: {text_color};">
    '''

    for evidence_item in confidence['evidence']:
        html += f'<li style="margin: 0.25rem 0;">{evidence_item}</li>'

    html += f'''
            </ul>
        </div>

        <div style="background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; padding: 0.75rem; border-radius: 4px;">
            <strong style="color: {text_color};">Verdict:</strong>
            <span style="color: {confidence['verdict_color']}; margin-left: 0.5rem;">
                {confidence['verdict_text']}
            </span>
        </div>
    </div>
    '''

    return html


def create_dimension_table_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML table showing all dimension correlations.

    Args:
        result: Sanity check result dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for the dimension table
    """
    if result['status'] == 'NO_DATA':
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    header_bg = '#2D2D2D' if dark_mode else '#4472C4'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    html = f'''
    <table style="width: 100%; border-collapse: collapse; font-size: 0.9rem; margin-top: 1rem;">
        <thead>
            <tr style="background: {header_bg}; color: white;">
                <th style="padding: 0.5rem; text-align: left;">Priority</th>
                <th style="padding: 0.5rem; text-align: left;">Dimension</th>
                <th style="padding: 0.5rem; text-align: left;">Category</th>
                <th style="padding: 0.5rem; text-align: left;">Top Value</th>
                <th style="padding: 0.5rem; text-align: right;">Count</th>
                <th style="padding: 0.5rem; text-align: right;">%</th>
                <th style="padding: 0.5rem; text-align: center;">Signal</th>
            </tr>
        </thead>
        <tbody>
    '''

    def add_row(analysis, priority, row_bg):
        signal_badge = {
            'STRONG': f'<span style="background: #27AE60; color: white; padding: 2px 8px; border-radius: 4px;">STRONG</span>',
            'MODERATE': f'<span style="background: #F39C12; color: white; padding: 2px 8px; border-radius: 4px;">MODERATE</span>',
            'WEAK': f'<span style="background: #9E9E9E; color: white; padding: 2px 8px; border-radius: 4px;">WEAK</span>',
        }
        return f'''
            <tr style="background: {row_bg}; border-bottom: 1px solid {border_color};">
                <td style="padding: 0.5rem; color: {text_color};">{priority}</td>
                <td style="padding: 0.5rem; color: {text_color};">{analysis['icon']} {analysis['dimension']}</td>
                <td style="padding: 0.5rem; color: {text_color};">{analysis['category']}</td>
                <td style="padding: 0.5rem; color: {text_color}; font-family: monospace;">{analysis['top_value']}</td>
                <td style="padding: 0.5rem; text-align: right; color: {text_color};">{analysis['top_count']}</td>
                <td style="padding: 0.5rem; text-align: right; color: {text_color};">{analysis['top_pct']}%</td>
                <td style="padding: 0.5rem; text-align: center;">{signal_badge.get(analysis['signal'], '')}</td>
            </tr>
        '''

    # Primary dimensions
    for i, analysis in enumerate(result.get('primary_results', [])):
        row_bg = '#2D2D2D' if dark_mode and i % 2 else (bg_color if not dark_mode or i % 2 == 0 else '#252525')
        html += add_row(analysis, '🔴 Primary', row_bg)

    # Secondary dimensions
    for i, analysis in enumerate(result.get('secondary_results', [])):
        row_bg = '#2D2D2D' if dark_mode and i % 2 else (bg_color if not dark_mode or i % 2 == 0 else '#252525')
        html += add_row(analysis, '🟡 Secondary', row_bg)

    html += '''
        </tbody>
    </table>
    '''

    return html


def create_address_stability_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML for address stability analysis.

    Args:
        result: Sanity check result dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for address stability section
    """
    address = result.get('address_stability')
    if not address:
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    conclusion_colors = {
        'STABLE_ADDRESS': '#27AE60',
        'PARTIAL_STABILITY': '#F39C12',
        'RANDOM_ADDRESS': '#E74C3C',
        'INSUFFICIENT_DATA': '#9E9E9E',
    }

    conclusion_color = conclusion_colors.get(address['conclusion'], '#9E9E9E')

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-top: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">📍 Address Stability Analysis</h4>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
    '''

    # ROW stability
    row_data = address.get('row_stability')
    if row_data:
        row_color = '#27AE60' if row_data['is_stable'] else '#E74C3C'
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px;">
                <div style="font-weight: bold; color: {text_color};">ROW Count Stability</div>
                <div style="font-size: 1.5rem; color: {row_color}; margin: 0.25rem 0;">
                    {row_data['concentration']:.1f}%
                </div>
                <div style="font-size: 0.8rem; color: #888;">
                    Dominant: {row_data['dominant_value']} ({row_data['unique_values']} unique values)
                </div>
            </div>
        '''

    # DQ stability
    dq_data = address.get('dq_stability')
    if dq_data:
        dq_color = '#27AE60' if dq_data['is_stable'] else '#E74C3C'
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px;">
                <div style="font-weight: bold; color: {text_color};">DQ Count Stability</div>
                <div style="font-size: 1.5rem; color: {dq_color}; margin: 0.25rem 0;">
                    {dq_data['concentration']:.1f}%
                </div>
                <div style="font-size: 0.8rem; color: #888;">
                    Dominant: {dq_data['dominant_value']} ({dq_data['unique_values']} unique values)
                </div>
            </div>
        '''

    html += f'''
        </div>

        <div style="margin-top: 1rem; padding: 0.5rem; background: {'#2D2D2D' if dark_mode else '#F0F0F0'}; border-left: 4px solid {conclusion_color}; border-radius: 0 4px 4px 0;">
            <strong style="color: {conclusion_color};">{address['conclusion'].replace('_', ' ')}</strong>
            <span style="color: {text_color}; margin-left: 0.5rem;">
                — {address.get('interpretation', '')}
            </span>
        </div>
    </div>
    '''

    return html


def create_debug_flow_html(debug_flow: dict, dark_mode: bool = False) -> str:
    """
    Create HTML showing the expected debug flow for the combination.

    Args:
        debug_flow: Debug flow dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for debug flow display
    """
    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    rca_colors = {
        'HW+SOP': '#3498DB',
        'BIOS': '#27AE60',
        'BIOS_PARTIAL': '#F39C12',
        'DRAM': '#E74C3C',
        'NO_FIX': '#7F8C8D',
        'UNCLASSIFIED': '#9E9E9E',
    }

    rca_color = rca_colors.get(debug_flow['rca_type'], '#9E9E9E')

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">🔍 Expected Debug Flow</h4>

        <div style="display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem;">
            <div style="padding: 0.5rem 1rem; background: {rca_color}; color: white; border-radius: 4px; font-weight: bold;">
                {debug_flow['rca_type']}
            </div>
            <div style="padding: 0.5rem 1rem; background: {'#2D2D2D' if dark_mode else '#F0F0F0'}; color: {text_color}; border-radius: 4px;">
                Expected Recovery: {debug_flow['expected_recovery']}%
            </div>
        </div>

        <div style="margin-bottom: 0.75rem;">
            <strong style="color: {text_color};">Primary Checks:</strong>
            <span style="color: #888; margin-left: 0.5rem;">
                {' → '.join(debug_flow['primary_checks'])}
            </span>
        </div>
    '''

    if debug_flow['secondary_checks']:
        html += f'''
        <div style="margin-bottom: 0.75rem;">
            <strong style="color: {text_color};">Secondary Checks:</strong>
            <span style="color: #888; margin-left: 0.5rem;">
                {' → '.join(debug_flow['secondary_checks'])}
            </span>
        </div>
        '''

    # Validation criteria
    if debug_flow.get('validation_criteria'):
        html += f'''
        <div style="margin-top: 1rem; padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px;">
            <strong style="color: {text_color};">Validation Criteria:</strong>
            <ul style="margin: 0.5rem 0 0 0; padding-left: 1.5rem; color: #888;">
        '''
        for key, value in debug_flow['validation_criteria'].items():
            html += f'<li style="margin: 0.25rem 0;"><strong>{key.replace("_", " ").title()}:</strong> {value}</li>'
        html += '''
            </ul>
        </div>
        '''

    html += '</div>'

    return html
