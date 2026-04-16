"""
AI Assistant Module for Module Yield Dashboard
Provides context-aware assistance and pattern analysis across tabs.
"""
import re
import pandas as pd
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# Query Parsing for FAILCRAWLER Drill-down
# =============================================================================

def parse_drilldown_query(query: str, available_fcs: list[str], available_statuses: list[str], available_steps: list[str]) -> Optional[dict]:
    """
    Parse natural language query to extract drill-down parameters.

    Examples:
        "show MSNs for NOFA NoFA at HMFN"
        "which modules have MULTI_BANK_MULTI_DQ failures"
        "drill down HANG DQ HMFN"
        "MSNs affected by TFF"

    Args:
        query: User's natural language query
        available_fcs: List of available FAILCRAWLER categories
        available_statuses: List of available MSN_STATUS values
        available_steps: List of available test steps

    Returns:
        Dict with 'failcrawler', 'msn_status', 'step' or None if not a drill-down query
    """
    query_upper = query.upper()
    query_lower = query.lower()

    # Check if this is a drill-down query
    drilldown_keywords = ['msn', 'module', 'drill', 'show', 'affected', 'which', 'list', 'find']
    if not any(kw in query_lower for kw in drilldown_keywords):
        return None

    result = {
        'failcrawler': None,
        'msn_status': None,
        'step': None
    }

    # Find FAILCRAWLER match (case-insensitive)
    for fc in available_fcs:
        if fc.upper() in query_upper or fc.upper().replace('_', ' ') in query_upper:
            result['failcrawler'] = fc
            break

    # Find MSN_STATUS match
    for status in available_statuses:
        if status.upper() in query_upper or status.upper().replace('-', ' ') in query_upper:
            result['msn_status'] = status
            break

    # Find step match
    for step in available_steps:
        if step.upper() in query_upper:
            result['step'] = step.upper()
            break

    # Only return if we found at least a FAILCRAWLER
    if result['failcrawler']:
        return result

    return None


def parse_pattern_analysis_query(query: str) -> bool:
    """
    Check if user is asking for pattern analysis.

    Examples:
        "analyze patterns"
        "what do these MSNs have in common"
        "find common factors"
        "why are these failing"
    """
    query_lower = query.lower()

    pattern_keywords = [
        'pattern', 'analyze', 'analysis', 'common', 'why', 'cause',
        'factor', 'correlat', 'trend', 'insight', 'explain', 'root cause'
    ]

    return any(kw in query_lower for kw in pattern_keywords)


# =============================================================================
# Pattern Analysis for MSN Data
# =============================================================================

def analyze_msn_patterns(drilldown_df: pd.DataFrame, failcrawler: str, msn_status: str = None) -> dict:
    """
    Analyze patterns in affected MSNs to find common factors.

    Args:
        drilldown_df: DataFrame with MSN-level drill-down data
        failcrawler: FAILCRAWLER category being analyzed
        msn_status: Optional MSN_STATUS filter

    Returns:
        Dict with pattern analysis results
    """
    if drilldown_df.empty:
        return {'error': 'No data to analyze'}

    analysis = {
        'total_msns': 0,
        'total_fid_fails': 0,
        'patterns': [],
        'summary': ''
    }

    # Get unique MSNs
    msn_col = 'MSN' if 'MSN' in drilldown_df.columns else None
    if not msn_col:
        return {'error': 'MSN column not found'}

    unique_msns = drilldown_df[msn_col].unique()
    analysis['total_msns'] = len(unique_msns)

    # Total FID fails
    if 'UFAIL' in drilldown_df.columns:
        analysis['total_fid_fails'] = int(pd.to_numeric(drilldown_df['UFAIL'], errors='coerce').sum())

    # Analyze FABLOT pattern (from SUMMARY column if available)
    if 'SUMMARY' in drilldown_df.columns:
        summaries = drilldown_df['SUMMARY'].dropna().unique()
        if len(summaries) > 0:
            # Extract FABLOT codes (format: FAB/LOT/...)
            fablots = []
            for s in summaries:
                parts = str(s).split('/')
                if len(parts) >= 2:
                    fablots.append(f"{parts[0]}/{parts[1]}")

            if fablots:
                from collections import Counter
                fablot_counts = Counter(fablots)
                top_fablot = fablot_counts.most_common(1)[0]
                if top_fablot[1] > 1:
                    pct = (top_fablot[1] / len(fablots)) * 100
                    analysis['patterns'].append({
                        'type': 'FABLOT',
                        'value': top_fablot[0],
                        'count': top_fablot[1],
                        'percentage': round(pct, 1),
                        'insight': f"{pct:.0f}% of failures from FABLOT {top_fablot[0]}"
                    })

    # Analyze workweek distribution
    if 'MFG_WORKWEEK' in drilldown_df.columns:
        ww_counts = drilldown_df.groupby('MFG_WORKWEEK')[msn_col].nunique()
        if len(ww_counts) > 1:
            peak_ww = ww_counts.idxmax()
            peak_count = ww_counts.max()
            total = ww_counts.sum()
            pct = (peak_count / total) * 100
            if pct > 50:
                analysis['patterns'].append({
                    'type': 'WORKWEEK',
                    'value': f"WW{int(peak_ww)}",
                    'count': int(peak_count),
                    'percentage': round(pct, 1),
                    'insight': f"{pct:.0f}% of affected modules from WW{int(peak_ww)}"
                })

    # Analyze MODFF (form factor) distribution
    if 'MODFF' in drilldown_df.columns:
        modff_counts = drilldown_df.groupby('MODFF')[msn_col].nunique()
        if len(modff_counts) > 0:
            top_modff = modff_counts.idxmax()
            top_count = modff_counts.max()
            total = modff_counts.sum()
            pct = (top_count / total) * 100
            analysis['patterns'].append({
                'type': 'FORM_FACTOR',
                'value': top_modff,
                'count': int(top_count),
                'percentage': round(pct, 1),
                'insight': f"{pct:.0f}% are {top_modff} modules"
            })

    # Analyze DISPO (disposition) if available
    if 'DISPO' in drilldown_df.columns:
        dispo_counts = drilldown_df.groupby('DISPO')[msn_col].nunique()
        if len(dispo_counts) > 0:
            for dispo, count in dispo_counts.items():
                total = dispo_counts.sum()
                pct = (count / total) * 100
                analysis['patterns'].append({
                    'type': 'DISPOSITION',
                    'value': dispo,
                    'count': int(count),
                    'percentage': round(pct, 1),
                    'insight': f"{pct:.0f}% are {dispo}"
                })

    # Generate summary
    filter_desc = failcrawler
    if msn_status:
        filter_desc += f" × {msn_status}"

    if analysis['patterns']:
        top_pattern = analysis['patterns'][0]
        analysis['summary'] = f"**{filter_desc}** affects {analysis['total_msns']} modules with {analysis['total_fid_fails']:,} FID fails. " \
                              f"Key pattern: {top_pattern['insight']}."
    else:
        analysis['summary'] = f"**{filter_desc}** affects {analysis['total_msns']} modules with {analysis['total_fid_fails']:,} FID fails. " \
                              f"No strong concentration pattern detected."

    return analysis


def create_pattern_analysis_html(analysis: dict, dark_mode: bool = False) -> str:
    """
    Create HTML display for pattern analysis results.
    """
    if 'error' in analysis:
        return f"<p style='color: #888;'>{analysis['error']}</p>"

    bg_color = '#1e3a2f' if dark_mode else '#f0fdf4'
    text_color = '#d1fae5' if dark_mode else '#166534'
    border_color = '#10b981'

    html = f'''
    <div style="background: {bg_color}; border-radius: 8px; padding: 15px; margin: 10px 0; border-left: 4px solid {border_color};">
        <div style="color: {text_color}; font-weight: bold; font-size: 14px; margin-bottom: 10px;">
            🔍 Pattern Analysis
        </div>
        <div style="color: {text_color}; font-size: 13px; margin-bottom: 12px;">
            {analysis['summary']}
        </div>
    '''

    if analysis['patterns']:
        html += f'<div style="display: flex; flex-wrap: wrap; gap: 10px;">'
        for pattern in analysis['patterns'][:4]:  # Show top 4 patterns
            html += f'''
            <div style="background: {'#064e3b' if dark_mode else '#dcfce7'}; border-radius: 6px; padding: 8px 12px; min-width: 120px;">
                <div style="color: #6ee7b7; font-size: 11px; text-transform: uppercase;">{pattern['type']}</div>
                <div style="color: {text_color}; font-weight: bold; font-size: 14px;">{pattern['value']}</div>
                <div style="color: {'#a7f3d0' if dark_mode else '#15803d'}; font-size: 11px;">{pattern['percentage']}% ({pattern['count']})</div>
            </div>
            '''
        html += '</div>'

    html += '</div>'
    return html


# =============================================================================
# AI Assistant Response Generation
# =============================================================================

def generate_assistant_response(
    query: str,
    context: dict,
    available_fcs: list[str] = None,
    available_statuses: list[str] = None,
    current_step: str = None
) -> dict:
    """
    Generate AI assistant response based on query and context.

    Args:
        query: User's query
        context: Current context (tab, data available, etc.)
        available_fcs: Available FAILCRAWLER categories
        available_statuses: Available MSN_STATUS values
        current_step: Current step being viewed

    Returns:
        Dict with 'type' (drilldown, analysis, help), 'response', and optional 'action'
    """
    available_fcs = available_fcs or []
    available_statuses = available_statuses or []
    available_steps = ['HMFN', 'HMB1', 'QMON', 'SLT']

    # Check for drill-down query
    drilldown_params = parse_drilldown_query(query, available_fcs, available_statuses, available_steps)
    if drilldown_params:
        # Use current step if not specified in query
        if not drilldown_params['step'] and current_step:
            drilldown_params['step'] = current_step

        return {
            'type': 'drilldown',
            'response': f"Fetching MSNs for **{drilldown_params['failcrawler']}**" +
                       (f" × **{drilldown_params['msn_status']}**" if drilldown_params['msn_status'] else "") +
                       (f" at **{drilldown_params['step']}**" if drilldown_params['step'] else "") + "...",
            'action': drilldown_params
        }

    # Check for pattern analysis query
    if parse_pattern_analysis_query(query):
        return {
            'type': 'analysis',
            'response': "I'll analyze the patterns in the current data...",
            'action': {'analyze': True}
        }

    # Default help response
    help_examples = [
        f"**Drill-down:** \"show MSNs for {available_fcs[0] if available_fcs else 'NOFA'}\"",
        "**Pattern analysis:** \"analyze patterns\" or \"what do these have in common\"",
        "**Filter by status:** \"MSNs with DQ status for HANG\""
    ]

    return {
        'type': 'help',
        'response': "I can help you with:\n\n" + "\n".join(f"• {ex}" for ex in help_examples),
        'action': None
    }


def get_ai_suggestion_for_heatmap(correlation_data: dict, step: str) -> Optional[str]:
    """
    Generate AI suggestion based on heatmap data.

    Args:
        correlation_data: Processed correlation data from heatmap
        step: Test step

    Returns:
        Suggestion string or None
    """
    if not correlation_data or 'matrix' not in correlation_data:
        return None

    matrix = correlation_data['matrix']
    if matrix.empty:
        return None

    # Find the highest cDPM cell
    max_val = 0
    max_fc = None
    max_status = None

    for fc in matrix.index:
        for status in matrix.columns:
            val = matrix.loc[fc, status]
            if pd.notna(val) and val > max_val:
                max_val = val
                max_fc = fc
                max_status = status

    if max_fc and max_val > 10:  # Only suggest if cDPM > 10
        return f"🔍 High cDPM detected: **{max_fc}** × **{max_status}** = {max_val:.1f}. " \
               f"Ask me to \"show MSNs for {max_fc} {max_status}\" to investigate."

    return None
