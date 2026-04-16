"""SMT6 Machine Yield Trend Data Fetching and Processing."""

import hashlib
import importlib
import json
import logging
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go

# Force reload fiscal_calendar to pick up latest changes
from src import fiscal_calendar
importlib.reload(fiscal_calendar)
from src.fiscal_calendar import get_fiscal_month

logger = logging.getLogger(__name__)

# Target yield for SMT6 machines
TARGET_YIELD = 99.0

# ============================================================================
# SMT6 CACHE - 24-hour file-based cache for SMT6 queries
# ============================================================================

SMT6_CACHE_DIR = Path.home() / ".cache" / "module_yield_dashboard" / "smt6"
SMT6_CACHE_TTL = 86400  # 24 hours


def _ensure_cache_dir() -> None:
    """Create cache directory if it doesn't exist."""
    SMT6_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _get_cache_key(cmd: str) -> str:
    """Generate a unique cache key from command string."""
    return hashlib.sha256(cmd.encode()).hexdigest()[:16]


def _get_cache_file(cache_key: str) -> Path:
    """Get the cache file path for a key."""
    return SMT6_CACHE_DIR / f"{cache_key}.json"


def _get_cached_result(cmd: str) -> Optional[str]:
    """Get cached result if available and not expired.

    Args:
        cmd: The frpt command string

    Returns:
        Cached stdout if valid, None otherwise
    """
    _ensure_cache_dir()
    cache_key = _get_cache_key(cmd)
    cache_file = _get_cache_file(cache_key)

    if not cache_file.exists():
        return None

    try:
        with open(cache_file, "r") as f:
            data = json.load(f)

        # Check if expired
        if (time.time() - data.get("timestamp", 0)) > SMT6_CACHE_TTL:
            logger.info(f"SMT6 cache expired for key {cache_key}")
            cache_file.unlink()
            return None

        logger.info(f"SMT6 cache HIT for key {cache_key}")
        return data.get("stdout", "")

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.warning(f"Invalid SMT6 cache entry: {e}")
        try:
            cache_file.unlink()
        except Exception:
            pass
        return None


def _set_cached_result(cmd: str, stdout: str) -> None:
    """Store a result in cache.

    Args:
        cmd: The frpt command string
        stdout: Command output to cache
    """
    _ensure_cache_dir()
    cache_key = _get_cache_key(cmd)
    cache_file = _get_cache_file(cache_key)

    try:
        with open(cache_file, "w") as f:
            json.dump({
                "stdout": stdout,
                "timestamp": time.time(),
                "command_hash": cache_key
            }, f)
        logger.info(f"SMT6 cached result for key {cache_key}")
    except IOError as e:
        logger.warning(f"Failed to write SMT6 cache: {e}")


def clear_smt6_cache() -> int:
    """Clear all SMT6 cached entries.

    Returns:
        Number of entries cleared
    """
    _ensure_cache_dir()
    count = 0
    for cache_file in SMT6_CACHE_DIR.glob("*.json"):
        try:
            cache_file.unlink()
            count += 1
        except IOError:
            pass
    logger.info(f"Cleared {count} SMT6 cache entries")
    return count


def get_smt6_cache_stats() -> dict:
    """Get SMT6 cache statistics.

    Returns:
        Dictionary with cache stats
    """
    _ensure_cache_dir()
    total = 0
    valid = 0
    expired = 0
    total_size = 0

    for cache_file in SMT6_CACHE_DIR.glob("*.json"):
        total += 1
        total_size += cache_file.stat().st_size
        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
            if (time.time() - data.get("timestamp", 0)) > SMT6_CACHE_TTL:
                expired += 1
            else:
                valid += 1
        except (json.JSONDecodeError, KeyError, TypeError, IOError):
            expired += 1

    return {
        "total_entries": total,
        "valid_entries": valid,
        "expired_entries": expired,
        "total_size_kb": total_size / 1024,
        "cache_dir": str(SMT6_CACHE_DIR),
        "ttl_hours": SMT6_CACHE_TTL / 3600,
    }

# SMT6 machine colors for consistent visualization
SMT6_COLORS = {
    'smt61-0001': '#2E86AB',  # Blue
    'smt61-0002': '#A23B72',  # Magenta
    'smt61-0007': '#F18F01',  # Orange
    'smt61-0003': '#C73E1D',  # Red
    'smt61-0004': '#8AC926',  # Green
    'smt61-0005': '#9B5DE5',  # Purple
    'smt61-0006': '#00F5D4',  # Cyan
}

# Target yield line
TARGET_YIELD = 99.0


def build_smt6_command(
    design_id: str,
    workweek: str,
    form_factor: str = "socamm2",
    facility: str = "PENANG",
    step: str = "hmfn",
    density: Optional[str] = None,
    speed: Optional[str] = None
) -> str:
    """
    Build the frpt command for SMT6 yield data.

    Args:
        design_id: Design ID (e.g., Y63N, Y6CP, Y62P)
        workweek: Manufacturing workweek (e.g., 202610)
        form_factor: Module form factor (default: socamm2)
        facility: Test facility (default: PENANG)
        step: Test step (default: hmfn)
        density: Module density filter (e.g., 192GB) - optional
        speed: Module speed filter (e.g., 7500MTPS) - optional

    Returns:
        frpt command string
    """
    cmd = (
        f"/u/pe_burn_dft/bin/frptx -xf -bin=soft "
        f"-myquick=/MFG_WORKWEEK/ "
        f"-quick=/myquick,machine_id/ "
        f"-sort=// "
        f"+regwidth +module "
        f"-dbase={design_id.lower()} "
        f"-step={step} "
        f"-all -n -r +imesh "
        f"-nonshippable=N/A -eng_summary=N/A "
        f"-standard_flow=yes "
        f"-machine_id=~SMT6 "
        f"+# "
        f"-module_form_factor={form_factor} "
        f"-mfg_workweek={workweek} "
        f"-test_facility={facility} "
    )
    # Add optional density filter
    if density:
        cmd += f"-module_density={density} "
    # Add optional speed filter (convert MTPS to MT format for frpt)
    if speed:
        # frpt uses "7500MT" format, not "7500MTPS"
        speed_frpt = speed.replace("MTPS", "MT") if speed.endswith("MTPS") else speed
        cmd += f"-module_speed={speed_frpt} "
    cmd += "+%"
    return cmd


def parse_smt6_output(output: str, design_id: str) -> list[dict]:
    """
    Parse frpt output to extract SMT6 machine yield data.

    Args:
        output: Raw frpt command output
        design_id: Design ID for this data

    Returns:
        List of dictionaries with machine yield data
    """
    results = []

    # Find data lines (format: workweek_machine_id    UIN    UPASS    UIN    UPASS    YIELD)
    # Example: 202610_smt61-0001, 202612_smt61e-0004
    # Pattern handles: smt61-0001, smt61e-0004, etc.
    pattern = r'^(\d+)_(smt\d+[a-z]*-\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)'

    for line in output.split('\n'):
        match = re.match(pattern, line.strip(), re.IGNORECASE)
        if match:
            workweek = match.group(1)
            machine_id = match.group(2).lower()
            uin_raw = int(match.group(3))
            upass_raw = int(match.group(4))
            uin_adj = int(match.group(5))
            upass_adj = int(match.group(6))
            yield_pct = float(match.group(7))

            results.append({
                'workweek': int(workweek),
                'machine_id': machine_id,
                'design_id': design_id.upper(),
                'uin_raw': uin_raw,
                'upass_raw': upass_raw,
                'uin_adj': uin_adj,
                'upass_adj': upass_adj,
                'yield_pct': yield_pct
            })

    return results


def fetch_smt6_yield_single(
    design_id: str,
    workweek: str,
    form_factor: str = "socamm2",
    timeout: int = 120,
    density: Optional[str] = None,
    speed: Optional[str] = None
) -> list[dict]:
    """
    Fetch SMT6 yield data for a single workweek and design ID.

    Args:
        design_id: Design ID
        workweek: Manufacturing workweek
        form_factor: Module form factor
        timeout: Command timeout in seconds
        density: Module density filter (optional)
        speed: Module speed filter (optional)

    Returns:
        List of machine yield data dictionaries
    """
    cmd = build_smt6_command(design_id, workweek, form_factor, density=density, speed=speed)

    # Check cache first
    cached_output = _get_cached_result(cmd)
    if cached_output is not None:
        return parse_smt6_output(cached_output, design_id)

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode == 0:
            # Cache successful result
            _set_cached_result(cmd, result.stdout)
            return parse_smt6_output(result.stdout, design_id)
        else:
            logger.warning(f"SMT6 fetch failed for {design_id}/WW{workweek}: {result.stderr}")
            return []

    except subprocess.TimeoutExpired:
        logger.error(f"SMT6 fetch timed out for {design_id}/WW{workweek}")
        return []
    except Exception as e:
        logger.error(f"SMT6 fetch error for {design_id}/WW{workweek}: {e}")
        return []


def fetch_smt6_yield_data(
    design_ids: list[str],
    workweeks: list[str],
    form_factor: str = "socamm2",
    max_workers: int = 8,
    progress_callback: Optional[callable] = None,
    density: Optional[str] = None,
    speed: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch SMT6 yield data for multiple workweeks and design IDs in parallel.

    Args:
        design_ids: List of design IDs
        workweeks: List of workweeks
        form_factor: Module form factor
        max_workers: Maximum parallel workers
        progress_callback: Optional callback(completed, total, msg) for progress updates
        density: Module density filter (optional)
        speed: Module speed filter (optional)

    Returns:
        DataFrame with SMT6 yield data
    """
    all_results = []
    cache_hits = 0
    cache_misses = 0

    # Create list of tasks
    tasks = [
        (did, ww, form_factor, density, speed)
        for did in design_ids
        for ww in workweeks
    ]

    total_tasks = len(tasks)
    logger.info(f"Fetching SMT6 data for {total_tasks} combinations (density={density}, speed={speed})...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_smt6_yield_single, did, ww, ff, density=dens, speed=spd): (did, ww)
            for did, ww, ff, dens, spd in tasks
        }

        completed = 0
        for future in as_completed(futures):
            did, ww = futures[future]
            completed += 1
            try:
                results = future.result()
                all_results.extend(results)
                # Count cache hits by checking if data was returned quickly
                if results:
                    logger.debug(f"Got {len(results)} records for {did}/WW{ww}")
            except Exception as e:
                logger.error(f"Failed to fetch SMT6 data for {did}/WW{ww}: {e}")

            if progress_callback:
                progress_callback(completed, total_tasks, f"Processed {did}/WW{ww}")

    logger.info(f"SMT6 fetch complete: {len(all_results)} total records from {total_tasks} queries")

    if all_results:
        df = pd.DataFrame(all_results)
        df = df.sort_values(['workweek', 'machine_id'])
        return df

    return pd.DataFrame()


def create_smt6_yield_chart(
    df: pd.DataFrame,
    design_id: str = None,
    dark_mode: bool = True,
    show_data_labels: bool = False,
    y_axis_min: float = None
) -> Optional[go.Figure]:
    """
    Create SMT6 yield trend chart with lines per machine.

    Args:
        df: DataFrame with SMT6 yield data
        design_id: Optional filter for specific design ID
        dark_mode: If True, use dark mode colors

    Returns:
        Plotly Figure object
    """
    if df.empty:
        return None

    # Filter by design_id if specified (can be comma-separated list)
    if design_id:
        # Handle comma-separated design IDs (e.g., "Y6CP, Y63N")
        design_ids = [d.strip().upper() for d in design_id.split(',')]
        df = df[df['design_id'].isin(design_ids)]

    if df.empty:
        return None

    # Get unique workweeks and create fiscal labels
    workweeks = sorted(df['workweek'].unique())

    fiscal_labels = []
    prev_month = None
    for ww in workweeks:
        month = get_fiscal_month(str(ww))
        if month != prev_month:
            fiscal_labels.append(f"{ww}<br> <br><b>{month}</b>")
            prev_month = month
        else:
            fiscal_labels.append(str(ww))

    # Create workweek to label mapping
    ww_to_label = dict(zip(workweeks, fiscal_labels))

    # Use colors that work in BOTH light and dark modes
    # Since Streamlit theme can change, we use high-contrast colors
    text_color = '#1a1a1a'  # Dark text works on light bg, visible on dark with plot_bg
    grid_color = 'rgba(128,128,128,0.3)'  # Neutral gray grid
    paper_bg = 'rgba(0,0,0,0)'  # Transparent paper
    plot_bg = 'rgba(248,249,250,0.95)'  # Light plot background for contrast
    target_color = '#dc3545'  # Bootstrap red

    # Hover label colors - dark text on light background for readability
    hover_bg = 'rgba(255,255,255,0.98)'
    hover_font_color = '#1a1a1a'
    hover_border_color = '#6c757d'

    # Legend styling - light background with border for visibility in both modes
    legend_bg = 'rgba(255,255,255,0.9)'
    legend_border = '#dee2e6'

    fig = go.Figure()

    # Add target line FIRST to establish x-axis categories in correct order
    fig.add_trace(
        go.Scatter(
            x=fiscal_labels,
            y=[TARGET_YIELD] * len(fiscal_labels),
            name=f'Target ({TARGET_YIELD}%)',
            mode='lines',
            line=dict(color=target_color, width=2, dash='dash'),
            hoverinfo='skip'
        )
    )

    # Add line for each machine
    machines = sorted(df['machine_id'].unique())

    for machine in machines:
        machine_df = df[df['machine_id'] == machine].sort_values('workweek')

        # Convert to native Python types to avoid numpy serialization issues
        x_labels = [ww_to_label.get(int(ww), str(ww)) for ww in machine_df['workweek']]
        y_values = [float(y) for y in machine_df['yield_pct']]

        color = SMT6_COLORS.get(machine, '#888888')

        # Determine mode based on show_data_labels
        trace_mode = 'lines+markers+text' if show_data_labels else 'lines+markers'

        fig.add_trace(
            go.Scatter(
                x=x_labels,
                y=y_values,
                name=machine.upper(),
                mode=trace_mode,
                line=dict(color=color, width=2),
                marker=dict(size=8, color=color),
                text=[f"{y:.1f}%" for y in y_values] if show_data_labels else None,
                textposition='top center',
                textfont=dict(size=10, color=text_color),
                hovertemplate=(
                    f"<b>{machine.upper()}</b><br>"
                    "Workweek: %{x}<br>"
                    "Yield: %{y:.2f}%<br>"
                    "<extra></extra>"
                )
            )
        )

    # Calculate y-axis range based on actual data (with padding)
    min_yield = df['yield_pct'].min()
    max_yield = df['yield_pct'].max()

    # Use user-specified y_axis_min if provided, otherwise calculate dynamically
    if y_axis_min is not None:
        y_min = y_axis_min
    else:
        y_min = max(0, min_yield - 5)  # 5% padding below, but not below 0

    # Add extra padding at top if showing data labels
    top_padding = 5 if show_data_labels else 2
    y_max = min(105, max_yield + top_padding)

    # Update layout
    title_did = f" - {design_id}" if design_id else ""
    fig.update_layout(
        title=dict(
            text=f"SMT6 Machine Yield Trend{title_did}",
            font=dict(color=text_color, size=16)
        ),
        xaxis=dict(
            title=dict(text="Work Week", font=dict(color=text_color)),
            type='category',
            categoryorder='array',
            categoryarray=fiscal_labels,  # Explicit order
            tickfont=dict(color=text_color),
            gridcolor=grid_color
        ),
        yaxis=dict(
            title=dict(text="Yield %", font=dict(color=text_color)),
            tickfont=dict(color=text_color),
            gridcolor=grid_color,
            range=[y_min, y_max]  # Dynamic range based on data
        ),
        legend=dict(
            font=dict(color=text_color, size=11),
            bgcolor=legend_bg,
            bordercolor=legend_border,
            borderwidth=1
        ),
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        hovermode='closest',
        hoverlabel=dict(
            bgcolor=hover_bg,
            font=dict(color=hover_font_color, size=13),
            bordercolor=hover_border_color,
            namelength=-1  # Show full trace name
        ),
        height=500
    )

    return fig


def create_smt6_summary_table(df: pd.DataFrame, dark_mode: bool = True) -> str:
    """
    Create HTML summary table for SMT6 machine yields.

    Args:
        df: DataFrame with SMT6 yield data
        dark_mode: If True, use dark mode colors

    Returns:
        HTML string for the summary table
    """
    if df.empty:
        return ""

    # Aggregate by machine - include week range info
    summary = df.groupby('machine_id').agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum',
        'yield_pct': 'mean',
        'workweek': ['count', 'min', 'max']
    }).reset_index()

    # Flatten column names
    summary.columns = ['machine_id', 'uin_adj', 'upass_adj', 'yield_pct', 'wk_count', 'wk_min', 'wk_max']

    summary['overall_yield'] = (summary['upass_adj'] / summary['uin_adj'] * 100).round(2)
    # Create week range string (show last 2 digits of workweek for brevity)
    summary['wk_range'] = summary.apply(
        lambda r: f"{str(r['wk_min'])[-2:]}-{str(r['wk_max'])[-2:]}" if r['wk_min'] != r['wk_max'] else str(r['wk_max'])[-2:],
        axis=1
    )
    summary = summary.sort_values('overall_yield', ascending=False)

    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#333333'
    header_bg = '#3d3d3d' if dark_mode else '#f5f5f5'
    border_color = '#555555' if dark_mode else '#dddddd'
    good_color = '#1a472a' if dark_mode else '#d4edda'
    bad_color = '#4a1a1a' if dark_mode else '#ffcccc'

    html = f'''
    <div style="margin: 0; padding: 0;">
        <table style="border-collapse: collapse; width: 100%; font-size: 11px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 5px 8px; text-align: left; color: {text_color};">Machine</th>
                    <th style="border: 1px solid {border_color}; padding: 5px 8px; text-align: right; color: {text_color};">UIN</th>
                    <th style="border: 1px solid {border_color}; padding: 5px 8px; text-align: right; color: {text_color};">UPASS</th>
                    <th style="border: 1px solid {border_color}; padding: 5px 8px; text-align: right; color: {text_color};">Yield</th>
                    <th style="border: 1px solid {border_color}; padding: 5px 8px; text-align: center; color: {text_color};">WW Range</th>
                </tr>
            </thead>
            <tbody>
    '''

    for _, row in summary.iterrows():
        machine = row['machine_id'].upper()
        color = SMT6_COLORS.get(row['machine_id'], '#888888')
        yield_val = row['overall_yield']

        # Highlight based on yield (green if >= target, red if below)
        row_style = f'background-color: {good_color};' if yield_val >= TARGET_YIELD else f'background-color: {bad_color};'

        html += f'''
            <tr style="{row_style}">
                <td style="border: 1px solid {border_color}; padding: 4px 8px; color: {text_color};"><span style="color:{color};">●</span> {machine}</td>
                <td style="border: 1px solid {border_color}; padding: 4px 8px; text-align: right; color: {text_color};">{int(row['uin_adj']):,}</td>
                <td style="border: 1px solid {border_color}; padding: 4px 8px; text-align: right; color: {text_color};">{int(row['upass_adj']):,}</td>
                <td style="border: 1px solid {border_color}; padding: 4px 8px; text-align: right; color: {text_color}; font-weight: 600;">{yield_val:.2f}%</td>
                <td style="border: 1px solid {border_color}; padding: 4px 8px; text-align: center; color: {text_color};">{row['wk_range']}</td>
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        <div style="font-size: 10px; color: {'#888' if dark_mode else '#666'}; margin-top: 4px;">🟢 ≥{TARGET_YIELD}% &nbsp; 🔴 Below target</div>
    </div>
    '''

    return html


# ============================================================================
# SITE-LEVEL YIELD FUNCTIONS
# ============================================================================

def build_smt6_site_command(
    design_id: str,
    workweek: str,
    form_factor: str = "socamm2",
    facility: str = "PENANG",
    step: str = "hmfn",
    density: Optional[str] = None,
    speed: Optional[str] = None
) -> str:
    """
    Build the frpt command for SMT6 site-level yield data.
    """
    cmd = (
        f"/u/pe_burn_dft/bin/frptx -xf -bin=soft "
        f"-myquick=/MFG_WORKWEEK/ "
        f"-quick=/myquick,machine_id,site/ "
        f"-sort=// "
        f"+regwidth +module "
        f"-dbase={design_id.lower()} "
        f"-step={step} "
        f"-all -n -r +imesh "
        f"-nonshippable=N/A -eng_summary=N/A "
        f"-standard_flow=yes "
        f"-machine_id=~SMT6 "
        f"+# "
        f"-module_form_factor={form_factor} "
        f"-mfg_workweek={workweek} "
        f"-test_facility={facility} "
    )
    # Add optional density filter
    if density:
        cmd += f"-module_density={density} "
    # Add optional speed filter (convert MTPS to MT format for frpt)
    if speed:
        # frpt uses "7500MT" format, not "7500MTPS"
        speed_frpt = speed.replace("MTPS", "MT") if speed.endswith("MTPS") else speed
        cmd += f"-module_speed={speed_frpt} "
    cmd += "+%"
    return cmd


def parse_smt6_site_output(output: str, design_id: str) -> list[dict]:
    """
    Parse frpt output to extract SMT6 site-level yield data.
    Format: workweek_machine_site  UIN  UPASS  UIN  UPASS  YIELD
    Example: 202610_smt61-0001_S0C0P00  17  14  14  14  100.00
    """
    results = []

    # Pattern: workweek_machine_site (handles smt61-0001, smt61e-0004, etc.)
    pattern = r'^(\d+)_(smt\d+[a-z]*-\d+)_([A-Z0-9]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)'

    for line in output.split('\n'):
        match = re.match(pattern, line.strip(), re.IGNORECASE)
        if match:
            workweek = match.group(1)
            machine_id = match.group(2).lower()
            site = match.group(3).upper()
            uin_raw = int(match.group(4))
            upass_raw = int(match.group(5))
            uin_adj = int(match.group(6))
            upass_adj = int(match.group(7))
            yield_pct = float(match.group(8))

            results.append({
                'workweek': int(workweek),
                'machine_id': machine_id,
                'site': site,
                'design_id': design_id.upper(),
                'uin_raw': uin_raw,
                'upass_raw': upass_raw,
                'uin_adj': uin_adj,
                'upass_adj': upass_adj,
                'yield_pct': yield_pct
            })

    return results


def fetch_smt6_site_single(
    design_id: str,
    workweek: str,
    form_factor: str = "socamm2",
    timeout: int = 120,
    density: Optional[str] = None,
    speed: Optional[str] = None
) -> list[dict]:
    """Fetch SMT6 site-level yield data for a single workweek and design ID."""
    cmd = build_smt6_site_command(design_id, workweek, form_factor, density=density, speed=speed)

    # Check cache first
    cached_output = _get_cached_result(cmd)
    if cached_output is not None:
        return parse_smt6_site_output(cached_output, design_id)

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode == 0:
            # Cache successful result
            _set_cached_result(cmd, result.stdout)
            return parse_smt6_site_output(result.stdout, design_id)
        else:
            logger.warning(f"SMT6 site fetch failed for {design_id}/WW{workweek}: {result.stderr}")
            return []

    except subprocess.TimeoutExpired:
        logger.error(f"SMT6 site fetch timed out for {design_id}/WW{workweek}")
        return []
    except Exception as e:
        logger.error(f"SMT6 site fetch error for {design_id}/WW{workweek}: {e}")
        return []


def fetch_smt6_site_data(
    design_ids: list[str],
    workweeks: list[str],
    form_factor: str = "socamm2",
    max_workers: int = 8,
    progress_callback: Optional[callable] = None,
    density: Optional[str] = None,
    speed: Optional[str] = None
) -> pd.DataFrame:
    """Fetch SMT6 site-level yield data for multiple workweeks and design IDs."""
    all_results = []

    tasks = [
        (did, ww, form_factor, density, speed)
        for did in design_ids
        for ww in workweeks
    ]

    total_tasks = len(tasks)
    logger.info(f"Fetching SMT6 site data for {total_tasks} combinations (density={density}, speed={speed})...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_smt6_site_single, did, ww, ff, density=dens, speed=spd): (did, ww)
            for did, ww, ff, dens, spd in tasks
        }

        completed = 0
        for future in as_completed(futures):
            did, ww = futures[future]
            completed += 1
            try:
                results = future.result()
                all_results.extend(results)
                if results:
                    logger.debug(f"Got {len(results)} site records for {did}/WW{ww}")
            except Exception as e:
                logger.error(f"Failed to fetch SMT6 site data for {did}/WW{ww}: {e}")

            if progress_callback:
                progress_callback(completed, total_tasks, f"Processed {did}/WW{ww}")

    logger.info(f"SMT6 site fetch complete: {len(all_results)} total records from {total_tasks} queries")

    if all_results:
        df = pd.DataFrame(all_results)
        df = df.sort_values(['workweek', 'machine_id', 'site'])
        return df

    return pd.DataFrame()


def create_machine_yield_cards(df: pd.DataFrame, dark_mode: bool = True) -> str:
    """
    Create tester status visualization for SMT6 machines with circular gauges.

    Args:
        df: DataFrame with SMT6 yield data (machine or site level)
        dark_mode: If True, use dark mode colors (ignored - uses neutral colors)

    Returns:
        HTML string with tester status visualization featuring circular progress rings
    """
    if df.empty:
        return ""

    # Get latest workweek only
    latest_ww = df['workweek'].max()
    latest_df = df[df['workweek'] == latest_ww]

    if latest_df.empty:
        return ""

    # Aggregate by machine for latest week
    summary = latest_df.groupby('machine_id').agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum'
    }).reset_index()

    summary['yield_pct'] = (summary['upass_adj'] / summary['uin_adj'] * 100).round(2)
    summary = summary.sort_values('machine_id')

    # Calculate week-over-week trend if we have multiple weeks
    trend_data = {}
    all_weeks = sorted(df['workweek'].unique())
    if len(all_weeks) >= 2:
        prev_ww = all_weeks[-2]
        prev_df = df[df['workweek'] == prev_ww]
        prev_summary = prev_df.groupby('machine_id').agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        prev_summary['yield_pct'] = (prev_summary['upass_adj'] / prev_summary['uin_adj'] * 100).round(2)
        for _, row in prev_summary.iterrows():
            trend_data[row['machine_id']] = row['yield_pct']

    def get_status_info(yield_val):
        """Get status color and styling based on yield value."""
        if yield_val >= TARGET_YIELD:
            return {
                'color': '#00C853',  # Bright green
                'bg': 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
                'ring_bg': 'rgba(0, 200, 83, 0.15)',
                'status': 'HEALTHY',
                'pulse': False
            }
        elif yield_val >= 98.0:
            return {
                'color': '#00BCD4',  # Cyan
                'bg': 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
                'ring_bg': 'rgba(0, 188, 212, 0.15)',
                'status': 'AT RISK',
                'pulse': False
            }
        elif yield_val >= 96.0:
            return {
                'color': '#FFB300',  # Amber
                'bg': 'linear-gradient(135deg, #1a1a2e 0%, #2d2a1e 100%)',
                'ring_bg': 'rgba(255, 179, 0, 0.15)',
                'status': 'WARNING',
                'pulse': True
            }
        else:
            return {
                'color': '#FF1744',  # Red
                'bg': 'linear-gradient(135deg, #1a1a2e 0%, #2e1a1a 100%)',
                'ring_bg': 'rgba(255, 23, 68, 0.2)',
                'status': 'CRITICAL',
                'pulse': True
            }

    # Count status summary
    healthy = sum(1 for _, r in summary.iterrows() if r['yield_pct'] >= TARGET_YIELD)
    monitor = sum(1 for _, r in summary.iterrows() if 98.0 <= r['yield_pct'] < TARGET_YIELD)
    warning = sum(1 for _, r in summary.iterrows() if 96.0 <= r['yield_pct'] < 98.0)
    critical = sum(1 for _, r in summary.iterrows() if r['yield_pct'] < 96.0)

    html = f'''
    <style>
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.6; }}
        }}
        .tester-card {{
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        .tester-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 8px 20px rgba(0,0,0,0.3);
        }}
        .pulse-animation {{
            animation: pulse 2s ease-in-out infinite;
        }}
    </style>
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
        <!-- Compact Header -->
        <div style="
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-radius: 10px;
            padding: 8px 12px;
            margin-bottom: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        ">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px;">
                <div>
                    <span style="color: #fff; font-size: 14px; font-weight: 600;">
                        🖥️ SMT6 Tester Fleet
                    </span>
                    <span style="color: #8892b0; font-size: 11px; margin-left: 8px;">
                        WW{latest_ww} • Target: {TARGET_YIELD}%
                    </span>
                </div>
                <div style="display: flex; gap: 12px; font-size: 11px;">
                    <span style="color: #00C853;">● {healthy} Healthy</span>
                    <span style="color: #FFB300;">● {warning + monitor} At Risk</span>
                    <span style="color: #FF1744;">● {critical} Critical</span>
                </div>
            </div>
        </div>

        <!-- Tester Cards Grid - Full Width -->
        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px;">
    '''

    for _, row in summary.iterrows():
        machine = row['machine_id'].upper()
        yield_val = row['yield_pct']
        uin = int(row['uin_adj'])
        upass = int(row['upass_adj'])
        ufail = uin - upass
        status = get_status_info(yield_val)

        # Calculate trend
        prev_yield = trend_data.get(row['machine_id'])
        trend_arrow = ""
        trend_color = "#8892b0"
        if prev_yield is not None:
            diff = yield_val - prev_yield
            if diff > 0.5:
                trend_arrow = "▲"
                trend_color = "#00C853"
            elif diff < -0.5:
                trend_arrow = "▼"
                trend_color = "#FF1744"
            else:
                trend_arrow = "―"
                trend_color = "#8892b0"

        # Calculate gap from target
        gap = yield_val - TARGET_YIELD
        gap_text = f"+{gap:.1f}%" if gap >= 0 else f"{gap:.1f}%"
        gap_color = "#00C853" if gap >= 0 else "#FF1744"

        # SVG circular progress ring
        radius = 45
        circumference = 2 * 3.14159 * radius
        progress = min(yield_val, 100) / 100
        stroke_dashoffset = circumference * (1 - progress)

        pulse_class = "pulse-animation" if status['pulse'] else ""
        ring_pulse = "ring-pulse" if status['pulse'] else ""

        # Larger gauge for better visibility
        sm_radius = 48
        sm_circumference = 2 * 3.14159 * sm_radius
        sm_stroke_dashoffset = sm_circumference * (1 - progress)

        html += f'''
        <div class="tester-card" style="
            background: {status['bg']};
            border-radius: 10px;
            padding: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            border: 1px solid rgba(255,255,255,0.08);
        ">
            <!-- Machine Name & Status -->
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="color: #fff; font-weight: 700; font-size: 14px;">{machine}</div>
                <div class="{pulse_class}" style="
                    background: {status['color']}22;
                    color: {status['color']};
                    padding: 3px 8px;
                    border-radius: 8px;
                    font-size: 9px;
                    font-weight: 600;
                    text-transform: uppercase;
                ">{status['status']}</div>
            </div>

            <!-- Circular Gauge -->
            <div style="display: flex; justify-content: center; margin-bottom: 8px;">
                <div style="position: relative; width: 120px; height: 120px;">
                    <svg width="120" height="120" style="transform: rotate(-90deg);">
                        <circle cx="60" cy="60" r="{sm_radius}" fill="none" stroke="{status['ring_bg']}" stroke-width="8"/>
                        <circle cx="60" cy="60" r="{sm_radius}" fill="none" stroke="{status['color']}" stroke-width="8"
                            stroke-linecap="round" stroke-dasharray="{sm_circumference}" stroke-dashoffset="{sm_stroke_dashoffset}"
                            style="transition: stroke-dashoffset 0.5s ease;"/>
                    </svg>
                    <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center;">
                        <div style="color: {status['color']}; font-size: 22px; font-weight: bold; line-height: 1;">{yield_val:.1f}%</div>
                        <div style="color: {trend_color}; font-size: 11px; margin-top: 2px;">{trend_arrow} WoW</div>
                    </div>
                </div>
            </div>

            <!-- Stats Row -->
            <div style="display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 6px;">
                <span style="color: #8892b0;">Pass: <span style="color: #00C853; font-weight: 600;">{upass:,}</span></span>
                <span style="color: #8892b0;">Fail: <span style="color: #FF1744; font-weight: 600;">{ufail:,}</span></span>
            </div>

            <!-- Target Gap -->
            <div style="background: rgba(255,255,255,0.08); border-radius: 6px; padding: 6px 10px; display: flex; justify-content: space-between; align-items: center;">
                <span style="color: #8892b0; font-size: 10px;">vs Target</span>
                <span style="color: {gap_color}; font-size: 13px; font-weight: 700;">{gap_text}</span>
            </div>
        </div>
        '''

    html += '''
        </div>
    </div>
    '''

    return html


def parse_site_components(site_name: str) -> dict:
    """
    Parse site name (SxCyPzz format) into socket, channel, position.

    Args:
        site_name: Site identifier like S0C1P05

    Returns:
        Dict with socket, channel, position keys
    """
    match = re.match(r'S(\d+)C(\d+)P(\d+)', str(site_name), re.IGNORECASE)
    if match:
        return {
            'socket': int(match.group(1)),
            'socket_id': f"S{match.group(1)}",
            'channel': int(match.group(2)),
            'channel_id': f"C{match.group(2)}",
            'position': int(match.group(3)),
            'position_id': f"P{match.group(3).zfill(2)}"
        }
    # Fallback
    return {
        'socket': 0, 'socket_id': 'S0',
        'channel': 0, 'channel_id': str(site_name),
        'position': 0, 'position_id': 'P00'
    }


def create_machine_socket_heatmap(
    df: pd.DataFrame,
    dark_mode: bool = True
) -> Optional[go.Figure]:
    """
    Create Machine × Socket heatmap - PRIMARY VIEW for hardware health.

    This is the recommended primary visualization for SMT6.1 socket yield analysis.
    It quickly surfaces machine-specific or socket-specific anomalies.

    Args:
        df: DataFrame with site-level yield data (must have machine_id, site columns)
        dark_mode: If True, use dark mode colors

    Returns:
        Plotly Figure object showing yield by machine and socket
    """
    if df.empty or 'site' not in df.columns or 'machine_id' not in df.columns:
        logger.warning("create_machine_socket_heatmap: Missing required columns")
        return None

    # Parse socket from site names
    df = df.copy()
    parsed = df['site'].apply(parse_site_components)
    df['socket_id'] = parsed.apply(lambda x: x['socket_id'])

    # Aggregate by machine and socket
    summary = df.groupby(['machine_id', 'socket_id']).agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum'
    }).reset_index()
    summary['yield_pct'] = (summary['upass_adj'] / summary['uin_adj'] * 100).round(2)
    summary['volume'] = summary['uin_adj']

    logger.info(f"Machine-Socket heatmap: {len(summary)} combinations")

    # Create pivot table: Machine (rows) x Socket (cols)
    pivot = summary.pivot_table(
        index='machine_id',
        columns='socket_id',
        values='yield_pct',
        aggfunc='mean'
    )

    # Also create volume pivot for hover info
    volume_pivot = summary.pivot_table(
        index='machine_id',
        columns='socket_id',
        values='volume',
        aggfunc='sum'
    )

    if pivot.empty:
        logger.warning("create_machine_socket_heatmap: Pivot is empty")
        return None

    # Sort machines and sockets
    pivot = pivot.reindex(sorted(pivot.index, key=lambda x: x.lower()), axis=0)
    pivot = pivot.reindex(sorted(pivot.columns, key=lambda x: int(x[1:]) if x[1:].isdigit() else 0), axis=1)
    volume_pivot = volume_pivot.reindex(pivot.index, axis=0).reindex(pivot.columns, axis=1)

    # Calculate stats for pattern detection
    machine_avg = pivot.mean(axis=1)
    socket_avg = pivot.mean(axis=0)
    overall_avg = pivot.mean().mean()

    # Create text with yield and volume
    text_values = []
    for i, row in enumerate(pivot.values):
        text_row = []
        for j, v in enumerate(row):
            if pd.notna(v):
                vol = volume_pivot.iloc[i, j] if pd.notna(volume_pivot.iloc[i, j]) else 0
                text_row.append(f"{v:.1f}%<br>n={int(vol)}")
            else:
                text_row.append("")
        text_values.append(text_row)

    # Create custom hover text with diagnostic hints
    hover_text = []
    for i, machine in enumerate(pivot.index):
        hover_row = []
        for j, socket in enumerate(pivot.columns):
            v = pivot.iloc[i, j]
            vol = volume_pivot.iloc[i, j] if pd.notna(volume_pivot.iloc[i, j]) else 0
            if pd.notna(v):
                # Add diagnostic hint
                hint = ""
                if v < machine_avg[machine] - 5:
                    hint = "<br>⚠️ Socket underperforming vs machine avg"
                elif v < overall_avg - 10:
                    hint = "<br>🔴 Significantly below fleet avg"
                hover_row.append(
                    f"<b>Machine:</b> {machine.upper()}<br>"
                    f"<b>Socket:</b> {socket}<br>"
                    f"<b>Yield:</b> {v:.2f}%<br>"
                    f"<b>Volume:</b> {int(vol)} units<br>"
                    f"<b>Machine Avg:</b> {machine_avg[machine]:.2f}%<br>"
                    f"<b>Socket Avg:</b> {socket_avg[socket]:.2f}%{hint}"
                )
            else:
                hover_row.append("")
        hover_text.append(hover_row)

    # Theme colors
    text_color = '#1a1a1a'

    # Dynamic color range
    min_yield = pivot.min().min()
    max_yield = pivot.max().max()
    zmin = max(0, min(90, min_yield - 2))
    zmax = min(100, max(100, max_yield + 1))

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=[m.upper() for m in pivot.index.tolist()],
        colorscale=[
            [0, '#FF1744'],      # Red - critical
            [0.3, '#FF9800'],    # Orange - warning
            [0.5, '#FFEB3B'],    # Yellow - monitor
            [0.7, '#8BC34A'],    # Light green - good
            [1, '#00C853']       # Bright green - excellent
        ],
        zmin=zmin,
        zmax=zmax,
        text=text_values,
        texttemplate="%{text}",
        textfont={"size": 11, "color": "#000"},
        hovertext=hover_text,
        hovertemplate="%{hovertext}<extra></extra>",
        colorbar=dict(
            title=dict(text="Yield %", font=dict(color=text_color, size=12)),
            tickfont=dict(color=text_color, size=10),
            len=0.8
        ),
        xgap=4,
        ygap=4
    ))

    fig.update_layout(
        title=dict(
            text="🔧 SMT6.1 Hardware Health: Machine × Socket Yield",
            font=dict(color=text_color, size=16),
            x=0.5
        ),
        xaxis=dict(
            title=dict(text="Socket", font=dict(color=text_color, size=12)),
            tickfont=dict(color=text_color, size=11),
            side='bottom'
        ),
        yaxis=dict(
            title=dict(text="Machine", font=dict(color=text_color, size=12)),
            tickfont=dict(color=text_color, size=11),
            autorange='reversed'
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(248,249,250,0.95)',
        height=max(250, len(pivot.index) * 60 + 120),
        margin=dict(l=100, r=100, t=60, b=60)
    )

    return fig


def create_socket_drilldown_heatmap(
    df: pd.DataFrame,
    machine_id: str,
    socket_id: str = None,
    dark_mode: bool = True
) -> Optional[go.Figure]:
    """
    Create Channel × Position heatmap for drill-down analysis within a socket.

    This is the DRILL-DOWN view for detailed site-level analysis after
    identifying a problematic machine/socket in the primary view.

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine to filter (required)
        socket_id: Optional socket filter (e.g., "S0")
        dark_mode: If True, use dark mode colors

    Returns:
        Plotly Figure showing Channel × Position yield grid
    """
    if df.empty or 'site' not in df.columns:
        return None

    # Filter by machine
    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return None

    # Parse site components
    parsed = df['site'].apply(parse_site_components)
    df['socket_id'] = parsed.apply(lambda x: x['socket_id'])
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])
    df['position_id'] = parsed.apply(lambda x: x['position_id'])

    # Filter by socket if specified
    if socket_id:
        df = df[df['socket_id'] == socket_id]
        if df.empty:
            return None

    # Aggregate by channel and position
    site_data = df.groupby(['channel_id', 'position_id']).agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum'
    }).reset_index()
    site_data['yield_pct'] = (site_data['upass_adj'] / site_data['uin_adj'] * 100).round(2)

    # Create pivot: Channel (rows) x Position (cols)
    pivot = site_data.pivot_table(
        index='channel_id',
        columns='position_id',
        values='yield_pct',
        aggfunc='mean'
    )

    if pivot.empty:
        return None

    # Sort naturally
    def safe_sort_key(x):
        try:
            return int(x[1:]) if len(x) > 1 and x[1:].isdigit() else 0
        except:
            return 0

    pivot = pivot.reindex(sorted(pivot.index, key=safe_sort_key), axis=0)
    pivot = pivot.reindex(sorted(pivot.columns, key=safe_sort_key), axis=1)

    # Theme colors
    text_color = '#1a1a1a'

    # Dynamic range
    min_yield = pivot.min().min()
    max_yield = pivot.max().max()
    zmin = max(0, min(85, min_yield - 2))
    zmax = min(100, max(100, max_yield + 1))

    # Text values
    text_values = [[f"{v:.1f}%" if pd.notna(v) else "" for v in row] for row in pivot.values]

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[
            [0, '#FF1744'],
            [0.3, '#FF9800'],
            [0.5, '#FFEB3B'],
            [0.7, '#8BC34A'],
            [1, '#00C853']
        ],
        zmin=zmin,
        zmax=zmax,
        text=text_values,
        texttemplate="%{text}",
        textfont={"size": 10, "color": "#000"},
        hovertemplate=(
            "<b>Channel:</b> %{y}<br>"
            "<b>Position:</b> %{x}<br>"
            "<b>Yield:</b> %{z:.2f}%<br>"
            "<extra></extra>"
        ),
        colorbar=dict(
            title=dict(text="Yield %", font=dict(color=text_color, size=12)),
            tickfont=dict(color=text_color, size=10),
            len=0.8
        ),
        xgap=3,
        ygap=3
    ))

    socket_label = f" / {socket_id}" if socket_id else " (All Sockets)"
    title_text = f"📍 Site Detail: {machine_id.upper()}{socket_label} - Channel × Position"

    fig.update_layout(
        title=dict(
            text=title_text,
            font=dict(color=text_color, size=14),
            x=0.5
        ),
        xaxis=dict(
            title=dict(text="Position", font=dict(color=text_color, size=12)),
            tickfont=dict(color=text_color, size=10),
            side='bottom',
            tickangle=0
        ),
        yaxis=dict(
            title=dict(text="Channel", font=dict(color=text_color, size=12)),
            tickfont=dict(color=text_color, size=10),
            autorange='reversed'
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(248,249,250,0.95)',
        height=max(280, len(pivot.index) * 45 + 100),
        margin=dict(l=60, r=100, t=60, b=60)
    )

    return fig


def create_site_yield_heatmap(
    df: pd.DataFrame,
    machine_id: str = None,
    dark_mode: bool = True
) -> Optional[go.Figure]:
    """
    Create site yield heatmap - wrapper that chooses appropriate view.

    If no machine specified: Shows Machine × Socket overview (primary view)
    If machine specified: Shows Channel × Position drill-down

    Args:
        df: DataFrame with site-level yield data
        machine_id: Optional machine filter for drill-down
        dark_mode: If True, use dark mode colors

    Returns:
        Plotly Figure object
    """
    if machine_id:
        # Drill-down view for specific machine
        return create_socket_drilldown_heatmap(df, machine_id, dark_mode=dark_mode)
    else:
        # Primary view: Machine × Socket
        return create_machine_socket_heatmap(df, dark_mode=dark_mode)


def create_site_grid_html(
    df: pd.DataFrame,
    machine_id: str,
    title: str = None,
    view_mode: str = "socket"
) -> str:
    """
    Create a beautiful HTML grid visualization of site yields.

    Following SMT6.1 visualization principles:
    - Primary view: Collapse S & C, show Socket Position (P##) only
    - P## is the physical socket where yield issues manifest
    - Expand to Channel level only for diagnosis

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine to filter
        title: Optional title override
        view_mode: "socket" (collapse to P##) or "channel" (show C×P grid)

    Returns:
        HTML string with the site grid visualization
    """
    if df.empty or 'site' not in df.columns:
        return ""

    # Filter for machine
    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return ""

    # Parse site components (S#C#P##)
    parsed = df['site'].apply(parse_site_components)
    df['socket_id'] = parsed.apply(lambda x: x['socket_id'])      # S#
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])    # C#
    df['position_id'] = parsed.apply(lambda x: x['position_id'])  # P## - PRIMARY

    def get_yield_color(yield_val, volume=None):
        """Get background color based on yield value."""
        # Reduce opacity for low volume (< 50 units)
        opacity = "CC" if volume and volume < 50 else ""
        if yield_val >= 99:
            return f'#00C853{opacity}'  # Bright green
        elif yield_val >= 97:
            return f'#8BC34A{opacity}'  # Light green
        elif yield_val >= 95:
            return f'#FFEB3B{opacity}'  # Yellow
        elif yield_val >= 90:
            return f'#FF9800{opacity}'  # Orange
        else:
            return f'#FF1744{opacity}'  # Red

    def get_text_color(yield_val):
        """Get text color for contrast."""
        return '#000' if yield_val >= 95 else '#fff'

    def get_confidence_indicator(volume):
        """Get confidence indicator based on volume."""
        if volume < 20:
            return "⚠️"  # Very low confidence
        elif volume < 50:
            return "⚡"   # Low confidence
        return ""

    if view_mode == "socket":
        # =====================================================================
        # PRIMARY VIEW: Collapse to Socket Position (P##)
        # This is the correct view for monitoring - P## is where physics happens
        # =====================================================================

        # Count unique slices (S#) and channels (C#) for info display
        num_slices = df['socket_id'].nunique()
        num_channels = df['channel_id'].nunique()

        # Aggregate by socket position (P##), collapsing S and C
        socket_data = df.groupby('position_id').agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        socket_data['yield_pct'] = (socket_data['upass_adj'] / socket_data['uin_adj'] * 100).round(2)
        socket_data['volume'] = socket_data['uin_adj']

        # Get positions and sort numerically
        positions = sorted(socket_data['position_id'].unique(),
                          key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

        # Calculate summary stats
        total_volume = socket_data['volume'].sum()
        avg_yield = (socket_data['upass_adj'].sum() / socket_data['uin_adj'].sum() * 100) if socket_data['uin_adj'].sum() > 0 else 0
        min_yield = socket_data['yield_pct'].min()
        max_yield = socket_data['yield_pct'].max()
        min_socket = socket_data.loc[socket_data['yield_pct'].idxmin(), 'position_id']

        # Create lookup
        socket_lookup = {row['position_id']: row for _, row in socket_data.iterrows()}

        title_text = title or f"🔧 {machine_id.upper()} - Socket Health"

        # Determine grid columns based on socket count
        num_sockets = len(positions)
        if num_sockets <= 4:
            grid_cols = 2
            max_width = "400px"
        else:
            grid_cols = 4  # 4 columns for 5-8 sockets
            max_width = "700px"

        # Build HTML with responsive grid layout
        html = f'''
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 15px;">
            <!-- Header -->
            <div style="
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                border-radius: 12px;
                padding: 15px 20px;
                margin-bottom: 15px;
                color: #fff;
            ">
                <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                    <div>
                        <span style="font-size: 16px; font-weight: 600;">{title_text}</span>
                        <span style="color: #8892b0; font-size: 12px; margin-left: 15px;">
                            {num_sockets} sockets | {num_slices} slice{'s' if num_slices > 1 else ''} | {num_channels} channel{'s' if num_channels > 1 else ''} | {total_volume:,} units
                        </span>
                    </div>
                    <div style="display: flex; gap: 20px; font-size: 12px;">
                        <span>Avg: <b style="color: {get_yield_color(avg_yield)}">{avg_yield:.1f}%</b></span>
                        <span>Worst: <b style="color: {get_yield_color(min_yield)}">{min_socket} ({min_yield:.1f}%)</b></span>
                    </div>
                </div>
            </div>

            <!-- Physical Socket Layout (responsive grid) -->
            <div style="display: flex; justify-content: center; margin-bottom: 15px;">
                <div style="display: grid; grid-template-columns: repeat({grid_cols}, 1fr); gap: 12px; max-width: {max_width};">
        '''

        # Arrange sockets in physical layout order
        physical_order = ['P00', 'P01', 'P02', 'P03', 'P04', 'P05', 'P06', 'P07', 'P08', 'P09', 'P10', 'P11']
        for pos in physical_order:
            if pos in socket_lookup:
                data = socket_lookup[pos]
                yield_val = data['yield_pct']
                volume = data['volume']
                bg_color = get_yield_color(yield_val, volume)
                text_color = get_text_color(yield_val)
                confidence = get_confidence_indicator(volume)

                # Determine status
                if yield_val >= 99:
                    status = "HEALTHY"
                    status_color = "#00C853"
                elif yield_val >= 97:
                    status = "MONITOR"
                    status_color = "#8BC34A"
                elif yield_val >= 95:
                    status = "WARNING"
                    status_color = "#FFB300"
                else:
                    status = "CRITICAL"
                    status_color = "#FF1744"

                html += f'''
                    <div style="
                        background: {bg_color};
                        border-radius: 12px;
                        padding: 15px;
                        min-width: 150px;
                        text-align: center;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                        cursor: pointer;
                        transition: transform 0.2s, box-shadow 0.2s;
                    " onmouseover="this.style.transform='scale(1.03)'; this.style.boxShadow='0 8px 20px rgba(0,0,0,0.25)';"
                       onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 4px 12px rgba(0,0,0,0.15)';"
                       title="Socket {pos}: {yield_val:.2f}% yield, {volume} units tested">
                        <div style="font-size: 14px; font-weight: 700; color: {text_color}; margin-bottom: 5px;">
                            {pos} {confidence}
                        </div>
                        <div style="font-size: 28px; font-weight: 800; color: {text_color}; line-height: 1;">
                            {yield_val:.1f}%
                        </div>
                        <div style="font-size: 11px; color: {text_color}; opacity: 0.8; margin-top: 5px;">
                            n={volume:,}
                        </div>
                        <div style="
                            margin-top: 8px;
                            padding: 3px 8px;
                            background: rgba(255,255,255,0.2);
                            border-radius: 10px;
                            font-size: 9px;
                            font-weight: 600;
                            color: {text_color};
                            text-transform: uppercase;
                        ">{status}</div>
                    </div>
                '''
            elif pos in positions:
                # Position exists but not in physical_order mapping
                pass

        html += '''
                </div>
            </div>

            <!-- Legend -->
            <div style="display: flex; justify-content: center; gap: 15px; font-size: 11px; color: #666;">
                <span><span style="display: inline-block; width: 12px; height: 12px; background: #00C853; border-radius: 2px; margin-right: 4px;"></span>≥99% Healthy</span>
                <span><span style="display: inline-block; width: 12px; height: 12px; background: #8BC34A; border-radius: 2px; margin-right: 4px;"></span>97-99%</span>
                <span><span style="display: inline-block; width: 12px; height: 12px; background: #FFEB3B; border-radius: 2px; margin-right: 4px;"></span>95-97%</span>
                <span><span style="display: inline-block; width: 12px; height: 12px; background: #FF1744; border-radius: 2px; margin-right: 4px;"></span>&lt;95% Critical</span>
                <span style="margin-left: 10px;">⚠️ = low volume</span>
            </div>

            <div style="margin-top: 10px; font-size: 10px; color: #888; text-align: center;">
                Socket positions mirror physical tester layout. Click for channel breakdown.
            </div>
        </div>
        '''

        return html

    else:
        # =====================================================================
        # DIAGNOSTIC VIEW: Channel × Position (expand for root-cause)
        # Only use when a socket is suspicious - shows routing path detail
        # =====================================================================

        # Aggregate by channel and position
        site_data = df.groupby(['channel_id', 'position_id']).agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        site_data['yield_pct'] = (site_data['upass_adj'] / site_data['uin_adj'] * 100).round(2)
        site_data['volume'] = site_data['uin_adj']

        channels = sorted(site_data['channel_id'].unique(),
                         key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
        positions = sorted(site_data['position_id'].unique(),
                          key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

        site_lookup = {}
        for _, row in site_data.iterrows():
            site_lookup[(row['channel_id'], row['position_id'])] = row

        total_volume = site_data['volume'].sum()
        avg_yield = (site_data['upass_adj'].sum() / site_data['uin_adj'].sum() * 100) if site_data['uin_adj'].sum() > 0 else 0

        title_text = title or f"🔬 {machine_id.upper()} - Channel × Position (Diagnostic)"

        html = f'''
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 15px;">
            <div style="
                background: linear-gradient(135deg, #2d1f3d 0%, #1a1a2e 100%);
                border-radius: 12px;
                padding: 12px 16px;
                margin-bottom: 12px;
                color: #fff;
            ">
                <span style="font-size: 14px; font-weight: 600;">{title_text}</span>
                <span style="color: #b39ddb; font-size: 11px; margin-left: 12px;">
                    {len(site_data)} sites | {total_volume:,} units | Avg: {avg_yield:.1f}%
                </span>
            </div>

            <div style="overflow-x: auto;">
                <table style="border-collapse: separate; border-spacing: 3px; width: 100%;">
                    <tr>
                        <th style="padding: 6px; font-size: 10px; color: #666;"></th>
        '''

        for pos in positions:
            html += f'<th style="padding: 6px; font-size: 10px; color: #666; text-align: center;">{pos}</th>'
        html += '</tr>'

        for channel in channels:
            html += f'<tr><td style="padding: 6px; font-size: 10px; color: #666; font-weight: 600;">{channel}</td>'
            for pos in positions:
                key = (channel, pos)
                if key in site_lookup:
                    row = site_lookup[key]
                    yield_val = row['yield_pct']
                    volume = row['volume']
                    bg_color = get_yield_color(yield_val, volume)
                    text_color = get_text_color(yield_val)
                    conf = get_confidence_indicator(volume)

                    html += f'''
                        <td style="
                            background: {bg_color};
                            color: {text_color};
                            padding: 6px 4px;
                            border-radius: 4px;
                            text-align: center;
                            font-size: 11px;
                            font-weight: 600;
                        " title="{channel}{pos}: {yield_val:.2f}% (n={volume})">
                            {yield_val:.1f}%{conf}
                        </td>
                    '''
                else:
                    html += '<td style="background: #f0f0f0; border-radius: 4px; text-align: center; color: #ccc;">-</td>'
            html += '</tr>'

        html += '''
                </table>
            </div>
            <div style="margin-top: 8px; font-size: 9px; color: #888; text-align: center;">
                Channel = routing path, Position = socket. Expand only for diagnosis.
            </div>
        </div>
        '''

        return html


def create_slice_overview_html(df: pd.DataFrame, machine_id: str) -> tuple[str, dict]:
    """
    Create HTML visualization showing slice-level overview for hierarchical navigation.

    Following SMT6.1 hierarchy: Slice (S#) → Channel (C#) → Position (P##)

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine to filter

    Returns:
        Tuple of (HTML string, slice_data dict for further drill-down)
    """
    if df.empty or 'site' not in df.columns:
        return "", {}

    # Filter for machine
    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return "", {}

    # Parse site components
    parsed = df['site'].apply(parse_site_components)
    df['slice_id'] = parsed.apply(lambda x: x['socket_id'])      # S#
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])   # C#
    df['position_id'] = parsed.apply(lambda x: x['position_id']) # P##

    # Aggregate by slice
    slice_data = df.groupby('slice_id').agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum',
        'channel_id': 'nunique',
        'position_id': 'nunique'
    }).reset_index()
    slice_data.columns = ['slice_id', 'volume', 'passed', 'num_channels', 'num_positions']
    slice_data['yield_pct'] = (slice_data['passed'] / slice_data['volume'] * 100).round(2)
    slice_data = slice_data.sort_values('slice_id', key=lambda x: x.str.extract(r'(\d+)')[0].astype(int))

    # Create lookup for drill-down
    slice_lookup = {row['slice_id']: row.to_dict() for _, row in slice_data.iterrows()}

    # Calculate totals
    total_volume = slice_data['volume'].sum()
    total_yield = (slice_data['passed'].sum() / slice_data['volume'].sum() * 100) if slice_data['volume'].sum() > 0 else 0

    def get_yield_color(yield_val):
        if yield_val >= 99: return '#00C853'
        elif yield_val >= 97: return '#8BC34A'
        elif yield_val >= 95: return '#FFEB3B'
        elif yield_val >= 90: return '#FF9800'
        else: return '#FF1744'

    def get_text_color(yield_val):
        return '#000' if yield_val >= 95 else '#fff'

    # Build HTML
    html = f'''
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 15px;">
        <!-- Header -->
        <div style="
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            border-radius: 12px;
            padding: 15px 20px;
            margin-bottom: 15px;
            color: #fff;
        ">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                <div>
                    <span style="font-size: 16px; font-weight: 600;">📊 {machine_id.upper()} - Slice Overview</span>
                    <span style="color: #b0c4de; font-size: 12px; margin-left: 15px;">
                        {len(slice_data)} slices | {total_volume:,} units
                    </span>
                </div>
                <div style="font-size: 12px;">
                    <span>Overall: <b style="color: {get_yield_color(total_yield)}">{total_yield:.1f}%</b></span>
                </div>
            </div>
        </div>

        <!-- Slice Cards -->
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 15px;">
    '''

    for _, row in slice_data.iterrows():
        slice_id = row['slice_id']
        yield_val = row['yield_pct']
        volume = row['volume']
        num_ch = row['num_channels']
        num_pos = row['num_positions']
        bg_color = get_yield_color(yield_val)
        text_color = get_text_color(yield_val)

        html += f'''
            <div style="
                background: {bg_color};
                border-radius: 10px;
                padding: 15px;
                text-align: center;
                box-shadow: 0 3px 10px rgba(0,0,0,0.15);
            ">
                <div style="font-size: 18px; font-weight: 700; color: {text_color}; margin-bottom: 8px;">
                    {slice_id}
                </div>
                <div style="font-size: 32px; font-weight: 800; color: {text_color}; line-height: 1;">
                    {yield_val:.1f}%
                </div>
                <div style="font-size: 11px; color: {text_color}; opacity: 0.85; margin-top: 8px;">
                    {num_ch} channels | {num_pos} positions
                </div>
                <div style="font-size: 10px; color: {text_color}; opacity: 0.7; margin-top: 4px;">
                    n={volume:,}
                </div>
            </div>
        '''

    html += '''
        </div>
        <div style="font-size: 10px; color: #888; text-align: center;">
            Select a slice above to see channel breakdown. Slice = Electronics partition serving one TSB.
        </div>
    </div>
    '''

    return html, slice_lookup


def create_channel_breakdown_html(df: pd.DataFrame, machine_id: str, slice_id: str) -> str:
    """
    Create HTML visualization showing channel breakdown for a specific slice.

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine to filter
        slice_id: Slice to show (e.g., 'S0')

    Returns:
        HTML string with channel × position grid
    """
    if df.empty or 'site' not in df.columns:
        return ""

    # Filter for machine
    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return ""

    # Parse site components
    parsed = df['site'].apply(parse_site_components)
    df['slice_id'] = parsed.apply(lambda x: x['socket_id'])
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])
    df['position_id'] = parsed.apply(lambda x: x['position_id'])

    # Filter for selected slice
    df = df[df['slice_id'] == slice_id]
    if df.empty:
        return ""

    # Aggregate by channel and position
    site_data = df.groupby(['channel_id', 'position_id']).agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum'
    }).reset_index()
    site_data['yield_pct'] = (site_data['upass_adj'] / site_data['uin_adj'] * 100).round(2)
    site_data['volume'] = site_data['uin_adj']

    channels = sorted(site_data['channel_id'].unique(),
                     key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
    positions = sorted(site_data['position_id'].unique(),
                      key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

    # Create lookup
    site_lookup = {}
    for _, row in site_data.iterrows():
        site_lookup[(row['channel_id'], row['position_id'])] = row

    total_volume = site_data['volume'].sum()
    avg_yield = (site_data['upass_adj'].sum() / site_data['uin_adj'].sum() * 100) if site_data['uin_adj'].sum() > 0 else 0

    def get_yield_color(yield_val, volume=None):
        opacity = "CC" if volume and volume < 50 else ""
        if yield_val >= 99: return f'#00C853{opacity}'
        elif yield_val >= 97: return f'#8BC34A{opacity}'
        elif yield_val >= 95: return f'#FFEB3B{opacity}'
        elif yield_val >= 90: return f'#FF9800{opacity}'
        else: return f'#FF1744{opacity}'

    def get_text_color(yield_val):
        return '#000' if yield_val >= 95 else '#fff'

    # Build HTML
    html = f'''
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 15px;">
        <div style="
            background: linear-gradient(135deg, #2d1f3d 0%, #1a1a2e 100%);
            border-radius: 12px;
            padding: 12px 16px;
            margin-bottom: 12px;
            color: #fff;
        ">
            <span style="font-size: 14px; font-weight: 600;">🔬 {machine_id.upper()} → {slice_id} - Channel × Position</span>
            <span style="color: #b39ddb; font-size: 11px; margin-left: 12px;">
                {len(channels)} channels | {len(positions)} positions | {total_volume:,} units | Avg: {avg_yield:.1f}%
            </span>
        </div>

        <div style="overflow-x: auto;">
            <table style="border-collapse: separate; border-spacing: 4px; width: 100%;">
                <tr>
                    <th style="padding: 8px; font-size: 11px; color: #666; font-weight: 600;"></th>
    '''

    # Header row with positions
    for pos in positions:
        html += f'<th style="padding: 8px; font-size: 11px; color: #666; text-align: center; font-weight: 600;">{pos}</th>'
    html += '</tr>'

    # Data rows by channel
    for ch in channels:
        html += f'<tr><td style="padding: 8px; font-size: 11px; color: #666; font-weight: 600;">{ch}</td>'
        for pos in positions:
            key = (ch, pos)
            if key in site_lookup:
                data = site_lookup[key]
                yield_val = data['yield_pct']
                volume = data['volume']
                bg_color = get_yield_color(yield_val, volume)
                text_color = get_text_color(yield_val)
                confidence = "⚠️" if volume < 50 else ""

                html += f'''
                    <td style="
                        background: {bg_color};
                        color: {text_color};
                        padding: 10px 8px;
                        border-radius: 6px;
                        text-align: center;
                        font-size: 13px;
                        font-weight: 600;
                        min-width: 70px;
                    " title="{ch}{pos}: {yield_val:.2f}% (n={volume})">
                        {yield_val:.1f}%{confidence}
                        <div style="font-size: 9px; opacity: 0.7; margin-top: 2px;">n={volume}</div>
                    </td>
                '''
            else:
                html += '<td style="background: #f0f0f0; border-radius: 6px; text-align: center; color: #ccc; min-width: 70px;">-</td>'
        html += '</tr>'

    html += '''
            </table>
        </div>
        <div style="margin-top: 10px; font-size: 10px; color: #888; text-align: center;">
            Channel = Address/control bus in COBRA3 ASIC | Position = Physical socket
        </div>
    </div>
    '''

    return html


def create_hierarchical_site_html(
    df: pd.DataFrame,
    machine_id: str,
    selected_slice: str = None,
    show_positions_only: bool = False
) -> str:
    """
    Create comprehensive hierarchical HTML visualization.

    Shows: Slice overview → (optional) Channel×Position for selected slice → Collapsed socket view

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine to filter
        selected_slice: If provided, show detailed breakdown for this slice
        show_positions_only: If True, show collapsed position view (monitoring mode)

    Returns:
        HTML string with the hierarchical visualization
    """
    if df.empty or 'site' not in df.columns:
        return ""

    # Filter for machine
    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return ""

    # Parse site components
    parsed = df['site'].apply(parse_site_components)
    df['slice_id'] = parsed.apply(lambda x: x['socket_id'])
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])
    df['position_id'] = parsed.apply(lambda x: x['position_id'])

    # Get unique counts
    num_slices = df['slice_id'].nunique()
    num_channels = df['channel_id'].nunique()
    num_positions = df['position_id'].nunique()
    total_volume = df['uin_adj'].sum()
    total_yield = (df['upass_adj'].sum() / df['uin_adj'].sum() * 100) if df['uin_adj'].sum() > 0 else 0

    def get_yield_color(yield_val):
        if yield_val >= 99: return '#00C853'
        elif yield_val >= 97: return '#8BC34A'
        elif yield_val >= 95: return '#FFEB3B'
        elif yield_val >= 90: return '#FF9800'
        else: return '#FF1744'

    def get_text_color(yield_val):
        return '#000' if yield_val >= 95 else '#fff'

    def get_status(yield_val):
        if yield_val >= 99: return ('HEALTHY', '#00C853')
        elif yield_val >= 97: return ('GOOD', '#8BC34A')
        elif yield_val >= 95: return ('WARNING', '#FFB300')
        else: return ('CRITICAL', '#FF1744')

    # Build comprehensive HTML
    html = f'''
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 15px;">
        <!-- Main Header with Hierarchy Info -->
        <div style="
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-radius: 12px;
            padding: 15px 20px;
            margin-bottom: 15px;
            color: #fff;
        ">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                <div>
                    <span style="font-size: 16px; font-weight: 600;">🔧 {machine_id.upper()} - Socket Health</span>
                </div>
                <div style="display: flex; gap: 15px; font-size: 11px; color: #8892b0;">
                    <span style="padding: 4px 10px; background: rgba(255,255,255,0.1); border-radius: 12px;">
                        📊 {num_slices} Slice{'s' if num_slices > 1 else ''}
                    </span>
                    <span style="padding: 4px 10px; background: rgba(255,255,255,0.1); border-radius: 12px;">
                        🔌 {num_channels} Channel{'s' if num_channels > 1 else ''}
                    </span>
                    <span style="padding: 4px 10px; background: rgba(255,255,255,0.1); border-radius: 12px;">
                        🎯 {num_positions} Position{'s' if num_positions > 1 else ''}
                    </span>
                    <span style="padding: 4px 10px; background: rgba(255,255,255,0.1); border-radius: 12px;">
                        📦 {total_volume:,} units
                    </span>
                </div>
            </div>
            <div style="margin-top: 10px; font-size: 12px;">
                Overall Yield: <b style="color: {get_yield_color(total_yield)}; font-size: 16px;">{total_yield:.1f}%</b>
            </div>
        </div>

        <!-- Hierarchy Visualization -->
        <div style="
            background: #f8f9fa;
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 15px;
        ">
            <div style="font-size: 11px; color: #666; margin-bottom: 10px; font-weight: 600;">
                SMT6.1 HIERARCHY: Slice → Channel → Position
            </div>
    '''

    # Aggregate by slice
    slice_data = df.groupby('slice_id').agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum',
        'channel_id': 'nunique',
        'position_id': 'nunique'
    }).reset_index()
    slice_data['yield_pct'] = (slice_data['upass_adj'] / slice_data['uin_adj'] * 100).round(2)
    slice_data = slice_data.sort_values('slice_id', key=lambda x: x.str.extract(r'(\d+)')[0].astype(int))

    # Slice row
    html += '<div style="display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 15px;">'
    for _, row in slice_data.iterrows():
        s_id = row['slice_id']
        s_yield = row['yield_pct']
        s_vol = row['uin_adj']
        s_ch = row['channel_id']
        s_pos = row['position_id']
        bg = get_yield_color(s_yield)
        txt = get_text_color(s_yield)
        status, _ = get_status(s_yield)
        is_selected = selected_slice == s_id
        border = "3px solid #333" if is_selected else "none"

        html += f'''
            <div style="
                background: {bg};
                border-radius: 10px;
                padding: 12px 16px;
                min-width: 120px;
                text-align: center;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                border: {border};
                flex: 1;
            ">
                <div style="font-size: 14px; font-weight: 700; color: {txt};">{s_id}</div>
                <div style="font-size: 24px; font-weight: 800; color: {txt}; margin: 5px 0;">{s_yield:.1f}%</div>
                <div style="font-size: 9px; color: {txt}; opacity: 0.8;">{s_ch}C × {s_pos}P | n={s_vol:,}</div>
                <div style="
                    margin-top: 6px;
                    padding: 2px 8px;
                    background: rgba(255,255,255,0.2);
                    border-radius: 8px;
                    font-size: 8px;
                    font-weight: 600;
                    color: {txt};
                ">{status}</div>
            </div>
        '''
    html += '</div>'

    # If a slice is selected, show its channel breakdown
    if selected_slice and selected_slice in slice_data['slice_id'].values:
        slice_df = df[df['slice_id'] == selected_slice]

        # Aggregate by channel and position for this slice
        ch_data = slice_df.groupby(['channel_id', 'position_id']).agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        ch_data['yield_pct'] = (ch_data['upass_adj'] / ch_data['uin_adj'] * 100).round(2)

        channels = sorted(ch_data['channel_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
        positions = sorted(ch_data['position_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

        ch_lookup = {(r['channel_id'], r['position_id']): r for _, r in ch_data.iterrows()}

        html += f'''
            <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #ddd;">
                <div style="font-size: 12px; color: #333; font-weight: 600; margin-bottom: 10px;">
                    📊 {selected_slice} Detail: Channel × Position
                </div>
                <table style="border-collapse: separate; border-spacing: 3px; width: 100%;">
                    <tr>
                        <th style="padding: 6px; font-size: 10px; color: #666;"></th>
        '''

        for pos in positions:
            html += f'<th style="padding: 6px; font-size: 10px; color: #666; text-align: center;">{pos}</th>'
        html += '</tr>'

        for ch in channels:
            html += f'<tr><td style="padding: 6px; font-size: 10px; color: #666; font-weight: 600;">{ch}</td>'
            for pos in positions:
                key = (ch, pos)
                if key in ch_lookup:
                    d = ch_lookup[key]
                    y = d['yield_pct']
                    v = d['uin_adj']
                    bg = get_yield_color(y)
                    txt = get_text_color(y)
                    warn = "⚠️" if v < 50 else ""
                    html += f'''
                        <td style="
                            background: {bg};
                            color: {txt};
                            padding: 8px 4px;
                            border-radius: 4px;
                            text-align: center;
                            font-size: 11px;
                            font-weight: 600;
                        " title="{selected_slice}{ch}{pos}: {y:.2f}% (n={v})">{y:.1f}%{warn}</td>
                    '''
                else:
                    html += '<td style="background: #eee; border-radius: 4px; text-align: center; color: #ccc;">-</td>'
            html += '</tr>'

        html += '</table></div>'

    html += '''
        </div>

        <!-- Legend -->
        <div style="display: flex; justify-content: center; gap: 12px; font-size: 10px; color: #666; flex-wrap: wrap;">
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #00C853; border-radius: 2px; margin-right: 3px;"></span>≥99%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #8BC34A; border-radius: 2px; margin-right: 3px;"></span>97-99%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FFEB3B; border-radius: 2px; margin-right: 3px;"></span>95-97%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FF9800; border-radius: 2px; margin-right: 3px;"></span>90-95%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FF1744; border-radius: 2px; margin-right: 3px;"></span>&lt;90%</span>
        </div>
    </div>
    '''

    return html


def get_slice_list(df: pd.DataFrame, machine_id: str) -> list[str]:
    """Get list of slices for a machine."""
    if df.empty or 'site' not in df.columns:
        return []

    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return []

    parsed = df['site'].apply(parse_site_components)
    slices = parsed.apply(lambda x: x['socket_id']).unique()
    return sorted(slices, key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)


def create_slice_channel_map_html(df: pd.DataFrame, machine_id: str, selected_slice: str = None) -> str:
    """
    Create HTML visualization showing channels grouped inside each slice.

    Physical SMT6.1 layout: Each Slice (S#) contains Channels (C#) routing to Positions (P##)

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine to filter
        selected_slice: Optional slice ID (e.g., "S0") to show only that slice. None shows all.

    Returns:
        HTML string with slice-based channel map visualization
    """
    if df.empty or 'site' not in df.columns:
        return ""

    # Filter for machine
    df = df[df['machine_id'] == machine_id.lower()].copy()
    if df.empty:
        return ""

    # Parse site components
    parsed = df['site'].apply(parse_site_components)
    df['slice_id'] = parsed.apply(lambda x: x['socket_id'])
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])
    df['position_id'] = parsed.apply(lambda x: x['position_id'])

    # Get unique slices (before filtering)
    all_slices = sorted(df['slice_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

    # Filter to selected slice if specified
    if selected_slice and selected_slice in all_slices:
        df = df[df['slice_id'] == selected_slice]
        slices = [selected_slice]
    else:
        slices = all_slices
    all_channels = sorted(df['channel_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
    all_positions = sorted(df['position_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

    # Calculate totals
    total_volume = df['uin_adj'].sum()
    total_yield = (df['upass_adj'].sum() / df['uin_adj'].sum() * 100) if df['uin_adj'].sum() > 0 else 0

    def get_yield_color(yield_val, volume=None):
        opacity = "CC" if volume and volume < 50 else ""
        if yield_val >= 99: return f'#00C853{opacity}'
        elif yield_val >= 97: return f'#8BC34A{opacity}'
        elif yield_val >= 95: return f'#FFEB3B{opacity}'
        elif yield_val >= 90: return f'#FF9800{opacity}'
        else: return f'#FF1744{opacity}'

    def get_text_color(yield_val):
        return '#000' if yield_val >= 95 else '#fff'

    # Calculate responsive sizing based on number of slices
    num_slices = len(slices)
    if num_slices == 1:
        grid_cols = "1fr"
        box_min_width = "100%"
    elif num_slices == 2:
        grid_cols = "repeat(2, 1fr)"
        box_min_width = "45%"
    elif num_slices == 3:
        grid_cols = "repeat(3, 1fr)"
        box_min_width = "30%"
    else:  # 4 slices
        grid_cols = "repeat(2, 1fr)"  # 2x2 grid for 4 slices
        box_min_width = "45%"

    # Build HTML
    html = f'''
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 15px;">
        <!-- Header -->
        <div style="
            background: linear-gradient(135deg, #1a3a4a 0%, #0d2137 100%);
            border-radius: 12px;
            padding: 15px 20px;
            margin-bottom: 15px;
            color: #fff;
        ">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                <div>
                    <span style="font-size: 16px; font-weight: 600;">🗺️ {machine_id.upper()} - Slice × Channel Map</span>
                    <span style="color: #8ecae6; font-size: 12px; margin-left: 15px;">
                        {num_slices} slice{'s' if num_slices > 1 else ''} | {len(all_channels)} channels | {len(all_positions)} positions
                    </span>
                </div>
                <div style="font-size: 12px;">
                    Overall: <b style="color: {get_yield_color(total_yield)}">{total_yield:.1f}%</b>
                    <span style="color: #8ecae6; margin-left: 10px;">({total_volume:,} units)</span>
                </div>
            </div>
        </div>

        <!-- Slice Boxes Container - Responsive Grid -->
        <div style="display: grid; grid-template-columns: {grid_cols}; gap: 20px;">
    '''

    # Create a box for each slice
    for slice_id in slices:
        slice_df = df[df['slice_id'] == slice_id]

        # Calculate slice stats
        slice_volume = slice_df['uin_adj'].sum()
        slice_yield = (slice_df['upass_adj'].sum() / slice_df['uin_adj'].sum() * 100) if slice_df['uin_adj'].sum() > 0 else 0

        # Get channels and positions for this slice
        slice_channels = sorted(slice_df['channel_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
        slice_positions = sorted(slice_df['position_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)

        # Create lookup for this slice
        slice_lookup = {}
        for _, row in slice_df.groupby(['channel_id', 'position_id']).agg({
            'uin_adj': 'sum', 'upass_adj': 'sum'
        }).reset_index().iterrows():
            y = (row['upass_adj'] / row['uin_adj'] * 100) if row['uin_adj'] > 0 else 0
            slice_lookup[(row['channel_id'], row['position_id'])] = {
                'yield': y, 'volume': row['uin_adj']
            }

        # Slice box border color based on yield
        box_border = get_yield_color(slice_yield).replace('CC', '')

        html += f'''
            <div style="
                background: #fff;
                border: 3px solid {box_border};
                border-radius: 12px;
                padding: 15px;
                width: 100%;
                box-sizing: border-box;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            ">
                <!-- Slice Header -->
                <div style="
                    background: {box_border};
                    color: {'#000' if slice_yield >= 95 else '#fff'};
                    border-radius: 8px;
                    padding: 12px 18px;
                    margin-bottom: 15px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                ">
                    <div>
                        <span style="font-size: 18px; font-weight: 700;">{slice_id}</span>
                        <span style="font-size: 12px; opacity: 0.85; margin-left: 10px;">
                            {len(slice_channels)}C × {len(slice_positions)}P
                        </span>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 24px; font-weight: 800;">{slice_yield:.1f}%</div>
                        <div style="font-size: 10px; opacity: 0.7;">n={slice_volume:,}</div>
                    </div>
                </div>

                <!-- Channel × Position Grid -->
                <table style="border-collapse: separate; border-spacing: 4px; width: 100%;">
                    <tr>
                        <th style="padding: 6px; font-size: 10px; color: #666;"></th>
        '''

        # Responsive cell sizing based on number of slices
        if num_slices == 1:
            cell_padding = "10px 8px"
            cell_font = "14px"
            vol_font = "10px"
            header_font = "11px"
        elif num_slices == 2:
            cell_padding = "8px 6px"
            cell_font = "13px"
            vol_font = "9px"
            header_font = "10px"
        else:
            cell_padding = "6px 4px"
            cell_font = "11px"
            vol_font = "8px"
            header_font = "10px"

        # Position headers
        for pos in slice_positions:
            html += f'<th style="padding: 6px; font-size: {header_font}; color: #666; text-align: center;">{pos}</th>'
        html += '</tr>'

        # Channel rows
        for ch in slice_channels:
            html += f'<tr><td style="padding: 6px; font-size: {header_font}; color: #666; font-weight: 600;">{ch}</td>'
            for pos in slice_positions:
                key = (ch, pos)
                if key in slice_lookup:
                    data = slice_lookup[key]
                    y = data['yield']
                    v = int(data['volume'])
                    bg = get_yield_color(y, v)
                    txt = get_text_color(y)
                    # Format volume: show K for thousands
                    if v >= 1000:
                        vol_str = f"{v/1000:.1f}K"
                    else:
                        vol_str = str(v)
                    # Low volume indicator
                    opacity = "0.7" if v < 100 else "1"

                    html += f'''
                        <td style="
                            background: {bg};
                            color: {txt};
                            padding: {cell_padding};
                            border-radius: 5px;
                            text-align: center;
                            opacity: {opacity};
                        " title="{slice_id}{ch}{pos}: {y:.1f}% (n={v:,})">
                            <div style="font-size: {cell_font}; font-weight: 700; line-height: 1.2;">{y:.0f}%</div>
                            <div style="font-size: {vol_font}; opacity: 0.8;">n={vol_str}</div>
                        </td>
                    '''
                else:
                    html += f'<td style="background: #f5f5f5; border-radius: 5px; text-align: center; color: #ddd; padding: {cell_padding};">-</td>'
            html += '</tr>'

        html += '''
                </table>
            </div>
        '''

    html += '''
        </div>

        <!-- Legend -->
        <div style="margin-top: 15px; display: flex; justify-content: center; gap: 12px; font-size: 10px; color: #666; flex-wrap: wrap;">
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #00C853; border-radius: 2px; margin-right: 3px;"></span>≥99%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #8BC34A; border-radius: 2px; margin-right: 3px;"></span>97-99%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FFEB3B; border-radius: 2px; margin-right: 3px;"></span>95-97%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FF9800; border-radius: 2px; margin-right: 3px;"></span>90-95%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FF1744; border-radius: 2px; margin-right: 3px;"></span>&lt;90%</span>
            <span style="margin-left: 10px;">⚠ = low volume (&lt;50)</span>
        </div>

        <div style="margin-top: 10px; font-size: 10px; color: #888; text-align: center;">
            Each box = 1 Slice (TSB electronics). Channels = COBRA3 address buses. Positions = Physical sockets.
        </div>
    </div>
    '''

    return html


def create_site_summary_table(df: pd.DataFrame, machine_id: str, dark_mode: bool = True) -> str:
    """
    Create HTML table showing site-level yields for a specific machine.

    Args:
        df: DataFrame with site-level yield data
        machine_id: Machine ID to filter
        dark_mode: If True, use dark mode colors

    Returns:
        HTML string for the site summary table
    """
    if df.empty or 'site' not in df.columns:
        return ""

    # Filter by machine
    machine_df = df[df['machine_id'] == machine_id.lower()]

    if machine_df.empty:
        return ""

    # Aggregate by site
    summary = machine_df.groupby('site').agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum',
        'workweek': 'nunique'
    }).reset_index()

    summary['yield_pct'] = (summary['upass_adj'] / summary['uin_adj'] * 100).round(2)
    summary = summary.sort_values('yield_pct', ascending=True)  # Worst first

    # Style colors
    bg_color = '#2d2d2d' if dark_mode else '#ffffff'
    text_color = '#ffffff' if dark_mode else '#333333'
    header_bg = '#3d3d3d' if dark_mode else '#f5f5f5'
    border_color = '#555555' if dark_mode else '#dddddd'

    def get_row_color(yield_val):
        if yield_val >= 99.0:
            return '#1a472a' if dark_mode else '#d4edda'  # Green
        elif yield_val >= 98.0:
            return '#1a3a4a' if dark_mode else '#d1ecf1'  # Cyan
        elif yield_val >= 96.0:
            return '#4a4a1a' if dark_mode else '#fff3cd'  # Yellow
        else:
            return '#4a1a1a' if dark_mode else '#f8d7da'  # Red

    html = f'''
    <div style="margin-bottom: 20px;">
        <h4 style="color: {text_color}; margin-bottom: 10px;">{machine_id.upper()} - Site Breakdown</h4>
        <table style="border-collapse: collapse; width: 100%; font-size: 11px; font-family: Arial, sans-serif; background-color: {bg_color};">
            <thead>
                <tr style="background-color: {header_bg};">
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: left; color: {text_color};">Site</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">UIN</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">UPASS</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">Yield</th>
                    <th style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">Weeks</th>
                </tr>
            </thead>
            <tbody>
    '''

    for _, row in summary.iterrows():
        row_bg = get_row_color(row['yield_pct'])
        html += f'''
            <tr style="background-color: {row_bg};">
                <td style="border: 1px solid {border_color}; padding: 6px; color: {text_color};">{row['site']}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{int(row['uin_adj']):,}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{int(row['upass_adj']):,}</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color}; font-weight: bold;">{row['yield_pct']:.2f}%</td>
                <td style="border: 1px solid {border_color}; padding: 6px; text-align: right; color: {text_color};">{int(row['workweek'])}</td>
            </tr>
        '''

    html += f'''
            </tbody>
        </table>
        <p style="font-size: 10px; color: {'#888888' if dark_mode else '#666666'}; margin-top: 5px;">Sorted by yield (worst first) | Color: Green ≥99%, Cyan ≥98%, Yellow ≥96%, Red &lt;96%</p>
    </div>
    '''

    return html


# ============================================================================
# SITE TREND ANALYSIS FUNCTIONS
# ============================================================================

def analyze_site_trends(df: pd.DataFrame, target_yield: float = 99.0) -> pd.DataFrame:
    """
    Analyze site yield trends over multiple weeks.

    Args:
        df: DataFrame with site-level yield data (multiple weeks)
        target_yield: Target yield percentage

    Returns:
        DataFrame with trend analysis per site
    """
    if df.empty or 'site' not in df.columns:
        return pd.DataFrame()

    # Need at least 2 weeks for trend analysis
    weeks = sorted(df['workweek'].unique())
    if len(weeks) < 2:
        return pd.DataFrame()

    results = []

    for site in df['site'].unique():
        site_df = df[df['site'] == site].sort_values('workweek')

        if len(site_df) < 2:
            continue

        # Calculate metrics
        yields = site_df['yield_pct'].values
        avg_yield = yields.mean()
        min_yield = yields.min()
        max_yield = yields.max()
        std_yield = yields.std() if len(yields) > 1 else 0
        latest_yield = yields[-1]
        first_yield = yields[0]

        # Trend calculation (simple linear)
        if len(yields) >= 2:
            trend_slope = (yields[-1] - yields[0]) / (len(yields) - 1)
        else:
            trend_slope = 0

        # Classify trend
        if std_yield > 5:
            trend_class = "VOLATILE"
            trend_icon = "🔶"
        elif avg_yield >= target_yield and min_yield >= target_yield - 1:
            trend_class = "STABLE_GOOD"
            trend_icon = "🟢"
        elif avg_yield < target_yield - 2:
            trend_class = "STABLE_BAD"
            trend_icon = "🔴"
        elif trend_slope > 0.5:
            trend_class = "IMPROVING"
            trend_icon = "📈"
        elif trend_slope < -0.5:
            trend_class = "DEGRADING"
            trend_icon = "📉"
        else:
            trend_class = "STABLE"
            trend_icon = "➖"

        # Check for sudden drops
        sudden_drop = False
        drop_week = None
        for i in range(1, len(yields)):
            if yields[i-1] - yields[i] > 5:  # 5% drop
                sudden_drop = True
                drop_week = site_df.iloc[i]['workweek']
                break

        results.append({
            'site': site,
            'machine_id': site_df['machine_id'].iloc[0],
            'weeks_data': len(site_df),
            'avg_yield': round(avg_yield, 2),
            'min_yield': round(min_yield, 2),
            'max_yield': round(max_yield, 2),
            'std_yield': round(std_yield, 2),
            'latest_yield': round(latest_yield, 2),
            'trend_slope': round(trend_slope, 3),
            'trend_class': trend_class,
            'trend_icon': trend_icon,
            'sudden_drop': sudden_drop,
            'drop_week': drop_week
        })

    return pd.DataFrame(results)


def create_site_trend_heatmap(
    df: pd.DataFrame,
    machine_id: str = None
) -> Optional[go.Figure]:
    """
    Create a heatmap showing site yield over time (weeks × sites).

    Args:
        df: DataFrame with site-level yield data (multiple weeks)
        machine_id: Optional filter for specific machine

    Returns:
        Plotly Figure object
    """
    if df.empty or 'site' not in df.columns:
        return None

    # Filter by machine if specified
    if machine_id:
        df = df[df['machine_id'] == machine_id.lower()]

    if df.empty:
        return None

    # Pivot: rows=sites, columns=workweeks, values=yield
    pivot = df.pivot_table(
        index='site',
        columns='workweek',
        values='yield_pct',
        aggfunc='mean'
    )

    if pivot.empty:
        return None

    # Sort columns (workweeks) and rows (sites)
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)
    pivot = pivot.sort_index()

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=[str(ww) for ww in pivot.columns],
        y=pivot.index,
        colorscale=[
            [0, '#dc3545'],      # Red - low yield
            [0.5, '#ffc107'],    # Yellow - medium
            [0.8, '#17a2b8'],    # Cyan - good
            [1, '#28a745']       # Green - excellent
        ],
        zmin=90,
        zmax=100,
        text=[[f"{v:.1f}%" if pd.notna(v) else "-" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont={"size": 9, "color": "#1a1a1a"},
        hovertemplate=(
            "<b>Site:</b> %{y}<br>"
            "<b>Week:</b> %{x}<br>"
            "<b>Yield:</b> %{z:.2f}%<br>"
            "<extra></extra>"
        ),
        colorbar=dict(
            title=dict(text="Yield %", font=dict(color="#1a1a1a")),
            tickfont=dict(color="#1a1a1a")
        )
    ))

    title = f"Site Yield Trend - {machine_id.upper()}" if machine_id else "Site Yield Trend Over Time"

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(color="#1a1a1a", size=16)
        ),
        xaxis=dict(
            title=dict(text="Work Week", font=dict(color="#1a1a1a")),
            tickfont=dict(color="#1a1a1a"),
            type='category'
        ),
        yaxis=dict(
            title=dict(text="Site", font=dict(color="#1a1a1a")),
            tickfont=dict(color="#1a1a1a", size=9),
            autorange='reversed'
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(248,249,250,0.95)',
        height=max(400, len(pivot.index) * 20 + 100)
    )

    return fig


def create_site_trend_summary_html(trend_df: pd.DataFrame) -> str:
    """
    Create HTML summary table for site trend analysis.

    Args:
        trend_df: DataFrame from analyze_site_trends()

    Returns:
        HTML string
    """
    if trend_df.empty:
        return ""

    # Count by trend class
    trend_counts = trend_df['trend_class'].value_counts().to_dict()

    # Sort by trend priority (worst first)
    priority = {'STABLE_BAD': 0, 'DEGRADING': 1, 'VOLATILE': 2, 'STABLE': 3, 'IMPROVING': 4, 'STABLE_GOOD': 5}
    trend_df = trend_df.sort_values('trend_class', key=lambda x: x.map(priority))

    # Filter to show problem sites
    problem_sites = trend_df[trend_df['trend_class'].isin(['STABLE_BAD', 'DEGRADING', 'VOLATILE'])]

    html = f'''
    <div style="margin-bottom: 20px; font-family: -apple-system, BlinkMacSystemFont, sans-serif;">
        <h4 style="color: #1a1a1a; margin-bottom: 10px;">📊 Site Trend Analysis</h4>

        <div style="display: flex; gap: 15px; margin-bottom: 15px; flex-wrap: wrap;">
            <span style="padding: 5px 10px; background: #d4edda; border-radius: 5px; font-size: 12px;">
                🟢 Stable Good: {trend_counts.get('STABLE_GOOD', 0)}
            </span>
            <span style="padding: 5px 10px; background: #d1ecf1; border-radius: 5px; font-size: 12px;">
                📈 Improving: {trend_counts.get('IMPROVING', 0)}
            </span>
            <span style="padding: 5px 10px; background: #e2e3e5; border-radius: 5px; font-size: 12px;">
                ➖ Stable: {trend_counts.get('STABLE', 0)}
            </span>
            <span style="padding: 5px 10px; background: #fff3cd; border-radius: 5px; font-size: 12px;">
                🔶 Volatile: {trend_counts.get('VOLATILE', 0)}
            </span>
            <span style="padding: 5px 10px; background: #ffeeba; border-radius: 5px; font-size: 12px;">
                📉 Degrading: {trend_counts.get('DEGRADING', 0)}
            </span>
            <span style="padding: 5px 10px; background: #f8d7da; border-radius: 5px; font-size: 12px;">
                🔴 Stable Bad: {trend_counts.get('STABLE_BAD', 0)}
            </span>
        </div>
    '''

    if not problem_sites.empty:
        html += '''
        <h5 style="color: #dc3545; margin: 15px 0 10px 0;">⚠️ Sites Needing Attention</h5>
        <table style="border-collapse: collapse; width: 100%; font-size: 11px; background: #fff;">
            <thead>
                <tr style="background: #f8f9fa;">
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: left;">Site</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: left;">Machine</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: center;">Trend</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">Avg Yield</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">Latest</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">Min</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">Std Dev</th>
                    <th style="border: 1px solid #dee2e6; padding: 6px; text-align: left;">Issue</th>
                </tr>
            </thead>
            <tbody>
        '''

        for _, row in problem_sites.head(15).iterrows():
            bg_color = '#f8d7da' if row['trend_class'] == 'STABLE_BAD' else '#fff3cd'
            issue = ""
            if row['sudden_drop']:
                issue = f"Drop in WW{row['drop_week']}"
            elif row['trend_class'] == 'STABLE_BAD':
                issue = "Chronic low yield"
            elif row['trend_class'] == 'DEGRADING':
                issue = "Downward trend"
            elif row['trend_class'] == 'VOLATILE':
                issue = f"High variance (σ={row['std_yield']:.1f})"

            html += f'''
                <tr style="background: {bg_color};">
                    <td style="border: 1px solid #dee2e6; padding: 6px;">{row['site']}</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px;">{row['machine_id'].upper()}</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px; text-align: center;">{row['trend_icon']}</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">{row['avg_yield']:.1f}%</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">{row['latest_yield']:.1f}%</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">{row['min_yield']:.1f}%</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px; text-align: right;">{row['std_yield']:.1f}</td>
                    <td style="border: 1px solid #dee2e6; padding: 6px;">{issue}</td>
                </tr>
            '''

        html += '''
            </tbody>
        </table>
        '''
    else:
        html += '<p style="color: #28a745;">✓ All sites are performing within acceptable range.</p>'

    html += '</div>'
    return html


# ============================================================================
# SITE HEALTH SUMMARY - Lowest 5 sockets per machine
# ============================================================================

def create_site_channel_summary_html(
    df: pd.DataFrame,
    max_issues: int = 5
) -> str:
    """
    Create a compact summary showing the lowest 5 yielding sockets per machine.

    Shows:
    - Lowest 5 sockets per machine (sorted by yield)
    - Color coded by yield severity
    - Deterioration trend and WW details for multi-week data

    Args:
        df: DataFrame with site-level yield data (may include multiple weeks)
        max_issues: Maximum sockets to show per machine (default 5)

    Returns:
        HTML string with compact summary
    """
    if df.empty or 'site' not in df.columns:
        return ""

    df = df.copy()
    has_multi_weeks = 'workweek' in df.columns and df['workweek'].nunique() > 1

    # Parse site components
    parsed = df['site'].apply(parse_site_components)
    df['channel_id'] = parsed.apply(lambda x: x['channel_id'])
    df['position_id'] = parsed.apply(lambda x: x['position_id'])

    # Calculate weekly yields for trend analysis (if multi-week data)
    weekly_socket_yields = None
    if has_multi_weeks:
        weekly_socket_yields = df.groupby(['machine_id', 'site', 'workweek']).agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        weekly_socket_yields['yield_pct'] = (weekly_socket_yields['upass_adj'] / weekly_socket_yields['uin_adj'] * 100).round(2)

    # Aggregate by machine and socket (cumulative)
    socket_data = df.groupby(['machine_id', 'channel_id', 'position_id', 'site']).agg({
        'uin_adj': 'sum',
        'upass_adj': 'sum'
    }).reset_index()
    socket_data['yield_pct'] = (socket_data['upass_adj'] / socket_data['uin_adj'] * 100).round(2)

    # Calculate statistics
    yields = socket_data['yield_pct']
    median_yield = yields.median()
    min_yield = yields.min()
    max_yield = yields.max()
    total_sockets = len(socket_data)

    # Get unique machines
    machines = sorted(socket_data['machine_id'].unique())

    # Check if all sockets are healthy (above 95%)
    if min_yield >= 95:
        ww_range = ""
        if has_multi_weeks:
            wws = sorted(df['workweek'].unique())
            ww_range = f" (WW{wws[0]}-{wws[-1]})"
        return f'''
        <div style="background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%);
                    border-radius: 8px; padding: 10px; display: flex; align-items: center; gap: 10px;">
            <span style="font-size: 18px;">✅</span>
            <span style="font-size: 12px; font-weight: 600; color: #fff;">All {total_sockets} Sites Healthy{ww_range}</span>
            <span style="font-size: 10px; color: #c8e6c9;">Median: {median_yield:.1f}% | Range: {min_yield:.0f}-{max_yield:.0f}%</span>
        </div>
        '''

    # Helper: Find detailed trend info for a socket
    def get_deterioration_info(machine_id: str, site: str) -> dict:
        """Find when socket started failing, trend direction, and yield trajectory."""
        if weekly_socket_yields is None:
            return {'start_ww': None, 'trend': None, 'weeks_bad': 0, 'total_weeks': 0,
                    'first_yield': None, 'last_yield': None, 'min_yield': None}

        site_weekly = weekly_socket_yields[
            (weekly_socket_yields['machine_id'] == machine_id) &
            (weekly_socket_yields['site'] == site)
        ].sort_values('workweek')

        if site_weekly.empty:
            return {'start_ww': None, 'trend': None, 'weeks_bad': 0, 'total_weeks': 0,
                    'first_yield': None, 'last_yield': None, 'min_yield': None}

        total_weeks = len(site_weekly)
        first_yield = site_weekly.iloc[0]['yield_pct']
        last_yield = site_weekly.iloc[-1]['yield_pct']
        min_yield_val = site_weekly['yield_pct'].min()

        # Find first week below 95%
        bad_weeks = site_weekly[site_weekly['yield_pct'] < 95]
        if bad_weeks.empty:
            return {'start_ww': None, 'trend': None, 'weeks_bad': 0, 'total_weeks': total_weeks,
                    'first_yield': first_yield, 'last_yield': last_yield, 'min_yield': min_yield_val}

        first_bad = bad_weeks['workweek'].min()
        weeks_bad = len(bad_weeks)

        # Calculate trend (compare first half vs second half)
        if total_weeks >= 2:
            first_half = site_weekly.head(total_weeks // 2)['yield_pct'].mean()
            second_half = site_weekly.tail(total_weeks // 2)['yield_pct'].mean()
            diff = second_half - first_half
            if diff < -2:
                trend = '📉'  # Declining
            elif diff > 2:
                trend = '📈'  # Improving
            else:
                trend = '➡️'  # Stable
        else:
            trend = '➡️'

        return {
            'start_ww': str(first_bad)[-2:],
            'trend': trend,
            'weeks_bad': weeks_bad,
            'total_weeks': total_weeks,
            'first_yield': first_yield,
            'last_yield': last_yield,
            'min_yield': min_yield_val
        }

    # Helper: Get background color based on yield (no green for healthy)
    def get_yield_color(y: float) -> tuple:
        """Return (background_color, text_color) based on yield."""
        if y < 50:
            return '#b71c1c', '#fff'  # Dark red - Critical
        elif y < 70:
            return '#c62828', '#fff'  # Red - Severe
        elif y < 85:
            return '#ef6c00', '#fff'  # Orange - Poor
        elif y < 95:
            return '#f9a825', '#000'  # Yellow - Warning
        else:
            return '#37474f', '#fff'  # Dark gray - OK (no green)

    # Machine header color - follows dashboard theme
    header_bg = '#1a237e'  # Dark indigo - matches dashboard theme

    # Get WW range for header
    ww_info = ""
    if has_multi_weeks:
        wws = sorted(df['workweek'].unique())
        ww_info = f" | WW{str(wws[0])[-2:]}-{str(wws[-1])[-2:]}"

    # Count sockets below 95%
    poor_count = len(socket_data[socket_data['yield_pct'] < 95])

    # Build HTML
    html = f'''<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:6px;padding:8px;">
<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;padding-bottom:4px;border-bottom:1px solid rgba(255,255,255,0.1);">
<span style="font-size:11px;font-weight:600;color:#fff;">📊 Lowest 5 Yield Sockets by Machine{ww_info}</span>
<div style="display:flex;gap:4px;margin-left:auto;font-size:8px;">
<span style="background:#ef6c00;color:#fff;padding:1px 5px;border-radius:6px;">⚠️ {poor_count} below 95%</span>
</div></div>
<div style="display:flex;flex-wrap:wrap;gap:8px;">'''

    # Create a column for each machine (only show machines with critical sockets)
    machines_with_issues = []
    for idx, machine_id in enumerate(machines):
        machine_sockets = socket_data[socket_data['machine_id'] == machine_id]
        # Only show sockets below 95% (critical)
        critical_sockets = machine_sockets[machine_sockets['yield_pct'] < 95].sort_values('yield_pct').head(max_issues)

        if critical_sockets.empty:
            continue  # Skip machines with no critical sockets

        machines_with_issues.append(machine_id)
        machine_name = machine_id.upper()
        machine_min = machine_sockets['yield_pct'].min()

        html += f'''<div style="flex:1;min-width:180px;max-width:250px;">
<div style="background:{header_bg};padding:4px 8px;border-radius:4px 4px 0 0;">
<span style="font-size:10px;font-weight:600;color:#fff;">{machine_name}</span>
<span style="font-size:9px;color:rgba(255,255,255,0.8);float:right;">Min: {machine_min:.0f}%</span>
</div>
<div style="background:rgba(0,0,0,0.2);border-radius:0 0 4px 4px;padding:4px;">'''

        for _, row in critical_sockets.iterrows():
            site = row['site']
            y = row['yield_pct']
            uin = int(row['uin_adj'])
            ufail = uin - int(row['upass_adj'])
            bg, text = get_yield_color(y)

            # Get trend info for multi-week data
            trend_detail = ""
            if has_multi_weeks:
                info = get_deterioration_info(machine_id, site)
                if info['start_ww']:
                    trajectory = f"{info['first_yield']:.0f}→{info['last_yield']:.0f}%" if info['first_yield'] is not None else ""
                    weeks_info = f"{info['weeks_bad']}/{info['total_weeks']}wks" if info['total_weeks'] > 1 else ""
                    trend_detail = f"{info['trend']} {trajectory} | {weeks_info} bad from WW{info['start_ww']}"

            html += f'''<div style="background:{bg};border-radius:3px;padding:3px 6px;margin-bottom:2px;">
<div style="display:flex;justify-content:space-between;align-items:center;">
<span style="font-size:8px;color:{text};font-weight:500;">{site}</span>
<span style="font-size:9px;font-weight:700;color:{text};">{y:.0f}%</span>
</div>
<div style="font-size:7px;color:{text};opacity:0.8;">UIN:{uin} | Fail:{ufail}</div>'''
            if trend_detail:
                detail_color = 'rgba(255,255,255,0.9)' if text == '#fff' else 'rgba(0,0,0,0.8)'
                html += f'<div style="font-size:7px;color:{detail_color};font-style:italic;">{trend_detail}</div>'
            html += '</div>'

        html += '</div></div>'

    # If no machines have issues, show all healthy message
    if not machines_with_issues:
        return f'''
        <div style="background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%);
                    border-radius: 8px; padding: 10px; display: flex; align-items: center; gap: 10px;">
            <span style="font-size: 18px;">✅</span>
            <span style="font-size: 12px; font-weight: 600; color: #fff;">All {total_sockets} Sites Healthy{ww_info}</span>
            <span style="font-size: 10px; color: #c8e6c9;">All sockets ≥95%</span>
        </div>
        '''

    html += '</div>'

    # Add legend (only critical categories, no OK)
    html += '''<div style="margin-top:6px;padding-top:5px;border-top:1px solid rgba(255,255,255,0.1);display:flex;gap:8px;flex-wrap:wrap;font-size:7px;color:#888;">
<span><span style="background:#b71c1c;color:#fff;padding:1px 4px;border-radius:2px;">&lt;50%</span> Critical</span>
<span><span style="background:#c62828;color:#fff;padding:1px 4px;border-radius:2px;">50-70%</span> Severe</span>
<span><span style="background:#ef6c00;color:#fff;padding:1px 4px;border-radius:2px;">70-85%</span> Poor</span>
<span><span style="background:#f9a825;color:#000;padding:1px 4px;border-radius:2px;">85-95%</span> Warning</span>
</div></div>'''

    return html


def create_multi_machine_socket_grid(
    df: pd.DataFrame,
    machine_ids: list[str],
    title: str = "Socket Health"
) -> str:
    """
    Create a single combined HTML panel showing socket health for multiple machines side-by-side.

    Args:
        df: DataFrame with site-level yield data for all machines
        machine_ids: List of machine IDs to display
        title: Section title

    Returns:
        HTML string with all machines' socket grids in a single panel
    """
    if df.empty or not machine_ids:
        return ""

    def get_yield_color(yield_val):
        if yield_val >= 99:
            return '#00C853'
        elif yield_val >= 97:
            return '#8BC34A'
        elif yield_val >= 95:
            return '#FFEB3B'
        elif yield_val >= 90:
            return '#FF9800'
        else:
            return '#FF1744'

    def get_text_color(yield_val):
        return '#000' if yield_val >= 95 else '#fff'

    num_machines = len(machine_ids)
    # Calculate flex basis for equal distribution
    flex_basis = f"{100 // num_machines}%"

    html = f'''
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 10px;">
        <div style="display: flex; gap: 15px; justify-content: center; flex-wrap: wrap;">
    '''

    for machine_id in machine_ids:
        machine_df = df[df['machine_id'] == machine_id.lower()].copy()
        if machine_df.empty:
            continue

        # Parse site components
        parsed = machine_df['site'].apply(parse_site_components)
        machine_df['position_id'] = parsed.apply(lambda x: x['position_id'])

        # Aggregate by socket position
        socket_data = machine_df.groupby('position_id').agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        socket_data['yield_pct'] = (socket_data['upass_adj'] / socket_data['uin_adj'] * 100).round(2)
        socket_data['volume'] = socket_data['uin_adj']

        # Calculate summary
        total_volume = socket_data['volume'].sum()
        avg_yield = (socket_data['upass_adj'].sum() / socket_data['uin_adj'].sum() * 100) if socket_data['uin_adj'].sum() > 0 else 0

        socket_lookup = {row['position_id']: row for _, row in socket_data.iterrows()}
        positions = sorted(socket_data['position_id'].unique(), key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
        num_sockets = len(positions)

        # Determine grid columns
        grid_cols = 2 if num_sockets <= 4 else 4

        # Machine card
        html += f'''
        <div style="flex: 1 1 {flex_basis}; min-width: 280px; max-width: 450px;
                    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                    border-radius: 12px; padding: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.2);">
            <!-- Machine Header -->
            <div style="text-align: center; margin-bottom: 10px; padding-bottom: 8px; border-bottom: 1px solid rgba(255,255,255,0.1);">
                <div style="font-size: 15px; font-weight: 700; color: #fff;">🔧 {machine_id.upper()}</div>
                <div style="font-size: 11px; color: #8892b0; margin-top: 3px;">
                    Avg: <span style="color: {get_yield_color(avg_yield)}; font-weight: 600;">{avg_yield:.1f}%</span> |
                    {num_sockets} sockets | {total_volume:,} units
                </div>
            </div>
            <!-- Socket Grid -->
            <div style="display: grid; grid-template-columns: repeat({grid_cols}, 1fr); gap: 8px;">
        '''

        # Add socket cells
        physical_order = ['P00', 'P01', 'P02', 'P03', 'P04', 'P05', 'P06', 'P07', 'P08', 'P09', 'P10', 'P11']
        for pos in physical_order:
            if pos in socket_lookup:
                data = socket_lookup[pos]
                yield_val = data['yield_pct']
                volume = data['volume']
                bg_color = get_yield_color(yield_val)
                text_color = get_text_color(yield_val)

                html += f'''
                <div style="background: {bg_color}; border-radius: 8px; padding: 10px; text-align: center;
                            box-shadow: 0 2px 6px rgba(0,0,0,0.15);"
                     title="{pos}: {yield_val:.2f}% ({volume:,} units)">
                    <div style="font-size: 12px; font-weight: 700; color: {text_color};">{pos}</div>
                    <div style="font-size: 18px; font-weight: 800; color: {text_color};">{yield_val:.1f}%</div>
                    <div style="font-size: 9px; color: {text_color}; opacity: 0.8;">n={volume:,}</div>
                </div>
                '''

        html += '''
            </div>
        </div>
        '''

    # Add shared legend
    html += '''
        </div>
        <div style="display: flex; justify-content: center; gap: 12px; margin-top: 12px; font-size: 10px; color: #888;">
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #00C853; border-radius: 2px; margin-right: 3px;"></span>≥99%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #8BC34A; border-radius: 2px; margin-right: 3px;"></span>97-99%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FFEB3B; border-radius: 2px; margin-right: 3px;"></span>95-97%</span>
            <span><span style="display: inline-block; width: 10px; height: 10px; background: #FF1744; border-radius: 2px; margin-right: 3px;"></span>&lt;95%</span>
        </div>
    </div>
    '''

    return html
