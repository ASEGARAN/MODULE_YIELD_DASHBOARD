"""
Web Fail Viewer - Plotly-based visualization

Recreates the functionality of Micron's Fail Viewer tool
as an interactive web component for the Module Yield Dashboard.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Tuple

from .utils import (
    load_geometry,
    load_fail_csv,
    process_fail_data,
    generate_bank_grid,
    generate_bank_labels,
    get_did_info,
    get_bank_group,
    generate_fail_summary
)


# Color scheme for DQs (similar to original viewer)
DQ_COLORS = [
    '#FF0000',  # DQ0 - Red
    '#00FF00',  # DQ1 - Green
    '#0000FF',  # DQ2 - Blue
    '#FFFF00',  # DQ3 - Yellow
    '#FF00FF',  # DQ4 - Magenta
    '#00FFFF',  # DQ5 - Cyan
    '#FFA500',  # DQ6 - Orange
    '#800080',  # DQ7 - Purple
    '#008000',  # DQ8 - Dark Green
    '#000080',  # DQ9 - Navy
    '#808000',  # DQ10 - Olive
    '#800000',  # DQ11 - Maroon
    '#008080',  # DQ12 - Teal
    '#C0C0C0',  # DQ13 - Silver
    '#FF6347',  # DQ14 - Tomato
    '#4682B4',  # DQ15 - Steel Blue
]


def create_fail_viewer(
    fail_data: pd.DataFrame,
    part_type: str = 'y62p',
    title: str = 'Fail Viewer',
    show_bank_grid: bool = True,
    show_bank_labels: bool = True,
    color_by: str = 'dq',
    marker_size: int = 3,
    width: int = 1000,
    height: int = 800
) -> go.Figure:
    """
    Create an interactive fail viewer plot.

    Args:
        fail_data: DataFrame with row, col, dq columns (or already processed with phys_x, phys_y)
        part_type: Part type for geometry ('y62p', 'y6cp', 'y63n')
        title: Plot title
        show_bank_grid: Whether to show bank boundary grid
        show_bank_labels: Whether to show bank number labels
        color_by: Column to color by ('dq', 'bank', or None for single color)
        marker_size: Size of fail markers
        width: Plot width in pixels
        height: Plot height in pixels

    Returns:
        Plotly Figure object
    """
    # Load geometry
    geometry = load_geometry(part_type)

    # Process data if not already processed
    if 'phys_x' not in fail_data.columns:
        df = process_fail_data(fail_data, geometry)
    else:
        df = fail_data.copy()

    # Create figure
    fig = go.Figure()

    # Ensure bank_group column exists
    if 'bank_group' not in df.columns and 'bank' in df.columns:
        df['bank_group'] = df['bank'].apply(get_bank_group)

    # Get DID info for title
    did_info = get_did_info(part_type)

    # Add fail points
    if color_by == 'dq' and 'dq' in df.columns:
        # Color by DQ
        for dq in sorted(df['dq'].unique()):
            dq_data = df[df['dq'] == dq]
            color = DQ_COLORS[int(dq) % len(DQ_COLORS)]
            fig.add_trace(go.Scattergl(
                x=dq_data['phys_x'],
                y=dq_data['phys_y'],
                mode='markers',
                marker=dict(size=marker_size, color=color),
                name=f'DQ{int(dq)}',
                hovertemplate=(
                    '<b>Fail Address</b><br>'
                    'Row: %{customdata[0]:,}<br>'
                    'Col: %{customdata[1]:,}<br>'
                    'DQ: %{customdata[2]}<br>'
                    'Bank: B%{customdata[3]} (BG%{customdata[4]})'
                    '<extra></extra>'
                ),
                customdata=dq_data[['row', 'col', 'dq', 'bank', 'bank_group']].values
            ))
    elif color_by == 'bank' and 'bank' in df.columns:
        # Color by bank
        for bank in sorted(df['bank'].unique()):
            bank_data = df[df['bank'] == bank]
            bg = get_bank_group(int(bank))
            fig.add_trace(go.Scattergl(
                x=bank_data['phys_x'],
                y=bank_data['phys_y'],
                mode='markers',
                marker=dict(size=marker_size),
                name=f'B{int(bank)} (BG{bg})',
                hovertemplate=(
                    '<b>Fail Address</b><br>'
                    'Row: %{customdata[0]:,}<br>'
                    'Col: %{customdata[1]:,}<br>'
                    'DQ: %{customdata[2]}<br>'
                    'Bank: B%{customdata[3]} (BG%{customdata[4]})'
                    '<extra></extra>'
                ),
                customdata=bank_data[['row', 'col', 'dq', 'bank', 'bank_group']].values
            ))
    else:
        # Single color (red dots as per Fail Viewer convention)
        fig.add_trace(go.Scattergl(
            x=df['phys_x'],
            y=df['phys_y'],
            mode='markers',
            marker=dict(size=marker_size, color='red'),
            name='Fails',
            hovertemplate=(
                '<b>Fail Address</b><br>'
                'Row: %{customdata[0]:,}<br>'
                'Col: %{customdata[1]:,}<br>'
                'DQ: %{customdata[2]}<br>'
                'Bank: B%{customdata[3]} (BG%{customdata[4]})'
                '<extra></extra>'
            ),
            customdata=df[['row', 'col', 'dq', 'bank', 'bank_group']].values
        ))

    # Add bank grid
    if show_bank_grid:
        shapes = generate_bank_grid(geometry)
        fig.update_layout(shapes=shapes)

    # Add bank labels
    if show_bank_labels:
        annotations = generate_bank_labels(geometry)
        fig.update_layout(annotations=annotations)

    # Update layout
    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    bank_x = getattr(geometry, 'BANK_X', 2)
    bank_y = getattr(geometry, 'BANK_Y', 8)

    # Enhanced title with DID info
    did_info = get_did_info(part_type)
    full_title = f"{title}<br><sub>{did_info['name']} - {did_info['description']} | 4 BG × 4 Banks = 16 Banks</sub>"

    fig.update_layout(
        title=dict(text=full_title, x=0.5),
        xaxis=dict(
            title='Row (Physical X) - Vertical',
            range=[0, row_per_bank * bank_x],
            scaleanchor='y',
            scaleratio=1,
            constrain='domain'
        ),
        yaxis=dict(
            title='Column (Physical Y) - Horizontal',
            range=[0, col_per_bank * bank_y],
        ),
        width=width,
        height=height,
        legend=dict(
            yanchor='top',
            y=0.99,
            xanchor='left',
            x=1.01
        ),
        hovermode='closest',
        template='plotly_white'
    )

    return fig


def create_fail_heatmap(
    fail_data: pd.DataFrame,
    part_type: str = 'y62p',
    title: str = 'Fail Density Heatmap',
    bin_size: int = 100,
    width: int = 1000,
    height: int = 800
) -> go.Figure:
    """
    Create a heatmap showing fail density.

    Args:
        fail_data: DataFrame with row, col, dq columns
        part_type: Part type for geometry
        title: Plot title
        bin_size: Size of bins for aggregation
        width: Plot width
        height: Plot height

    Returns:
        Plotly Figure object
    """
    # Load geometry
    geometry = load_geometry(part_type)

    # Process data
    if 'phys_x' not in fail_data.columns:
        df = process_fail_data(fail_data, geometry)
    else:
        df = fail_data.copy()

    # Create 2D histogram
    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    bank_x = getattr(geometry, 'BANK_X', 2)
    bank_y = getattr(geometry, 'BANK_Y', 8)

    x_bins = int((row_per_bank * bank_x) / bin_size)
    y_bins = int((col_per_bank * bank_y) / bin_size)

    fig = go.Figure(data=go.Histogram2d(
        x=df['phys_x'],
        y=df['phys_y'],
        nbinsx=x_bins,
        nbinsy=y_bins,
        colorscale='Hot',
        reversescale=True,
        colorbar=dict(title='Fail Count')
    ))

    # Add bank grid
    shapes = generate_bank_grid(geometry)
    for shape in shapes:
        shape['line']['color'] = 'rgba(255, 255, 255, 0.5)'
    fig.update_layout(shapes=shapes)

    # Update layout
    fig.update_layout(
        title=dict(text=title, x=0.5),
        xaxis=dict(title='Row (Physical X)'),
        yaxis=dict(title='Column (Physical Y)'),
        width=width,
        height=height,
        template='plotly_dark'
    )

    return fig


def create_dq_distribution(
    fail_data: pd.DataFrame,
    title: str = 'Fail Distribution by DQ'
) -> go.Figure:
    """
    Create a bar chart showing fail distribution by DQ.

    Args:
        fail_data: DataFrame with dq column
        title: Plot title

    Returns:
        Plotly Figure object
    """
    dq_counts = fail_data['dq'].value_counts().sort_index()

    fig = go.Figure(data=go.Bar(
        x=[f'DQ{int(dq)}' for dq in dq_counts.index],
        y=dq_counts.values,
        marker_color=[DQ_COLORS[int(dq) % len(DQ_COLORS)] for dq in dq_counts.index]
    ))

    fig.update_layout(
        title=dict(text=title, x=0.5),
        xaxis=dict(title='DQ'),
        yaxis=dict(title='Fail Count'),
        template='plotly_white'
    )

    return fig


def create_bank_distribution(
    fail_data: pd.DataFrame,
    part_type: str = 'y62p',
    title: str = 'Fail Distribution by Bank'
) -> go.Figure:
    """
    Create a bar chart showing fail distribution by bank.

    Args:
        fail_data: DataFrame with bank column (or row/col for processing)
        part_type: Part type for geometry
        title: Plot title

    Returns:
        Plotly Figure object
    """
    # Load geometry and process if needed
    geometry = load_geometry(part_type)

    if 'bank' not in fail_data.columns:
        df = process_fail_data(fail_data, geometry)
    else:
        df = fail_data.copy()

    bank_counts = df['bank'].value_counts().sort_index()

    fig = go.Figure(data=go.Bar(
        x=[f'Bank {int(b)}' for b in bank_counts.index],
        y=bank_counts.values,
        marker_color='steelblue'
    ))

    fig.update_layout(
        title=dict(text=title, x=0.5),
        xaxis=dict(title='Bank'),
        yaxis=dict(title='Fail Count'),
        template='plotly_white'
    )

    return fig


# Convenience function for quick viewing
def view_fails(
    csv_path: str,
    part_type: str = 'y62p',
    **kwargs
) -> go.Figure:
    """
    Quick function to load CSV and create fail viewer.

    Args:
        csv_path: Path to CSV file with row,col,dq data
        part_type: Part type for geometry
        **kwargs: Additional arguments for create_fail_viewer

    Returns:
        Plotly Figure object
    """
    df = load_fail_csv(csv_path)
    return create_fail_viewer(df, part_type=part_type, **kwargs)


# Legacy function name for backwards compatibility
def render_fail_map(csv_path: str = None, df: pd.DataFrame = None, part_type: str = 'y62p', **kwargs):
    """
    Render fail addresses as interactive Plotly scatter plot.

    This is the legacy function name - use view_fails() or create_fail_viewer() instead.
    """
    if csv_path:
        return view_fails(csv_path, part_type=part_type, **kwargs)
    elif df is not None:
        return create_fail_viewer(df, part_type=part_type, **kwargs)
    else:
        raise ValueError("Either csv_path or df must be provided")
