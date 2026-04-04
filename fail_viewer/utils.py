"""
Utility functions for Web Fail Viewer

- CSV parsing
- Logical to physical coordinate mapping
- Bank/section grid generation

DRAM Hierarchy (Fail Viewer abstraction):
- 4 Bank Groups × 4 Banks = 16 total banks
- Rows are vertical (X-axis), Columns are horizontal (Y-axis)
- Configuration width: x48 (default)

Supported DIDs:
- Y62P: SOCAMM/SOCAMM2 reference, medium density
- Y6CP: High-density derivative of Y62P, dual-spine tiling
- Y63N: Next-gen high-speed LPDDR5X SOCAMM2
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional
import importlib
import os


# DID-specific metadata
DID_INFO = {
    'y62p': {
        'name': 'Y62P',
        'description': 'SOCAMM/SOCAMM2 reference',
        'density': 'medium',
        'config_width': 'x48',
        'bank_groups': 4,
        'banks_per_group': 4,
        'total_banks': 16,
    },
    'y6cp': {
        'name': 'Y6CP',
        'description': 'High-density derivative of Y62P',
        'density': 'high',
        'config_width': 'x48 or x32',
        'bank_groups': 4,
        'banks_per_group': 4,
        'total_banks': 16,
        'notes': 'Dual-spine / higher-density internal tiling',
    },
    'y63n': {
        'name': 'Y63N',
        'description': 'Next-gen high-speed SOCAMM2',
        'density': 'high',
        'config_width': 'x48 or x32',
        'bank_groups': 4,
        'banks_per_group': 4,
        'total_banks': 16,
        'notes': 'LPDDR5X, tighter electrical margins',
    },
}


def get_did_info(part_type: str) -> Dict:
    """Get DID-specific metadata."""
    return DID_INFO.get(part_type.lower(), DID_INFO['y62p'])


def load_geometry(part_type: str):
    """
    Load geometry definitions for a part type.

    Args:
        part_type: Part type string (e.g., 'y62p', 'y6cp', 'y63n')

    Returns:
        Module with geometry constants
    """
    # Import from local geometry folder
    geometry_path = os.path.dirname(__file__)
    import sys
    if geometry_path not in sys.path:
        sys.path.insert(0, geometry_path)

    try:
        return importlib.import_module(f'geometry.{part_type}')
    except ImportError:
        raise ValueError(f"Unknown part type: {part_type}. Available: y62p, y6cp, y63n")


def load_fail_csv(csv_path: str, has_header: bool = False) -> pd.DataFrame:
    """
    Load fail address CSV file.

    Expected format: row,col,dq (no header by default)
    Alternative format: bank,row,col,dq (with bank column)
    Example: 1204545,434,1

    Args:
        csv_path: Path to CSV file
        has_header: Whether CSV has a header row

    Returns:
        DataFrame with columns: row, col, dq (and optionally bank)
    """
    if has_header:
        df = pd.read_csv(csv_path)
        # Normalize column names
        df.columns = [c.lower().strip() for c in df.columns]
    else:
        df = pd.read_csv(csv_path, header=None, names=['row', 'col', 'dq'])

    # Ensure numeric types
    df['row'] = pd.to_numeric(df['row'], errors='coerce')
    df['col'] = pd.to_numeric(df['col'], errors='coerce')
    df['dq'] = pd.to_numeric(df['dq'], errors='coerce')

    return df.dropna()


def get_bank_group(bank_num: int) -> int:
    """
    Get bank group number from bank number.

    Bank Group layout (4 groups × 4 banks):
    - BG0: Banks 0-3
    - BG1: Banks 4-7
    - BG2: Banks 8-11
    - BG3: Banks 12-15
    """
    return bank_num // 4


def get_bank_from_address(row: int, col: int, geometry) -> int:
    """
    Determine bank number from row/col address.

    Uses the BANK_ARRAY from geometry for actual bank mapping.
    """
    # Use bank position arrays if available
    if hasattr(geometry, 'BANK_POS_X') and hasattr(geometry, 'BANK_POS_Y'):
        row_per_bank = geometry.ROW_PER_BANK
        col_per_bank = geometry.COL_PER_BANK
        bank_x = getattr(geometry, 'BANK_X', 2)
        bank_y = getattr(geometry, 'BANK_Y', 8)

        # Calculate which bank region based on address
        bx = min(int(row / row_per_bank), bank_x - 1)
        by = min(int(col / col_per_bank), bank_y - 1)

        # Find bank number from position arrays
        for bank_num in range(geometry.NUM_BANKS):
            if (geometry.BANK_POS_X[bank_num] == bx and
                geometry.BANK_POS_Y[bank_num] == by):
                return bank_num

    return 0  # Default bank


def logical_to_physical(row: int, col: int, dq: int, geometry) -> Tuple[float, float, int, int]:
    """
    Convert logical address to physical die coordinates.

    DRAM Fail Viewer convention:
    - Rows are vertical (mapped to X-axis)
    - Columns are horizontal (mapped to Y-axis)

    Args:
        row: Logical row address
        col: Logical column address
        dq: DQ number (0-15)
        geometry: Part geometry module (y62p, y6cp, y63n)

    Returns:
        (phys_x, phys_y, bank_num, bank_group)
    """
    # Get geometry constants
    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    bank_x = getattr(geometry, 'BANK_X', 2)
    bank_y = getattr(geometry, 'BANK_Y', 8)
    num_banks = getattr(geometry, 'NUM_BANKS', 16)

    # Get bank position arrays
    bank_pos_x = getattr(geometry, 'BANK_POS_X', [0] * num_banks)
    bank_pos_y = getattr(geometry, 'BANK_POS_Y', list(range(num_banks)))

    # Determine bank from address
    bank_num = get_bank_from_address(row, col, geometry)
    bank_group = get_bank_group(bank_num)

    # Calculate position within bank
    row_in_bank = row % row_per_bank
    col_in_bank = col % col_per_bank

    # Get bank position in grid
    bx = bank_pos_x[bank_num] if bank_num < len(bank_pos_x) else 0
    by = bank_pos_y[bank_num] if bank_num < len(bank_pos_y) else 0

    # Calculate physical coordinates
    # Row (vertical) maps to X-axis
    # Col (horizontal) maps to Y-axis
    phys_x = bx * row_per_bank + row_in_bank
    phys_y = by * col_per_bank + col_in_bank

    return (phys_x, phys_y, bank_num, bank_group)


def process_fail_data(df: pd.DataFrame, geometry) -> pd.DataFrame:
    """
    Process fail data and add physical coordinates.

    Args:
        df: DataFrame with row, col, dq columns
        geometry: Part geometry module

    Returns:
        DataFrame with added phys_x, phys_y, bank, bank_group columns
    """
    results = []
    for _, row_data in df.iterrows():
        phys_x, phys_y, bank, bank_group = logical_to_physical(
            int(row_data['row']),
            int(row_data['col']),
            int(row_data['dq']),
            geometry
        )
        results.append({
            'row': row_data['row'],
            'col': row_data['col'],
            'dq': row_data['dq'],
            'phys_x': phys_x,
            'phys_y': phys_y,
            'bank': bank,
            'bank_group': bank_group
        })

    return pd.DataFrame(results)


def generate_bank_grid(geometry, show_bank_groups: bool = True) -> List[Dict]:
    """
    Generate bank and bank group boundary lines for Plotly overlay.

    DRAM hierarchy:
    - 4 Bank Groups (thick lines)
    - 4 Banks per group (thin lines)

    Args:
        geometry: Part geometry module
        show_bank_groups: Whether to show bank group boundaries with thicker lines

    Returns:
        List of Plotly shape dicts for bank grid
    """
    shapes = []

    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    bank_x = getattr(geometry, 'BANK_X', 2)
    bank_y = getattr(geometry, 'BANK_Y', 8)

    total_rows = row_per_bank * bank_x
    total_cols = col_per_bank * bank_y

    # Bank group boundaries (every 2 banks in Y direction = 4 banks per group)
    # BG0: Banks 0-3, BG1: Banks 4-7, BG2: Banks 8-11, BG3: Banks 12-15
    banks_per_group_y = 2  # In Y direction, each group spans 2 rows of banks

    # Vertical lines (bank X boundaries)
    for i in range(bank_x + 1):
        x = i * row_per_bank
        # Main bank boundary
        shapes.append({
            'type': 'line',
            'x0': x, 'y0': 0,
            'x1': x, 'y1': total_cols,
            'line': {'color': 'rgba(100, 100, 100, 0.8)', 'width': 2}
        })

    # Horizontal lines (bank Y boundaries)
    for i in range(bank_y + 1):
        y = i * col_per_bank

        # Determine if this is a bank group boundary
        is_bank_group_boundary = (i % banks_per_group_y == 0) and show_bank_groups

        line_color = 'rgba(50, 50, 50, 0.9)' if is_bank_group_boundary else 'rgba(150, 150, 150, 0.5)'
        line_width = 3 if is_bank_group_boundary else 1

        shapes.append({
            'type': 'line',
            'x0': 0, 'y0': y,
            'x1': total_rows, 'y1': y,
            'line': {'color': line_color, 'width': line_width}
        })

    return shapes


def generate_bank_labels(geometry, show_bank_groups: bool = True) -> List[Dict]:
    """
    Generate bank and bank group label annotations for Plotly.

    Args:
        geometry: Part geometry module
        show_bank_groups: Whether to show bank group labels

    Returns:
        List of Plotly annotation dicts
    """
    annotations = []

    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    num_banks = getattr(geometry, 'NUM_BANKS', 16)
    bank_pos_x = getattr(geometry, 'BANK_POS_X', [0] * num_banks)
    bank_pos_y = getattr(geometry, 'BANK_POS_Y', list(range(num_banks)))

    # Bank labels
    for bank_num in range(num_banks):
        bx = bank_pos_x[bank_num] if bank_num < len(bank_pos_x) else 0
        by = bank_pos_y[bank_num] if bank_num < len(bank_pos_y) else 0

        # Center of bank
        x = (bx + 0.5) * row_per_bank
        y = (by + 0.5) * col_per_bank

        bank_group = get_bank_group(bank_num)

        annotations.append({
            'x': x,
            'y': y,
            'text': f'B{bank_num}',
            'showarrow': False,
            'font': {'size': 9, 'color': 'rgba(80, 80, 80, 0.8)'},
            'opacity': 0.8
        })

    # Bank group labels (on the right side)
    if show_bank_groups:
        bank_y = getattr(geometry, 'BANK_Y', 8)
        banks_per_group_y = 2

        for bg in range(4):
            # Position bank group label on the right edge
            x = row_per_bank * 2 + row_per_bank * 0.05  # Just outside right edge
            y = (bg * banks_per_group_y + banks_per_group_y / 2) * col_per_bank

            annotations.append({
                'x': x,
                'y': y,
                'text': f'<b>BG{bg}</b>',
                'showarrow': False,
                'font': {'size': 12, 'color': 'darkblue'},
                'xanchor': 'left',
                'opacity': 0.9
            })

    return annotations


def generate_fail_summary(df: pd.DataFrame, geometry) -> Dict:
    """
    Generate summary statistics for fail data.

    Args:
        df: Processed DataFrame with fail data
        geometry: Part geometry module

    Returns:
        Dictionary with summary statistics
    """
    summary = {
        'total_fails': len(df),
        'unique_rows': df['row'].nunique() if 'row' in df.columns else 0,
        'unique_cols': df['col'].nunique() if 'col' in df.columns else 0,
        'unique_dqs': df['dq'].nunique() if 'dq' in df.columns else 0,
        'unique_banks': df['bank'].nunique() if 'bank' in df.columns else 0,
        'dq_distribution': df['dq'].value_counts().to_dict() if 'dq' in df.columns else {},
        'bank_distribution': df['bank'].value_counts().to_dict() if 'bank' in df.columns else {},
    }

    # Add bank group distribution
    if 'bank_group' in df.columns:
        summary['bank_group_distribution'] = df['bank_group'].value_counts().to_dict()

    # Add row/col ranges
    if 'row' in df.columns and len(df) > 0:
        summary['row_range'] = (int(df['row'].min()), int(df['row'].max()))
    if 'col' in df.columns and len(df) > 0:
        summary['col_range'] = (int(df['col'].min()), int(df['col'].max()))

    return summary
