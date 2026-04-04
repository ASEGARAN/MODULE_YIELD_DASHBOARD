"""
Y62P Redundancy Equations

Ported from y62p_redundancy.h

Row and column repair decode equations.
These determine where repair lines are drawn on the fail viewer.

Redundancy resources per bank:
- 86 redundant rows (RED_ROWS_PER_BANK)
- 512 redundant columns (RED_COLS_PER_BANK)

TODO: Update equations from actual y62p_redundancy.h source
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

from .convert import (
    NUM_BANKS,
    ROW_PER_BANK,
    COL_PER_BANK,
    BANK_POS_X,
    BANK_POS_Y,
    NUM_ROW_PER_BLOCK_TOTAL,
    NUM_COL_BLOCKS_PER_BANK,
    decode_row,
    decode_column,
)


# =============================================================================
# REDUNDANCY CONSTANTS (from y62p_redundancy.h)
# =============================================================================

RED_ROWS_PER_BANK = 86
RED_COLS_PER_BANK = 512

# Redundancy address multipliers
RED_ROW_MULT = 4
RED_COL_MULT = 16

# Redundancy masks
RED_ROW_MASK = 0x1FFFC    # Row redundancy mask
RED_COL_MASK = 0xFFF0     # Column redundancy mask
FULL_RED_ROW_MASK = 0x1FFFC
FULL_RED_COL_MASK = 0xFFF0


@dataclass
class RowRepairResult:
    """Result of row repair decode."""
    bank: int
    phy_row_in_bank: int
    phy_row_absolute: int  # Including bank offset
    sec_x: int
    repair_index: int
    valid: bool = True
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ColRepairResult:
    """Result of column repair decode."""
    bank: int
    phy_col_in_bank: int
    phy_col_absolute: int  # Including bank offset
    sec_y: int
    repair_index: int
    valid: bool = True
    raw: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# ROW REPAIR EQUATIONS (from y62p_redundancy.h)
# =============================================================================

def decode_row_repair(
    repair_element: int,
    repaired_element: int,
    bank: int
) -> RowRepairResult:
    """
    Decode row repair to physical coordinates.

    From y62p_redundancy.h:
    // Row repair physical position
    // phy_row = (repaired_element & RED_ROW_MASK) * RED_ROW_MULT

    The repaired_element contains the logical row that was repaired.
    We apply the mask and multiplier to get the physical row.

    Args:
        repair_element: Redundant element (fuse address)
        repaired_element: Logical row being repaired
        bank: Bank number

    Returns:
        RowRepairResult with physical coordinates
    """
    if bank < 0 or bank >= NUM_BANKS:
        return RowRepairResult(
            bank=bank,
            phy_row_in_bank=0,
            phy_row_absolute=0,
            sec_x=0,
            repair_index=0,
            valid=False,
            raw={'error': f'Invalid bank {bank}'}
        )

    # Apply Y62P row redundancy equation
    row_masked = repaired_element & RED_ROW_MASK
    phy_row_in_bank = (row_masked * RED_ROW_MULT) % ROW_PER_BANK

    # Get bank position
    bx = BANK_POS_X[bank]
    by = BANK_POS_Y[bank]

    # Calculate absolute physical row (die-level)
    phy_row_absolute = (bx * ROW_PER_BANK) + phy_row_in_bank

    # Calculate section position
    sec_x = phy_row_in_bank // NUM_ROW_PER_BLOCK_TOTAL

    # Repair index (which redundant element)
    repair_index = repair_element % RED_ROWS_PER_BANK

    return RowRepairResult(
        bank=bank,
        phy_row_in_bank=phy_row_in_bank,
        phy_row_absolute=phy_row_absolute,
        sec_x=sec_x,
        repair_index=repair_index,
        valid=True,
        raw={
            'repair_element': repair_element,
            'repaired_element': repaired_element,
            'row_masked': row_masked,
            'equation': f'({repaired_element} & {hex(RED_ROW_MASK)}) * {RED_ROW_MULT}',
            'bx': bx,
            'by': by,
        }
    )


# =============================================================================
# COLUMN REPAIR EQUATIONS (from y62p_redundancy.h)
# =============================================================================

def decode_col_repair(
    repair_element: int,
    repaired_element: int,
    bank: int
) -> ColRepairResult:
    """
    Decode column repair to physical coordinates.

    From y62p_redundancy.h:
    // Column repair physical position
    // phy_col = ((repaired_element & RED_COL_MASK) >> 4) * RED_COL_MULT

    Args:
        repair_element: Redundant element (fuse address)
        repaired_element: Logical column being repaired
        bank: Bank number

    Returns:
        ColRepairResult with physical coordinates
    """
    if bank < 0 or bank >= NUM_BANKS:
        return ColRepairResult(
            bank=bank,
            phy_col_in_bank=0,
            phy_col_absolute=0,
            sec_y=0,
            repair_index=0,
            valid=False,
            raw={'error': f'Invalid bank {bank}'}
        )

    # Apply Y62P column redundancy equation
    col_masked = (repaired_element & RED_COL_MASK) >> 4
    phy_col_in_bank = (col_masked * RED_COL_MULT) % COL_PER_BANK

    # Get bank position
    bx = BANK_POS_X[bank]
    by = BANK_POS_Y[bank]

    # Calculate absolute physical column (die-level)
    phy_col_absolute = (by * COL_PER_BANK) + phy_col_in_bank

    # Calculate section position
    cols_per_section = COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK
    sec_y = phy_col_in_bank // cols_per_section

    # Repair index (which redundant element)
    repair_index = repair_element % RED_COLS_PER_BANK

    return ColRepairResult(
        bank=bank,
        phy_col_in_bank=phy_col_in_bank,
        phy_col_absolute=phy_col_absolute,
        sec_y=sec_y,
        repair_index=repair_index,
        valid=True,
        raw={
            'repair_element': repair_element,
            'repaired_element': repaired_element,
            'col_masked': col_masked,
            'equation': f'(({repaired_element} & {hex(RED_COL_MASK)}) >> 4) * {RED_COL_MULT}',
            'bx': bx,
            'by': by,
        }
    )


# =============================================================================
# REPAIR OVERLAY GENERATION
# =============================================================================

def generate_row_repair_overlay(
    repair_element: int,
    repaired_element: int,
    bank: int,
    test_step: str = "HMFN"
) -> Optional[Dict[str, Any]]:
    """
    Generate row repair overlay data for rendering.

    Returns a dict compatible with PhysicalRepair schema.

    Args:
        repair_element: Redundant element
        repaired_element: Logical row being repaired
        bank: Bank number
        test_step: Origin test step

    Returns:
        Dict with overlay data or None if invalid
    """
    result = decode_row_repair(repair_element, repaired_element, bank)
    if not result.valid:
        return None

    # Get bank column span for full row line
    by = BANK_POS_Y[bank]
    col_start = by * COL_PER_BANK
    col_end = col_start + COL_PER_BANK

    # Color based on test step
    colors = {
        'PROBE': '#FF6600',
        'BURN': '#FF9933',
        'HMFN': '#1E90FF',
    }
    color = colors.get(test_step.upper(), '#FF6600')

    return {
        'repair_type': 'ROW',
        'bank': bank,
        'bank_group': bank // 4,
        'physical_location': {
            'mode': 'ROW_LINE',
            'row': result.phy_row_absolute,
            'column': None,
            'sec': {
                'sec_x': result.sec_x,
                'sec_y': 0,
            },
            'span': {
                'start': {'row': result.phy_row_absolute, 'column': col_start},
                'end': {'row': result.phy_row_absolute, 'column': col_end},
            },
            'confidence': 1.0,
        },
        'render': {
            'layer': 'REPAIR_ROW',
            'color': color,
            'style': {
                'stroke_width': 1.5,
                'stroke_dash': [6, 3],
                'opacity': 0.9,
            },
            'legend_label': f'Row repair ({test_step})',
        },
        'raw': result.raw,
    }


def generate_col_repair_overlay(
    repair_element: int,
    repaired_element: int,
    bank: int,
    test_step: str = "HMFN"
) -> Optional[Dict[str, Any]]:
    """
    Generate column repair overlay data for rendering.

    Returns a dict compatible with PhysicalRepair schema.

    Args:
        repair_element: Redundant element
        repaired_element: Logical column being repaired
        bank: Bank number
        test_step: Origin test step

    Returns:
        Dict with overlay data or None if invalid
    """
    result = decode_col_repair(repair_element, repaired_element, bank)
    if not result.valid:
        return None

    # Get bank row span for full column line
    bx = BANK_POS_X[bank]
    row_start = bx * ROW_PER_BANK
    row_end = row_start + ROW_PER_BANK

    # Color based on test step
    colors = {
        'PROBE': '#0066FF',
        'BURN': '#3399FF',
        'HMFN': '#00B894',
    }
    color = colors.get(test_step.upper(), '#00B894')

    return {
        'repair_type': 'COLUMN',
        'bank': bank,
        'bank_group': bank // 4,
        'physical_location': {
            'mode': 'COL_LINE',
            'row': None,
            'column': result.phy_col_absolute,
            'sec': {
                'sec_x': 0,
                'sec_y': result.sec_y,
            },
            'span': {
                'start': {'row': row_start, 'column': result.phy_col_absolute},
                'end': {'row': row_end, 'column': result.phy_col_absolute},
            },
            'confidence': 1.0,
        },
        'render': {
            'layer': 'REPAIR_COL',
            'color': color,
            'style': {
                'stroke_width': 1.5,
                'stroke_dash': [2, 2],
                'opacity': 0.9,
            },
            'legend_label': f'Column repair ({test_step})',
        },
        'raw': result.raw,
    }
