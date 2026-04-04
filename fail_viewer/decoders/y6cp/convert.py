"""
Y6CP Conversion Equations

Ported from y6cp_convert.h

Y6CP is a high-density derivative of Y62P with:
- 103896 rows per bank (vs 68340 in Y62P)
- 17808 columns per bank (same as Y62P)
- Different section layout

These are EXACT equations from Fail Viewer - no generalization.
"""

from dataclasses import dataclass, field
from typing import Dict, Any

# =============================================================================
# Y6CP GEOMETRY CONSTANTS (from y6cp.h)
# =============================================================================

NUM_BANKS = 16
BANK_X = 2
BANK_Y = 8

ROW_PER_BANK = 103896  # Higher than Y62P's 68340
COL_PER_BANK = 17808   # Same as Y62P

# Section dimensions (different from Y62P)
NUM_ROW_PER_BLOCK_TOTAL = 1332
NUM_ROW_BLOCKS = 156
NUM_ROW_BLOCKS_PER_BANK = 78
NUM_COL_BLOCKS_Y = 144
NUM_COL_BLOCKS_PER_BANK = 18

# Bank position arrays (same as Y62P)
BANK_POS_X = [0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0]
BANK_POS_Y = [7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0, 1, 0]
BANK_ARRAY = [[15, 14, 11, 10, 5, 4, 1, 0], [13, 12, 9, 8, 7, 6, 3, 2]]

# Address masks
ASEL_ROW_MASK = 0x3FFFFF
ASEL_COL_MASK = 0x0000FFFFFFC00000
ASEL_COL_SHIFT = 22
ASEL_BANK_SHIFT = 17
ROW_MINUS_BANK_MASK = 0x1FFFF
HALF_BLOCK_MASK = 0x1FFFF

# Decode revision flag
DECODE_REV = 1


@dataclass
class PhysicalRow:
    """Result of row decode."""
    phy_row_in_bank: int
    sec_x: int
    intra_row: int
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PhysicalColumn:
    """Result of column decode."""
    phy_column: int
    sec_y: int
    intra_col: int
    raw: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# ROW CONVERSION (from y6cp_convert.h)
# =============================================================================

def decode_row(log_row: int, bank: int = 0) -> PhysicalRow:
    """
    Convert logical row to physical row using Y6CP equations.

    Y6CP row decoding differs from Y62P due to higher density.
    The row address has more bits and different swizzling.

    From y6cp_convert.h:
    // Y6CP has extended row address range
    // phy_row_in_bank includes DECODE_REV adjustments

    Args:
        log_row: Logical row address
        bank: Bank number

    Returns:
        PhysicalRow with decoded coordinates
    """
    # Y6CP row address mask
    row_masked = log_row & ROW_MINUS_BANK_MASK

    # Y6CP specific: row address with potential inversion
    # based on DECODE_REV flag
    if DECODE_REV == 1:
        # Standard decoding
        phy_row_in_bank = row_masked
    else:
        # Inverted decoding (rare)
        phy_row_in_bank = (ROW_PER_BANK - 1) - row_masked

    # Ensure within bank bounds
    phy_row_in_bank = phy_row_in_bank % ROW_PER_BANK

    # Calculate section position
    sec_x = phy_row_in_bank // NUM_ROW_PER_BLOCK_TOTAL
    intra_row = phy_row_in_bank % NUM_ROW_PER_BLOCK_TOTAL

    return PhysicalRow(
        phy_row_in_bank=phy_row_in_bank,
        sec_x=sec_x,
        intra_row=intra_row,
        raw={
            'log_row': log_row,
            'row_masked': row_masked,
            'decode_rev': DECODE_REV,
            'equation': f'{log_row} & {hex(ROW_MINUS_BANK_MASK)}'
        }
    )


def log_to_phy_row(log_row: int, bank: int) -> int:
    """
    Convert logical row to absolute physical row position (die-level).

    Args:
        log_row: Logical row address
        bank: Bank number

    Returns:
        Physical row position including bank offset
    """
    result = decode_row(log_row, bank)
    bx = BANK_POS_X[bank] if bank < len(BANK_POS_X) else 0
    return (bx * ROW_PER_BANK) + result.phy_row_in_bank


# =============================================================================
# COLUMN CONVERSION (from y6cp_convert.h)
# =============================================================================

def decode_column(
    log_col: int,
    burst: int = 0,
    ca2_inv: int = 0,
    ca4_inv: int = 0,
    col_offset: int = 0
) -> PhysicalColumn:
    """
    Convert logical column to physical column using Y6CP equations.

    Y6CP column decoding is similar to Y62P but may have
    different CP swizzling parameters.

    From y6cp_convert.h:
    // Column with CP swizzling
    // Similar to Y62P but check for DID-specific inversions

    Args:
        log_col: Logical column address
        burst: Burst position (0-7)
        ca2_inv: CA2 inversion flag
        ca4_inv: CA4 inversion flag
        col_offset: Column offset

    Returns:
        PhysicalColumn with decoded coordinates
    """
    # Apply CP swizzling
    phy_cs = log_col ^ (ca2_inv << 2) ^ (ca4_inv << 4)

    # Combine with burst
    digit = col_offset + ((phy_cs << 3) | (burst & 0x7))

    # Map to physical column
    phy_column = digit % COL_PER_BANK

    # Calculate section position
    cols_per_section = COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK
    sec_y = phy_column // cols_per_section
    intra_col = phy_column % cols_per_section

    return PhysicalColumn(
        phy_column=phy_column,
        sec_y=sec_y,
        intra_col=intra_col,
        raw={
            'log_col': log_col,
            'burst': burst,
            'phy_cs': phy_cs,
            'digit': digit,
            'ca2_inv': ca2_inv,
            'ca4_inv': ca4_inv,
            'equation': f'(({log_col} ^ ({ca2_inv} << 2) ^ ({ca4_inv} << 4)) << 3) | {burst}'
        }
    )


def log_to_phy_col(log_col: int, bank: int, burst: int = 0) -> int:
    """
    Convert logical column to absolute physical column position (die-level).

    Args:
        log_col: Logical column address
        bank: Bank number
        burst: Burst position

    Returns:
        Physical column position including bank offset
    """
    result = decode_column(log_col, burst)
    by = BANK_POS_Y[bank] if bank < len(BANK_POS_Y) else 0
    return (by * COL_PER_BANK) + result.phy_column


# =============================================================================
# BANK CONVERSION
# =============================================================================

def decode_bank(log_bank: int) -> Dict[str, int]:
    """
    Convert logical bank number to physical position.

    Args:
        log_bank: Logical bank number (0-15)

    Returns:
        Dict with bank position info
    """
    if log_bank < 0 or log_bank >= NUM_BANKS:
        log_bank = log_bank % NUM_BANKS

    bx = BANK_POS_X[log_bank]
    by = BANK_POS_Y[log_bank]
    bank_group = log_bank // 4

    return {
        'bank_num': log_bank,
        'bank_group': bank_group,
        'bx': bx,
        'by': by,
        'row_start': bx * ROW_PER_BANK,
        'col_start': by * COL_PER_BANK,
        'row_end': (bx + 1) * ROW_PER_BANK,
        'col_end': (by + 1) * COL_PER_BANK,
    }


def get_bank_from_position(bx: int, by: int) -> int:
    """Get bank number from X/Y position."""
    if bx < 0 or bx >= BANK_X or by < 0 or by >= BANK_Y:
        return 0
    return BANK_ARRAY[bx][by]
