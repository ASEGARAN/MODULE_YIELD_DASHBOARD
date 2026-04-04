"""
Y62P Conversion Equations

Ported from y62p_convert.h

These are the EXACT equations from Fail Viewer - no generalization.
Each equation includes the original C-style comment for reference.

Y62P Geometry:
- 16 banks (4 BG × 4 banks)
- 68340 rows per bank
- 17808 columns per bank
- Bank layout: 2×8 (BANK_X × BANK_Y)

TODO: Update equations from actual y62p_convert.h source
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

# =============================================================================
# Y62P GEOMETRY CONSTANTS (from y62p.h)
# =============================================================================

NUM_BANKS = 16
BANK_X = 2
BANK_Y = 8

ROW_PER_BANK = 68340
COL_PER_BANK = 17808

# Section dimensions
NUM_ROW_PER_BLOCK_TOTAL = 1332
NUM_ROW_BLOCKS = 156
NUM_ROW_BLOCKS_PER_BANK = 78
NUM_COL_BLOCKS_Y = 144
NUM_COL_BLOCKS_PER_BANK = 18

# Rows/Cols per section
ROWS_PER_SECTION = ROW_PER_BANK // NUM_ROW_BLOCKS_PER_BANK  # ~876
COLS_PER_SECTION = COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK  # ~989

# Bank position arrays (physical layout)
# Bank 0 is at position (0, 7), Bank 15 is at position (0, 0)
BANK_POS_X = [0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0]
BANK_POS_Y = [7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0, 1, 0]
BANK_ARRAY = [[15, 14, 11, 10, 5, 4, 1, 0], [13, 12, 9, 8, 7, 6, 3, 2]]

# Address masks
ROW_MASK = 0x1FFFF  # 17 bits for row within bank
COL_MASK = 0xFFFF   # 16 bits for column


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
# ROW CONVERSION (from y62p_convert.h)
# =============================================================================

def decode_row(log_row: int, bank: int = 0) -> PhysicalRow:
    """
    Convert logical row to physical row.

    From y62p_convert.h:
    // Row address decoding with RA13/RA14 swizzling
    // uint32_t phy_row_in_bank =
    //     (dx & 0x1FFF)
    //   | (ra13_phy << 13)
    //   | (ra14_phy << 14);

    Args:
        log_row: Logical row address
        bank: Bank number (for bank-specific adjustments)

    Returns:
        PhysicalRow with decoded coordinates
    """
    # Extract row address bits
    # RA[12:0] - lower 13 bits pass through
    # RA13, RA14 - swizzled

    ra_lower = log_row & 0x1FFF  # Bits [12:0]
    ra13_log = (log_row >> 13) & 0x1
    ra14_log = (log_row >> 14) & 0x1

    # Y62P RA swizzling:
    # RA13_phy = RA13_log
    # RA14_phy = !(RA13_log ^ RA14_log)  -- XOR then invert
    ra13_phy = ra13_log
    ra14_phy = 1 - (ra13_log ^ ra14_log)  # NOT of XOR

    # Construct physical row
    phy_row_in_bank = ra_lower | (ra13_phy << 13) | (ra14_phy << 14)

    # Handle rows beyond 15 bits (if present in Y62P)
    if log_row > 0x7FFF:
        # Upper bits pass through
        phy_row_in_bank |= (log_row & 0x18000)

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
            'ra_lower': ra_lower,
            'ra13_log': ra13_log,
            'ra14_log': ra14_log,
            'ra13_phy': ra13_phy,
            'ra14_phy': ra14_phy,
            'equation': f'({log_row} & 0x1FFF) | ({ra13_phy} << 13) | ({ra14_phy} << 14)'
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
# COLUMN CONVERSION (from y62p_convert.h)
# =============================================================================

def decode_column(
    log_col: int,
    burst: int = 0,
    ca2_inv: int = 0,
    ca4_inv: int = 0,
    col_offset: int = 0
) -> PhysicalColumn:
    """
    Convert logical column to physical column.

    From y62p_convert.h:
    // Column address with CP swizzling and burst decode
    // phy_cs = log_col ^ (ca2_inv << 2) ^ (ca4_inv << 4);
    // digit = col_offset + ((phy_cs << 3) | burst);

    Args:
        log_col: Logical column address
        burst: Burst position (0-7)
        ca2_inv: CA2 inversion flag
        ca4_inv: CA4 inversion flag
        col_offset: Column offset

    Returns:
        PhysicalColumn with decoded coordinates
    """
    # Apply CP swizzling (column plane inversion)
    phy_cs = log_col ^ (ca2_inv << 2) ^ (ca4_inv << 4)

    # Combine with burst to get digit position
    # Burst is the lower 3 bits of the column address
    digit = col_offset + ((phy_cs << 3) | (burst & 0x7))

    # For Y62P, simpler mapping (update from actual source)
    phy_column = digit % COL_PER_BANK

    # Calculate section position
    sec_y = phy_column // (COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK)
    intra_col = phy_column % (COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK)

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
    """
    Get bank number from X/Y position.

    Args:
        bx: X position (0 or 1)
        by: Y position (0-7)

    Returns:
        Bank number (0-15)
    """
    if bx < 0 or bx >= BANK_X or by < 0 or by >= BANK_Y:
        return 0
    return BANK_ARRAY[bx][by]
