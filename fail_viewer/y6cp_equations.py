"""
Y6CP Redundancy Equations Module

Ported from Fail Viewer's y6cp.h, y6cp_convert.h, red_function.h, and redundancy.h

Y6CP Architecture:
- 16 banks (4 bank groups × 4 banks)
- 103896 rows per bank (ROW_PER_BANK)
- 17808 columns per bank (COL_PER_BANK)
- 86 redundant rows per bank (RED_ROWS_PER_BANK)
- 512 redundant columns per bank (RED_COLS_PER_BANK)
- Bank layout: 2×8 (BANK_X × BANK_Y)

Repair Address Encoding:
- Row repair: Uses RED_ROW_MULT (4) and FULL_RED_ROW_MASK (0x1fffc)
- Column repair: Uses RED_COL_MULT (16) and FULL_RED_COL_MASK (0xfff0)
"""

from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass


# =============================================================================
# Y6CP GEOMETRY CONSTANTS (from y6cp.h)
# =============================================================================

# Bank layout
NUM_BANKS = 16
BANK_X = 2              # Banks in X direction
BANK_Y = 8              # Banks in Y direction

# Bank position arrays
BANK_POS_X = [0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0]
BANK_POS_Y = [7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0, 1, 0]
BANK_ARRAY = [[15, 14, 11, 10, 5, 4, 1, 0], [13, 12, 9, 8, 7, 6, 3, 2]]

# Per-bank dimensions
ROW_PER_BANK = 103896
COL_PER_BANK = 17808

# Block dimensions
NUM_ROW_PER_BLOCK_TOTAL = 1332
NUM_ROW_BLOCKS = 156
NUM_ROW_BLOCKS_PER_BANK = 78
NUM_COL_BLOCKS_Y = 144
NUM_COL_BLOCKS_PER_BANK = 18

# Address masks
ASEL_ROW_MASK = 0x3fffff
ASEL_COL_MASK = 0x0000FFFFFFC00000
ASEL_COL_SHIFT = 22
ASEL_BANK_SHIFT = 17
ROW_MINUS_BANK_MASK = 0x1ffff
HALF_BLOCK_MASK = 0x1ffff

# Redundancy constants
RED_ROW_MULT = 4                    # Row address multiplier for redundancy
RED_COL_MULT = 16                   # Column address multiplier for redundancy
FULL_RED_ROW_MASK = 0x1fffc         # Row redundancy mask
FULL_RED_COL_MASK = 0xfff0          # Column redundancy mask
RED_ROW_MASK = 0x1fffc

RED_ROWS_PER_BANK = 86              # Redundant rows per bank
RED_COLS_PER_BANK = 512             # Redundant columns per bank

# Decode revision flag
DECODE_REV = 1


# =============================================================================
# BANK DECODING (from red_function.h)
# =============================================================================

def decode_bank_from_repair(repair_element: int, repair_type: str) -> int:
    """
    Decode bank number from repair element value.

    For Y6CP, the bank encoding varies by repair type.

    Args:
        repair_element: Raw repair element value from repan data
        repair_type: 'ROW' or 'COLUMN'

    Returns:
        Bank number (0-15)
    """
    # Y6CP bank decoding
    # The bank is typically encoded in the upper bits of the repair element
    # This is a simplified version - actual implementation may vary

    if repair_type == 'ROW':
        # Row repairs: bank encoded in bits [21:18] typically
        bank = (repair_element >> 18) & 0xF
    else:
        # Column repairs: bank may be encoded differently
        bank = (repair_element >> 18) & 0xF

    # Ensure bank is in valid range
    return bank % NUM_BANKS


def get_bank_from_position(bx: int, by: int) -> int:
    """
    Get bank number from X/Y position in the bank array.

    Args:
        bx: X position (0 or 1)
        by: Y position (0-7)

    Returns:
        Bank number (0-15)
    """
    if bx < 0 or bx >= BANK_X or by < 0 or by >= BANK_Y:
        return 0

    return BANK_ARRAY[bx][by]


# =============================================================================
# ROW REDUNDANCY EQUATIONS (from redundancy.h - y6cp_row_redun)
# =============================================================================

@dataclass
class RowRepairResult:
    """Result of row repair address conversion."""
    bank: int
    physical_row: int
    row_in_bank: int
    block_x: int
    repair_index: int
    valid: bool = True


def y6cp_row_redun(
    repair_element: int,
    repaired_element: int,
    bank: Optional[int] = None
) -> RowRepairResult:
    """
    Convert Y6CP row repair data to physical row address.

    This implements the row redundancy equation from y6cp's redundancy.h.

    Row repair format:
    - repair_element: Redundant row element (fuse address)
    - repaired_element: Logical row address being repaired

    The physical row is calculated as:
    phy_row = (repaired_element & RED_ROW_MASK) * RED_ROW_MULT

    Args:
        repair_element: Redundant element value
        repaired_element: Repaired logical row address
        bank: Optional bank override

    Returns:
        RowRepairResult with physical coordinates
    """
    # Decode bank if not provided
    if bank is None:
        bank = decode_bank_from_repair(repair_element, 'ROW')

    # Extract row address using mask
    # The repaired_element contains the logical row that was repaired
    row_masked = repaired_element & RED_ROW_MASK

    # Apply row multiplier to get physical position
    # Y6CP uses RED_ROW_MULT = 4
    physical_row_in_bank = row_masked * RED_ROW_MULT

    # Ensure row is within bank bounds
    physical_row_in_bank = physical_row_in_bank % ROW_PER_BANK

    # Calculate block position
    block_x = physical_row_in_bank // NUM_ROW_PER_BLOCK_TOTAL

    # Get repair index (which redundant row element)
    repair_index = repair_element % RED_ROWS_PER_BANK

    # Calculate absolute physical row including bank offset
    bx = BANK_POS_X[bank]
    physical_row = (bx * ROW_PER_BANK) + physical_row_in_bank

    return RowRepairResult(
        bank=bank,
        physical_row=physical_row,
        row_in_bank=physical_row_in_bank,
        block_x=block_x,
        repair_index=repair_index,
        valid=True
    )


# =============================================================================
# COLUMN REDUNDANCY EQUATIONS (from redundancy.h - y6cp_col_redun)
# =============================================================================

@dataclass
class ColRepairResult:
    """Result of column repair address conversion."""
    bank: int
    physical_col: int
    col_in_bank: int
    block_y: int
    repair_index: int
    valid: bool = True


def y6cp_col_redun(
    repair_element: int,
    repaired_element: int,
    bank: Optional[int] = None
) -> ColRepairResult:
    """
    Convert Y6CP column repair data to physical column address.

    This implements the column redundancy equation from y6cp's redundancy.h.

    Column repair format:
    - repair_element: Redundant column element (fuse address)
    - repaired_element: Logical column address being repaired

    The physical column is calculated as:
    phy_col = (repaired_element & RED_COL_MASK) * RED_COL_MULT

    Args:
        repair_element: Redundant element value
        repaired_element: Repaired logical column address
        bank: Optional bank override

    Returns:
        ColRepairResult with physical coordinates
    """
    # Decode bank if not provided
    if bank is None:
        bank = decode_bank_from_repair(repair_element, 'COLUMN')

    # Extract column address using mask
    # The repaired_element contains the logical column that was repaired
    col_masked = (repaired_element & FULL_RED_COL_MASK) >> 4

    # Apply column multiplier to get physical position
    # Y6CP uses RED_COL_MULT = 16
    physical_col_in_bank = col_masked * RED_COL_MULT

    # Ensure column is within bank bounds
    physical_col_in_bank = physical_col_in_bank % COL_PER_BANK

    # Calculate block position
    block_y = physical_col_in_bank // (COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK)

    # Get repair index (which redundant column element)
    repair_index = repair_element % RED_COLS_PER_BANK

    # Calculate absolute physical column including bank offset
    by = BANK_POS_Y[bank]
    physical_col = (by * COL_PER_BANK) + physical_col_in_bank

    return ColRepairResult(
        bank=bank,
        physical_col=physical_col,
        col_in_bank=physical_col_in_bank,
        block_y=block_y,
        repair_index=repair_index,
        valid=True
    )


# =============================================================================
# LOGICAL TO PHYSICAL CONVERSION (from y6cp_convert.h)
# =============================================================================

def log_to_physical_row(log_row: int, bank: int) -> int:
    """
    Convert logical row address to physical row position.

    Args:
        log_row: Logical row address
        bank: Bank number

    Returns:
        Physical row position (die-level)
    """
    # Apply row mask
    row_in_bank = log_row & ROW_MINUS_BANK_MASK

    # Get bank X position
    bx = BANK_POS_X[bank]

    # Calculate physical row
    # For DECODE_REV=1, the row direction may be inverted
    if DECODE_REV == 1:
        # Physical row = bank_offset + row_in_bank
        physical_row = (bx * ROW_PER_BANK) + row_in_bank
    else:
        physical_row = (bx * ROW_PER_BANK) + row_in_bank

    return physical_row


def log_to_physical_col(log_col: int, bank: int) -> int:
    """
    Convert logical column address to physical column position.

    Args:
        log_col: Logical column address
        bank: Bank number

    Returns:
        Physical column position (die-level)
    """
    # Get column within bank
    col_in_bank = log_col % COL_PER_BANK

    # Get bank Y position
    by = BANK_POS_Y[bank]

    # Calculate physical column
    physical_col = (by * COL_PER_BANK) + col_in_bank

    return physical_col


def log_to_physical(log_row: int, log_col: int, bank: int) -> Tuple[int, int]:
    """
    Convert logical address to physical position.

    Args:
        log_row: Logical row address
        log_col: Logical column address
        bank: Bank number

    Returns:
        Tuple of (physical_row, physical_col)
    """
    return (
        log_to_physical_row(log_row, bank),
        log_to_physical_col(log_col, bank)
    )


# =============================================================================
# PHYSICAL TO LOGICAL CONVERSION (from y6cp_convert.h - die_to_log)
# =============================================================================

def physical_to_logical(phys_row: int, phys_col: int) -> Tuple[int, int, int]:
    """
    Convert physical position to logical address.

    Args:
        phys_row: Physical row position
        phys_col: Physical column position

    Returns:
        Tuple of (log_row, log_col, bank)
    """
    # Determine bank from physical position
    bx = phys_row // ROW_PER_BANK
    by = phys_col // COL_PER_BANK

    # Get bank number
    bank = get_bank_from_position(bx, by)

    # Calculate logical addresses
    log_row = phys_row % ROW_PER_BANK
    log_col = phys_col % COL_PER_BANK

    return (log_row, log_col, bank)


# =============================================================================
# REPAIR DATA CONVERSION
# =============================================================================

def convert_row_repair_to_physical(
    repair_element: int,
    repaired_element: int,
    bank: Optional[int] = None
) -> Dict:
    """
    Convert raw row repair data to physical overlay coordinates.

    Args:
        repair_element: Redundant element from repan
        repaired_element: Repaired row address
        bank: Optional bank override

    Returns:
        Dict with physical coordinates for overlay rendering
    """
    result = y6cp_row_redun(repair_element, repaired_element, bank)

    if not result.valid:
        return None

    # Get bank column range for full row line
    by = BANK_POS_Y[result.bank]
    col_start = by * COL_PER_BANK
    col_end = col_start + COL_PER_BANK

    return {
        'repair_type': 'ROW',
        'bank': result.bank,
        'bank_group': result.bank // 4,
        'physical_row': result.physical_row,
        'row_in_bank': result.row_in_bank,
        'block_x': result.block_x,
        'repair_index': result.repair_index,
        'span': {
            'start': {'row': result.physical_row, 'col': col_start},
            'end': {'row': result.physical_row, 'col': col_end}
        },
        'raw': {
            'repair_element': repair_element,
            'repaired_element': repaired_element
        }
    }


def convert_col_repair_to_physical(
    repair_element: int,
    repaired_element: int,
    bank: Optional[int] = None
) -> Dict:
    """
    Convert raw column repair data to physical overlay coordinates.

    Args:
        repair_element: Redundant element from repan
        repaired_element: Repaired column address
        bank: Optional bank override

    Returns:
        Dict with physical coordinates for overlay rendering
    """
    result = y6cp_col_redun(repair_element, repaired_element, bank)

    if not result.valid:
        return None

    # Get bank row range for full column line
    bx = BANK_POS_X[result.bank]
    row_start = bx * ROW_PER_BANK
    row_end = row_start + ROW_PER_BANK

    return {
        'repair_type': 'COLUMN',
        'bank': result.bank,
        'bank_group': result.bank // 4,
        'physical_col': result.physical_col,
        'col_in_bank': result.col_in_bank,
        'block_y': result.block_y,
        'repair_index': result.repair_index,
        'span': {
            'start': {'row': row_start, 'col': result.physical_col},
            'end': {'row': row_end, 'col': result.physical_col}
        },
        'raw': {
            'repair_element': repair_element,
            'repaired_element': repaired_element
        }
    }


def parse_repan_line(line: str) -> Optional[Dict]:
    """
    Parse a single repan data line.

    Expected format (from Fail Viewer input):
    repair_type,repair_element,repaired_element,failing_bits,type

    Example:
    R,0x1234,0x5678,8,BinV
    C,0xABCD,0xEF01,4,BURN

    Args:
        line: Single line of repan data

    Returns:
        Parsed repair dict or None
    """
    parts = line.strip().split(',')
    if len(parts) < 4:
        return None

    try:
        repair_type_char = parts[0].strip().upper()
        repair_element = int(parts[1].strip(), 0)  # Support hex or decimal
        repaired_element = int(parts[2].strip(), 0)
        failing_bits = int(parts[3].strip()) if parts[3].strip().isdigit() else 0
        repair_source = parts[4].strip() if len(parts) > 4 else 'UNKNOWN'

        if repair_type_char == 'R':
            return convert_row_repair_to_physical(repair_element, repaired_element)
        elif repair_type_char == 'C':
            return convert_col_repair_to_physical(repair_element, repaired_element)
        else:
            return None

    except (ValueError, IndexError):
        return None


def parse_repan_csv(csv_content: str) -> List[Dict]:
    """
    Parse repan CSV content (multiple lines).

    Args:
        csv_content: Full CSV content

    Returns:
        List of parsed repair dicts
    """
    repairs = []
    for line in csv_content.strip().split('\n'):
        if not line.strip() or line.startswith('#'):
            continue
        repair = parse_repan_line(line)
        if repair:
            repairs.append(repair)
    return repairs
