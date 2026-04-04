"""
Y42M Decoder Implementation

Y42M is the simplest SOCAMM design - good for regression sanity check.
Key characteristics:
- Minimal swizzling
- Fewer sections
- Simpler redundancy equations

TODO: Update from actual y42m_convert.h and y42m_redundancy.h sources
Currently using simplified Y62P equations as placeholder.
"""

from typing import Optional
from dataclasses import dataclass, field
from typing import Dict, Any

from ..base import (
    DidDecoder,
    DecodeContext,
    PhysicalRow,
    PhysicalColumn,
    PhysicalBank,
    PhysicalRepair,
)


# =============================================================================
# Y42M GEOMETRY CONSTANTS (from y42m.h)
# These are typically simpler than Y62P/Y6CP
# =============================================================================

NUM_BANKS = 16
BANK_X = 2
BANK_Y = 8

# Y42M has smaller geometry than Y62P
ROW_PER_BANK = 65536    # Simpler power-of-2
COL_PER_BANK = 16384    # Simpler power-of-2

# Section dimensions (fewer than Y6CP)
NUM_ROW_PER_BLOCK_TOTAL = 1024
NUM_ROW_BLOCKS_PER_BANK = 64
NUM_COL_BLOCKS_PER_BANK = 16

# Bank positions (same layout as Y62P/Y6CP)
BANK_POS_X = [0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0]
BANK_POS_Y = [7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0, 1, 0]

# Redundancy constants (simpler)
RED_ROWS_PER_BANK = 64
RED_COLS_PER_BANK = 256
RED_ROW_MULT = 1        # No multiplication (simpler)
RED_COL_MULT = 1        # No multiplication (simpler)
RED_ROW_MASK = 0xFFFF   # Full 16-bit mask
RED_COL_MASK = 0xFFFF   # Full 16-bit mask


class Y42MDecoder(DidDecoder):
    """
    Y42M-specific decoder.

    Simplest SOCAMM design - ideal for regression sanity check.

    Geometry:
    - 16 banks (4 BG × 4 banks)
    - 65536 rows per bank (power-of-2)
    - 16384 columns per bank (power-of-2)
    - Minimal swizzling
    """

    @property
    def did(self) -> str:
        return "Y42M"

    def get_context(self) -> DecodeContext:
        """Return Y42M-specific decode context."""
        return DecodeContext(
            num_banks=NUM_BANKS,
            bank_x=BANK_X,
            bank_y=BANK_Y,
            rows_per_bank=ROW_PER_BANK,
            cols_per_bank=COL_PER_BANK,
            rows_per_section=NUM_ROW_PER_BLOCK_TOTAL,
            cols_per_section=COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK,
            num_row_sections=NUM_ROW_BLOCKS_PER_BANK,
            num_col_sections=NUM_COL_BLOCKS_PER_BANK,
            red_rows_per_bank=RED_ROWS_PER_BANK,
            red_cols_per_bank=RED_COLS_PER_BANK,
            red_row_mult=RED_ROW_MULT,
            red_col_mult=RED_COL_MULT,
            red_row_mask=RED_ROW_MASK,
            red_col_mask=RED_COL_MASK,
            ca2_inv=0,
            ca4_inv=0,
            col_offset=0,
            bank_pos_x=BANK_POS_X,
            bank_pos_y=BANK_POS_Y,
        )

    def decode_row(self, log_row: int, ctx: Optional[DecodeContext] = None) -> PhysicalRow:
        """
        Convert logical row to physical row using Y42M equations.

        Y42M has minimal swizzling - nearly direct mapping.
        """
        # Y42M: Simple direct mapping (no RA swizzling)
        phy_row_in_bank = log_row % ROW_PER_BANK

        # Calculate section position
        sec_x = phy_row_in_bank // NUM_ROW_PER_BLOCK_TOTAL
        intra_row = phy_row_in_bank % NUM_ROW_PER_BLOCK_TOTAL

        return PhysicalRow(
            phy_row_in_bank=phy_row_in_bank,
            sec_x=sec_x,
            intra_row=intra_row,
            raw={
                'log_row': log_row,
                'equation': f'{log_row} % {ROW_PER_BANK}',
                'did': 'Y42M',
                'note': 'Direct mapping (no swizzling)',
            }
        )

    def decode_column(
        self,
        log_col: int,
        burst: int = 0,
        ctx: Optional[DecodeContext] = None
    ) -> PhysicalColumn:
        """
        Convert logical column to physical column using Y42M equations.

        Y42M has minimal CP swizzling.
        """
        # Y42M: Simple direct mapping with burst
        phy_column = ((log_col << 3) | (burst & 0x7)) % COL_PER_BANK

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
                'equation': f'(({log_col} << 3) | {burst}) % {COL_PER_BANK}',
                'did': 'Y42M',
                'note': 'Minimal swizzling',
            }
        )

    def decode_bank(self, log_bank: int) -> PhysicalBank:
        """Convert logical bank to physical position."""
        if log_bank < 0 or log_bank >= NUM_BANKS:
            log_bank = log_bank % NUM_BANKS

        bx = BANK_POS_X[log_bank]
        by = BANK_POS_Y[log_bank]

        return PhysicalBank(
            bank_num=log_bank,
            bank_group=log_bank // 4,
            bx=bx,
            by=by,
            row_start=bx * ROW_PER_BANK,
            col_start=by * COL_PER_BANK,
        )

    def decode_row_repair(
        self,
        repair_element: int,
        repaired_element: int,
        bank: int,
        test_step: str = "HMFN"
    ) -> PhysicalRepair:
        """
        Convert row repair using Y42M equations.

        Y42M: Simple direct mapping (RED_ROW_MULT = 1)
        """
        if bank < 0 or bank >= NUM_BANKS:
            return PhysicalRepair(
                physical_repair_id=f"PR-ROW-INVALID-{repair_element}",
                repair_type="ROW",
                bank=bank,
                bank_group=bank // 4,
                physical_location={'mode': 'ROW_LINE', 'row': 0, 'confidence': 0.0},
                render={'layer': 'REPAIR_ROW', 'color': '#FF0000'},
                raw={'error': f'Invalid bank {bank}'},
            )

        # Y42M row repair: direct mapping
        phy_row_in_bank = (repaired_element & RED_ROW_MASK) % ROW_PER_BANK

        bx = BANK_POS_X[bank]
        by = BANK_POS_Y[bank]
        phy_row_absolute = (bx * ROW_PER_BANK) + phy_row_in_bank

        col_start = by * COL_PER_BANK
        col_end = col_start + COL_PER_BANK

        colors = {'PROBE': '#FF6600', 'BURN': '#FF9933', 'HMFN': '#1E90FF'}
        color = colors.get(test_step.upper(), '#FF6600')

        return PhysicalRepair(
            physical_repair_id=f"PR-ROW-{repair_element:04X}",
            repair_type="ROW",
            bank=bank,
            bank_group=bank // 4,
            physical_location={
                'mode': 'ROW_LINE',
                'row': phy_row_absolute,
                'column': None,
                'sec': {'sec_x': phy_row_in_bank // NUM_ROW_PER_BLOCK_TOTAL, 'sec_y': 0},
                'span': {
                    'start': {'row': phy_row_absolute, 'column': col_start},
                    'end': {'row': phy_row_absolute, 'column': col_end},
                },
                'confidence': 1.0,
            },
            render={
                'layer': 'REPAIR_ROW',
                'color': color,
                'style': {'stroke_width': 1.5, 'stroke_dash': [6, 3], 'opacity': 0.9},
                'legend_label': f'Row repair ({test_step})',
            },
            source_repair_id=f"RE-{repair_element:04X}",
            raw={
                'repair_element': repair_element,
                'repaired_element': repaired_element,
                'phy_row_in_bank': phy_row_in_bank,
                'equation': f'{repaired_element} & {hex(RED_ROW_MASK)}',
                'did': 'Y42M',
                'note': 'Direct mapping (RED_ROW_MULT=1)',
            },
        )

    def decode_col_repair(
        self,
        repair_element: int,
        repaired_element: int,
        bank: int,
        test_step: str = "HMFN"
    ) -> PhysicalRepair:
        """
        Convert column repair using Y42M equations.

        Y42M: Simple direct mapping (RED_COL_MULT = 1)
        """
        if bank < 0 or bank >= NUM_BANKS:
            return PhysicalRepair(
                physical_repair_id=f"PR-COL-INVALID-{repair_element}",
                repair_type="COLUMN",
                bank=bank,
                bank_group=bank // 4,
                physical_location={'mode': 'COL_LINE', 'column': 0, 'confidence': 0.0},
                render={'layer': 'REPAIR_COL', 'color': '#FF0000'},
                raw={'error': f'Invalid bank {bank}'},
            )

        # Y42M column repair: direct mapping
        phy_col_in_bank = (repaired_element & RED_COL_MASK) % COL_PER_BANK

        bx = BANK_POS_X[bank]
        by = BANK_POS_Y[bank]
        phy_col_absolute = (by * COL_PER_BANK) + phy_col_in_bank

        row_start = bx * ROW_PER_BANK
        row_end = row_start + ROW_PER_BANK

        colors = {'PROBE': '#0066FF', 'BURN': '#3399FF', 'HMFN': '#00B894'}
        color = colors.get(test_step.upper(), '#00B894')

        cols_per_section = COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK

        return PhysicalRepair(
            physical_repair_id=f"PR-COL-{repair_element:04X}",
            repair_type="COLUMN",
            bank=bank,
            bank_group=bank // 4,
            physical_location={
                'mode': 'COL_LINE',
                'row': None,
                'column': phy_col_absolute,
                'sec': {'sec_x': 0, 'sec_y': phy_col_in_bank // cols_per_section},
                'span': {
                    'start': {'row': row_start, 'column': phy_col_absolute},
                    'end': {'row': row_end, 'column': phy_col_absolute},
                },
                'confidence': 1.0,
            },
            render={
                'layer': 'REPAIR_COL',
                'color': color,
                'style': {'stroke_width': 1.5, 'stroke_dash': [2, 2], 'opacity': 0.9},
                'legend_label': f'Column repair ({test_step})',
            },
            source_repair_id=f"CE-{repair_element:04X}",
            raw={
                'repair_element': repair_element,
                'repaired_element': repaired_element,
                'phy_col_in_bank': phy_col_in_bank,
                'equation': f'{repaired_element} & {hex(RED_COL_MASK)}',
                'did': 'Y42M',
                'note': 'Direct mapping (RED_COL_MULT=1)',
            },
        )
