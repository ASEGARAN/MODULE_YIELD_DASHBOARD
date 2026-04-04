"""
Y62P Decoder Implementation

Implements DidDecoder interface for Y62P (SOCAMM/SOCAMM2 reference).

This is the baseline decoder - Y6CP and Y63N are derived from this.
"""

from typing import Optional, Dict, Any

from ..base import (
    DidDecoder,
    DecodeContext,
    PhysicalRow,
    PhysicalColumn,
    PhysicalBank,
    PhysicalRepair,
)

from .convert import (
    NUM_BANKS,
    BANK_X,
    BANK_Y,
    ROW_PER_BANK,
    COL_PER_BANK,
    NUM_ROW_PER_BLOCK_TOTAL,
    NUM_COL_BLOCKS_PER_BANK,
    BANK_POS_X,
    BANK_POS_Y,
    decode_row as convert_decode_row,
    decode_column as convert_decode_column,
    decode_bank as convert_decode_bank,
)

from .redundancy import (
    RED_ROWS_PER_BANK,
    RED_COLS_PER_BANK,
    RED_ROW_MULT,
    RED_COL_MULT,
    RED_ROW_MASK,
    RED_COL_MASK,
    decode_row_repair as redun_decode_row_repair,
    decode_col_repair as redun_decode_col_repair,
    generate_row_repair_overlay,
    generate_col_repair_overlay,
)


class Y62PDecoder(DidDecoder):
    """
    Y62P-specific decoder.

    Geometry:
    - 16 banks (4 BG × 4 banks)
    - 68340 rows per bank
    - 17808 columns per bank
    - Bank layout: 2×8

    Sources:
    - y62p.h
    - y62p_convert.h
    - y62p_redundancy.h
    """

    @property
    def did(self) -> str:
        return "Y62P"

    def get_context(self) -> DecodeContext:
        """Return Y62P-specific decode context."""
        return DecodeContext(
            num_banks=NUM_BANKS,
            bank_x=BANK_X,
            bank_y=BANK_Y,
            rows_per_bank=ROW_PER_BANK,
            cols_per_bank=COL_PER_BANK,
            rows_per_section=NUM_ROW_PER_BLOCK_TOTAL,
            cols_per_section=COL_PER_BANK // NUM_COL_BLOCKS_PER_BANK,
            num_row_sections=ROW_PER_BANK // NUM_ROW_PER_BLOCK_TOTAL,
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
        Convert logical row to physical row using Y62P equations.

        From y62p_convert.h - includes RA13/RA14 swizzling.
        """
        result = convert_decode_row(log_row)
        return PhysicalRow(
            phy_row_in_bank=result.phy_row_in_bank,
            sec_x=result.sec_x,
            intra_row=result.intra_row,
            raw=result.raw,
        )

    def decode_column(
        self,
        log_col: int,
        burst: int = 0,
        ctx: Optional[DecodeContext] = None
    ) -> PhysicalColumn:
        """
        Convert logical column to physical column using Y62P equations.

        From y62p_convert.h - includes CP swizzling.
        """
        context = ctx or self.get_context()
        result = convert_decode_column(
            log_col,
            burst,
            ca2_inv=context.ca2_inv,
            ca4_inv=context.ca4_inv,
            col_offset=context.col_offset,
        )
        return PhysicalColumn(
            phy_column=result.phy_column,
            sec_y=result.sec_y,
            intra_col=result.intra_col,
            raw=result.raw,
        )

    def decode_bank(self, log_bank: int) -> PhysicalBank:
        """Convert logical bank to physical position."""
        result = convert_decode_bank(log_bank)
        return PhysicalBank(
            bank_num=result['bank_num'],
            bank_group=result['bank_group'],
            bx=result['bx'],
            by=result['by'],
            row_start=result['row_start'],
            col_start=result['col_start'],
        )

    def decode_row_repair(
        self,
        repair_element: int,
        repaired_element: int,
        bank: int,
        test_step: str = "HMFN"
    ) -> PhysicalRepair:
        """
        Convert logical row repair to physical overlay.

        From y62p_redundancy.h - row repair equation.
        """
        overlay = generate_row_repair_overlay(
            repair_element, repaired_element, bank, test_step
        )

        if overlay is None:
            # Return invalid repair
            return PhysicalRepair(
                physical_repair_id=f"PR-ROW-INVALID-{repair_element}",
                repair_type="ROW",
                bank=bank,
                bank_group=bank // 4,
                physical_location={
                    'mode': 'ROW_LINE',
                    'row': 0,
                    'column': None,
                    'span': None,
                    'confidence': 0.0,
                },
                render={
                    'layer': 'REPAIR_ROW',
                    'color': '#FF0000',
                    'style': {'stroke_width': 1.0, 'opacity': 0.5},
                },
                raw={'error': 'Invalid repair'},
            )

        return PhysicalRepair(
            physical_repair_id=f"PR-ROW-{repair_element:04X}",
            repair_type="ROW",
            bank=overlay['bank'],
            bank_group=overlay['bank_group'],
            physical_location=overlay['physical_location'],
            render=overlay['render'],
            source_repair_id=f"RE-{repair_element:04X}",
            raw=overlay['raw'],
        )

    def decode_col_repair(
        self,
        repair_element: int,
        repaired_element: int,
        bank: int,
        test_step: str = "HMFN"
    ) -> PhysicalRepair:
        """
        Convert logical column repair to physical overlay.

        From y62p_redundancy.h - column repair equation.
        """
        overlay = generate_col_repair_overlay(
            repair_element, repaired_element, bank, test_step
        )

        if overlay is None:
            # Return invalid repair
            return PhysicalRepair(
                physical_repair_id=f"PR-COL-INVALID-{repair_element}",
                repair_type="COLUMN",
                bank=bank,
                bank_group=bank // 4,
                physical_location={
                    'mode': 'COL_LINE',
                    'row': None,
                    'column': 0,
                    'span': None,
                    'confidence': 0.0,
                },
                render={
                    'layer': 'REPAIR_COL',
                    'color': '#FF0000',
                    'style': {'stroke_width': 1.0, 'opacity': 0.5},
                },
                raw={'error': 'Invalid repair'},
            )

        return PhysicalRepair(
            physical_repair_id=f"PR-COL-{repair_element:04X}",
            repair_type="COLUMN",
            bank=overlay['bank'],
            bank_group=overlay['bank_group'],
            physical_location=overlay['physical_location'],
            render=overlay['render'],
            source_repair_id=f"CE-{repair_element:04X}",
            raw=overlay['raw'],
        )
