"""
Y6CP Decoder Module

Y6CP is a high-density derivative of Y62P.
Key differences from Y62P:
- Higher row count: 103896 vs 68340
- Different section counts
- May have dual-spine adjustments

Source files ported:
- y6cp.h (geometry constants)
- y6cp_convert.h (row/column conversion)
- y6cp_redundancy.h (repair equations)
"""

from .decoder import Y6CPDecoder
from .convert import (
    decode_row,
    decode_column,
    log_to_phy_row,
    log_to_phy_col,
)
from .redundancy import (
    decode_row_repair,
    decode_col_repair,
)

__all__ = [
    'Y6CPDecoder',
    'decode_row',
    'decode_column',
    'log_to_phy_row',
    'log_to_phy_col',
    'decode_row_repair',
    'decode_col_repair',
]
