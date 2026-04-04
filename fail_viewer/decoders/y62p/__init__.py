"""
Y62P Decoder Module

Y62P is the SOCAMM/SOCAMM2 reference design.
This serves as the baseline for Y6CP and Y63N.

Source files ported:
- y62p.h (geometry constants)
- y62p_convert.h (row/column conversion)
- y62p_redundancy.h (repair equations)
"""

from .decoder import Y62PDecoder
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
    'Y62PDecoder',
    'decode_row',
    'decode_column',
    'log_to_phy_row',
    'log_to_phy_col',
    'decode_row_repair',
    'decode_col_repair',
]
