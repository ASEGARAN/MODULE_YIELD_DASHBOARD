"""
Y42M Decoder Module

Y42M is the simplest SOCAMM design.
Good for regression sanity check due to:
- Minimal swizzling
- Fewer sections
- Simpler equations

Source files to port:
- y42m.h (geometry constants)
- y42m_convert.h (row/column conversion)
- y42m_redundancy.h (repair equations)
"""

from .decoder import Y42MDecoder

__all__ = ['Y42MDecoder']
