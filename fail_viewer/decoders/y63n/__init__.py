"""
Y63N Decoder Module

Y63N is the next-gen high-speed LPDDR5X SOCAMM2.
Derived from Y6CP baseline with:
- High-speed column equations
- Potential CP inversion differences

Source files to port:
- y63n.h (geometry constants)
- y63n_convert.h (row/column conversion)
- y63n_redundancy.h (repair equations)

TODO: Port actual equations from y63n sources
"""

from .decoder import Y63NDecoder

__all__ = ['Y63NDecoder']
