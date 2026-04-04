"""
DID-Specific Decoders for Web Fail Viewer

Architecture mirrors Fail Viewer's internal structure:
- Each DID has its own conversion equations (<did>_convert.h)
- Each DID has its own redundancy semantics (<did>_redundancy.h)
- No cross-DID generalization

Directory structure:
decoders/
├─ base.py           # DidDecoder interface
├─ y62p/             # SOCAMM/SOCAMM2 reference
│   ├─ convert.py    # from y62p_convert.h
│   ├─ redundancy.py # from y62p_redundancy.h
│   └─ decoder.py    # DidDecoder implementation
├─ y6cp/             # High-density derivative
├─ y63n/             # Next-gen LPDDR5X
└─ y42m/             # Simpler validation target
"""

from .base import (
    DidDecoder,
    DecodeContext,
    PhysicalRow,
    PhysicalColumn,
    PhysicalBank,
    PhysicalRepair,
    get_decoder,
    SUPPORTED_DIDS,
)

__all__ = [
    'DidDecoder',
    'DecodeContext',
    'PhysicalRow',
    'PhysicalColumn',
    'PhysicalBank',
    'PhysicalRepair',
    'get_decoder',
    'SUPPORTED_DIDS',
]
