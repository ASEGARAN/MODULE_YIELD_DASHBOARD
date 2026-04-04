"""
DID Decoder Base Interface

Defines the interface that each DID decoder must implement.
This mirrors Fail Viewer's per-DID structure where each DID
has its own conversion and redundancy equations.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass
class DecodeContext:
    """
    Context for decode operations.

    Contains DID-specific geometry and configuration that
    the decoder needs to perform conversions.
    """
    # Bank geometry
    num_banks: int = 16
    bank_x: int = 2
    bank_y: int = 8

    # Per-bank dimensions
    rows_per_bank: int = 68340
    cols_per_bank: int = 17808

    # Section/block dimensions
    rows_per_section: int = 1332
    cols_per_section: int = 989
    num_row_sections: int = 78
    num_col_sections: int = 18

    # Redundancy parameters
    red_rows_per_bank: int = 86
    red_cols_per_bank: int = 512
    red_row_mult: int = 4
    red_col_mult: int = 16

    # Masks
    red_row_mask: int = 0x1FFFC
    red_col_mask: int = 0xFFF0

    # Column inversion flags (DID-specific)
    ca2_inv: int = 0
    ca4_inv: int = 0
    col_offset: int = 0

    # Bank position arrays
    bank_pos_x: List[int] = field(default_factory=lambda: [0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0])
    bank_pos_y: List[int] = field(default_factory=lambda: [7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0, 1, 0])


@dataclass
class PhysicalRow:
    """Result of row decode operation."""
    phy_row_in_bank: int
    sec_x: int
    intra_row: int
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PhysicalColumn:
    """Result of column decode operation."""
    phy_column: int
    sec_y: int
    intra_col: int
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PhysicalBank:
    """Result of bank decode operation."""
    bank_num: int
    bank_group: int
    bx: int  # X position in bank array
    by: int  # Y position in bank array
    row_start: int  # Physical row start
    col_start: int  # Physical column start


@dataclass
class PhysicalRepair:
    """
    Result of repair decode operation.

    Maps directly to the PhysicalRepair schema in repair.py
    """
    physical_repair_id: str
    repair_type: str  # "ROW" or "COLUMN"
    bank: int
    bank_group: int
    physical_location: Dict[str, Any]
    render: Dict[str, Any]
    source_repair_id: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


class DidDecoder(ABC):
    """
    Abstract base class for DID-specific decoders.

    Each DID implementation must provide:
    - Geometry constants
    - Row conversion equations (from <did>_convert.h)
    - Column conversion equations (from <did>_convert.h)
    - Redundancy equations (from <did>_redundancy.h)

    DO NOT generalize across DIDs - each has unique equations.
    """

    @property
    @abstractmethod
    def did(self) -> str:
        """Return the DID this decoder handles (e.g., 'Y62P')."""
        pass

    @abstractmethod
    def get_context(self) -> DecodeContext:
        """Return the decode context with DID-specific geometry."""
        pass

    @abstractmethod
    def decode_row(self, log_row: int, ctx: Optional[DecodeContext] = None) -> PhysicalRow:
        """
        Convert logical row to physical row.

        Implements equations from <did>_convert.h:
        - RA swizzling
        - Bank-relative positioning
        - Section calculation

        Args:
            log_row: Logical row address
            ctx: Optional context override

        Returns:
            PhysicalRow with coordinates and debug info
        """
        pass

    @abstractmethod
    def decode_column(
        self,
        log_col: int,
        burst: int = 0,
        ctx: Optional[DecodeContext] = None
    ) -> PhysicalColumn:
        """
        Convert logical column to physical column.

        Implements equations from <did>_convert.h:
        - CP swizzling
        - Burst decoding
        - Column plane inversion

        Args:
            log_col: Logical column address
            burst: Burst position (0-7 typically)
            ctx: Optional context override

        Returns:
            PhysicalColumn with coordinates and debug info
        """
        pass

    @abstractmethod
    def decode_bank(self, log_bank: int) -> PhysicalBank:
        """
        Convert logical bank to physical bank position.

        Args:
            log_bank: Logical bank number (0-15 typically)

        Returns:
            PhysicalBank with position info
        """
        pass

    @abstractmethod
    def decode_row_repair(
        self,
        repair_element: int,
        repaired_element: int,
        bank: int,
        test_step: str = "HMFN"
    ) -> PhysicalRepair:
        """
        Convert logical row repair to physical overlay.

        Implements equations from <did>_redundancy.h

        Args:
            repair_element: Redundant element (fuse address)
            repaired_element: Logical row being repaired
            bank: Bank number
            test_step: Origin test step for styling

        Returns:
            PhysicalRepair ready for overlay rendering
        """
        pass

    @abstractmethod
    def decode_col_repair(
        self,
        repair_element: int,
        repaired_element: int,
        bank: int,
        test_step: str = "HMFN"
    ) -> PhysicalRepair:
        """
        Convert logical column repair to physical overlay.

        Implements equations from <did>_redundancy.h

        Args:
            repair_element: Redundant element (fuse address)
            repaired_element: Logical column being repaired
            bank: Bank number
            test_step: Origin test step for styling

        Returns:
            PhysicalRepair ready for overlay rendering
        """
        pass


# Registry of available decoders
_DECODER_REGISTRY: Dict[str, type] = {}

SUPPORTED_DIDS = ['Y62P', 'Y6CP', 'Y63N', 'Y42M']


def register_decoder(decoder_class: type) -> type:
    """Decorator to register a decoder class."""
    did = decoder_class.did.fget(None) if hasattr(decoder_class.did, 'fget') else None
    if did is None:
        # Try to instantiate to get DID
        try:
            instance = decoder_class()
            did = instance.did
        except Exception:
            pass
    if did:
        _DECODER_REGISTRY[did.upper()] = decoder_class
    return decoder_class


def get_decoder(did: str) -> Optional[DidDecoder]:
    """
    Get decoder instance for a DID.

    Args:
        did: Design ID (e.g., 'Y62P', 'Y6CP')

    Returns:
        DidDecoder instance or None if not supported
    """
    did_upper = did.upper()

    # Lazy import to avoid circular dependencies
    if did_upper not in _DECODER_REGISTRY:
        try:
            if did_upper == 'Y62P':
                from .y62p.decoder import Y62PDecoder
                _DECODER_REGISTRY['Y62P'] = Y62PDecoder
            elif did_upper == 'Y6CP':
                from .y6cp.decoder import Y6CPDecoder
                _DECODER_REGISTRY['Y6CP'] = Y6CPDecoder
            elif did_upper == 'Y63N':
                from .y63n.decoder import Y63NDecoder
                _DECODER_REGISTRY['Y63N'] = Y63NDecoder
            elif did_upper == 'Y42M':
                from .y42m.decoder import Y42MDecoder
                _DECODER_REGISTRY['Y42M'] = Y42MDecoder
        except ImportError as e:
            # Decoder not yet implemented
            return None

    decoder_class = _DECODER_REGISTRY.get(did_upper)
    if decoder_class:
        return decoder_class()
    return None
