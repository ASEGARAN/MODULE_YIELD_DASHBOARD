"""
Repair Data Module for Web Fail Viewer

Two-schema architecture:
1. LogicalRepair - Raw repair data from artifacts (tool-agnostic)
2. PhysicalRepairOverlay - Rendered coordinates after applying DID equations

This matches Fail Viewer's internal architecture where:
- Artifacts are parsed independently of DID knowledge
- DID equations (<did>.h) convert logical → physical
- Physical coordinates drive the overlay rendering

Supported DIDs:
- Y6CP: via y6cp_equations.py (ported from CLI Fail Viewer)
- Y62P: (future - similar to Y6CP)
- Y63N: (future)
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================

class RepairType(str, Enum):
    ROW = "ROW"
    COLUMN = "COLUMN"


class RepairStatus(str, Enum):
    ENABLED = "ENABLED"
    BLOWN = "BLOWN"
    APPLIED = "APPLIED"


class RepairScope(str, Enum):
    DIE = "DIE"
    PACKAGE = "PACKAGE"
    MODULE = "MODULE"


class TestStep(str, Enum):
    PROBE = "PROBE"
    BURN = "BURN"
    HMFN = "HMFN"


class SpaceType(str, Enum):
    DIE_PHYSICAL = "DIE_PHYSICAL"
    FAILVIEWER_GRID = "FAILVIEWER_GRID"


class RenderMode(str, Enum):
    ROW_LINE = "ROW_LINE"
    COL_LINE = "COL_LINE"
    RECT_REGION = "RECT_REGION"
    POLYLINE = "POLYLINE"


class RenderLayer(str, Enum):
    REPAIR_ROW = "REPAIR_ROW"
    REPAIR_COL = "REPAIR_COL"
    REPAIR_REGION = "REPAIR_REGION"


# =============================================================================
# LOGICAL REPAIR SCHEMA (Layer 1 - Artifact Parsing)
# =============================================================================

@dataclass
class LogicalAddress:
    """Logical address for a repair (bank, row, column)."""
    bank: int
    row: Optional[int] = None
    column: Optional[int] = None

    def to_dict(self) -> Dict:
        return {"bank": self.bank, "row": self.row, "column": self.column}


@dataclass
class SourceArtifact:
    """Source artifact information for repair data."""
    type: str  # "binary", "csv", "database"
    path: Optional[str] = None
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict:
        return {"type": self.type, "path": self.path, "timestamp": self.timestamp}


@dataclass
class LogicalRepairEntry:
    """Single logical repair entry (before DID mapping)."""
    repair_id: str
    repair_type: RepairType
    logical_address: LogicalAddress
    repair_status: RepairStatus = RepairStatus.APPLIED
    repair_scope: RepairScope = RepairScope.DIE
    origin_test_step: Optional[TestStep] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "repair_id": self.repair_id,
            "repair_type": self.repair_type.value if isinstance(self.repair_type, RepairType) else self.repair_type,
            "logical_address": self.logical_address.to_dict() if isinstance(self.logical_address, LogicalAddress) else self.logical_address,
            "repair_status": self.repair_status.value if isinstance(self.repair_status, RepairStatus) else self.repair_status,
            "repair_scope": self.repair_scope.value if isinstance(self.repair_scope, RepairScope) else self.repair_scope,
            "origin_test_step": self.origin_test_step.value if isinstance(self.origin_test_step, TestStep) else self.origin_test_step,
            "metadata": self.metadata
        }


@dataclass
class LogicalRepairData:
    """Full logical repair data structure for a FID (Layer 1)."""
    fid: str
    did: str
    test_step: TestStep
    repairs: List[LogicalRepairEntry] = field(default_factory=list)
    source_artifact: Optional[SourceArtifact] = None

    def to_dict(self) -> Dict:
        return {
            "fid": self.fid,
            "did": self.did,
            "test_step": self.test_step.value if isinstance(self.test_step, TestStep) else self.test_step,
            "source_artifact": self.source_artifact.to_dict() if self.source_artifact else None,
            "repairs": [r.to_dict() for r in self.repairs]
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: Dict) -> 'LogicalRepairData':
        """Create LogicalRepairData from dictionary."""
        repairs = []
        for r in data.get("repairs", []):
            addr = r.get("logical_address", {})
            repairs.append(LogicalRepairEntry(
                repair_id=r.get("repair_id", ""),
                repair_type=RepairType(r.get("repair_type", "ROW")),
                logical_address=LogicalAddress(
                    bank=addr.get("bank", 0),
                    row=addr.get("row"),
                    column=addr.get("column")
                ),
                repair_status=RepairStatus(r.get("repair_status", "APPLIED")),
                repair_scope=RepairScope(r.get("repair_scope", "DIE")),
                origin_test_step=TestStep(r["origin_test_step"]) if r.get("origin_test_step") else None,
                metadata=r.get("metadata", {})
            ))

        source = data.get("source_artifact")
        source_artifact = SourceArtifact(
            type=source.get("type", "unknown"),
            path=source.get("path"),
            timestamp=source.get("timestamp")
        ) if source else None

        return cls(
            fid=data.get("fid", ""),
            did=data.get("did", ""),
            test_step=TestStep(data.get("test_step", "HMFN")),
            repairs=repairs,
            source_artifact=source_artifact
        )


# =============================================================================
# PHYSICAL REPAIR OVERLAY SCHEMA (Layer 2 - After DID Mapping)
# =============================================================================

@dataclass
class AxisConvention:
    """Fail Viewer axis convention: rows vertical, cols horizontal."""
    rows_direction: str = "VERTICAL"
    cols_direction: str = "HORIZONTAL"

    def to_dict(self) -> Dict:
        return {"rows_direction": self.rows_direction, "cols_direction": self.cols_direction}


@dataclass
class GridInfo:
    """Optional grid/section information for Fail Viewer style rendering."""
    bank_layout: Optional[str] = None
    sec_x_count: Optional[int] = None
    sec_y_count: Optional[int] = None

    def to_dict(self) -> Dict:
        result = {}
        if self.bank_layout:
            result["bank_layout"] = self.bank_layout
        if self.sec_x_count:
            result["sec_x_count"] = self.sec_x_count
        if self.sec_y_count:
            result["sec_y_count"] = self.sec_y_count
        return result


@dataclass
class CoordinateSpace:
    """Coordinate space definition for physical overlay."""
    space_type: SpaceType = SpaceType.FAILVIEWER_GRID
    axis_convention: AxisConvention = field(default_factory=AxisConvention)
    grid: Optional[GridInfo] = None

    def to_dict(self) -> Dict:
        result = {
            "space_type": self.space_type.value if isinstance(self.space_type, SpaceType) else self.space_type,
            "axis_convention": self.axis_convention.to_dict()
        }
        if self.grid:
            result["grid"] = self.grid.to_dict()
        return result


@dataclass
class MappingProvenance:
    """Traceability to mapping equations / DID header versions."""
    did_header: str
    did_header_revision: Optional[int] = None
    mapping_variant: Optional[str] = None

    def to_dict(self) -> Dict:
        result = {"did_header": self.did_header}
        if self.did_header_revision is not None:
            result["did_header_revision"] = self.did_header_revision
        if self.mapping_variant:
            result["mapping_variant"] = self.mapping_variant
        return result


@dataclass
class SectionCoord:
    """Section/block coordinates in Fail Viewer style."""
    sec_x: int
    sec_y: int
    intra_x: Optional[int] = None
    intra_y: Optional[int] = None

    def to_dict(self) -> Dict:
        result = {"sec_x": self.sec_x, "sec_y": self.sec_y}
        if self.intra_x is not None:
            result["intra_x"] = self.intra_x
        if self.intra_y is not None:
            result["intra_y"] = self.intra_y
        return result


@dataclass
class SpanCoord:
    """Start/end coordinates for line/region overlays."""
    row: Optional[int] = None
    column: Optional[int] = None

    def to_dict(self) -> Dict:
        return {"row": self.row, "column": self.column}


@dataclass
class Span:
    """Defines start/end for line/region overlays."""
    start: SpanCoord
    end: SpanCoord

    def to_dict(self) -> Dict:
        return {"start": self.start.to_dict(), "end": self.end.to_dict()}


@dataclass
class PhysicalLocation:
    """Physical coordinates suitable for drawing overlay lines/regions."""
    mode: RenderMode
    row: Optional[int] = None
    column: Optional[int] = None
    sec: Optional[SectionCoord] = None
    span: Optional[Span] = None
    confidence: float = 1.0

    def to_dict(self) -> Dict:
        result = {
            "mode": self.mode.value if isinstance(self.mode, RenderMode) else self.mode,
            "row": self.row,
            "column": self.column,
            "confidence": self.confidence
        }
        if self.sec:
            result["sec"] = self.sec.to_dict()
        if self.span:
            result["span"] = self.span.to_dict()
        return result


@dataclass
class RenderStyle:
    """Rendering style properties."""
    stroke_width: float = 1.5
    stroke_dash: Optional[List[float]] = None
    opacity: float = 0.9

    def to_dict(self) -> Dict:
        result = {"stroke_width": self.stroke_width, "opacity": self.opacity}
        if self.stroke_dash:
            result["stroke_dash"] = self.stroke_dash
        return result


@dataclass
class RenderProperties:
    """Rendering properties for a physical repair overlay."""
    layer: RenderLayer
    color: str
    style: RenderStyle
    legend_label: Optional[str] = None

    def to_dict(self) -> Dict:
        result = {
            "layer": self.layer.value if isinstance(self.layer, RenderLayer) else self.layer,
            "color": self.color,
            "style": self.style.to_dict()
        }
        if self.legend_label:
            result["legend_label"] = self.legend_label
        return result


@dataclass
class PhysicalRepair:
    """Single physical repair overlay element (after DID mapping)."""
    physical_repair_id: str
    repair_type: RepairType
    status: RepairStatus
    bank: int
    physical_location: PhysicalLocation
    render: RenderProperties
    source_repair_id: Optional[str] = None
    bank_group: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict:
        result = {
            "physical_repair_id": self.physical_repair_id,
            "repair_type": self.repair_type.value if isinstance(self.repair_type, RepairType) else self.repair_type,
            "status": self.status.value if isinstance(self.status, RepairStatus) else self.status,
            "bank": self.bank,
            "physical_location": self.physical_location.to_dict(),
            "render": self.render.to_dict()
        }
        if self.source_repair_id:
            result["source_repair_id"] = self.source_repair_id
        if self.bank_group is not None:
            result["bank_group"] = self.bank_group
        if self.raw:
            result["raw"] = self.raw
        return result


@dataclass
class PhysicalRepairOverlay:
    """Full physical repair overlay for a FID (Layer 2 - after DID mapping)."""
    fid: str
    did: str
    test_step: TestStep
    coordinate_space: CoordinateSpace
    overlay_repairs: List[PhysicalRepair] = field(default_factory=list)
    mapping_provenance: Optional[MappingProvenance] = None

    def to_dict(self) -> Dict:
        result = {
            "fid": self.fid,
            "did": self.did,
            "test_step": self.test_step.value if isinstance(self.test_step, TestStep) else self.test_step,
            "coordinate_space": self.coordinate_space.to_dict(),
            "overlay_repairs": [r.to_dict() for r in self.overlay_repairs]
        }
        if self.mapping_provenance:
            result["mapping_provenance"] = self.mapping_provenance.to_dict()
        return result

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# =============================================================================
# REPAIR COLORS & STYLES
# =============================================================================

# Default colors by repair type and test step
REPAIR_COLORS = {
    "ROW": {
        "PROBE": "#FF6600",    # Orange - Probe row repair
        "BURN": "#FF9933",     # Light orange - Burn row repair
        "HMFN": "#1E90FF",     # Dodger blue - HMFN row repair (per schema example)
        "default": "#FF6600"
    },
    "COLUMN": {
        "PROBE": "#0066FF",    # Blue - Probe column repair
        "BURN": "#3399FF",     # Light blue - Burn column repair
        "HMFN": "#00B894",     # Green - HMFN column repair (per schema example)
        "default": "#00B894"
    }
}

# Default dash patterns
REPAIR_DASH_PATTERNS = {
    "ROW": [6, 3],      # Longer dashes for rows
    "COLUMN": [2, 2],   # Shorter dashes for columns
}


def get_repair_color(repair_type: str, test_step: Optional[str] = None) -> str:
    """Get color for a repair based on type and source."""
    type_colors = REPAIR_COLORS.get(repair_type.upper(), REPAIR_COLORS["ROW"])
    if test_step:
        return type_colors.get(test_step.upper(), type_colors["default"])
    return type_colors["default"]


def get_repair_dash(repair_type: str) -> List[float]:
    """Get dash pattern for a repair type."""
    return REPAIR_DASH_PATTERNS.get(repair_type.upper(), [6, 3])


# =============================================================================
# MAPPING FUNCTIONS (Logical → Physical)
# =============================================================================

def get_bank_group(bank_num: int) -> int:
    """Get bank group from bank number (4 banks per group)."""
    return bank_num // 4


def apply_did_equations(
    logical_data: LogicalRepairData,
    geometry=None
) -> PhysicalRepairOverlay:
    """
    Apply DID-specific redundancy equations to convert logical to physical coordinates.

    Uses the decoder architecture (decoders/<did>/) for equation isolation:
    - Y62P: decoders/y62p/ (SOCAMM reference, 68340 rows)
    - Y6CP: decoders/y6cp/ (high-density, 103896 rows)
    - Y63N: decoders/y63n/ (next-gen LPDDR5X)
    - Y42M: decoders/y42m/ (simplest, sanity check)

    For ROW repairs: Creates horizontal bar across the bank width
    For COLUMN repairs: Creates vertical bar across the bank height

    Args:
        logical_data: LogicalRepairData with logical addresses
        geometry: Optional DID geometry module (auto-detected from DID if None)

    Returns:
        PhysicalRepairOverlay with physical coordinates and render properties
    """
    did = logical_data.did.upper()

    # Try decoder architecture first (preferred)
    try:
        from .decoders import get_decoder
        decoder = get_decoder(did)
        if decoder:
            return _apply_decoder_equations(logical_data, decoder)
    except ImportError:
        logger.debug("Decoder architecture not available, falling back")

    # Fallback: Use legacy Y6CP equations for Y6CP
    if did == "Y6CP":
        return _apply_y6cp_equations(logical_data)
    elif did in ("Y62P", "Y63N"):
        logger.info(f"Using Y6CP-like geometry for {did}")
        from . import y6cp_equations as geom
        geometry = geom
    elif geometry is None:
        logger.warning(f"No DID-specific equations for {did}, using defaults")
        geometry = _get_default_geometry()

    # Generic implementation for DIDs without specific equations
    return _apply_generic_equations(logical_data, geometry)


def _apply_decoder_equations(
    logical_data: LogicalRepairData,
    decoder
) -> PhysicalRepairOverlay:
    """
    Apply DID equations using the decoder architecture.

    This is the preferred path - uses isolated per-DID decoders.
    """
    ctx = decoder.get_context()
    did = decoder.did

    # Create coordinate space
    coord_space = CoordinateSpace(
        space_type=SpaceType.FAILVIEWER_GRID,
        axis_convention=AxisConvention(rows_direction="VERTICAL", cols_direction="HORIZONTAL"),
        grid=GridInfo(
            bank_layout=f"{ctx.bank_x}x{ctx.bank_y}",
            sec_x_count=ctx.bank_x,
            sec_y_count=ctx.bank_y
        )
    )

    # Create mapping provenance
    provenance = MappingProvenance(
        did_header=f"{did.lower()}.h",
        did_header_revision=1,
        mapping_variant="decoder_architecture"
    )

    # Convert each logical repair using the decoder
    physical_repairs = []
    test_step_str = logical_data.test_step.value if isinstance(logical_data.test_step, TestStep) else logical_data.test_step

    for repair in logical_data.repairs:
        bank = repair.logical_address.bank
        if bank >= ctx.num_banks:
            logger.warning(f"Bank {bank} exceeds {did} NUM_BANKS={ctx.num_banks}")
            continue

        repair_type_val = repair.repair_type.value if isinstance(repair.repair_type, RepairType) else repair.repair_type
        repair_status_val = repair.repair_status.value if isinstance(repair.repair_status, RepairStatus) else repair.repair_status

        # Use decoder to convert repair
        if repair_type_val == "ROW":
            log_row = repair.logical_address.row or 0
            decoded = decoder.decode_row_repair(
                repair_element=0,  # Not available from LogicalRepairEntry
                repaired_element=log_row,
                bank=bank,
                test_step=test_step_str
            )
        else:  # COLUMN
            log_col = repair.logical_address.column or 0
            decoded = decoder.decode_col_repair(
                repair_element=0,
                repaired_element=log_col,
                bank=bank,
                test_step=test_step_str
            )

        # Convert decoder result to PhysicalRepair
        physical_repair = PhysicalRepair(
            physical_repair_id=f"PR-{repair_type_val}-{repair.repair_id}",
            source_repair_id=repair.repair_id,
            repair_type=RepairType(repair_type_val),
            status=RepairStatus(repair_status_val),
            bank=decoded.bank,
            bank_group=decoded.bank_group,
            physical_location=PhysicalLocation(
                mode=RenderMode.ROW_LINE if repair_type_val == "ROW" else RenderMode.COL_LINE,
                row=decoded.physical_location.get('row'),
                column=decoded.physical_location.get('column'),
                span=Span(
                    start=SpanCoord(
                        row=decoded.physical_location.get('span', {}).get('start', {}).get('row'),
                        column=decoded.physical_location.get('span', {}).get('start', {}).get('column')
                    ),
                    end=SpanCoord(
                        row=decoded.physical_location.get('span', {}).get('end', {}).get('row'),
                        column=decoded.physical_location.get('span', {}).get('end', {}).get('column')
                    )
                ) if decoded.physical_location.get('span') else None,
                confidence=decoded.physical_location.get('confidence', 1.0)
            ),
            render=RenderProperties(
                layer=RenderLayer(decoded.render.get('layer', 'REPAIR_ROW')),
                color=decoded.render.get('color', '#FF6600'),
                style=RenderStyle(
                    stroke_width=decoded.render.get('style', {}).get('stroke_width', 1.5),
                    stroke_dash=decoded.render.get('style', {}).get('stroke_dash'),
                    opacity=decoded.render.get('style', {}).get('opacity', 0.9)
                ),
                legend_label=decoded.render.get('legend_label')
            ),
            raw=decoded.raw
        )

        physical_repairs.append(physical_repair)

    return PhysicalRepairOverlay(
        fid=logical_data.fid,
        did=logical_data.did,
        test_step=logical_data.test_step,
        coordinate_space=coord_space,
        overlay_repairs=physical_repairs,
        mapping_provenance=provenance
    )


def _get_default_geometry():
    """Return default geometry constants."""
    class DefaultGeometry:
        ROW_PER_BANK = 68340
        COL_PER_BANK = 17808
        NUM_BANKS = 16
        BANK_X = 2
        BANK_Y = 8
        BANK_POS_X = [0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0]
        BANK_POS_Y = [7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0, 1, 0]
    return DefaultGeometry()


def _apply_y6cp_equations(logical_data: LogicalRepairData) -> PhysicalRepairOverlay:
    """
    Apply Y6CP-specific redundancy equations from y6cp_equations.py.

    Uses the ported equations from CLI Fail Viewer's:
    - y6cp.h (geometry constants)
    - redundancy.h (y6cp_row_redun, y6cp_col_redun)
    """
    from . import y6cp_equations as y6cp

    # Create coordinate space with Y6CP geometry
    coord_space = CoordinateSpace(
        space_type=SpaceType.FAILVIEWER_GRID,
        axis_convention=AxisConvention(rows_direction="VERTICAL", cols_direction="HORIZONTAL"),
        grid=GridInfo(
            bank_layout=f"{y6cp.BANK_X}x{y6cp.BANK_Y}",
            sec_x_count=y6cp.BANK_X,
            sec_y_count=y6cp.BANK_Y
        )
    )

    # Create mapping provenance
    provenance = MappingProvenance(
        did_header="y6cp.h",
        did_header_revision=y6cp.DECODE_REV,
        mapping_variant="web_failviewer_port"
    )

    # Convert each logical repair to physical using Y6CP equations
    physical_repairs = []
    test_step_str = logical_data.test_step.value if isinstance(logical_data.test_step, TestStep) else logical_data.test_step

    for repair in logical_data.repairs:
        bank = repair.logical_address.bank
        if bank >= y6cp.NUM_BANKS:
            logger.warning(f"Bank {bank} exceeds Y6CP NUM_BANKS={y6cp.NUM_BANKS}")
            continue

        repair_type_val = repair.repair_type.value if isinstance(repair.repair_type, RepairType) else repair.repair_type
        repair_status_val = repair.repair_status.value if isinstance(repair.repair_status, RepairStatus) else repair.repair_status

        # Get bank positions
        bx = y6cp.BANK_POS_X[bank]
        by = y6cp.BANK_POS_Y[bank]

        if repair_type_val == "ROW":
            # Use Y6CP row redundancy equation
            log_row = repair.logical_address.row or 0

            # Apply Y6CP row equation: phy_row = (log_row & RED_ROW_MASK) * RED_ROW_MULT
            row_masked = log_row & y6cp.RED_ROW_MASK
            phy_row_in_bank = (row_masked * y6cp.RED_ROW_MULT) % y6cp.ROW_PER_BANK
            phys_row = (bx * y6cp.ROW_PER_BANK) + phy_row_in_bank

            # Column span for row repair (full bank width)
            col_start = by * y6cp.COL_PER_BANK
            col_end = col_start + y6cp.COL_PER_BANK

            physical_location = PhysicalLocation(
                mode=RenderMode.ROW_LINE,
                row=phys_row,
                column=None,
                span=Span(
                    start=SpanCoord(row=phys_row, column=col_start),
                    end=SpanCoord(row=phys_row, column=col_end)
                ),
                confidence=1.0
            )

            render = RenderProperties(
                layer=RenderLayer.REPAIR_ROW,
                color=get_repair_color("ROW", test_step_str),
                style=RenderStyle(
                    stroke_width=1.5,
                    stroke_dash=get_repair_dash("ROW"),
                    opacity=0.9
                ),
                legend_label=f"Row repair ({test_step_str})"
            )

            raw_data = {
                "log_row": log_row,
                "row_masked": row_masked,
                "phy_row_in_bank": phy_row_in_bank,
                "equation": f"({log_row} & {hex(y6cp.RED_ROW_MASK)}) * {y6cp.RED_ROW_MULT}"
            }

        else:  # COLUMN
            # Use Y6CP column redundancy equation
            log_col = repair.logical_address.column or 0

            # Apply Y6CP col equation: phy_col = ((log_col & FULL_RED_COL_MASK) >> 4) * RED_COL_MULT
            col_masked = (log_col & y6cp.FULL_RED_COL_MASK) >> 4
            phy_col_in_bank = (col_masked * y6cp.RED_COL_MULT) % y6cp.COL_PER_BANK
            phys_col = (by * y6cp.COL_PER_BANK) + phy_col_in_bank

            # Row span for column repair (full bank height)
            row_start = bx * y6cp.ROW_PER_BANK
            row_end = row_start + y6cp.ROW_PER_BANK

            physical_location = PhysicalLocation(
                mode=RenderMode.COL_LINE,
                row=None,
                column=phys_col,
                span=Span(
                    start=SpanCoord(row=row_start, column=phys_col),
                    end=SpanCoord(row=row_end, column=phys_col)
                ),
                confidence=1.0
            )

            render = RenderProperties(
                layer=RenderLayer.REPAIR_COL,
                color=get_repair_color("COLUMN", test_step_str),
                style=RenderStyle(
                    stroke_width=1.5,
                    stroke_dash=get_repair_dash("COLUMN"),
                    opacity=0.9
                ),
                legend_label=f"Column repair ({test_step_str})"
            )

            raw_data = {
                "log_col": log_col,
                "col_masked": col_masked,
                "phy_col_in_bank": phy_col_in_bank,
                "equation": f"(({log_col} & {hex(y6cp.FULL_RED_COL_MASK)}) >> 4) * {y6cp.RED_COL_MULT}"
            }

        physical_repair = PhysicalRepair(
            physical_repair_id=f"PR-{repair_type_val}-{repair.repair_id}",
            source_repair_id=repair.repair_id,
            repair_type=RepairType(repair_type_val),
            status=RepairStatus(repair_status_val),
            bank=bank,
            bank_group=get_bank_group(bank),
            physical_location=physical_location,
            render=render,
            raw=raw_data
        )

        physical_repairs.append(physical_repair)

    return PhysicalRepairOverlay(
        fid=logical_data.fid,
        did=logical_data.did,
        test_step=logical_data.test_step,
        coordinate_space=coord_space,
        overlay_repairs=physical_repairs,
        mapping_provenance=provenance
    )


def _apply_generic_equations(
    logical_data: LogicalRepairData,
    geometry
) -> PhysicalRepairOverlay:
    """
    Generic implementation for DIDs without specific equations.

    Uses simple modulo-based address mapping.
    """
    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    num_banks = getattr(geometry, 'NUM_BANKS', 16)
    bank_pos_x = getattr(geometry, 'BANK_POS_X', [0] * num_banks)
    bank_pos_y = getattr(geometry, 'BANK_POS_Y', list(range(num_banks)))

    # Create coordinate space
    coord_space = CoordinateSpace(
        space_type=SpaceType.FAILVIEWER_GRID,
        axis_convention=AxisConvention(rows_direction="VERTICAL", cols_direction="HORIZONTAL"),
        grid=GridInfo(
            bank_layout=f"{getattr(geometry, 'BANK_X', 2)}x{getattr(geometry, 'BANK_Y', 8)}",
            sec_x_count=getattr(geometry, 'BANK_X', 2),
            sec_y_count=getattr(geometry, 'BANK_Y', 8)
        )
    )

    # Create mapping provenance
    provenance = MappingProvenance(
        did_header=f"{logical_data.did.lower()}_generic.py",
        did_header_revision=1,
        mapping_variant="generic"
    )

    # Convert each logical repair to physical
    physical_repairs = []
    test_step_str = logical_data.test_step.value if isinstance(logical_data.test_step, TestStep) else logical_data.test_step

    for repair in logical_data.repairs:
        bank = repair.logical_address.bank
        if bank >= num_banks:
            continue

        # Get bank position in grid
        bx = bank_pos_x[bank] if bank < len(bank_pos_x) else 0
        by = bank_pos_y[bank] if bank < len(bank_pos_y) else 0

        # Bank boundaries in physical coordinates
        bank_row_start = bx * row_per_bank
        bank_col_start = by * col_per_bank

        repair_type = repair.repair_type.value if isinstance(repair.repair_type, RepairType) else repair.repair_type
        repair_status = repair.repair_status.value if isinstance(repair.repair_status, RepairStatus) else repair.repair_status

        if repair_type == "ROW":
            # ROW repair: horizontal line at the row address, spanning full column width
            row = repair.logical_address.row or 0
            row_in_bank = row % row_per_bank
            phys_row = bank_row_start + row_in_bank

            physical_location = PhysicalLocation(
                mode=RenderMode.ROW_LINE,
                row=phys_row,
                column=None,
                span=Span(
                    start=SpanCoord(row=phys_row, column=bank_col_start),
                    end=SpanCoord(row=phys_row, column=bank_col_start + col_per_bank)
                ),
                confidence=1.0
            )

            render = RenderProperties(
                layer=RenderLayer.REPAIR_ROW,
                color=get_repair_color("ROW", test_step_str),
                style=RenderStyle(
                    stroke_width=1.5,
                    stroke_dash=get_repair_dash("ROW"),
                    opacity=0.9
                ),
                legend_label=f"Row repair ({test_step_str})"
            )

            raw_data = {"phy_row_in_bank": row_in_bank, "bank_row_start": bank_row_start}

        else:  # COLUMN
            # COLUMN repair: vertical line at the column address, spanning full row height
            col = repair.logical_address.column or 0
            col_in_bank = col % col_per_bank
            phys_col = bank_col_start + col_in_bank

            physical_location = PhysicalLocation(
                mode=RenderMode.COL_LINE,
                row=None,
                column=phys_col,
                span=Span(
                    start=SpanCoord(row=bank_row_start, column=phys_col),
                    end=SpanCoord(row=bank_row_start + row_per_bank, column=phys_col)
                ),
                confidence=1.0
            )

            render = RenderProperties(
                layer=RenderLayer.REPAIR_COL,
                color=get_repair_color("COLUMN", test_step_str),
                style=RenderStyle(
                    stroke_width=1.5,
                    stroke_dash=get_repair_dash("COLUMN"),
                    opacity=0.9
                ),
                legend_label=f"Column repair ({test_step_str})"
            )

            raw_data = {"phy_col_in_bank": col_in_bank, "bank_col_start": bank_col_start}

        physical_repair = PhysicalRepair(
            physical_repair_id=f"PR-{repair_type}-{repair.repair_id}",
            source_repair_id=repair.repair_id,
            repair_type=RepairType(repair_type),
            status=RepairStatus(repair_status),
            bank=bank,
            bank_group=get_bank_group(bank),
            physical_location=physical_location,
            render=render,
            raw=raw_data
        )

        physical_repairs.append(physical_repair)

    return PhysicalRepairOverlay(
        fid=logical_data.fid,
        did=logical_data.did,
        test_step=logical_data.test_step,
        coordinate_space=coord_space,
        overlay_repairs=physical_repairs,
        mapping_provenance=provenance
    )


# =============================================================================
# PLOTLY TRACE GENERATION
# =============================================================================

def generate_repair_traces(overlay: PhysicalRepairOverlay) -> List[Dict]:
    """
    Generate Plotly trace data for repair overlays.

    Creates line traces for each repair using the render properties
    from the PhysicalRepairOverlay schema.

    Args:
        overlay: PhysicalRepairOverlay with physical coordinates

    Returns:
        List of Plotly trace dicts
    """
    traces = []

    # Group by layer for legend management
    layers_seen = set()

    for repair in overlay.overlay_repairs:
        loc = repair.physical_location
        render = repair.render
        style = render.style

        # Determine if this is the first trace for this layer (for legend)
        layer_key = render.layer.value if isinstance(render.layer, RenderLayer) else render.layer
        show_legend = layer_key not in layers_seen
        layers_seen.add(layer_key)

        # Get coordinates from span
        if loc.span:
            x_coords = [loc.span.start.row, loc.span.end.row]
            y_coords = [loc.span.start.column, loc.span.end.column]
        else:
            # Fallback to simple point
            x_coords = [loc.row or 0]
            y_coords = [loc.column or 0]

        # Build line properties
        line_props = {
            'color': render.color,
            'width': style.stroke_width
        }
        if style.stroke_dash:
            # Convert dash pattern to Plotly format
            line_props['dash'] = 'dash'

        # Build hover template
        repair_type = repair.repair_type.value if isinstance(repair.repair_type, RepairType) else repair.repair_type
        status = repair.status.value if isinstance(repair.status, RepairStatus) else repair.status

        hover_text = (
            f"<b>{repair_type} Repair {repair.physical_repair_id}</b><br>"
            f"Bank: {repair.bank} (BG{repair.bank_group})<br>"
            f"Status: {status}<br>"
        )
        if repair_type == "ROW" and loc.row is not None:
            hover_text += f"Row: {loc.row}<br>"
        elif repair_type == "COLUMN" and loc.column is not None:
            hover_text += f"Column: {loc.column}<br>"
        hover_text += "<extra></extra>"

        trace = {
            'type': 'scatter',
            'x': x_coords,
            'y': y_coords,
            'mode': 'lines',
            'line': line_props,
            'name': render.legend_label or f"{repair_type} Repair",
            'showlegend': show_legend,
            'legendgroup': layer_key,
            'hovertemplate': hover_text,
            'opacity': style.opacity
        }

        traces.append(trace)

    return traces


# =============================================================================
# SUMMARY & UTILITY FUNCTIONS
# =============================================================================

def get_repair_summary(overlay: PhysicalRepairOverlay) -> Dict:
    """
    Generate summary statistics for repair overlay.

    Args:
        overlay: PhysicalRepairOverlay object

    Returns:
        Dictionary with summary statistics
    """
    repairs = overlay.overlay_repairs

    row_repairs = [r for r in repairs if r.repair_type in (RepairType.ROW, "ROW")]
    col_repairs = [r for r in repairs if r.repair_type in (RepairType.COLUMN, "COLUMN")]

    # Count by bank
    bank_counts = {}
    for r in repairs:
        bank_counts[r.bank] = bank_counts.get(r.bank, 0) + 1

    # Count by bank group
    bg_counts = {}
    for r in repairs:
        bg = r.bank_group if r.bank_group is not None else get_bank_group(r.bank)
        bg_counts[bg] = bg_counts.get(bg, 0) + 1

    test_step = overlay.test_step.value if isinstance(overlay.test_step, TestStep) else overlay.test_step

    return {
        'total_repairs': len(repairs),
        'row_repairs': len(row_repairs),
        'column_repairs': len(col_repairs),
        'banks_with_repairs': len(bank_counts),
        'repairs_by_bank': bank_counts,
        'repairs_by_bank_group': bg_counts,
        'fid': overlay.fid,
        'did': overlay.did,
        'test_step': test_step,
        'coordinate_space': overlay.coordinate_space.space_type.value if isinstance(overlay.coordinate_space.space_type, SpaceType) else overlay.coordinate_space.space_type
    }


def create_mock_logical_repair_data(fid: str, did: str, test_step: str = "HMFN") -> LogicalRepairData:
    """
    Create mock logical repair data for testing.

    Args:
        fid: FID string
        did: Design ID
        test_step: Test step

    Returns:
        LogicalRepairData with sample repairs
    """
    repairs = [
        # Sample row repairs
        LogicalRepairEntry(
            repair_id="R001",
            repair_type=RepairType.ROW,
            logical_address=LogicalAddress(bank=0, row=10000, column=None),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        LogicalRepairEntry(
            repair_id="R002",
            repair_type=RepairType.ROW,
            logical_address=LogicalAddress(bank=3, row=50000, column=None),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        LogicalRepairEntry(
            repair_id="R003",
            repair_type=RepairType.ROW,
            logical_address=LogicalAddress(bank=7, row=30000, column=None),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        LogicalRepairEntry(
            repair_id="R004",
            repair_type=RepairType.ROW,
            logical_address=LogicalAddress(bank=12, row=45000, column=None),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        # Sample column repairs
        LogicalRepairEntry(
            repair_id="C001",
            repair_type=RepairType.COLUMN,
            logical_address=LogicalAddress(bank=1, row=None, column=5000),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        LogicalRepairEntry(
            repair_id="C002",
            repair_type=RepairType.COLUMN,
            logical_address=LogicalAddress(bank=5, row=None, column=12000),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        LogicalRepairEntry(
            repair_id="C003",
            repair_type=RepairType.COLUMN,
            logical_address=LogicalAddress(bank=10, row=None, column=8000),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
        LogicalRepairEntry(
            repair_id="C004",
            repair_type=RepairType.COLUMN,
            logical_address=LogicalAddress(bank=14, row=None, column=15000),
            repair_status=RepairStatus.APPLIED,
            origin_test_step=TestStep(test_step)
        ),
    ]

    return LogicalRepairData(
        fid=fid,
        did=did,
        test_step=TestStep(test_step),
        repairs=repairs,
        source_artifact=SourceArtifact(type="mock", path=None, timestamp=None)
    )


# Backward compatibility alias
def create_mock_repair_data(fid: str, did: str, test_step: str = "HMFN") -> LogicalRepairData:
    """Alias for create_mock_logical_repair_data (backward compatibility)."""
    return create_mock_logical_repair_data(fid, did, test_step)


# Legacy aliases for backward compatibility
RepairData = LogicalRepairData
RepairEntry = LogicalRepairEntry
