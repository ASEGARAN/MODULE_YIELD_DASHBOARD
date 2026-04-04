# Fail Viewer Module for Module Yield Dashboard
# Web-based Plotly implementation of Micron's DRAM Fail Viewer
#
# DRAM Hierarchy (Fail Viewer abstraction):
# - 4 Bank Groups × 4 Banks = 16 total banks
# - Rows are vertical (X-axis), Columns are horizontal (Y-axis)
#
# Supported DIDs:
# - Y62P: SOCAMM/SOCAMM2 reference, medium density
# - Y6CP: High-density derivative of Y62P
# - Y63N: Next-gen high-speed LPDDR5X SOCAMM2

from .viewer import (
    create_fail_viewer,
    create_fail_heatmap,
    create_dq_distribution,
    create_bank_distribution,
    view_fails,
    render_fail_map,  # Legacy
    add_repair_overlay,
    create_fail_viewer_with_repairs,
    DQ_COLORS
)

from .utils import (
    load_geometry,
    load_fail_csv,
    process_fail_data,
    generate_bank_grid,
    generate_bank_labels,
    get_did_info,
    get_bank_group,
    generate_fail_summary,
    DID_INFO
)

from .repair_loader import (
    load_repair_data,
    get_available_repair_sources,
    find_stress_fail_artifact,
    get_repair_info_from_mtsums,
    parse_fid,
)

from .y6cp_equations import (
    # Geometry constants
    NUM_BANKS as Y6CP_NUM_BANKS,
    ROW_PER_BANK as Y6CP_ROW_PER_BANK,
    COL_PER_BANK as Y6CP_COL_PER_BANK,
    RED_ROW_MULT as Y6CP_RED_ROW_MULT,
    RED_COL_MULT as Y6CP_RED_COL_MULT,
    RED_ROWS_PER_BANK as Y6CP_RED_ROWS_PER_BANK,
    RED_COLS_PER_BANK as Y6CP_RED_COLS_PER_BANK,
    BANK_POS_X as Y6CP_BANK_POS_X,
    BANK_POS_Y as Y6CP_BANK_POS_Y,
    # Redundancy equations
    y6cp_row_redun,
    y6cp_col_redun,
    # Conversion functions
    convert_row_repair_to_physical as y6cp_convert_row_repair,
    convert_col_repair_to_physical as y6cp_convert_col_repair,
    log_to_physical_row as y6cp_log_to_physical_row,
    log_to_physical_col as y6cp_log_to_physical_col,
    physical_to_logical as y6cp_physical_to_logical,
    # Repan parsing
    parse_repan_line,
    parse_repan_csv,
)

from .repair import (
    # Logical Repair Schema (Layer 1)
    LogicalRepairData,
    LogicalRepairEntry,
    LogicalAddress,
    SourceArtifact,
    # Physical Repair Overlay Schema (Layer 2)
    PhysicalRepairOverlay,
    PhysicalRepair,
    PhysicalLocation,
    CoordinateSpace,
    MappingProvenance,
    RenderProperties,
    RenderStyle,
    Span,
    SpanCoord,
    # Enums
    RepairType,
    RepairStatus,
    RepairScope,
    TestStep,
    SpaceType,
    RenderMode,
    RenderLayer,
    # Functions
    apply_did_equations,
    generate_repair_traces,
    get_repair_summary,
    create_mock_repair_data,
    create_mock_logical_repair_data,
    get_repair_color,
    get_bank_group,
    # Constants
    REPAIR_COLORS,
    REPAIR_DASH_PATTERNS,
    # Legacy aliases
    RepairData,
    RepairEntry,
)

# DID-specific decoders (per-DID equation isolation)
from .decoders import (
    DidDecoder,
    DecodeContext,
    get_decoder,
    SUPPORTED_DIDS,
)

__all__ = [
    # Repair loader functions
    'load_repair_data',
    'get_available_repair_sources',
    'find_stress_fail_artifact',
    'get_repair_info_from_mtsums',
    'parse_fid',

    # Y6CP geometry and equations
    'Y6CP_NUM_BANKS',
    'Y6CP_ROW_PER_BANK',
    'Y6CP_COL_PER_BANK',
    'Y6CP_RED_ROW_MULT',
    'Y6CP_RED_COL_MULT',
    'Y6CP_RED_ROWS_PER_BANK',
    'Y6CP_RED_COLS_PER_BANK',
    'Y6CP_BANK_POS_X',
    'Y6CP_BANK_POS_Y',
    'y6cp_row_redun',
    'y6cp_col_redun',
    'y6cp_convert_row_repair',
    'y6cp_convert_col_repair',
    'y6cp_log_to_physical_row',
    'y6cp_log_to_physical_col',
    'y6cp_physical_to_logical',
    'parse_repan_line',
    'parse_repan_csv',

    # Main viewer functions
    'create_fail_viewer',
    'create_fail_heatmap',
    'create_dq_distribution',
    'create_bank_distribution',
    'view_fails',
    'render_fail_map',
    'add_repair_overlay',
    'create_fail_viewer_with_repairs',

    # Utility functions
    'load_geometry',
    'load_fail_csv',
    'process_fail_data',
    'generate_bank_grid',
    'generate_bank_labels',
    'get_did_info',
    'get_bank_group',
    'generate_fail_summary',

    # Logical Repair Schema (Layer 1)
    'LogicalRepairData',
    'LogicalRepairEntry',
    'LogicalAddress',
    'SourceArtifact',

    # Physical Repair Overlay Schema (Layer 2)
    'PhysicalRepairOverlay',
    'PhysicalRepair',
    'PhysicalLocation',
    'CoordinateSpace',
    'MappingProvenance',
    'RenderProperties',
    'RenderStyle',
    'Span',
    'SpanCoord',

    # Enums
    'RepairType',
    'RepairStatus',
    'RepairScope',
    'TestStep',
    'SpaceType',
    'RenderMode',
    'RenderLayer',

    # Repair functions
    'apply_did_equations',
    'generate_repair_traces',
    'get_repair_summary',
    'create_mock_repair_data',
    'create_mock_logical_repair_data',
    'get_repair_color',
    'get_bank_group',

    # Constants
    'DQ_COLORS',
    'DID_INFO',
    'REPAIR_COLORS',
    'REPAIR_DASH_PATTERNS',

    # Legacy aliases
    'RepairData',
    'RepairEntry',

    # DID-specific decoders
    'DidDecoder',
    'DecodeContext',
    'get_decoder',
    'SUPPORTED_DIDS',
]
