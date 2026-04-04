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

__all__ = [
    # Main viewer functions
    'create_fail_viewer',
    'create_fail_heatmap',
    'create_dq_distribution',
    'create_bank_distribution',
    'view_fails',
    'render_fail_map',

    # Utility functions
    'load_geometry',
    'load_fail_csv',
    'process_fail_data',
    'generate_bank_grid',
    'generate_bank_labels',
    'get_did_info',
    'get_bank_group',
    'generate_fail_summary',

    # Constants
    'DQ_COLORS',
    'DID_INFO',
]
