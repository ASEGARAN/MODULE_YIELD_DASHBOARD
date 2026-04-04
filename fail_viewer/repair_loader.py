"""
Repair Data Loader Module

Implements the recommended architecture for loading real repair data:
1. Ingest Probe / Burn / HMFN artifacts
2. Parse logical redundancy records
3. Apply <did>.h redundancy equations
4. Generate PhysicalRepairOverlay objects

Data Sources (in order of preference):
1. Parse test artifacts directly (.bin, .csv from /vol/pedata_dft)
2. HMFN repository queries
3. CLI tools (limited, not recommended for production)

Artifact Locations:
- Burn Stress Fails: /vol/pedata_dft/pedft_slt/Burn_Images/Stress_Fails/<DID>/<lot>
- HMFN: Via mtsums/fdat95 queries

Binary Format (.bin stress fail files):
- Header: key-value pairs (length-prefixed strings)
  - die, dqs_per_die, fuse_id, num_die, num_sites, site, test_num
  - Fail_mechanism, FC_UE, dbase, LOT, FailMech_Date
- Data: Binary fail addresses starting ~offset 0xF0
"""

import subprocess
import os
import struct
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path

from .repair import (
    LogicalRepairData,
    LogicalRepairEntry,
    LogicalAddress,
    SourceArtifact,
    RepairType,
    RepairStatus,
    TestStep,
)

logger = logging.getLogger(__name__)


# =============================================================================
# ARTIFACT PATH RESOLUTION
# =============================================================================

STRESS_FAILS_BASE = "/vol/pedata_dft/pedft_slt/Burn_Images/Stress_Fails"
STRESS_DATA_BASE = "/vol/pedata_dft/pedft_slt/Burn_Images/Stress_Data"


def parse_fid(fid: str) -> Dict[str, str]:
    """
    Parse FID into components.

    FID format: FABLOT:WW:XPOS:YPOS
    Example: 785322L:14:P15:04

    Returns:
        Dict with lot, wafer, x_pos, y_pos
    """
    parts = fid.split(':')
    if len(parts) != 4:
        raise ValueError(f"Invalid FID format: {fid}")

    return {
        'lot': parts[0],        # 785322L
        'wafer': parts[1],      # 14
        'x_pos': parts[2],      # P15
        'y_pos': parts[3],      # 04
        'lot_prefix': parts[0][:3],   # 785
        'lot_suffix': parts[0][3:5],  # 32
        'lot_letter': parts[0][5:],   # 2L
    }


def find_stress_fail_artifact(fid: str, did: str) -> Optional[Path]:
    """
    Find stress fail .bin artifact for a FID.

    Path structure: /vol/pedata_dft/pedft_slt/Burn_Images/Stress_Fails/<DID>/<lot_prefix>/<lot_suffix>/<lot_letter>/<ww>/<?>/<lot>_<wafer>_<x>_<y>.bin

    Args:
        fid: Full FID string
        did: Design ID (e.g., Y6CP)

    Returns:
        Path to .bin file if found, None otherwise
    """
    try:
        fid_parts = parse_fid(fid)

        # Build expected filename
        # Example: 785077L_14_P27_13.bin
        filename = f"{fid_parts['lot']}_{fid_parts['wafer']}_{fid_parts['x_pos']}_{fid_parts['y_pos']}.bin"

        # Build search path
        base_path = Path(STRESS_FAILS_BASE) / did.upper()

        if not base_path.exists():
            logger.warning(f"DID directory not found: {base_path}")
            return None

        # Search for the file (directory structure varies)
        for root, dirs, files in os.walk(base_path):
            if filename in files:
                return Path(root) / filename

        logger.warning(f"Artifact not found for FID {fid}")
        return None

    except Exception as e:
        logger.error(f"Error finding artifact: {e}")
        return None


# =============================================================================
# BINARY ARTIFACT PARSING
# =============================================================================

@dataclass
class BinFileHeader:
    """Parsed header from .bin stress fail file."""
    die: Optional[str] = None
    dqs_per_die: Optional[int] = None
    fuse_id: Optional[str] = None
    num_die: Optional[int] = None
    num_sites: Optional[int] = None
    site: Optional[int] = None
    test_num: Optional[int] = None
    fail_mechanism: Optional[str] = None
    fc_ue: Optional[str] = None
    dbase: Optional[str] = None
    lot: Optional[str] = None
    fail_mech_date: Optional[str] = None


def parse_bin_header(data: bytes) -> Tuple[BinFileHeader, int]:
    """
    Parse header from .bin stress fail file.

    Binary format uses length-prefixed key-value pairs:
    - 2 bytes: key length
    - key string (null-terminated)
    - 2 bytes: value length
    - value string (null-terminated)

    Args:
        data: Raw binary data

    Returns:
        Tuple of (BinFileHeader, data_offset where actual fail data starts)
    """
    header = BinFileHeader()
    offset = 0

    # Skip initial bytes (format version?)
    offset = 8

    try:
        while offset < len(data) - 4:
            # Read key length
            key_len = struct.unpack_from('<H', data, offset)[0]
            offset += 2

            if key_len == 0 or offset + key_len > len(data):
                break

            # Read key
            key = data[offset:offset + key_len].rstrip(b'\x00').decode('utf-8', errors='ignore')
            offset += key_len

            # Read value length
            if offset + 2 > len(data):
                break
            val_len = struct.unpack_from('<H', data, offset)[0]
            offset += 2

            if offset + val_len > len(data):
                break

            # Read value
            value = data[offset:offset + val_len].rstrip(b'\x00').decode('utf-8', errors='ignore')
            offset += val_len

            # Map to header fields
            key_lower = key.lower()
            if key_lower == 'die':
                header.die = value
            elif key_lower == 'dqs_per_die':
                header.dqs_per_die = int(value) if value.isdigit() else None
            elif key_lower == 'fuse_id':
                header.fuse_id = value
            elif key_lower == 'num_die':
                header.num_die = int(value) if value.isdigit() else None
            elif key_lower == 'num_sites':
                header.num_sites = int(value) if value.isdigit() else None
            elif key_lower == 'site':
                header.site = int(value) if value.isdigit() else None
            elif key_lower == 'test_num':
                header.test_num = int(value) if value.isdigit() else None
            elif key_lower == 'fail_mechanism':
                header.fail_mechanism = value
            elif key_lower == 'fc_ue':
                header.fc_ue = value
            elif key_lower == 'dbase':
                header.dbase = value
            elif key_lower == 'lot':
                header.lot = value
            elif key_lower == 'failmech_date':
                header.fail_mech_date = value

            # Check for end of header markers
            if key.startswith('XXXXXXXX'):
                break

    except Exception as e:
        logger.warning(f"Error parsing bin header: {e}")

    return header, offset


def parse_bin_repair_data(data: bytes, offset: int, did: str) -> List[Dict]:
    """
    Parse repair/fail data from binary artifact.

    This is DID-specific parsing. The exact format depends on the DID.

    Args:
        data: Raw binary data
        offset: Offset where fail data starts
        did: Design ID for format selection

    Returns:
        List of repair dictionaries with bank, row, col, type
    """
    repairs = []

    # Binary format varies by DID - this is a basic implementation
    # that extracts 8-byte records (common format)
    record_size = 8

    while offset + record_size <= len(data):
        record = data[offset:offset + record_size]

        # Extract row:col:dq format (varies by DID)
        # This is a simplified extraction - real implementation
        # needs DID-specific decoding
        if len(record) >= 6:
            # Common format: 3 bytes row, 2 bytes col, 1 byte dq/bank
            row = int.from_bytes(record[0:3], 'little') & 0x3FFFFF
            col = int.from_bytes(record[3:5], 'little') & 0xFFFF
            bank_dq = record[5] if len(record) > 5 else 0
            bank = bank_dq >> 4
            dq = bank_dq & 0x0F

            if row > 0 or col > 0:  # Skip empty records
                repairs.append({
                    'row': row,
                    'col': col,
                    'bank': bank,
                    'dq': dq,
                    'raw': record.hex()
                })

        offset += record_size

    return repairs


# =============================================================================
# REPAIR DATA EXTRACTION FROM MTSUMS/FDAT95
# =============================================================================

def get_repair_info_from_mtsums(test_summary: str, fid: str, timeout: int = 60) -> Dict:
    """
    Get repair-related information from mtsums.

    Extracts ADDRMASK, ADDRCNT, ROWCNT, COLCNT which indicate repair patterns.

    Args:
        test_summary: Test summary string
        fid: FID string
        timeout: Command timeout

    Returns:
        Dict with repair metadata
    """
    try:
        cmd = f'mtsums {test_summary} -fid=/{fid}/ +quiet -format=FID,DESIGN,STEP,FAILCRAWLER,ADDRMASK,ADDRCNT,ROWCNT,COLCNT 2>/dev/null | grep -v "^FID" | grep -v "^~" | head -1'

        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )

        if not result.stdout.strip():
            return {}

        # Parse output
        fields = result.stdout.strip().split()
        if len(fields) >= 8:
            return {
                'fid': fields[0],
                'design': fields[1],
                'step': fields[2],
                'failcrawler': fields[3],
                'addrmask': fields[4],
                'addrcnt': int(fields[5]) if fields[5].isdigit() else 0,
                'rowcnt': int(fields[6]) if fields[6].isdigit() else 0,
                'colcnt': int(fields[7]) if fields[7].isdigit() else 0,
            }

        return {}

    except Exception as e:
        logger.error(f"Error querying mtsums: {e}")
        return {}


def get_repair_from_fdat95(test_summary: str, fid: str, timeout: int = 120) -> List[Dict]:
    """
    Attempt to extract repair info from fdat95 +rp output.

    Note: fdat95 +rp provides limited repair information.
    This is NOT the recommended method for full repair data.

    Args:
        test_summary: Test summary string
        fid: FID string
        timeout: Command timeout

    Returns:
        List of partial repair records
    """
    try:
        cmd = f"fdat95 {test_summary} -fgrp=/{fid}/ +rp +archive 2>/dev/null"

        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )

        repairs = []
        for line in result.stdout.strip().split('\n'):
            if line.startswith('%RP'):
                # Parse %RP line - format is limited
                parts = line.split()
                if len(parts) >= 2:
                    repairs.append({
                        'fid': parts[1],
                        'source': 'fdat95',
                        'limited': True
                    })

        return repairs

    except Exception as e:
        logger.error(f"Error querying fdat95: {e}")
        return []


# =============================================================================
# MAIN LOADER FUNCTIONS
# =============================================================================

def load_repair_data(
    fid: str,
    did: str,
    test_step: str = "HMFN",
    test_summary: Optional[str] = None,
    artifact_path: Optional[str] = None
) -> Optional[LogicalRepairData]:
    """
    Load real repair data from available sources.

    Priority:
    1. Direct artifact path (if provided)
    2. Auto-locate artifact in Stress_Fails
    3. Query mtsums for metadata
    4. Return None if no data found

    Args:
        fid: FID string
        did: Design ID
        test_step: Test step (PROBE, BURN, HMFN)
        test_summary: Optional test summary for CLI queries
        artifact_path: Optional direct path to artifact

    Returns:
        LogicalRepairData or None
    """
    repairs = []
    source_artifact = None

    # Try 1: Direct artifact path
    if artifact_path:
        path = Path(artifact_path)
        if path.exists() and path.suffix == '.bin':
            try:
                data = path.read_bytes()
                header, offset = parse_bin_header(data)
                raw_repairs = parse_bin_repair_data(data, offset, did)

                source_artifact = SourceArtifact(
                    type="binary",
                    path=str(path),
                    timestamp=None
                )

                # Convert raw repairs to LogicalRepairEntry
                for i, r in enumerate(raw_repairs):
                    # Determine repair type based on pattern
                    # (This is simplified - real implementation needs DID-specific logic)
                    repair_type = RepairType.ROW if r.get('col', 0) == 0 else RepairType.COLUMN

                    repairs.append(LogicalRepairEntry(
                        repair_id=f"BIN-{i:04d}",
                        repair_type=repair_type,
                        logical_address=LogicalAddress(
                            bank=r.get('bank', 0),
                            row=r.get('row') if repair_type == RepairType.ROW else None,
                            column=r.get('col') if repair_type == RepairType.COLUMN else None
                        ),
                        repair_status=RepairStatus.APPLIED,
                        origin_test_step=TestStep(test_step),
                        metadata={'raw': r.get('raw', '')}
                    ))

                logger.info(f"Loaded {len(repairs)} repairs from artifact: {path}")

            except Exception as e:
                logger.error(f"Error parsing artifact {path}: {e}")

    # Try 2: Auto-locate artifact
    if not repairs:
        auto_path = find_stress_fail_artifact(fid, did)
        if auto_path:
            # Recursive call with found path
            return load_repair_data(fid, did, test_step, test_summary, str(auto_path))

    # Try 3: Get metadata from mtsums (limited info)
    if not repairs and test_summary:
        mtsums_info = get_repair_info_from_mtsums(test_summary, fid)
        if mtsums_info:
            # ADDRMASK can indicate repair patterns but not actual coordinates
            logger.info(f"Got repair metadata from mtsums: {mtsums_info}")
            source_artifact = SourceArtifact(
                type="database",
                path=f"mtsums:{test_summary}",
                timestamp=None
            )

            # Can't generate actual repairs from just metadata
            # But we can flag that repair info exists
            if mtsums_info.get('rowcnt', 0) > 0 or mtsums_info.get('colcnt', 0) > 0:
                repairs.append(LogicalRepairEntry(
                    repair_id="MTSUMS-META",
                    repair_type=RepairType.ROW if mtsums_info.get('rowcnt', 0) > 0 else RepairType.COLUMN,
                    logical_address=LogicalAddress(bank=0, row=None, column=None),
                    repair_status=RepairStatus.APPLIED,
                    origin_test_step=TestStep(test_step),
                    metadata={
                        'source': 'mtsums_metadata',
                        'addrmask': mtsums_info.get('addrmask'),
                        'rowcnt': mtsums_info.get('rowcnt'),
                        'colcnt': mtsums_info.get('colcnt'),
                        'note': 'Metadata only - actual repair coordinates require artifact parsing'
                    }
                ))

    if repairs:
        return LogicalRepairData(
            fid=fid,
            did=did,
            test_step=TestStep(test_step),
            repairs=repairs,
            source_artifact=source_artifact
        )

    return None


def get_available_repair_sources(fid: str, did: str) -> Dict[str, Any]:
    """
    Check which repair data sources are available for a FID.

    Args:
        fid: FID string
        did: Design ID

    Returns:
        Dict with availability status for each source
    """
    sources = {
        'stress_fail_artifact': False,
        'stress_fail_path': None,
        'mtsums_available': False,
        'recommendations': []
    }

    # Check stress fail artifact
    artifact_path = find_stress_fail_artifact(fid, did)
    if artifact_path:
        sources['stress_fail_artifact'] = True
        sources['stress_fail_path'] = str(artifact_path)
        sources['recommendations'].append('Use stress fail .bin artifact (recommended)')

    # Check if DID is supported
    did_upper = did.upper()
    supported_dids = ['Y62P', 'Y6CP', 'Y63N', 'Y42M']
    if did_upper in supported_dids:
        sources['did_supported'] = True
    else:
        sources['did_supported'] = False
        sources['recommendations'].append(f'DID {did} may not have full repair support')

    if not sources['recommendations']:
        sources['recommendations'].append('No repair artifacts found - use mock data for demonstration')

    return sources
