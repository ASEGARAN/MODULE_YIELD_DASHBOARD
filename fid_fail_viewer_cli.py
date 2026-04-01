#!/usr/bin/env python3
"""Command-line FID Fail Viewer - Extract fail bit data without GUI."""

import sys
import os

# Add pylib path
sys.path.insert(0, '/u/peprobe/burntools/fail_viewer')

import pylib
import numpy as np

def find_fid_bin(fid: str) -> tuple:
    """Find the .bin file path for a given FID."""
    lis_path = f"/vol/pedata_dft/pedft_slt/lists/{fid[:3]}/{fid}.lis"

    if os.path.exists(lis_path):
        with open(lis_path, 'r') as f:
            content = f.read().strip()
            if ',' in content:
                parts = content.split(',')
                return parts[0], parts[1] if len(parts) > 1 else "UNKNOWN"
            return content, "UNKNOWN"
    return None, None


def analyze_fid(fid: str, p: pylib.PyLib) -> dict:
    """Analyze fail data for a given FID."""
    result = {
        "fid": fid,
        "bin_path": None,
        "fail_type": None,
        "header_info": None,
        "num_fails": 0,
        "fail_summary": None,
        "error": None
    }

    # Find bin file
    bin_path, fail_type = find_fid_bin(fid)

    if not bin_path:
        result["error"] = f"Could not find .lis file for {fid}"
        return result

    if not os.path.exists(bin_path):
        result["error"] = f"Bin file not found: {bin_path}"
        return result

    result["bin_path"] = bin_path
    result["fail_type"] = fail_type

    try:
        # Get header info
        header_info = p.get_header_info(bin_path, "bin", 0)
        result["header_info"] = header_info

        # Extract part type from path (e.g., Y6CP)
        path_parts = bin_path.split('/')
        part_type = None
        for part in path_parts:
            if part.upper() in ['Y6CP', 'Y52Q', 'T62M', 'Y56M']:
                part_type = part.lower()
                break

        if not part_type:
            part_type = 'y6cp'  # Default

        # Change globals for part type
        p.change_globals(part_type)

        # Load the file
        # C++ signature: loadfile_all(PyLib, string, string, string, int, list, int, int, string, int, int, int, int, string, int, int)
        fails_list = []  # Empty list for fails parameter
        load_result = p.loadfile_all(
            bin_path,      # filename (string)
            part_type,     # part_type (string)
            "bin",         # file_type (string)
            0,             # swap_en (int)
            fails_list,    # fails (list)
            0,             # col_discard (int)
            0,             # row_discard (int)
            fid,           # fid_val (string)
            0,             # persist_storage (int)
            0,             # which_storage (int)
            0,             # type_send (int)
            0,             # burn_compressed (int)
            "",            # burn_comp_type (string)
            0,             # verification (int)
            0              # type_output (int)
        )

        if load_result and len(load_result) >= 2:
            result["header_list"] = load_result[0]
            result["num_fails"] = load_result[1]

        # Try to get fail array
        try:
            fail_array = p.get_entire_np_fail_array()
            if fail_array is not None:
                # Count non-zero elements (fails)
                if isinstance(fail_array, np.ndarray):
                    fail_count = np.count_nonzero(fail_array)
                    result["fail_array_shape"] = fail_array.shape
                    result["fail_count_from_array"] = fail_count
        except Exception as e:
            pass

        # Run fail crawler
        try:
            crawler_result = p.run_crawler(
                part_type,     # part_type
                0,             # which_storage
                result.get("header_list", []),  # header_list
                fid,           # fid_val
                0              # type_channel
            )
            if crawler_result:
                result["crawler_result"] = crawler_result[0]
                result["crawler_status"] = crawler_result[1]
        except Exception as e:
            result["crawler_error"] = str(e)

    except Exception as e:
        result["error"] = str(e)

    return result


def extract_key_info(header_info) -> dict:
    """Extract key fail-related fields from header info."""
    key_fields = [
        'FID_STATUS', 'FMECH', 'FAIL_MECHANISM', 'BIT_COUNT', 'ROWS', 'COLS',
        'ULOC', 'MSN', 'SUMMARY', 'DESIGN_ID', 'MODULE_SPEED', 'MODULE_DENSITY',
        'TEST_STEP', 'STEP', 'PROCESSCODE', 'MFG_WORKWEEK', 'UIN', 'UPASS',
        'UFAIL', 'UE_FAIL', 'ONDIE_UE', 'RISK_PRIORITY', 'FC_VERSION'
    ]

    extracted = {}
    if header_info and isinstance(header_info, (list, tuple)) and len(header_info) > 0:
        header_list = header_info[0]
        if isinstance(header_list, list):
            for item in header_list:
                if isinstance(item, tuple) and len(item) == 2:
                    key, value = item
                    if key in key_fields:
                        extracted[key] = value
    return extracted


def print_result(result: dict):
    """Print analysis result in formatted output."""
    print("\n" + "=" * 80)
    print(f"FID: {result['fid']}")
    print("=" * 80)

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
        return

    print(f"  Bin Path: {result['bin_path']}")
    print(f"  Fail Type: {result['fail_type']}")

    if result.get("num_fails"):
        print(f"  Number of Fails: {result['num_fails']}")

    # Extract and show key info
    if result.get("header_info"):
        key_info = extract_key_info(result["header_info"])
        if key_info:
            print(f"\n  Key Fail Information:")
            print(f"    MSN: {key_info.get('MSN', 'N/A')}")
            print(f"    Summary: {key_info.get('SUMMARY', 'N/A')}")
            print(f"    Design ID: {key_info.get('DESIGN_ID', 'N/A')}")
            print(f"    Step: {key_info.get('STEP', 'N/A')}")
            print(f"    Module Speed: {key_info.get('MODULE_SPEED', 'N/A')}")
            print(f"    Module Density: {key_info.get('MODULE_DENSITY', 'N/A')}")
            print(f"    MFG Workweek: {key_info.get('MFG_WORKWEEK', 'N/A')}")
            print(f"\n  Fail Details:")
            print(f"    FID Status: {key_info.get('FID_STATUS', 'N/A')}")
            print(f"    Fail Mechanism: {key_info.get('FMECH', key_info.get('FAIL_MECHANISM', 'N/A'))}")
            print(f"    ULOC: {key_info.get('ULOC', 'N/A')}")
            print(f"    Bit Count: {key_info.get('BIT_COUNT', 'N/A')}")
            print(f"    Rows: {key_info.get('ROWS', 'N/A')}")
            print(f"    Cols: {key_info.get('COLS', 'N/A')}")
            print(f"\n  Test Results:")
            print(f"    UIN: {key_info.get('UIN', 'N/A')}")
            print(f"    UPASS: {key_info.get('UPASS', 'N/A')}")
            print(f"    UFAIL: {key_info.get('UFAIL', 'N/A')}")
            print(f"    UE Fail: {key_info.get('UE_FAIL', 'N/A')}")
            print(f"    On-Die UE: {key_info.get('ONDIE_UE', 'N/A')}")
            print(f"    Risk Priority: {key_info.get('RISK_PRIORITY', 'N/A')}")

    if result.get("fail_array_shape"):
        print(f"\n  Fail Array:")
        print(f"    Shape: {result['fail_array_shape']}")
        print(f"    Count: {result.get('fail_count_from_array', 'N/A')}")

    if result.get("crawler_result"):
        print(f"\n  Fail Crawler:")
        print(f"    Result: {result['crawler_result']}")
        print(f"    Status: {result.get('crawler_status', 'N/A')}")


def main():
    # Default FIDs from FABLOT analysis
    default_fids = [
        "786640L:08:P05:28",
        "791152L:09:P14:15",
        "791385L:17:P08:14",
        "791648L:13:P18:20",
        "791891L:12:N03:19",
        "804017L:04:N17:13",
        "804017L:13:N02:15"
    ]

    # Use command line args or defaults
    fids = sys.argv[1:] if len(sys.argv) > 1 else default_fids

    print("=" * 80)
    print("FID FAIL VIEWER - Command Line Interface")
    print("=" * 80)
    print(f"Analyzing {len(fids)} FID(s)...")

    # Initialize pylib
    p = pylib.PyLib()

    for fid in fids:
        result = analyze_fid(fid, p)
        print_result(result)

    print("\n" + "=" * 80)
    print("Analysis Complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
