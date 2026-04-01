#!/usr/bin/env python3
"""Command-line FID lookup tool - finds fail data for given FIDs."""

import os
import sys
import subprocess

def find_fid_lis(fid: str) -> dict:
    """Look up FID and return paths and info."""
    result = {
        "fid": fid,
        "found": False,
        "lis_path": None,
        "bin_path": None,
        "fail_type": None,
        "error": None
    }

    if len(fid) < 5:
        result["error"] = "Invalid FID (too short)"
        return result

    # Path 1: Main lists directory
    lis_path = f"/vol/pedata_dft/pedft_slt/lists/{fid[:3]}/{fid}.lis"

    if os.path.exists(lis_path):
        result["found"] = True
        result["lis_path"] = lis_path

        # Read .lis file to get .bin path
        try:
            with open(lis_path, 'r') as f:
                content = f.read().strip()
                if ',' in content:
                    parts = content.split(',')
                    result["bin_path"] = parts[0]
                    result["fail_type"] = parts[1] if len(parts) > 1 else "UNKNOWN"
                else:
                    result["bin_path"] = content
        except Exception as e:
            result["error"] = str(e)

        return result

    # Path 2: Try SLT_RMA paths (need part_type)
    try:
        # Try to get part type from fid command
        part_type_cmd = subprocess.run(
            ['/u/summary/bin/fid', fid, '-s', '-parttype'],
            capture_output=True, text=True, timeout=10
        )
        part_type = part_type_cmd.stdout.strip().split('\n')[-1] if part_type_cmd.returncode == 0 else None

        if part_type:
            for base_path in [
                f"/vol/pedata_dft/pedft_slt/SLT_RMA/server_farm/{part_type.upper()}/FARM_ALL/{fid}.lis",
                f"/vol/pedata_dft/pedft_slt/SLT_RMA/validation/{part_type.upper()}/VALIDATION_ALL/{fid}.lis"
            ]:
                if os.path.exists(base_path):
                    result["found"] = True
                    result["lis_path"] = base_path

                    with open(base_path, 'r') as f:
                        content = f.read().strip()
                        if ',' in content:
                            parts = content.split(',')
                            result["bin_path"] = parts[0]
                            result["fail_type"] = parts[1] if len(parts) > 1 else "UNKNOWN"
                        else:
                            result["bin_path"] = content
                    return result
    except Exception as e:
        pass

    result["error"] = f"Could not find {fid}.lis"
    return result


def main():
    # FIDs from the FABLOT analysis
    fids = [
        "786640L:08:P05:28",
        "791152L:09:P14:15",
        "791385L:17:P08:14",
        "791648L:13:P18:20",
        "791891L:12:N03:19",
        "804017L:04:N17:13",
        "804017L:13:N02:15"
    ]

    # Allow command-line override
    if len(sys.argv) > 1:
        fids = sys.argv[1:]

    print("=" * 80)
    print("FID LOOKUP RESULTS")
    print("=" * 80)

    for fid in fids:
        result = find_fid_lis(fid)
        print(f"\nFID: {result['fid']}")
        print("-" * 40)

        if result["found"]:
            print(f"  Status: FOUND")
            print(f"  LIS Path: {result['lis_path']}")
            print(f"  BIN Path: {result['bin_path']}")
            print(f"  Fail Type: {result['fail_type']}")

            # Check if bin file exists
            if result["bin_path"] and os.path.exists(result["bin_path"]):
                size = os.path.getsize(result["bin_path"])
                print(f"  BIN Size: {size:,} bytes")
            else:
                print(f"  BIN Status: File not found or inaccessible")
        else:
            print(f"  Status: NOT FOUND")
            print(f"  Error: {result['error']}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
