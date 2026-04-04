#!/usr/bin/env python3
"""
Y6CP Decoder Validation Script

Compare web Fail Viewer repair overlays against native Fail Viewer.

USAGE:
1. Load a FID in native Fail Viewer
2. Note the repair row/column positions
3. Run this script with the same FID and repair addresses
4. Compare the physical row/column values

Example:
    python validate_y6cp.py --bank 3 --row 0x5678 --type row
    python validate_y6cp.py --bank 5 --col 0xABCD --type col
"""

import argparse
import sys
sys.path.insert(0, '/home/asegaran/MODULE_YIELD_DASHBOARD')

from fail_viewer.decoders import get_decoder


def validate_row_repair(bank: int, repaired_element: int):
    """Validate row repair against expected equation."""
    decoder = get_decoder('Y6CP')
    ctx = decoder.get_context()

    # Apply Y6CP row equation
    row_masked = repaired_element & ctx.red_row_mask
    expected_phy = (row_masked * ctx.red_row_mult) % ctx.rows_per_bank

    # Get decoder result
    repair = decoder.decode_row_repair(
        repair_element=0,
        repaired_element=repaired_element,
        bank=bank,
        test_step='HMFN'
    )

    bx = ctx.bank_pos_x[bank]
    actual_row = repair.physical_location.get('row', 0)
    actual_phy_in_bank = actual_row - (bx * ctx.rows_per_bank)

    print(f"\n{'='*60}")
    print(f"Y6CP ROW REPAIR VALIDATION")
    print(f"{'='*60}")
    print(f"Input:")
    print(f"  Bank: {bank}")
    print(f"  Repaired element: {hex(repaired_element)} ({repaired_element})")
    print(f"\nEquation:")
    print(f"  ({hex(repaired_element)} & {hex(ctx.red_row_mask)}) * {ctx.red_row_mult}")
    print(f"  = {hex(row_masked)} * {ctx.red_row_mult}")
    print(f"  = {expected_phy}")
    print(f"\nResult:")
    print(f"  Bank position BX: {bx}")
    print(f"  Phy row in bank: {expected_phy}")
    print(f"  Phy row absolute: {actual_row}")
    print(f"\nFor Native Fail Viewer comparison:")
    print(f"  -> Row {expected_phy} in bank {bank}")
    print(f"  -> Absolute row {actual_row}")
    print(f"{'='*60}\n")

    return {
        'bank': bank,
        'repaired_element': repaired_element,
        'phy_row_in_bank': expected_phy,
        'phy_row_absolute': actual_row,
    }


def validate_col_repair(bank: int, repaired_element: int):
    """Validate column repair against expected equation."""
    decoder = get_decoder('Y6CP')
    ctx = decoder.get_context()

    # Apply Y6CP column equation
    col_masked = (repaired_element & ctx.red_col_mask) >> 4
    expected_phy = (col_masked * ctx.red_col_mult) % ctx.cols_per_bank

    # Get decoder result
    repair = decoder.decode_col_repair(
        repair_element=0,
        repaired_element=repaired_element,
        bank=bank,
        test_step='HMFN'
    )

    by = ctx.bank_pos_y[bank]
    actual_col = repair.physical_location.get('column', 0)
    actual_phy_in_bank = actual_col - (by * ctx.cols_per_bank) if actual_col else 0

    print(f"\n{'='*60}")
    print(f"Y6CP COLUMN REPAIR VALIDATION")
    print(f"{'='*60}")
    print(f"Input:")
    print(f"  Bank: {bank}")
    print(f"  Repaired element: {hex(repaired_element)} ({repaired_element})")
    print(f"\nEquation:")
    print(f"  (({hex(repaired_element)} & {hex(ctx.red_col_mask)}) >> 4) * {ctx.red_col_mult}")
    print(f"  = ({hex(repaired_element & ctx.red_col_mask)} >> 4) * {ctx.red_col_mult}")
    print(f"  = {hex(col_masked)} * {ctx.red_col_mult}")
    print(f"  = {expected_phy}")
    print(f"\nResult:")
    print(f"  Bank position BY: {by}")
    print(f"  Phy col in bank: {expected_phy}")
    print(f"  Phy col absolute: {actual_col}")
    print(f"\nFor Native Fail Viewer comparison:")
    print(f"  -> Column {expected_phy} in bank {bank}")
    print(f"  -> Absolute column {actual_col}")
    print(f"{'='*60}\n")

    return {
        'bank': bank,
        'repaired_element': repaired_element,
        'phy_col_in_bank': expected_phy,
        'phy_col_absolute': actual_col,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Validate Y6CP repair equations against native Fail Viewer'
    )
    parser.add_argument('--bank', type=int, required=True, help='Bank number (0-15)')
    parser.add_argument('--row', type=lambda x: int(x, 0), help='Row address (hex or decimal)')
    parser.add_argument('--col', type=lambda x: int(x, 0), help='Column address (hex or decimal)')
    parser.add_argument('--type', choices=['row', 'col'], required=True, help='Repair type')

    args = parser.parse_args()

    if args.type == 'row':
        if args.row is None:
            print("Error: --row required for row repair validation")
            sys.exit(1)
        validate_row_repair(args.bank, args.row)
    else:
        if args.col is None:
            print("Error: --col required for column repair validation")
            sys.exit(1)
        validate_col_repair(args.bank, args.col)


if __name__ == '__main__':
    main()
