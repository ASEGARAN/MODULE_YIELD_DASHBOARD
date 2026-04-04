"""
Demo script for Web Fail Viewer

Creates sample fail data and renders visualizations.
Run from MODULE_YIELD_DASHBOARD directory:
    python -m fail_viewer.demo
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fail_viewer import (
    create_fail_viewer,
    create_fail_heatmap,
    create_dq_distribution,
    create_bank_distribution,
    load_geometry
)


def generate_sample_fail_data(n_fails: int = 1000, part_type: str = 'y62p') -> pd.DataFrame:
    """
    Generate sample fail data for testing.

    Creates realistic-looking fail patterns including:
    - Random scattered fails
    - Some column-plane-like patterns
    - Some row-aligned patterns
    """
    geometry = load_geometry(part_type)

    row_per_bank = getattr(geometry, 'ROW_PER_BANK', 68340)
    col_per_bank = getattr(geometry, 'COL_PER_BANK', 17808)
    num_banks = getattr(geometry, 'NUM_BANKS', 16)

    np.random.seed(42)

    # Mix of different fail patterns
    data = []

    # 1. Random scattered fails (60%)
    n_random = int(n_fails * 0.6)
    for _ in range(n_random):
        data.append({
            'row': np.random.randint(0, row_per_bank),
            'col': np.random.randint(0, col_per_bank),
            'dq': np.random.randint(0, 16)
        })

    # 2. Column-plane pattern (20%) - vertical stripe
    n_col_plane = int(n_fails * 0.2)
    col_plane_col = np.random.randint(1000, col_per_bank - 1000)
    for _ in range(n_col_plane):
        data.append({
            'row': np.random.randint(0, row_per_bank),
            'col': col_plane_col + np.random.randint(-50, 50),
            'dq': np.random.choice([3, 7])  # Same DQs
        })

    # 3. Row-aligned pattern (20%) - horizontal stripe
    n_row = int(n_fails * 0.2)
    row_fail = np.random.randint(10000, row_per_bank - 10000)
    for _ in range(n_row):
        data.append({
            'row': row_fail + np.random.randint(-20, 20),
            'col': np.random.randint(0, col_per_bank),
            'dq': np.random.randint(0, 16)
        })

    return pd.DataFrame(data)


def main():
    """Run demo and save visualizations."""
    print("Generating sample fail data...")
    df = generate_sample_fail_data(n_fails=2000, part_type='y62p')
    print(f"Generated {len(df)} fail points")
    print(f"DQ distribution: {df['dq'].value_counts().to_dict()}")

    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    os.makedirs(output_dir, exist_ok=True)

    # 1. Main fail viewer
    print("\nCreating fail viewer...")
    fig1 = create_fail_viewer(
        df,
        part_type='y62p',
        title='Sample Fail Viewer - Y62P',
        color_by='dq'
    )
    fig1.write_html(os.path.join(output_dir, 'fail_viewer_demo.html'))
    print(f"  Saved: {output_dir}/fail_viewer_demo.html")

    # 2. Heatmap
    print("Creating fail density heatmap...")
    fig2 = create_fail_heatmap(
        df,
        part_type='y62p',
        title='Fail Density Heatmap - Y62P',
        bin_size=500
    )
    fig2.write_html(os.path.join(output_dir, 'fail_heatmap_demo.html'))
    print(f"  Saved: {output_dir}/fail_heatmap_demo.html")

    # 3. DQ distribution
    print("Creating DQ distribution chart...")
    fig3 = create_dq_distribution(df, title='Fail Distribution by DQ')
    fig3.write_html(os.path.join(output_dir, 'dq_distribution_demo.html'))
    print(f"  Saved: {output_dir}/dq_distribution_demo.html")

    # 4. Bank distribution
    print("Creating bank distribution chart...")
    fig4 = create_bank_distribution(df, part_type='y62p', title='Fail Distribution by Bank')
    fig4.write_html(os.path.join(output_dir, 'bank_distribution_demo.html'))
    print(f"  Saved: {output_dir}/bank_distribution_demo.html")

    print("\nDemo complete! Open the HTML files in a browser to view.")
    print(f"Output directory: {output_dir}")


if __name__ == '__main__':
    main()
