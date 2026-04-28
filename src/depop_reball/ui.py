"""
SOCAMM2 LPDRAMM De-pop & Re-ball Tracker - UI Module

Streamlit UI components for the tracker tab.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from typing import Optional

from .tracker import (
    load_doe_excel,
    calculate_weekly_metrics,
    calculate_trailing_success_rate,
    get_hold_go_status,
    calculate_pic_metrics,
    calculate_failure_pareto,
    calculate_method_comparison,
    get_overall_summary,
    SUCCESS_THRESHOLD,
    HOLD_LOOKBACK_WEEKS,
)


# =============================================================================
# Constants
# =============================================================================

EXCEL_PATH = Path(__file__).parent / 'DOE_SOCAMM_SUMMARY.xlsx'

# Color scheme
COLORS = {
    'pass': '#27AE60',
    'fail': '#E74C3C',
    'damage': '#F39C12',
    'pending': '#9E9E9E',
    'target': '#3498DB',
}


# =============================================================================
# Main Render Function
# =============================================================================

def render_depop_reball_tab():
    """Main render function for the De-pop & Re-ball Tracker tab."""

    st.markdown("""
    <div style="background: linear-gradient(90deg, #1e3a5f 0%, #2d5a87 100%);
                padding: 20px; border-radius: 10px; margin-bottom: 20px;
                border: 1px solid #4a90d9; box-shadow: 0 4px 12px rgba(74, 144, 217, 0.3);">
        <h2 style="color: #ffffff; margin: 0; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
            🔩 SOCAMM2 LPDRAMM De-pop & Re-ball Tracker
        </h2>
        <p style="color: #d0e8ff; margin: 5px 0 0 0; font-size: 14px;">
            Track request lifecycle, execution outcomes, and HOLD/GO decisions | Target: 80% component functional after re-ball
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    if not EXCEL_PATH.exists():
        st.error(f"Data file not found: {EXCEL_PATH}")
        st.info("Please ensure DOE_SOCAMM_SUMMARY.xlsx is in the depop_reball folder.")
        return

    data = load_doe_excel(str(EXCEL_PATH))
    attempts_df = data['attempts']
    requests_df = data['requests']

    if attempts_df.empty:
        st.warning("No data found in the Excel file.")
        return

    # Calculate metrics
    weekly_metrics = calculate_weekly_metrics(attempts_df)
    overall_summary = get_overall_summary(attempts_df)
    trailing_rate = calculate_trailing_success_rate(weekly_metrics, HOLD_LOOKBACK_WEEKS)
    hold_go = get_hold_go_status(trailing_rate)

    # ==========================================================================
    # Section A: HOLD/GO Banner + KPI Tiles
    # ==========================================================================
    render_hold_go_banner(hold_go)
    render_kpi_tiles(overall_summary, weekly_metrics, trailing_rate)

    # ==========================================================================
    # Section B: Weekly Trend Charts
    # ==========================================================================
    st.markdown("### 📈 Weekly Trend Analysis")

    col1, col2 = st.columns(2)

    with col1:
        render_success_rate_chart(weekly_metrics)

    with col2:
        render_outcome_stacked_bar(weekly_metrics)

    # ==========================================================================
    # Section C: Failure Intelligence
    # ==========================================================================
    st.markdown("### 🔍 Failure Analysis")

    col1, col2 = st.columns(2)

    with col1:
        render_failure_pareto(attempts_df)

    with col2:
        render_unit_position_analysis(attempts_df)

    # ==========================================================================
    # Section D: Product Comparison
    # ==========================================================================
    st.markdown("### 📊 Product Comparison")
    render_product_comparison(attempts_df)

    # ==========================================================================
    # Section E: MSN Detail Table
    # ==========================================================================
    st.markdown("### 📋 MSN Status Detail")
    render_msn_table(requests_df, attempts_df)


# =============================================================================
# Component Renderers
# =============================================================================

def render_hold_go_banner(hold_go: dict):
    """Render the HOLD/GO decision banner."""

    if hold_go['status'] == 'HOLD':
        st.markdown(f"""
        <div style="background: linear-gradient(90deg, #c0392b 0%, #e74c3c 100%);
                    padding: 15px 20px; border-radius: 8px; margin-bottom: 20px;
                    border-left: 5px solid #922b21;">
            <div style="display: flex; align-items: center; gap: 15px;">
                <span style="font-size: 2em;">{hold_go['icon']}</span>
                <div>
                    <h3 style="color: #ffffff; margin: 0; font-size: 1.3em;">
                        {hold_go['status']} — New Requests
                    </h3>
                    <p style="color: #fadbd8; margin: 5px 0 0 0;">
                        {hold_go['action']}
                    </p>
                    <p style="color: #f5b7b1; margin: 3px 0 0 0; font-size: 0.9em;">
                        {hold_go['message']}
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background: linear-gradient(90deg, #1e8449 0%, #27ae60 100%);
                    padding: 15px 20px; border-radius: 8px; margin-bottom: 20px;
                    border-left: 5px solid #196f3d;">
            <div style="display: flex; align-items: center; gap: 15px;">
                <span style="font-size: 2em;">{hold_go['icon']}</span>
                <div>
                    <h3 style="color: #ffffff; margin: 0; font-size: 1.3em;">
                        {hold_go['status']} — Accepting Requests
                    </h3>
                    <p style="color: #d5f5e3; margin: 5px 0 0 0;">
                        {hold_go['action']}
                    </p>
                    <p style="color: #abebc6; margin: 3px 0 0 0; font-size: 0.9em;">
                        {hold_go['message']}
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_kpi_tiles(summary: dict, weekly_metrics: pd.DataFrame, trailing_rate: float):
    """Render KPI tiles row."""

    # Get current week metrics
    current_week = weekly_metrics.iloc[-1] if not weekly_metrics.empty else None
    current_rate = current_week['e2e_success_rate'] if current_week is not None else 0

    cols = st.columns(5)

    with cols[0]:
        st.metric(
            label="📦 Total Units Tested",
            value=summary['total_attempts'],
            delta=f"{summary['total_msn']} MSNs",
        )

    with cols[1]:
        st.metric(
            label="✅ Pass",
            value=summary['total_pass'],
            delta=f"{summary['total_pass'] / summary['total_attempts'] * 100:.1f}%" if summary['total_attempts'] > 0 else "0%",
        )

    with cols[2]:
        st.metric(
            label="❌ Fail",
            value=summary['total_fail'],
            delta=f"{summary['total_fail'] / summary['total_attempts'] * 100:.1f}%" if summary['total_attempts'] > 0 else "0%",
            delta_color="inverse",
        )

    with cols[3]:
        st.metric(
            label="🔧 Damage",
            value=summary['total_damage'],
            delta=f"{summary['total_damage'] / summary['total_attempts'] * 100:.1f}%" if summary['total_attempts'] > 0 else "0%",
            delta_color="inverse",
        )

    with cols[4]:
        # E2E Success Rate with target comparison
        rate_pct = summary['e2e_success_rate'] * 100
        target_delta = rate_pct - (SUCCESS_THRESHOLD * 100)
        st.metric(
            label="🎯 E2E Success Rate",
            value=f"{rate_pct:.1f}%",
            delta=f"{target_delta:+.1f}% vs 80% target",
            delta_color="normal" if target_delta >= 0 else "inverse",
        )


def render_success_rate_chart(weekly_metrics: pd.DataFrame):
    """Render weekly success rate trend chart."""

    if weekly_metrics.empty:
        st.info("No weekly data available")
        return

    fig = go.Figure()

    # E2E Success Rate line
    fig.add_trace(go.Scatter(
        x=weekly_metrics['workweek_label'],
        y=weekly_metrics['e2e_success_rate'] * 100,
        mode='lines+markers',
        name='E2E Success Rate',
        line=dict(color=COLORS['pass'], width=3),
        marker=dict(size=10),
        hovertemplate='%{x}<br>Success: %{y:.1f}%<extra></extra>',
    ))

    # Target line (80%)
    fig.add_hline(
        y=SUCCESS_THRESHOLD * 100,
        line_dash="dash",
        line_color=COLORS['target'],
        annotation_text="80% Target",
        annotation_position="right",
    )

    # Red zone shading below target
    fig.add_hrect(
        y0=0, y1=SUCCESS_THRESHOLD * 100,
        fillcolor="rgba(231, 76, 60, 0.1)",
        line_width=0,
    )

    fig.update_layout(
        title="End-to-End Success Rate Trend",
        xaxis_title="Workweek",
        yaxis_title="Success Rate (%)",
        yaxis_range=[0, 100],
        height=350,
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode='x unified',
    )

    st.plotly_chart(fig, use_container_width=True)


def render_outcome_stacked_bar(weekly_metrics: pd.DataFrame):
    """Render weekly outcome stacked bar chart."""

    if weekly_metrics.empty:
        st.info("No weekly data available")
        return

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=weekly_metrics['workweek_label'],
        y=weekly_metrics['functional_success_count'],
        name='Pass',
        marker_color=COLORS['pass'],
    ))

    fig.add_trace(go.Bar(
        x=weekly_metrics['workweek_label'],
        y=weekly_metrics['fail_count'],
        name='Fail',
        marker_color=COLORS['fail'],
    ))

    fig.add_trace(go.Bar(
        x=weekly_metrics['workweek_label'],
        y=weekly_metrics['damage_count'],
        name='Damage',
        marker_color=COLORS['damage'],
    ))

    fig.add_trace(go.Bar(
        x=weekly_metrics['workweek_label'],
        y=weekly_metrics['pending_count'],
        name='Pending',
        marker_color=COLORS['pending'],
    ))

    fig.update_layout(
        title="Weekly Outcomes Distribution",
        xaxis_title="Workweek",
        yaxis_title="Count",
        barmode='stack',
        height=350,
        margin=dict(l=40, r=40, t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_failure_pareto(attempts_df: pd.DataFrame):
    """Render failure reason Pareto chart."""

    # Count by status
    status_counts = attempts_df['status_raw'].value_counts()

    if status_counts.empty:
        st.info("No failure data available")
        return

    # Create simple bar chart of outcomes
    fig = go.Figure()

    colors_map = {
        'PASS': COLORS['pass'],
        'FAIL': COLORS['fail'],
        'DAMAGE': COLORS['damage'],
        'PENDING': COLORS['pending'],
    }

    fig.add_trace(go.Bar(
        x=status_counts.index,
        y=status_counts.values,
        marker_color=[colors_map.get(s, '#333') for s in status_counts.index],
        text=status_counts.values,
        textposition='auto',
    ))

    fig.update_layout(
        title="Outcome Distribution",
        xaxis_title="Status",
        yaxis_title="Count",
        height=300,
        margin=dict(l=40, r=40, t=60, b=40),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_unit_position_analysis(attempts_df: pd.DataFrame):
    """Render ULOC position (ULOC1-ULOC4) success rate analysis."""

    if 'component_uloc' not in attempts_df.columns:
        st.info("No ULOC position data available")
        return

    # Filter to only ULOC1-ULOC4
    valid_ulocs = ['ULOC1', 'ULOC2', 'ULOC3', 'ULOC4']
    uloc_df = attempts_df[attempts_df['component_uloc'].isin(valid_ulocs)].copy()

    if uloc_df.empty:
        st.info("No ULOC1-ULOC4 data available")
        return

    # Group by ULOC position
    uloc_metrics = uloc_df.groupby('component_uloc').agg(
        total=('attempt_id', 'count'),
        passed=('component_functional_after_reball', 'sum'),
    ).reset_index()

    uloc_metrics['success_rate'] = uloc_metrics['passed'] / uloc_metrics['total'] * 100

    # Sort in ULOC1, ULOC2, ULOC3, ULOC4 order
    uloc_order = {uloc: i for i, uloc in enumerate(valid_ulocs)}
    uloc_metrics['sort_order'] = uloc_metrics['component_uloc'].map(uloc_order)
    uloc_metrics = uloc_metrics.sort_values('sort_order')

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=uloc_metrics['component_uloc'],
        y=uloc_metrics['success_rate'],
        marker_color=[
            COLORS['pass'] if r >= 80 else COLORS['fail'] if r < 60 else COLORS['damage']
            for r in uloc_metrics['success_rate']
        ],
        text=[f"{r:.0f}%" for r in uloc_metrics['success_rate']],
        textposition='auto',
        hovertemplate='%{x}<br>Success Rate: %{y:.1f}%<br>Total: %{customdata}<extra></extra>',
        customdata=uloc_metrics['total'],
    ))

    fig.add_hline(y=80, line_dash="dash", line_color=COLORS['target'])

    fig.update_layout(
        title="Success Rate by ULOC Position",
        xaxis_title="ULOC Position",
        yaxis_title="Success Rate (%)",
        yaxis_range=[0, 100],
        height=300,
        margin=dict(l=40, r=40, t=60, b=40),
        xaxis=dict(categoryorder='array', categoryarray=valid_ulocs),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_product_comparison(attempts_df: pd.DataFrame):
    """Render product comparison (Merlin vs SM3.5)."""

    if 'product' not in attempts_df.columns:
        st.info("No product data available")
        return

    # Group by product and week
    product_weekly = attempts_df.groupby(['product', 'workweek']).agg(
        total=('attempt_id', 'count'),
        passed=('component_functional_after_reball', 'sum'),
    ).reset_index()

    product_weekly['success_rate'] = product_weekly['passed'] / product_weekly['total'] * 100
    product_weekly['workweek_label'] = product_weekly['workweek'].apply(lambda x: f'WW{x}')

    # Overall by product
    product_summary = attempts_df.groupby('product').agg(
        total=('attempt_id', 'count'),
        passed=('component_functional_after_reball', 'sum'),
    ).reset_index()

    product_summary['success_rate'] = product_summary['passed'] / product_summary['total'] * 100

    col1, col2 = st.columns(2)

    with col1:
        # Overall comparison
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=product_summary['product'],
            y=product_summary['success_rate'],
            marker_color=[
                COLORS['pass'] if r >= 80 else COLORS['damage']
                for r in product_summary['success_rate']
            ],
            text=[f"{r:.1f}%" for r in product_summary['success_rate']],
            textposition='auto',
        ))

        fig.add_hline(y=80, line_dash="dash", line_color=COLORS['target'])

        fig.update_layout(
            title="Overall Success Rate by Product",
            xaxis_title="Product",
            yaxis_title="Success Rate (%)",
            yaxis_range=[0, 100],
            height=300,
            margin=dict(l=40, r=40, t=60, b=40),
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Weekly trend by product
        fig = go.Figure()

        for product in product_weekly['product'].unique():
            prod_data = product_weekly[product_weekly['product'] == product]
            fig.add_trace(go.Scatter(
                x=prod_data['workweek_label'],
                y=prod_data['success_rate'],
                mode='lines+markers',
                name=product,
            ))

        fig.add_hline(y=80, line_dash="dash", line_color=COLORS['target'])

        fig.update_layout(
            title="Weekly Trend by Product",
            xaxis_title="Workweek",
            yaxis_title="Success Rate (%)",
            yaxis_range=[0, 100],
            height=300,
            margin=dict(l=40, r=40, t=60, b=40),
        )

        st.plotly_chart(fig, use_container_width=True)


def render_msn_table(requests_df: pd.DataFrame, attempts_df: pd.DataFrame):
    """Render MSN-level detail table."""

    if requests_df.empty:
        st.info("No MSN data available")
        return

    # Add filtering
    col1, col2, col3 = st.columns(3)

    with col1:
        status_filter = st.multiselect(
            "Filter by Status",
            options=requests_df['status'].unique().tolist(),
            default=requests_df['status'].unique().tolist(),
        )

    with col2:
        product_filter = st.multiselect(
            "Filter by Product",
            options=requests_df['product'].unique().tolist(),
            default=requests_df['product'].unique().tolist(),
        )

    with col3:
        workweek_filter = st.multiselect(
            "Filter by Workweek",
            options=sorted(requests_df['workweek'].unique().tolist()),
            default=sorted(requests_df['workweek'].unique().tolist()),
        )

    # Apply filters
    filtered = requests_df[
        (requests_df['status'].isin(status_filter)) &
        (requests_df['product'].isin(product_filter)) &
        (requests_df['workweek'].isin(workweek_filter))
    ].copy()

    # Format for display
    display_df = filtered[[
        'msn', 'product', 'workweek', 'total_units',
        'units_passed', 'units_failed', 'units_damaged', 'units_pending',
        'success_rate', 'status'
    ]].copy()

    display_df['workweek'] = display_df['workweek'].apply(lambda x: f'WW{x}')
    display_df['success_rate'] = display_df['success_rate'].apply(
        lambda x: f'{x*100:.0f}%' if pd.notna(x) else 'N/A'
    )

    display_df.columns = [
        'MSN', 'Product', 'Week', 'Total Units',
        'Passed', 'Failed', 'Damaged', 'Pending',
        'Success Rate', 'Status'
    ]

    # Color-code status
    def highlight_status(row):
        if row['Status'] == 'Success':
            return ['background-color: rgba(39, 174, 96, 0.2)'] * len(row)
        elif row['Status'] == 'Damaged':
            return ['background-color: rgba(231, 76, 60, 0.2)'] * len(row)
        elif row['Status'] == 'Partial':
            return ['background-color: rgba(243, 156, 18, 0.2)'] * len(row)
        else:
            return [''] * len(row)

    st.dataframe(
        display_df.style.apply(highlight_status, axis=1),
        use_container_width=True,
        height=400,
    )

    # Export button
    col1, col2 = st.columns([1, 5])
    with col1:
        csv = filtered.to_csv(index=False)
        st.download_button(
            label="📥 Export CSV",
            data=csv,
            file_name="depop_reball_msn_status.csv",
            mime="text/csv",
        )
