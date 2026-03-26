"""Module Yield Dashboard - Main Streamlit Application."""

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from src.frpt_runner import FrptRunner, FrptCommand
from src.frpt_parser import FrptParser
from src.data_processor import DataProcessor
from config.settings import Settings


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_WEEKS_PER_YEAR = 52
MIN_YEAR = 2020
MAX_YEAR = 2030


def get_current_workweek() -> str:
    """Get current work week in YYYYWW format."""
    now = datetime.now()
    week = now.isocalendar()[1]
    return f"{now.year}{week:02d}"


def init_session_state() -> None:
    """Initialize session state variables."""
    if "data" not in st.session_state:
        st.session_state.data = pd.DataFrame()
    if "last_error" not in st.session_state:
        st.session_state.last_error = None


def setup_page() -> None:
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="Module Yield Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("Module Yield Dashboard")
    st.markdown("Weekly yield tracking for SOCAMM/SOCAMM2 modules")


def render_primary_filters() -> dict[str, Any]:
    """Render primary filter widgets (form factor, step, design_id, facility)."""
    form_factors = st.sidebar.multiselect(
        "Module Form Factor",
        options=Settings.FORM_FACTORS,
        default=Settings.FORM_FACTORS,
        help="Select one or more module form factors",
    )

    test_steps = st.sidebar.multiselect(
        "Test Step",
        options=Settings.TEST_STEPS,
        default=Settings.TEST_STEPS,
        help="Select one or more test steps",
    )

    design_id = st.sidebar.selectbox(
        "Design ID",
        options=Settings.DESIGN_IDS,
        index=0,
        help="Select design ID (DBASE parameter)",
    )

    facility = st.sidebar.selectbox(
        "Test Facility",
        options=Settings.FACILITIES,
        index=0,
        help="Select test facility",
    )

    return {
        "form_factors": form_factors,
        "test_steps": test_steps,
        "design_id": design_id,
        "facility": facility,
    }


def render_workweek_filters() -> dict[str, str]:
    """Render work week range filter widgets."""
    st.sidebar.divider()
    st.sidebar.subheader("Work Week Range")

    current_ww = get_current_workweek()
    current_year = int(current_ww[:4])
    current_week = min(int(current_ww[4:]), MAX_WEEKS_PER_YEAR)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_year = st.number_input(
            "Start Year",
            min_value=MIN_YEAR,
            max_value=MAX_YEAR,
            value=current_year,
        )
        start_week = st.number_input(
            "Start Week",
            min_value=1,
            max_value=MAX_WEEKS_PER_YEAR,
            value=1,
        )
    with col2:
        end_year = st.number_input(
            "End Year",
            min_value=MIN_YEAR,
            max_value=MAX_YEAR,
            value=current_year,
        )
        end_week = st.number_input(
            "End Week",
            min_value=1,
            max_value=MAX_WEEKS_PER_YEAR,
            value=current_week,
        )

    return {
        "start_ww": f"{start_year}{start_week:02d}",
        "end_ww": f"{end_year}{end_week:02d}",
    }


def render_optional_filters() -> dict[str, Optional[list[str]]]:
    """Render optional filter widgets (density, speed)."""
    st.sidebar.divider()
    st.sidebar.subheader("Optional Filters")

    densities = st.sidebar.multiselect(
        "Density",
        options=Settings.DENSITIES,
        default=[],
        help="Filter by module density (optional)",
    )

    speeds = st.sidebar.multiselect(
        "Speed",
        options=Settings.SPEEDS,
        default=[],
        help="Filter by module speed (optional)",
    )

    return {
        "densities": densities if densities else None,
        "speeds": speeds if speeds else None,
    }


def validate_filters(filters: dict[str, Any]) -> Optional[str]:
    """Validate filter selections.

    Args:
        filters: Filter dictionary

    Returns:
        Error message if validation fails, None if valid
    """
    if not filters["form_factors"]:
        return "Please select at least one Form Factor"
    if not filters["test_steps"]:
        return "Please select at least one Test Step"

    # Validate workweek range
    start_ww = filters["start_ww"]
    end_ww = filters["end_ww"]
    if start_ww > end_ww:
        return "Start work week must be before or equal to end work week"

    return None


def render_sidebar() -> dict[str, Any]:
    """Render sidebar filters and return selected values."""
    st.sidebar.header("Filters")

    primary = render_primary_filters()
    workweek = render_workweek_filters()
    optional = render_optional_filters()

    return {**primary, **workweek, **optional}


def fetch_data(filters: dict[str, Any]) -> pd.DataFrame:
    """Fetch data from frpt commands based on filters.

    Args:
        filters: Filter parameters

    Returns:
        DataFrame with yield data

    Raises:
        RuntimeError: If data fetching fails
    """
    runner = FrptRunner()
    parser = FrptParser()

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
    except Exception as e:
        logger.error("Failed to generate workweek range: %s", e)
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    results = []
    total_calls = len(filters["test_steps"]) * len(filters["form_factors"]) * len(workweeks)

    if total_calls == 0:
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()

    call_count = 0
    errors = []

    for step in filters["test_steps"]:
        for form_factor in filters["form_factors"]:
            for workweek in workweeks:
                call_count += 1
                status_text.text(f"Fetching {step} / {form_factor} / WW{workweek}...")
                progress_bar.progress(call_count / total_calls)

                try:
                    command = FrptCommand(
                        step=step,
                        form_factor=form_factor,
                        workweek=workweek,
                        dbase=filters["design_id"],
                        facility=filters["facility"],
                    )
                    result = runner.run(command)

                    if result.success and result.stdout:
                        results.append((result.stdout, step, form_factor))
                    elif not result.success:
                        errors.append(f"{step}/{form_factor}/WW{workweek}: {result.stderr}")
                except ValueError as e:
                    errors.append(f"{step}/{form_factor}/WW{workweek}: {e}")
                    logger.warning("Invalid parameters: %s", e)

    progress_bar.empty()
    status_text.empty()

    if errors:
        logger.warning("Fetch errors: %s", errors[:5])

    if not results:
        return pd.DataFrame()

    try:
        return parser.parse_multiple(results)
    except Exception as e:
        logger.error("Failed to parse results: %s", e)
        raise RuntimeError(f"Failed to parse frpt output: {e}") from e


def render_yield_trend_chart(processor: DataProcessor) -> None:
    """Render weekly yield trend line chart."""
    st.subheader("Weekly Yield Trend")

    try:
        trend_data = processor.get_weekly_yield_trend()
        if trend_data.empty:
            st.info("No trend data available")
            return

        trend_data = trend_data.copy()
        trend_data["series"] = trend_data["step"] + " - " + trend_data["form_factor"]

        fig = px.line(
            trend_data,
            x="workweek",
            y="yield_pct",
            color="series",
            markers=True,
            title="Yield % by Work Week",
            labels={
                "workweek": "Work Week",
                "yield_pct": "Yield %",
                "series": "Step / Form Factor",
            },
        )

        fig.update_layout(
            xaxis_title="Work Week",
            yaxis_title="Yield %",
            yaxis_range=[0, 100],
            legend_title="Step / Form Factor",
            hovermode="x unified",
        )

        fig.update_traces(line={"width": 2}, marker={"size": 8})
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        logger.error("Failed to render trend chart: %s", e)
        st.error("Failed to render trend chart")


def render_summary_metrics(processor: DataProcessor) -> None:
    """Render summary metric cards."""
    try:
        summary = processor.get_yield_summary()

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Units In", f"{summary.total_uin:,}")
        with col2:
            st.metric("Total Units Pass", f"{summary.total_upass:,}")
        with col3:
            st.metric("Overall Yield", f"{summary.overall_yield:.2f}%")
        with col4:
            st.metric(
                "Yield Range",
                f"{summary.min_yield:.1f}% - {summary.max_yield:.1f}%",
            )
    except Exception as e:
        logger.error("Failed to render metrics: %s", e)
        st.error("Failed to render summary metrics")


def render_summary_table(processor: DataProcessor) -> None:
    """Render yield summary table."""
    st.subheader("Yield Summary Table")

    try:
        table_data = processor.get_summary_table()
        if table_data.empty:
            st.info("No data available for table")
            return

        st.dataframe(
            table_data,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Units In": st.column_config.NumberColumn(format="%d"),
                "Units Pass": st.column_config.NumberColumn(format="%d"),
                "Yield %": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )

        csv = table_data.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="yield_summary.csv",
            mime="text/csv",
        )
    except Exception as e:
        logger.error("Failed to render table: %s", e)
        st.error("Failed to render summary table")


def render_bin_distribution_chart(processor: DataProcessor) -> None:
    """Render bin distribution bar chart."""
    st.subheader("Bin Distribution")

    try:
        bin_data = processor.get_bin_distribution_by_step()
        if bin_data.empty:
            st.info("No bin distribution data available")
            return

        fig = px.bar(
            bin_data,
            x="bin",
            y="percentage",
            color="step",
            barmode="group",
            title="Bin Distribution by Test Step",
            labels={
                "bin": "Bin",
                "percentage": "Percentage %",
                "step": "Test Step",
            },
        )

        fig.update_layout(
            xaxis_title="Bin",
            yaxis_title="Percentage %",
            legend_title="Test Step",
        )

        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        logger.error("Failed to render bin chart: %s", e)
        st.error("Failed to render bin distribution chart")


def render_density_speed_heatmap(processor: DataProcessor) -> None:
    """Render yield heatmap by density and speed."""
    st.subheader("Yield by Density & Speed")

    try:
        data = processor.get_yield_by_density_speed()
        if data.empty:
            st.info("No density/speed data available")
            return

        pivot = data.pivot_table(
            index="density",
            columns="speed",
            values="yield_pct",
            aggfunc="mean",
        )

        if pivot.empty:
            st.info("Insufficient data for heatmap")
            return

        fig = px.imshow(
            pivot,
            text_auto=".1f",
            color_continuous_scale="RdYlGn",
            aspect="auto",
            title="Average Yield % by Density and Speed",
            labels={"color": "Yield %"},
        )

        fig.update_layout(
            xaxis_title="Speed",
            yaxis_title="Density",
        )

        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        logger.error("Failed to render heatmap: %s", e)
        st.error("Failed to render density/speed heatmap")


def render_dashboard(processor: DataProcessor) -> None:
    """Render all dashboard components."""
    render_summary_metrics(processor)
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        render_yield_trend_chart(processor)
    with col2:
        render_bin_distribution_chart(processor)

    st.divider()
    render_density_speed_heatmap(processor)

    st.divider()
    render_summary_table(processor)


def main() -> None:
    """Main application entry point."""
    setup_page()
    init_session_state()

    filters = render_sidebar()

    # Validate filters
    validation_error = validate_filters(filters)
    if validation_error:
        st.warning(validation_error)
        return

    # Fetch data button
    st.sidebar.divider()
    fetch_button = st.sidebar.button(
        "Fetch Data",
        type="primary",
        use_container_width=True,
    )

    if fetch_button:
        try:
            with st.spinner("Fetching data from frpt..."):
                st.session_state.data = fetch_data(filters)
                st.session_state.last_error = None

            if st.session_state.data.empty:
                st.error("No data returned. Check your filter parameters.")
                return

            st.success(f"Loaded {len(st.session_state.data)} records")
        except RuntimeError as e:
            st.session_state.last_error = str(e)
            st.error(f"Failed to fetch data: {e}")
            logger.error("Fetch error: %s", e)
            return
        except Exception as e:
            st.session_state.last_error = str(e)
            st.error(f"Unexpected error: {e}")
            logger.exception("Unexpected fetch error")
            return

    # Display dashboard if data exists
    if not st.session_state.data.empty:
        processor = DataProcessor(st.session_state.data)
        processor = processor.filter_data(
            form_factors=filters["form_factors"],
            steps=filters["test_steps"],
            densities=filters["densities"],
            speeds=filters["speeds"],
        )
        render_dashboard(processor)
    else:
        st.info("Click 'Fetch Data' to load yield data")


if __name__ == "__main__":
    main()
