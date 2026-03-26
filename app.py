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
from src.cache import FrptCache
from config.settings import Settings


# Configure logging to file for debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/asegaran/MODULE_YIELD_DASHBOARD/dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Suppress noisy loggers
logging.getLogger('watchdog').setLevel(logging.WARNING)

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
    if "fetch_in_progress" not in st.session_state:
        st.session_state.fetch_in_progress = False
    if "last_fetch_time" not in st.session_state:
        st.session_state.last_fetch_time = None
    if "last_fetch_filters" not in st.session_state:
        st.session_state.last_fetch_filters = None


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


def get_previous_workweeks(end_ww: str, count: int = 10) -> tuple[str, str]:
    """Calculate start workweek that is 'count' weeks before end_ww.

    Args:
        end_ww: End workweek in YYYYWW format
        count: Number of weeks to include (default 10)

    Returns:
        Tuple of (start_ww, end_ww) in YYYYWW format
    """
    year = int(end_ww[:4])
    week = int(end_ww[4:])

    # Go back (count - 1) weeks to get start
    weeks_back = count - 1
    while weeks_back > 0:
        week -= 1
        if week < 1:
            year -= 1
            week = 52
        weeks_back -= 1

    return f"{year}{week:02d}", end_ww


def render_workweek_filters() -> dict[str, str]:
    """Render work week filter widget (single workweek input)."""
    st.sidebar.divider()
    st.sidebar.subheader("Work Week")

    current_ww = get_current_workweek()
    current_year = int(current_ww[:4])
    current_week = min(int(current_ww[4:]), MAX_WEEKS_PER_YEAR)

    st.sidebar.caption("Select target workweek (will show 10 weeks of data)")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        selected_year = st.number_input(
            "Year",
            min_value=MIN_YEAR,
            max_value=MAX_YEAR,
            value=current_year,
        )
    with col2:
        selected_week = st.number_input(
            "Week",
            min_value=1,
            max_value=MAX_WEEKS_PER_YEAR,
            value=current_week,
        )

    selected_ww = f"{selected_year}{selected_week:02d}"
    start_ww, end_ww = get_previous_workweeks(selected_ww, count=10)

    st.sidebar.caption(f"Range: WW{start_ww} to WW{end_ww}")

    return {
        "start_ww": start_ww,
        "end_ww": end_ww,
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

    return None


def render_sidebar() -> dict[str, Any]:
    """Render sidebar filters and return selected values."""
    st.sidebar.header("Filters")

    primary = render_primary_filters()
    workweek = render_workweek_filters()
    optional = render_optional_filters()

    return {**primary, **workweek, **optional}


def fetch_data(filters: dict[str, Any], use_cache: bool = True) -> pd.DataFrame:
    """Fetch data from frpt commands based on filters using parallel execution.

    Args:
        filters: Filter parameters
        use_cache: Whether to use cached results

    Returns:
        DataFrame with yield data

    Raises:
        RuntimeError: If data fetching fails
    """
    logger.info("="*60)
    logger.info(f"FETCH DATA STARTED (PARALLEL, cache={'ON' if use_cache else 'OFF'})")
    logger.info(f"Filters: {filters}")

    runner = FrptRunner(max_workers=4, use_cache=use_cache)  # Run up to 4 queries in parallel
    parser = FrptParser()

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        logger.info(f"Workweeks to fetch: {workweeks}")
    except Exception as e:
        logger.error("Failed to generate workweek range: %s", e)
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    # Build all commands first
    commands = []
    for step in filters["test_steps"]:
        for form_factor in filters["form_factors"]:
            for workweek in workweeks:
                try:
                    command = FrptCommand(
                        step=step,
                        form_factor=form_factor,
                        workweek=workweek,
                        dbase=filters["design_id"],
                        facility=filters["facility"],
                    )
                    commands.append(command)
                except ValueError as e:
                    logger.warning(f"Invalid command parameters: {e}")

    total_calls = len(commands)
    logger.info(f"Total frpt calls to make: {total_calls} (running in parallel)")

    if total_calls == 0:
        logger.warning("No calls to make (empty filters)")
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()
    cache_hits_text = st.empty()

    cache_mode = "with cache" if use_cache else "no cache"
    status_text.text(f"Running {total_calls} queries in parallel ({cache_mode})...")

    # Progress callback for UI updates
    completed_count = [0]  # Use list for mutable closure
    cache_hits = [0]

    def progress_callback(completed: int, total: int, cmd: FrptCommand) -> None:
        completed_count[0] = completed
        progress_bar.progress(completed / total)
        status_text.text(
            f"Completed {completed}/{total}: {cmd.step}/{cmd.form_factor}/WW{cmd.workweek}"
        )

    # Run commands in parallel
    results = []
    errors = []

    try:
        parallel_results = runner.run_parallel(commands, progress_callback)

        for command, result in parallel_results:
            if result.success and result.stdout:
                results.append((result.stdout, command.step, command.form_factor))
                logger.info(f"Added result for {command.step}/{command.form_factor}/WW{command.workweek}")
            elif not result.success:
                err_msg = f"{command.step}/{command.form_factor}/WW{command.workweek}: {result.stderr[:200]}"
                errors.append(err_msg)
                logger.error(f"Command failed: {err_msg}")

    except Exception as e:
        logger.error(f"Parallel execution error: {e}", exc_info=True)
        raise RuntimeError(f"Parallel execution failed: {e}") from e

    progress_bar.empty()
    status_text.empty()
    cache_hits_text.empty()

    logger.info(f"Fetch complete. Results collected: {len(results)}, Errors: {len(errors)}")
    if errors:
        logger.warning("Fetch errors: %s", errors[:5])

    if not results:
        logger.warning("No results to parse")
        return pd.DataFrame()

    try:
        df = parser.parse_multiple(results)
        logger.info(f"Parsed DataFrame: {len(df)} rows, columns={list(df.columns) if not df.empty else []}")
        if not df.empty:
            logger.debug(f"First few rows:\n{df.head()}")
        return df
    except Exception as e:
        logger.error("Failed to parse results: %s", e, exc_info=True)
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

        # Ensure workweek is string for proper categorical display (YYYYWW format)
        trend_data["workweek"] = trend_data["workweek"].astype(str)

        # Sort by workweek to ensure correct order
        trend_data = trend_data.sort_values("workweek")

        fig = px.line(
            trend_data,
            x="workweek",
            y="yield_pct",
            color="series",
            markers=True,
            title="Yield % by Work Week",
            labels={
                "workweek": "Work Week (YYYYWW)",
                "yield_pct": "Yield %",
                "series": "Step / Form Factor",
            },
        )

        fig.update_layout(
            xaxis_title="Work Week (YYYYWW)",
            yaxis_title="Yield %",
            yaxis_range=[0, 100],
            legend_title="Step / Form Factor",
            hovermode="x unified",
            xaxis_type="category",  # Force categorical x-axis for proper YYYYWW display
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
    # Summary metrics at top
    render_summary_metrics(processor)
    st.divider()

    # Summary table (moved to top per user request)
    render_summary_table(processor)
    st.divider()

    # Charts in two columns
    col1, col2 = st.columns(2)
    with col1:
        render_yield_trend_chart(processor)
    with col2:
        render_bin_distribution_chart(processor)

    st.divider()
    render_density_speed_heatmap(processor)


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

    # Display last fetch info
    if st.session_state.last_fetch_time:
        st.sidebar.info(f"Last fetch: {st.session_state.last_fetch_time}")

    # Cache controls
    st.sidebar.divider()
    st.sidebar.subheader("Cache Settings")

    cache = FrptCache()
    cache_stats = cache.get_stats()
    st.sidebar.caption(
        f"Cached: {cache_stats['valid_entries']} queries | "
        f"Size: {cache_stats['total_size_mb']:.1f} MB"
    )

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Clear Cache", use_container_width=True):
            cleared = cache.clear()
            st.sidebar.success(f"Cleared {cleared} entries")
    with col2:
        use_cache = st.checkbox("Use Cache", value=True, help="Uncheck to force fresh data fetch")

    # Store cache preference in session state
    st.session_state.use_cache = use_cache

    # Fetch data button
    st.sidebar.divider()

    # Estimate number of calls (with parallel execution)
    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        total_calls = len(filters["test_steps"]) * len(filters["form_factors"]) * len(workweeks)
        # With 4 parallel workers, time is ~ceil(total/4) * 3 minutes
        parallel_batches = (total_calls + 3) // 4  # ceil division
        estimated_time = max(parallel_batches * 3, 3)  # at least 3 min
        st.sidebar.caption(f"{len(workweeks)} weeks x {len(filters['test_steps'])} steps x {len(filters['form_factors'])} forms = {total_calls} queries")
        st.sidebar.caption(f"Estimated time: ~{estimated_time} min (parallel)")
    except Exception:
        pass

    fetch_button = st.sidebar.button(
        "Fetch Data",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.fetch_in_progress,
    )

    if fetch_button:
        st.session_state.fetch_in_progress = True
        logger.info("Fetch button clicked")

        try:
            use_cache = st.session_state.get("use_cache", True)
            if use_cache:
                st.info("Fetching data... Cached results will be used when available (instant). Fresh queries may take a few minutes.")
            else:
                st.warning("Fetching fresh data (cache disabled)... This may take several minutes. Please wait.")

            data = fetch_data(filters, use_cache=use_cache)
            st.session_state.data = data
            st.session_state.last_error = None
            st.session_state.last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state.last_fetch_filters = str(filters)

            if data.empty:
                st.error("No data returned. Check your filter parameters or try different workweeks.")
                logger.warning("Fetch returned empty DataFrame")
            else:
                st.success(f"Loaded {len(data)} records successfully!")
                logger.info(f"Fetch successful: {len(data)} records")

        except RuntimeError as e:
            st.session_state.last_error = str(e)
            st.error(f"Failed to fetch data: {e}")
            logger.error("Fetch error: %s", e)
        except Exception as e:
            st.session_state.last_error = str(e)
            st.error(f"Unexpected error: {e}")
            logger.exception("Unexpected fetch error")
        finally:
            st.session_state.fetch_in_progress = False

    # Show last error if any
    if st.session_state.last_error:
        st.error(f"Last error: {st.session_state.last_error}")

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
        st.info("Click 'Fetch Data' to load yield data. Note: Each workweek may take 2-5 minutes to fetch.")


if __name__ == "__main__":
    main()
