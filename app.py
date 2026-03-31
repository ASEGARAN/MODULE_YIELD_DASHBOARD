"""Module Yield Dashboard - Main Streamlit Application."""

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
    # ELC tab session state
    if "elc_data" not in st.session_state:
        st.session_state.elc_data = pd.DataFrame()
    if "elc_last_fetch_time" not in st.session_state:
        st.session_state.elc_last_fetch_time = None
    # Pareto tab session state
    if "pareto_data" not in st.session_state:
        st.session_state.pareto_data = pd.DataFrame()
    if "pareto_last_fetch_time" not in st.session_state:
        st.session_state.pareto_last_fetch_time = None


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

    design_ids = st.sidebar.multiselect(
        "Design ID",
        options=Settings.DESIGN_IDS,
        default=[Settings.DESIGN_IDS[0]],
        help="Select one or more design IDs (DBASE parameter)",
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
        "design_ids": design_ids,
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

    # Hardcoded speed options with MTPS suffix
    SPEED_OPTIONS = ["6400MTPS", "7500MTPS", "8533MTPS", "9600MTPS"]

    densities = st.sidebar.multiselect(
        "Density",
        options=Settings.DENSITIES,
        default=[],
        help="Filter by module density (optional)",
    )

    speeds = st.sidebar.multiselect(
        "Speed",
        options=SPEED_OPTIONS,  # Use hardcoded values
        default=[],
        help="Filter by module speed (optional)",
        key="speed_filter_v5",  # Force widget refresh
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
    if not filters["design_ids"]:
        return "Please select at least one Design ID"

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

    # Build all commands first (iterate over all combinations including design_ids)
    commands = []
    for design_id in filters["design_ids"]:
        for step in filters["test_steps"]:
            for form_factor in filters["form_factors"]:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=design_id,
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
            logger.info(f"Processing result: {command.step}/WW{command.workweek} success={result.success} stdout_len={len(result.stdout) if result.stdout else 0}")
            if result.success and result.stdout:
                results.append((result.stdout, command.step, command.form_factor))
                logger.info(f"Added result for {command.step}/{command.form_factor}/WW{command.workweek}")
            elif not result.success:
                err_msg = f"{command.step}/{command.form_factor}/WW{command.workweek}: {result.stderr[:200]}"
                errors.append(err_msg)
                logger.error(f"Command failed: {err_msg}")
            else:
                logger.warning(f"Skipped result (no stdout): {command.step}/WW{command.workweek}")

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

    # Debug: Check if results have pipe character (bin data)
    for i, (stdout, step, ff) in enumerate(results[:2]):  # Check first 2
        pipe_count = stdout.count('|')
        data_lines = [l for l in stdout.split('\n') if '|' in l and '202' in l]
        logger.info(f"Result {i} [{step}/{ff}]: {len(stdout)} bytes, {pipe_count} pipes, {len(data_lines)} data lines with pipe")
        if data_lines:
            logger.info(f"  Sample line: {data_lines[0][:120]}")

    try:
        df = parser.parse_multiple(results)
        bin_cols = [c for c in df.columns if c.startswith('BIN')]
        logger.info(f"Parsed DataFrame: {len(df)} rows, columns={list(df.columns) if not df.empty else []}")
        logger.info(f"BIN columns after parsing: {bin_cols}")

        # WORKAROUND: If no BIN columns, extract them manually from results
        if not bin_cols and not df.empty:
            logger.info("Attempting manual bin extraction...")
            bin_data = []
            bin_names = {}  # Map BIN1 -> "GOOD", BIN20 -> "CONT", etc.
            bin_numbers = []  # List of actual bin numbers [1, 20, 65, ...]

            for stdout, step, ff in results:
                lines = stdout.split('\n')

                # Extract actual bin numbers from header (line with "Bin_" pattern)
                # Format: |    Bin_1   Bin_20   Bin_65
                for line in lines:
                    if 'Bin_' in line and '|' in line:
                        parts = line.split('|')
                        if len(parts) > 1:
                            bin_tokens = parts[1].strip().split()
                            bin_numbers = []
                            for token in bin_tokens:
                                if token.startswith('Bin_'):
                                    try:
                                        bin_num = int(token.replace('Bin_', ''))
                                        bin_numbers.append(bin_num)
                                    except ValueError:
                                        pass
                            logger.info(f"Extracted bin numbers: {bin_numbers}")
                        break

                # Extract bin names from MYQUICK or MULTI- line (line has bin names after |)
                # Format HMFN: MYQUICK: ... |     GOOD     CONT      SLT
                # Format HMB1/QMON: MULTI- ... |     GOOD   RETEST FUNC-FAI
                for line in lines:
                    if ('MYQUICK' in line or 'MULTI-' in line) and '|' in line:
                        parts = line.split('|')
                        if len(parts) > 1:
                            names = parts[1].strip().split()
                            # Map actual bin numbers to names
                            for i, name in enumerate(names):
                                if i < len(bin_numbers):
                                    bin_names[f'BIN{bin_numbers[i]}'] = name
                            logger.info(f"Bin names mapping: {bin_names}")
                        break

                # Extract bin values from data lines
                for line in lines:
                    if '|' in line and '202' in line:
                        parts = line.split('|')
                        if len(parts) > 1:
                            main_part = parts[0].strip().split()
                            bin_part = parts[1].strip()
                            if main_part and bin_part:
                                myquick = main_part[0]
                                bin_values = bin_part.split()
                                row_data = {'MYQUICK': myquick}
                                for i, bv in enumerate(bin_values):
                                    if i < len(bin_numbers):
                                        try:
                                            if bv != '-':
                                                row_data[f'BIN{bin_numbers[i]}'] = float(bv.replace(',', ''))
                                        except ValueError:
                                            pass
                                if len(row_data) > 1:  # Has at least one bin
                                    bin_data.append(row_data)

            if bin_data:
                bin_df = pd.DataFrame(bin_data)
                logger.info(f"Manual bin extraction: {len(bin_df)} rows, columns={list(bin_df.columns)}")
                logger.info(f"Bin names mapping: {bin_names}")
                # Store bin names in session state for chart labels
                st.session_state.bin_names = bin_names
                # Merge bin data with main df
                df = df.merge(bin_df, on='MYQUICK', how='left')
                bin_cols = [c for c in df.columns if c.startswith('BIN')]
                logger.info(f"After merge: BIN columns = {bin_cols}")

        if not df.empty:
            logger.debug(f"First few rows:\n{df.head()}")
        return df
    except Exception as e:
        logger.error("Failed to parse results: %s", e, exc_info=True)
        raise RuntimeError(f"Failed to parse frpt output: {e}") from e


def render_yield_trend_chart(processor: DataProcessor) -> None:
    """Render weekly yield trend line chart with volume bars."""
    st.subheader("Weekly Yield Trend")

    try:
        trend_data = processor.get_weekly_yield_trend()
        if trend_data.empty:
            st.info("No trend data available")
            return

        trend_data = trend_data.copy()
        # Series: design_id, form_factor, speed, density, step
        trend_data["series"] = (
            trend_data["design_id"].fillna("") + "_" +
            trend_data["form_factor"].fillna("") + "_" +
            trend_data["speed"].fillna("") + "_" +
            trend_data["density"].fillna("") + "_" +
            trend_data["step"].fillna("")
        )

        # Ensure workweek is string for proper categorical display (YYYYWW format)
        trend_data["workweek"] = trend_data["workweek"].astype(str)

        # Sort by workweek to ensure correct chronological order
        # Convert to int for proper numeric sorting, then back to string
        trend_data = trend_data.copy()
        trend_data["_ww_sort"] = trend_data["workweek"].astype(int)
        trend_data = trend_data.sort_values("_ww_sort")
        trend_data = trend_data.drop(columns=["_ww_sort"])

        # Get sorted unique workweeks for explicit category order
        sorted_workweeks = sorted(trend_data["workweek"].unique().tolist(), key=int)

        # Get unique series for filter
        all_series = sorted(trend_data["series"].unique().tolist())

        # Series filter
        selected_series = st.multiselect(
            "Select Series to Display",
            options=all_series,
            default=all_series,
            key="trend_series_filter",
            help="Filter which product combinations to show in the chart"
        )

        if not selected_series:
            st.warning("Please select at least one series to display")
            return

        # Option to pin data labels on chart
        show_labels = st.checkbox("Show data labels on chart", value=False, key="trend_show_labels")

        # Filter data by selected series
        filtered_data = trend_data[trend_data["series"].isin(selected_series)]

        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Color palette for series
        colors = px.colors.qualitative.Set1

        # Add traces for each series
        for i, series_name in enumerate(selected_series):
            series_data = filtered_data[filtered_data["series"] == series_name]
            color = colors[i % len(colors)]

            # Determine mode based on show_labels option
            trace_mode = "lines+markers+text" if show_labels else "lines+markers"
            text_values = series_data["yield_pct"].apply(lambda x: f"{x:.1f}%") if show_labels else None

            # Add yield line (primary y-axis)
            fig.add_trace(
                go.Scatter(
                    x=series_data["workweek"],
                    y=series_data["yield_pct"],
                    mode=trace_mode,
                    name=f"{series_name} (Yield)",
                    line=dict(color=color, width=2),
                    marker=dict(size=8),
                    text=text_values,
                    textposition="top center",
                    textfont=dict(size=9),
                    hovertemplate="WW%{x}<br>Yield: %{y:.2f}%<extra></extra>",
                ),
                secondary_y=False,
            )

            # Add volume bars (secondary y-axis)
            fig.add_trace(
                go.Bar(
                    x=series_data["workweek"],
                    y=series_data["UIN"],
                    name=f"{series_name} (Volume)",
                    marker=dict(color=color, opacity=0.3),
                    text=series_data["UIN"].apply(lambda x: f"{x:,.0f}") if show_labels else None,
                    textposition="outside",
                    textfont=dict(size=8),
                    hovertemplate="WW%{x}<br>Volume: %{y:,}<extra></extra>",
                ),
                secondary_y=True,
            )

        # Update layout with explicit category order for workweeks
        fig.update_layout(
            title="Yield % and Volume by Work Week",
            xaxis_title="Work Week (YYYYWW)",
            hovermode="x unified",
            xaxis=dict(
                type="category",
                categoryorder="array",
                categoryarray=sorted_workweeks,
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            barmode="group",
        )

        # Update y-axes
        fig.update_yaxes(title_text="Yield %", range=[95, 100], secondary_y=False)
        fig.update_yaxes(title_text="Volume (UIN)", secondary_y=True)

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
    """Render bin distribution bar chart with series filter."""
    st.subheader("Bin Distribution")

    try:
        # Get raw data to build series
        df = processor.dataframe
        if df.empty:
            st.info("No bin distribution data available")
            logger.warning("Bin chart: DataFrame is empty")
            return

        # Show bin type description based on test steps in data
        if "step" in df.columns:
            steps_in_data = df["step"].unique().tolist()
            bin_descriptions = []
            for step in steps_in_data:
                if step == "HMFN":
                    bin_descriptions.append(f"**{step}**: Soft Bin (Bin_1, Bin_20, Bin_65, etc.)")
                elif step in ["HMB1", "QMON"]:
                    bin_descriptions.append(f"**{step}**: Hard Bin (Bin_1, Bin_2, Bin_3)")
                else:
                    bin_descriptions.append(f"**{step}**: Bin type varies")
            if bin_descriptions:
                st.caption(" | ".join(bin_descriptions))

        # Find BIN columns (parser creates BIN1, BIN2, etc.)
        bin_cols = [col for col in df.columns if col.startswith("BIN")]
        logger.info(f"Bin chart: Found columns={list(df.columns)}")
        logger.info(f"Bin chart: BIN columns found={bin_cols}")

        if not bin_cols:
            st.info("No bin data available in the dataset")
            logger.warning("Bin chart: No BIN columns found in data")
            return

        # Check if bins have actual data (not all zeros/NaN)
        bin_data_sample = df[bin_cols].head()
        logger.info(f"Bin chart: Sample bin data:\n{bin_data_sample}")

        # Check for non-zero values
        total_non_zero = (df[bin_cols] > 0).sum().sum()
        logger.info(f"Bin chart: Total non-zero bin values={total_non_zero}")

        # Create series column
        df = df.copy()
        df["series"] = (
            df["design_id"].fillna("") + "_" +
            df["form_factor"].fillna("") + "_" +
            df["speed"].fillna("") + "_" +
            df["density"].fillna("") + "_" +
            df["step"].fillna("")
        )

        # Get unique series for filter
        all_series = sorted(df["series"].unique().tolist())

        # Series filter
        selected_series = st.multiselect(
            "Select Series to Display",
            options=all_series,
            default=all_series,
            key="bin_series_filter",
            help="Filter which product combinations to show"
        )

        if not selected_series:
            st.warning("Please select at least one series to display")
            return

        # Filter data by selected series
        filtered_df = df[df["series"].isin(selected_series)]

        # Melt bin columns to long format
        bin_data = filtered_df.melt(
            id_vars=["series", "step"],
            value_vars=bin_cols,
            var_name="bin",
            value_name="percentage"
        )

        # Drop NaN values
        logger.info(f"Bin chart: Melted data rows before dropna={len(bin_data)}")
        bin_data = bin_data.dropna(subset=["percentage"])
        logger.info(f"Bin chart: Melted data rows after dropna={len(bin_data)}")

        if bin_data.empty:
            st.info("No bin data available for selected series")
            logger.warning("Bin chart: All bin data was NaN")
            return

        # Group by series and bin, take mean
        bin_data = bin_data.groupby(["series", "bin"])["percentage"].mean().reset_index()

        # Sort bins naturally (BIN1, BIN2, ...)
        bin_data["bin_num"] = bin_data["bin"].str.extract(r"(\d+)").astype(int)
        bin_data = bin_data.sort_values(["bin_num", "series"])

        # Add bin names in frpt format (e.g., "Bin_1:GOOD") if available
        bin_names = st.session_state.get("bin_names", {})
        if bin_names:
            bin_data["bin_label"] = bin_data["bin"].apply(
                lambda x: f"Bin_{x[3:]}:{bin_names.get(x, '')}" if bin_names.get(x) else f"Bin_{x[3:]}"
            )
        else:
            # Convert BIN1 to Bin_1 format
            bin_data["bin_label"] = bin_data["bin"].apply(lambda x: f"Bin_{x[3:]}")

        # Option to pin data labels on chart
        show_bin_labels = st.checkbox("Show data labels on chart", value=False, key="bin_show_labels")

        # Add text column for labels
        bin_data["label_text"] = bin_data["percentage"].apply(lambda x: f"{x:.1f}%")

        fig = px.bar(
            bin_data,
            x="bin_label",
            y="percentage",
            color="series",
            barmode="group",
            title="Bin Distribution by Series",
            labels={
                "bin_label": "Bin",
                "percentage": "Percentage %",
                "series": "Series",
            },
            custom_data=["series", "bin"],
            text="label_text" if show_bin_labels else None,
        )

        # Enhanced hover template with multiple data points
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" +
                          "Series: %{customdata[0]}<br>" +
                          "Percentage: %{y:.2f}%<br>" +
                          "<extra></extra>",
            textposition="outside" if show_bin_labels else None,
            textfont=dict(size=9) if show_bin_labels else None,
        )

        fig.update_layout(
            xaxis_title="Bin",
            yaxis_title="Percentage %",
            legend_title="Series",
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
        )

        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        logger.error("Failed to render bin chart: %s", e)
        st.error("Failed to render bin distribution chart")


def render_density_speed_heatmap(processor: DataProcessor) -> None:
    """Render yield heatmap by density and speed, with separate charts per step/design_id."""
    st.subheader("Yield by Density & Speed")

    try:
        data = processor.get_yield_by_density_speed()
        if data.empty:
            st.info("No density/speed data available")
            return

        # Check if we have multiple steps or design_ids
        df = processor.dataframe
        unique_steps = df["step"].unique().tolist() if "step" in df.columns else []
        unique_design_ids = df["design_id"].unique().tolist() if "design_id" in df.columns else []

        # If multiple steps or design_ids, create separate charts
        if len(unique_steps) > 1 or len(unique_design_ids) > 1:
            # Add step and design_id to the data if not present
            raw_df = df.copy()

            for design_id in unique_design_ids:
                for step in unique_steps:
                    # Filter data for this combination
                    filtered = raw_df[(raw_df["design_id"] == design_id) & (raw_df["step"] == step)]
                    if filtered.empty:
                        continue

                    # Calculate yield by density/speed for this subset
                    if "density" in filtered.columns and "speed" in filtered.columns:
                        grouped = (
                            filtered.groupby(["density", "speed"])
                            .agg({"UIN": "sum", "UPASS": "sum"})
                            .reset_index()
                        )
                        grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(2)

                        if grouped.empty:
                            continue

                        pivot = grouped.pivot_table(
                            index="density",
                            columns="speed",
                            values="yield_pct",
                            aggfunc="mean",
                        )

                        if pivot.empty:
                            continue

                        fig = px.imshow(
                            pivot,
                            text_auto=".1f",
                            color_continuous_scale="RdYlGn",
                            aspect="auto",
                            title=f"Yield % by Density & Speed - {design_id} / {step}",
                            labels={"color": "Yield %"},
                        )

                        # Enhanced hover for heatmap
                        fig.update_traces(
                            hovertemplate="<b>Density:</b> %{y}<br>" +
                                          "<b>Speed:</b> %{x}<br>" +
                                          "<b>Yield:</b> %{z:.2f}%<br>" +
                                          f"<b>Design:</b> {design_id}<br>" +
                                          f"<b>Step:</b> {step}<br>" +
                                          "<extra></extra>"
                        )

                        fig.update_layout(
                            xaxis_title="Speed",
                            yaxis_title="Density",
                        )

                        st.plotly_chart(fig, use_container_width=True)
        else:
            # Single step/design_id - show combined chart
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

            # Enhanced hover for heatmap
            fig.update_traces(
                hovertemplate="<b>Density:</b> %{y}<br>" +
                              "<b>Speed:</b> %{x}<br>" +
                              "<b>Yield:</b> %{z:.2f}%<br>" +
                              "<extra></extra>"
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


def fetch_elc_data(filters: dict[str, Any], use_cache: bool = True) -> pd.DataFrame:
    """Fetch data for ELC yield calculation (HMFN, HMB1, QMON steps).

    Args:
        filters: Filter parameters (design_ids, form_factors, workweeks, etc.)
        use_cache: Whether to use cached results

    Returns:
        DataFrame with yield data for all three steps
    """
    logger.info("=" * 60)
    logger.info(f"FETCH ELC DATA STARTED (cache={'ON' if use_cache else 'OFF'})")
    logger.info(f"Filters: {filters}")

    # Use more workers for ELC since it's 3 steps
    runner = FrptRunner(max_workers=8, use_cache=use_cache)
    parser = FrptParser()

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        logger.info(f"Workweeks to fetch: {workweeks}")
    except Exception as e:
        logger.error("Failed to generate workweek range: %s", e)
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    # Build commands for HMFN, HMB1, QMON steps
    elc_steps = ["HMFN", "HMB1", "QMON"]
    commands = []
    for design_id in filters["design_ids"]:
        for step in elc_steps:
            for form_factor in filters["form_factors"]:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=design_id,
                            facility=filters["facility"],
                        )
                        commands.append(command)
                    except ValueError as e:
                        logger.warning(f"Invalid command parameters: {e}")

    total_calls = len(commands)
    logger.info(f"Total frpt calls for ELC: {total_calls}")

    if total_calls == 0:
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(completed: int, total: int, cmd: FrptCommand) -> None:
        progress_bar.progress(completed / total)
        status_text.text(f"Completed {completed}/{total}: {cmd.step}/{cmd.form_factor}/WW{cmd.workweek}")

    # Execute all commands in parallel
    cmd_results = runner.run_parallel(commands, progress_callback=progress_callback)

    progress_bar.empty()
    status_text.empty()

    # Collect results - run_parallel returns list of (FrptCommand, FrptResult) tuples
    results = []
    for cmd, result in cmd_results:
        if result.success and result.stdout:
            results.append((result.stdout, cmd.step, cmd.form_factor))

    if not results:
        logger.warning("No successful results for ELC data")
        return pd.DataFrame()

    # Parse results
    try:
        all_parsed = []
        for stdout, step, ff in results:
            parsed = parser.parse(stdout, step)
            if not parsed.empty:
                all_parsed.append(parsed)

        if not all_parsed:
            return pd.DataFrame()

        df = pd.concat(all_parsed, ignore_index=True)
        logger.info(f"ELC data parsed: {len(df)} rows")
        return df

    except Exception as e:
        logger.error("Failed to parse ELC results: %s", e)
        raise RuntimeError(f"Failed to parse frpt output: {e}") from e


def calculate_elc_yields(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate HMFN, SLT, and ELC yields from raw data.

    SLT yield = HMB1 yield × QMON yield
    ELC yield = SLT yield × HMFN yield

    Args:
        df: DataFrame with yield data including step column

    Returns:
        DataFrame with calculated yields per design_id, form_factor, density, speed, workweek
    """
    if df.empty:
        return pd.DataFrame()

    # Group by key dimensions and step, calculate yield
    group_cols = ["design_id", "form_factor", "density", "speed", "workweek"]
    available_cols = [c for c in group_cols if c in df.columns]

    if not available_cols:
        logger.warning("No grouping columns available for ELC calculation")
        return pd.DataFrame()

    # Calculate yield per group
    grouped = df.groupby(available_cols + ["step"]).agg({
        "UIN": "sum",
        "UPASS": "sum"
    }).reset_index()

    grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(4)

    # Pivot to get HMFN, HMB1, QMON as separate columns
    pivot = grouped.pivot_table(
        index=available_cols,
        columns="step",
        values="yield_pct",
        aggfunc="first"
    ).reset_index()

    # Rename columns for clarity
    pivot.columns.name = None

    # Calculate SLT and ELC yields
    if "HMB1" in pivot.columns and "QMON" in pivot.columns:
        pivot["SLT"] = (pivot["HMB1"] * pivot["QMON"] / 100).round(2)
    else:
        pivot["SLT"] = None

    if "HMFN" in pivot.columns and "SLT" in pivot.columns:
        pivot["ELC"] = (pivot["HMFN"] * pivot["SLT"] / 100).round(2)
    else:
        pivot["ELC"] = None

    # Round individual yields
    for col in ["HMFN", "HMB1", "QMON"]:
        if col in pivot.columns:
            pivot[col] = pivot[col].round(2)

    logger.info(f"ELC yields calculated: {len(pivot)} rows")
    return pivot


def render_elc_yield_tab(filters: dict[str, Any]) -> None:
    """Render the Module ELC Yield tab content."""
    st.header("Module ELC Yield")
    st.markdown("""
    **Yield Calculations:**
    - **HMFN**: Hot Module Final Test yield
    - **SLT**: System Level Test yield = HMB1 × QMON
    - **ELC**: End-of-Line yield = HMFN × SLT
    """)

    # Cache controls
    use_cache = st.session_state.get("use_cache", True)

    # Calculate estimated queries
    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        total_queries = len(filters["design_ids"]) * 3 * len(filters["form_factors"]) * len(workweeks)
        est_time = max((total_queries // 8) * 0.5, 1)  # ~30sec per batch of 8
    except Exception:
        total_queries = 0
        est_time = 0

    # Fetch button for ELC data
    col1, col2 = st.columns([1, 4])
    with col1:
        fetch_elc = st.button(
            "Fetch ELC Data",
            type="primary",
            use_container_width=True,
            key="fetch_elc_btn"
        )
    with col2:
        if total_queries > 0:
            st.caption(f"Will fetch {total_queries} queries (3 steps × {len(workweeks)} weeks). Est. time: ~{est_time:.0f} min if not cached.")

    if fetch_elc:
        try:
            st.warning(f"Fetching HMFN, HMB1, and QMON data ({total_queries} queries)... Cached results are instant, fresh queries take ~30s each.")
            elc_raw = fetch_elc_data(filters, use_cache=use_cache)

            if elc_raw.empty:
                st.warning("No data returned for ELC calculation.")
                return

            # Apply speed and density filters if specified
            if filters.get("speeds") and "speed" in elc_raw.columns:
                elc_raw = elc_raw[elc_raw["speed"].isin(filters["speeds"])]
                logger.info(f"Filtered by speeds {filters['speeds']}: {len(elc_raw)} rows")

            if filters.get("densities") and "density" in elc_raw.columns:
                elc_raw = elc_raw[elc_raw["density"].isin(filters["densities"])]
                logger.info(f"Filtered by densities {filters['densities']}: {len(elc_raw)} rows")

            if elc_raw.empty:
                st.warning("No data after applying speed/density filters.")
                return

            # Calculate yields
            elc_yields = calculate_elc_yields(elc_raw)
            st.session_state.elc_data = elc_yields
            st.session_state.elc_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success(f"Loaded {len(elc_yields)} ELC yield records!")

        except Exception as e:
            st.error(f"Failed to fetch ELC data: {e}")
            logger.exception("ELC fetch error")
            return

    # Display ELC data if available
    if not st.session_state.elc_data.empty:
        elc_df = st.session_state.elc_data.copy()

        if st.session_state.elc_last_fetch_time:
            st.caption(f"Last fetched: {st.session_state.elc_last_fetch_time}")

        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            avg_hmfn = elc_df["HMFN"].mean() if "HMFN" in elc_df.columns else 0
            st.metric("Avg HMFN Yield", f"{avg_hmfn:.2f}%")
        with col2:
            avg_slt = elc_df["SLT"].mean() if "SLT" in elc_df.columns else 0
            st.metric("Avg SLT Yield", f"{avg_slt:.2f}%")
        with col3:
            avg_elc = elc_df["ELC"].mean() if "ELC" in elc_df.columns else 0
            st.metric("Avg ELC Yield", f"{avg_elc:.2f}%")

        st.divider()

        # Display order for columns
        display_cols = ["design_id", "form_factor", "density", "speed", "workweek",
                        "HMFN", "HMB1", "QMON", "SLT", "ELC"]
        available_display = [c for c in display_cols if c in elc_df.columns]

        # Data table
        st.subheader("ELC Yield Data")
        st.dataframe(
            elc_df[available_display].sort_values(
                by=["workweek"] if "workweek" in elc_df.columns else available_display[:1],
                ascending=True
            ),
            use_container_width=True,
            hide_index=True
        )

        st.divider()

        # ELC Trend Chart
        if "workweek" in elc_df.columns:
            st.subheader("ELC Yield Trend")

            # Chart options
            col1, col2 = st.columns(2)
            with col1:
                # Option to pin data labels on chart
                show_elc_labels = st.checkbox("Show data labels on chart", value=False, key="elc_trend_show_labels")
            with col2:
                # Y-axis scale adjustment
                y_min = st.slider(
                    "Y-axis minimum (%)",
                    min_value=0,
                    max_value=98,
                    value=90,
                    step=1,
                    key="elc_yaxis_min",
                    help="Adjust to zoom in on data points close together"
                )

            # Create series identifier
            series_cols = [c for c in ["design_id", "form_factor", "density", "speed"] if c in elc_df.columns]
            if series_cols:
                elc_df["series"] = elc_df[series_cols].astype(str).agg("_".join, axis=1)
            else:
                elc_df["series"] = "All"

            # Ensure workweek is in YYYYWW format (remove any WW prefix if present)
            elc_df["workweek"] = elc_df["workweek"].astype(str).str.replace("WW", "")

            # Sort by workweek numerically
            elc_df["ww_sort"] = elc_df["workweek"].astype(int)
            elc_df = elc_df.sort_values("ww_sort")

            # Get sorted workweeks for x-axis ordering
            sorted_workweeks = elc_df["workweek"].unique().tolist()

            # Melt for plotting
            plot_cols = [c for c in ["HMFN", "SLT", "ELC"] if c in elc_df.columns]
            if plot_cols:
                plot_df = elc_df.melt(
                    id_vars=["workweek", "series"],
                    value_vars=plot_cols,
                    var_name="Yield Type",
                    value_name="Yield %"
                )

                # Add text column for labels
                plot_df["label_text"] = plot_df["Yield %"].apply(lambda x: f"{x:.1f}%")

                # Build chart using graph objects for better control
                fig = go.Figure()

                # Color map for yield types
                colors = {"HMFN": "#636EFA", "SLT": "#EF553B", "ELC": "#00CC96"}

                # Group by series and yield type
                for series_name in plot_df["series"].unique():
                    for yield_type in plot_cols:
                        mask = (plot_df["series"] == series_name) & (plot_df["Yield Type"] == yield_type)
                        series_data = plot_df[mask].sort_values("workweek")

                        if series_data.empty:
                            continue

                        # Determine trace mode based on show_labels
                        trace_mode = "lines+markers+text" if show_elc_labels else "lines+markers"

                        fig.add_trace(
                            go.Scatter(
                                x=series_data["workweek"],
                                y=series_data["Yield %"],
                                mode=trace_mode,
                                name=f"{yield_type} ({series_name})" if series_name != "All" else yield_type,
                                text=series_data["label_text"] if show_elc_labels else None,
                                textposition="top center",
                                textfont=dict(size=9),
                                line=dict(color=colors.get(yield_type, "#636EFA")),
                                marker=dict(size=8),
                                customdata=[[series_name, yield_type]] * len(series_data),
                                hovertemplate="<b>Work Week:</b> %{x}<br>" +
                                              f"<b>Yield Type:</b> {yield_type}<br>" +
                                              "<b>Yield:</b> %{y:.2f}%<br>" +
                                              f"<b>Series:</b> {series_name}<br>" +
                                              "<extra></extra>",
                            )
                        )

                # Add HMFN yield target marker (99% dotted line)
                # Applies to all design_id regardless of speed and density
                fig.add_hline(
                    y=99,
                    line_dash="dot",
                    line_color="red",
                    line_width=2,
                    annotation_text="HMFN Target: 99%",
                    annotation_position="right",
                    annotation_font_size=10,
                    annotation_font_color="red",
                )

                fig.update_layout(
                    title="HMFN, SLT & ELC Yield Trend",
                    xaxis_title="Work Week (YYYYWW)",
                    yaxis_title="Yield %",
                    yaxis=dict(range=[y_min, 101]),
                    legend_title="Yield Type",
                    hovermode="x unified",
                    xaxis=dict(
                        type="category",
                        categoryorder="array",
                        categoryarray=sorted_workweeks
                    )
                )

                st.plotly_chart(fig, use_container_width=True)

        # Heatmap by density/speed
        if "density" in elc_df.columns and "speed" in elc_df.columns and "ELC" in elc_df.columns:
            st.subheader("ELC Yield by Density & Speed")

            pivot = elc_df.pivot_table(
                index="density",
                columns="speed",
                values="ELC",
                aggfunc="mean"
            )

            if not pivot.empty:
                fig = px.imshow(
                    pivot,
                    text_auto=".1f",
                    color_continuous_scale="RdYlGn",
                    aspect="auto",
                    title="Average ELC Yield % by Density and Speed",
                    labels={"color": "ELC Yield %"}
                )

                # Enhanced hover for ELC heatmap
                fig.update_traces(
                    hovertemplate="<b>Density:</b> %{y}<br>" +
                                  "<b>Speed:</b> %{x}<br>" +
                                  "<b>ELC Yield:</b> %{z:.2f}%<br>" +
                                  "<extra></extra>"
                )

                fig.update_layout(
                    xaxis_title="Speed",
                    yaxis_title="Density"
                )
                st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Click 'Fetch ELC Data' to load HMFN, HMB1, and QMON yield data for ELC calculation.")


def parse_failed_registers(stdout: str, step: str, workweek: str, design_id: str,
                           form_factor: str) -> list[dict]:
    """Parse failed register data from frpt output.

    Args:
        stdout: Raw frpt output
        step: Test step (HMFN, HMB1, QMON)
        workweek: Work week (YYYYWW)
        design_id: Design ID
        form_factor: Module form factor

    Returns:
        List of dicts with register failure data
    """
    results = []
    lines = stdout.split('\n')

    # Find the register failure section (starts after "Register Name" header)
    in_register_section = False
    for line in lines:
        if 'Register Name' in line and 'FFail' in line:
            in_register_section = True
            continue

        if in_register_section:
            # Skip separator lines
            if line.startswith('~~~') or not line.strip():
                continue

            # Parse register line: "register_name#bin    count    %    per_myquick_values..."
            parts = line.split()
            if len(parts) >= 3:
                try:
                    register_name = parts[0]
                    ffail = int(parts[1])
                    pct = float(parts[2])

                    # Extract MYQUICK-specific percentages if available
                    myquick_pcts = {}
                    if len(parts) > 3:
                        # Remaining parts are percentages per MYQUICK column
                        for i, val in enumerate(parts[3:]):
                            try:
                                if val != '-':
                                    myquick_pcts[f'pct_{i}'] = float(val)
                            except ValueError:
                                pass

                    results.append({
                        'register_name': register_name,
                        'ffail': ffail,
                        'fallout_pct': pct,
                        'step': step,
                        'workweek': workweek,
                        'design_id': design_id,
                        'form_factor': form_factor,
                        **myquick_pcts
                    })
                except (ValueError, IndexError):
                    continue

    return results


def fetch_pareto_data(filters: dict[str, Any], use_cache: bool = True) -> pd.DataFrame:
    """Fetch pareto data for HMFN, HMB1, QMON steps.

    Args:
        filters: Filter parameters
        use_cache: Whether to use cached results

    Returns:
        DataFrame with failed register data
    """
    logger.info("=" * 60)
    logger.info(f"FETCH PARETO DATA STARTED (cache={'ON' if use_cache else 'OFF'})")

    runner = FrptRunner(max_workers=8, use_cache=use_cache)

    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
    except Exception as e:
        raise RuntimeError(f"Invalid workweek range: {e}") from e

    # Build commands for HMFN, HMB1, QMON
    pareto_steps = ["HMFN", "HMB1", "QMON"]
    commands = []
    for design_id in filters["design_ids"]:
        for step in pareto_steps:
            for form_factor in filters["form_factors"]:
                for workweek in workweeks:
                    try:
                        command = FrptCommand(
                            step=step,
                            form_factor=form_factor,
                            workweek=workweek,
                            dbase=design_id,
                            facility=filters["facility"],
                        )
                        commands.append(command)
                    except ValueError as e:
                        logger.warning(f"Invalid command: {e}")

    total_calls = len(commands)
    logger.info(f"Total frpt calls for Pareto: {total_calls}")

    if total_calls == 0:
        return pd.DataFrame()

    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(completed: int, total: int, cmd: FrptCommand) -> None:
        progress_bar.progress(completed / total)
        status_text.text(f"Completed {completed}/{total}: {cmd.step}/{cmd.form_factor}/WW{cmd.workweek}")

    cmd_results = runner.run_parallel(commands, progress_callback=progress_callback)

    progress_bar.empty()
    status_text.empty()

    # Parse failed registers from each result
    all_registers = []
    for cmd, result in cmd_results:
        if result.success and result.stdout:
            registers = parse_failed_registers(
                result.stdout,
                cmd.step,
                cmd.workweek,
                cmd.dbase,
                cmd.form_factor
            )
            all_registers.extend(registers)

    if not all_registers:
        return pd.DataFrame()

    df = pd.DataFrame(all_registers)
    logger.info(f"Pareto data: {len(df)} register failures parsed")
    return df


def render_pareto_tab(filters: dict[str, Any]) -> None:
    """Render the Pareto Analysis tab content."""
    st.header("Pareto Analysis")
    st.markdown("""
    **Top 5 Failed Registers** by test step (HMFN, HMB1, QMON).
    Shows register fallout breakdown by design_id, form_factor, speed, density, and workweek.
    """)

    use_cache = st.session_state.get("use_cache", True)

    # Calculate estimated queries
    try:
        workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
        total_queries = len(filters["design_ids"]) * 3 * len(filters["form_factors"]) * len(workweeks)
    except Exception:
        total_queries = 0

    # Fetch button
    col1, col2 = st.columns([1, 4])
    with col1:
        fetch_pareto = st.button(
            "Fetch Pareto Data",
            type="primary",
            use_container_width=True,
            key="fetch_pareto_btn"
        )
    with col2:
        if total_queries > 0:
            st.caption(f"Will fetch {total_queries} queries (3 steps × {len(workweeks)} weeks).")

    if fetch_pareto:
        try:
            st.warning(f"Fetching failed register data for HMFN, HMB1, QMON ({total_queries} queries)...")
            pareto_df = fetch_pareto_data(filters, use_cache=use_cache)

            if pareto_df.empty:
                st.warning("No failed register data returned.")
                return

            st.session_state.pareto_data = pareto_df
            st.session_state.pareto_last_fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success(f"Loaded {len(pareto_df)} register failure records!")

        except Exception as e:
            st.error(f"Failed to fetch pareto data: {e}")
            logger.exception("Pareto fetch error")
            return

    # Display pareto data if available
    if not st.session_state.pareto_data.empty:
        pareto_df = st.session_state.pareto_data.copy()

        if st.session_state.pareto_last_fetch_time:
            st.caption(f"Last fetched: {st.session_state.pareto_last_fetch_time}")

        # Ensure workweek is in YYYYWW format
        if "workweek" in pareto_df.columns:
            pareto_df["workweek"] = pareto_df["workweek"].astype(str).str.replace("WW", "")

        st.divider()

        # Option to pin data labels on chart
        show_pareto_labels = st.checkbox("Show data labels on chart", value=False, key="pareto_show_labels")

        # Display by step
        for step in ["HMFN", "HMB1", "QMON"]:
            step_data = pareto_df[pareto_df["step"] == step]
            if step_data.empty:
                continue

            st.subheader(f"{step} - Top 5 Failed Registers by Fallout %")

            # Get unique combinations of design_id, form_factor, density
            group_cols = [c for c in ["design_id", "form_factor"] if c in step_data.columns]

            if not group_cols:
                continue

            # Get unique groups
            unique_groups = step_data[group_cols].drop_duplicates()

            for _, group_row in unique_groups.iterrows():
                # Filter data for this group
                mask = True
                group_label_parts = []
                for col in group_cols:
                    mask = mask & (step_data[col] == group_row[col])
                    group_label_parts.append(f"{group_row[col]}")

                group_data = step_data[mask]
                group_label = " / ".join(group_label_parts)

                if group_data.empty:
                    continue

                st.markdown(f"**{group_label}**")

                # Get top 5 registers by average fallout % across all weeks
                top_registers = (
                    group_data.groupby("register_name")["fallout_pct"]
                    .mean()
                    .nlargest(5)
                    .index.tolist()
                )

                if not top_registers:
                    continue

                # Filter to only top 5 registers
                top_data = group_data[group_data["register_name"].isin(top_registers)]

                # Pivot: registers as rows, workweeks as columns
                pivot_df = top_data.pivot_table(
                    index="register_name",
                    columns="workweek",
                    values="fallout_pct",
                    aggfunc="first"
                )

                # Sort columns (workweeks) numerically
                sorted_cols = sorted(pivot_df.columns, key=lambda x: int(str(x)))
                pivot_df = pivot_df[sorted_cols]

                # Sort rows by average fallout % descending
                pivot_df["avg"] = pivot_df.mean(axis=1)
                pivot_df = pivot_df.sort_values("avg", ascending=False)
                pivot_df = pivot_df.drop(columns=["avg"])

                # Reset index to make register_name a column
                pivot_df = pivot_df.reset_index()
                pivot_df = pivot_df.rename(columns={"register_name": "Register Name"})

                # Display table
                st.dataframe(
                    pivot_df,
                    use_container_width=True,
                    hide_index=True
                )

                # Pareto chart for this group - fallout %
                st.markdown(f"**{step} Pareto Chart - {group_label} (Avg Fallout %)**")

                chart_data = (
                    group_data.groupby("register_name").agg({
                        "fallout_pct": "mean",
                        "ffail": "sum"
                    }).reset_index()
                )
                chart_data = chart_data.nlargest(10, "fallout_pct")
                chart_data = chart_data.sort_values("fallout_pct", ascending=True)

                # Add text column for labels
                chart_data["label_text"] = chart_data["fallout_pct"].apply(lambda x: f"{x:.2f}%")

                fig = px.bar(
                    chart_data,
                    y="register_name",
                    x="fallout_pct",
                    orientation="h",
                    title=f"{step} - Top 10 Failed Registers (Avg Fallout %)",
                    labels={"register_name": "Register", "fallout_pct": "Avg Fallout %"},
                    custom_data=["ffail"],
                    text="label_text" if show_pareto_labels else None,
                )

                # Enhanced hover template for Pareto chart
                fig.update_traces(
                    hovertemplate="<b>Register:</b> %{y}<br>" +
                                  "<b>Avg Fallout:</b> %{x:.2f}%<br>" +
                                  "<b>Total Fail Count:</b> %{customdata[0]:,}<br>" +
                                  f"<b>Step:</b> {step}<br>" +
                                  f"<b>Group:</b> {group_label}<br>" +
                                  "<extra></extra>",
                    textposition="outside" if show_pareto_labels else None,
                    textfont=dict(size=9) if show_pareto_labels else None,
                )

                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")

            st.divider()

    else:
        st.info("Click 'Fetch Pareto Data' to load failed register data for HMFN, HMB1, and QMON.")


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

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Yield Analysis", "Module ELC Yield", "Pareto Analysis"])

    with tab1:
        # Fetch button for Module Yield data
        col1, col2 = st.columns([1, 4])
        with col1:
            fetch_button = st.button(
                "Fetch Module Yield Data",
                type="primary",
                use_container_width=True,
                disabled=st.session_state.fetch_in_progress,
                key="fetch_module_yield_btn"
            )
        with col2:
            # Estimate number of calls
            try:
                workweeks = Settings.get_workweek_range(filters["start_ww"], filters["end_ww"])
                total_calls = len(filters["design_ids"]) * len(filters["test_steps"]) * len(filters["form_factors"]) * len(workweeks)
                parallel_batches = (total_calls + 3) // 4
                estimated_time = max(parallel_batches * 0.5, 1)
                st.caption(f"Will fetch {total_calls} queries ({len(filters['design_ids'])} designs × {len(workweeks)} weeks × {len(filters['test_steps'])} steps × {len(filters['form_factors'])} forms). Est. time: ~{estimated_time:.0f} min if not cached.")
            except Exception:
                pass

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

    with tab2:
        render_elc_yield_tab(filters)

    with tab3:
        render_pareto_tab(filters)


if __name__ == "__main__":
    main()
