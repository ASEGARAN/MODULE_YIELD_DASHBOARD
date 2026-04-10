"""Process and transform data for visualization."""

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class YieldSummary:
    """Immutable yield summary statistics."""

    total_uin: int
    total_upass: int
    overall_yield: float
    avg_yield: float
    min_yield: float
    max_yield: float


class DataProcessor:
    """Transform data for dashboard visualization."""

    def __init__(self, df: pd.DataFrame):
        """Initialize processor with data.

        Args:
            df: DataFrame from FrptParser
        """
        self._df = df.copy()

    @property
    def dataframe(self) -> pd.DataFrame:
        """Get the underlying DataFrame."""
        return self._df.copy()

    def filter_data(
        self,
        form_factors: Optional[list[str]] = None,
        steps: Optional[list[str]] = None,
        densities: Optional[list[str]] = None,
        speeds: Optional[list[str]] = None,
        workweeks: Optional[list[str]] = None,
    ) -> "DataProcessor":
        """Filter data by specified criteria.

        Returns new DataProcessor with filtered data (immutable pattern).
        """
        filtered = self._df.copy()

        if form_factors:
            filtered = filtered[filtered["form_factor"].isin(form_factors)]
        if steps:
            filtered = filtered[filtered["step"].isin(steps)]
        if densities:
            filtered = filtered[filtered["density"].isin(densities)]
        if speeds:
            filtered = filtered[filtered["speed"].isin(speeds)]
        if workweeks:
            filtered = filtered[filtered["workweek"].isin(workweeks)]

        return DataProcessor(filtered)

    def get_weekly_yield_trend(self) -> pd.DataFrame:
        """Get yield trend data grouped by workweek.

        Returns:
            DataFrame with columns: workweek, design_id, form_factor, speed, density, step, UIN, UPASS, yield_pct
        """
        if self._df.empty:
            return pd.DataFrame()

        # Group by workweek, design_id, form_factor, speed, density, step
        group_cols = ["workweek", "design_id", "form_factor", "speed", "density", "step"]
        # Filter to only columns that exist in dataframe
        group_cols = [col for col in group_cols if col in self._df.columns]

        grouped = (
            self._df.groupby(group_cols)
            .agg({"UIN": "sum", "UPASS": "sum"})
            .reset_index()
        )

        # Calculate yield percentage
        grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(2)
        grouped["yield_pct"] = grouped["yield_pct"].fillna(0)

        # Sort by workweek
        grouped = grouped.sort_values("workweek")

        return grouped

    def get_yield_by_density_speed(self) -> pd.DataFrame:
        """Get yield breakdown by density and speed.

        Returns:
            DataFrame with yield metrics per density/speed combination
        """
        if self._df.empty:
            return pd.DataFrame()

        grouped = (
            self._df.groupby(["density", "speed", "step"])
            .agg({"UIN": "sum", "UPASS": "sum"})
            .reset_index()
        )

        grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(2)
        grouped["yield_pct"] = grouped["yield_pct"].fillna(0)

        return grouped

    def get_bin_distribution(self) -> pd.DataFrame:
        """Get bin distribution data.

        Returns:
            DataFrame with bin percentages
        """
        if self._df.empty:
            return pd.DataFrame()

        # Find BIN columns (support both "BIN" and "Bin_" formats)
        bin_cols = [col for col in self._df.columns if col.startswith("BIN") or col.startswith("Bin_")]
        if not bin_cols:
            return pd.DataFrame()

        # Calculate average bin percentages
        bin_data = self._df[bin_cols].mean().reset_index()
        bin_data.columns = ["bin", "percentage"]
        bin_data = bin_data.sort_values("bin")

        return bin_data

    def get_bin_distribution_by_step(self) -> pd.DataFrame:
        """Get bin distribution grouped by step.

        Returns:
            DataFrame with bin percentages per step
        """
        if self._df.empty:
            return pd.DataFrame()

        # Find BIN columns (support both "BIN" and "Bin_" formats)
        bin_cols = [col for col in self._df.columns if col.startswith("BIN") or col.startswith("Bin_")]
        if not bin_cols:
            return pd.DataFrame()

        # Group by step and calculate mean of bins
        grouped = self._df.groupby("step")[bin_cols].mean().reset_index()

        # Melt to long format for charting
        melted = grouped.melt(
            id_vars=["step"],
            value_vars=bin_cols,
            var_name="bin",
            value_name="percentage",
        )

        return melted

    def get_yield_summary(self) -> YieldSummary:
        """Get overall yield summary statistics.

        Returns:
            YieldSummary with aggregated metrics
        """
        if self._df.empty:
            return YieldSummary(
                total_uin=0,
                total_upass=0,
                overall_yield=0.0,
                avg_yield=0.0,
                min_yield=0.0,
                max_yield=0.0,
            )

        total_uin = int(self._df["UIN"].sum())
        total_upass = int(self._df["UPASS"].sum())
        overall_yield = (total_upass / total_uin * 100) if total_uin > 0 else 0.0

        # Calculate per-row yields for statistics
        row_yields = self._df["YIELD%"] if "YIELD%" in self._df.columns else pd.Series([0])

        return YieldSummary(
            total_uin=total_uin,
            total_upass=total_upass,
            overall_yield=round(overall_yield, 2),
            avg_yield=round(row_yields.mean(), 2) if not row_yields.empty else 0.0,
            min_yield=round(row_yields.min(), 2) if not row_yields.empty else 0.0,
            max_yield=round(row_yields.max(), 2) if not row_yields.empty else 0.0,
        )

    def get_did_breakdown(self, latest_week_only: bool = True, by_step: bool = False) -> pd.DataFrame:
        """Get yield breakdown by Design ID for the latest workweek.

        Args:
            latest_week_only: If True, only return data for the latest workweek
            by_step: If True, also break down by test step (HMFN, SLT, ELC)

        Returns:
            DataFrame with DID-level yield statistics
        """
        if self._df.empty:
            return pd.DataFrame()

        df = self._df.copy()

        # Filter to latest workweek if requested
        if latest_week_only and 'MFG_WORKWEEK' in df.columns:
            latest_ww = df['MFG_WORKWEEK'].max()
            df = df[df['MFG_WORKWEEK'] == latest_ww]

        # Determine DID column name
        did_col = None
        for col in ['DBASE', 'design_id', 'DESIGN_ID']:
            if col in df.columns:
                did_col = col
                break

        if did_col is None or df.empty:
            return pd.DataFrame()

        # Determine step column name
        step_col = None
        for col in ['STEP', 'step']:
            if col in df.columns:
                step_col = col
                break

        # Build group columns
        group_cols = [did_col]
        if by_step and step_col:
            group_cols.append(step_col)

        # Aggregate by DID (and optionally step)
        did_summary = df.groupby(group_cols).agg({
            'UIN': 'sum',
            'UPASS': 'sum'
        }).reset_index()

        # Rename columns
        if by_step and step_col:
            did_summary.columns = ['design_id', 'step', 'uin', 'upass']
        else:
            did_summary.columns = ['design_id', 'uin', 'upass']

        did_summary['yield_pct'] = (did_summary['upass'] / did_summary['uin'] * 100).round(2)
        did_summary['ufail'] = did_summary['uin'] - did_summary['upass']

        # Sort by DID then step (if applicable)
        if by_step and 'step' in did_summary.columns:
            # Define step order
            step_order = {'hmfn': 0, 'slt': 1, 'elc': 2}
            did_summary['step_order'] = did_summary['step'].str.lower().map(step_order).fillna(99)
            did_summary = did_summary.sort_values(['design_id', 'step_order'])
            did_summary = did_summary.drop(columns=['step_order'])
        else:
            did_summary = did_summary.sort_values('uin', ascending=False)

        # Add latest workweek info
        if 'MFG_WORKWEEK' in self._df.columns:
            did_summary['workweek'] = self._df['MFG_WORKWEEK'].max()

        return did_summary

    def get_summary_table(self) -> pd.DataFrame:
        """Get summary table for display.

        Returns:
            DataFrame suitable for table display
        """
        if self._df.empty:
            return pd.DataFrame()

        # Determine groupby columns based on available data
        group_cols = ["workweek", "step", "form_factor", "density", "speed"]
        if "design_id" in self._df.columns:
            group_cols.insert(2, "design_id")

        # Group by key dimensions
        grouped = (
            self._df.groupby(group_cols)
            .agg({"UIN": "sum", "UPASS": "sum"})
            .reset_index()
        )

        grouped["yield_pct"] = (grouped["UPASS"] / grouped["UIN"] * 100).round(2)
        grouped = grouped.sort_values(["workweek", "step", "form_factor"])

        # Rename columns for display
        rename_map = {
            "workweek": "Work Week",
            "step": "Test Step",
            "design_id": "Design ID",
            "form_factor": "Form Factor",
            "density": "Density",
            "speed": "Speed",
            "UIN": "Units In",
            "UPASS": "Units Pass",
            "yield_pct": "Yield %",
        }
        grouped = grouped.rename(columns=rename_map)

        return grouped

    def calculate_wow_change(self) -> pd.DataFrame:
        """Calculate week-over-week yield change.

        Returns:
            DataFrame with WoW change percentages
        """
        trend = self.get_weekly_yield_trend()
        if trend.empty:
            return pd.DataFrame()

        # Determine grouping columns (all except workweek and metrics)
        group_cols = ["design_id", "form_factor", "speed", "density", "step"]
        group_cols = [col for col in group_cols if col in trend.columns]

        # Sort and calculate diff
        sort_cols = group_cols + ["workweek"]
        trend = trend.sort_values(sort_cols)
        trend["wow_change"] = trend.groupby(group_cols)["yield_pct"].diff()
        trend["wow_change"] = trend["wow_change"].round(2)

        return trend
