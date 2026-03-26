"""Parse frpt command output into structured data."""

import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class ParsedMyquick:
    """Parsed MYQUICK identifier components."""

    workweek: str
    design_id: str
    form_factor: str
    density: str
    speed: str

    @classmethod
    def from_string(cls, myquick: str, step: str = "") -> Optional["ParsedMyquick"]:
        """Parse MYQUICK string into components.

        Args:
            myquick: MYQUICK string
            step: Test step name to determine format

        Format for HMB1/QMON: "202602_DESIGNID_SOCAMM_7500MTPS_192GB"
            (workweek, design_id, form_factor, speed, density)
        Format for HMFN: "202611_DESIGNID_SOCAMM2_192GB_7500MTPS"
            (workweek, design_id, form_factor, density, speed)
            Note: DBASE field in HMFN myquick is actually design_id

        Returns:
            ParsedMyquick or None if parsing fails
        """
        parts = myquick.split("_")
        if len(parts) < 5:
            return None

        step_upper = step.upper()
        if step_upper in ("HMB1", "QMON"):
            # HMB1/QMON format: workweek, design_id, form_factor, speed, density
            return cls(
                workweek=parts[0],
                design_id=parts[1],
                form_factor=parts[2],
                speed=parts[3],
                density=parts[4],
            )
        else:
            # HMFN format: workweek, design_id (DBASE), form_factor, density, speed
            return cls(
                workweek=parts[0],
                design_id=parts[1],  # DBASE field is actually design_id
                form_factor=parts[2],
                density=parts[3],
                speed=parts[4],
            )


class FrptParser:
    """Parse frpt tabular output."""

    # Pattern to match header separator line (dashes)
    SEPARATOR_PATTERN = re.compile(r"^[-\s]+$")

    # Pattern to extract numeric values (handles percentages)
    NUMERIC_PATTERN = re.compile(r"[\d.]+")

    def __init__(self):
        """Initialize parser."""
        self._columns: list[str] = []

    def parse(self, output: str, step: str) -> pd.DataFrame:
        """Parse frpt output into DataFrame.

        Args:
            output: Raw frpt command output
            step: Test step name for the data

        Returns:
            DataFrame with parsed yield data
        """
        lines = output.strip().split("\n")
        if not lines:
            return pd.DataFrame()

        # Find header and data sections
        header_idx = self._find_header_index(lines)
        if header_idx is None:
            return pd.DataFrame()

        # Parse header
        header_line = lines[header_idx]
        columns = self._parse_header(header_line)

        # Parse data rows
        data_rows = []
        for line in lines[header_idx + 2:]:  # Skip header and separator
            if not line.strip():
                continue
            if self.SEPARATOR_PATTERN.match(line):
                continue
            row = self._parse_data_row(line, columns)
            if row:
                row["step"] = step
                data_rows.append(row)

        if not data_rows:
            return pd.DataFrame()

        df = pd.DataFrame(data_rows)
        return self._enrich_dataframe(df, step)

    def _find_header_index(self, lines: list[str]) -> Optional[int]:
        """Find the index of the header line.

        The header is typically followed by a separator line of dashes.
        """
        for i, line in enumerate(lines[:-1]):
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            if self.SEPARATOR_PATTERN.match(next_line) and "MYQUICK" in line.upper():
                return i
        return None

    def _parse_header(self, header_line: str) -> list[str]:
        """Parse header line into column names."""
        # Split on multiple spaces (columns are space-separated)
        columns = header_line.split()
        return [col.strip().upper() for col in columns if col.strip()]

    def _parse_data_row(self, line: str, columns: list[str]) -> Optional[dict]:
        """Parse a single data row."""
        values = line.split()
        if len(values) < len(columns):
            return None

        row = {}
        for i, col in enumerate(columns):
            if i < len(values):
                value = values[i].strip()
                # Convert numeric values
                if col in ["UIN", "UPASS"]:
                    row[col] = self._parse_int(value)
                elif col == "YIELD%" or col.startswith("BIN"):
                    row[col] = self._parse_float(value)
                else:
                    row[col] = value
        return row

    def _parse_int(self, value: str) -> int:
        """Parse string to integer."""
        try:
            # Remove commas from numbers like "1,234"
            clean = value.replace(",", "")
            return int(clean)
        except (ValueError, TypeError):
            return 0

    def _parse_float(self, value: str) -> float:
        """Parse string to float (handles percentages)."""
        try:
            # Remove % sign and parse
            clean = value.replace("%", "").replace(",", "")
            return float(clean)
        except (ValueError, TypeError):
            return 0.0

    def _enrich_dataframe(self, df: pd.DataFrame, step: str) -> pd.DataFrame:
        """Enrich DataFrame with parsed MYQUICK components.

        Args:
            df: DataFrame with MYQUICK column
            step: Test step name to determine MYQUICK format

        Returns:
            DataFrame with additional parsed columns
        """
        if "MYQUICK" not in df.columns:
            return df

        # Parse MYQUICK into components (pass step for format detection)
        parsed = df["MYQUICK"].apply(
            lambda x: ParsedMyquick.from_string(x, step) if isinstance(x, str) else None
        )

        # Add component columns
        df = df.copy()
        df["workweek"] = parsed.apply(lambda p: p.workweek if p else None)
        df["design_id"] = parsed.apply(lambda p: p.design_id if p else None)
        df["form_factor"] = parsed.apply(lambda p: p.form_factor if p else None)
        df["density"] = parsed.apply(lambda p: p.density if p else None)
        df["speed"] = parsed.apply(lambda p: p.speed if p else None)

        return df

    def parse_multiple(self, results: list[tuple[str, str, str]]) -> pd.DataFrame:
        """Parse multiple frpt outputs and combine.

        Args:
            results: List of tuples (output, step, form_factor)

        Returns:
            Combined DataFrame
        """
        dfs = []
        for output, step, form_factor in results:
            df = self.parse(output, step)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
