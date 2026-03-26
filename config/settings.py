"""Configuration settings for Module Yield Dashboard."""

from dataclasses import dataclass, field
from typing import ClassVar


@dataclass(frozen=True)
class Settings:
    """Application settings and default values."""

    # Module form factors
    FORM_FACTORS: ClassVar[list[str]] = ["SOCAMM", "SOCAMM2"]

    # Test steps
    TEST_STEPS: ClassVar[list[str]] = ["HMFN", "HMB1", "QMON"]

    # Available databases
    DATABASES: ClassVar[list[str]] = ["y6cp", "y6ck", "y6cn"]

    # Module densities
    DENSITIES: ClassVar[list[str]] = ["48GB", "96GB", "192GB", "384GB"]

    # Module speeds
    SPEEDS: ClassVar[list[str]] = ["7500MTPS", "9600MTPS", "8000MTPS", "8800MTPS"]

    # Test facilities
    FACILITIES: ClassVar[list[str]] = ["PENANG", "SUZHOU", "XIAN"]

    # Default values
    DEFAULT_DATABASE: ClassVar[str] = "y6cp"
    DEFAULT_FACILITY: ClassVar[str] = "PENANG"

    # frpt command template
    FRPT_COMMAND_TEMPLATE: ClassVar[str] = (
        "frpt -xf -bin=soft "
        "-myquick=/MFG_WORKWEEK,DBASE,MODULE_FORM_FACTOR,MODULE_DENSITY,MODULE_SPEED/ "
        "-quick=/MFG_WORKWEEK,myquick/ -sort=// +nowrap +echo +regwidth +module "
        "-dbase={dbase} -step={step} -all -n -r +imesh "
        "-nonshippable=N/A -eng_summary=N/A -standard_flow=yes "
        "-machine_id=~SMT6 +# -module_form_factor={form_factor} "
        "-mfg_workweek={workweek} -test_facility={facility} +% +debug"
    )

    # Bin columns to extract (common soft bins)
    BIN_COLUMNS: ClassVar[list[str]] = [
        "BIN01", "BIN02", "BIN03", "BIN04", "BIN05",
        "BIN06", "BIN07", "BIN08", "BIN09", "BIN10",
        "BIN11", "BIN12", "BIN13", "BIN14", "BIN15",
    ]

    # Yield metrics columns
    YIELD_COLUMNS: ClassVar[list[str]] = ["UIN", "UPASS", "YIELD%"]

    @classmethod
    def get_workweek_range(cls, start_ww: str, end_ww: str) -> list[str]:
        """Generate list of workweeks between start and end (inclusive).

        Args:
            start_ww: Start workweek in YYYYWW format (e.g., "202601")
            end_ww: End workweek in YYYYWW format (e.g., "202612")

        Returns:
            List of workweeks in YYYYWW format
        """
        workweeks = []
        start_year = int(start_ww[:4])
        start_week = int(start_ww[4:])
        end_year = int(end_ww[:4])
        end_week = int(end_ww[4:])

        year = start_year
        week = start_week

        while (year < end_year) or (year == end_year and week <= end_week):
            workweeks.append(f"{year}{week:02d}")
            week += 1
            if week > 52:
                week = 1
                year += 1

        return workweeks
