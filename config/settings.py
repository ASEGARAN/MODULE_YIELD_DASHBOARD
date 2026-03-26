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

    # Design IDs (DBASE parameter in frpt)
    DESIGN_IDS: ClassVar[list[str]] = ["Y42M", "Y62P", "Y6CP", "Y63N"]

    # Module densities
    DENSITIES: ClassVar[list[str]] = ["32GB", "48GB", "64GB", "96GB", "128GB", "192GB", "256GB"]

    # Module speeds
    SPEEDS: ClassVar[list[str]] = ["6400", "7500", "8533", "9600"]

    # Test facilities
    FACILITIES: ClassVar[list[str]] = ["all", "PENANG", "SUZHOU", "XIAN"]

    # Default values
    DEFAULT_DESIGN_ID: ClassVar[str] = "Y6CP"
    DEFAULT_FACILITY: ClassVar[str] = "all"

    # frpt command template for HMFN step (soft bins)
    FRPT_COMMAND_TEMPLATE_HMFN: ClassVar[str] = (
        "frpt -xf -bin=soft "
        "-myquick=/MFG_WORKWEEK,DBASE,MODULE_FORM_FACTOR,MODULE_DENSITY,MODULE_SPEED/ "
        "-quick=/MFG_WORKWEEK,myquick/ -sort=// +nowrap +echo +regwidth +module "
        "-dbase={dbase} -step=hmfn -all -n -r +imesh "
        "-nonshippable=N/A -eng_summary=N/A -standard_flow=yes "
        "-machine_id=~SMT6 +# -module_form_factor={form_factor} "
        "-mfg_workweek={workweek} -test_facility={facility} +% +debug"
    )

    # frpt command template for HMB1 step (hard bins)
    FRPT_COMMAND_TEMPLATE_HMB1: ClassVar[str] = (
        "frpt +regwidth +% +# -test_facility={facility} -dbase={dbase} +% +module -xf +# "
        "-sort=// -myquick=/MFG_WORKWEEK,DESIGN_ID,MODULE_FORM_FACTOR,MODULE_SPEED,MODULE_DENSITY/ "
        "+quick=/myquick,step/ +echo -bin=hard -step=hmb1 -eng_summary=N/A "
        "-standard_flow=YES -module_form_factor={form_factor} "
        "-mfg_workweek={workweek} -n -r +imesh -nonshippable=N/A +debug -top5"
    )

    # frpt command template for QMON step (hard bins, same as HMB1)
    FRPT_COMMAND_TEMPLATE_QMON: ClassVar[str] = (
        "frpt +regwidth +% +# -test_facility={facility} -dbase={dbase} +% +module -xf +# "
        "-sort=// -myquick=/MFG_WORKWEEK,DESIGN_ID,MODULE_FORM_FACTOR,MODULE_SPEED,MODULE_DENSITY/ "
        "+quick=/myquick,step/ +echo -bin=hard -step=qmon -eng_summary=N/A "
        "-standard_flow=YES -module_form_factor={form_factor} "
        "-mfg_workweek={workweek} -n -r +imesh -nonshippable=N/A +debug -top5"
    )

    @classmethod
    def get_command_template(cls, step: str) -> str:
        """Get the appropriate frpt command template for a step.

        Args:
            step: Test step name

        Returns:
            Command template string
        """
        step_upper = step.upper()
        if step_upper == "HMFN":
            return cls.FRPT_COMMAND_TEMPLATE_HMFN
        elif step_upper == "HMB1":
            return cls.FRPT_COMMAND_TEMPLATE_HMB1
        elif step_upper == "QMON":
            return cls.FRPT_COMMAND_TEMPLATE_QMON
        # Fallback to HMFN template
        return cls.FRPT_COMMAND_TEMPLATE_HMFN

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
