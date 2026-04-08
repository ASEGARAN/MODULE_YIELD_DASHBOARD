"""Micron Fiscal Calendar utilities.

Provides mapping between mfg_workweek and fiscal months/quarters.
Micron fiscal year starts in September and ends in August.
"""

from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

# FY26 Workweek to Fiscal Month mapping
# Format: YYYYWW -> (fiscal_month, fiscal_quarter, fiscal_year)
# Fiscal months: Sep=1, Oct=2, Nov=3, Dec=4, Jan=5, Feb=6, Mar=7, Apr=8, May=9, Jun=10, Jul=11, Aug=12

# Workweek to calendar month mapping for 2025-2027
# Based on FRIDAY of each week (Micron workweek starts on Friday)
WW_TO_MONTH: Dict[str, str] = {
    # 2025 (FY26 Q1 starts) - Friday-based
    "202535": "Aug'25", "202536": "Sep'25", "202537": "Sep'25", "202538": "Sep'25",
    "202539": "Sep'25", "202540": "Oct'25", "202541": "Oct'25", "202542": "Oct'25",
    "202543": "Oct'25", "202544": "Oct'25", "202545": "Nov'25", "202546": "Nov'25",
    "202547": "Nov'25", "202548": "Nov'25", "202549": "Dec'25", "202550": "Dec'25",
    "202551": "Dec'25", "202552": "Dec'25",
    # 2026 - Based on FRIDAY start of each workweek
    "202601": "Jan'26", "202602": "Jan'26", "202603": "Jan'26", "202604": "Jan'26", "202605": "Jan'26",  # Fri Jan 2-30
    "202606": "Feb'26", "202607": "Feb'26", "202608": "Feb'26", "202609": "Feb'26",  # Fri Feb 6-27
    "202610": "Mar'26", "202611": "Mar'26", "202612": "Mar'26", "202613": "Mar'26",  # Fri Mar 6-27
    "202614": "Apr'26", "202615": "Apr'26", "202616": "Apr'26", "202617": "Apr'26",  # Fri Apr 3-24
    "202618": "May'26", "202619": "May'26", "202620": "May'26", "202621": "May'26", "202622": "May'26",  # Fri May 1-29
    "202623": "Jun'26", "202624": "Jun'26", "202625": "Jun'26", "202626": "Jun'26",  # Fri Jun 5-26
    "202627": "Jul'26", "202628": "Jul'26", "202629": "Jul'26", "202630": "Jul'26", "202631": "Jul'26",  # Fri Jul 3-31
    "202632": "Aug'26", "202633": "Aug'26", "202634": "Aug'26", "202635": "Aug'26",  # Fri Aug 7-28
    "202636": "Sep'26", "202637": "Sep'26", "202638": "Sep'26", "202639": "Sep'26",  # Fri Sep 4-25
    "202640": "Oct'26", "202641": "Oct'26", "202642": "Oct'26", "202643": "Oct'26", "202644": "Oct'26",  # Fri Oct 2-30
    "202645": "Nov'26", "202646": "Nov'26", "202647": "Nov'26", "202648": "Nov'26",  # Fri Nov 6-27
    "202649": "Dec'26", "202650": "Dec'26", "202651": "Dec'26", "202652": "Dec'26",  # Fri Dec 4-25
    # 2027 - Based on FRIDAY start of each workweek
    "202701": "Jan'27", "202702": "Jan'27", "202703": "Jan'27", "202704": "Jan'27", "202705": "Jan'27",  # Fri Jan 1-29
    "202706": "Feb'27", "202707": "Feb'27", "202708": "Feb'27", "202709": "Feb'27",  # Fri Feb 5-26
    "202710": "Mar'27", "202711": "Mar'27", "202712": "Mar'27", "202713": "Mar'27",  # Fri Mar 5-26
    "202714": "Apr'27", "202715": "Apr'27", "202716": "Apr'27", "202717": "Apr'27",  # Fri Apr 2-23
    "202718": "Apr'27", "202719": "May'27", "202720": "May'27", "202721": "May'27",  # Fri Apr 30, May 7-21
    "202722": "May'27",  # Fri May 28
}

# Workweek to Fiscal Quarter mapping
WW_TO_FISCAL_QUARTER: Dict[str, str] = {
    # FY26 Q1 (Sep-Nov 2025)
    "202535": "FY26Q1", "202536": "FY26Q1", "202537": "FY26Q1", "202538": "FY26Q1",
    "202539": "FY26Q1", "202540": "FY26Q1", "202541": "FY26Q1", "202542": "FY26Q1",
    "202543": "FY26Q1", "202544": "FY26Q1", "202545": "FY26Q1", "202546": "FY26Q1",
    "202547": "FY26Q1",
    # FY26 Q2 (Dec 2025 - Feb 2026)
    "202548": "FY26Q2", "202549": "FY26Q2", "202550": "FY26Q2", "202551": "FY26Q2",
    "202552": "FY26Q2", "202601": "FY26Q2", "202602": "FY26Q2", "202603": "FY26Q2",
    "202604": "FY26Q2", "202605": "FY26Q2", "202606": "FY26Q2", "202607": "FY26Q2",
    "202608": "FY26Q2",
    # FY26 Q3 (Mar-May 2026)
    "202609": "FY26Q3", "202610": "FY26Q3", "202611": "FY26Q3", "202612": "FY26Q3",
    "202613": "FY26Q3", "202614": "FY26Q3", "202615": "FY26Q3", "202616": "FY26Q3",
    "202617": "FY26Q3", "202618": "FY26Q3", "202619": "FY26Q3", "202620": "FY26Q3",
    "202621": "FY26Q3",
    # FY26 Q4 (Jun-Aug 2026)
    "202622": "FY26Q4", "202623": "FY26Q4", "202624": "FY26Q4", "202625": "FY26Q4",
    "202626": "FY26Q4", "202627": "FY26Q4", "202628": "FY26Q4", "202629": "FY26Q4",
    "202630": "FY26Q4", "202631": "FY26Q4", "202632": "FY26Q4", "202633": "FY26Q4",
    "202634": "FY26Q4", "202635": "FY26Q4",
    # FY27 Q1 (Sep-Nov 2026)
    "202636": "FY27Q1", "202637": "FY27Q1", "202638": "FY27Q1", "202639": "FY27Q1",
    "202640": "FY27Q1", "202641": "FY27Q1", "202642": "FY27Q1", "202643": "FY27Q1",
    "202644": "FY27Q1", "202645": "FY27Q1", "202646": "FY27Q1", "202647": "FY27Q1",
    "202648": "FY27Q1",
    # FY27 Q2 (Dec 2026 - Feb 2027)
    "202649": "FY27Q2", "202650": "FY27Q2", "202651": "FY27Q2", "202652": "FY27Q2",
    "202701": "FY27Q2", "202702": "FY27Q2", "202703": "FY27Q2", "202704": "FY27Q2",
    "202705": "FY27Q2", "202706": "FY27Q2", "202707": "FY27Q2", "202708": "FY27Q2",
    "202709": "FY27Q2",
    # FY27 Q3 (Mar-May 2027)
    "202710": "FY27Q3", "202711": "FY27Q3", "202712": "FY27Q3", "202713": "FY27Q3",
    "202714": "FY27Q3", "202715": "FY27Q3", "202716": "FY27Q3", "202717": "FY27Q3",
    "202718": "FY27Q3", "202719": "FY27Q3", "202720": "FY27Q3", "202721": "FY27Q3",
    "202722": "FY27Q3",
}


def get_fiscal_month(ww: str) -> str:
    """Get fiscal month label for a workweek.

    Args:
        ww: Workweek in YYYYWW format (e.g., '202614')

    Returns:
        Month label (e.g., "Apr'26")
    """
    ww_str = str(ww).replace("WW", "")
    if ww_str in WW_TO_MONTH:
        return WW_TO_MONTH[ww_str]

    # Fallback: calculate from workweek
    return _calculate_fiscal_month(ww_str)


def get_fiscal_quarter(ww: str) -> str:
    """Get fiscal quarter for a workweek.

    Args:
        ww: Workweek in YYYYWW format (e.g., '202614')

    Returns:
        Fiscal quarter (e.g., "FY26Q3")
    """
    ww_str = str(ww).replace("WW", "")
    if ww_str in WW_TO_FISCAL_QUARTER:
        return WW_TO_FISCAL_QUARTER[ww_str]

    # Fallback: calculate from workweek
    return _calculate_fiscal_quarter(ww_str)


def _calculate_fiscal_month(ww: str) -> str:
    """Calculate fiscal month from workweek using ISO week date logic.

    This is a fallback for workweeks not in the static mapping.
    """
    try:
        year = int(ww[:4])
        week = int(ww[4:])

        # Get the Monday of the ISO week
        jan4 = datetime(year, 1, 4)
        start_of_week1 = jan4 - timedelta(days=jan4.isoweekday() - 1)
        monday = start_of_week1 + timedelta(weeks=week - 1)

        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }

        return f"{month_names[monday.month]}'{str(monday.year)[2:]}"
    except (ValueError, KeyError):
        return ww


def _calculate_fiscal_quarter(ww: str) -> str:
    """Calculate fiscal quarter from workweek.

    Micron fiscal quarters:
    - Q1: Sep-Nov (WW35-47 approximately)
    - Q2: Dec-Feb (WW48-08 approximately)
    - Q3: Mar-May (WW09-21 approximately)
    - Q4: Jun-Aug (WW22-35 approximately)
    """
    try:
        year = int(ww[:4])
        week = int(ww[4:])

        # Determine fiscal year and quarter based on workweek
        if 35 <= week <= 47:
            # Q1: Sep-Nov (same calendar year)
            fy = year + 1
            fq = 1
        elif week >= 48 or week <= 8:
            # Q2: Dec-Feb (crosses calendar year boundary)
            fy = year + 1 if week >= 48 else year
            fq = 2
        elif 9 <= week <= 21:
            # Q3: Mar-May
            fy = year
            fq = 3
        else:  # 22 <= week <= 34
            # Q4: Jun-Aug
            fy = year
            fq = 4

        return f"FY{fy % 100}Q{fq}"
    except ValueError:
        return "Unknown"


def get_workweek_label(ww: str, include_month: bool = True) -> str:
    """Get formatted label for a workweek.

    Args:
        ww: Workweek in YYYYWW format
        include_month: Whether to include month below workweek

    Returns:
        Formatted label (e.g., "202614" or "202614<br><b>Apr'26</b>")
    """
    ww_str = str(ww).replace("WW", "")

    if include_month:
        month = get_fiscal_month(ww_str)
        return f"{ww_str}<br><b>{month}</b>"

    return ww_str


def get_workweek_labels_with_months(workweeks: list) -> list:
    """Get labels for workweeks with months shown only at first occurrence.

    Args:
        workweeks: List of workweeks in YYYYWW format

    Returns:
        List of formatted labels with months at boundaries
    """
    labels = []
    prev_month = None

    for ww in workweeks:
        ww_str = str(ww).replace("WW", "")
        month = get_fiscal_month(ww_str)

        if month != prev_month:
            labels.append(f"{ww_str}<br><b>{month}</b>")
            prev_month = month
        else:
            labels.append(ww_str)

    return labels


def get_calendar_year_month(ww: str) -> Tuple[int, int]:
    """Get calendar year and month for a workweek.

    Parses the fiscal month label to extract (year, month) tuple.

    Args:
        ww: Workweek in YYYYWW format (e.g., '202614')

    Returns:
        Tuple of (year, month) - e.g., (2026, 4) for April 2026
    """
    month_label = get_fiscal_month(ww)

    # Parse month label format: "Apr'26" -> (2026, 4)
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }

    try:
        month_name = month_label[:3]
        year_suffix = month_label[4:6]  # After the apostrophe
        month = month_map.get(month_name, 1)
        year = 2000 + int(year_suffix)
        return (year, month)
    except (ValueError, IndexError):
        # Fallback: use ISO week calculation
        year = int(ww[:4])
        week = int(ww[4:])
        jan4 = datetime(year, 1, 4)
        start_of_week1 = jan4 - timedelta(days=jan4.isoweekday() - 1)
        monday = start_of_week1 + timedelta(weeks=week - 1)
        return (monday.year, monday.month)
