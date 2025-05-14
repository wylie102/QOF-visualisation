"""Constants and configuration for QOF visualization.

This module defines constants used throughout the QOF visualization dashboard.
It includes configuration for:
    - Organization level tables
    - Achievement bucket definitions
    - Default map settings
    - Chart colors

The constants are organized by category and use type hints for clarity.
"""

from typing import Final, TypeAlias

# Type aliases for improved readability
TableName: TypeAlias = str
SQLCondition: TypeAlias = str
Color: TypeAlias = str

# Organization level tables - mapping from display names to database table names
ORG_TABLE: Final[dict[str, TableName]] = {
    "Practice": "qof_vis.fct__practice_achievement",
    "PCN": "qof_vis.fct__pcn_achievement",
    "Sub-ICB": "qof_vis.fct__sub_icb_achievement",
    "ICB": "qof_vis.fct__icb_achievement",
    "Region": "qof_vis.fct__region_achievement",
}

# Achievement bucket definitions - mapping from display labels to SQL conditions
BUCKET_SQL: Final[dict[str, SQLCondition]] = {
    "< 20 %": "< 20",
    "20-40 %": ">= 20 AND percentage_patients_achieved < 40",
    "40-60 %": ">= 40 AND percentage_patients_achieved < 60",
    "60-80 %": ">= 60 AND percentage_patients_achieved < 80",
    "80-100 %": ">= 80",
}
DEFAULT_BUCKET: Final[str] = "80-100 %"

# Map settings
DEFAULT_MAP_CENTER: tuple[float, float] = (54.5, -2)
DEFAULT_MAP_ZOOM: float = 6.0

# Chart colors - default colors for organization data and national average
PRIMARY_COLOR: Final[Color] = "#1f77b4"  # Blue for organization data
SECONDARY_COLOR: Final[Color] = "#ff7f0e"  # Orange for national average
