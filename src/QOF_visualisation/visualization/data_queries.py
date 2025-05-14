"""Database queries and data transformations for QOF visualization.

This module provides functions for querying achievement data and indicators from
the QOF database at different organizational levels (practice, PCN, ICB, etc.).

Typical usage example:
    years, indicators = get_available_indicators()
    data = get_achievement_by_org_level(
        level="qof_vis.fct__practice_achievement",
        indic="BP002",
        yr=2024,
        bucket_condition=">= 80"
    )
"""


import polars as pl

from QOF_visualisation.visualization.db_connection import query


def get_achievement_by_org_level(
    level: str,
    indic: str,
    yr: int,
    bucket_condition: str,
) -> pl.DataFrame:
    """Get achievement data for a specific organization level.

    Args:
        level: The organization level table name (e.g., 'qof_vis.fct__practice_achievement')
        indic: The QOF indicator code
        yr: The reporting year
        bucket_condition: SQL condition defining the achievement bucket (e.g., '>= 80')

    Returns:
        DataFrame containing organization name, code, achievement percentage,
        description, and geographic coordinates.
    """
    q = f"""
        SELECT 
            organisation_name,
            organisation_code,
            percentage_patients_achieved AS pct,
            output_description AS descr,
            lat,
            lng
        FROM {level}
        WHERE indicator_code = '{indic}'
        AND reporting_year = {yr}
        AND percentage_patients_achieved {bucket_condition}
    """
    return query(q)


def get_org_achievement_data(
    table_name: str,
    org_name: str,
    yr: int,
) -> pl.DataFrame:
    """Get organization achievement data grouped by indicator groups.

    Args:
        table_name: The organization level table name
        org_name: The name of the organization to get data for
        yr: The reporting year

    Returns:
        DataFrame containing group descriptions and achievement percentages
        for the specified organization.
    """
    q = f"""
        WITH group_avgs AS (
            SELECT
                a.group_code,
                a.group_description,
                COUNT(DISTINCT a.indicator_code) as indicators,
                AVG(CASE WHEN a.percentage_patients_achieved IS NOT NULL 
                         THEN a.percentage_patients_achieved 
                         ELSE NULL END) as org_achievement
            FROM {table_name} a
            WHERE a.organisation_name = '{org_name}'
            AND a.reporting_year = {yr}
            GROUP BY a.group_code, a.group_description
            HAVING COUNT(DISTINCT a.indicator_code) > 0
        )
        SELECT 
            group_description,
            org_achievement
        FROM group_avgs
        ORDER BY group_description DESC
    """
    return query(q)


def get_national_achievement_data(yr: int) -> pl.DataFrame:
    """Get national achievement averages by indicator groups.

    Args:
        yr: The reporting year

    Returns:
        DataFrame containing group codes, descriptions, and national achievement
        percentages averaged across all organizations.
    """
    nat_sql = f"""
        WITH nat_avgs AS (
            SELECT
                n.group_code,
                n.group_description,
                COUNT(DISTINCT n.indicator_code) as indicators,
                AVG(CASE WHEN n.percentage_patients_achieved IS NOT NULL 
                         THEN n.percentage_patients_achieved 
                         ELSE NULL END) as nat_achievement
            FROM qof_vis.fct__national_achievement n
            WHERE n.reporting_year = {yr}
            GROUP BY n.group_code, n.group_description
            HAVING COUNT(DISTINCT n.indicator_code) > 0
        )
        SELECT 
            group_code,
            group_description,
            nat_achievement
        FROM nat_avgs
        ORDER BY group_description DESC
    """
    return query(nat_sql)


def get_available_indicators() -> tuple[list[int], list[str]]:
    """Get all available years and indicator codes.

    Returns:
        A tuple containing:
            - A list of available reporting years
            - A list of available indicator codes
        Both lists are sorted in ascending order.
    """
    pairs = query("""
        SELECT DISTINCT indicator_code, reporting_year 
        FROM qof_vis.fct__practice_achievement 
        WHERE percentage_patients_achieved IS NOT NULL
    """)

    years = sorted([int(y) for y in pairs["reporting_year"].unique().to_list()])
    indicators = sorted([str(i) for i in pairs["indicator_code"].unique().to_list()])

    return years, indicators


def get_indicators_by_year(yr: int) -> list[str]:
    """Get available indicators for a specific year.

    Args:
        yr: The reporting year to get indicators for

    Returns:
        A sorted list of indicator codes available for the specified year.
    """
    pairs = query(f"""
        SELECT DISTINCT indicator_code
        FROM qof_vis.fct__practice_achievement
        WHERE reporting_year = {yr}
        AND percentage_patients_achieved IS NOT NULL
    """)
    return sorted([str(i) for i in pairs["indicator_code"].unique().to_list()])


def check_bucket_has_data(
    table: str,
    indic: str,
    yr: int,
    condition: str,
) -> bool:
    """Check if an achievement bucket has any data.

    Args:
        table: The organization level table name
        indic: The QOF indicator code
        yr: The reporting year
        condition: SQL condition defining the achievement bucket

    Returns:
        True if the specified bucket contains data points, False otherwise.
    """
    count_df = query(f"""
        SELECT COUNT(*) as n
        FROM {table}
        WHERE indicator_code = '{indic}'
        AND reporting_year = {yr}
        AND percentage_patients_achieved {condition}
    """)
    return count_df["n"].item() > 0
