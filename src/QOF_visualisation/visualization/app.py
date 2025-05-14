"""Main application entry point for QOF visualization.

This module defines the main Dash application for visualizing QOF (Quality and Outcomes Framework)
performance data. It includes:

1. Dashboard layout creation and initialization
2. Interactive callbacks for synchronizing dropdowns
3. Visualization updates based on user selections
4. Data filtering and processing logic

The module uses a modular architecture with components organized into:
- Layout components (header, controls, visualizations)
- State management (dropdown synchronization, filter logic)
- Data queries (database interaction)
- Visualization utilities (map and chart creation)
- Helper utilities (text formatting)

Typical usage:
    app = dash.Dash(__name__)
    app.layout = create_app_layout(DEFAULT_YEAR, DEFAULT_IND)
    app.run_server()
"""

import dash
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, State, ctx, dcc

# Import application components
from QOF_visualisation.visualization.constants import BUCKET_SQL, DEFAULT_BUCKET, ORG_TABLE
from QOF_visualisation.visualization.data_queries import (
    check_bucket_has_data,
    get_achievement_by_org_level,
    get_available_indicators,
    get_indicators_by_year,
    get_national_achievement_data,
    get_org_achievement_data,
)
from QOF_visualisation.visualization.layout_components import create_app_layout
from QOF_visualisation.visualization.state_management import select_bucket_value
from QOF_visualisation.visualization.text_utils import md_wrap
from QOF_visualisation.visualization.visualization_utils import (
    create_bar_chart,
    create_blank_bar,
    create_blank_map,
    create_map,
)
from QOF_visualisation.visualization.db_connection import query

# Type definitions for component options
DropdownOption = dict[str, str | int | bool | None]
ClickPoint = dict[str, list[str | float] | list[dict[str, str | float]]]

# Import type from state_management
from QOF_visualisation.visualization.state_management import BucketOption

# Type aliases for collections
DropdownOptions = list[DropdownOption]
BucketOptions = list[BucketOption]
ClickData = dict[str, list[ClickPoint]]

# Initialize application
app = dash.Dash(__name__)

# Get initial data
ALL_YEARS, ALL_INDS = get_available_indicators()

# Set default selections
DEFAULT_IND = ALL_INDS[0] if ALL_INDS else None
DEFAULT_YEAR = ALL_YEARS[-1] if ALL_YEARS else None

# Configure application layout
app.layout = create_app_layout(DEFAULT_YEAR, DEFAULT_IND)


@app.callback(
    Output("ind", "options"),
    Output("ind", "value"),
    Output("yr", "options"),
    Output("yr", "value"),
    Output("bucket", "options"),
    Output("bucket", "value"),
    Input("ind", "value"),
    Input("yr", "value"),
    Input("level", "value"),
)
def sync_dropdowns(
    ind_val: str | None, yr_val: int | None, level_val: str | None
) -> tuple[DropdownOptions, str | None, DropdownOptions, int | None, BucketOptions, str | None]:
    """Synchronize dropdown options based on selected values.

    Args:
        ind_val: Currently selected indicator code
        yr_val: Currently selected year
        level_val: Currently selected organization level

    Returns:
        A tuple containing:
        - Indicator dropdown options and selected value
        - Year dropdown options and selected value
        - Bucket dropdown options and selected value
    """

    def make_dropdown_opt(value: str | int) -> DropdownOption:
        """Create a dropdown option from a value."""
        return {"label": str(value), "value": value, "disabled": None}

    # Get indicator options based on year selection
    if yr_val is not None:
        valid_ind = get_indicators_by_year(yr_val)
        ind_opts = list(map(make_dropdown_opt, valid_ind))
        ind_val = ind_val if ind_val in valid_ind else (valid_ind[0] if valid_ind else None)
    else:
        ind_opts = list(map(make_dropdown_opt, ALL_INDS))
        ind_val = ind_val if ind_val in ALL_INDS else (ALL_INDS[0] if ALL_INDS else None)

    yr_opts = list(map(make_dropdown_opt, ALL_YEARS))

    # Generate year options
    yr_opts = list(map(make_dropdown_opt, ALL_YEARS))

    # Configure bucket options
    table = ORG_TABLE.get(level_val or "Practice", "qof_vis.fct__practice_achievement")
    bucket_opts: list[BucketOption] = []

    if ind_val is not None and yr_val is not None:
        for label, cond in BUCKET_SQL.items():
            has_data = check_bucket_has_data(table, ind_val, yr_val, cond)
            bucket_opts.append(BucketOption(label=label, value=label, disabled=not has_data))
    else:
        bucket_opts = [BucketOption(label=k, value=k, disabled=True) for k in BUCKET_SQL]

    # Select appropriate bucket
    selected_bucket = select_bucket_value(
        bucket_opts,
        ctx.states.get("bucket.value") if hasattr(ctx, "states") else None,
        DEFAULT_BUCKET,
    )

    return ind_opts, ind_val, yr_opts, yr_val, bucket_opts, selected_bucket


@app.callback(
    Output("map", "figure"),
    Output("desc", "children"),
    Input("ind", "value"),
    Input("yr", "value"),
    Input("level", "value"),
    Input("bucket", "value"),
)
def update_map(
    indic: str | None, yr: int | None, level: str | None, bucket: str | None
) -> tuple[go.Figure, dcc.Markdown | None]:
    """Update map visualization based on selected filters.

    Args:
        indic: Selected indicator code
        yr: Selected year
        level: Selected organization level
        bucket: Selected achievement bucket

    Returns:
        A tuple containing:
        - The map figure
        - The indicator description as markdown, or None if no data
    """
    if indic is None or yr is None or bucket is None or level is None:
        return create_blank_map(), None

    # Get data for selected filters
    table_name = ORG_TABLE.get(level, "qof_vis.fct__practice_achievement")
    df = get_achievement_by_org_level(table_name, indic, yr, BUCKET_SQL[bucket])

    if df.is_empty():
        return create_blank_map(), None

    # Create visualization
    fig = create_map(df)
    descr_val = df["descr"][0] if df.height > 0 else ""

    return fig, dcc.Markdown(md_wrap(str(descr_val)))


@app.callback(
    Output("bars", "figure"),
    Input("map", "clickData"),
    State("ind", "value"),
    State("yr", "value"),
    State("level", "value"),
)
def update_bars(
    click: ClickData | None, indic: str | None, yr: int | None, level: str | None
) -> go.Figure:
    """Update bar chart based on selected organization."""
    if not click or indic is None or yr is None or level is None:
        return create_blank_bar()

    # Get clicked organization details
    clicked_point = click["points"][0]
    if not clicked_point.get("customdata") or len(clicked_point["customdata"]) < 2:
        return create_blank_bar("Invalid click data")

    org_name = str(clicked_point["customdata"][1])
    org_name_sql = org_name.replace("'", "''")  # Escape single quotes for SQL

    # Query organization achievement data
    org_sql = f"""
        SELECT 
            group_description,
            avg_achievement as achievement,
            level,
            organisation_name
        FROM qof_vis.fct__long_organisation_achievement
        WHERE organisation_name = '{org_name_sql}'
        AND reporting_year = {yr}
        GROUP BY level, group_description, avg_achievement, organisation_name
        ORDER BY group_description
    """
    org_df = query(org_sql)

    # Query national achievement data
    nat_sql = f"""
        SELECT 
            group_description,
            avg_achievement as achievement,
            level,
            organisation_name
        FROM qof_vis.fct__long_organisation_achievement
        WHERE level = 'National'
        AND reporting_year = {yr}
        GROUP BY level, group_description, avg_achievement, organisation_name
        ORDER BY group_description
    """
    nat_df = query(nat_sql)

    if org_df.is_empty() and nat_df.is_empty():
        return create_blank_bar(f"No data for {org_name} or National Average in {yr}")

    # Prepare and return the visualization
    return create_bar_chart(prepare_comparison_data(org_df, nat_df), org_name)


def prepare_comparison_data(org_df: pl.DataFrame, nat_df: pl.DataFrame) -> pl.DataFrame:
    """Prepare data for organization vs national comparison.

    Args:
        org_df: Organization achievement data from fct__long_organisation_achievement
        nat_df: National achievement data from fct__long_organisation_achievement

    Returns:
        Combined DataFrame with organization and national achievement data
        in the format required by create_bar_chart:
        - Group: Indicator group name
        - Achievement: Achievement percentage
        - Source: Organization name or "National Average"
    """
    # Get sorted groups
    all_groups = sorted(
        set(
            org_df["group_description"].unique().to_list()
            + nat_df["group_description"].unique().to_list()
        ),
        reverse=True,
    )

    # Create plot data structure
    rows: list[dict[str, str | float | None]] = []

    # Add organization data
    for group in all_groups:
        org_rows = org_df.filter(pl.col("group_description") == group)
        if not org_rows.is_empty():
            rows.append(
                {
                    "Group": group,
                    "Achievement": org_rows["achievement"][0],
                    "Source": org_rows["organisation_name"][0],
                }
            )

    # Add national average data
    for group in all_groups:
        nat_rows = nat_df.filter(pl.col("group_description") == group)
        if not nat_rows.is_empty():
            rows.append(
                {
                    "Group": group,
                    "Achievement": nat_rows["achievement"][0],
                    "Source": "National Average",
                }
            )

    return pl.DataFrame(rows)


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
