"""QOF Visualization using Dash, Plotly, and DuckDB."""

from __future__ import annotations

import textwrap
from typing import Final

import dash
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, State, ctx, dcc, html
from dash.development.base_component import Component

from QOF_visualisation.db_connection import query

# Constants for organization levels and achievement buckets
ORG_TABLE: dict[str, str] = {
    "Practice": "qof_vis.fct__practice_achievement",
    "PCN": "qof_vis.fct__pcn_achievement",
    "Sub-ICB": "qof_vis.fct__sub_icb_achievement",
    "ICB": "qof_vis.fct__icb_achievement",
    "Region": "qof_vis.fct__region_achievement",
}

BUCKET_SQL: dict[str, str] = {
    "< 20 %": "< 20",
    "20-40 %": ">= 20 AND percentage_patients_achieved < 40",
    "40-60 %": ">= 40 AND percentage_patients_achieved < 60",
    "60-80 %": ">= 60 AND percentage_patients_achieved < 80",
    "80-100 %": ">= 80",
}
DEFAULT_BUCKET: Final[str] = "80-100 %"


def md_wrap(text: str | None, width: int = 80) -> str:
    """Wrap text to specified width with line breaks."""
    if not text:
        return ""
    return "  \n".join(textwrap.wrap(text, width))


def make_blank_map() -> go.Figure:
    """Create an empty map figure."""
    fig = go.Figure()
    fig.update_layout(
        mapbox=dict(style="carto-positron", center={"lat": 53, "lon": -1.5}, zoom=5),
        margin=dict(t=0, r=0, l=0, b=0),
        height=700,
    )
    return fig


def make_blank_bar(msg: str = "Click a point") -> go.Figure:
    """Create an empty bar chart figure with message."""
    fig = go.Figure()
    fig.update_layout(
        title=msg,
        xaxis=dict(visible=True, range=[0, 110], title="% Achieved"),
        yaxis=dict(visible=False),
        margin=dict(t=40, r=10, l=10, b=40),
        showlegend=False,
    )
    return fig


# Initialize available years and indicators
pairs: pl.DataFrame = query("""
    SELECT DISTINCT indicator_code, reporting_year 
    FROM qof_vis.fct__practice_achievement 
    WHERE percentage_patients_achieved IS NOT NULL
""")

ALL_YEARS: list[int] = sorted([int(y) for y in pairs["reporting_year"].unique().to_list()])
ALL_INDS: list[str] = sorted([str(i) for i in pairs["indicator_code"].unique().to_list()])

# Cache indicator codes by year at startup
CODES_BY_YEAR: dict[int, list[str]] = {
    int(year): sorted(
        pairs.filter(pl.col("reporting_year") == year)["indicator_code"]
        .cast(str)
        .unique()
        .to_list()
    )
    for year in pairs["reporting_year"].unique().to_list()
}

# Cache national averages by year at startup
nat_df: pl.DataFrame = query("""
    WITH nat_avgs AS (
        SELECT 
            reporting_year,
            group_code,
            AVG(percentage_patients_achieved) as percentage_patients_achieved
        FROM qof_vis.fct__national_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY reporting_year, group_code
    )
    SELECT * FROM nat_avgs
""")

NAT_AVG_BY_YEAR: dict[int, pl.DataFrame] = {
    int(year): nat_df.filter(pl.col("reporting_year") == year).clone()
    for year in nat_df["reporting_year"].unique().to_list()
}

# Initialize Dash app
app = dash.Dash(__name__)

DEFAULT_IND: str | None = ALL_INDS[0] if ALL_INDS else None
DEFAULT_YEAR: int | None = ALL_YEARS[-1] if ALL_YEARS else None

app.layout = html.Div(
    [
        html.H3("QOF Performance Map"),
        html.Div(
            [
                dcc.Dropdown(
                    id="yr",
                    style={
                        "width": 110,
                        "minWidth": 110,
                        "maxWidth": 110,
                        "marginLeft": 8,
                        "marginRight": 8,
                    },
                    value=DEFAULT_YEAR,
                ),
                dcc.Dropdown(
                    id="ind",
                    style={"width": 180, "minWidth": 180, "maxWidth": 180},
                    value=DEFAULT_IND,
                ),
                dcc.RadioItems(
                    id="level",
                    options=[{"label": k, "value": k} for k in ORG_TABLE],
                    value="Practice",
                    inline=True,
                    style={"paddingLeft": 16},
                ),
                dcc.RadioItems(
                    id="bucket",
                    options=[{"label": k, "value": k} for k in BUCKET_SQL],
                    value=DEFAULT_BUCKET,
                    inline=True,
                    style={"paddingLeft": 16},
                ),
            ],
            style={"display": "flex", "gap": 6, "alignItems": "center"},
        ),
        html.Div(id="desc", style={"fontSize": 14, "marginTop": 6}),
        html.Div(
            [
                dcc.Graph(
                    id="map", style={"height": "82vh", "width": "60vw"}, config={"scrollZoom": True}
                ),
                dcc.Graph(id="bars", style={"height": "82vh", "width": "38vw"}),
            ],
            style={"display": "flex", "gap": "1%"},
        ),
    ],
    style={"padding": 12},
)


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
) -> tuple[
    list[dict[str, str]],
    str | None,
    list[dict[str, int]],
    int | None,
    list[dict[str, str | bool]],
    str | None,
]:
    """Sync dropdown options based on selected values."""
    # Filter indicator codes by year
    if yr_val is not None:
        valid_ind = CODES_BY_YEAR.get(int(yr_val), [])
        ind_opts = [{"label": i, "value": i} for i in valid_ind]
        ind_val = ind_val if ind_val in valid_ind else (valid_ind[0] if valid_ind else None)
    else:
        ind_opts = [{"label": i, "value": i} for i in ALL_INDS]
        ind_val = ind_val if ind_val in ALL_INDS else (ALL_INDS[0] if ALL_INDS else None)

    yr_opts = [{"label": y, "value": y} for y in ALL_YEARS]

    # Show all buckets but disable those with no data
    table = ORG_TABLE.get(level_val or "Practice", "qof_vis.fct__practice_achievement")
    bucket_opts = []

    if ind_val is not None and yr_val is not None:
        for label, cond in BUCKET_SQL.items():
            count_df = query(f"""
                SELECT COUNT(*) as n
                FROM {table}
                WHERE indicator_code = '{ind_val}'
                AND reporting_year = {yr_val}
                AND percentage_patients_achieved {cond}
            """)
            has_data = count_df["n"].item() > 0
            bucket_opts.append({"label": label, "value": label, "disabled": not has_data})
    else:
        bucket_opts = [{"label": k, "value": k, "disabled": True} for k in BUCKET_SQL]

    # Set bucket value
    enabled_buckets = [opt for opt in bucket_opts if not opt.get("disabled", False)]
    current_bucket = ctx.states.get("bucket.value") if hasattr(ctx, "states") else None

    if current_bucket and any(
        opt["value"] == current_bucket and not opt.get("disabled", False) for opt in bucket_opts
    ):
        selected_bucket = current_bucket
    elif any(
        opt["value"] == DEFAULT_BUCKET and not opt.get("disabled", False) for opt in bucket_opts
    ):
        selected_bucket = DEFAULT_BUCKET
    elif enabled_buckets:
        selected_bucket = str(enabled_buckets[0]["value"])
    else:
        selected_bucket = None

    return ind_opts, ind_val, yr_opts, yr_val, bucket_opts, selected_bucket


@app.callback(
    Output("map", "figure"),
    Output("desc", "children"),
    Input("ind", "value"),
    Input("yr", "value"),
    Input("level", "value"),
    Input("bucket", "value"),
)
def build_map(
    indic: str | None, yr: int | None, level: str | None, bucket: str | None
) -> tuple[go.Figure, Component | None]:
    """Build the map visualization."""
    if indic is None or yr is None or bucket is None or level is None:
        return make_blank_map(), None

    # Get data from the correct organization level
    table_name = ORG_TABLE.get(level, "qof_vis.fct__practice_achievement")
    q = f"""
        SELECT 
            organisation_name,
            organisation_code,
            percentage_patients_achieved AS pct,
            output_description AS descr,
            lat,
            lng
        FROM {table_name}
        WHERE indicator_code = '{indic}'
        AND reporting_year = {yr}
        AND percentage_patients_achieved {BUCKET_SQL[bucket]}
    """
    df = query(q)
    if df.is_empty():
        return make_blank_map(), None

    # Create the map using scatter_mapbox with updated config
    fig = px.scatter_mapbox(
        df.to_pandas(),
        lat="lat",
        lon="lng",
        hover_name="organisation_name",
        custom_data=["pct", "organisation_name"],
        color_discrete_sequence=["#1f77b4"],
        zoom=6,
        height=700,
    )

    # Configure mapbox separately
    fig.update_layout(
        mapbox=dict(style="carto-positron", center=dict(lat=53, lon=-1.5)),
        margin=dict(t=0, r=0, l=0, b=0),
    )

    # Update marker and hover properties
    fig.update_traces(
        marker=dict(size=7),
        hovertemplate="<b>%{customdata[1]}</b><br>%{customdata[0]:.1f}%<extra></extra>",
        hoverlabel=dict(bgcolor="white"),
    )

    # Add description
    descr_val = df["descr"][0] if df.height > 0 else ""
    return fig, dcc.Markdown(md_wrap(str(descr_val)))


@app.callback(
    Output("bars", "figure"),
    Input("map", "clickData"),
    State("ind", "value"),
    State("yr", "value"),
    State("level", "value"),
)
def build_bars(
    click: dict[str, list[dict[str, list[str | float]]]] | None,
    indic: str | None,
    yr: int | None,
    level: str | None,
) -> go.Figure:
    """Build the bar chart comparison visualization."""
    if not click or indic is None or yr is None or level is None:
        return make_blank_bar()

    # Get organization name from click data
    clicked_point = click["points"][0]
    if not clicked_point.get("customdata") or len(clicked_point["customdata"]) < 2:
        return make_blank_bar("Invalid click data")

    org_name = str(clicked_point["customdata"][1])
    org_name_sql = org_name.replace("'", "''")

    # Get organization-level achievement data
    table_name = ORG_TABLE.get(level or "Practice", "qof_vis.fct__practice_achievement")
    org_df = query(f"""
        WITH group_avgs AS (
            SELECT
                a.group_code,
                a.group_description,
                COUNT(DISTINCT a.indicator_code) as indicators,
                AVG(a.percentage_patients_achieved) as org_achievement
            FROM {table_name} a
            WHERE a.organisation_name = '{org_name_sql}'
            AND a.reporting_year = {yr}
            AND a.percentage_patients_achieved IS NOT NULL
            GROUP BY a.group_code, a.group_description
        )
        SELECT 
            group_description,
            org_achievement
        FROM group_avgs
        WHERE indicators > 0
        ORDER BY group_description
    """)

    # Get national averages for comparison
    nat_sql = f"""
        WITH nat_avgs AS (
            SELECT
                n.group_code,
                n.group_description,
                COUNT(DISTINCT n.indicator_code) as indicators,
                AVG(n.percentage_patients_achieved) as nat_achievement
            FROM qof_vis.fct__national_achievement n
            WHERE n.reporting_year = {yr}
            AND n.percentage_patients_achieved IS NOT NULL
            GROUP BY n.group_code, n.group_description
        )
        SELECT 
            group_code,
            group_description,
            nat_achievement
        FROM nat_avgs
        WHERE indicators > 0
        ORDER BY group_description
    """
    nat_df = query(nat_sql)

    if org_df.is_empty() and nat_df.is_empty():
        return make_blank_bar(f"No data for {org_name} or National Average in {yr}")

    # Combine data using full join and handle missing values
    if not org_df.is_empty() and not nat_df.is_empty():
        combined_df = org_df.join(nat_df, on="group_description", how="full")
    elif not org_df.is_empty():
        combined_df = org_df.with_columns(pl.lit(None).cast(pl.Float64).alias("nat_achievement"))
    else:
        combined_df = nat_df.with_columns(pl.lit(None).cast(pl.Float64).alias("org_achievement"))

    # Fill null values and prepare for plotting
    combined_df = combined_df.with_columns(
        [pl.col("org_achievement").fill_null(0.0), pl.col("nat_achievement").fill_null(0.0)]
    )

    # Transform to long format
    plot_data = {
        "Group": combined_df["group_description"].to_list() * 2,
        "Achievement": combined_df["org_achievement"].to_list()
        + combined_df["nat_achievement"].to_list(),
        "Source": [org_name] * combined_df.height + ["National Average"] * combined_df.height,
    }
    plot_df = pl.DataFrame(plot_data)

    # Create bar chart with fixed height title area
    fig = px.bar(
        plot_df.to_pandas(),
        x="Achievement",
        y="Group",
        color="Source",
        barmode="group",
        orientation="h",
        height=max(400, len(combined_df["group_description"].unique()) * 40 + 150),
        labels={"Achievement": "% Achieved", "Group": "Indicator Group", "Source": "Comparison"},
        color_discrete_map={org_name: "#1f77b4", "National Average": "#ff7f0e"},
    )

    # Add title as annotation for stable positioning
    fig.add_annotation(
        text=f"{org_name} vs National Average — {yr}",
        xref="paper",
        yref="paper",
        x=0.5,
        y=1.1,
        showarrow=False,
        font=dict(size=16),
        xanchor="center",
    )

    # Highlight current indicator group if applicable
    if indic:
        group_df = query(f"""
            SELECT DISTINCT group_description
            FROM {table_name}
            WHERE indicator_code = '{indic}'
            AND reporting_year = {yr}
            LIMIT 1
        """)
        if not group_df.is_empty():
            current_group = group_df["group_description"][0]
            if current_group in plot_df["Group"].unique():
                fig.add_hrect(
                    y0=current_group,
                    y1=current_group,
                    fillcolor="rgba(0,0,0,0.05)",
                    line_width=0,
                    layer="below",
                )

    # Update layout with improved scaling and formatting
    fig.update_layout(
        xaxis=dict(
            title="% Achieved",
            range=[0, 110],
            tickmode="linear",
            dtick=20,
            showgrid=True,
            gridcolor="rgba(0,0,0,0.1)",
            zeroline=True,
            zerolinecolor="black",
            zerolinewidth=1,
        ),
        yaxis=dict(title="", categoryorder="total ascending", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=50, r=20, l=20, b=40),
        hoverlabel=dict(bgcolor="white"),
        bargap=0.2,
        bargroupgap=0.1,
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    # Add reference lines
    for x in range(20, 101, 20):
        fig.add_vline(x=x, line_dash="dot", line_color="rgba(0,0,0,0.2)")

    # Update hover template to show exact values
    fig.update_traces(hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra>%{fullData.name}</extra>")

    return fig


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
