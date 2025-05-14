"""QOF Visualization using Dash, Plotly, and DuckDB."""

from __future__ import annotations

import textwrap
from typing import Final

import dash
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, State, ctx, dcc, html
from dash.development.base_component import Component

from QOF_visualisation.visualization.db_connection import query

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
                html.Div(
                    dcc.Graph(
                        id="bars",
                        style={"width": "100%", "height": "100%"},
                        config={"scrollZoom": False},
                    ),
                    style={
                        "width": "38vw",
                        "height": "82vh",
                        "overflowY": "auto",
                        "marginTop": "0vh",  # Remove the negative margin
                    },
                ),
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

    # Create the map using Scattermap
    fig = go.Figure(
        data=[
            go.Scattermap(
                lat=df["lat"].to_list(),
                lon=df["lng"].to_list(),
                mode="markers",
                marker=dict(
                    size=6,
                    color="#1f77b4",
                    opacity=0.95,  # More opaque
                ),
                hoverinfo="text",
                text=[
                    f"<b>{name}</b><br>{pct:.1f}%"
                    for name, pct in zip(
                        df["organisation_name"].to_list(), df["pct"].to_list(), strict=True
                    )
                ],
                customdata=list(
                    zip(df["pct"].to_list(), df["organisation_name"].to_list(), strict=True)
                ),
            )
        ]
    )

    # Configure map with proper zoom and bounds
    fig.update_layout(
        margin=dict(t=0, r=0, l=0, b=0),
        height=700,
        uirevision="constant",  # Maintain zoom/pan state
        mapbox=dict(
            style="carto-positron",
            zoom=6.0,  # Try different zoom level
            center=dict(lat=54.5, lon=-2),
        ),
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
    """Build the bar chart comparing organization achievement to national average.

    Args:
        click: Click event data from map, containing organization info
        indic: Selected indicator code
        yr: Selected reporting year
        level: Selected organization level (Practice, PCN, etc.)

    Returns:
        A plotly Figure object containing the bar chart
    """
    if not click or indic is None or yr is None or level is None:
        return make_blank_bar()

    # Get organization name from click data
    clicked_point = click["points"][0]
    if not clicked_point.get("customdata") or len(clicked_point["customdata"]) < 2:
        return make_blank_bar("Invalid click data")

    org_name = str(clicked_point["customdata"][1])
    org_name_sql = org_name.replace("'", "''")  # Escape single quotes for SQL

    # Get organization-level achievement data
    table_name = ORG_TABLE.get(level or "Practice", "qof_vis.fct__practice_achievement")
    org_sql = f"""
        WITH group_avgs AS (
            SELECT
                a.group_code,
                a.group_description,
                COUNT(DISTINCT a.indicator_code) as indicators,
                AVG(CASE WHEN a.percentage_patients_achieved IS NOT NULL 
                         THEN a.percentage_patients_achieved 
                         ELSE NULL END) as achievement
            FROM {table_name} a
            WHERE a.organisation_name = '{org_name_sql}'
            AND a.reporting_year = {yr}
            GROUP BY a.group_code, a.group_description
            HAVING COUNT(DISTINCT a.indicator_code) > 0
        )
        SELECT 
            group_description,
            achievement
        FROM group_avgs
        ORDER BY group_description DESC
    """
    org_df = query(org_sql)

    # Get national averages for comparison
    nat_sql = f"""
        WITH nat_avgs AS (
            SELECT
                n.group_code,
                n.group_description,
                COUNT(DISTINCT n.indicator_code) as indicators,
                AVG(CASE WHEN n.percentage_patients_achieved IS NOT NULL 
                         THEN n.percentage_patients_achieved 
                         ELSE NULL END) as achievement
            FROM qof_vis.fct__national_achievement n
            WHERE n.reporting_year = {yr}
            GROUP BY n.group_code, n.group_description
            HAVING COUNT(DISTINCT n.indicator_code) > 0
        )
        SELECT 
            group_description,
            achievement
        FROM nat_avgs
        ORDER BY group_description DESC
    """
    nat_df = query(nat_sql)

    if org_df.is_empty() and nat_df.is_empty():
        return make_blank_bar(f"No data for {org_name} or National Average in {yr}")

    # Get sorted group descriptions for consistent ordering
    groups = sorted(
        set(
            g
            for g in org_df["group_description"].to_list() + nat_df["group_description"].to_list()
            if g is not None
        ),
        reverse=True,
    )

    # Create plot data with achievement values
    plot_data: dict[str, list[str | float | None]] = {
        "Group": groups * 2,  # Each group appears twice, once for org and once for national
        "Achievement": [],  # Will contain achievement values
        "Source": [org_name] * len(groups)
        + ["National Average"] * len(groups),  # Data source labels
    }

    # Add achievement values, handling missing data
    for g in groups:
        # Organization achievement
        org_row = org_df.filter(pl.col("group_description") == g)
        plot_data["Achievement"].append(
            org_row["achievement"].item() if not org_row.is_empty() else None
        )

    for g in groups:
        # National achievement
        nat_row = nat_df.filter(pl.col("group_description") == g)
        plot_data["Achievement"].append(
            nat_row["achievement"].item() if not nat_row.is_empty() else None
        )

    plot_df = pl.DataFrame(plot_data)

    # Create horizontal bar chart
    bar_fig = go.Figure()

    # Add organization data
    org_data = plot_df.filter(pl.col("Source") == org_name)
    if not org_data.is_empty():
        bar_fig.add_trace(
            go.Bar(
                x=org_data["Achievement"].to_list(),
                y=org_data["Group"].to_list(),
                orientation="h",
                name=org_name,
                marker_color="#1f77b4",
                hovertemplate="%{y}<br>%{x:.1f}%<extra></extra>",
                hoverlabel=dict(bgcolor="white", font=dict(size=12), bordercolor="#1f77b4"),
            )
        )

    # Add national average data
    nat_data = plot_df.filter(pl.col("Source") == "National Average")
    if not nat_data.is_empty():
        bar_fig.add_trace(
            go.Bar(
                x=nat_data["Achievement"].to_list(),
                y=nat_data["Group"].to_list(),
                orientation="h",
                name="National Average",
                marker_color="#ff7f0e",
                hovertemplate="%{y}<br>%{x:.1f}%<extra></extra>",
                hoverlabel=dict(bgcolor="white", font=dict(size=12), bordercolor="#ff7f0e"),
            )
        )

    # Update layout
    bar_fig.update_layout(
        barmode="group",
        bargroupgap=0.3,
        bargap=0.15,
        margin=dict(l=200, r=50, t=100, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=800,
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(0,0,0,0.1)",
            title=dict(text="Achievement (%)", font=dict(size=12), standoff=10),
            range=[0, 100],
            zeroline=True,
            zerolinecolor="rgba(0,0,0,0.2)",
            zerolinewidth=1,
        ),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.1)", tickfont=dict(size=11)),
        hoverdistance=100,
        hovermode="closest",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="rgba(0,0,0,0.1)",
            borderwidth=1,
        ),
        title=dict(
            text=f"Performance Comparison - {org_name}",
            x=0.5,
            xanchor="center",
            y=0.95,
            font=dict(size=14),
        ),
    )

    return bar_fig


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
