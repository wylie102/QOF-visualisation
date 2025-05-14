# QOF visualisation – Dash 3 · Plotly ≥5.24 · DuckDB
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Final

import dash
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, State, ctx, dcc, html  # removed no_update
from dash.development.base_component import Component  # for typing

DB_PATH: Final = Path("../../qof_vis.db").resolve()

ORG_TABLE: dict[str, str] = {
    "Practice": "fct__practice_achievement",
    "PCN": "fct__pcn_achievement",
    "Sub-ICB": "fct__sub_icb_achievement",
    "ICB": "fct__icb_achievement",
    "Region": "fct__region_achievement",
}

BUCKET_SQL: dict[str, str] = {
    "< 20 %": "< 20",
    "20-40 %": ">= 20 AND percentage_patients_achieved < 40",
    "40-60 %": ">= 40 AND percentage_patients_achieved < 60",
    "60-80 %": ">= 60 AND percentage_patients_achieved < 80",
    "80-100 %": ">= 80",
}
DEFAULT_BUCKET = "80-100 %"


def query(sql: str) -> pd.DataFrame:
    with duckdb.connect(DB_PATH.as_posix(), read_only=True) as con:
        return con.sql(sql).df()


def md_wrap(text: str | None, width: int = 80) -> str:
    if not text:
        return ""
    return "  \n".join(textwrap.wrap(text, width))


def make_blank_map() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        map=dict(style="carto-positron", center={"lat": 53, "lon": -1.5}, zoom=5),
        margin=dict(t=0, r=0, l=0, b=0),
        height=700,
    )
    return fig


def make_blank_bar(msg: str = "Click a point") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=msg, xaxis_visible=False, yaxis_visible=False, margin=dict(t=40, r=10, l=10, b=10)
    )
    return fig


# valid indicator-year pairs (few ms)
pairs = query("SELECT DISTINCT indicator_code, reporting_year FROM fct__practice_achievement")
ALL_YEARS: list[int] = sorted([int(y) for y in pairs.reporting_year.unique()])
ALL_INDS: list[str] = sorted([str(i) for i in pairs.indicator_code.unique()])

app = dash.Dash(__name__)

DEFAULT_IND = ALL_INDS[0] if ALL_INDS else None
DEFAULT_YEAR = ALL_YEARS[-1] if ALL_YEARS else None

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
def sync_dropdowns(ind_val: str | None, yr_val: int | None, level_val: str | None):
    # Always filter codes to those with data for the selected year, if a year is selected
    if yr_val is not None:
        valid_df = query(
            f"""
            SELECT DISTINCT indicator_code
            FROM fct__practice_achievement
            WHERE reporting_year = {yr_val}
              AND percentage_patients_achieved IS NOT NULL
            """
        )
        valid_ind = sorted([str(i) for i in valid_df.indicator_code])
        ind_opts = [{"label": i, "value": i} for i in valid_ind]
        ind_val = ind_val if ind_val in valid_ind else (valid_ind[0] if valid_ind else None)
    else:
        ind_opts = [{"label": i, "value": i} for i in ALL_INDS]
        ind_val = ind_val if ind_val in ALL_INDS else (ALL_INDS[0] if ALL_INDS else None)
    yr_opts = [{"label": y, "value": y} for y in ALL_YEARS]

    # Always show all buckets, but disable those with no data for the selected org level
    table = ORG_TABLE.get(level_val, "fct__practice_achievement")
    bucket_opts = []
    for label, cond in BUCKET_SQL.items():
        count_df = query(
            f"""
            SELECT COUNT(*) as n
            FROM {table}
            WHERE indicator_code = '{ind_val}'
              AND reporting_year = {yr_val}
              AND percentage_patients_achieved {cond}
            """
        )
        has_data = count_df.n.iloc[0] > 0
        bucket_opts.append({"label": label, "value": label, "disabled": not has_data})

    # Set selected_bucket to DEFAULT_BUCKET if available and enabled, else first enabled
    enabled_buckets = [opt for opt in bucket_opts if not opt.get("disabled", False)]
    selected_bucket = None
    if any(
        opt["value"] == DEFAULT_BUCKET and not opt.get("disabled", False) for opt in bucket_opts
    ):
        selected_bucket = DEFAULT_BUCKET
    elif enabled_buckets:
        selected_bucket = enabled_buckets[0]["value"]
    else:
        selected_bucket = None
    # Try to keep the current bucket if still valid and enabled
    current_bucket = ctx.states.get("bucket.value") if hasattr(ctx, "states") else None
    if current_bucket and any(
        opt["value"] == current_bucket and not opt.get("disabled", False) for opt in bucket_opts
    ):
        selected_bucket = current_bucket

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
    indic: str | None, yr: int | None, level: str, bucket: str
) -> tuple[go.Figure, Component | None]:
    if indic is None or yr is None:
        return make_blank_map(), None

    tbl = ORG_TABLE[level]
    q = (
        f"SELECT organisation_name, organisation_code, "
        f"percentage_patients_achieved AS pct, "
        f"output_description AS descr, lat, lng "
        f"FROM {tbl} "
        f"WHERE indicator_code = '{indic}' "
        f"AND reporting_year = {yr} "
        f"AND percentage_patients_achieved {BUCKET_SQL[bucket]}"
    )
    df = query(q)
    if df.empty:
        return make_blank_map(), None

    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lng",
        hover_name="organisation_name",
        custom_data=["pct", "organisation_name"],
        map_style="carto-positron",
        center={"lat": 53, "lon": -1.5},
        zoom=6,
        height=700,
    )
    fig.update_traces(
        marker={"size": 7, "color": "blue"},
        hovertemplate="<b>%{customdata[1]}</b><br>%{customdata[0]}%<extra></extra>",
        hoverlabel=dict(bgcolor="white"),
    )
    fig.update_layout(margin=dict(t=0, r=0, l=0, b=0))

    return fig, dcc.Markdown(md_wrap(df.descr.iat[0]))


@app.callback(
    Output("bars", "figure"),
    Input("map", "clickData"),
    State("ind", "value"),
    State("yr", "value"),
    State("level", "value"),
)
def build_bars(click, indic: str | None, yr: int | None, level: str) -> go.Figure:
    if not click or indic is None or yr is None:
        return make_blank_bar()

    org_name: str = click["points"][0]["customdata"][1]

    org = query(
        f"""
        SELECT group_description AS g,
               ANY_VALUE(percentage_patients_achieved) AS org
        FROM {ORG_TABLE[level]}
        WHERE organisation_name = '{org_name}'
          AND reporting_year = {yr}
        GROUP BY g
        """
    )
    nat = query(
        f"""
        SELECT group_description AS g,
               ANY_VALUE(percentage_patients_achieved) AS nat
        FROM fct__national_achievement
        WHERE reporting_year = {yr}
        GROUP BY g
        """
    )
    df = org.merge(nat, on="g", how="left")
    if df.empty:
        return make_blank_bar("No data for this organisation")

    grp_row = query(
        f"""
        SELECT DISTINCT group_description
        FROM fct__practice_achievement
        WHERE indicator_code = '{indic}'
        """
    )
    highlight = grp_row.group_description.iat[0] if not grp_row.empty else None

    long_df = df.melt("g", value_name="pct", var_name="series")

    fig = px.bar(
        long_df,
        x="pct",
        y="g",
        color="series",
        barmode="group",
        orientation="h",
        labels={"pct": "% achieved", "g": "Indicator group", "series": ""},
        color_discrete_map={"org": "#1f77b4", "nat": "#ff7f0e"},
        height=750,
        title=f"{org_name} vs National — {yr}",
    )
    if highlight and highlight in df.g.values:
        fig.add_hrect(
            y0=highlight, y1=highlight, line_width=0, fillcolor="rgba(0,0,0,0.05)", layer="below"
        )

    fig.update_layout(
        hoverlabel=dict(bgcolor="white"),
        yaxis=dict(categoryorder="category ascending", autorange="reversed"),
        margin=dict(t=40, r=10, l=10, b=10),
    )
    return fig


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
