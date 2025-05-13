import textwrap

import dash
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from dash import Input, Output, callback_context, dcc, html

conn = duckdb.connect("./sources/qof.db")
percent_achieved = conn.sql(
    "SELECT * FROM './sources/Percent_achieved.parquet' WHERE REGISTER IS NULL;"
)
gp_location_info = conn.sql("SELECT * FROM './sources/GP_info_and_coordinates.parquet';")

print("percent achieved schema:")
print(conn.sql("DESCRIBE SELECT * FROM percent_achieved;").df())
print("\nSample data:")
print(conn.sql("SELECT * FROM './sources/percent_achieved.parquet'").df())

indicator_codes = sorted(
    r[0] for r in conn.sql("SELECT DISTINCT INDICATOR_CODE FROM percent_achieved;").fetchall()
)

bucket_sql = {
    "< 20%": "< 20",
    "20-40%": ">= 20 AND percentage_patients_achieved < 40",
    "40-60%": ">= 40 AND percentage_patients_achieved < 60",
    "60-80%": ">= 60 AND percentage_patients_achieved < 80",
    "80-100%": ">= 80",
}

# national average for every indicator group – computed once
national_df = conn.sql("""
    SELECT
        GROUP_CODE,
        GROUP_DESCRIPTION AS group_desc,
        AVG(Percentage_patients_achieved) as national_avg
    FROM percent_achieved
    GROUP BY GROUP_CODE, GROUP_DESCRIPTION
    ORDER BY GROUP_CODE
""").df()

app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H3("QOF Practice Performance Map"),
        html.Div(
            [
                dcc.Dropdown(
                    id="indicator-dropdown",
                    options=[{"label": c, "value": c} for c in indicator_codes],
                    value=indicator_codes[0],
                    clearable=False,
                    style={"width": "260px"},
                ),
                dcc.RadioItems(
                    id="bucket-radio",
                    options=[{"label": k, "value": k} for k in bucket_sql],
                    value=list(bucket_sql.keys())[0],
                    inline=True,
                    style={"paddingLeft": "20px"},
                ),
            ],
            style={"display": "flex", "alignItems": "center"},
        ),
        html.Div(id="indicator-description", style={"fontSize": "16px", "marginTop": "8px"}),
        # two‑panel area: map left, bar chart right
        html.Div(
            [
                dcc.Graph(id="map-graph", style={"height": "80vh", "width": "60vw"}),
                dcc.Graph(
                    id="bar-chart",
                    style={
                        "height": "80vh",
                        "width": "38vw",
                        "overflowY": "auto",
                    },
                ),
            ],
            style={"display": "flex", "gap": "1%"},
        ),
    ],
    style={"padding": "12px"},
)


def wrap_md(text: str, width: int = 80) -> str:
    return "  \n".join(textwrap.wrap(text, width))


@app.callback(
    Output("map-graph", "figure"),
    Output("indicator-description", "children"),
    Input("indicator-dropdown", "value"),
    Input("bucket-radio", "value"),
)
def update_map(indicator: str, bucket: str):
    sql = f"""
        WITH indicator_results AS (
            SELECT
                PRACTICE_CODE,
                Percentage_patients_achieved AS pct,
                Output_Description AS description
            FROM percent_achieved
            WHERE INDICATOR_CODE = '{indicator}'
              AND percentage_patients_achieved {bucket_sql[bucket]}
        )
        SELECT
            p.PRACTICE_CODE,
            p.pct,
            p.description,
            g.PRACTICE_NAME AS practice_name,
            g.lat,
            g.lon
        FROM indicator_results p
        LEFT JOIN gp_location_info g
          ON p.PRACTICE_CODE = g.PRACTICE_CODE;
    """
    df: pd.DataFrame = conn.sql(sql).df()

    desc_md = wrap_md(df["description"].iloc[0]) if not df.empty else "*No description provided.*"

    # include PRACTICE_CODE in customdata so click can pick it up
    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        hover_name="practice_name",
        custom_data=["pct", "PRACTICE_CODE"],
        zoom=6,
        center=dict(lat=53, lon=-1.5),
        map_style="carto-positron",
    )
    fig.update_traces(
        marker=dict(size=6, color="blue"),
        hovertemplate="<b>%{hovertext}</b><br>%{customdata[0]}%<extra></extra>",
    )
    fig.update_layout(margin=dict(r=0, t=0, l=0, b=0))

    return fig, dcc.Markdown(desc_md)


def blank_fig():
    fig = go.Figure()
    fig.update_layout(
        title="Click a practice dot to see practice vs. national by group",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(t=40, r=10, l=10, b=10),
    )
    return fig


@app.callback(
    Output("bar-chart", "figure"),
    Input("map-graph", "clickData"),
    Input("indicator-dropdown", "value"),
    Input("bucket-radio", "value"),
)
def make_bar(click_data, indicator, bucket):
    ctx = callback_context
    # no inputs fired yet (app just loaded)
    if not ctx.triggered:
        return blank_fig()

    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
    # if user changed filters, clear
    if triggered_id in ("indicator-dropdown", "bucket-radio"):
        return blank_fig()

    # real map click
    if not click_data:
        return blank_fig()

    practice_code = click_data["points"][0]["customdata"][1]

    # 1) start from your precomputed national_df (all groups, alphabetic)
    base = national_df.copy()  # cols: group_desc, national_avg

    practice_df = conn.sql(f"""
        SELECT
            p.GROUP_DESCRIPTION AS group_desc,
            AVG(p.Percentage_patients_achieved) AS practice_avg,
            p.INDICATOR_CODE
        FROM percent_achieved p
        WHERE PRACTICE_CODE = '{practice_code}'
        GROUP BY GROUP_DESCRIPTION, INDICATOR_CODE
        ORDER BY GROUP_DESCRIPTION, INDICATOR_CODE
    """).df()

    # Get practice averages
    practice_df = conn.sql(f"""
        SELECT 
            GROUP_CODE,
            GROUP_DESCRIPTION as group_desc,
            AVG(Percentage_patients_achieved) as practice_avg
        FROM percent_achieved 
        WHERE PRACTICE_CODE = '{practice_code}'
        GROUP BY GROUP_CODE, GROUP_DESCRIPTION
    """).df()

    # Merge practice and national data
    base = practice_df.merge(national_df, on=["GROUP_CODE", "group_desc"], how="left").sort_values(
        "group_desc"
    )

    # Sort by group description
    base = base.sort_values("group_desc").reset_index(drop=True)

    # Debug prints
    print("\nNational averages sample:")
    print(practice_df.head())
    print("\nPractice averages sample:")
    print(practice_df.head())
    print("\nMerged data sample:")
    print(base.head())

    # Melt to long form for visualization
    long_df = base.melt(
        id_vars="group_desc",
        value_vars=["practice_avg", "national_avg"],
        var_name="series",
        value_name="pct",
    )

    # Debug: print the data going into the visualization
    print("\nData for visualization:")
    print(base.sort_values("group_desc"))

    # Melt to long form for visualization
    long_df = base.melt(
        id_vars=["GROUP_CODE", "group_desc"],
        value_vars=["practice_avg", "national_avg"],
        var_name="series",
        value_name="pct",
    )

    # Debug: print the melted data
    print("\nLong form data:")
    print(long_df.sort_values(["group_desc", "series"]))

    # 5) draw grouped horizontal bars
    fig = px.bar(
        long_df,
        x="pct",
        y="group_desc",
        color="series",
        orientation="h",
        barmode="group",
        title=f"Practice {practice_code} vs National Average",
        labels={"pct": "% achieved", "group_desc": "Indicator group", "series": "Average type"},
        color_discrete_map={"practice_avg": "#1f77b4", "national_avg": "#ff7f0e"},
    )

    # 6) force A→Z with A at the top
    fig.update_layout(
        yaxis=dict(
            automargin=True,
            categoryorder="category ascending",
            autorange="reversed",
        ),
        margin=dict(t=40, r=10, l=10, b=10),
        height=750,
    )
    return fig


if __name__ == "__main__":
    app.run(debug=True)
