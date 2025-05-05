import dash
from dash import dcc, html, Input, Output
import duckdb
import plotly.express as px
import textwrap

conn = duckdb.connect()

percent_achieved = conn.sql("SELECT * FROM './sources/Percent_achieved.parquet';")
gp_location_info = conn.sql("SELECT * FROM './sources/GP_info_and_coordinates.parquet';")

indicator_codes = [
    r[0] for r in conn.sql("SELECT DISTINCT INDICATOR_CODE FROM percent_achieved;").fetchall()
]

bucket_sql = {
    "< 20%": "< 20",
    "20-40%": ">= 20 AND percentage_patients_achieved < 40",
    "40-60%": ">= 40 AND percentage_patients_achieved < 60",
    "60-80%": ">= 60 AND percentage_patients_achieved < 80",
    "80-100%": ">= 80",
}

app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H3("QOF Practice Performance Map"),
        dcc.Dropdown(
            id="indicator-dropdown",
            options=[{"label": c, "value": c} for c in indicator_codes],
            value=indicator_codes[0],
            clearable=False,
            style={"width": "300px"},
        ),
        dcc.RadioItems(
            id="bucket-radio",
            options=[{"label": k, "value": k} for k in bucket_sql],
            value=list(bucket_sql.keys())[0],
            inline=True,
            style={"paddingTop": "10px"},
        ),
        dcc.Graph(id="map-graph", style={"height": "80vh"}),
    ]
)


@app.callback(
    Output("map-graph", "figure"),
    Input("indicator-dropdown", "value"),
    Input("bucket-radio", "value"),
)
def update_map(indicator, bucket):
    sql = f"""
        WITH indicator_results AS (
            SELECT
                PRACTICE_CODE,
                percentage_patients_achieved AS pct,
                GROUP_CODE,
                GROUP_DESCRIPTION,
                Output_description
            FROM percent_achieved
            WHERE INDICATOR_CODE = '{indicator}'
              AND percentage_patients_achieved {bucket_sql[bucket]}
        )
        SELECT
            p.PRACTICE_CODE,
            p.pct,
            g.PRACTICE_NAME AS practice_name,
            g.lat,
            g.lon,
            p.GROUP_CODE,
            p.GROUP_DESCRIPTION,
            p.Output_description
        FROM indicator_results p
        LEFT JOIN gp_location_info g
          ON p.PRACTICE_CODE = g.PRACTICE_CODE
    """
    df = conn.sql(sql).df()  # pandas DataFrame
    print(df.columns)

    if "Output_Description" in df.columns and not df.empty:
        description = df["Output_Description"].iloc[0]
    else:
        description = indicator  # fallback

    wrapped = "<br>".join(textwrap.wrap(description, width=60))

    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        hover_name="practice_name",
        custom_data=["pct"],
        zoom=6,
        center={"lat": 53, "lon": -1.5},
        map_style="carto-positron",
    )
    fig.update_traces(
        marker={"size": 6, "color": "blue"},
        hovertemplate="<b>%{hovertext}</b><br>%{customdata[0]}%<extra></extra>",
    )
    fig.update_layout(
        title=f"{wrapped}<br><span style='font-size:0.9em'>{bucket}</span>",
        margin={"r": 0, "t": 70, "l": 0, "b": 0},
    )
    return fig


if __name__ == "__main__":
    app.run(debug=True)
