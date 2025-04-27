import duckdb
import plotly.express as px
from duckdb.typing import DuckDBPyConnection
from pandas import DataFrame


def main():
    info_and_coordinates = "/Users/wylie/Desktop/Projects/QOF_visualisation/practice_folder/info_and_coordinates.parquet"

    con: DuckDBPyConnection = duckdb.connect()
    df: DataFrame = con.sql(
        f"select PRACTICE_NAME as practice_name, lat, lon, telephone_no as telephone from {info_and_coordinates};"
    ).to_df()

    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        hover_name="practice_name",
        custom_data=["telephone"],
        zoom=6,
        center=dict(lat=53, lon=-1.5),
        map_style="carto-positron",
    )

    fig.update_traces(
        marker=dict(
            size=4,
            color="blue",
        ),
        hovertemplate="<b>%{hovertext}</b><br>Telephone: %{customdata[0]}<extra></extra>",
    )

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )

    fig.show()


if __name__ == "__main__":
    main()
