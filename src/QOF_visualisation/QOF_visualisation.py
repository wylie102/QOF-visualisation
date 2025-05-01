from pathlib import Path

import duckdb
import plotly.express as px
from polars import DataFrame

# Define Sources.
info_and_coordinates: Path = Path("../../sources/GP_info_and_coordinates.parquet")
achievement_2324: Path = Path("../../sources/achievement_2324.parquet")
mapping_indicators: Path = Path("../../sources/mapping_indicators_2324.parquet")
output_descriptions: Path = Path("../../sources/PCD_Output_Descriptions_20241205.parquet")

# Store query strings.
pivot_query: str = f"""
from '{achievement_2324}'
select
    * exclude (MEASURE, "VALUE"), 
    MAX(case when MEASURE = 'NUMERATOR' then "VALUE" else NULL end) as NUMERATOR,
    MAX(case when MEASURE = 'DENOMINATOR' then "VALUE" else NULL end) as DENOMINATOR,
    MAX(case when MEASURE = 'REGISTER' then "VALUE" else NULL end) as REGISTER,
    MAX(case when MEASURE = 'ACHIEVED_POINTS' then "VALUE" else NULL end) as ACHIEVED_POINTS
group by all;
"""

percent_achieved_query: str = f"""
from "pivot" p
left join (from '{mapping_indicators}') i
on p.INDICATOR_CODE = i.INDICATOR_CODE
left join (from '{output_descriptions}') d
on p.INDICATOR_CODE = d.Output_ID
select
    p.Practice_code,
    i.GROUP_CODE,
    i.GROUP_DESCRIPTION,
    p.INDICATOR_CODE,
    d.Output_Description,
    p.NUMERATOR,
    p.DENOMINATOR,
    p.REGISTER,
    p.ACHIEVED_POINTS,
    i.INDICATOR_POINT_VALUE,
    round((p.NUMERATOR / p.DENOMINATOR) * 100, 1)  as Percentage_patients_achieved,
    round((p.ACHIEVED_POINTS / i.INDICATOR_POINT_VALUE) * 100, 1)  as Percentage_points_achieved;
"""

con = duckdb.connect()
pivot = con.sql(pivot_query)
percent_achieved = con.sql(percent_achieved_query)


def main():
    percent_achieved.to_parquet("Percent_achieved.parquet")
    selected_practices = con.sql(
        """
        from percent_achieved 
        select
            practice_code,
            Percentage_patients_achieved 
        where
            INDICATOR_CODE = 'COPD014' and
            Percentage_patients_achieved < 20;
        """
    )
    df: DataFrame = con.sql(
        f"""
        from selected_practices p
        left join (from '{info_and_coordinates}') i
        on p.practice_code = i.practice_code
        select
            i.PRACTICE_NAME as practice_name,
            i.lat as lat, i.lon as lon,
            i.telephone_no as telephone,
            p.Percentage_patients_achieved as "patients percentage";
        """
    ).pl()

    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        hover_name="practice_name",
        custom_data=["patients percentage"],
        zoom=6,
        center=dict(lat=53, lon=-1.5),
        map_style="carto-positron",
        title="Practices achieving <20% in COPD014 in 2023/2024",
    )

    fig.update_traces(
        marker=dict(
            size=4,
            color="blue",
        ),
        hovertemplate="<b>%{hovertext}</b><br> COPD014 Achievement: %{customdata[0]}%<extra></extra>",
    )

    fig.update_layout(
        margin={"r": 0, "t": 60, "l": 0, "b": 0},
    )

    fig.show()


if __name__ == "__main__":
    main()
