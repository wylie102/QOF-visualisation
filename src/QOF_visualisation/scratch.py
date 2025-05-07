from pathlib import Path

import duckdb


def create_practice_geographical_list(
    address_csv: Path, gp_list_csv_list: list[Path], target_dir: str
) -> Path:
    output_path = Path("../../sources" + "/" + "GP_contact_info.parquet")
    with duckdb.connect() as con:
        con.sql(f"""
        COPY (
            SELECT
                l.* exclude(NAT_ONS_CODE, NAT_CODE, COUNTRY),
                coalesce(lower(a.column04), '') AS address_line_1,
                coalesce(lower(a.column05), '') AS address_line_2,
                coalesce(lower(a.column06), '') AS address_line_3,
                coalesce(lower(a.column07), '') AS address_line_4,
                a.column09 AS postcode,
                concat_ws(', ', l.practice_name, a.column09) AS short_address,
                concat_ws(', ', l.practice_name,
                    a.column04.lower(), a.column05.lower(),
                    a.column06.lower(), a.column07.lower(), a.column09
                ) AS long_address,
                a.column17 AS telephone_no
            FROM '{gp_list_csv_list}' l
            LEFT JOIN (FROM '{address_csv}') a
              ON l.practice_code = a.column00
        ) TO '{output_path}' (FORMAT PARQUET)
        """)
    return output_path


def shape_QOF_data(directory_path: Path, PCD_dir: Path):
    """Shape data from raw csv files to parquet files with percentage achieved stats"""

    achievement = directory_path / "ACHIEVEMENT_*.csv"
    mapping_indicators = directory_path / "MAPPING_INDICATORS_*.csv"
    output_descriptions = PCD_dir / "20241205_PCD_Output_Descriptions.txt"
    pivot_query: str = f"""
    from '{achievement}'
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
    left join (from read_csv('{output_descriptions}')) d
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

    Path("../../sources").mkdir(parents=True, exist_ok=True)
    output = "../../sources/" + directory_path.stem + ".parquet"

    with duckdb.connect() as con:
        pivot = con.sql(pivot_query)
        percent_achieved = con.sql(percent_achieved_query)
        percent_achieved.to_parquet(output)

    return Path(output)
