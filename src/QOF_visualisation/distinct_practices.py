from pathlib import Path

import duckdb

conn = duckdb.connect()
sources = Path("./sources/").resolve()

output_file_name = "gp_location_info.parquet"
output_str = str(sources / output_file_name)

address_file_name = "gp_location_info.parquet"


def get_distinct_practice_codes():
    nhs_organisations_str = str(sources / "nhs_organisations_*.parquet")
    nhs_organisations = conn.read_parquet(nhs_organisations_str)
    codes = nhs_organisations.select("PRACTICE_CODE as practice_code").distinct()
    return codes


def get_address_list():
    address_str = str(sources / address_file_name)
    columns = """
    column00 as practice_code,
    column01.lower() as practice_name,
    column04.lower() as address_line_1,
    column05.lower() as address_line_2,
    column06.lower() as address_line_3,
    column07.lower() as address_line_4,
    column08.lower() as address_line_5,
    column09.lower() as postcode,
    column10 as open_date,
    column11 as closed_date,
    column17 as telephone_number
    """
    address_list_raw = conn.read_parquet(address_str)
    address_list = address_list_raw.select(columns)
    return address_list


practice_codes = get_distinct_practice_codes()
unfiltered_address_list = get_address_list()
filtered_address_list = practice_codes.join(unfiltered_address_list, "practice_code")

conc_string = """
    *,
    concat_ws(', ', practice_name, postcode) AS short_address,
    concat_ws(', ', practice_name, address_line_1, address_line_2, address_line_3, address_line_4, postcode) AS long_address,
"""
final_address_list = filtered_address_list.select(conc_string)
final_address_list.to_parquet(output_str)
