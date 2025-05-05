import io
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Dict of QOF year csv zip urls.
raw_qof_path_dict: dict[str, str] = {
    "1920": "https://files.digital.nhs.uk/E2/BF16AA/QOF_1920.zip",
    "2021": "https://files.digital.nhs.uk/AC/3C964F/QOF2021_v2.zip",
    "2122": "https://files.digital.nhs.uk/90/6F833F/QOF_2122_V2.zip",
    "2223": "https://files.digital.nhs.uk/32/40B931/QOF%202022-23%20Raw%20data%20.csv%20files.zip",
    "2324": "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip",
}

# Dict of supplementary data.
supplementary_url_dict: dict[str, str] = {
    "PCD_reference_set": "https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/data-collections/qof/primary-care-domain-reference-set-portal/20240531_pcd_refset_content_text_files.zip",
    "Practices": "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip",
}


def get_with_retry(url: str, retries: int = 5, backoff: float = 0.5) -> requests.Response:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,  # Waits: 0.5s, 1s, 2s, etc.
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session.get(url, timeout=10)


def create_practice_geographical_list(
    address_csv: Path, gp_list_csv_list: list[Path], tmpdirname: str
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


def download_and_extract_zip(dir_name: str, url: str, tmpdirname: str) -> Path:
    print(f"Downloading: {url}")
    resp = get_with_retry(url)
    resp.raise_for_status()

    tp = Path(tmpdirname) / dir_name

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        if tp.exists():
            if tp.is_file():
                tp.unlink()
            else:
                shutil.rmtree(tp)
        z.extractall(tp)

    # Re-encode CSV files to UTF-8
    for csv_file in tp.rglob("*.csv"):
        content = csv_file.read_text(encoding="latin1")
        csv_file.write_text(content, encoding="utf-8")

    return tp


def main():
    with tempfile.TemporaryDirectory() as tmpdirname:
        PCD = download_and_extract_zip(
            "PCD_reference_set", supplementary_url_dict["PCD_reference_set"], tmpdirname
        )
        Practice_location_info = download_and_extract_zip(
            "Practice_location_info", supplementary_url_dict["Practices"], tmpdirname
        )

        # Initialize qof data lists/dicts.
        shaped_qof_path_dict: dict[str, Path] = {}
        raw_qof_geo_map_path_list: list[Path] = []
        for year, raw_directory_path in raw_qof_path_dict.items():
            # download and shape qof data.
            year_path: Path = download_and_extract_zip(year, raw_directory_path, tmpdirname)
            qof_path: Path = shape_QOF_data(year_path, PCD)

            # Add to dict.
            shaped_qof_path_dict[year] = qof_path

            # get geo map file path.
            mapping_file_path = next(Path(year_path).glob("MAPPING_NHS_GEOGRAPHIES_*.csv"))
            raw_qof_geo_map_path_list.append(mapping_file_path)
            print(f"Processed year {year}")

        # Create practive address and PCN mapping list.
        practice_geo_list = create_practice_geographical_list(
            Practice_location_info, raw_qof_geo_map_path_list, tmpdirname
        )
        print("process complete")


if __name__ == "__main__":
    main()
