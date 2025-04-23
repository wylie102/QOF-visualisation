import io
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import requests


def download_and_extract_zip(url: str, extract_to: Path) -> list[Path]:
    print(f"Downloading: {url}")
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Clean up any old versions of extracted files
        for name in z.namelist():
            target_path = extract_to / name
            if target_path.exists():
                if target_path.is_dir():
                    shutil.rmtree(target_path)
                else:
                    target_path.unlink()

        z.extractall(path=extract_to)
        return [extract_to / name for name in z.namelist()]


def find_target_files(files: list[Path], match_terms: list[str]) -> list[Path]:
    return [
        f
        for f in files
        if any(term in f.name.lower() for term in match_terms) and f.suffix == ".csv"
    ]


def get_input_paths():
    temp_dir = Path(tempfile.gettempdir()) / "gp_data"
    temp_dir.mkdir(exist_ok=True)

    # Clear old contents
    for f in temp_dir.iterdir():
        if f.is_dir():
            shutil.rmtree(f)
        else:
            f.unlink()

    gp_zip_url = "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip"
    qof_zip_url = "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip"

    gp_files = download_and_extract_zip(gp_zip_url, temp_dir)
    qof_files = download_and_extract_zip(qof_zip_url, temp_dir)

    address_csv = find_target_files(gp_files, ["epraccur"])
    gp_list_csv = find_target_files(qof_files, ["mapping_nhs_geographies_2324"])

    return address_csv, gp_list_csv


def create_address_list(address_csv: Path, gp_list_csv: Path) -> Path:
    temp_dir = Path(tempfile.gettempdir()) / "gp_data"
    output_path = temp_dir / "GP_contact_info.parquet"

    with duckdb.connect() as con:
        query = f"""
        copy (
            select
                l.practice_code,
                l.practice_name,
                a.column04.lower() as address_line_1,
                a.column05.lower() as address_line_2,
                a.column06.lower() as address_line_3,
                a.column07.lower() as address_line_4,
                a.column09 as postcode,
                concat_ws(', ', l.practice_name, a.column09) as short_address,
                concat_ws(', ', l.practice_name, a.column04.lower(), a.column05.lower(), a.column06.lower(), a.column07.lower(), a.column09) as long_address,
                a.column17 as telephone_no
            from '{gp_list_csv}' l
            left join (from '{address_csv}') a
                on l.practice_code = a.column00
        )
        to '{output_path}' (format parquet);
        """
        con.sql(query)

    return output_path


address_csv, gp_list_csv = get_input_paths()
parquet_path = create_address_list(address_csv[0], gp_list_csv[0])
print(str(parquet_path))
