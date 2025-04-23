import io
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import NamedTuple, cast

import duckdb
import googlemaps
import requests
from dotenv import load_dotenv

load_dotenv()


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
    target_directory_path = Path(target_directory)
    # Clear old contents
    for f in target_directory_path.iterdir():
        if f.is_dir():
            shutil.rmtree(f)
        else:
            f.unlink()

    gp_zip_url = "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip"
    qof_zip_url = "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip"

    gp_files = download_and_extract_zip(gp_zip_url, target_directory_path)
    qof_files = download_and_extract_zip(qof_zip_url, target_directory_path)

    address_csv = find_target_files(gp_files, ["epraccur"])
    gp_list_csv = find_target_files(qof_files, ["mapping_nhs_geographies_2324"])

    return address_csv, gp_list_csv


def create_address_list(address_csv: Path, gp_list_csv: Path) -> Path:
    output_path = Path(target_directory) / "GP_contact_info.parquet"

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


class Coordinates(NamedTuple):
    lat: float | None
    lng: float | None


def get_coordinates(gmaps: googlemaps.Client, address: str, retries: int = 3) -> Coordinates:
    for attempt in range(retries):
        try:
            time.sleep(0.02)
            result = gmaps.geocode(address)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownArgumentType, reportUnknownVariableType]
            if not result:
                print(f"[WARN] No result for: {address}")
                return Coordinates(lat=None, lng=None)
            location = cast(dict[str, float], result[0]["geometry"]["location"])
            return Coordinates(lat=location["lat"], lng=location["lng"])
        except Exception as e:
            print(f"[RETRY {attempt + 1}] Failed for ' {address}': {e}")
            delay: int = 2**attempt
            time.sleep(delay)
    return Coordinates(lat=None, lng=None)


def create_practice_coordinate_list(
    addresses: list[tuple[str, str]],
) -> list[tuple[str, float | None, float | None]]:
    results: list[tuple[str, float | None, float | None]] = []
    gmaps = googlemaps.Client(key=api_key)
    for practice_code, address in addresses:
        lat, lon = get_coordinates(gmaps, address)
        results.append((practice_code, lat, lon))
    return results


api_key = str(os.getenv("GOOGLE_MAPS_API_KEY"))
target_directory = str(os.getenv("TARGET_DIRECTORY"))
if not target_directory or target_directory == "":
    print("target_directory is not set, please set target_directory and run again.")
else:
    with duckdb.connect() as con:
        address_csv, gp_list_csv = get_input_paths()
        gp_addresses = create_address_list(address_csv[0], gp_list_csv[0])
        addresses: list[tuple[str, str]] = con.sql(
            f"select practice_code, short_address from '{gp_addresses}'"
        ).fetchall()
        practice_list = create_practice_coordinate_list(addresses)
        con.sql(
            "CREATE OR REPLACE TABLE Practice_coordinates (Practice_code VARCHAR, lat DOUBLE, lon DOUBLE)"
        )
        insert_query = "INSERT INTO Practice_coordinates VALUES (?, ?, ?)"
        con.executemany(insert_query, practice_list)
        con.table("Practice_coordinates").to_parquet(
            f"{target_directory}/practice_coordinates.parquet"
        )
        print(f"Files saved to: {target_directory}")
