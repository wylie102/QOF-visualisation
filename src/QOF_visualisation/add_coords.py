import os
import sys
from pathlib import Path
from sys import argv
from typing import NamedTuple

import duckdb
from dotenv import load_dotenv
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from QOF_visualisation.batch_geocode import batch_geocode
from QOF_visualisation.coord_dataclasses import PracticeAdd, PracticeCoords


class Settings(NamedTuple):
    target_file: Path
    output_file: Path
    api_key: str
    conn: DuckDBPyConnection
    concurrent: int
    retries: int


def setup(target_file: Path, api_key: str | None, concurrent: int, retries: int) -> Settings:
    output_file: Path = target_file.with_stem(target_file.stem + "_new")
    if not api_key:
        api_key = get_api_key()
    conn: DuckDBPyConnection = duckdb.connect()

    settings: Settings = Settings(target_file, output_file, api_key, conn, concurrent, retries)
    return settings


def get_api_key() -> str:
    load_dotenv()
    api_key: str | None = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        sys.exit("No api-key found")
    return api_key


def table_from_file(conn: DuckDBPyConnection, target_file: Path) -> DuckDBPyRelation:
    table_name: str = target_file.stem
    file_relation: DuckDBPyRelation = conn.read_parquet(str(target_file))
    file_relation.create(table_name)
    table = conn.table(table_name)

    return table


def get_null_rows(table: DuckDBPyRelation) -> list[PracticeAdd]:
    filtered: DuckDBPyRelation = table.filter("lat is null or lon is null")
    columns: DuckDBPyRelation = filtered.select("practice_code, short_address, long_address")
    null_list: list[tuple[str, str, str]] = columns.fetchall()
    practice_adds_list: list[PracticeAdd] = [
        PracticeAdd(practice_code, short_address, long_address)
        for (practice_code, short_address, long_address) in null_list
    ]
    return practice_adds_list


def add_to_table(settings: Settings, results_list: list[PracticeCoords]):
    # “joined” (or whatever your table is) must already exist
    base = settings.target_file.stem

    # Build the parameter list directly from your NamedTuples
    params = [(r.lat, r.lon, r.practice_code) for r in results_list]

    # Execute one parameterized UPDATE per tuple
    settings.conn.executemany(
        f"""
        UPDATE {base}
        SET
            lat = ?,
            lon = ?
        WHERE practice_code = ?
        """,
        params,
    )

    # Return a handle to the now‐updated table
    return settings.conn.table(base)


def add_coords(
    target_file: Path, concurrent: int = 10, retries: int = 3, api_key: str | None = None
) -> Path:
    settings: Settings = setup(target_file, api_key, concurrent, retries)
    current_practice_info: DuckDBPyRelation = table_from_file(settings.conn, settings.target_file)
    null_list: list[PracticeAdd] = get_null_rows(current_practice_info)
    results_list, failure_list = batch_geocode(
        null_list,
        settings.api_key,
    )
    print(f"Results list length: {len(results_list)}\nFailure list length: {len(failure_list)}")
    print(failure_list)
    if results_list:
        new_table = add_to_table(settings, results_list)
        new_table.to_parquet(str(settings.output_file))

    return settings.output_file


if __name__ == "__main__":
    target_file: Path = Path(argv[1]).resolve()
    output_file = add_coords(target_file)
    print(output_file)
