import os
import sys
from pathlib import Path
from sys import argv
from typing import NamedTuple

import duckdb
from dotenv import load_dotenv
from duckdb import DuckDBPyConnection, DuckDBPyRelation

load_dotenv()
API_KEY: str | None = os.getenv("GOOGLE_MAPS_API_KEY")


class Settings(NamedTuple):
    target_file: Path
    output_file: Path
    api_key: str
    conn: DuckDBPyConnection
    concurrent: int
    retries: int


class PracticeAdds(NamedTuple):
    practice_code: str
    short_address: str
    long_address: str


class Coordinates(NamedTuple):
    lat: float | None
    lng: float | None


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
    table: DuckDBPyRelation = conn.table(table_name)
    file_relation.insert_into(table_name)

    return table


def get_null_rows(table: DuckDBPyRelation) -> list[PracticeAdds]:
    columns: DuckDBPyRelation = table.select("practice_code, short_address, long_address")
    filtered: DuckDBPyRelation = columns.filter("lat is null or lon is null")
    null_list: list[tuple[str, str, str]] = filtered.fetchall()
    practice_adds_list: list[PracticeAdds] = [
        PracticeAdds(practice_code, short_address, long_address)
        for (practice_code, short_address, long_address) in null_list
    ]
    return practice_adds_list


def add_coords(
    target_file: Path, concurrent: int = 10, retries: int = 3, api_key: str | None = None
) -> Path:
    settings: Settings = setup(target_file, api_key, concurrent, retries)
    current_practice_info: DuckDBPyRelation = table_from_file(settings.conn, settings.target_file)
    null_list: list[PracticeAdds] = get_null_rows(current_practice_info)

    return settings.output_file


if __name__ == "__main__":
    target_file: Path = Path(argv[1]).resolve()
    add_coords(target_file)
