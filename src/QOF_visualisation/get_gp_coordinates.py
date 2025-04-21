import os
import time
from typing import NamedTuple, cast

import duckdb
import googlemaps
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("GOOGLE_MAPS_API_KEY")


class Coordinates(NamedTuple):
    lat: float | None
    lng: float | None


def get_coordinates(address: str, retries: int = 3) -> Coordinates:
    gmaps = googlemaps.Client(key=api_key)
    for attempt in range(retries):
        try:
            time.sleep(0.3)
            result = gmaps.geocode(address)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownArgumentType, reportUnknownVariableType]
            if not result:
                print(f"[WARN] No result for: {address}")
                return Coordinates(lat=None, lng=None)
            location = cast(dict[str, float], result[0]["geometry"]["location"])
            print(f"Success for: {address}")
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
    for practice_code, address in addresses:
        lat, lon = get_coordinates(address)
        results.append((practice_code, lat, lon))
    return results


conn = duckdb.connect("/Users/wylie/Desktop/Data_files/QOF2324_parquet/qof.duckdb")
addresses: list[tuple[str, str]] = conn.view("Gp_addresses").fetchall()
practice_list = create_practice_coordinate_list(addresses)
conn.sql(
    "CREATE OR REPLACE TABLE Practice_coordinates (Practice_code VARCHAR, lat DOUBLE, lon DOUBLE)"
)
insert_query = "INSERT INTO Gp_practice_coordinates VALUES (?, ?, ?)"
conn.executemany(insert_query, practice_list)
