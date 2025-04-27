import asyncio
import io
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import NamedTuple, cast

import duckdb
import googlemaps
import httpx
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
TARGET_DIR = Path(os.getenv("TARGET_DIRECTORY", ""))
MAX_CONCURRENT = 10  # tune to your quota
RETRIES = 3


class Coordinates(NamedTuple):
    lat: float | None
    lng: float | None


def download_and_extract_zip(url: str, extract_to: Path) -> list[Path]:
    print(f"Downloading: {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        for name in z.namelist():
            tp = extract_to / name
            if tp.exists():
                tp.unlink() if tp.is_file() else shutil.rmtree(tp)
        z.extractall(path=extract_to)
        return [extract_to / name for name in z.namelist()]


def find_target_files(files: list[Path], match_terms: list[str]) -> list[Path]:
    return [
        f
        for f in files
        if f.suffix == ".csv" and any(term in f.name.lower() for term in match_terms)
    ]


def create_address_list(address_csv: Path, gp_list_csv: Path) -> Path:
    output_path = TARGET_DIR / "GP_contact_info.parquet"
    with duckdb.connect() as con:
        con.sql(f"""
        COPY (
            SELECT
                l.practice_code,
                l.practice_name,
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
            FROM '{gp_list_csv}' l
            LEFT JOIN (FROM '{address_csv}') a
              ON l.practice_code = a.column00
        ) TO '{output_path}' (FORMAT PARQUET)
        """)
    return output_path


def get_coordinates_sync(
    gmaps: googlemaps.Client,
    address: str,
    retries: int = 3,
) -> Coordinates:
    for attempt in range(retries):
        try:
            time.sleep(0.02)
            result = gmaps.geocode(address)  # pyright: ignore[]
            if not result:
                return Coordinates(lat=None, lng=None)
            loc = cast(dict[str, float], result[0]["geometry"]["location"])
            return Coordinates(lat=loc["lat"], lng=loc["lng"])
        except Exception:
            delay = 2**attempt * 0.1
            time.sleep(delay)
    return Coordinates(lat=None, lng=None)


async def geocode_one(
    session: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    practice_code: str,
    short_addr: str,
    long_addr: str,
) -> tuple[str, float | None, float | None]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"key": API_KEY}
    async with sem:
        # try short, then long address
        for address in (short_addr, long_addr):
            params["address"] = address
            for attempt in range(RETRIES):
                r = await session.get(url, params=params)
                data = r.json()
                status = data.get("status")
                if status == "OK":
                    loc = data["results"][0]["geometry"]["location"]
                    return practice_code, loc["lat"], loc["lng"]
                if status == "ZERO_RESULTS":
                    break
                await asyncio.sleep(2**attempt * 0.1)
        # both failed
        return practice_code, None, None


async def batch_geocode(
    addresses: list[tuple[str, str, str]],
) -> list[tuple[str, float | None, float | None]]:
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    async with httpx.AsyncClient(timeout=10.0) as session:
        tasks = [
            geocode_one(session, sem, code, short, long)
            for code, short, long in addresses
        ]
        return await asyncio.gather(*tasks)


async def main():
    # prepare output dir
    TARGET_DIR.mkdir(exist_ok=True)

    # download & extract
    gp_files = download_and_extract_zip(
        "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip", TARGET_DIR
    )
    qof_files = download_and_extract_zip(
        "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip", TARGET_DIR
    )

    # create contact list parquet
    addr_csv = find_target_files(gp_files, ["epraccur"])[0]
    gp_csv = find_target_files(qof_files, ["mapping_nhs_geographies_2324"])[0]
    gp_parquet = create_address_list(addr_csv, gp_csv)

    # load practice_code + both addresses
    with duckdb.connect() as con:
        addresses = con.sql(f"""
            SELECT practice_code, short_address, long_address
            FROM '{gp_parquet}'
        """).fetchall()

    # async geocode
    coords = await batch_geocode(addresses)

    # sync fallback for any NULLs
    gmaps_sync = googlemaps.Client(key=API_KEY)
    rescued = 0
    for idx, (code, lat, lon) in enumerate(coords):
        if lat is None and lon is None:
            short_addr, long_addr = addresses[idx][1], addresses[idx][2]
            c = get_coordinates_sync(gmaps_sync, short_addr)
            if c.lat is None:
                c = get_coordinates_sync(gmaps_sync, long_addr)
            if c.lat is not None:
                coords[idx] = (code, c.lat, c.lng)
                rescued += 1
    print(f"Sync fallback rescued {rescued} practices.")

    # write out Practice_coordinates.parquet
    with duckdb.connect() as con:
        con.sql("""
            CREATE OR REPLACE TABLE Practice_coordinates (
                practice_code VARCHAR,
                lat DOUBLE,
                lon DOUBLE
            )
        """)
        con.executemany("INSERT INTO Practice_coordinates VALUES (?, ?, ?)", coords)
        con.table("Practice_coordinates").to_parquet(
            str(TARGET_DIR / "practice_coordinates.parquet")
        )

    print(f"Geocoded {len(coords)} practices → {TARGET_DIR}")


if __name__ == "__main__":
    if not API_KEY or not TARGET_DIR:
        raise RuntimeError("Set GOOGLE_MAPS_API_KEY and TARGET_DIRECTORY in .env")
    asyncio.run(main())
