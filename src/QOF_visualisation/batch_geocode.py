import asyncio
import time
from typing import NamedTuple

import googlemaps
import httpx

from QOF_visualisation.coord_dataclasses import PracticeAdd, PracticeCoords


class GeocodeSettings(NamedTuple):
    practice_list: list[PracticeAdd]
    api_key: str
    sem: asyncio.Semaphore
    retries: int


def setup(
    practice_list: list[PracticeAdd], api_key: str, concurrent: int, retries: int
) -> GeocodeSettings:
    sem = asyncio.Semaphore(concurrent)
    settings = GeocodeSettings(practice_list, api_key, sem, retries)
    return settings


def check_addresses_sync(gmaps_sync: googlemaps.Client, p_add: PracticeAdd):
    c = get_coordinates_sync(gmaps_sync, p_add, p_add.short_addr)
    if isinstance(c, PracticeAdd):
        c = get_coordinates_sync(gmaps_sync, p_add, p_add.long_addr)
    return c


def get_coordinates_sync(
    gmaps: googlemaps.Client,
    p_add: PracticeAdd,
    address: str,
    retries: int = 3,
) -> PracticeCoords | PracticeAdd:
    for attempt in range(retries):
        try:
            time.sleep(0.02)
            result = gmaps.geocode(address)  # pyright: ignore[]
            if not result:
                return p_add
            loc = result[0]["geometry"]["location"]
            return PracticeCoords(p_add.practice_code, lat=loc["lat"], lon=loc["lon"])
        except Exception:
            delay = 2**attempt * 0.1
            time.sleep(delay)
    return p_add


async def geocode_one(
    session: httpx.AsyncClient, settings: GeocodeSettings, practice_add: PracticeAdd
) -> PracticeCoords | PracticeAdd:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"key": settings.api_key}
    async with settings.sem:
        # try short, then long address
        for address in (practice_add.short_addr, practice_add.long_addr):
            params["address"] = address
            for attempt in range(settings.retries):
                r = await session.get(url, params=params)
                data = r.json()
                status = data.get("status")
                if status == "OK":
                    loc = data["results"][0]["geometry"]["location"]
                    practice_coords = PracticeCoords(
                        practice_add.practice_code, loc["lat"], loc["lng"]
                    )
                    return practice_coords
                if status == "ZERO_RESULTS":
                    break
                await asyncio.sleep(2**attempt * 0.1)
        # both failed
        return practice_add


async def _batch_geocode_async(
    practice_list: list[PracticeAdd], api_key: str, concurrent: int = 10, retries: int = 3
) -> tuple[list[PracticeCoords], list[PracticeAdd]]:
    settings = setup(practice_list, api_key, concurrent, retries)
    async with httpx.AsyncClient(timeout=10.0) as session:
        tasks = [
            geocode_one(session, settings, practice_add) for practice_add in settings.practice_list
        ]
        results: list[PracticeCoords | PracticeAdd] = await asyncio.gather(*tasks)
        succesful: list[PracticeCoords] = [
            result for result in results if isinstance(result, PracticeCoords)
        ]
        unsuccesful: list[PracticeAdd] = [
            result for result in results if isinstance(result, PracticeAdd)
        ]

    # sync fallback for any NULLs
    gmaps_sync = googlemaps.Client(key=settings.api_key)
    failed: list[PracticeAdd] = []
    for p_add in unsuccesful:
        c = check_addresses_sync(gmaps_sync, p_add)
        if isinstance(c, PracticeCoords):
            succesful.append(c)
        if isinstance(c, PracticeAdd):
            failed.append(c)

    return succesful, failed


def batch_geocode(
    practice_list: list[PracticeAdd], api_key: str, concurrent: int = 10, retries: int = 3
) -> tuple[list[PracticeCoords], list[PracticeAdd]]:
    return asyncio.run(_batch_geocode_async(practice_list, api_key, concurrent, retries))
