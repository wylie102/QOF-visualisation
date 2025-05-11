from typing import NamedTuple


class PracticeCoords(NamedTuple):
    practice_code: str
    lat: float
    lon: float


class PracticeAdd(NamedTuple):
    practice_code: str
    short_addr: str
    long_addr: str
