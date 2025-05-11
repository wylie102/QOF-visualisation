from pathlib import Path

import duckdb

conn = duckdb.connect()
sources: Path = Path("./sources/").resolve()

str_address_list = str(sources / "gp_location_info.parquet")
str_lat_lon = str(sources / "full_practice_coordinates_16527.parquet")

lat_lon = conn.read_parquet(str_lat_lon)
address_list = conn.read_parquet(str_address_list)

coords = address_list.join(lat_lon, "practice_code")
coords.show()
coords.filter("lat is null").show()
lat_lon.filter("lat is null").to_parquet("missing_coords")
