#!/usr/bin/env -S uv run --script

import io
import os
import shutil
import zipfile
from pathlib import Path

import duckdb
import requests
from dotenv import load_dotenv
from duckdb import DuckDBPyConnection
from requests.adapters import HTTPAdapter
from requests.models import Response
from urllib3.util.retry import Retry

from QOF_visualisation.csv_to_parquet import csv_to_parquet

# Dict of QOF year csv zip urls.
source_url_dict: dict[str, str] = {
    "achievement_2019_2020": "https://files.digital.nhs.uk/E2/BF16AA/QOF_1920.zip",
    "achievement_2020_2021": "https://files.digital.nhs.uk/AC/3C964F/QOF2021_v2.zip",
    "achievement_2021_2022": "https://files.digital.nhs.uk/90/6F833F/QOF_2122_V2.zip",
    "achievement_2022_2023": "https://files.digital.nhs.uk/32/40B931/QOF%202022-23%20Raw%20data%20.csv%20files.zip",
    "achievement_2023_2024": "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip",
    "reference_set": "https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/data-collections/qof/primary-care-domain-reference-set-portal/20240531_pcd_refset_content_text_files.zip",
    "location_info": "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip",
}

pattern_dict = {
    "achievement": "ACHIEVEMENT_*.csv",
    "nhs_organisations": "MAPPING_NHS_GEOGRAPHIES_*.csv",
    "qof_indicators": "MAPPING_INDICATORS_*.csv",
    "reference_set": "20241205_PCD_Output_Descriptions.csv",
    "location_info": "epraccur.csv",
}


def get_with_retry(url: str, retries: int = 5, backoff: float = 0.5) -> requests.Response:
    """Download files using requests and a sequential backoff to avoid timeout/DNS issues."""
    session: requests.Session = requests.Session()
    retry: Retry = Retry(
        total=retries,
        backoff_factor=backoff,  # Waits: 0.5s, 1s, 2s, etc.
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter: HTTPAdapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session.get(url, timeout=10)


def download_and_extract_zip(target_dir: Path, url: str) -> Path:
    """Dowload and extract a zip folder to the target directory."""
    resp: Response = get_with_retry(url)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        if target_dir.exists():
            if target_dir.is_file():
                target_dir.unlink()
            else:
                shutil.rmtree(target_dir)
        z.extractall(target_dir)

    # Change .txt files to csv files.
    for txt_file in target_dir.rglob("*.txt"):
        txt_file.rename(txt_file.with_suffix(".csv"))

    # Re-encode CSV files to UTF-8
    for csv_file in target_dir.rglob("*.csv"):
        csv_content: str = csv_file.read_text(encoding="latin1")
        csv_file.write_text(csv_content, encoding="utf-8")

    return target_dir


def assign_target_directory() -> Path:
    """
    Assign the target directory.

    Default is ./data created in the current working directory.
    A target directory string can be passed in from a .env file.
    The string should be assigned to TARGET_DIRECTORY
    """
    # Load from local env.
    load_dotenv()

    # Set target dir.
    env_dir: str = os.getenv("TARGET_DIRECTORY", "")
    if env_dir:
        dir_path: Path = Path(env_dir).resolve()
    else:
        dir_path = Path("./sources").resolve()
    dir_path.mkdir(parents=True, exist_ok=True)
    print(f"Target Directory: {dir_path}")
    return dir_path


def main() -> None:
    """Main function for get_sources.py"""

    # Create ducbdk connection and assign target directory.
    target_dir: Path = assign_target_directory()
    conn: DuckDBPyConnection = duckdb.connect()

    # Download sources and store csv paths in a dict.
    source_csv_dict: dict[str, Path] = {
        name: download_and_extract_zip(target_dir / name, url)
        for name, url in source_url_dict.items()
    }

    # Initialize parquet dict.
    souce_parquet_dict: dict[str, Path] = {}

    # Create qof dir list then convert required files to parquet.
    qof_name_list: list[str] = [
        name for name in source_csv_dict.keys() if name.startswith("achievement")
    ]
    for name in qof_name_list:
        # Convert achievement files.
        souce_parquet_dict[name] = csv_to_parquet(
            conn, source_csv_dict[name], pattern_dict["achievement"]
        )

        # Convert nhs organisation files.
        nhs_organisations_name = "structures_" + name[-9:]
        souce_parquet_dict[nhs_organisations_name] = csv_to_parquet(
            conn, source_csv_dict[name], pattern_dict["nhs_organisations"], nhs_organisations_name
        )

        # Convert qof indicator files.
        qof_indicators_name = "qof_indicators_" + name[-9:]
        souce_parquet_dict[qof_indicators_name] = csv_to_parquet(
            conn, source_csv_dict[name], pattern_dict["qof_indicators"], qof_indicators_name
        )

    # Convert pcd_reference_set file.
    souce_parquet_dict["reference_set"] = csv_to_parquet(
        conn, source_csv_dict["reference_set"], pattern_dict["reference_set"]
    )

    # Convert gp_location_info file.
    souce_parquet_dict["location_info"] = csv_to_parquet(
        conn, source_csv_dict["location_info"], pattern_dict["location_info"]
    )

    # Clean up csv files.
    for path in source_csv_dict.values():
        shutil.rmtree(path)


if __name__ == "__main__":
    main()
