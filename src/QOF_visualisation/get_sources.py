import io
import os
import shutil
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Dict of QOF year csv zip urls.
raw_qof_url_dict: dict[str, str] = {
    "QOF_2019_2020": "https://files.digital.nhs.uk/E2/BF16AA/QOF_1920.zip",
    "QOF_2020_2021": "https://files.digital.nhs.uk/AC/3C964F/QOF2021_v2.zip",
    "QOF_2021_2022": "https://files.digital.nhs.uk/90/6F833F/QOF_2122_V2.zip",
    "QOF_2022_2023": "https://files.digital.nhs.uk/32/40B931/QOF%202022-23%20Raw%20data%20.csv%20files.zip",
    "QOF_2023_2024": "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip",
}

# Dict of supplementary data.
supplementary_url_dict: dict[str, str] = {
    "PCD_reference_set": "https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/data-collections/qof/primary-care-domain-reference-set-portal/20240531_pcd_refset_content_text_files.zip",
    "epraccur": "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip",
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


def download_and_extract_zip(url: str, target_dir: Path) -> Path:
    """Dowload and extract a zip folder to the target directory."""
    print(f"Downloading: {url}")
    resp = get_with_retry(url)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        if target_dir.exists():
            if target_dir.is_file():
                target_dir.unlink()
            else:
                shutil.rmtree(target_dir)
        z.extractall(target_dir)

    # Re-encode CSV files to UTF-8
    for csv_file in target_dir.rglob("*.csv"):
        content: str = csv_file.read_text(encoding="latin1")
        csv_file.write_text(content, encoding="utf-8")

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


def get_sources() -> list[dict[str, Path]]:
    """
    Download and unzip QOF and supplementary sources.
    Sources currently include QOF practice level data from years 2019/2020 - 2023/2024.

    By default all files will be downloaded to a "sources" directory in the current working directory.
    You can also pass in a file string by storing it in a .env file in the root directory,
    assigned to the variable TARGET_DIRECTORY.
    e.g. TARGET_DIRECTORY = "./sources"
    """

    target_dir: Path = assign_target_directory()
    # Download each qof year to a named directory within the target directory.
    # Store paths in qof_path_dict
    qof_path_dict: dict[str, Path] = {}
    for qof_year, url in raw_qof_url_dict.items():
        qof_year_path: Path = download_and_extract_zip(url=url, target_dir=target_dir / qof_year)
        qof_path_dict[qof_year] = qof_year_path

    # Download each supplementary folder to a named directory within the target directory.
    # Store paths in supplementary_path_dict
    supplementary_path_dict: dict[str, Path] = {}
    for supplementary_file, url in supplementary_url_dict.items():
        supplementary_file_path: Path = download_and_extract_zip(
            url=url, target_dir=target_dir / supplementary_file
        )
        supplementary_path_dict[supplementary_file] = supplementary_file_path

    return [qof_path_dict, supplementary_path_dict]


def main() -> None:
    """Main function for get_sources.py"""
    source_dict_list: list[dict[str, Path]] = get_sources()
    for source_dict in source_dict_list:
        for name, path in source_dict.items():
            print(f"{name} downloaded to {path}.")


if __name__ == "__main__":
    main()
