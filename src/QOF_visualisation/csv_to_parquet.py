#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

"""
For each directory in the list, load all files that match the passed in pattern into one parquet file.
Files are saved in the same directory as their parent folder with lower case file names.
Returns a list of the parquet file Path objects.
"""

from pathlib import Path

from duckdb import DuckDBPyConnection, DuckDBPyRelation


def csv_to_parquet(
    conn: DuckDBPyConnection,
    csv_dir: Path,
    file_pattern: str,
    table_name: str | None = None,
) -> Path:
    """
    For each directory in the list, load all files that match the passed in pattern into one parquet file.

    Files are saved in the same directory as their parent folder with lower case file names.
    Returns a list of the parquet file Path objects.
    """
    # Set table and view names.
    if not table_name:
        table_name = csv_dir.stem.lower()
    view_name: str = table_name + "_view"
    parquet_path: Path = csv_dir.parents[0] / (table_name + ".parquet")

    # Create view of data and create relation of it.
    conn.read_csv(str(csv_dir / file_pattern)).create_view(view_name)
    print(f"Created {parquet_path.name}")
    view: DuckDBPyRelation = conn.view(view_name)

    view.to_parquet(str(parquet_path))

    return parquet_path


if __name__ == "__main__":
    pass
