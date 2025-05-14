"""Database connection management for QOF visualization."""

from __future__ import annotations

import atexit
from pathlib import Path
from typing import Final

import duckdb
import polars as pl


class DatabaseConnection:
    """Manages database connection and caching for QOF visualization."""

    def __init__(self, db_path: Path) -> None:
        """Initialize database connection."""
        self.conn: duckdb.DuckDBPyConnection | None = duckdb.connect(":memory:")

        # Connect to database in read-only mode to avoid locking issues
        self.conn.execute(f"ATTACH DATABASE '{db_path}' AS source_db (READ_ONLY)")

        # Register cleanup handler
        atexit.register(self.cleanup)

    def query_df(self, sql: str) -> pl.DataFrame:
        """Execute query and return results as a Polars DataFrame."""
        if self.conn is None:
            raise ValueError("Database connection is closed")

        result = self.conn.execute(sql).arrow()
        return pl.from_arrow(result)

    def cleanup(self) -> None:
        """Close database connection."""
        if hasattr(self, "conn") and self.conn is not None:
            self.conn.close()
            self.conn = None


# Global connection instance
DB_PATH: Final = Path(__file__).parent.parent.parent / "qof_vis.db"
db = DatabaseConnection(DB_PATH)


def query(sql: str) -> pl.DataFrame:
    """Global query function that uses the shared connection."""
    return db.query_df(sql)
