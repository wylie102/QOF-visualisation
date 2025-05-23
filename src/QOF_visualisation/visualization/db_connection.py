"""Database connection management for QOF visualization."""

from __future__ import annotations

import atexit
from pathlib import Path
from typing import Final

import duckdb
import polars as pl


class DatabaseConnection:
    """Manages database connection and caching for QOF visualization.

    This class provides a single connection point for querying the QOF database.
    It sets up an in-memory connection and attaches the specified database file
    in read-only mode.

    Attributes:
        conn: The DuckDB connection, initialized in memory
    """

    conn: duckdb.DuckDBPyConnection  # Type annotation at class level

    def __init__(self, db_path: Path) -> None:
        """Initialize database connection.

        Args:
            db_path: Path to the DuckDB database file

        Raises:
            RuntimeError: If unable to connect to database
        """
        # Connect to in-memory database
        connection = duckdb.connect(":memory:")
        if not connection:
            raise RuntimeError("Failed to connect to in-memory database")

        # Store connection and attach database
        self.conn = connection
        self.conn.execute(f"ATTACH DATABASE '{db_path}' AS qof_vis (READ_ONLY)")

        # Create materialized views for frequently used queries
        self._create_materialized_views()

        # Register cleanup handler
        atexit.register(self.cleanup)

    def _create_materialized_views(self) -> None:
        """Cache commonly used data in memory for better performance."""
        # Cache national averages
        self.conn.execute("""
            CREATE TABLE national_averages AS 
            SELECT 
                reporting_year,
                group_description,
                AVG(percentage_patients_achieved) as avg_achievement
            FROM qof_vis.fct__national_achievement
            WHERE percentage_patients_achieved IS NOT NULL
            GROUP BY reporting_year, group_description
        """)

    def query_df(self, sql: str) -> pl.DataFrame:
        """Execute query and return results as a Polars DataFrame.

        Args:
            sql: SQL query string to execute

        Returns:
            A Polars DataFrame containing the query results

        Raises:
            RuntimeError: If database connection is closed
        """
        # Execute query and get result as Arrow table
        result = self.conn.execute(sql).fetch_arrow_table()

        # DuckDB's fetch_arrow_table() returns a more predictable type
        # that pl.from_arrow can handle without ambiguity
        return pl.DataFrame(result)

    def cleanup(self) -> None:
        """Close database connection.

        This method is automatically called during program exit
        to clean up database resources.
        """
        try:
            if hasattr(self, "conn"):
                self.conn.close()
        except Exception:
            pass  # Ignore errors during cleanup


# Global connection instance
DB_PATH: Final = Path(__file__).parent.parent.parent.parent / "qof_vis.db"
db = DatabaseConnection(DB_PATH)


def query(sql: str) -> pl.DataFrame:
    """Global query function that uses the shared connection."""
    return db.query_df(sql)
