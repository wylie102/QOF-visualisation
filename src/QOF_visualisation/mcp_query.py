"""MCP server query test script."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent.parent.parent / "qof_vis.db"


# Test queries
def test_queries() -> None:
    """Test our visualization queries using MCP server."""
    conn = duckdb.connect(DB_PATH.as_posix())

    # List all tables
    print("\nAvailable tables:")
    result = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    for row in result:
        print(f"- {row[0]}")

    # Test a few sample queries
    print("\nTesting practice achievement query:")
    result = conn.execute("""
        SELECT COUNT(*) 
        FROM source_db.fct__practice_achievement
    """).fetchone()
    print(f"Number of practice achievement records: {result[0]}")

    print("\nTesting national achievement query:")
    result = conn.execute("""
        SELECT COUNT(*) 
        FROM source_db.fct__national_achievement
    """).fetchone()
    print(f"Number of national achievement records: {result[0]}")


if __name__ == "__main__":
    test_queries()
