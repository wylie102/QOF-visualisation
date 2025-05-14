"""Test script to create a long organisation achievement model using direct DuckDB query."""

import duckdb
import os

# Get current directory for working with relative paths
cwd = os.getcwd()
print(f"Current working directory: {cwd}")

# Connect directly to the database
db_path = os.path.join(cwd, "qof_vis.db")
print(f"Database path: {db_path}")

try:
    # Create a new connection with read_only=True to avoid locking
    conn = duckdb.connect(db_path, read_only=True)

    print("\nVerifying tables exist:")
    tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    print(f"Found {len(tables)} tables")
    for i, table in enumerate(tables[:10]):  # Just show first 10
        print(f"- {table[0]}")

    print("\nCreating long organisation achievement model...")
    # Create the long organisation achievement model
    query = """
    WITH practice_ach AS (
        SELECT
            organisation_code,
            organisation_name,
            indicator_code,
            group_description,
            reporting_year,
            AVG(percentage_patients_achieved) as avg_achievement,
            'Practice' as level
        FROM fct__practice_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    ),
    pcn_ach AS (
        SELECT
            organisation_code,
            organisation_name,
            indicator_code,
            group_description,
            reporting_year,
            AVG(percentage_patients_achieved) as avg_achievement,
            'PCN' as level
        FROM fct__pcn_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    ),
    sub_icb_ach AS (
        SELECT
            organisation_code,
            organisation_name,
            indicator_code,
            group_description,
            reporting_year,
            AVG(percentage_patients_achieved) as avg_achievement,
            'Sub-ICB' as level
        FROM fct__sub_icb_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    ),
    icb_ach AS (
        SELECT
            organisation_code,
            organisation_name,
            indicator_code,
            group_description,
            reporting_year,
            AVG(percentage_patients_achieved) as avg_achievement,
            'ICB' as level
        FROM fct__icb_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    ),
    region_ach AS (
        SELECT
            organisation_code,
            organisation_name,
            indicator_code,
            group_description,
            reporting_year,
            AVG(percentage_patients_achieved) as avg_achievement,
            'Region' as level
        FROM fct__region_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    ),
    national_ach AS (
        SELECT
            'National' as organisation_code,
            'National Average' as organisation_name,
            indicator_code,
            group_description,
            reporting_year,
            AVG(percentage_patients_achieved) as avg_achievement,
            'National' as level
        FROM fct__national_achievement
        WHERE percentage_patients_achieved IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT * FROM practice_ach
    UNION ALL
    SELECT * FROM pcn_ach
    UNION ALL
    SELECT * FROM sub_icb_ach
    UNION ALL
    SELECT * FROM icb_ach
    UNION ALL
    SELECT * FROM region_ach
    UNION ALL
    SELECT * FROM national_ach
    """

    # Save the results to an in-memory view for testing
    conn.execute("CREATE OR REPLACE VIEW v_long_achievement AS " + query)

    # Show sample results
    result = conn.execute(
        "SELECT level, COUNT(*) as count FROM v_long_achievement GROUP BY level"
    ).fetchall()
    print("\nLong model record counts by level:")
    for row in result:
        print(f"- {row[0]}: {row[1]} records")

    print("\nSample records:")
    samples = conn.execute("SELECT * FROM v_long_achievement LIMIT 5").fetchall()
    for row in samples:
        print(f"- {row[1]} ({row[0]}) | {row[2]} | {row[4]} | {row[5]}%")

    conn.close()
    print("\nDone! Created v_long_achievement view successfully.")

except Exception as e:
    print(f"Error: {e}")
