"""Test script to query the qof_vis.db database using the MCP server."""

import duckdb

# Connect to the database
conn = duckdb.connect()
conn.execute("ATTACH DATABASE '/Users/wylie/Desktop/Projects/QOF_visualisation/qof_vis.db' AS qof")

# List all tables in the database
print("Tables in qof_vis.db:")
tables = conn.execute(
    "SELECT table_name FROM qof.information_schema.tables WHERE table_schema='main'"
).fetchall()
for table in tables:
    print(f"- {table[0]}")

# Test queries for practice and national achievement
print("\nTesting practice achievement query:")
result = conn.execute("""
    SELECT COUNT(*) FROM qof.fct__practice_achievement
""").fetchone()
print(f"Number of practice achievement records: {result[0]}")

print("\nTesting national achievement query:")
result = conn.execute("""
    SELECT COUNT(*) FROM qof.fct__national_achievement
""").fetchone()
print(f"Number of national achievement records: {result[0]}")

print("\nSample of practice data:")
samples = conn.execute("""
    SELECT organisation_name, indicator_code, percentage_patients_achieved, reporting_year 
    FROM qof.fct__practice_achievement 
    LIMIT 5
""").fetchall()
for row in samples:
    print(f"- {row[0]} | {row[1]} | {row[2]}% | {row[3]}")

print("\nSample of available org levels:")
orgs = conn.execute("""
    WITH org_levels AS (
        SELECT 'Practice' as level, COUNT(*) as count FROM qof.fct__practice_achievement
        UNION ALL
        SELECT 'PCN' as level, COUNT(*) as count FROM qof.fct__pcn_achievement
        UNION ALL
        SELECT 'Sub-ICB' as level, COUNT(*) as count FROM qof.fct__sub_icb_achievement
        UNION ALL
        SELECT 'ICB' as level, COUNT(*) as count FROM qof.fct__icb_achievement
        UNION ALL
        SELECT 'Region' as level, COUNT(*) as count FROM qof.fct__region_achievement
    )
    SELECT * FROM org_levels
""").fetchall()
for row in orgs:
    print(f"- {row[0]}: {row[1]} records")

conn.close()
