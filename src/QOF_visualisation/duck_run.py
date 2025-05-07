import duckdb

conn = duckdb.connect()
_ = conn.sql(".read qof_transformation.sql")
