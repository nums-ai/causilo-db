# Changelog

## 0.1.0

First release.

- `causilo-db predict` command line and `causilo_db.predict_table` for any
  SQLAlchemy database, with extras for PostgreSQL, MySQL and MariaDB, SQL
  Server, Oracle, ClickHouse, Trino, Redshift, BigQuery and Snowflake.
- `causilo_db.duckdb.predict`, and `causilo_db.spark.predict` and
  `predict_table` for Spark and Databricks.
- `causilo.predict`, a PL/Python function for PostgreSQL
  (`sql/postgres/install.sql`).
- `CAUSILO.PREDICT`, a stored procedure for Snowflake
  (`sql/snowflake/install.sql`).
- A Databricks notebook (`notebooks/databricks_predict.py`).
- The id column, `if_exists`, the output table and the target are checked
  before any call is made, so a mistake costs nothing.
