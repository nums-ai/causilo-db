# Changelog

## 0.2.0

- A Snowflake Native App (`snowflake/native_app/`) with `CORE.PREDICT`, the
  Snowflake procedure packaged so that an account installs it as an
  application. `scripts/build_snowflake_app.py` generates it from the core and
  `scripts/deploy_snowflake_app.py` installs it in a test account and checks it
  on the example tables.
- A test fails when `sql/snowflake/install.sql` or the app's files are out of
  date with `causilo_db/core.py`.
- `sql/postgres/install_http.sql`: `causilo.predict` in PL/pgSQL on the `http`
  extension (pgsql-http 1.6 or later), for hosted PostgreSQL that allows that
  extension but not PL/Python, such as Supabase. Same arguments and result
  columns as the PL/Python function; the README lists where it differs.
- `docker/postgres-http.Dockerfile`, a PostgreSQL 17 with pgsql-http 1.6.1 for
  trying it.
- `scripts/live_check.py --function` checks an installed `causilo.predict`.
- The README's guidance on hosted PostgreSQL now names the services that allow
  each installer.
- A BigQuery remote function, `causilo_predict` and
  `causilo_predict_with_options`, backed by a Cloud Run service you deploy in
  your own project (`bigquery/`, with `deploy.sh` and `teardown.sh`). It takes
  table names, reads the table with its own service account, writes the result
  to a new table and returns a one-line summary. A call that BigQuery repeats
  or retries is predicted once.
- `scripts/bigquery_live_check.py` repeats the live check through the
  function.
- When a call fails after earlier calls of the same prediction completed,
  `predict_frame` raises `PartialPredictionError` (a `CausiloDBError`), which
  says how many calls were charged, counting a call whose answer came back but
  could not be used, even the first; the PL/pgSQL function and the Snowflake
  procedures say the same in their error. The BigQuery service keeps the
  output table and does not ask BigQuery to retry such a call.
- BigQuery: DATE columns are sent as seconds since 1970, as elsewhere; the id
  column is required; id and target names a load job could not write are
  refused before anything is paid for; the call fingerprint includes the
  source's last-modified time and a view's definition; dataset names in
  `ALLOWED_DATASETS` are compared case-sensitively; `teardown.sh` deletes only
  what `deploy.sh` created.
- `install_http.sql`: a call stopped by `statement_timeout` or a client cancel
  is cancelled at the API.

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
