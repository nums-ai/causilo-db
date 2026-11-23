# causilo-db

causilo-db predicts the blank cells of a database column. Rows where the
column has a value are the examples, rows where it is blank are predicted, and
the predictions come back as a table. The source table is never modified.

The predictions are made by Causilo, the hosted tabular prediction API from
Nums AI (`https://api.nums.world`, documentation at
[docs.nums.world](https://docs.nums.world)). You need your own Causilo API key,
and every call counts against that key's account.

What leaves your database: the target and feature columns of the blank rows
and of up to 50,000 filled rows, sent over HTTPS. The id column is not sent.
`features` and `where` narrow what is read. causilo-db stores neither the data
nor the key.

Requires Python 3.11 or later. The code is under the Apache License 2.0; the
two example tables keep their own licences (CC BY 4.0 and CC0).

| Use it from | Call |
|---|---|
| Any database SQLAlchemy reaches | `causilo-db predict <url> <table> <column>` or `causilo_db.predict_table(...)` |
| PostgreSQL, inside the database | `SELECT * FROM causilo.predict('customers', 'churn', 'customer_id')` |
| DuckDB | `causilo_db.duckdb.predict(con, 'customers', 'churn')`, returns a relation |
| Spark, Databricks | `causilo_db.spark.predict_table(spark, 'main.sales.customers', 'churn', ...)` |
| Snowflake, inside the warehouse | `CALL CAUSILO.PREDICT('SALES.CUSTOMERS', 'CHURN', 'CUSTOMER_ID')` |

The SQL installers, the Databricks notebook and the scripts are not installed by
`pip install`; they are in the [GitHub repository](https://github.com/nums-ai/causilo-db)
and in the source archive on PyPI. Clone the repository, or download the one
file you need. What has been run against the live API is under [Tested](#tested).

## The result

One row per blank cell, with the id column you name (or `row_number`, the
0-based position in what was read; `ctid` in the PostgreSQL function):

- a number column (regression): the target column holds the predicted value
  (the median), and `<column>_p25`, `<column>_p50`, `<column>_p75` hold the
  predicted quartiles; the true value should fall between p25 and p75 about
  half the time;
- anything else (classification): the most likely class,
  `<column>_probability` for it, and `<column>_prob_<class>` for every class
  (characters other than letters, digits and `_` in the class become `_`, names
  are cut to 56 characters, and names that would clash get a number, so every
  database takes them).

The PostgreSQL function returns fixed columns instead: `row_id`, `prediction`
(as text), `probability`, `p25`, `p50`, `p75`. Snowflake returns the names
above in upper case.

A numeric column with 20 or fewer distinct whole values is read as classes;
pass `task='reg'` or `--task reg` to say otherwise (the Snowflake procedure has
no such argument). A text column with more than 200 distinct values is refused
unless you pass `task='clf'`, since it is more likely a name than a class.
DECIMAL columns are read as numbers, and dates and timestamps are sent as
seconds since 1970.

Every column except the target and the id is a feature unless you pass
`features`. The PostgreSQL function and the Snowflake procedure take no
`features` or `where`; point them at a view to choose columns or rows. The
filled rows are sampled down to at most 50,000, fewer for a wide table so that
each call stays under 7,000,000 cells, and a large table is sent in several
calls.

## API key

causilo-db never writes the key into a table or a file:

- command line and Python: `CAUSILO_API_KEY` in the environment, or `api_key=`;
- PostgreSQL function: `SET causilo.api_key = 'csl_...';` for the session. The
  statement is ordinary SQL, so it shows in `pg_stat_activity` and, with
  `log_statement` on, in the server log. `CAUSILO_API_KEY` in the server's
  environment works too, and every role that can call the function then spends
  that key;
- Snowflake: a Snowflake secret you set once;
- Databricks: a secret scope, `dbutils.secrets.get("causilo", "api_key")`.

## Python

```python
from causilo_db import predict_table

result, pred = predict_table("postgresql+psycopg://user@host/shop", "customers", "churn",
                             id_column="customer_id", out_table="customers_churn_prediction")
print(pred.task, pred.n_query, pred.n_context, pred.notes)
```

`result` is a pandas DataFrame and `pred` describes what was sent.
`causilo_db.predict_frame(df, "churn")` does the same for a DataFrame you
already have.

## Command line

```bash
pip install "causilo-db[postgres]"   # or [mysql], [mssql], [oracle], [clickhouse], [trino], [redshift], [bigquery], [snowflake]
export CAUSILO_API_KEY=csl_...
causilo-db predict postgresql+psycopg://user@host/shop customers churn \
    --id customer_id --out customers_churn_prediction
```

The URL names the driver that the extra installs:

| Extra | URL |
|---|---|
| `postgres` | `postgresql+psycopg://user:password@host/db` |
| `mysql` | `mysql+pymysql://user:password@host/db` |
| `mssql` | `mssql+pymssql://user:password@host/db` |
| `oracle` | `oracle+oracledb://user:password@host/?service_name=FREEPDB1` |
| `clickhouse` | `clickhouse+native://user:password@host/db` |
| `trino` | `trino://user@host:8080/catalog/schema` |
| `redshift` | `redshift+redshift_connector://user:password@host:5439/db` |
| `bigquery` | `bigquery://project/dataset` |
| `snowflake` | `snowflake://user:password@account/db/schema?warehouse=wh` |

A password in the URL shows in shell history and the process list; where the
driver has its own password file or environment variables (`~/.pgpass`,
`PGPASSWORD`, `~/.my.cnf`), prefer those.

Without `--out` the result is printed as CSV. `--where "region = 'KR'"` reads
part of a table, `--features a,b,c` picks the features, `--schema` names the
schema, `--out-schema` the schema of the result, `--if-exists replace`
overwrites an earlier result and `--if-exists append` adds to it. If the
prediction succeeds but the table cannot be written, the result is printed as
CSV instead, so nothing that was paid for is lost.

The result table is written with types each database accepts: Oracle gets
`BINARY_DOUBLE` and `VARCHAR2`, ClickHouse a `MergeTree` table, on Trino
names such as `trim` are quoted, and on Redshift and BigQuery rows go in batches
of one multi-row INSERT rather than one statement each.

## PostgreSQL function

On a server where you can install PL/Python (`plpython3u`) and the
`causilo-db` package into the Python the server runs:

```sql
\i sql/postgres/install.sql   -- from a clone of this repository, as a superuser
SET causilo.api_key = 'csl_...';
SELECT * FROM causilo.predict('concrete', 'strength', 'mix_id');
SELECT * FROM causilo.predict('orders', 'units', 'order_id', task => 'reg');
CREATE TABLE churn_scores AS SELECT * FROM causilo.predict('customers', 'churn', 'customer_id');
```

The function reads the table as the role that calls it. A view works when an id
column is given; without one, `row_id` is the row's `ctid`, which changes when
the row is updated. `docker/postgres.Dockerfile` builds a PostgreSQL 16 with
PL/Python and causilo-db, for trying it locally.

Managed PostgreSQL (RDS, Cloud SQL, Azure) does not allow `plpython3u`; run the
command line from outside instead.

## DuckDB

```python
# pip install "causilo-db[duckdb]"
import duckdb, causilo_db.duckdb as cx

con = duckdb.connect("shop.duckdb")
rel = cx.predict(con, "customers", "churn", id_column="customer_id")
rel.create("customers_churn_prediction")
```

## Spark and Databricks

```python
# pip install "causilo-db[spark]" (Databricks already has Spark: pip install causilo-db)
from causilo_db.spark import predict, predict_table

out = predict(spark.table("main.sales.customers"), "churn", id_column="customer_id")
predict_table(spark, "main.sales.customers", "churn", id_column="customer_id",
              output_table="main.sales.customers_churn_prediction",
              api_key=dbutils.secrets.get("causilo", "api_key"))
```

The table stays in Spark. Only the rows to predict and a sample of at most
50,000 filled rows, drawn inside Spark, reach the driver. Past 1,000,000 rows to
predict it stops and asks you to filter first (`max_query_rows=` raises it).
Without `id_column` the result is the blank rows themselves with the target
filled in and the other prediction columns after; the rows pass through pandas,
so an integer column with blanks comes back as a double. `predict_table` refuses
an existing `output_table` before predicting unless you pass
`mode='overwrite'` or `mode='append'`.

`notebooks/databricks_predict.py` is the same as a Databricks notebook. It needs:

- Python 3.11 or later. It was run on Databricks Runtime 16.4 LTS; check the
  Python version of other runtimes and serverless environments first;
- outbound HTTPS to `api.nums.world`. Classic compute has it. Serverless compute
  on Free Edition and on trial workspaces reaches only a fixed list of hosts,
  and opening another host takes a serverless network policy.

## Snowflake procedure

`sql/snowflake/install.sql` creates a network rule for `api.nums.world`, a
secret for the key, an external access integration and the procedure, in a
`CAUSILO` schema of the current database (`USE DATABASE ...` first). Run the
file as a role that can create integrations (ACCOUNTADMIN or one granted it),
then put your key in the secret. Running the file again later keeps the key.

```sql
ALTER SECRET CAUSILO.API_KEY SET SECRET_STRING = 'csl_...';
CALL CAUSILO.PREDICT('SALES.CUSTOMERS', 'CHURN', 'CUSTOMER_ID');
CREATE TABLE SALES.CHURN_PREDICTION AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

The procedure carries `causilo_db/core.py` inline and takes `causilo-client`
from PyPI through Snowflake's shared artifact repository, so nothing is uploaded
to a stage. `scripts/build_snowflake_sql.py` regenerates the file after a change
to the core. Trial accounts refuse external access integrations, so the account
needs billing set up first.

The procedure runs with the caller's rights, so it reads only the tables the
calling role can read. Every role granted USAGE on it spends the key in the
secret.

## Tested

Run against the live API in November 2026 with the two
[example tables](https://github.com/nums-ai/causilo-db/blob/main/examples/README.md),
part of the answers hidden. On the command-line rows the predictions were
written to a table with `--out`, read back and scored against the hidden
answers (`scripts/live_check.py` does this); on the other rows the returned
result was scored.

| Database | Path | Regression, concrete strength (229 cells) | Classification, car trim (385 cells) |
|---|---|---|---|
| PostgreSQL 16 | command line | R² 0.939 | accuracy 73.5% |
| PostgreSQL 16 | `causilo.predict` function | R² 0.939 | accuracy 73.5% |
| MySQL 8.4 | command line | R² 0.939 | accuracy 73.5% |
| MariaDB 11 | command line | R² 0.939 | accuracy 73.5% |
| SQL Server 2022 | command line | R² 0.939 | accuracy 73.5% |
| Oracle 23ai Free | command line | R² 0.939 | accuracy 73.5% |
| ClickHouse | command line | R² 0.939 | accuracy 73.5% |
| Trino (memory catalog) | command line | R² 0.939 | accuracy 73.5% |
| Redshift Serverless | command line | R² 0.939 | accuracy 73.5% |
| BigQuery | command line | R² 0.939 | accuracy 73.5% |
| Snowflake | command line | R² 0.939 | accuracy 73.5% |
| Snowflake | `CALL CAUSILO.PREDICT` procedure | R² 0.939 | accuracy 73.5% |
| DuckDB | `causilo_db.duckdb` | R² 0.939 | accuracy 73.5% |
| Spark 4.3, local | `causilo_db.spark` | R² 0.939 | accuracy 73.8% |
| Databricks, classic single-node cluster (Runtime 16.4 LTS) | `causilo_db.spark` | R² 0.939 | accuracy 73.5% |

The table shows that every path returns the same answer, not that the car
trim is easy to predict: always guessing the most common class ("lounge") is
right on 72.5% of the hidden rows, so 73.5% is about four rows better. The
regression result is the meaningful one.

Where the same rows went in, the same numbers came out on fourteen paths across
twelve databases. The small differences elsewhere come from the rows the model
saw: a table read without ORDER BY can come back in another order (a second
Redshift run gave 73.2%), and local Spark ran on its own split (73.8%). Spark
also predicted 1,000 cells of a 300,000-row synthetic table (R² 0.997),
sampling the filled rows inside Spark.

On Databricks the result was checked as a DataFrame; writing it with
`predict_table(..., output_table=)` has not been run there.

`scripts/live_check.py <url>` repeats the database check on any SQLAlchemy URL.

## Tests

Python 3.11 or later:

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest -q
```

They use a fake client and SQLite, DuckDB and a local Spark session (skipped
without pyspark or Java); nothing reaches the network, even with
`CAUSILO_API_KEY` set. `scripts/live_check.py` calls the real API and is
charged like any other call.

See also the [changelog](https://github.com/nums-ai/causilo-db/blob/main/CHANGELOG.md)
and the [security policy](https://github.com/nums-ai/causilo-db/blob/main/SECURITY.md).
