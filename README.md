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
| PostgreSQL, inside the database (PL/Python, or the `http` extension) | `SELECT * FROM causilo.predict('customers', 'churn', 'customer_id')` |
| DuckDB | `causilo_db.duckdb.predict(con, 'customers', 'churn')`, returns a relation |
| Spark, Databricks | `causilo_db.spark.predict_table(spark, 'main.sales.customers', 'churn', ...)` |
| Snowflake, inside the warehouse | `CALL CAUSILO.PREDICT('SALES.CUSTOMERS', 'CHURN', 'CUSTOMER_ID')` |
| Snowflake, as a Native App | `CALL CAUSILO.CORE.PREDICT('SALES.PUBLIC.CUSTOMERS', 'CHURN', 'CUSTOMER_ID')` |
| BigQuery, inside the warehouse | `SELECT ds.causilo_predict('p.ds.customers', 'churn', 'customer_id', 'p.ds.churn_pred')` |

The SQL installers, the Snowflake Native App, the BigQuery service, the
Databricks notebook and the scripts are not installed by `pip install`; they are in the [GitHub repository](https://github.com/nums-ai/causilo-db)
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

Each of those calls is charged when it completes. If a later call fails, the
predictions of the calls before it are not returned, and the error says how
many calls had completed and been charged (in Python,
`causilo_db.PartialPredictionError`, with `calls_completed`, raised from the
original error); running again pays for them again. A call that was answered
but whose answer cannot be used counts among the charged calls, even when it
is the first. A first call that fails before the API answers is the original
error.

## API key

causilo-db never writes the key into a table or a file:

- command line and Python: `CAUSILO_API_KEY` in the environment, or `api_key=`;
- PostgreSQL function: `SET causilo.api_key = 'csl_...';` for the session. The
  statement is ordinary SQL, so it shows in `pg_stat_activity` and, with
  `log_statement` on, in the server log. With the PL/Python function,
  `CAUSILO_API_KEY` in the server's environment works too, and every role that
  can call the function then spends that key;
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

Most hosted PostgreSQL services do not allow `plpython3u`. Where the `http`
extension is allowed, use the function below instead; elsewhere, run the
command line from outside the database.

## PostgreSQL function without Python

`sql/postgres/install_http.sql` creates the same `causilo.predict` in PL/pgSQL
on the [`http` extension](https://github.com/pramsey/pgsql-http) (pgsql-http
1.6 or later, which can send a binary body). The function builds the request,
two Parquet tables, in SQL and calls the API itself, so nothing is installed
on the server's file system and no Python runs in the database.

```sql
CREATE EXTENSION IF NOT EXISTS http WITH SCHEMA extensions;   -- Supabase keeps extensions there
\i sql/postgres/install_http.sql
SET causilo.api_key = 'csl_...';
SELECT * FROM causilo.predict('concrete', 'strength', 'mix_id');
```

The installer refuses an `http` older than 1.6; `ALTER EXTENSION http UPDATE`
first. Two optional settings: `causilo.endpoint` (default
`https://api.nums.world`) and `causilo.timeout_ms`, how long one call may take
(default 900000, fifteen minutes, as in the Python client). Hosted services
often set a `statement_timeout` for each role; a large prediction can need
`SET statement_timeout = '20min'` for the session. When `statement_timeout`
or a cancel from the client stops the function during a call, the function
asks the API to cancel that call, waiting at most five seconds for the answer;
a call the API had already started is charged all the same. Calls that had
completed before it were charged too, and the error says how many. The database server must be
able to reach `api.nums.world` on port 443.

Where it can be used, from each provider's extension list in November 2026:

| Service | `http` extension | `plpython3u` | Use |
|---|---|---|---|
| [Supabase](https://supabase.com/docs/guides/database/extensions/http) | yes, 1.6.1 on PostgreSQL 15 and 17 ([build pins](https://github.com/supabase/postgres/blob/develop/nix/ext/versions.json); the PostgreSQL 15 build also carries 1.5.0, too old) | not listed | `install_http.sql` |
| [Crunchy Bridge](https://docs.crunchybridge.com/extensions-and-languages) | yes | [yes](https://docs.crunchybridge.com/extensions-and-languages/python), but `install.sql` also needs the causilo-db package in the server's Python | `install_http.sql` |
| [Amazon RDS for PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-extensions.html), [Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/AuroraPostgreSQL.Extensions.html) | no | no | command line |
| [Google Cloud SQL](https://cloud.google.com/sql/docs/postgres/extensions), [AlloyDB](https://cloud.google.com/alloydb/docs/reference/extensions) | no | no | command line |
| [Azure Database for PostgreSQL flexible server](https://learn.microsoft.com/en-us/azure/postgresql/extensions/concepts-extensions-versions) | no | no | command line |
| [Neon](https://neon.com/docs/extensions/pg-extensions), [Aiven](https://aiven.io/docs/products/postgresql/reference/list-of-extensions), [DigitalOcean](https://docs.digitalocean.com/products/databases/postgresql/details/supported-extensions/), [Render](https://render.com/docs/postgresql-extensions) | no | no | command line |

The cloud providers have their own routes out of SQL instead, each to their
own services rather than to any HTTPS address: `aws_lambda` on RDS and Aurora
(and `aws_ml` on Aurora) calls AWS Lambda, SageMaker and Bedrock;
`google_ml_integration` on Cloud SQL and AlloyDB registers model endpoints
that take and return JSON; `azure_ai` on Azure calls Azure OpenAI, Azure AI
services and one configured Azure Machine Learning endpoint. causilo-db has no
installer for these. Supabase's `pg_net` is asynchronous and sends only JSON,
so it cannot carry the request either.

What it does the same as `install.sql`: the arguments and the six result
columns, `row_id` from the id column or the `ctid`, the target's type deciding
regression or classification (20 distinct whole values, 200 text values), the
features and their types (an integer column with a blank goes as a float, a
boolean column with a blank as text, dates as seconds since 1970, blank
columns dropped), the class labels, the split into calls by cell count, and
every refusal before a call is made. On the two example tables it sent the
same tables and returned the same predictions as the PL/Python function, row
for row (see [Tested](#tested)).

Where it differs:

- the key comes only from `causilo.api_key`, not from `CAUSILO_API_KEY` in the
  server's environment;
- above 50,000 filled rows (fewer for a wide table) the context is sampled by
  a hash of each row's position rather than by NumPy's random draw, so a large
  table gets a different sample, and slightly different predictions, than the
  other adapters;
- the table is read more than once (counts first, then the rows sent). A
  write in between that changes the counts stops the function with "the table
  changed while it was being read"; to read one snapshot, run it inside
  `BEGIN ISOLATION LEVEL REPEATABLE READ;` ... `COMMIT;`;
- the notes the other adapters return (columns dropped, dates converted,
  context sampled) are raised as `NOTICE`s;
- array and composite values are sent in PostgreSQL's text form (`{1,2}`)
  rather than Python's (`[1, 2]`), a date or timestamp target read as classes
  keeps PostgreSQL's text form, and an id column of type `real`, `double
  precision` or `boolean` comes back as PostgreSQL prints it (`1`, `true`
  rather than `1.0`, `True`);
- a failure to connect is not retried, since the extension cannot tell
  whether the request had left; a call that times out is cancelled, as the
  Python client does, which refunds it if it had not started. HTTP failures
  are retried as the client retries them (429, 500, 503, or what the server
  marks retryable; never 502 or 504);
- the request is built in the database server's memory. A full call of
  7,000,000 numeric cells is about 60 MB of request; 20,000 rows of 301 features took
  18 seconds to build against a stand-in API, 200,000 rows of 11 features 4
  seconds.

`docker/postgres-http.Dockerfile` builds a PostgreSQL 17 with pgsql-http
1.6.1 and the function, for trying it locally.

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

## Snowflake Native App

`snowflake/native_app/` packages the same procedure as a Snowflake Native App,
so that an account installs it as an application instead of running
`install.sql`. The app requests the account-level CREATE EXTERNAL ACCESS
INTEGRATION privilege, which lets it create external access integrations in
the consumer account; it uses it to create one, to `api.nums.world` only (its
network rule allows that host and nothing else), which works once an
administrator approves the app's `CAUSILO_API` specification; the key is a sensitive configuration value of the
app, redacted from query history. The app reads with its own rights, so it sees
only the tables granted to it:

```sql
ALTER APPLICATION CAUSILO SET CONFIGURATION CAUSILO_API_KEY VALUE = 'csl_...';
GRANT USAGE ON DATABASE SALES TO APPLICATION CAUSILO;
GRANT USAGE ON SCHEMA SALES.PUBLIC TO APPLICATION CAUSILO;
GRANT SELECT ON TABLE SALES.PUBLIC.CUSTOMERS TO APPLICATION CAUSILO;
CALL CAUSILO.CORE.PREDICT('SALES.PUBLIC.CUSTOMERS', 'CHURN', 'CUSTOMER_ID');
```

The procedure takes the same arguments and returns the same columns as
`CAUSILO.PREDICT`; name the table in full. Every role granted the app's
`APP_USER` or `APP_ADMIN` role can predict from every table granted to the app
and spends the key. [`snowflake/native_app/README.md`](https://github.com/nums-ai/causilo-db/blob/main/snowflake/native_app/README.md)
is the consumer guide shown in Snowsight.

`scripts/build_snowflake_app.py` writes `manifest.yml` and `setup.sql`, with
`causilo_db/core.py` inlined. `causilo-client` is bundled as a zip on the
app's stage rather than taken from PyPI. `scripts/deploy_snowflake_app.py`
creates a test application package in your account, registers a version on its
QA release channel, installs the app from it, approves the specification, sets
the key and runs the two example tables through it, scored as under
[Tested](#tested); `--cleanup` drops what it created. It needs
`pip install snowflake-connector-python`, a role that can create application
packages, applications and databases (ACCOUNTADMIN by default), and
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` (a programmatic
access token) and `CAUSILO_API_KEY_FILE` in the environment. The app was run
this way against the live API (see [Tested](#tested)); it is not yet listed on
the Snowflake Marketplace.

## BigQuery remote function

`bigquery/` holds a small Cloud Run service that you deploy in your own GCP
project, and a BigQuery
[remote function](https://cloud.google.com/bigquery/docs/remote-functions) in
front of it. A prediction needs the whole table, while a remote function is
called row by row, so the function takes table names rather than values: the
service reads the source table with the BigQuery API, predicts, writes the
result to a new table and returns one line saying what it did.

```sql
SELECT analytics.causilo_predict(
  'my-project.analytics.customers', 'churn', 'customer_id',
  'my-project.analytics.customers_churn_prediction');
-- predicted 1,204 blank cells of churn (classification) from 48,796 rows in 1 call; wrote ...

SELECT analytics.causilo_predict_with_options(
  'my-project.analytics.orders', 'units', 'order_id', 'my-project.analytics.orders_units',
  '{"features": "price,region,week", "task": "reg"}');
```

The result table has the id column and the columns described under
[The result](#the-result). The id column is required, and a NULL is refused: a
table read through the API has no fixed row order, so a `row_number` could not
be joined back. `options` takes `features` (a list, or names separated by
commas) and `task`; for anything else, point the function at a view.

The id and target names must hold only letters, digits and `_` and not start
with a digit, and `<target>_probability` must fit in 300 characters; a load
job takes other names only where flexible column names are enabled, so such a
call is refused before anything is paid for. Rename the columns in a view.

Deploy from a clone of this repository, logged in with gcloud as someone who
can enable APIs, create service accounts and grant roles:

```bash
PROJECT=my-project REGION=us-central1 DATASET=analytics KEY_FILE=~/causilo.key \
    bigquery/deploy.sh
```

The script enables the APIs, builds the image with Cloud Build into an
Artifact Registry repository (the build runs as a service account of its own
that may only read its source bucket, push to the repository and write logs;
`BUILD=docker` builds the image locally instead), puts the key in a
Secret Manager secret, creates a service account for the service, deploys the
service with `--no-allow-unauthenticated`, creates a BigQuery connection,
lets the connection's service account invoke the service, and creates the two
functions in `DATASET`. Every name starts with `PREFIX` (default `causilo`).
A repository, secret, bucket, service account or connection of that name that
exists already is reused; what the script creates carries the label
`causilo_db=deploy` (on a service account and the connection, the description
"Created by causilo-db bigquery/deploy.sh"). `bigquery/teardown.sh` with the
same settings drops the two functions and deletes only the pieces that carry
the mark, with the grants of the service accounts it deletes; it names what it
keeps. The dataset and its tables are kept unless `DELETE_DATASET=1`. A
deployment made before the script marked what it created has no marks:
`FORCE=1` deletes every piece named after `PREFIX`, whoever created it, after
you type the project id.

Who can reach what:

- the service reads and writes as its own service account, not as the person
  running the query. It may use only the datasets in `ALLOWED_DATASETS`
  (default `PROJECT.DATASET`), on which it is granted BigQuery Data Editor,
  and it refuses table names outside them. Dataset names are compared exactly,
  as BigQuery treats them (case-sensitive); project ids in any case;
- anyone who can call the function can therefore have any table in those
  datasets predicted and write new tables into them, and spends the key.
  Calling needs access to the function's dataset and BigQuery Connection User
  on the connection;
- the caller chooses the id column, and the output table copies it for every
  predicted row. So a caller can copy any column of any table in
  `ALLOWED_DATASETS`, for the rows whose target is blank, into a new table in
  those datasets. Put in `ALLOWED_DATASETS` only datasets whose tables every
  caller may read in full, and that every caller may create tables in;
- only the connection's service account may invoke the Cloud Run service.

What the service does about BigQuery calling more than once. BigQuery may send
the same call again, even after a success, in the same request or at the same
time in another one, and retries some failures:

- the service claims the output table by creating it, empty, before it
  predicts, with a fingerprint of the call in the table description. Of several
  copies of one call only one predicts; the others wait for it and return its
  summary. A copy that comes later finds the finished table and returns the
  same summary without predicting again. So rerunning the same query costs
  nothing; to predict again, drop the output table;
- the fingerprint includes the source table's last-modified time, and a
  view's definition. Once the source has changed, the same query is refused
  with "output table already exists" rather than answered with the earlier
  summary; drop the output table to predict from the new data. A view's
  fingerprint does not change when only the tables under it do;
- an output table that exists for any other reason is refused before anything
  is paid for;
- once a prediction may have been paid for, a failure (writing the table, a
  timeout, or a later call failing when a large table is sent in several
  calls) fails the query and is not retried. The empty output table is kept,
  marked as failed, so that a copy of the call fails at once instead of
  predicting again; drop it before running again. If the rows were written but
  the table's description could not be updated, the query succeeds and a copy
  finds the rows and does not predict again.

Limits: a call has at most 20 minutes, BigQuery's limit for a remote call to
Cloud Run, and the service is deployed with the same timeout. Source tables
over 1,000,000 rows are refused (`CAUSILO_MAX_SOURCE_ROWS`), and the service
runs one call at a time per instance with 2 CPUs and 4 GiB of memory, at most
`MAX_INSTANCES` (default 3) instances. BigQuery runs at most ten queries with
remote functions at a time per project. Remote function results are never
cached, so every query reaches the service; the fingerprint is what keeps a
rerun free.

## Tested

Run against the live API in November 2026 with the two
[example tables](https://github.com/nums-ai/causilo-db/blob/main/examples/README.md),
part of the answers hidden. On the command-line rows the predictions were
written to a table with `--out`, read back and scored against the hidden
answers (`scripts/live_check.py` does this), and on the BigQuery remote
function row the table the function wrote was read back the same way
(`scripts/bigquery_live_check.py`); on the other rows the returned result was
scored.

| Database | Path | Regression, concrete strength (229 cells) | Classification, car trim (385 cells) |
|---|---|---|---|
| PostgreSQL 16 | command line | R² 0.939 | accuracy 73.5% |
| PostgreSQL 16 | `causilo.predict` function | R² 0.939 | accuracy 73.5% |
| PostgreSQL 17, http extension 1.6.1 | `causilo.predict` function without Python (`install_http.sql`) | R² 0.939 | accuracy 73.5% |
| MySQL 8.4 | command line | R² 0.939 | accuracy 73.5% |
| MariaDB 11 | command line | R² 0.939 | accuracy 73.5% |
| SQL Server 2022 | command line | R² 0.939 | accuracy 73.5% |
| Oracle 23ai Free | command line | R² 0.939 | accuracy 73.5% |
| ClickHouse | command line | R² 0.939 | accuracy 73.5% |
| Trino (memory catalog) | command line | R² 0.939 | accuracy 73.5% |
| Redshift Serverless | command line | R² 0.939 | accuracy 73.5% |
| BigQuery | command line | R² 0.939 | accuracy 73.5% |
| BigQuery | `causilo_predict` remote function, Cloud Run | R² 0.939 | accuracy 73.5% |
| Snowflake | command line | R² 0.939 | accuracy 73.5% |
| Snowflake | `CALL CAUSILO.PREDICT` procedure | R² 0.939 | accuracy 73.5% |
| Snowflake | Native App, `CALL CAUSILO.CORE.PREDICT` | R² 0.939 | accuracy 73.5% |
| DuckDB | `causilo_db.duckdb` | R² 0.939 | accuracy 73.5% |
| Spark 4.3, local | `causilo_db.spark` | R² 0.939 | accuracy 73.8% |
| Databricks, classic single-node cluster (Runtime 16.4 LTS) | `causilo_db.spark` | R² 0.939 | accuracy 73.5% |

The table shows that every path returns the same answer, not that the car
trim is easy to predict: always guessing the most common class ("lounge") is
right on 72.5% of the hidden rows, so 73.5% is about four rows better. The
regression result is the meaningful one.

Where the same rows went in, the same numbers came out on seventeen paths across
twelve databases; the PL/pgSQL function's 229 predicted strengths and 385
predicted trims were identical, row for row, to the command line's on the same
tables. The small differences elsewhere come from the rows the model
saw: a table read without ORDER BY can come back in another order (a second
Redshift run gave 73.2%), and local Spark ran on its own split (73.8%). Spark
also predicted 1,000 cells of a 300,000-row synthetic table (R² 0.997),
sampling the filled rows inside Spark.

On Databricks the result was checked as a DataFrame; writing it with
`predict_table(..., output_table=)` has not been run there.

`scripts/live_check.py <url>` repeats the database check on any SQLAlchemy URL;
`--function` checks the installed `causilo.predict` of a PostgreSQL instead,
and `scripts/bigquery_live_check.py <project> <dataset>` repeats it through a
deployed BigQuery remote function. Sending the same call again, and three
copies of a new call in one query, wrote no second result and made no second
prediction (one prediction per output table in the service log).

## Tests

Python 3.11 or later:

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest -q
```

They use a fake client and SQLite, DuckDB and a local Spark session (skipped
without pyspark or Java); nothing reaches the network, even with
`CAUSILO_API_KEY` set. `tests/test_postgres_http.py` runs `install_http.sql`
in a PostgreSQL you name with `CAUSILO_TEST_PG_HTTP` (it is skipped otherwise)
against a stand-in API on 127.0.0.1, and checks that the Parquet tables sent
and the rows returned are the ones `install.sql` would produce; its docstring
says how to start the container. `scripts/live_check.py` calls the real API and is
charged like any other call.

See also the [changelog](https://github.com/nums-ai/causilo-db/blob/main/CHANGELOG.md)
and the [security policy](https://github.com/nums-ai/causilo-db/blob/main/SECURITY.md).
