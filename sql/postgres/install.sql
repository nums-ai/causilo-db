-- Causilo for PostgreSQL: SELECT * FROM causilo.predict('customers', 'churn', 'customer_id');
--
-- Needs PL/Python (plpython3u) and, in the Python the server runs, the
-- causilo-db package:  pip install causilo-db  (as the server's Python).
-- Managed services that do not offer plpython3u (RDS, Cloud SQL, Azure) use
-- the causilo-db command or Python package from outside instead.
--
-- The key is per session and never stored in a table:
--   SET causilo.api_key = 'csl_...';
-- (or CAUSILO_API_KEY in the server's environment, which every role allowed to
-- run the function then spends). The SET statement is plain SQL text, so it
-- shows in pg_stat_activity and, with log_statement on, in the server log.

CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE SCHEMA IF NOT EXISTS causilo;

-- One row per blank cell of target_column in tbl.
--   row_id       the id_column value, or the row's ctid when no id is given
--                (a ctid changes when the row is updated and is not unique across
--                partitions; pass an id to join back)
--   prediction   the predicted value as text (a class, or the median for a number)
--   probability  classification: probability of the predicted class
--   p25/p50/p75  regression: the predicted quantiles
CREATE OR REPLACE FUNCTION causilo.predict(
    tbl regclass,
    target_column text,
    id_column text DEFAULT NULL,
    task text DEFAULT NULL
) RETURNS TABLE (row_id text, prediction text, probability double precision,
                 p25 double precision, p50 double precision, p75 double precision)
LANGUAGE plpython3u
AS $$
import decimal
import os

import pandas as pd
from causilo_db.core import CausiloDBError, _label, check_id_column, predict_frame

key = plpy.execute("SELECT current_setting('causilo.api_key', true) AS k")[0]["k"] or os.environ.get("CAUSILO_API_KEY", "")
if not key:
    plpy.error("Set a Causilo API key first: SET causilo.api_key = 'csl_...'")

try:
    check_id_column(id_column, target_column)
except CausiloDBError as exc:
    plpy.error(str(exc))

# tbl is a regclass; its text form is already quoted and schema-qualified.
# Views have no ctid, so it is read only when no id is given.
if id_column is None:
    rows = plpy.execute("SELECT ctid::text AS __causilo_ctid, * FROM %s" % tbl)
else:
    rows = plpy.execute("SELECT * FROM %s" % tbl)
names = rows.colnames()
types = dict(zip(names, rows.coltypes()))
df = pd.DataFrame.from_records([[r[n] for n in names] for r in rows], columns=names)

for name in (target_column, id_column):
    if name is not None and name not in df.columns:
        plpy.error("No column named %s in %s" % (name, tbl))

# PL/Python hands NUMERIC over as Decimal and dates as text; give them their
# types as the other adapters see them. The id is left as it is.
DATES = {1082, 1114, 1184}  # date, timestamp, timestamptz
for name in names:
    if name in (id_column, "__causilo_ctid"):
        continue
    if types[name] in DATES:
        df[name] = pd.to_datetime(df[name], utc=True, errors="coerce", format="ISO8601")
    elif df[name].dtype == object and df[name].map(lambda v: isinstance(v, decimal.Decimal)).any():
        df[name] = df[name].map(lambda v: None if v is None else float(v))

exclude = ["__causilo_ctid"] + ([id_column] if id_column else [])
try:
    pred = predict_frame(df, target_column, api_key=key, task=task, exclude=exclude)
except CausiloDBError as exc:
    plpy.error(str(exc))

ids = df[id_column if id_column else "__causilo_ctid"]
out = pred.frame
for r in out.to_dict("records"):
    row_id = str(ids.iloc[int(r["row_number"])])
    if pred.task == "reg":
        yield (row_id, str(r[target_column]), None,
               float(r[target_column + "_p25"]), float(r[target_column + "_p50"]), float(r[target_column + "_p75"]))
    else:
        yield (row_id, _label(r[target_column]), float(r[target_column + "_probability"]), None, None, None)
$$;

COMMENT ON FUNCTION causilo.predict(regclass, text, text, text) IS
  'Predict the blank cells of a column with Causilo. SET causilo.api_key first.';
