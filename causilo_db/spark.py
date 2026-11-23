"""Spark and Databricks: predict the blank cells of a column of a Spark DataFrame.

    from causilo_db.spark import predict, predict_table
    out = predict(spark.table("main.sales.customers"), "churn", id_column="customer_id")
    predict_table(spark, "main.sales.customers", "churn", id_column="customer_id",
                  output_table="main.sales.customers_churn_prediction")

Only what the API needs reaches the driver: the rows to predict, and a sample of
at most 50,000 filled rows drawn inside Spark. The table itself stays where it is.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .core import MAX_CONTEXT_ROWS, CausiloDBError, check_id_column, is_result_name, predict_frame

# Rows to predict that are pulled to the driver in one go. Above this, filter
# the DataFrame first (or raise it, if the driver has the memory).
MAX_QUERY_ROWS = 1_000_000


def _blank(sdf: Any, column: str) -> Any:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    col = F.col(f"`{column}`")
    cond = col.isNull()
    if isinstance(sdf.schema[column].dataType, (DoubleType, FloatType)):
        cond = cond | F.isnan(col)
    return cond


def predict(sdf: Any, target: str, *, id_column: str | None = None, features: list[str] | None = None,
            task: str | None = None, api_key: str | None = None, endpoint: str | None = None,
            client: Any = None, seed: int = 0, max_query_rows: int = MAX_QUERY_ROWS) -> Any:
    """Predict the rows of ``sdf`` whose ``target`` is blank; returns a Spark DataFrame.

    With ``id_column`` the result is that column plus the prediction columns, ready
    to join back. Without it, the result is the blank rows as they are, with the
    prediction columns filled in after them.
    """
    check_id_column(id_column, target)
    names = list(sdf.columns)
    for name in [target] + ([id_column] if id_column else []) + list(features or []):
        if name not in names:
            raise CausiloDBError(f"No column named {name!r}.")
    if features is not None:
        keep = [c for c in names if c in features or c == target or c == id_column]
        sdf = sdf.select(*[f"`{c}`" for c in keep])
    if id_column is None:
        # The result keeps the source columns, so a result name must not be one.
        clash = [c for c in sdf.columns if is_result_name(c, target)]
        if clash:
            raise CausiloDBError(f"Column {clash[0]!r} clashes with a result column; pass id_column instead.")

    blank = _blank(sdf, target)
    query = sdf.filter(blank)
    filled = sdf.filter(~blank)

    n_query = query.count()
    if n_query == 0:
        raise CausiloDBError(f"{target!r} has no blank cells to predict.")
    if n_query > max_query_rows:
        raise CausiloDBError(
            f"{n_query:,} rows to predict is more than max_query_rows={max_query_rows:,}; "
            "filter the DataFrame first, or raise the limit."
        )
    n_filled = filled.count()
    if n_filled == 0:
        raise CausiloDBError(f"{target!r} has no filled cells to learn from.")
    notes: list[str] = []
    if n_filled > MAX_CONTEXT_ROWS:
        # Oversample a little in Spark, then cut to size on the driver at random:
        # .limit() would keep the first partitions and drop the tail.
        fraction = min(1.0, MAX_CONTEXT_ROWS * 1.1 / n_filled)
        filled = filled.sample(withReplacement=False, fraction=fraction, seed=seed)
        notes.append(f"context sampled in Spark from {n_filled:,} rows")

    context_pd = filled.toPandas()
    if len(context_pd) > MAX_CONTEXT_ROWS:
        context_pd = context_pd.sample(n=MAX_CONTEXT_ROWS, random_state=seed)
    query_pd = query.toPandas()
    df = pd.concat([context_pd, query_pd], ignore_index=True)

    pred = predict_frame(df, target, features=None, exclude=[id_column] if id_column else None,
                         task=task, api_key=api_key, endpoint=endpoint, client=client)
    result = pred.frame
    rows = df.iloc[result["row_number"].to_numpy()].reset_index(drop=True)
    predicted = result.drop(columns=["row_number"]).reset_index(drop=True)
    if id_column is not None:
        out = pd.concat([rows[[id_column]], predicted], axis=1)
    else:
        # The blank rows in their own column order, the target filled in where
        # it was, and the extra prediction columns after.
        out = rows.copy()
        out[target] = predicted[target].to_numpy()
        out = pd.concat([out, predicted.drop(columns=[target])], axis=1)
        # Spark cannot infer a type for a column that is blank in every row.
        for name in out.columns:
            if out[name].isna().all() and out[name].dtype == object:
                out[name] = out[name].astype("string")
    spark = sdf.sparkSession
    return spark.createDataFrame(out)


def predict_table(spark: Any, table: str, target: str, *, output_table: str | None = None,
                  mode: str = "error", **kwargs: Any) -> Any:
    """Predict a table by name; with ``output_table``, also save the result there.

    ``mode`` is Spark's save mode: ``error`` (the default, as with the other
    adapters) refuses an existing table before anything is predicted;
    ``overwrite`` and ``append`` do what they say.
    """
    if mode not in ("error", "errorifexists", "overwrite", "append"):
        raise CausiloDBError(f"mode must be 'error', 'overwrite' or 'append', not {mode!r}.")
    if output_table and output_table == table:
        raise CausiloDBError("output_table is the table being read; write the result to another table.")
    if output_table and mode in ("error", "errorifexists") and spark.catalog.tableExists(output_table):
        raise CausiloDBError(f"Table {output_table} already exists; pass mode='overwrite' or 'append'.")
    out = predict(spark.table(table), target, **kwargs)
    if output_table:
        out.write.mode(mode).saveAsTable(output_table)
    return out
