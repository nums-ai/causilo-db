"""Any database SQLAlchemy can reach: read a table, predict, write the result.

    from causilo_db import predict_table
    result, pred = predict_table("postgresql+psycopg://...", "customers", "churn",
                                 id_column="customer_id", out_table="customers_churn_prediction")

Works with the SQLAlchemy dialects: PostgreSQL, MySQL and MariaDB, SQL Server,
Oracle and SQLite, and through their own dialects ClickHouse, Trino, Snowflake,
BigQuery, Redshift and others. The table is read whole, so ``where`` is the
way to send part of one. The URL names the driver:
``postgresql+psycopg://``, ``mysql+pymysql://``, ``mssql+pymssql://``,
``oracle+oracledb://`` and so on.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .core import CausiloDBError, Prediction, check_id_column, predict_frame


# Words Trino reserves that its SQLAlchemy dialect does not know to quote; a
# column called "trim" otherwise breaks CREATE TABLE and SELECT alike.
_TRINO_RESERVED = {
    "trim", "extract", "normalize", "listagg", "skip", "uescape", "cube", "rollup", "grouping",
    "localtime", "localtimestamp", "current_catalog", "current_path", "current_schema",
    "json_array", "json_exists", "json_object", "json_query", "json_table", "json_value",
}


def _engine(url_or_engine: Any):
    if isinstance(url_or_engine, str):
        import sqlalchemy

        engine = sqlalchemy.create_engine(url_or_engine)
    else:
        engine = url_or_engine
    if engine.dialect.name == "trino":
        prep = engine.dialect.identifier_preparer
        prep.reserved_words = set(prep.reserved_words) | _TRINO_RESERVED
        # The preparer caches each name's quoting; forget what it decided before.
        getattr(prep, "_strings", {}).clear()
    return engine


def read_table(engine: Any, table: str, *, schema: str | None = None, where: str | None = None,
               columns: list[str] | None = None) -> pd.DataFrame:
    """The table as a DataFrame. Names are quoted by the dialect, ``where`` is raw SQL."""
    import sqlalchemy as sa

    meta = sa.MetaData()
    try:
        t = sa.Table(table, meta, schema=schema, autoload_with=engine)
    except sa.exc.NoSuchTableError as exc:
        raise CausiloDBError(f"No table {table!r}" + (f" in schema {schema!r}" if schema else "") + ".") from exc
    cols = [t.c[c] for c in columns] if columns else [t]
    query = sa.select(*cols)
    if where:
        query = query.where(sa.text(where))
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def predict_table(
    url_or_engine: Any,
    table: str,
    target: str,
    *,
    schema: str | None = None,
    where: str | None = None,
    id_column: str | None = None,
    features: list[str] | None = None,
    task: str | None = None,
    out_table: str | None = None,
    out_schema: str | None = None,
    if_exists: str = "fail",
    api_key: str | None = None,
    endpoint: str | None = None,
    client: Any = None,
) -> tuple[pd.DataFrame, Prediction]:
    """Predict the blank cells of ``target`` in ``table``.

    The result has ``id_column`` (when given; otherwise ``row_number``) and the
    prediction columns. With ``out_table`` it is also written to the database;
    ``if_exists`` is passed to pandas (``fail``, ``replace`` or ``append``).
    """
    if if_exists not in ("fail", "replace", "append"):
        raise CausiloDBError(f"if_exists must be 'fail', 'replace' or 'append', not {if_exists!r}.")
    check_id_column(id_column, target)
    if out_table and out_table == table and (out_schema or None) == (schema or None):
        raise CausiloDBError("out_table is the table being read; write the result to another table.")
    engine = _engine(url_or_engine)
    df = read_table(engine, table, schema=schema, where=where)
    if df.empty:
        raise CausiloDBError(f"No rows were read from {table!r}" + (" with that condition." if where else "."))
    if id_column is not None and id_column not in df.columns:
        raise CausiloDBError(f"No column named {id_column!r}.")
    if out_table and if_exists == "fail":
        import sqlalchemy as sa

        # Refuse before the prediction is paid for, not after.
        if sa.inspect(engine).has_table(out_table, schema=out_schema):
            name = f"{out_schema}.{out_table}" if out_schema else out_table
            raise CausiloDBError(f"Table {name} already exists; pass if_exists='replace' or 'append'.")
    pred = predict_frame(df, target, features=features, exclude=[id_column] if id_column else None,
                         task=task, api_key=api_key, endpoint=endpoint, client=client)
    result = pred.frame
    if id_column is not None:
        ids = df[id_column].reset_index(drop=True)
        result = result.copy()
        result.insert(0, id_column, ids.iloc[result["row_number"]].to_numpy())
        result = result.drop(columns=["row_number"])
    if out_table:
        try:
            write_table(result, engine, out_table, schema=out_schema, if_exists=if_exists)
        except Exception as exc:
            err = CausiloDBError(f"The prediction was made but could not be written to {out_table}: {exc}")
            err.result, err.prediction = result, pred
            raise err from exc
    return result, pred


def _clickhouse_type(col: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(col):
        return "Nullable(UInt8)"
    if pd.api.types.is_integer_dtype(col):
        return "Nullable(Int64)"
    if pd.api.types.is_float_dtype(col):
        return "Nullable(Float64)"
    if pd.api.types.is_datetime64_any_dtype(col):
        return "Nullable(DateTime64(6))"
    return "Nullable(String)"


def write_table(df: pd.DataFrame, engine: Any, table: str, *, schema: str | None = None,
                if_exists: str = "fail") -> None:
    """Write ``df`` as a table, smoothing over the dialects pandas cannot write to as is."""
    import sqlalchemy as sa

    if if_exists not in ("fail", "replace", "append"):
        raise CausiloDBError(f"if_exists must be 'fail', 'replace' or 'append', not {if_exists!r}.")
    engine = _engine(engine)
    dialect = engine.dialect.name
    if dialect == "clickhouse":
        # ClickHouse needs a table engine, which pandas has no way to name.
        name = f"{schema}.{table}" if schema else table
        quoted = ".".join("`" + p.replace("`", "``") + "`" for p in ([schema, table] if schema else [table]))
        with engine.begin() as con:
            exists = sa.inspect(con).has_table(table, schema=schema)
            if exists and if_exists == "fail":
                raise CausiloDBError(f"Table {name} already exists; pass if_exists='replace' or 'append'.")
            if exists and if_exists == "replace":
                con.execute(sa.text(f"DROP TABLE {quoted}"))
                exists = False
            if not exists:
                cols = ", ".join("`" + str(c).replace("`", "``") + f"` {_clickhouse_type(df[c])}" for c in df.columns)
                con.execute(sa.text(f"CREATE TABLE {quoted} ({cols}) ENGINE = MergeTree ORDER BY tuple()"))
        df.to_sql(table, engine, schema=schema, if_exists="append", index=False)
        return
    dtype = None
    if dialect == "redshift":
        # pandas asks for TEXT, which Redshift makes VARCHAR(256) bytes.
        dtype = {c: sa.types.VARCHAR(65535) for c in df.columns
                 if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c])}
    if dialect == "oracle":
        # pandas asks Oracle for FLOAT(53), which SQLAlchemy refuses, and for CLOB for text.
        from sqlalchemy.dialects import oracle

        dtype = {}
        for c in df.columns:
            if pd.api.types.is_float_dtype(df[c]):
                dtype[c] = oracle.BINARY_DOUBLE()
            elif pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
                lengths = df[c].dropna().astype(str).str.len()
                longest = int(lengths.max()) if len(lengths) else 1
                dtype[c] = sa.String(min(4000, max(1, longest)))
    # Redshift and BigQuery run each single-row INSERT as its own statement (on
    # BigQuery a job of its own); a multi-row INSERT per batch costs about one.
    # A multi-row INSERT binds rows x columns parameters, and BigQuery takes at
    # most 10,000 in one query; size the batch by the width of the table.
    limits = {"redshift": (1000, 30_000), "bigquery": (500, 9_000)}
    method, chunksize = None, None
    if dialect in limits:
        rows, params = limits[dialect]
        method, chunksize = "multi", max(1, min(rows, params // max(1, len(df.columns))))
    if dialect == "redshift" and if_exists == "replace":
        # pandas checks for a view before replacing, and the Redshift dialect asks
        # pg_class for a column Redshift does not have; drop the table ourselves.
        prep = engine.dialect.identifier_preparer
        name = prep.quote(table) if not schema else f"{prep.quote_schema(schema)}.{prep.quote(table)}"
        with engine.begin() as con:
            con.execute(sa.text(f"DROP TABLE IF EXISTS {name}"))
        if_exists = "append"
    df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False, dtype=dtype,
              method=method, chunksize=chunksize)
