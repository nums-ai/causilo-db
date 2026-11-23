"""DuckDB: predict from a table or a query, get a DuckDB relation back.

    import duckdb, causilo_db.duckdb as cx
    con = duckdb.connect("shop.duckdb")
    rel = cx.predict(con, "customers", "churn", id_column="customer_id")
    rel.create("customers_churn_prediction")       # or rel.df(), or query it:
    con.sql("SELECT * FROM rel WHERE churn_probability > 0.8")

``source`` is a table name (``schema.table`` works) or any SELECT.
"""

from __future__ import annotations

import re
from typing import Any

from .core import CausiloDBError, Prediction, check_id_column, predict_frame


def predict(con: Any, source: str, target: str, *, id_column: str | None = None,
            features: list[str] | None = None, task: str | None = None,
            api_key: str | None = None, endpoint: str | None = None, client: Any = None) -> Any:
    """The predicted rows as a DuckDB relation."""
    # A query starts with a keyword followed by a space; "selected_rows" is a table.
    is_query = re.match(r"\s*\(?\s*(select|with|from)\s", source, re.IGNORECASE) is not None
    check_id_column(id_column, target)
    df = (con.sql(source) if is_query else con.table(source)).df()
    if id_column is not None and id_column not in df.columns:
        raise CausiloDBError(f"No column named {id_column!r}.")
    pred: Prediction = predict_frame(df, target, features=features, exclude=[id_column] if id_column else None,
                                     task=task, api_key=api_key, endpoint=endpoint, client=client)
    result = pred.frame.copy()
    if id_column is not None:
        result.insert(0, id_column, df[id_column].reset_index(drop=True).iloc[result["row_number"]].to_numpy())
        result = result.drop(columns=["row_number"])
    return con.from_df(result)
