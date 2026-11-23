"""Fill the blank cells of one column of a table with Causilo.

Every adapter (SQLAlchemy, PostgreSQL, DuckDB, Snowflake, Spark) reads a table
into a DataFrame and hands it here. The rows where ``target`` has a value are
the examples; the rows where it is blank are predicted. Nothing else about the table is
assumed: no column has to be an id, and the result keeps the input's row order
through ``row_number``.

The call goes to the hosted API with the person's own key; this package keeps
no key and writes nothing anywhere.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd

DEFAULT_ENDPOINT = "https://api.nums.world"
MODEL_CLF = "causilo-clf"
MODEL_REG = "causilo-reg"

# The API's limit on cells per call; staying under it here turns a 413 into a
# split. Context beyond 50,000 rows stops helping and is billed.
MAX_CELLS_PER_CALL = 7_000_000
MAX_CONTEXT_ROWS = 50_000
DEFAULT_QUANTILES = (0.25, 0.5, 0.75)

# A numeric column with at most this many distinct whole values is read as classes.
CLASS_LIMIT = 20

# A text target with more distinct values than this is more likely a name or a
# code than a class; refuse it unless task="clf" was asked for.
MAX_INFERRED_CLASSES = 200

# Longest class column name: PostgreSQL keeps 63 bytes and MySQL allows 64;
# the rest is room for a clash number.
MAX_NAME = 56


class CausiloDBError(Exception):
    """A table that cannot be predicted as asked. The message says why."""


@dataclass
class Prediction:
    """The predicted rows, and what was done to get them."""

    frame: pd.DataFrame
    task: str
    target: str
    features: list[str]
    n_context: int
    n_query: int
    calls: int
    notes: list[str] = field(default_factory=list)


def infer_task(values: pd.Series) -> str:
    """'reg' for a number with many values, 'clf' for anything else."""
    known = values.dropna()
    if known.empty:
        raise CausiloDBError(f"Column {values.name!r} has no values to learn from.")
    if pd.api.types.is_bool_dtype(known):
        return "clf"
    if pd.api.types.is_numeric_dtype(known):
        if known.nunique() > CLASS_LIMIT:
            return "reg"
        if not np.allclose(known.astype("float64"), np.round(known.astype("float64"))):
            return "reg"
        return "clf"
    return "clf"


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Give object columns that hold numbers or dates a real dtype.

    Drivers hand back DECIMAL and DATE columns, and integer columns with blanks
    after a concat, as Python objects; read as they are, a price would become a
    class label and a date a category.
    """
    out = df.copy()
    for name in out.columns:
        col = out[name]
        if col.dtype != object:
            continue
        known = col.dropna()
        if known.empty:
            continue
        if known.map(lambda v: isinstance(v, (int, float, decimal.Decimal, np.integer, np.floating))
                     and not isinstance(v, (bool, np.bool_))).all():
            out[name] = pd.to_numeric(col, errors="coerce").astype("float64")
        elif known.map(lambda v: isinstance(v, (_dt.date, _dt.datetime, pd.Timestamp))).all():
            out[name] = pd.to_datetime(col, errors="coerce", utc=True)
    return out


def _usable(df: pd.DataFrame, target: str, features: list[str] | None, exclude: list[str]) -> tuple[pd.DataFrame, list[str], list[str]]:
    """The feature columns the API can take, converted, and notes on what was dropped."""
    notes: list[str] = []
    if isinstance(features, str):
        features = [features]
    if features is None:
        features = [c for c in df.columns if c != target and c not in exclude]
    features = list(dict.fromkeys(c for c in features if c != target and c not in exclude))
    missing = [c for c in features if c not in df.columns]
    if missing:
        raise CausiloDBError(f"No such columns: {', '.join(missing)}.")
    cols: dict[str, pd.Series] = {}
    for name in features:
        col = df[name]
        if pd.api.types.is_datetime64_any_dtype(col):
            # The API refuses datetimes; seconds since the epoch keep the order.
            col = pd.to_datetime(col, utc=True)
            col = (col - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()
            notes.append(f"{name}: dates sent as seconds since 1970-01-01")
        elif not (pd.api.types.is_numeric_dtype(col) or pd.api.types.is_bool_dtype(col)):
            col = col.map(lambda v: None if v is None or (pd.api.types.is_scalar(v) and pd.isna(v)) else str(v))
        if col.isna().all():
            notes.append(f"{name}: dropped, every value is blank")
            continue
        cols[name] = col
    if not cols:
        raise CausiloDBError("No feature columns are left to predict from.")
    # One concat rather than a column at a time: a wide table stays one block.
    return pd.concat(cols, axis=1), list(cols), notes


def _class_columns(target: str, classes: list[str]) -> list[str]:
    """``<target>_prob_<class>`` names that every database accepts.

    Class labels can hold spaces, commas or anything else, and databases fold
    case, so "A" and "a" would land on one column; letters, digits and "_" are
    kept, the rest becomes "_", and a clash gets a number.
    """
    names: list[str] = []
    seen: set[str] = set()
    for c in classes:
        base = (f"{target}_prob_" + (re.sub(r"[^0-9A-Za-z_]", "_", str(c)) or "_"))[:MAX_NAME]
        name, k = base, 2
        while name.lower() in seen:
            name, k = f"{base}_{k}", k + 1
        seen.add(name.lower())
        names.append(name)
    return names


def is_result_name(name: str, target: str) -> bool:
    """Whether ``name`` is, or folds to, a column the result adds (``row_number``, ``<target>_p50`` ...)."""
    low = str(name).lower()
    return low == "row_number" or re.fullmatch(
        rf"{re.escape(str(target).lower())}_(p\d+|probability|prob_.*)", low) is not None


def check_id_column(id_column: str | None, target: str) -> None:
    """Refuse an id column that would clash with a result column, before anything is paid for."""
    if id_column is None:
        return
    if str(id_column).lower() == str(target).lower():
        raise CausiloDBError("The id column cannot be the target column.")
    if is_result_name(id_column, target):
        raise CausiloDBError(f"An id column named {id_column!r} clashes with a result column; use another one.")


def _label(v: Any) -> str:
    if isinstance(v, (float, np.floating)) and float(v).is_integer():
        return str(int(v))
    if isinstance(v, (np.integer,)):
        return str(int(v))
    return str(v)


def predict_frame(
    df: pd.DataFrame,
    target: str,
    *,
    api_key: str | None = None,
    endpoint: str | None = None,
    features: list[str] | None = None,
    exclude: list[str] | None = None,
    task: str | None = None,
    quantiles: tuple[float, ...] = DEFAULT_QUANTILES,
    client: Any = None,
    progress: Callable[[int, int], None] | None = None,
) -> Prediction:
    """Predict the blank cells of ``target`` from the rows where it is filled.

    Returns one row per blank cell: ``row_number`` (0-based position in
    ``df``), the predicted ``target``, and for a regression one column per
    quantile (``target_p25`` ...), for a classification ``target_probability``
    and one ``target_prob_<class>`` column per class.

    ``exclude`` names columns that are not features, an id for example.
    ``api_key`` defaults to the ``CAUSILO_API_KEY`` environment variable.
    """
    if target not in df.columns:
        raise CausiloDBError(f"No column named {target!r}.")
    df = _normalize(df.reset_index(drop=True))
    exclude = list(exclude or [])
    if str(target).lower() == "row_number":
        raise CausiloDBError("A target named 'row_number' clashes with the result's row_number column; rename it.")
    if len(set(round(q * 100) for q in quantiles)) != len(quantiles) or not all(0 < q < 1 for q in quantiles):
        raise CausiloDBError("quantiles must be distinct whole percentiles between 0 and 1, e.g. (0.1, 0.5, 0.9).")
    blank = df[target].isna().to_numpy()
    query_rows = np.flatnonzero(blank)
    context_rows = np.flatnonzero(~blank)
    if len(query_rows) == 0:
        raise CausiloDBError(f"Column {target!r} has no blank cells, so there is nothing to predict.")
    if len(context_rows) < 2:
        raise CausiloDBError(f"Column {target!r} has {len(context_rows)} filled rows; at least 2 are needed.")

    asked = task
    task = task or infer_task(df[target])
    if task not in ("clf", "reg"):
        raise CausiloDBError("task must be 'clf' or 'reg'.")
    X, feats, notes = _usable(df, target, features, exclude)
    known = df[target].iloc[context_rows]
    if task == "clf":
        n_classes = known.nunique()
        if asked is None and n_classes > MAX_INFERRED_CLASSES:
            raise CausiloDBError(f"Column {target!r} has {n_classes:,} distinct values, which looks more like a name "
                                 "or a code than a class; pass task='clf' if it really is one.")
        if asked is None and pd.api.types.is_numeric_dtype(known) and not pd.api.types.is_bool_dtype(known):
            notes.append(f"{target}: whole numbers with {n_classes} distinct values, read as classes; "
                         "pass task='reg' if it is a quantity")
    elif not pd.api.types.is_numeric_dtype(known):
        try:
            known.astype("float64")
        except (TypeError, ValueError) as exc:
            raise CausiloDBError(f"Column {target!r} is not numeric; task='reg' needs numbers.") from exc

    n_cols = len(feats) + 1
    budget_rows = MAX_CELLS_PER_CALL // n_cols
    # Every call carries the context and is billed for it, so a wide table must
    # not be answered a few hundred rows per call: the context gets at most half
    # of a call, unless the rows to predict fit in what is left anyway.
    keep = min(MAX_CONTEXT_ROWS, max(budget_rows // 2, budget_rows - len(query_rows)))
    if len(context_rows) > keep:
        rng = np.random.default_rng(0)
        notes.append(f"context sampled from {len(context_rows):,} to {keep:,} rows")
        context_rows = np.sort(rng.choice(context_rows, size=keep, replace=False))
    chunk = max(min(budget_rows - len(context_rows), len(query_rows)), 1)

    if client is None:
        import causilo_client

        key = api_key or os.environ.get("CAUSILO_API_KEY", "")
        if not key:
            raise CausiloDBError("A Causilo API key is needed: pass api_key or set CAUSILO_API_KEY.")
        client = causilo_client.Causilo(endpoint or os.environ.get("CAUSILO_ENDPOINT", DEFAULT_ENDPOINT), token=key)

    y = df[target]
    context = X.iloc[context_rows].reset_index(drop=True)
    if task == "clf":
        labels = {_label(v): (v.item() if hasattr(v, "item") else v) for v in y.iloc[context_rows].dropna().unique()}
        answers = y.iloc[context_rows].map(_label).to_numpy()
    else:
        answers = y.iloc[context_rows].astype("float64").to_numpy()
    context = pd.concat([context, pd.Series(answers, name=target)], axis=1)

    parts: list[np.ndarray] = []
    classes: list[str] = []
    calls = 0
    for start in range(0, len(query_rows), chunk):
        rows = query_rows[start:start + chunk]
        query = X.iloc[rows].reset_index(drop=True)
        if progress:
            progress(start, len(query_rows))
        if task == "clf":
            pred, meta = client.predict_with_metadata(context, query, target=target, model=MODEL_CLF)
            got = [str(c) for c in meta.get("classes", [])]
            if classes and got != classes:
                raise CausiloDBError("The class order changed between calls; run again with fewer rows.")
            classes = classes or got
        else:
            pred, _ = client.predict_with_metadata(context, query, target=target, model=MODEL_REG,
                                                   output_type="quantiles", quantiles=list(quantiles))
        m = np.asarray(pred, dtype="float64")
        if m.ndim == 1:
            m = m.reshape(len(rows), -1)
        parts.append(m)
        calls += 1
    matrix = np.vstack(parts)

    out = pd.DataFrame({"row_number": query_rows.astype("int64")})
    if task == "reg":
        qs = list(quantiles)
        median = matrix[:, qs.index(0.5)] if 0.5 in qs else matrix.mean(axis=1)
        out[target] = median
        for i, q in enumerate(qs):
            out[f"{target}_p{round(q * 100):g}"] = matrix[:, i]
    else:
        best = matrix.argmax(axis=1)
        out[target] = [labels.get(classes[j], classes[j]) for j in best]
        out[f"{target}_probability"] = matrix[np.arange(len(best)), best]
        for j, name in enumerate(_class_columns(target, classes)):
            out[name] = matrix[:, j]
    return Prediction(frame=out, task=task, target=target, features=feats,
                      n_context=len(context_rows), n_query=len(query_rows), calls=calls, notes=notes)
