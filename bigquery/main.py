"""A BigQuery remote function that predicts the blank cells of a table column with Causilo.

    SELECT dataset.causilo_predict('project.dataset.table', 'target', 'id_column',
                                   'project.dataset.output_table');

BigQuery calls a remote function once per row of the query, in batches, but a
prediction needs the whole table as its examples. So each call carries table
names, not row values: this service reads the source table with the BigQuery
API under its own service account, predicts with ``causilo_db.core.predict_frame``,
writes the result to a new table and replies with a one-line summary.

Request and reply follow the remote function contract: a JSON body with
``calls`` (one argument list per call) and a reply of ``{"replies": [...]}``
with one entry per call, or a non-200 status with ``errorMessage``. BigQuery
retries 408, 429, 500, 503 and 504, and may send the same call again even after
a success, in the same request or in another one running at the same time. So
a call is answered at most once per output table:

- identical calls in one request are predicted once and get the same reply;
- before predicting, the call claims its output table by creating it, empty,
  with a fingerprint of the call in the table description. Creating a table is
  atomic, so of several copies of one call only one predicts. The others wait
  for it to finish and reply with its summary; a copy arriving after the end
  finds the finished table and replies the same, without predicting again;
- an output table that exists and was not made by this same call is refused
  before anything is paid for, as ``predict_table`` does with ``if_exists='fail'``;
- the fingerprint covers the source table's last-modified time (and a view's
  definition), so a rerun after the source changed is refused as "already
  exists" instead of answered with the old summary;
- a status BigQuery retries is returned only when nothing has been paid for
  (BigQuery or Causilo briefly unavailable before the first call of the
  prediction, or a copy still waiting). Anything else is a 400, which fails the
  query without a retry: a failure after the prediction, and a failure of a
  later call when a large table is sent in several, since the calls before it
  were charged.

Configuration, from the environment:

- ``CAUSILO_API_KEY``: the key, from Secret Manager;
- ``CAUSILO_ALLOWED_DATASETS``: comma-separated ``project.dataset`` names that
  source and output tables must be in. Required: the service reads and writes
  with its own rights, so this list is what a caller can reach;
- ``CAUSILO_MAX_SOURCE_ROWS``: refuse larger source tables (default 1,000,000);
- ``CAUSILO_ENDPOINT``: the API (default https://api.nums.world).

Run locally with ``gunicorn main:app``; ``app`` is a plain WSGI callable.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from causilo_db.core import CausiloDBError, PartialPredictionError, check_id_column, predict_frame

log = logging.getLogger("causilo_bigquery")

DEFAULT_MAX_SOURCE_ROWS = 1_000_000
MARK = "causilo-db prediction"
# BigQuery gives one HTTP invocation of a Cloud Run endpoint 20 minutes. A copy
# of a call waits for the first one at most this long, then asks to be retried.
WAIT_S = 17 * 60
POLL_S = 5.0
# A claim older than this belongs to an attempt that was cut off.
STALE_S = 25 * 60
# A table found loaded but still marked running is read again this many times,
# this far apart, before its summary is taken to be lost: longer than
# Warehouse.write spends retrying the description.
LOADED_READS = 5
LOADED_POLL_S = 3.0
# project.dataset.table, with the characters BigQuery allows in each part.
_TABLE = re.compile(r"^([a-z][a-z0-9.:-]{4,61}[a-z0-9])\.([A-Za-z0-9_]{1,1024})\.([^.`\s]{1,1024})$")
_OPTIONS = {"features", "task"}
# A column name every BigQuery load takes, flexible column names or not.
_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAX_COLUMN = 300


class CallError(Exception):
    """A call that cannot be answered. ``retry`` marks one BigQuery may send again."""

    def __init__(self, message: str, *, retry: bool = False, keep_claim: bool = False) -> None:
        super().__init__(message)
        self.retry = retry
        # The prediction may have been paid for: keep the output table so that
        # no copy of the call predicts again.
        self.keep_claim = keep_claim


class AlreadyExists(Exception):
    """The table exists."""


class NotMarked(Exception):
    """The result was written, but its table description could not be set."""


@dataclass
class TableInfo:
    kind: str            # "TABLE", "VIEW", "MATERIALIZED_VIEW", ...
    num_rows: int | None
    description: str
    columns: list[str]
    modified: Any = None        # last-modified time, a datetime
    view_query: str | None = None


class Warehouse:
    """What the service needs from BigQuery, so tests can stand in a fake."""

    def __init__(self, client: Any = None) -> None:
        from google.cloud import bigquery

        self._bq = bigquery
        self.client = client or bigquery.Client()

    def table(self, name: str) -> TableInfo | None:
        from google.api_core import exceptions

        try:
            t = self.client.get_table(name)
        except exceptions.NotFound:
            return None
        return TableInfo(kind=t.table_type, num_rows=t.num_rows, description=t.description or "",
                         columns=[f.name for f in t.schema], modified=t.modified,
                         view_query=getattr(t, "view_query", None) or getattr(t, "mview_query", None))

    # With these off, DATE comes back as datetime64 and TIME as datetime.time
    # objects rather than the dbdate and dbtime dtypes, so that dates are sent
    # as seconds since 1970 and times as text, as every other adapter sends them.
    FRAME = {"create_bqstorage_client": False, "date_dtype": None, "time_dtype": None}

    def read(self, name: str, info: TableInfo, columns: list[str]) -> pd.DataFrame:
        if info.kind == "TABLE":
            schema = [f for f in self.client.get_table(name).schema if f.name in columns]
            # tabledata.list: no query job and no bytes billed.
            return self.client.list_rows(name, selected_fields=schema).to_dataframe(**self.FRAME)
        # Views and the like cannot be listed; select from them. Column names
        # come from the table's own schema, so quoting them is enough.
        cols = ", ".join(f"`{c}`" for c in columns)
        return self.client.query(f"SELECT {cols} FROM `{name}`").to_dataframe(**self.FRAME)

    def claim(self, name: str, description: str) -> None:
        """Create ``name`` empty, or raise AlreadyExists."""
        from google.api_core import exceptions

        t = self._bq.Table(name)
        t.description = description
        try:
            self.client.create_table(t, exists_ok=False)
        except exceptions.Conflict as exc:
            raise AlreadyExists(name) from exc

    def write(self, name: str, df: pd.DataFrame, description: str) -> None:
        """Fill the claimed table, then mark it finished.

        Raises :class:`NotMarked` when the rows were loaded but the description
        could not be set.
        """
        config = self._bq.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        self.client.load_table_from_dataframe(df, name, job_config=config).result()
        last: Exception | None = None
        for wait in (0, 2, 5):
            time.sleep(wait)
            try:
                t = self.client.get_table(name)
                t.description = description
                self.client.update_table(t, ["description"])
                return
            except Exception as exc:
                last = exc
        raise NotMarked(str(last)) from last

    def mark(self, name: str, description: str) -> None:
        """Set the description of ``name``."""
        t = self.client.get_table(name)
        t.description = description
        self.client.update_table(t, ["description"])

    def drop(self, name: str) -> None:
        self.client.delete_table(name, not_found_ok=True)


def _allowed() -> set[tuple[str, str]]:
    """(project, dataset) pairs. Project ids are lower case in BigQuery; dataset names are case-sensitive."""
    out = set()
    for d in os.environ.get("CAUSILO_ALLOWED_DATASETS", "").split(","):
        project, dot, dataset = d.strip().rpartition(".")
        if dot and project and dataset:
            out.add((project.lower(), dataset))
    return out


def _table_name(value: Any, what: str, allowed: set[tuple[str, str]]) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CallError(f"{what} must be a table name, project.dataset.table.")
    name = value.strip().strip("`")
    m = _TABLE.match(name)
    if not m:
        raise CallError(f"{what} {value!r} is not a full table name, project.dataset.table.")
    if (m.group(1).lower(), m.group(2)) not in allowed:
        raise CallError(f"{what} {name} is outside the datasets this function may use "
                        f"({', '.join(sorted(f'{p}.{d}' for p, d in allowed))}).")
    # The project part folds to lower case; dataset and table names keep theirs.
    return f"{m.group(1).lower()}.{m.group(2)}.{m.group(3)}"


def _check_names(id_column: str, target: str) -> None:
    """Refuse names the result table could not be loaded with, before anything is paid for.

    The result's columns are the id, the target and names made from the target
    (``<target>_p25``, ``<target>_probability``, ``<target>_prob_<class>``, the
    class part reduced to letters, digits and "_"). A load job takes a name with
    a space or a symbol only where flexible column names are enabled, so only
    letters, digits and "_" are accepted here.
    """
    for what, name in (("id_column", id_column), ("target", target)):
        if not _COLUMN.match(name):
            raise CallError(f"{what} {name!r} has characters other than letters, digits and _, or starts with "
                            "a digit; the result table could not be written. Rename it in a view and predict "
                            "from the view.")
    # Class columns are cut to 56 characters, so _probability gives the longest name.
    longest = f"{target}_probability"
    if len(id_column) > MAX_COLUMN or len(longest) > MAX_COLUMN:
        raise CallError(f"The result column {longest[:40]}... would be longer than BigQuery's "
                        f"{MAX_COLUMN} characters; use a shorter target name.")


def _options(value: Any) -> dict[str, Any]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return {"features": None, "task": None}
    try:
        opts = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise CallError(f"options must be a JSON object, e.g. '{{\"task\": \"reg\"}}': {exc}") from exc
    if not isinstance(opts, dict):
        raise CallError("options must be a JSON object, e.g. '{\"task\": \"reg\"}'.")
    unknown = set(opts) - _OPTIONS
    if unknown:
        raise CallError(f"Unknown options: {', '.join(sorted(unknown))}; known: {', '.join(sorted(_OPTIONS))}.")
    features = opts.get("features")
    if isinstance(features, str):
        features = [f.strip() for f in features.split(",") if f.strip()]
    if features is not None and not (isinstance(features, list) and all(isinstance(f, str) for f in features)):
        raise CallError("options.features must be a list of column names or a comma-separated string.")
    task = opts.get("task")
    if task not in (None, "clf", "reg"):
        raise CallError("options.task must be 'clf' or 'reg'.")
    return {"features": features, "task": task}


def source_version(info: TableInfo) -> list[Any]:
    """What changes when the source does: its last-modified time, and a view's definition."""
    modified = info.modified.isoformat() if hasattr(info.modified, "isoformat") else info.modified
    return [modified, info.view_query]


def fingerprint(source: str, target: str, id_column: str | None, output: str, opts: dict[str, Any],
                version: Any = None) -> str:
    """The same call on the same source always gets the same fingerprint, whatever the spacing of its JSON.

    ``version`` is :func:`source_version` of the source table, so a call made
    after the source changed is a different call.
    """
    canon = json.dumps([source, target, id_column, output, opts, version], sort_keys=True, default=str)
    return hashlib.sha256(canon.encode()).hexdigest()[:32]


def _pending(fp: str) -> str:
    return f"{MARK}; call {fp}; running since {int(time.time())}"


def _done(fp: str, summary: str) -> str:
    return f"{MARK}; call {fp}; done; {summary}"


def _failed(fp: str, message: str) -> str:
    return f"{MARK}; call {fp}; failed; {message}"


def _state(description: str, fp: str) -> tuple[str, Any] | None:
    """('done', summary), ('failed', message) or ('running', started) for a table this call made;
    None for anything else."""
    m = re.match(rf"^{re.escape(MARK)}; call ([0-9a-f]+); (done; (.*)|failed; (.*)|running since (\d+))$",
                 description, re.S)
    if not m or m.group(1) != fp:
        return None
    if m.group(3) is not None:
        return ("done", m.group(3))
    if m.group(4) is not None:
        return ("failed", m.group(4))
    return ("running", int(m.group(5)))


def _causilo_retryable(exc: Exception) -> bool:
    """A Causilo refusal that charged nothing and may clear: unavailable, overloaded, rate limited."""
    try:
        import causilo_client
    except ImportError:  # pragma: no cover
        return False
    kinds = tuple(getattr(causilo_client, n) for n in ("CausiloUnavailable", "CausiloOverloaded")
                  if hasattr(causilo_client, n))
    return bool(kinds) and isinstance(exc, kinds)


def _causilo_charged(exc: Exception) -> bool:
    """A failure after the API took the work: a timeout or an answer cut off."""
    try:
        import causilo_client
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, getattr(causilo_client, "CausiloTimeout", ()))


def _bigquery_retryable(exc: Exception) -> bool:
    try:
        from google.api_core import exceptions
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, (exceptions.TooManyRequests, exceptions.InternalServerError,
                            exceptions.ServiceUnavailable, exceptions.GatewayTimeout))


def _lookup(wh: Warehouse, name: str) -> TableInfo | None:
    try:
        return wh.table(name)
    except Exception as exc:
        raise CallError(f"Could not look up {name}: {exc}", retry=_bigquery_retryable(exc)) from exc


def _wait_for(wh: Warehouse, output: str, fp: str, *, sleep: Callable[[float], None], clock: Callable[[], float]
              ) -> str | None:
    """The summary once the attempt holding ``output`` is done; None if it gave the table up."""
    deadline = clock() + WAIT_S
    loaded_reads = 0
    while True:
        info = _lookup(wh, output)
        if info is None:
            return None
        state = _state(info.description, fp)
        if state is None:
            raise CallError(f"Table {output} already exists; drop it or name another output table.")
        if state[0] == "done":
            return state[1]
        if state[0] == "failed":
            raise CallError(f"An earlier attempt of this call failed after it may have been charged, and left "
                            f"{output} empty; drop that table and run the query again. It said: {state[1]}")
        stale = time.time() - state[1] > STALE_S
        if info.num_rows:
            # The rows are in, and the description is set just after the load
            # (Warehouse.write retries it for a few seconds). Only once it has
            # stayed unset is the summary taken to be lost.
            loaded_reads += 1
            if stale or loaded_reads > LOADED_READS:
                return f"wrote {output} (an earlier run of this call; its summary could not be recorded)"
            sleep(LOADED_POLL_S)
            continue
        if stale:
            raise CallError(f"An earlier attempt of this call was cut off and left {output} empty; "
                            "drop that table and run the query again.")
        if clock() >= deadline:
            raise CallError(f"Still predicting into {output}.", retry=True)
        sleep(POLL_S)


def predict_call(args: list[Any], wh: Warehouse, *, client: Any = None, session_user: str = "",
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> str:
    """Answer one call: ``[source, target, id_column, output_table]`` and an optional ``options``."""
    if len(args) not in (4, 5):
        raise CallError(f"Expected 4 or 5 arguments (source, target, id_column, output_table[, options]), "
                        f"got {len(args)}.")
    allowed = _allowed()
    if not allowed:
        raise CallError("CAUSILO_ALLOWED_DATASETS is not set on the service; nothing may be read.")
    source = _table_name(args[0], "source_table", allowed)
    output = _table_name(args[3], "output_table", allowed)
    target, id_column = args[1], args[2]
    if not isinstance(target, str) or not target:
        raise CallError("target must be a column name.")
    if id_column is None:
        raise CallError("id_column is required: a table read through the BigQuery API has no fixed row order, "
                        "so a row_number could not be joined back to it.")
    if not isinstance(id_column, str) or not id_column:
        raise CallError("id_column must be a column name.")
    opts = _options(args[4] if len(args) == 5 else None)
    if source == output:  # both normalized: the project folded to lower case, the rest as given
        raise CallError("output_table is the table being read; write the result to another table.")
    try:
        check_id_column(id_column, target)
    except CausiloDBError as exc:
        raise CallError(str(exc)) from exc
    _check_names(id_column, target)

    # Everything that can be refused is refused before the claim, and the claim
    # comes before anything is paid for.
    while True:
        info = _lookup(wh, source)
        if info is None:
            raise CallError(f"No table {source}, or this function's service account cannot read it.")
        fp = fingerprint(source, target, id_column, output, opts, source_version(info))
        if _lookup(wh, output) is not None:
            done = _wait_for(wh, output, fp, sleep=sleep, clock=clock)
            if done is not None:
                return done
            continue  # the other attempt failed and let the table go: try ourselves
        missing = [c for c in [target, id_column, *(opts["features"] or [])]
                   if c is not None and c not in info.columns]
        if missing:
            raise CallError(f"No such columns in {source}: {', '.join(missing)}.")
        limit = int(os.environ.get("CAUSILO_MAX_SOURCE_ROWS", DEFAULT_MAX_SOURCE_ROWS))
        if info.num_rows is not None and info.num_rows > limit:
            raise CallError(f"{source} has {info.num_rows:,} rows, more than this service reads "
                            f"(CAUSILO_MAX_SOURCE_ROWS={limit:,}); predict from a smaller table.")
        try:
            wh.claim(output, _pending(fp))
        except AlreadyExists:
            continue  # a copy of this call, or something else, got there first
        except Exception as exc:
            raise CallError(f"Could not create {output}: {exc}", retry=_bigquery_retryable(exc)) from exc
        break

    try:
        return _predict_into(wh, source, info, output, target, id_column, opts, fp, limit,
                             client=client, session_user=session_user)
    except CallError as exc:
        if exc.keep_claim:
            # Copies of this call (BigQuery repeats calls) fail at once rather than wait.
            try:
                wh.mark(output, _failed(fp, str(exc)[:500]))
            except Exception:  # pragma: no cover
                log.exception("could not mark %s as failed", output)
        else:
            # Nothing was written: let the table go so a retry or a rerun can predict.
            try:
                wh.drop(output)
            except Exception:  # pragma: no cover
                log.exception("could not drop the claim on %s", output)
        raise


def _predict_into(wh, source, info, output, target, id_column, opts, fp, limit, *, client, session_user) -> str:
    if opts["features"] is not None:
        columns = list(dict.fromkeys([id_column, target, *opts["features"]]))
    else:
        columns = info.columns
    try:
        df = wh.read(source, info, columns)
    except Exception as exc:
        raise CallError(f"Could not read {source}: {exc}", retry=_bigquery_retryable(exc)) from exc
    if df.empty:
        raise CallError(f"No rows were read from {source}.")
    if len(df) > limit:
        raise CallError(f"{source} has {len(df):,} rows, more than this service reads "
                        f"(CAUSILO_MAX_SOURCE_ROWS={limit:,}).")

    log.info("predicting %s.%s for %s into %s", source, target, session_user or "an unknown user", output)
    try:
        pred = predict_frame(df, target, features=opts["features"], exclude=[id_column],
                             task=opts["task"], client=client)
    except PartialPredictionError as exc:
        # Earlier calls were charged: a retry would pay for them again.
        raise CallError(f"Causilo: {exc} Drop {output} before running again.", keep_claim=True) from exc
    except CausiloDBError as exc:
        raise CallError(str(exc)) from exc
    except Exception as exc:
        raise CallError(f"Causilo: {exc}", retry=_causilo_retryable(exc), keep_claim=_causilo_charged(exc)) from exc

    result = pred.frame.copy()
    result.insert(0, id_column, df[id_column].reset_index(drop=True).iloc[result["row_number"]].to_numpy())
    result = result.drop(columns=["row_number"])
    kind = "regression" if pred.task == "reg" else "classification"
    summary = (f"predicted {pred.n_query:,} blank cells of {target} ({kind}) from {pred.n_context:,} rows "
               f"in {pred.calls} call{'s' if pred.calls != 1 else ''}; wrote {output}")
    try:
        wh.write(output, result, _done(fp, summary))
    except NotMarked as exc:
        # The rows are in place; only the record of this call is missing.
        log.warning("wrote %s but could not set its description: %s", output, exc)
        return summary
    except Exception as exc:
        # Paid for: never a status BigQuery retries into a second charge.
        raise CallError(f"The prediction was made but could not be written to {output}: {exc}",
                        keep_claim=True) from exc
    return summary


def handle(body: Any, wh: Warehouse, *, client: Any = None, **waits: Any) -> tuple[int, dict[str, Any]]:
    """The remote function contract: (status, JSON reply)."""
    if not isinstance(body, dict) or not isinstance(body.get("calls"), list):
        return 400, {"errorMessage": "Expected a JSON body with a 'calls' list."}
    user = body.get("sessionUser") or ""
    answers: dict[str, str] = {}
    replies: list[str] = []
    for args in body["calls"]:
        if not isinstance(args, list):
            return 400, {"errorMessage": "Each call must be a list of arguments."}
        key = json.dumps(args, sort_keys=True, default=str)
        if key not in answers:
            try:
                answers[key] = predict_call(args, wh, client=client, session_user=user, **waits)
            except CallError as exc:
                msg = str(exc)
                # The reply must stay under 1 KB.
                if len(msg.encode()) >= 1000:
                    msg = msg.encode()[:990].decode(errors="ignore") + " ..."
                return (503 if exc.retry else 400), {"errorMessage": msg}
        replies.append(answers[key])
    return 200, {"replies": replies}


def make_app(warehouse: Callable[[], Warehouse] = Warehouse, client: Any = None):
    """A WSGI app answering POST with :func:`handle`."""
    state: dict[str, Warehouse] = {}

    def app(environ, start_response):
        if environ.get("REQUEST_METHOD") != "POST":
            start_response("405 Method Not Allowed", [("Content-Type", "application/json")])
            return [b'{"errorMessage": "POST only."}']
        try:
            size = int(environ.get("CONTENT_LENGTH") or 0)
            body = json.loads(environ["wsgi.input"].read(size) or b"null")
        except (ValueError, json.JSONDecodeError):
            status, reply = 400, {"errorMessage": "The body is not JSON."}
        else:
            try:
                if "wh" not in state:
                    state["wh"] = warehouse()
                status, reply = handle(body, state["wh"], client=client)
            except Exception as exc:  # a bug: a 400, so BigQuery does not retry it into a charge
                log.exception("unexpected error")
                status, reply = 400, {"errorMessage": f"Unexpected error: {type(exc).__name__}"}
        data = json.dumps(reply).encode()
        reason = {200: "OK", 400: "Bad Request", 503: "Service Unavailable"}.get(status, "Error")
        start_response(f"{status} {reason}", [("Content-Type", "application/json"),
                                              ("Content-Length", str(len(data)))])
        return [data]

    return app


logging.basicConfig(level=logging.INFO)
if os.environ.get("CAUSILO_API_KEY"):
    # A key pasted into Secret Manager often ends in a newline, which cannot travel in a header.
    os.environ["CAUSILO_API_KEY"] = os.environ["CAUSILO_API_KEY"].strip()
app = make_app()
