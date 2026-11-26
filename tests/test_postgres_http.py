"""sql/postgres/install_http.sql, run in a real PostgreSQL that has the http extension.

Skipped unless CAUSILO_TEST_PG_HTTP is a libpq connection string for a database
where the installer may be run and tables in a schema ``cx_http_test`` created,
for example the image built from docker/postgres-http.Dockerfile:

    docker run -d --name cx-http -e POSTGRES_PASSWORD=pw -p 127.0.0.1:55432:5432 causilo-postgres-http
    CAUSILO_TEST_PG_HTTP="host=127.0.0.1 port=55432 user=postgres password=pw" \\
        .venv/bin/python -m pytest tests/test_postgres_http.py

The function is pointed (``causilo.endpoint``) at a stand-in API that this test
starts on 127.0.0.1; the database reaches it at CAUSILO_TEST_PG_HTTP_HOST,
``host.docker.internal`` by default. Nothing reaches the real API.

The stand-in answers with tests/fakes.py, and the checks compare the function
with ``predict_frame`` fed the same table the way install.sql feeds it: the
Parquet tables sent must match column for column, dtype for dtype, and so must
the rows returned.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from causilo_db import core
from tests.fakes import FakeCausilo

DSN = os.environ.get("CAUSILO_TEST_PG_HTTP")
psycopg = pytest.importorskip("psycopg") if DSN else None
pq = pytest.importorskip("pyarrow.parquet")
pytestmark = pytest.mark.skipif(not DSN, reason="CAUSILO_TEST_PG_HTTP is not set")

INSTALL = Path(__file__).resolve().parent.parent / "sql" / "postgres" / "install_http.sql"
SCHEMA = "cx_http_test"


class Recording(FakeCausilo):
    """FakeCausilo that keeps the tables it was sent."""

    def __init__(self) -> None:
        super().__init__()
        self.tables: list[tuple[pd.DataFrame, pd.DataFrame, dict]] = []

    def predict_with_metadata(self, context, query, *, target, model, output_type=None, quantiles=None):
        self.tables.append((context, query, {"target": target, "model": model, "output_type": output_type,
                                             "quantiles": quantiles}))
        return super().predict_with_metadata(context, query, target=target, model=model,
                                             output_type=output_type, quantiles=quantiles)


class StandIn:
    """The API, for the database to call: answers /v1/predict with FakeCausilo."""

    def __init__(self) -> None:
        self.fake = Recording()
        self.cancels: list[str] = []
        self.headers: list[dict] = []
        self.script: list[tuple[int, dict, dict]] = []  # (status, headers, body) answered before the fake
        self.delay = 0.0
        self.fail_from: int | None = None  # calls from this one on (0-based) are refused with a 422
        self.short_from: int | None = None  # calls from this one on answer one row too few
        self.hang_up = False  # /v1/predict closes the connection without an answer
        self.cancel_delay = 0.0
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.path == "/v1/cancel":
                    stand_in.cancels.append(self.headers.get("X-Causilo-Cancel-Key", ""))
                    if stand_in.cancel_delay:
                        time.sleep(stand_in.cancel_delay)
                    return self._send(200, {}, {"found": True, "refunded": 0, "started": False})
                stand_in.headers.append(dict(self.headers))
                if stand_in.hang_up:
                    self.close_connection = True
                    return
                if stand_in.delay:
                    time.sleep(stand_in.delay)
                if stand_in.fail_from is not None and len(stand_in.headers) > stand_in.fail_from:
                    return self._send(422, {}, {"code": "validation_failed", "detail": "refused",
                                                "retryable": False})
                if stand_in.script:
                    return self._send(*stand_in.script.pop(0))
                parts = _multipart(self.headers["Content-Type"], body)
                params = json.loads(parts["params"])
                context = pq.read_table(BytesIO(parts["context"])).to_pandas()
                query = pq.read_table(BytesIO(parts["query"])).to_pandas()
                pred, meta = stand_in.fake.predict_with_metadata(
                    context, query, target=params["target"], model=params["model"],
                    output_type=params.get("output_type"), quantiles=params.get("quantiles"))
                rows = np.asarray(pred).tolist()
                if stand_in.short_from is not None and len(stand_in.headers) > stand_in.short_from:
                    rows = rows[:-1]
                self._send(200, {}, {"prediction": rows, "metadata": meta})

            def _send(self, status, headers, doc):
                raw = json.dumps(doc).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                for k, v in headers.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


def _multipart(content_type: str, body: bytes) -> dict[str, bytes]:
    boundary = content_type.split("boundary=")[1].encode()
    out = {}
    for chunk in body.split(b"--" + boundary)[1:-1]:
        head, _, content = chunk.partition(b"\r\n\r\n")
        name = head.split(b'name="')[1].split(b'"')[0].decode()
        out[name] = content[:-2]  # the CRLF before the next boundary
    return out


@pytest.fixture(scope="module")
def db():
    con = psycopg.connect(DSN, autocommit=True)
    con.execute(INSTALL.read_text())
    con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    con.execute(f"CREATE SCHEMA {SCHEMA}")
    yield con
    con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    con.execute(INSTALL.read_text())
    con.close()


@pytest.fixture
def api(db):
    stand_in = StandIn()
    host = os.environ.get("CAUSILO_TEST_PG_HTTP_HOST", "host.docker.internal")
    db.execute("SELECT set_config('causilo.endpoint', %s, false)", (f"http://{host}:{stand_in.port}",))
    db.execute("SELECT set_config('causilo.api_key', 'csl_test', false)")
    db.execute("SELECT set_config('causilo.timeout_ms', '', false)")
    yield stand_in
    stand_in.server.shutdown()


def _create(db, name: str, ddl: str, rows: list[tuple]) -> None:
    db.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{name}")
    db.execute(f"CREATE TABLE {SCHEMA}.{name} ({ddl})")
    if rows:
        marks = ", ".join(["%s"] * len(rows[0]))
        with db.cursor() as cur:
            cur.executemany(f"INSERT INTO {SCHEMA}.{name} VALUES ({marks})", rows)


def _as_install_sql_reads(db, name: str, id_column: str | None) -> tuple[pd.DataFrame, str]:
    """The table as install.sql hands it to predict_frame."""
    with db.cursor() as cur:
        cur.execute(f"SELECT {'ctid::text AS __causilo_ctid, ' if id_column is None else ''}* FROM {SCHEMA}.{name}")
        names = [d.name for d in cur.description]
        types = {d.name: d.type_code for d in cur.description}
        df = pd.DataFrame.from_records(cur.fetchall(), columns=names)
    for col in names:
        if col in (id_column, "__causilo_ctid"):
            continue
        if types[col] in (1082, 1114, 1184):
            df[col] = pd.to_datetime(df[col].map(lambda v: None if v is None else str(v)),
                                     utc=True, errors="coerce", format="ISO8601")
        elif types[col] == 700:  # PL/Python hands a real over as its exact value
            df[col] = df[col].map(lambda v: None if v is None else float(np.float32(v)))
        elif df[col].dtype == object and df[col].map(lambda v: isinstance(v, decimal.Decimal)).any():
            df[col] = df[col].map(lambda v: None if v is None else float(v))
    return df, id_column or "__causilo_ctid"


def _expected(db, name, target, id_column=None, task=None, client=None):
    df, ids = _as_install_sql_reads(db, name, id_column)
    client = client or Recording()
    pred = core.predict_frame(df, target, task=task, exclude=[c for c in ("__causilo_ctid", id_column) if c],
                              client=client)
    rows = []
    for r in pred.frame.to_dict("records"):
        row_id = str(df[ids].iloc[int(r["row_number"])])
        if pred.task == "reg":
            rows.append((row_id, str(r[target]), None, float(r[f"{target}_p25"]), float(r[f"{target}_p50"]),
                         float(r[f"{target}_p75"])))
        else:
            rows.append((row_id, core._label(r[target]), float(r[f"{target}_probability"]), None, None, None))
    return rows, client, pred


def _run(db, name, target, id_column=None, task=None):
    return db.execute("SELECT * FROM causilo.predict(%s, %s, %s, %s)",
                      (f"{SCHEMA}.{name}", target, id_column, task)).fetchall()


def _same_tables(sent, expected) -> None:
    assert len(sent) == len(expected)
    for (c1, q1, p1), (c2, q2, p2) in zip(sent, expected):
        pd.testing.assert_frame_equal(c1.reset_index(drop=True), c2.reset_index(drop=True))
        pd.testing.assert_frame_equal(q1.reset_index(drop=True), q2.reset_index(drop=True))
        assert p1 == p2


MIXED = ("id int, f_int int, f_int_blank bigint, f_float float8, f_real real, f_num numeric, f_bool bool, "
         "f_bool_blank bool, f_text text, f_date date, f_ts timestamp, f_tstz timestamptz, f_empty text, "
         "y float8")


def _mixed_rows(n=40):
    rows = []
    for i in range(n):
        rows.append((
            i, i * 3 - 7, None if i % 5 == 0 else i, None if i % 7 == 0 else (float("nan") if i % 11 == 0 else i / 3),
            i * 0.1, decimal.Decimal(i) / 8, i % 2 == 0, None if i % 6 == 0 else i % 3 == 0,
            None if i % 4 == 0 else f"c{i % 3}", dt.date(2020, 1, 1) + dt.timedelta(days=i),
            dt.datetime(2021, 5, 1, 12) + dt.timedelta(hours=i),
            dt.datetime(2021, 5, 1, 12, tzinfo=dt.timezone.utc) + dt.timedelta(minutes=i), None,
            None if i % 3 == 0 else (float("nan") if i == 4 else i * 1.7 + 0.25)))
    return rows


def test_regression_sends_what_install_sql_sends(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    got = _run(db, "mixed", "y", "id")
    want, client, pred = _expected(db, "mixed", "y", "id")
    assert got == want
    assert pred.task == "reg" and len(got) == pred.n_query == 15
    _same_tables(api.fake.tables, client.tables)
    context, query, _ = api.fake.tables[0]
    assert str(context["f_int"].dtype) == "int64" and str(context["f_int_blank"].dtype) == "float64"
    assert str(context["f_bool"].dtype) == "bool" and "f_empty" not in context


def test_without_an_id_the_row_id_is_the_ctid(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    got = _run(db, "mixed", "y")
    want, client, _ = _expected(db, "mixed", "y")
    assert got == want and got[0][0].startswith("(")
    _same_tables(api.fake.tables, client.tables)


@pytest.mark.parametrize("target,task", [("f_text", None), ("f_int", "clf"), ("f_bool", None), ("f_real", "clf")])
def test_classification_sends_what_install_sql_sends(db, api, target, task):
    rows = _mixed_rows()
    col = MIXED.split(", ").index(next(c for c in MIXED.split(", ") if c.startswith(target + " ")))
    rows = [r[:col] + (None if i % 3 == 0 else r[col],) + r[col + 1:] for i, r in enumerate(rows)]
    _create(db, "mixed", MIXED, rows)
    got = _run(db, "mixed", target, "id", task)
    want, client, pred = _expected(db, "mixed", target, "id", task)
    assert pred.task == "clf"
    assert got == want
    _same_tables(api.fake.tables, client.tables)


def test_whole_numbers_with_few_values_are_classes(db, api):
    _create(db, "few", "id int, x float8, y int", [(i, i / 2, None if i % 4 == 0 else i % 3) for i in range(30)])
    got = _run(db, "few", "y", "id")
    want, client, pred = _expected(db, "few", "y", "id")
    assert pred.task == "clf" and got == want
    assert {r[1] for r in got} <= {"0", "1", "2"}
    _same_tables(api.fake.tables, client.tables)


def test_a_view_works_with_an_id(db, api):
    _create(db, "base", "id int, a float8, b text, y float8",
            [(i, i * 1.5, f"b{i % 4}", None if i % 3 == 0 else i * 2.5 + 0.1) for i in range(60)])
    db.execute(f"CREATE OR REPLACE VIEW {SCHEMA}.v AS SELECT id, a, y FROM {SCHEMA}.base")
    got = _run(db, "v", "y", "id")
    want, client, _ = _expected(db, "v", "y", "id")
    assert got == want
    _same_tables(api.fake.tables, client.tables)
    assert list(api.fake.tables[0][1].columns) == ["a"]


def test_a_large_table_is_split_like_predict_frame(db, api, monkeypatch):
    """A budget of 60 cells per call: the context is sampled and the rows go in several calls."""
    small = INSTALL.read_text().replace("max_cells constant bigint := 7000000;", "max_cells constant bigint := 60;")
    assert small != INSTALL.read_text()
    db.execute(small)
    try:
        _create(db, "big", "id int, a float8, b float8, y float8",
                [(i, float(i), float(i % 7), None if i % 5 == 0 else i * 0.5 + 0.1) for i in range(100)])
        got = _run(db, "big", "y", "id")
        monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 60)
        want, client, pred = _expected(db, "big", "y", "id")
    finally:
        db.execute(INSTALL.read_text())
    assert pred.calls > 1 and len(api.fake.tables) == pred.calls
    for (c1, q1, _), (c2, q2, _) in zip(api.fake.tables, client.tables):
        assert c1.shape == c2.shape and q1.shape == q2.shape
        assert list(c1.columns) == list(c2.columns)
    # The sampled context differs (another random draw), so only the rows predicted are compared.
    assert [r[0] for r in got] == [r[0] for r in want]
    assert sorted(int(r[0]) for r in got) == [i for i in range(100) if i % 5 == 0]


@pytest.mark.parametrize("args,message", [
    (("mixed", "y", "y", None), "cannot be the target"),
    (("mixed", "y", "y_p50", None), "clashes with a result column"),
    (("mixed", "y", "Y_Prob_a", None), "clashes with a result column"),
    (("mixed", "y", "row_number", None), "clashes with a result column"),
    (("mixed", "nope", "id", None), "No column named nope"),
    (("mixed", "y", "nope", None), "No column named nope"),
    (("mixed", "y", "id", "regression"), "task must be"),
    (("dates", "y", "id", "reg"), "is not numeric"),
    (("mixed", "f_text", "id", "reg"), "is not numeric"),
    (("full", "y", "id", None), "no blank cells"),
    (("one", "y", "id", None), "1 filled rows"),
    (("names", "y", "id", None), "looks more like a name"),
    (("empty", "y", "id", None), "No feature columns"),
])
def test_bad_arguments_are_refused_before_any_call(db, api, args, message):
    _create(db, "mixed", MIXED, _mixed_rows())
    _create(db, "full", "id int, a float8, y float8", [(i, i, i) for i in range(5)])
    _create(db, "one", "id int, a float8, y float8", [(0, 1, 1), (1, 2, None)])
    _create(db, "names", "id int, a float8, y text", [(i, i, None if i == 0 else f"n{i}") for i in range(300)])
    _create(db, "dates", "id int, a float8, y date", [(i, i, None if i == 0 else dt.date(2020, 1, i)) for i in range(5)])
    _create(db, "empty", "id int, a float8, y float8", [(i, None, None if i == 0 else i) for i in range(5)])
    with pytest.raises(psycopg.Error, match=message):
        _run(db, *args)
    assert api.headers == []


def test_no_key_is_refused_before_any_call(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    db.execute("SELECT set_config('causilo.api_key', '', false)")
    with pytest.raises(psycopg.Error, match="Set a Causilo API key"):
        _run(db, "mixed", "y", "id")
    assert api.headers == []


def test_the_key_goes_in_the_authorization_header(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    _run(db, "mixed", "y", "id")
    h = {k.lower(): v for k, v in api.headers[0].items()}
    assert h["authorization"] == "Bearer csl_test"
    assert h["accept"] == "application/json" and len(h["x-causilo-cancel-key"]) == 32


def test_a_retryable_failure_is_tried_again(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    api.script = [(503, {"Retry-After": "1"}, {"code": "not_ready", "retryable": True})]
    got = _run(db, "mixed", "y", "id")
    assert len(api.headers) == 2 and len(got) == 15


def test_a_refused_table_is_not_tried_again(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    api.script = [(422, {}, {"code": "validation_failed", "detail": "column a is odd", "retryable": False,
                             "request_id": "req_1"})]
    with pytest.raises(psycopg.Error, match="422 validation_failed: column a is odd \\(request req_1\\)"):
        _run(db, "mixed", "y", "id")
    assert len(api.headers) == 1


def test_a_502_is_not_tried_again_even_when_marked_retryable(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    api.script = [(502, {}, {"code": "upstream_unavailable", "retryable": True})]
    with pytest.raises(psycopg.Error, match="502"):
        _run(db, "mixed", "y", "id")
    assert len(api.headers) == 1


def test_a_call_that_times_out_is_cancelled_and_not_retried(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    db.execute("SELECT set_config('causilo.timeout_ms', '500', false)")
    api.delay = 2.0
    with pytest.raises(psycopg.Error, match="did not complete"):
        _run(db, "mixed", "y", "id")
    time.sleep(2.5)
    assert len(api.headers) == 1
    assert api.cancels == [{k.lower(): v for k, v in api.headers[0].items()}["x-causilo-cancel-key"]]


def test_a_statement_timeout_cancels_the_call_in_flight(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    api.delay = 3.0
    db.execute("SET statement_timeout = '700ms'")
    try:
        with pytest.raises(psycopg.errors.QueryCanceled):
            _run(db, "mixed", "y", "id")
    finally:
        db.execute("RESET statement_timeout")
    time.sleep(3.5)
    assert len(api.headers) == 1
    assert api.cancels == [{k.lower(): v for k, v in api.headers[0].items()}["x-causilo-cancel-key"]]


def test_a_later_call_failing_says_how_many_calls_were_charged(db, api):
    small = INSTALL.read_text().replace("max_cells constant bigint := 7000000;", "max_cells constant bigint := 60;")
    db.execute(small)
    try:
        _create(db, "big", "id int, a float8, b float8, y float8",
                [(i, float(i), float(i % 7), None if i % 5 == 0 else i * 0.5 + 0.1) for i in range(100)])
        api.fail_from = 1
        with pytest.raises(psycopg.Error, match=r"refused \(call 2 of 2; the 1 call before it completed "
                                                r"and was charged, 10 of 20 rows"):
            _run(db, "big", "y", "id")
        assert len(api.headers) == 2
        api.headers.clear()
        api.fail_from = 0
        with pytest.raises(psycopg.Error, match="refused") as info:
            _run(db, "big", "y", "id")
        assert "charged" not in str(info.value)
    finally:
        db.execute(INSTALL.read_text())


def test_an_unusable_answer_counts_its_call_as_charged(db, api):
    small = INSTALL.read_text().replace("max_cells constant bigint := 7000000;", "max_cells constant bigint := 60;")
    db.execute(small)
    try:
        _create(db, "big", "id int, a float8, b float8, y float8",
                [(i, float(i), float(i % 7), None if i % 5 == 0 else i * 0.5 + 0.1) for i in range(100)])
        api.short_from = 1
        with pytest.raises(psycopg.Error, match=r"answered 9 rows for 10 sent\. \(call 2 of 2 was answered but its "
                                                r"answer could not be used; the 2 calls up to and including it "
                                                r"were charged, 20 of 20 rows"):
            _run(db, "big", "y", "id")
        api.headers.clear()
        api.short_from = 0
        with pytest.raises(psycopg.Error, match=r"answered 9 rows for 10 sent\. \(call 1 of 2 was answered but its "
                                                r"answer could not be used; the 1 call up to and including it "
                                                r"was charged, 10 of 20 rows"):
            _run(db, "big", "y", "id")
        assert len(api.headers) == 1
    finally:
        db.execute(INSTALL.read_text())


def test_a_failed_call_waits_at_most_5_seconds_for_its_cancel(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    api.hang_up = True
    api.cancel_delay = 9.0
    began = time.monotonic()
    with pytest.raises(psycopg.Error, match="did not complete"):
        _run(db, "mixed", "y", "id")
    assert time.monotonic() - began < 7.5
    assert len(api.headers) == 1 and len(api.cancels) == 1


def test_a_cancel_during_the_cancel_request_is_not_swallowed(db, api):
    _create(db, "mixed", MIXED, _mixed_rows())
    api.hang_up = True
    api.cancel_delay = 9.0
    db.execute("SET statement_timeout = '1500ms'")
    began = time.monotonic()
    try:
        with pytest.raises(psycopg.errors.QueryCanceled):
            _run(db, "mixed", "y", "id")
    finally:
        db.execute("RESET statement_timeout")
    assert time.monotonic() - began < 4
