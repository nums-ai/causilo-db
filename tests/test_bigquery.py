"""The BigQuery remote function service (bigquery/main.py), with a fake warehouse and a fake client."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from .fakes import FakeCausilo
from .test_core import table

_spec = importlib.util.spec_from_file_location("causilo_bigquery_main",
                                               Path(__file__).resolve().parent.parent / "bigquery" / "main.py")
bq = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = bq
_spec.loader.exec_module(bq)

SRC = "proj-1.shop.mixes"
OUT = "proj-1.shop.mixes_pred"
CALL = [SRC, "strength", "mix_id", OUT]
FP = bq.fingerprint(SRC, "strength", "mix_id", OUT, {"features": None, "task": None}, [None, None])


class FakeWarehouse:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self.tables = {k: (v, "") for k, v in tables.items()}
        self.reads: list[tuple[str, list[str]]] = []
        self.claims: list[str] = []
        self.writes: list[str] = []
        self.drops: list[str] = []
        self.fail_write: Exception | None = None
        self.fail_mark = False
        self.on_claim = None
        self.modified: dict[str, object] = {}

    def table(self, name):
        if name not in self.tables:
            return None
        df, desc = self.tables[name]
        return bq.TableInfo(kind="TABLE", num_rows=len(df), description=desc, columns=list(df.columns),
                            modified=self.modified.get(name))

    def read(self, name, info, columns):
        self.reads.append((name, list(columns)))
        return self.tables[name][0][columns].copy()

    def claim(self, name, description):
        if self.on_claim is not None:
            self.on_claim, hook = None, self.on_claim
            hook()
        if name in self.tables:
            raise bq.AlreadyExists(name)
        self.claims.append(name)
        self.tables[name] = (pd.DataFrame(), description)

    def write(self, name, df, description):
        if self.fail_write is not None:
            raise self.fail_write
        self.writes.append(name)
        if self.fail_mark:
            # The load went through; setting the description did not.
            self.tables[name] = (df.copy(), self.tables[name][1])
            raise bq.NotMarked("description update failed")
        self.tables[name] = (df.copy(), description)

    def mark(self, name, description):
        self.tables[name] = (self.tables[name][0], description)

    def drop(self, name):
        self.drops.append(name)
        self.tables.pop(name, None)


@pytest.fixture(autouse=True)
def _allowed(monkeypatch):
    monkeypatch.setenv("CAUSILO_ALLOWED_DATASETS", "proj-1.shop, proj-1.out")


@pytest.fixture
def wh():
    df = table()
    df.insert(0, "mix_id", np.arange(100, 100 + len(df)))
    return FakeWarehouse({SRC: df})


def body(*calls):
    return {"requestId": "r1", "caller": "//bigquery.googleapis.com/projects/p/jobs/j",
            "sessionUser": "someone@example.com", "calls": [list(c) for c in calls]}


class Clock:
    """A clock that moves only when the service sleeps."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps = 0
        self.during_sleep = None

    def __call__(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.now += s
        self.sleeps += 1
        if self.during_sleep:
            self.during_sleep()


def test_predicts_writes_and_summarizes(wh):
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client)
    assert status == 200, reply
    assert reply["replies"] == [f"predicted 20 blank cells of strength (regression) from 40 rows in 1 call; wrote {OUT}"]
    out, desc = wh.tables[OUT]
    assert list(out.columns[:2]) == ["mix_id", "strength"] and "row_number" not in out.columns
    assert out["mix_id"].tolist() == list(range(140, 160))
    assert desc.startswith(f"causilo-db prediction; call {FP}; done; predicted 20")
    assert wh.claims == [OUT] and len(client.calls) == 1


def test_duplicate_calls_in_one_batch_are_predicted_once(wh):
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL, CALL, CALL), wh, client=client)
    assert status == 200 and len(reply["replies"]) == 3 and len(set(reply["replies"])) == 1
    assert len(client.calls) == 1 and wh.writes == [OUT]


def test_a_retry_after_success_is_answered_without_predicting(wh):
    first = bq.handle(body(CALL), wh, client=FakeCausilo())
    client = FakeCausilo()
    again = bq.handle(body(CALL), wh, client=client)
    assert again == first and client.calls == []


def test_a_copy_running_at_the_same_time_waits_for_the_first(wh):
    # The first copy has claimed the table and is predicting; it finishes while the second waits.
    wh.tables[OUT] = (pd.DataFrame(), bq._pending(FP))
    clock = Clock()
    clock.during_sleep = lambda: wh.tables.__setitem__(OUT, (pd.DataFrame({"x": [1]}), bq._done(FP, "the summary")))
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client, sleep=clock.sleep, clock=clock)
    assert (status, reply) == (200, {"replies": ["the summary"]})
    assert client.calls == [] and clock.sleeps == 1


def test_losing_the_claim_race_waits_instead_of_predicting(wh):
    clock = Clock()
    wh.on_claim = lambda: wh.tables.__setitem__(OUT, (pd.DataFrame(), bq._pending(FP)))
    clock.during_sleep = lambda: wh.tables.__setitem__(OUT, (pd.DataFrame({"x": [1]}), bq._done(FP, "theirs")))
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client, sleep=clock.sleep, clock=clock)
    assert reply == {"replies": ["theirs"]} and client.calls == []


def test_a_copy_that_waits_too_long_asks_for_a_retry(wh):
    wh.tables[OUT] = (pd.DataFrame(), bq._pending(FP))
    clock = Clock()
    status, reply = bq.handle(body(CALL), wh, client=FakeCausilo(), sleep=clock.sleep, clock=clock)
    assert status == 503 and "Still predicting" in reply["errorMessage"]
    assert OUT in wh.tables  # not the waiting copy's table to drop


def test_a_stale_claim_is_reported(wh):
    old = f"causilo-db prediction; call {FP}; running since {int(time.time()) - 3600}"
    wh.tables[OUT] = (pd.DataFrame(), old)
    status, reply = bq.handle(body(CALL), wh, client=FakeCausilo())
    assert status == 400 and "cut off" in reply["errorMessage"]


def test_a_copy_predicts_when_the_first_gave_the_table_up(wh):
    wh.tables[OUT] = (pd.DataFrame(), bq._pending(FP))
    clock = Clock()
    clock.during_sleep = lambda: wh.tables.pop(OUT)
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client, sleep=clock.sleep, clock=clock)
    assert status == 200 and len(client.calls) == 1


def test_an_output_table_from_something_else_is_refused_before_predicting(wh):
    wh.tables[OUT] = (pd.DataFrame({"x": [1]}), "made by hand")
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client)
    assert status == 400 and "already exists" in reply["errorMessage"] and client.calls == []
    assert wh.tables[OUT][1] == "made by hand"


def test_a_different_call_into_an_existing_output_is_refused(wh):
    bq.handle(body(CALL), wh, client=FakeCausilo())
    client = FakeCausilo()
    status, reply = bq.handle(body([*CALL, '{"task": "reg"}']), wh, client=client)
    assert status == 400 and "already exists" in reply["errorMessage"] and client.calls == []


def test_a_failed_write_after_predicting_is_not_retried_and_keeps_the_claim(wh):
    wh.fail_write = RuntimeError("quota")
    status, reply = bq.handle(body(CALL), wh, client=FakeCausilo())
    assert status == 400 and "prediction was made but could not be written" in reply["errorMessage"]
    assert wh.drops == [] and OUT in wh.tables


def test_a_refusal_after_the_claim_gives_the_table_back(wh):
    status, reply = bq.handle(body([SRC, "cement", "mix_id", OUT]), wh, client=FakeCausilo())
    assert status == 400 and "no blank cells" in reply["errorMessage"]
    assert wh.drops == [OUT] and OUT not in wh.tables


def test_options_pick_features_and_task(wh):
    client = FakeCausilo()
    status, reply = bq.handle(body([SRC, "churn", "mix_id", OUT, '{"features": "cement, grade", "task": "clf"}']),
                              wh, client=client)
    assert status == 200, reply
    assert wh.reads == [(SRC, ["mix_id", "churn", "cement", "grade"])]
    assert client.calls[0]["columns"] == ["cement", "grade"] and "(classification)" in reply["replies"][0]


def test_null_id_column_is_refused_before_the_claim(wh):
    client = FakeCausilo()
    status, reply = bq.handle(body([SRC, "strength", None, OUT]), wh, client=client)
    assert status == 400 and "id_column is required" in reply["errorMessage"]
    assert client.calls == [] and wh.claims == []


def _two_calls(monkeypatch):
    # 4 columns sent (cement, grade, churn and the target): 30 rows per call,
    # 15 of context and 15 to predict, so the 20 blank cells take 2 calls.
    from causilo_db import core
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 4 * 30)


def test_a_later_call_failing_is_not_retried_into_a_second_charge(wh, monkeypatch):
    causilo_client = pytest.importorskip("causilo_client")
    _two_calls(monkeypatch)

    class SecondDown(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            if self.calls:
                self.calls.append("failed")
                raise causilo_client.CausiloUnavailable("busy", status=503)
            return super().predict_with_metadata(*a, **k)

    client = SecondDown()
    status, reply = bq.handle(body(CALL), wh, client=client)
    assert status == 400, reply  # not 503, which BigQuery retries
    assert "Call 2 of 2 failed" in reply["errorMessage"] and "1 call before it completed" in reply["errorMessage"]
    assert OUT in wh.tables and wh.drops == [] and wh.writes == []
    # BigQuery sending the call again pays for nothing.
    again = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=again)
    assert status == 400 and again.calls == [] and "may have been charged" in reply["errorMessage"]
    assert len(client.calls) == 2


def test_an_unusable_first_answer_keeps_the_claim(wh, monkeypatch):
    _two_calls(monkeypatch)

    class Misshapen(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            _, meta = super().predict_with_metadata(*a, **k)
            return np.zeros(7), meta

    client = Misshapen()
    status, reply = bq.handle(body(CALL), wh, client=client)
    assert status == 400, reply
    assert "1 call up to and including it was charged" in reply["errorMessage"]
    assert OUT in wh.tables and wh.drops == [] and wh.writes == []
    again = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=again)
    assert status == 400 and again.calls == [] and "may have been charged" in reply["errorMessage"]


def test_a_first_call_failing_still_frees_the_table_for_a_retry(wh, monkeypatch):
    causilo_client = pytest.importorskip("causilo_client")
    _two_calls(monkeypatch)

    class Down(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            raise causilo_client.CausiloUnavailable("busy", status=503)

    status, _ = bq.handle(body(CALL), wh, client=Down())
    assert status == 503 and OUT not in wh.tables


def test_a_rerun_after_the_source_changed_is_refused(wh):
    import datetime as dt

    wh.modified[SRC] = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
    first = bq.handle(body(CALL), wh, client=FakeCausilo())
    assert first[0] == 200
    assert bq.handle(body(CALL), wh, client=FakeCausilo()) == first  # unchanged: the same summary
    wh.modified[SRC] = dt.datetime(2026, 11, 2, tzinfo=dt.timezone.utc)
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client)
    assert status == 400 and "already exists" in reply["errorMessage"] and client.calls == []


def test_a_view_definition_is_part_of_the_fingerprint():
    a = bq.TableInfo(kind="VIEW", num_rows=None, description="", columns=[], view_query="SELECT 1")
    b = bq.TableInfo(kind="VIEW", num_rows=None, description="", columns=[], view_query="SELECT 2")
    opts = {"features": None, "task": None}
    assert (bq.fingerprint(SRC, "y", "id", OUT, opts, bq.source_version(a))
            != bq.fingerprint(SRC, "y", "id", OUT, opts, bq.source_version(b)))


@pytest.mark.parametrize("id_column, target, says", [
    ("mix id", "strength", "id_column 'mix id'"),
    ("mix_id", "str ength", "target 'str ength'"),
    ("mix_id", "1strength", "starts with a digit"),
    ("mix_id", "s" * 290, "longer than"),
])
def test_names_the_result_could_not_be_loaded_with_are_refused_before_the_claim(wh, id_column, target, says):
    df = wh.tables[SRC][0].rename(columns={"mix_id": id_column, "strength": target})
    wh.tables[SRC] = (df, "")
    client = FakeCausilo()
    status, reply = bq.handle(body([SRC, target, id_column, OUT]), wh, client=client)
    assert status == 400 and says in reply["errorMessage"], reply
    assert client.calls == [] and wh.claims == []


def test_a_failed_description_update_still_returns_the_summary(wh):
    wh.fail_mark = True
    status, reply = bq.handle(body(CALL), wh, client=FakeCausilo())
    assert status == 200 and reply["replies"][0].startswith("predicted 20 blank cells")
    assert len(wh.tables[OUT][0]) == 20
    # A later copy finds the rows and, once the description stays unset, answers
    # without predicting or calling the table cut off.
    client = FakeCausilo()
    clock = Clock()
    status, reply = bq.handle(body(CALL), wh, client=client, sleep=clock.sleep, clock=clock)
    assert status == 200 and "could not be recorded" in reply["replies"][0] and client.calls == []
    assert clock.sleeps == bq.LOADED_READS


def test_a_copy_reading_between_the_load_and_the_description_gets_the_summary(wh):
    # The first copy has loaded its rows and is about to set the description.
    wh.tables[OUT] = (pd.DataFrame({"x": [1]}), bq._pending(FP))
    clock = Clock()
    clock.during_sleep = lambda: wh.tables.__setitem__(OUT, (pd.DataFrame({"x": [1]}), bq._done(FP, "the summary")))
    client = FakeCausilo()
    status, reply = bq.handle(body(CALL), wh, client=client, sleep=clock.sleep, clock=clock)
    assert (status, reply) == (200, {"replies": ["the summary"]}) and client.calls == []


def test_an_output_differing_from_the_source_only_in_case_is_allowed(wh, monkeypatch):
    monkeypatch.setenv("CAUSILO_ALLOWED_DATASETS", "proj-1.shop")
    upper = "proj-1.shop.Mixes"
    wh.tables[upper] = wh.tables.pop(SRC)
    status, reply = bq.handle(body([upper, "strength", "mix_id", SRC]), wh, client=FakeCausilo())
    assert status == 200, reply
    status, reply = bq.handle(body([upper, "strength", "mix_id", f" `{upper}` "]), wh, client=FakeCausilo())
    assert status == 400 and "table being read" in reply["errorMessage"]


def test_dataset_names_are_case_sensitive_and_projects_are_not(wh, monkeypatch):
    monkeypatch.setenv("CAUSILO_ALLOWED_DATASETS", "Proj-1.shop")
    allowed = bq._allowed()
    assert bq._table_name("proj-1.shop.t", "t", allowed) == "proj-1.shop.t"
    assert bq._table_name("proj-1.shop.T", "t", allowed) == "proj-1.shop.T"
    with pytest.raises(bq.CallError, match="outside the datasets"):
        bq._table_name("proj-1.Shop.t", "t", allowed)


def test_dates_and_times_are_not_read_as_db_dtypes():
    class Rows:
        def to_dataframe(self, **kw):
            seen.update(kw)
            return pd.DataFrame()

    class Client:
        def get_table(self, name):
            return type("T", (), {"schema": []})()

        def list_rows(self, name, selected_fields):
            return Rows()

        def query(self, sql):
            return Rows()

    wh = bq.Warehouse.__new__(bq.Warehouse)
    wh.client = Client()
    for kind in ("TABLE", "VIEW"):
        seen = {}
        wh.read("p.d.t", bq.TableInfo(kind=kind, num_rows=1, description="", columns=["a"]), ["a"])
        assert seen["date_dtype"] is None and seen["time_dtype"] is None


def test_dates_as_python_objects_are_sent_as_seconds():
    import datetime as dt

    client = FakeCausilo()
    df = pd.DataFrame({"day": [dt.date(2020, 1, 1) + dt.timedelta(days=i) for i in range(6)],
                       "y": [1.5, 2.25, 3.0, 4.75, None, None]})
    pred = bq.predict_frame(df, "y", client=client, task="reg")
    assert any("day: dates sent as seconds" in n for n in pred.notes)


@pytest.mark.parametrize("args, says", [
    (["proj-1.other.mixes", "strength", "mix_id", OUT], "outside the datasets"),
    ([SRC, "strength", "mix_id", "proj-1.secret.t"], "outside the datasets"),
    (["mixes", "strength", "mix_id", OUT], "not a full table name"),
    ([SRC, "strength", "mix_id", SRC], "table being read"),
    ([SRC, "strength", "nope", OUT], "No such columns"),
    (["proj-1.shop.missing", "strength", "mix_id", OUT], "No table"),
    ([SRC, "strength", "mix_id", OUT, "{bad"], "JSON object"),
    ([SRC, "strength", "mix_id", OUT, '{"where": "1=1"}'], "Unknown options"),
    ([SRC, "strength", "mix_id", OUT, '{"task": "big"}'], "'clf' or 'reg'"),
    ([SRC, "strength", "strength", OUT], "cannot be the target"),
    ([SRC, "strength"], "Expected 4 or 5 arguments"),
])
def test_bad_calls_fail_before_the_claim(wh, args, says):
    client = FakeCausilo()
    status, reply = bq.handle(body(args), wh, client=client)
    assert status == 400 and says in reply["errorMessage"], reply
    assert client.calls == [] and wh.claims == [] and wh.writes == []


def test_no_allowed_datasets_means_nothing_is_read(wh, monkeypatch):
    monkeypatch.delenv("CAUSILO_ALLOWED_DATASETS")
    status, reply = bq.handle(body(CALL), wh, client=FakeCausilo())
    assert status == 400 and "CAUSILO_ALLOWED_DATASETS" in reply["errorMessage"] and wh.reads == []


def test_a_large_table_is_refused_before_reading(wh, monkeypatch):
    monkeypatch.setenv("CAUSILO_MAX_SOURCE_ROWS", "10")
    status, reply = bq.handle(body(CALL), wh, client=FakeCausilo())
    assert status == 400 and "more than this service reads" in reply["errorMessage"] and wh.reads == []


def test_an_unavailable_api_is_retryable_and_frees_the_table(wh):
    causilo_client = pytest.importorskip("causilo_client")

    class Down(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            raise causilo_client.CausiloUnavailable("busy", status=503)

    status, reply = bq.handle(body(CALL), wh, client=Down())
    assert status == 503 and wh.writes == [] and OUT not in wh.tables


def test_a_timeout_is_not_retried_and_keeps_the_claim(wh):
    causilo_client = pytest.importorskip("causilo_client")

    class Slow(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            raise causilo_client.CausiloTimeout("no answer")

    status, _ = bq.handle(body(CALL), wh, client=Slow())
    assert status == 400 and OUT in wh.tables


def test_long_errors_stay_under_1_kb(wh):
    status, reply = bq.handle(body([SRC, "strength", "x" * 3000, OUT]), wh, client=FakeCausilo())
    assert status == 400 and len(reply["errorMessage"].encode()) < 1024


def test_wsgi_app_speaks_the_remote_function_contract(wh):
    app = bq.make_app(warehouse=lambda: wh, client=FakeCausilo())
    raw = json.dumps(body(CALL)).encode()
    seen = {}

    def start_response(status, headers):
        seen["status"] = status

    environ = {"REQUEST_METHOD": "POST", "CONTENT_LENGTH": str(len(raw)), "wsgi.input": io.BytesIO(raw)}
    reply = json.loads(b"".join(app(environ, start_response)))
    assert seen["status"].startswith("200") and len(reply["replies"]) == 1

    bad = {"REQUEST_METHOD": "POST", "CONTENT_LENGTH": "3", "wsgi.input": io.BytesIO(b"{x}")}
    reply = json.loads(b"".join(app(bad, start_response)))
    assert seen["status"].startswith("400") and "errorMessage" in reply
