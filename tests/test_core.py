from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from causilo_db import core
from causilo_db.core import CausiloDBError, infer_task, predict_frame

from .fakes import FakeCausilo


def table(n=60, blank_from=40):
    return pd.DataFrame({
        "cement": np.arange(n, dtype=float) * 3 + 100,
        "grade": ["a" if i % 2 else "b" for i in range(n)],
        "strength": [None if i >= blank_from else 20.0 + i * 0.7 for i in range(n)],
        "churn": [None if i >= blank_from else int(i % 3 == 0) for i in range(n)],
    })


def test_task_is_inferred():
    assert infer_task(pd.Series([1.5, 2.25, 3.0])) == "reg"
    assert infer_task(pd.Series([0, 1, 1, 0])) == "clf"
    assert infer_task(pd.Series(["x", "y"])) == "clf"
    assert infer_task(pd.Series(range(100))) == "reg"


def test_regression_fills_blanks_with_quantiles():
    fake = FakeCausilo()
    pred = predict_frame(table(), "strength", client=fake, exclude=["churn"])
    out = pred.frame
    assert pred.task == "reg" and pred.n_query == 20 and pred.n_context == 40
    assert list(out.columns) == ["row_number", "strength", "strength_p25", "strength_p50", "strength_p75"]
    assert out["row_number"].tolist() == list(range(40, 60))
    assert (out["strength_p25"] < out["strength_p75"]).all()
    assert fake.calls[0]["columns"] == ["cement", "grade"] and fake.calls[0]["target_in_context"]


def test_classification_returns_class_and_probabilities():
    pred = predict_frame(table(), "churn", client=FakeCausilo(), exclude=["strength"])
    out = pred.frame
    assert pred.task == "clf"
    assert set(out["churn"]) <= {0, 1}
    assert {"churn_probability", "churn_prob_0", "churn_prob_1"} <= set(out.columns)
    np.testing.assert_allclose(out["churn_prob_0"] + out["churn_prob_1"], 1.0)


def test_dates_go_as_numbers_and_empty_columns_are_dropped():
    df = table()
    df["when"] = pd.date_range("2026-01-01", periods=len(df), freq="D")
    df["empty"] = None
    fake = FakeCausilo()
    pred = predict_frame(df, "strength", client=fake, exclude=["churn"])
    assert "when" in pred.features and "empty" not in pred.features
    assert any("when" in n for n in pred.notes) and any("empty" in n for n in pred.notes)


def test_large_query_is_split_into_calls(monkeypatch):
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 3 * 50)
    fake = FakeCausilo()
    pred = predict_frame(table(n=200, blank_from=20), "strength", client=fake, exclude=["churn"])
    assert pred.calls == len(fake.calls) > 1
    assert len(pred.frame) == 180 and pred.frame["row_number"].tolist() == list(range(20, 200))


def test_refusals_say_why():
    with pytest.raises(CausiloDBError, match="no blank"):
        predict_frame(table(blank_from=60), "strength", client=FakeCausilo())
    with pytest.raises(CausiloDBError, match="No column"):
        predict_frame(table(), "nope", client=FakeCausilo())
    with pytest.raises(CausiloDBError, match="API key"):
        predict_frame(table(), "strength", api_key="", exclude=["churn"])


def test_decimal_and_date_columns_get_real_types():
    import datetime
    import decimal

    df = table()
    df["strength"] = [None if v is None or v != v else decimal.Decimal(str(round(v, 2))) for v in df["strength"]]
    df["cast_on"] = [datetime.date(2026, 1, 1) + datetime.timedelta(days=i) for i in range(len(df))]
    fake = FakeCausilo()
    pred = predict_frame(df, "strength", client=fake, exclude=["churn"])
    assert pred.task == "reg"
    assert "cast_on" in pred.features and any("seconds since" in n for n in pred.notes)


def test_target_named_in_features_is_not_sent_as_a_feature():
    fake = FakeCausilo()
    predict_frame(table(), "strength", features=["cement", "strength"], client=fake)
    assert fake.calls[0]["columns"] == ["cement"]


def test_class_columns_are_safe_names_without_case_clashes():
    from causilo_db.core import _class_columns

    names = _class_columns("trim", ["Lounge Plus", "a,b", "A", "a"])
    assert names == ["trim_prob_Lounge_Plus", "trim_prob_a_b", "trim_prob_A", "trim_prob_a_2"]


def test_ids_that_only_look_like_result_names_are_allowed():
    from causilo_db.core import check_id_column

    for ok in ["order_pk", "price_plan_id", "churn_period_id"]:
        check_id_column(ok, ok.split("_")[0])
    for bad in [("strength", "Strength"), ("strength_p50", "strength"), ("Row_Number", "y"), ("y_prob_a", "y")]:
        with pytest.raises(CausiloDBError):
            check_id_column(*bad)


def test_list_cells_are_sent_as_text():
    df = table()
    df["tags"] = [["a", "b"]] * len(df)
    client = FakeCausilo()
    predict_frame(df, "strength", client=client)
    assert "tags" in client.calls[0]["columns"]


def test_a_wide_table_is_not_answered_a_few_rows_per_call(monkeypatch):
    # 10,000 rows fit in a call; 12,000 filled and 20,000 blank rows.
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 141 * 10_000)
    rng = np.random.default_rng(1)
    n_cols, n = 140, 32_000
    df = pd.DataFrame(rng.normal(size=(n, n_cols)), columns=[f"x{i}" for i in range(n_cols)])
    df["y"] = rng.normal(size=n)
    df.loc[12_000:, "y"] = np.nan
    client = FakeCausilo()
    predict_frame(df, "y", client=client)
    assert len(client.calls) == 4 and client.calls[0]["n_context"] == 5_000


def test_a_text_target_with_too_many_values_is_refused_unless_asked():
    big = pd.concat([table()] * 5, ignore_index=True)
    big["name"] = [f"n{i}" for i in range(len(big))]
    big.loc[big.index % 10 == 0, "name"] = None
    with pytest.raises(CausiloDBError, match="distinct values"):
        predict_frame(big, "name", client=FakeCausilo())


def test_a_later_call_failing_says_how_many_calls_were_charged(monkeypatch):
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 3 * 50)

    class ThirdFails(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            if len(self.calls) == 2:
                raise RuntimeError("connection reset")
            return super().predict_with_metadata(*a, **k)

    with pytest.raises(core.PartialPredictionError) as info:
        predict_frame(table(n=200, blank_from=20), "strength", client=ThirdFails(), exclude=["churn"])
    err = info.value
    assert isinstance(err, CausiloDBError) and isinstance(err.__cause__, RuntimeError)
    assert err.calls_completed == 2 and err.calls_planned > 2 and 0 < err.rows_completed < err.n_query == 180
    assert "Call 3 of" in str(err) and "2 calls before it completed and were charged" in str(err)


def test_a_first_call_failing_raises_the_original_error(monkeypatch):
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 3 * 50)

    class Fails(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            raise RuntimeError("connection reset")

    with pytest.raises(RuntimeError, match="connection reset"):
        predict_frame(table(n=200, blank_from=20), "strength", client=Fails(), exclude=["churn"])


def test_an_unusable_answer_counts_its_call_as_charged(monkeypatch):
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 3 * 50)

    class ThirdMisshapen(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            pred, meta = super().predict_with_metadata(*a, **k)
            if len(self.calls) == 3:
                return np.zeros(7), meta  # cannot be one row per query row
            return pred, meta

    with pytest.raises(core.PartialPredictionError) as info:
        predict_frame(table(n=200, blank_from=20), "strength", client=ThirdMisshapen(), exclude=["churn"])
    err = info.value
    assert err.calls_completed == 3 and isinstance(err.__cause__, ValueError)
    assert "Call 3 of" in str(err) and "3 calls up to and including it were charged" in str(err)


def test_an_unusable_first_answer_is_reported_as_charged(monkeypatch):
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 3 * 50)

    class Misshapen(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            _, meta = super().predict_with_metadata(*a, **k)
            return np.zeros(7), meta

    with pytest.raises(core.PartialPredictionError) as info:
        predict_frame(table(n=200, blank_from=20), "strength", client=Misshapen(), exclude=["churn"])
    err = info.value
    assert err.calls_completed == 1 and err.rows_completed > 0
    assert "Call 1 of" in str(err) and "1 call up to and including it was charged" in str(err)


def test_a_changed_class_order_counts_the_call_that_returned_it(monkeypatch):
    monkeypatch.setattr(core, "MAX_CELLS_PER_CALL", 3 * 50)

    class Reorders(FakeCausilo):
        def predict_with_metadata(self, *a, **k):
            pred, meta = super().predict_with_metadata(*a, **k)
            if len(self.calls) == 2:
                meta = {"classes": list(reversed(meta["classes"]))}
            return pred, meta

    df = table(n=200, blank_from=20)
    with pytest.raises(core.PartialPredictionError, match="class order changed") as info:
        predict_frame(df, "churn", client=Reorders(), exclude=["strength"], task="clf")
    assert info.value.calls_completed == 2
