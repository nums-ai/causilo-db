"""The Spark adapter on a local session. Skipped without pyspark or a Java runtime."""

from __future__ import annotations

import subprocess

import numpy as np
import pandas as pd
import pytest

from causilo_db import CausiloDBError

from .fakes import FakeCausilo
from .test_core import table

pyspark = pytest.importorskip("pyspark")


def _has_java() -> bool:
    # macOS ships a /usr/bin/java stub that only prints "Unable to locate a Java Runtime".
    try:
        return subprocess.run(["java", "-version"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


if not _has_java():
    pytest.skip("no Java runtime for Spark", allow_module_level=True)

import causilo_db.spark as cx  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")

    s = (SparkSession.builder.master("local[1]").appName("causilo-db-tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.warehouse.dir", str(warehouse)).getOrCreate())
    yield s
    s.stop()


def frame() -> pd.DataFrame:
    df = table()
    df.insert(0, "mix_id", [f"M{i:03d}" for i in range(len(df))])
    return df


def test_predicts_by_id(spark):
    sdf = spark.createDataFrame(frame())
    fake = FakeCausilo()
    out = cx.predict(sdf, "strength", id_column="mix_id", features=["cement", "grade"], client=fake).toPandas()
    assert list(out.columns)[:2] == ["mix_id", "strength"]
    assert len(out) == 20 and out["mix_id"].is_unique
    assert fake.calls[0]["columns"] == ["cement", "grade"]


def test_nan_counts_as_blank_and_rows_come_back_without_id(spark):
    df = frame()[["cement", "grade", "strength"]]
    df["strength"] = df["strength"].astype("float64")
    sdf = spark.createDataFrame(df)  # pandas NaN arrives as NaN, not null
    out = cx.predict(sdf, "strength", client=FakeCausilo()).toPandas()
    assert len(out) == int(df["strength"].isna().sum())
    assert {"cement", "grade", "strength", "strength_p25"} <= set(out.columns)
    # Each predicted row keeps its own features: the fake predicts twice the first column.
    assert np.allclose(out["strength"], out["cement"] * 2)


def test_context_is_sampled_inside_spark(spark, monkeypatch):
    monkeypatch.setattr(cx, "MAX_CONTEXT_ROWS", 30)
    fake = FakeCausilo()
    cx.predict(spark.createDataFrame(frame()), "strength", id_column="mix_id", client=fake)
    assert fake.calls[0]["n_context"] <= 30


def test_refuses_what_it_cannot_do(spark):
    sdf = spark.createDataFrame(frame())
    with pytest.raises(CausiloDBError, match="No column"):
        cx.predict(sdf, "nope", client=FakeCausilo())
    with pytest.raises(CausiloDBError, match="max_query_rows"):
        cx.predict(sdf, "strength", max_query_rows=5, client=FakeCausilo())


def test_predict_table_saves_the_result(spark):
    spark.createDataFrame(frame()).createOrReplaceTempView("mixes")
    cx.predict_table(spark, "mixes", "churn", id_column="mix_id", output_table="mixes_churn_pred",
                     client=FakeCausilo())
    assert spark.table("mixes_churn_pred").count() == 20


def test_predict_table_refuses_an_existing_table_before_predicting(spark):
    spark.createDataFrame(frame()).createOrReplaceTempView("mixes2")
    fake = FakeCausilo()
    cx.predict_table(spark, "mixes2", "churn", id_column="mix_id", output_table="mixes2_pred", client=fake)
    with pytest.raises(CausiloDBError, match="already exists"):
        cx.predict_table(spark, "mixes2", "churn", id_column="mix_id", output_table="mixes2_pred", client=fake)
    assert len(fake.calls) == 1
