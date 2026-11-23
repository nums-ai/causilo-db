from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa

from causilo_db import predict_table
from causilo_db.cli import main

from .fakes import FakeCausilo
from .test_core import table


@pytest.fixture
def sqlite(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'shop.db'}")
    df = table()
    df.insert(0, "mix_id", [f"M{i:03d}" for i in range(len(df))])
    df.to_sql("mixes", engine, index=False)
    return engine


def test_sqlalchemy_reads_predicts_and_writes(sqlite):
    result, pred = predict_table(sqlite, "mixes", "strength", id_column="mix_id", features=["cement", "grade"],
                                 out_table="mixes_pred", client=FakeCausilo())
    assert result.columns[0] == "mix_id" and result["mix_id"].iloc[0] == "M040"
    back = pd.read_sql("SELECT * FROM mixes_pred", sqlite)
    assert len(back) == 20 and back["mix_id"].tolist() == result["mix_id"].tolist()


def test_where_limits_the_rows(sqlite):
    result, pred = predict_table(sqlite, "mixes", "strength", where="cement > 150", features=["cement"],
                                 client=FakeCausilo())
    assert pred.n_context + pred.n_query == 43


def test_cli_prints_csv(sqlite, monkeypatch, capsys):
    import causilo_db.sql as s

    real = s.predict_frame
    monkeypatch.setattr(s, "predict_frame", lambda *a, **k: real(*a, **{**k, "client": FakeCausilo()}))
    code = main(["predict", str(sqlite.url), "mixes", "strength", "--id", "mix_id", "--features", "cement,grade"])
    out = capsys.readouterr()
    assert code == 0 and out.out.splitlines()[0].startswith("mix_id,strength,strength_p25")
    assert "20 blank cells of strength predicted from 40 rows" in out.err


def test_cli_explains_a_bad_target(sqlite, capsys):
    assert main(["predict", str(sqlite.url), "mixes", "cement"]) == 2
    assert "no blank cells" in capsys.readouterr().err


def test_duckdb_returns_a_relation():
    duckdb = pytest.importorskip("duckdb")
    import causilo_db.duckdb as cx

    con = duckdb.connect()
    df = table()
    con.register("t_df", df)
    con.sql("CREATE TABLE mixes AS SELECT * FROM t_df")
    rel = cx.predict(con, "mixes", "churn", features=["cement", "grade"], client=FakeCausilo())
    assert rel.shape[0] == 20 and "churn_probability" in rel.columns
    rel2 = cx.predict(con, "SELECT cement, strength FROM mixes WHERE cement > 120", "strength", client=FakeCausilo())
    assert rel2.shape[0] == 20


def test_an_existing_output_table_is_refused_before_predicting(sqlite):
    fake = FakeCausilo()
    predict_table(sqlite, "mixes", "strength", id_column="mix_id", features=["cement"], out_table="pred", client=fake)
    with pytest.raises(Exception, match="already exists"):
        predict_table(sqlite, "mixes", "strength", id_column="mix_id", features=["cement"], out_table="pred", client=fake)
    assert len(fake.calls) == 1


def test_duckdb_table_named_like_a_keyword_is_a_table():
    duckdb = pytest.importorskip("duckdb")
    import causilo_db.duckdb as cx

    con = duckdb.connect()
    df = table()
    con.register("t_df", df)
    con.sql("CREATE TABLE selected_rows AS SELECT * FROM t_df")
    rel = cx.predict(con, "selected_rows", "strength", features=["cement"], client=FakeCausilo())
    assert rel.shape[0] == 20


class _Refuse:
    def predict_with_metadata(self, *a, **k):
        raise AssertionError("the API was called")


@pytest.mark.parametrize("kwargs", [
    {"id_column": "strength"},
    {"id_column": "mix_id", "out_table": "x", "if_exists": "overwrite"},
])
def test_bad_arguments_are_refused_before_predicting(sqlite, kwargs):
    from causilo_db import CausiloDBError

    with pytest.raises(CausiloDBError):
        predict_table(sqlite, "mixes", "strength", client=_Refuse(), **kwargs)


def test_the_id_column_is_never_a_feature(sqlite):
    _, pred = predict_table(sqlite, "mixes", "strength", id_column="mix_id", features=["mix_id", "cement"],
                            client=FakeCausilo())
    assert pred.features == ["cement"]


def test_writing_over_the_source_table_is_refused(sqlite):
    from causilo_db import CausiloDBError

    with pytest.raises(CausiloDBError, match="being read"):
        predict_table(sqlite, "mixes", "strength", out_table="mixes", if_exists="replace", client=_Refuse())


def test_duckdb_reads_a_schema_qualified_table():
    duckdb = pytest.importorskip("duckdb")
    import causilo_db.duckdb as cx

    con = duckdb.connect()
    con.sql("CREATE SCHEMA s")
    df = table()
    con.register("t_df", df)
    con.sql("CREATE TABLE s.mixes AS SELECT * FROM t_df")
    rel = cx.predict(con, "s.mixes", "strength", client=FakeCausilo())
    assert len(rel.df()) == 20
    rel = cx.predict(con, "(SELECT * FROM s.mixes)", "strength", client=FakeCausilo())
    assert len(rel.df()) == 20
