"""Check a real database end to end against the live API.

    CAUSILO_API_KEY=csl_... python scripts/live_check.py <sqlalchemy-url> [--function]

Loads the two example tables with part of the answers hidden, runs the
``causilo-db`` command line with ``--out``, reads the written tables back and
scores them against the hidden answers. Uses the tables ``cx_mixes``,
``cx_mixes_pred``, ``cx_fiat`` and ``cx_fiat_pred``, replacing them.

With ``--function`` (PostgreSQL) the predictions come from the installed
``causilo.predict`` function instead, written with ``CREATE TABLE ... AS
SELECT``; the car table goes in through a view ``cx_fiat_v`` of the same six
features, since the function takes no feature list, and the view is dropped
afterwards.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy as sa

from causilo_db.sql import write_table

ROOT = Path(__file__).resolve().parent.parent


def run(url: str, table: str, target: str, id_column: str, extra: list[str]) -> None:
    cmd = [sys.executable, "-m", "causilo_db.cli", "predict", url, table, target, "--id", id_column,
           "--out", f"{table}_pred", "--if-exists", "replace", *extra]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(r.stderr.strip() or r.stdout.strip())
    said = (r.stderr.strip() or r.stdout.strip()).splitlines()
    if said:
        print("  " + said[0])


def run_function(engine, table: str, target: str, id_column: str, features: list[str]) -> None:
    source = table
    with engine.begin() as con:
        if features:
            source = f"{table}_v"
            con.execute(sa.text(f"DROP VIEW IF EXISTS {source}"))
            con.execute(sa.text(f"CREATE VIEW {source} AS SELECT {id_column}, {', '.join(features)}, {target} "
                                f"FROM {table}"))
        con.execute(sa.text("SELECT set_config('causilo.api_key', :k, false)"),
                    {"k": os.environ["CAUSILO_API_KEY"]})
        con.execute(sa.text(f"DROP TABLE IF EXISTS {table}_pred"))
        con.execute(sa.text(f"CREATE TABLE {table}_pred AS SELECT row_id::bigint AS {id_column}, "
                            f"prediction AS {target} FROM causilo.predict('{source}', '{target}', '{id_column}')"))
        if features:
            con.execute(sa.text(f"DROP VIEW {source}"))


def read(engine, table: str) -> pd.DataFrame:
    out = pd.read_sql(sa.text(f"select * from {table}"), engine)
    out.columns = [c.lower() for c in out.columns]
    return out


def main() -> None:
    url = sys.argv[1]
    function = "--function" in sys.argv[2:]
    engine = sa.create_engine(url)

    c = pd.read_csv(ROOT / "examples" / "concrete.csv")
    c = c[c["strength"].notna()].reset_index(drop=True)
    c.insert(0, "mix_id", range(len(c)))
    truth = c.set_index("mix_id")["strength"].copy()
    c.loc[c["mix_id"] % 3 == 0, "strength"] = np.nan
    write_table(c, engine, "cx_mixes", if_exists="replace")
    print("regression (concrete strength):")
    if function:
        run_function(engine, "cx_mixes", "strength", "mix_id", [])
    else:
        run(url, "cx_mixes", "strength", "mix_id", [])
    out = read(engine, "cx_mixes_pred")
    expected = set(c.loc[c["strength"].isna(), "mix_id"])
    if set(out["mix_id"].astype(int)) != expected or len(out) != len(expected):
        raise SystemExit(f"  the written rows are not the {len(expected)} hidden ones")
    y = truth.loc[out["mix_id"].astype(int)].to_numpy()
    p = out["strength"].astype(float).to_numpy()
    r2 = 1 - ((y - p) ** 2).sum() / ((y - y.mean()) ** 2).sum()
    print(f"  {len(out)} rows read back, R2 {r2:.3f}, MAE {np.abs(y - p).mean():.2f}")

    f = pd.read_csv(ROOT / "examples" / "fiat500.csv")
    f.insert(0, "car_id", range(len(f)))
    truth = f.set_index("car_id")["trim"].copy()
    f.loc[f["car_id"] % 4 == 0, "trim"] = None
    write_table(f, engine, "cx_fiat", if_exists="replace")
    print("classification (fiat trim):")
    features = ["power_kw", "age_days", "km", "previous_owners", "lat", "lon"]
    if function:
        run_function(engine, "cx_fiat", "trim", "car_id", features)
    else:
        run(url, "cx_fiat", "trim", "car_id", ["--features", ",".join(features)])
    out = read(engine, "cx_fiat_pred")
    expected = set(f.loc[f["trim"].isna(), "car_id"])
    if set(out["car_id"].astype(int)) != expected or len(out) != len(expected):
        raise SystemExit(f"  the written rows are not the {len(expected)} hidden ones")
    hidden = truth.loc[out["car_id"].astype(int)]
    acc = (out["trim"].to_numpy() == truth.loc[out["car_id"].astype(int)].to_numpy()).mean()
    baseline = (hidden == truth[f["trim"].notna().to_numpy()].mode()[0]).mean()
    print(f"  {len(out)} rows read back, accuracy {acc:.3f} (guessing the most common class: {baseline:.3f})")


if __name__ == "__main__":
    main()
