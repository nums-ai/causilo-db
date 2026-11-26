"""Check the BigQuery remote function end to end against the live API.

    python scripts/bigquery_live_check.py <project> <dataset> [<location>]

Run after ``bigquery/deploy.sh`` with ``DATASET=<dataset>``. Loads the two
example tables into the dataset with part of the answers hidden, exactly as
``scripts/live_check.py`` does, calls ``causilo_predict`` and
``causilo_predict_with_options`` from a BigQuery query, reads the written
tables back and scores them against the hidden answers. Then sends the same
calls again, three copies in one query, to check that a repeated call is
answered from the table already written, and sends three copies of a new call
in one query, which should be predicted once. Uses the tables ``cx_mixes``,
``cx_mixes_pred``, ``cx_mixes_pred_copies``, ``cx_fiat`` and ``cx_fiat_pred``,
replacing them.

Needs ``google-cloud-bigquery``. Credentials are Application Default
Credentials, or a token in ``GOOGLE_OAUTH_ACCESS_TOKEN`` (for example from
``gcloud auth print-access-token``). Each prediction is charged to the key the
function was deployed with.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent


def client(project: str, location: str) -> bigquery.Client:
    token = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN")
    if token:
        from google.oauth2.credentials import Credentials

        return bigquery.Client(project=project, location=location, credentials=Credentials(token))
    return bigquery.Client(project=project, location=location)


def rows(bq: bigquery.Client, sql: str) -> pd.DataFrame:
    result = bq.query(sql).result()
    return pd.DataFrame([dict(r.items()) for r in result])


def load(bq: bigquery.Client, df: pd.DataFrame, name: str) -> None:
    bq.delete_table(name + "_pred", not_found_ok=True)
    config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(df, name, job_config=config).result()


def call(bq: bigquery.Client, sql: str) -> list[str]:
    t = time.time()
    out = rows(bq, sql)
    print(f"  {time.time() - t:.0f}s: {out.iloc[0, 0]}")
    return out.iloc[:, 0].tolist()


def main() -> None:
    project, dataset = sys.argv[1], sys.argv[2]
    location = sys.argv[3] if len(sys.argv) > 3 else None
    bq = client(project, location)
    ds = f"{project}.{dataset}"
    fn = f"`{ds}`.causilo_predict"
    fn_opts = f"`{ds}`.causilo_predict_with_options"

    c = pd.read_csv(ROOT / "examples" / "concrete.csv")
    c = c[c["strength"].notna()].reset_index(drop=True)
    c.insert(0, "mix_id", range(len(c)))
    truth = c.set_index("mix_id")["strength"].copy()
    c.loc[c["mix_id"] % 3 == 0, "strength"] = np.nan
    load(bq, c, f"{ds}.cx_mixes")
    print("regression (concrete strength):")
    mixes = f"'{ds}.cx_mixes', 'strength', 'mix_id', '{ds}.cx_mixes_pred'"
    first = call(bq, f"SELECT {fn}({mixes})")
    out = rows(bq, f"SELECT * FROM `{ds}.cx_mixes_pred`")
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
    load(bq, f, f"{ds}.cx_fiat")
    print("classification (fiat trim):")
    fiat = (f"'{ds}.cx_fiat', 'trim', 'car_id', '{ds}.cx_fiat_pred', "
            """'{"features": "power_kw,age_days,km,previous_owners,lat,lon"}'""")
    call(bq, f"SELECT {fn_opts}({fiat})")
    out = rows(bq, f"SELECT * FROM `{ds}.cx_fiat_pred`")
    expected = set(f.loc[f["trim"].isna(), "car_id"])
    if set(out["car_id"].astype(int)) != expected or len(out) != len(expected):
        raise SystemExit(f"  the written rows are not the {len(expected)} hidden ones")
    hidden = truth.loc[out["car_id"].astype(int)]
    acc = (out["trim"].to_numpy() == hidden.to_numpy()).mean()
    baseline = (hidden == truth[f["trim"].notna().to_numpy()].mode()[0]).mean()
    print(f"  {len(out)} rows read back, accuracy {acc:.3f} (guessing the most common class: {baseline:.3f})")

    print("the same call again, three copies in one query:")
    before = bq.get_table(f"{ds}.cx_mixes_pred").modified
    again = call(bq, f"SELECT {fn}({mixes}) FROM UNNEST([1, 2, 3])")
    after = bq.get_table(f"{ds}.cx_mixes_pred").modified
    same = len(again) == 3 and set(again) == set(first)
    print(f"  {'same reply' if same else 'DIFFERENT reply'}, table "
          f"{'untouched' if before == after else 'MODIFIED'}")
    if not same or before != after:
        raise SystemExit(1)

    print("a new call, three copies in one query:")
    copies = f"{ds}.cx_mixes_pred_copies"
    bq.delete_table(copies, not_found_ok=True)
    got = call(bq, f"SELECT {fn}('{ds}.cx_mixes', 'strength', 'mix_id', '{copies}') FROM UNNEST([1, 2, 3])")
    n = bq.get_table(copies).num_rows
    print(f"  {len(set(got))} distinct repl{'y' if len(set(got)) == 1 else 'ies'}, {n} rows written "
          "(the service log shows how many copies predicted)")
    if len(set(got)) != 1 or n != int(c["strength"].isna().sum()):
        raise SystemExit(1)

    print("a different call into an existing table:")
    try:
        rows(bq, f"SELECT {fn}('{ds}.cx_mixes', 'cement', 'mix_id', '{ds}.cx_mixes_pred')")
    except Exception as exc:  # the function fails the query, as it should
        print(f"  refused: {str(exc).splitlines()[0][:200]}")
    else:
        raise SystemExit("  it was not refused")


if __name__ == "__main__":
    main()
