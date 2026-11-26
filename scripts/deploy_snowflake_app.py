"""Deploy the Causilo Native App to a Snowflake account, install it there and check it live.

    pip install snowflake-connector-python
    export SNOWFLAKE_ACCOUNT=myorg-myaccount SNOWFLAKE_USER=me
    export SNOWFLAKE_PASSWORD=...           # a programmatic access token
    export CAUSILO_API_KEY_FILE=~/causilo.key
    python scripts/deploy_snowflake_app.py            # deploy, install, test
    python scripts/deploy_snowflake_app.py --cleanup  # drop everything it made

Optional: SNOWFLAKE_ROLE (default ACCOUNTADMIN) and SNOWFLAKE_WAREHOUSE (default
the user's default warehouse).

What it does, in the account it connects to:

1. creates the application package CAUSILO_APP_TEST_PKG (dropping an earlier
   one of that name, and its test app, first), uploads snowflake/native_app/
   and the causilo-client zip to a stage in it, registers the app version (V0_2_0 for 0.2.0) and
   sets it as the default of the QA release channel;
2. installs the app CAUSILO_APP_TEST from that channel, approves its
   CAUSILO_API external access specification, grants its application roles to
   the current role and sets the CAUSILO_API_KEY configuration from the key
   file;
3. loads the two example tables into the database CAUSILO_APP_TEST_DATA with
   part of the answers hidden (every third concrete strength, every fourth car
   trim, as scripts/live_check.py does), grants them to the app, calls
   CORE.PREDICT, writes each result to a table with RESULT_SCAN, reads it back
   and scores it against the hidden answers.

The two predictions are charged to the key like any other call. The key is
never printed. --cleanup drops the app (CASCADE), the package and the test
database, and reports any app-made integration left behind.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_snowflake_app import APP_DIR, APP_VERSION, CLIENT_ZIP, ROOT, client_zip, render  # noqa: E402

PACKAGE = "CAUSILO_APP_TEST_PKG"
APP = "CAUSILO_APP_TEST"
DATA = "CAUSILO_APP_TEST_DATA"
VERSION = APP_VERSION
STAGE = f"{PACKAGE}.STAGE_CONTENT.APP_STAGE"
INTEGRATION = "CAUSILO_APP_API_ACCESS"
SPECIFICATION = "CAUSILO_API"

# What the existing CAUSILO.PREDICT procedure scored on the same split.
EXPECTED_R2 = 0.939
EXPECTED_ACCURACY = 0.735


def connect():
    import snowflake.connector

    missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD") if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"Set {', '.join(missing)}.")
    kwargs = dict(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
                  password=os.environ["SNOWFLAKE_PASSWORD"], role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
                  session_parameters={"QUERY_TAG": "causilo-native-app-test"})
    if os.environ.get("SNOWFLAKE_WAREHOUSE"):
        kwargs["warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]
    return snowflake.connector.connect(**kwargs)


def sql(cur, statement: str, params=None, quiet: bool = False):
    if not quiet:
        print("  " + " ".join(statement.split())[:150])
    cur.execute(statement, params)
    return cur


def rows(cur) -> list[dict]:
    names = [d[0].lower() for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def read_key() -> str:
    path = os.environ.get("CAUSILO_API_KEY_FILE")
    if not path:
        raise SystemExit("Set CAUSILO_API_KEY_FILE to a file that holds the Causilo API key.")
    key = Path(path).expanduser().read_text().strip()
    # The key is put into one SQL statement; refuse anything that is not a plain token.
    if not re.fullmatch(r"[A-Za-z0-9_\-]{8,200}", key):
        raise SystemExit(f"{path} does not hold a Causilo API key on its own.")
    return key


def cleanup(cur) -> None:
    print("cleanup:")
    sql(cur, f"DROP APPLICATION IF EXISTS {APP} CASCADE")
    sql(cur, f"DROP APPLICATION PACKAGE IF EXISTS {PACKAGE}")
    sql(cur, f"DROP DATABASE IF EXISTS {DATA}")
    left = rows(sql(cur, f"SHOW INTEGRATIONS LIKE '{INTEGRATION}'"))
    if left:
        print(f"  integration {INTEGRATION} is still there (owner {left[0].get('owner')}); "
              f"drop it with DROP INTEGRATION {INTEGRATION} once nothing uses it")


def check_files() -> None:
    for name, text in render().items():
        on_disk = (APP_DIR / name).read_text()
        if on_disk != text:
            raise SystemExit(f"snowflake/native_app/{name} is out of date; run python scripts/build_snowflake_app.py")


def deploy(cur, key: str) -> None:
    check_files()
    with tempfile.TemporaryDirectory() as tmp:
        zipped = client_zip(Path(tmp) / CLIENT_ZIP)
        print("package:")
        sql(cur, f"DROP APPLICATION IF EXISTS {APP} CASCADE")
        sql(cur, f"DROP APPLICATION PACKAGE IF EXISTS {PACKAGE}")
        sql(cur, f"CREATE APPLICATION PACKAGE {PACKAGE}")
        sql(cur, f"CREATE SCHEMA {PACKAGE}.STAGE_CONTENT")
        sql(cur, f"CREATE STAGE {STAGE}")
        uploads = [(APP_DIR / n, "") for n in ("manifest.yml", "setup.sql", "README.md")]
        uploads.append((zipped, str(Path(CLIENT_ZIP).parent)))
        for path, sub in uploads:
            target = f"@{STAGE}/{VERSION}" + (f"/{sub}" if sub else "")
            sql(cur, f"PUT 'file://{path.resolve().as_posix()}' '{target}' AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        sql(cur, f"ALTER APPLICATION PACKAGE {PACKAGE} REGISTER VERSION {VERSION} USING '@{STAGE}/{VERSION}'")
    sql(cur, f"ALTER APPLICATION PACKAGE {PACKAGE} MODIFY RELEASE CHANNEL QA ADD VERSION {VERSION}")
    versions = [r for r in rows(sql(cur, f"SHOW VERSIONS IN APPLICATION PACKAGE {PACKAGE}"))
                if str(r.get("version")).upper() == VERSION]
    if not versions:
        raise SystemExit(f"version {VERSION} is not in SHOW VERSIONS")
    patch = max(int(r["patch"]) for r in versions)
    sql(cur, f"ALTER APPLICATION PACKAGE {PACKAGE} MODIFY RELEASE CHANNEL QA "
             f"SET DEFAULT RELEASE DIRECTIVE VERSION = {VERSION} PATCH = {patch}")

    print("app:")
    sql(cur, f"CREATE APPLICATION {APP} FROM APPLICATION PACKAGE {PACKAGE} USING RELEASE CHANNEL QA")
    specs = [r for r in rows(sql(cur, f"SHOW SPECIFICATIONS IN APPLICATION {APP}"))
             if str(r.get("name")).upper() == SPECIFICATION]
    if not specs:
        raise SystemExit(f"the app did not request the {SPECIFICATION} specification")
    if str(specs[0].get("status")).upper() != "APPROVED":
        sql(cur, f"ALTER APPLICATION {APP} APPROVE SPECIFICATION {SPECIFICATION} "
                 f"SEQUENCE_NUMBER = {int(specs[0]['sequence_number'])}")
    role = rows(sql(cur, "SELECT CURRENT_ROLE() AS r", quiet=True))[0]["r"]
    for app_role in ("APP_ADMIN", "APP_USER"):
        sql(cur, f'GRANT APPLICATION ROLE {APP}.{app_role} TO ROLE "{role}"')
    print(f"  ALTER APPLICATION {APP} SET CONFIGURATION CAUSILO_API_KEY VALUE = '<from CAUSILO_API_KEY_FILE>'")
    sql(cur, f"ALTER APPLICATION {APP} SET CONFIGURATION CAUSILO_API_KEY VALUE = '{key}'", quiet=True)


def load(cur, table: str, df: pd.DataFrame) -> None:
    types = {c: ("NUMBER(38,0)" if pd.api.types.is_integer_dtype(df[c]) else
                 "FLOAT" if pd.api.types.is_float_dtype(df[c]) else "STRING") for c in df.columns}
    cols = ", ".join(f'"{c.upper()}" {t}' for c, t in types.items())
    sql(cur, f"CREATE OR REPLACE TABLE {DATA}.PUBLIC.{table} ({cols})")
    values = [[None if pd.isna(v) else v.item() if isinstance(v, np.generic) else v for v in row]
              for row in df.itertuples(index=False)]
    marks = ", ".join(["%s"] * len(df.columns))
    print(f"  INSERT {len(values)} rows into {table}")
    cur.executemany(f"INSERT INTO {DATA}.PUBLIC.{table} VALUES ({marks})", values)


def predict(cur, source: str, target: str, id_column: str) -> pd.DataFrame:
    sql(cur, f"CALL {APP}.CORE.PREDICT('{DATA}.PUBLIC.{source}', '{target}', '{id_column}')")
    out = f"{DATA}.PUBLIC.{source}_PRED"
    sql(cur, f"CREATE OR REPLACE TABLE {out} AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
    back = pd.DataFrame(rows(sql(cur, f"SELECT * FROM {out}")))
    return back


def test(cur) -> bool:
    print("data:")
    sql(cur, f"CREATE OR REPLACE DATABASE {DATA}")

    c = pd.read_csv(ROOT / "examples" / "concrete.csv")
    c = c[c["strength"].notna()].reset_index(drop=True)
    c.insert(0, "mix_id", range(len(c)))
    c_truth = c.set_index("mix_id")["strength"].copy()
    c.loc[c["mix_id"] % 3 == 0, "strength"] = np.nan
    load(cur, "CX_MIXES", c)

    f = pd.read_csv(ROOT / "examples" / "fiat500.csv")
    f.insert(0, "car_id", range(len(f)))
    f_truth = f.set_index("car_id")["trim"].copy()
    f.loc[f["car_id"] % 4 == 0, "trim"] = None
    load(cur, "CX_FIAT", f)
    # The procedure takes no feature list; a view picks the same features as live_check.py.
    sql(cur, f'CREATE OR REPLACE VIEW {DATA}.PUBLIC.CX_FIAT_V AS SELECT "CAR_ID", "TRIM", "POWER_KW", '
             f'"AGE_DAYS", "KM", "PREVIOUS_OWNERS", "LAT", "LON" FROM {DATA}.PUBLIC.CX_FIAT')

    # What a consumer runs to let the app read a table.
    sql(cur, f"GRANT USAGE ON DATABASE {DATA} TO APPLICATION {APP}")
    sql(cur, f"GRANT USAGE ON SCHEMA {DATA}.PUBLIC TO APPLICATION {APP}")
    sql(cur, f"GRANT SELECT ON TABLE {DATA}.PUBLIC.CX_MIXES TO APPLICATION {APP}")
    sql(cur, f"GRANT SELECT ON VIEW {DATA}.PUBLIC.CX_FIAT_V TO APPLICATION {APP}")

    ok = True
    print("regression (concrete strength):")
    out = predict(cur, "CX_MIXES", "STRENGTH", "MIX_ID")
    expected = set(c.loc[c["strength"].isna(), "mix_id"])
    if set(out["mix_id"].astype(int)) != expected or len(out) != len(expected):
        print(f"  FAIL: the written rows are not the {len(expected)} hidden ones")
        ok = False
    else:
        y = c_truth.loc[out["mix_id"].astype(int)].to_numpy()
        p = out["strength"].astype(float).to_numpy()
        r2 = 1 - ((y - p) ** 2).sum() / ((y - y.mean()) ** 2).sum()
        print(f"  {len(out)} rows read back, R2 {r2:.3f}, MAE {np.abs(y - p).mean():.2f} "
              f"(the CAUSILO.PREDICT procedure: R2 {EXPECTED_R2})")
        ok &= r2 > 0.9

    print("classification (car trim):")
    out = predict(cur, "CX_FIAT_V", "TRIM", "CAR_ID")
    expected = set(f.loc[f["trim"].isna(), "car_id"])
    if set(out["car_id"].astype(int)) != expected or len(out) != len(expected):
        print(f"  FAIL: the written rows are not the {len(expected)} hidden ones")
        ok = False
    else:
        hidden = f_truth.loc[out["car_id"].astype(int)]
        acc = (out["trim"].to_numpy() == hidden.to_numpy()).mean()
        baseline = (hidden == f_truth[f["trim"].notna().to_numpy()].mode()[0]).mean()
        print(f"  {len(out)} rows read back, accuracy {acc:.3f} (guessing the most common class: {baseline:.3f}; "
              f"the CAUSILO.PREDICT procedure: {EXPECTED_ACCURACY})")
        ok &= acc >= baseline
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cleanup", action="store_true", help="drop the app, the package and the test database, then stop")
    ap.add_argument("--no-test", action="store_true", help="deploy and install, but load no tables and make no prediction")
    args = ap.parse_args()
    key = None if args.cleanup else read_key()
    con = connect()
    try:
        cur = con.cursor()
        if args.cleanup:
            cleanup(cur)
            return
        if not args.no_test and not rows(sql(cur, "SELECT CURRENT_WAREHOUSE() AS w", quiet=True))[0]["w"]:
            raise SystemExit("No warehouse: set SNOWFLAKE_WAREHOUSE or give the user a default warehouse.")
        deploy(cur, key)
        if args.no_test:
            return
        ok = test(cur)
        print("PASS" if ok else "FAIL")
        print(f"Run python scripts/deploy_snowflake_app.py --cleanup to drop {APP}, {PACKAGE} and {DATA}.")
        if not ok:
            raise SystemExit(1)
    finally:
        con.close()


if __name__ == "__main__":
    main()
