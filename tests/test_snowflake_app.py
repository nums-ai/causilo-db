"""The Snowflake installer and Native App files match the core, and the app's handler runs."""

from __future__ import annotations

import importlib
import re
import sys
import types
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from .fakes import FakeCausilo

ROOT = Path(__file__).resolve().parent.parent
APP_DIR = ROOT / "snowflake" / "native_app"
sys.path.insert(0, str(ROOT / "scripts"))
build_sql = importlib.import_module("build_snowflake_sql")
build_app = importlib.import_module("build_snowflake_app")


def test_snowflake_installer_matches_the_core():
    assert (ROOT / "sql" / "snowflake" / "install.sql").read_text() == build_sql.render(), \
        "run python scripts/build_snowflake_sql.py"


@pytest.mark.parametrize("name", sorted(build_app.render()))
def test_native_app_files_match_the_core(name):
    assert (APP_DIR / name).read_text() == build_app.render()[name], "run python scripts/build_snowflake_app.py"


def test_manifest_names_files_that_exist():
    yaml = pytest.importorskip("yaml")
    manifest = yaml.safe_load((APP_DIR / "manifest.yml").read_text())
    assert manifest["manifest_version"] == 2
    for key in ("setup_script", "readme"):
        assert (APP_DIR / manifest["artifacts"][key]).is_file()
    privileges = [next(iter(p)) for p in manifest["privileges"]]
    assert privileges == ["CREATE EXTERNAL ACCESS INTEGRATION"]


def body() -> str:
    setup = (APP_DIR / "setup.sql").read_text()
    parts = setup.split("$$")
    assert len(parts) == 3, "setup.sql must hold exactly one $$ ... $$ body"
    return parts[1]


def test_setup_script_is_consistent():
    setup = (APP_DIR / "setup.sql").read_text()
    # Every IMPORTS path is relative to the version root, and the deploy script uploads it there.
    assert re.findall(r"IMPORTS = \('([^']+)'\)", setup) == ["/" + build_app.CLIENT_ZIP]
    # The specification and the network rule must name the host the same way.
    assert "VALUE_LIST = ('api.nums.world')" in setup and "HOST_PORTS = ('api.nums.world')" in setup
    # Native Apps refuse caller's rights.
    assert "EXECUTE AS CALLER" not in setup
    compile(body(), "setup.sql", "exec")


class _Table:
    def __init__(self, df):
        self.df = df

    def to_pandas(self):
        return self.df.copy()


class _Rows:
    def __init__(self, value):
        self.value = value

    def collect(self):
        return [(self.value,)]


class FakeSession:
    def __init__(self, df, key="csl_test"):
        self.df, self.key, self.read = df, key, []

    def table(self, name):
        self.read.append(name)
        return _Table(self.df)

    def sql(self, query):
        assert "GET_CONFIGURATION_VALUE" in query and "CAUSILO_API_KEY" in query
        return _Rows(self.key)

    def create_dataframe(self, df):
        return df


class UserError(Exception):
    pass


@pytest.fixture
def handler(monkeypatch):
    """The procedure body, run as Snowflake would, with a fake client and session."""
    monkeypatch.setitem(sys.modules, "_snowflake", types.SimpleNamespace(SnowflakeUserException=UserError))
    import causilo_client

    fake = FakeCausilo()
    keys = []

    def make(endpoint, token):
        keys.append(token)
        return fake

    monkeypatch.setattr(causilo_client, "Causilo", make)
    # Dataclasses look their module up in sys.modules.
    module = types.ModuleType("causilo_app")
    monkeypatch.setitem(sys.modules, "causilo_app", module)
    exec(compile(body(), "setup.sql", "exec"), module.__dict__)
    return module.run, fake, keys


def mixes():
    n = 30
    return pd.DataFrame({
        "MIX_ID": np.arange(n) + 100,
        "CEMENT": np.arange(n, dtype=float) * 3 + 100,
        "STRENGTH": [None if i % 3 == 0 else 20.0 + i * 0.7 for i in range(n)],
    })


def test_the_app_predicts_with_the_configured_key(handler):
    run, fake, keys = handler
    session = FakeSession(mixes())
    out = run(session, "SALES.PUBLIC.MIXES", "strength", "mix_id")
    assert session.read == ["SALES.PUBLIC.MIXES"] and keys == ["csl_test"]
    assert list(out.columns) == ["MIX_ID", "STRENGTH", "STRENGTH_P25", "STRENGTH_P50", "STRENGTH_P75"]
    assert out["MIX_ID"].tolist() == [100 + i for i in range(0, 30, 3)]
    assert fake.calls[0]["columns"] == ["CEMENT"]


def test_the_app_explains_a_missing_key(handler):
    run, fake, _ = handler
    with pytest.raises(UserError, match="SET CONFIGURATION CAUSILO_API_KEY"):
        run(FakeSession(mixes(), key=None), "SALES.PUBLIC.MIXES", "STRENGTH")
    assert fake.calls == []


def test_the_app_shows_only_a_message_for_a_bad_column(handler):
    run, _, _ = handler
    with pytest.raises(UserError, match="No column named"):
        run(FakeSession(mixes()), "SALES.PUBLIC.MIXES", "NOPE")


def test_client_zip_is_reproducible_and_importable(tmp_path):
    import causilo_client

    if causilo_client.__version__ != build_app.CLIENT_VERSION:
        pytest.skip(f"causilo-client {causilo_client.__version__} installed, the app bundles {build_app.CLIENT_VERSION}")
    a = build_app.client_zip(tmp_path / "a" / "causilo_client.zip")
    b = build_app.client_zip(tmp_path / "b" / "causilo_client.zip")
    assert a.read_bytes() == b.read_bytes()
    names = zipfile.ZipFile(a).namelist()
    assert "causilo_client/__init__.py" in names and not any(n.endswith(".pyc") for n in names)
    probe = "import sys; sys.path.insert(0, sys.argv[1]); import causilo_client; print(causilo_client.__file__)"
    import subprocess

    got = subprocess.run([sys.executable, "-I", "-c", probe, str(a)], capture_output=True, text=True, check=True)
    assert got.stdout.strip().startswith(str(a))
