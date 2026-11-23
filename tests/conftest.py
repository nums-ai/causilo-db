import pytest


@pytest.fixture(autouse=True)
def _no_real_key(monkeypatch):
    # A key in the environment would turn the "no key" test into a real, billed call.
    monkeypatch.delenv("CAUSILO_API_KEY", raising=False)
    monkeypatch.delenv("CAUSILO_ENDPOINT", raising=False)
